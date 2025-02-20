import argparse
from functools import cached_property
import hashlib
import io
import logging
from pathlib import Path
from typing import Generator
import zipfile

import zipfile_deflate64

from extract import CDEAPI
from load import DuckDBManager
from utils import setup_logging
from parsers import nibrs
from parsers.constants import STATE_CODES


class SchemaMissingError(Exception):
    """Raised when a not-yet-created duckdb schema is referenced."""

    pass


class TableMissingError(Exception):
    """Raised when a not-yet-created duckdb table is referenced."""

    pass


class SourceDataChangedError(Exception):
    """Raised when a static data file has changed."""

    pass


class DatabaseRecordCountError(Exception):
    """Raised when the database has an unexpected number of records."""

    pass


class NIBRSMasterFilePipeline:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.db_manager = self.get_db_manager()
        setup_logging()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setup()

    def setup(self):
        self.setup_schemas()
        self.setup_nibrs_metadata_table()

    def get_db_manager(self) -> DuckDBManager:
        db_path = self.project_root.joinpath("data", "databases", "cde_dwh.duckdb")
        return DuckDBManager(db_path=db_path)

    def setup_schemas(self):
        self.db_manager.create_schema("nibrs_raw")
        self.db_manager.create_schema("nibrs_metadata")

    def setup_nibrs_metadata_table(self) -> None:
        self.db_manager.query(
            """CREATE SEQUENCE IF NOT EXISTS seq_nibrs_master_ingestion_id START 1"""
        )
        _ = self.db_manager.query("""
            CREATE TABLE IF NOT EXISTS nibrs_metadata.nibrs_master_metadata (
                id BIGINT PRIMARY KEY DEFAULT nextval('seq_nibrs_master_ingestion_id'),
                year INT NOT NULL,
                segment_bh_count INT NOT NULL,
                segment_b1_count INT NOT NULL,
                segment_b2_count INT NOT NULL,
                segment_b3_count INT NOT NULL,
                segment_01_count INT NOT NULL,
                segment_02_count INT NOT NULL,
                segment_03_count INT NOT NULL,
                segment_04_count INT NOT NULL,
                segment_05_count INT NOT NULL,
                segment_06_count INT NOT NULL,
                segment_07_count INT NOT NULL,
                segment_w1_count INT NOT NULL,
                segment_w3_count INT NOT NULL,
                segment_w6_count INT NOT NULL,
                file_hash VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.logger.info("Successfully created metadata table.")

    def _format_create_table_stmt_from_parser(
        self, nibrs_parser, table_name: str, schema_name: str = "nibrs_raw"
    ) -> str:
        int_col_lines_str = "    nibrs_year INTEGER,"
        str_cols = list(nibrs_parser(line=" " * 300).record.keys())
        str_col_lines_str = ",\n".join([f"    {col} VARCHAR" for col in str_cols])
        create_stmt_lines = [
            f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (",
            int_col_lines_str,
            str_col_lines_str,
            ")",
        ]
        create_stmt = "\n".join(create_stmt_lines)
        return create_stmt

    def _create_table_from_parser(
        self, nibrs_parser, table_name: str, schema_name: str = "nibrs_raw", verbose: bool = True
    ) -> None:
        table_exists = table_name in self.db_manager.list_tables(schema_name)
        if table_exists:
            self.logger.info(f"Table {table_name} already exists.")
        else:
            create_stmt = self._format_create_table_stmt_from_parser(
                nibrs_parser, table_name, schema_name
            )
            self.logger.info(
                f"Creating table {schema_name}.{table_name} with this statement:\n\n{create_stmt}"
            )
            result = self.db_manager.query(create_stmt)
            self.logger.info(f"Successfully created table {schema_name}.{table_name}")
            self.logger.info(f"Result: {result}")

    def setup_segment_tables(self) -> None:
        self._create_table_from_parser(nibrs.SegmentBHParser, "batch_header")
        self._create_table_from_parser(nibrs.SegmentB1Parser, "batch_header_p1")
        self._create_table_from_parser(nibrs.SegmentB2Parser, "batch_header_p2")
        self._create_table_from_parser(nibrs.SegmentB3Parser, "batch_header_p3")
        self._create_table_from_parser(nibrs.Segment01Parser, "administrative")
        self._create_table_from_parser(nibrs.Segment02Parser, "offense")
        self._create_table_from_parser(nibrs.Segment03Parser, "property")
        self._create_table_from_parser(nibrs.Segment04Parser, "victim")
        self._create_table_from_parser(nibrs.Segment05Parser, "offender")
        self._create_table_from_parser(nibrs.Segment06Parser, "arrestee")
        self._create_table_from_parser(nibrs.Segment07Parser, "arrest")
        self._create_table_from_parser(nibrs.SegmentW1Parser, "window_ex_clear")
        self._create_table_from_parser(nibrs.SegmentW3Parser, "window_property")
        self._create_table_from_parser(nibrs.SegmentW6Parser, "window_arrestee")


class NIBRSMasterFileIngester:
    nibrs_segments = {
        "BH": "batch_header",
        "B1": "batch_header_p1",
        "B2": "batch_header_p2",
        "B3": "batch_header_p3",
        "01": "administrative",
        "02": "offense",
        "03": "property",
        "04": "victim",
        "05": "offender",
        "06": "arrestee",
        "07": "arrest",
        "W1": "window_ex_clear",
        "W3": "window_property",
        "W6": "window_arrestee",
    }
    nibrs_segment_parsers = {
        "BH": nibrs.SegmentBHParser,
        "B1": nibrs.SegmentB1Parser,
        "B2": nibrs.SegmentB2Parser,
        "B3": nibrs.SegmentB3Parser,
        "01": nibrs.Segment01Parser,
        "02": nibrs.Segment02Parser,
        "03": nibrs.Segment03Parser,
        "04": nibrs.Segment04Parser,
        "05": nibrs.Segment05Parser,
        "06": nibrs.Segment06Parser,
        "07": nibrs.Segment07Parser,
        "W1": nibrs.SegmentW1Parser,
        "W3": nibrs.SegmentW3Parser,
        "W6": nibrs.SegmentW6Parser,
    }

    def __init__(self, project_root: Path, nibrs_year: int):
        self.project_root = project_root
        self.nibrs_year = nibrs_year
        self.assert_nibrs_year_data_available()
        self.db_manager = self.get_db_manager()
        self.check_that_database_is_set_up()
        self.segment_counter = self.init_segment_counter()
        # self.run_ingestion()

    def run_ingestion(self):
        already_ingested = self.nibrs_year_already_ingested()
        if not already_ingested:
            self.ingest_all_lines()
            # ToDo: Implement _format_metadata_insert_stmt() and call it here

    @cached_property
    def file_name(self) -> str:
        return f"nibrs-{str(self.nibrs_year)}.zip"

    @cached_property
    def data_dir(self) -> Path:
        return self.project_root.joinpath("data", "master")

    @cached_property
    def file_path(self) -> Path:
        return self.data_dir.joinpath(self.file_name)

    def init_segment_counter(self) -> dict[str, str]:
        return {
            "BH": 0,
            "B1": 0,
            "B2": 0,
            "B3": 0,
            "01": 0,
            "02": 0,
            "03": 0,
            "04": 0,
            "05": 0,
            "06": 0,
            "07": 0,
            "W1": 0,
            "W3": 0,
            "W6": 0,
        }

    def get_db_manager(self) -> DuckDBManager:
        db_path = self.project_root.joinpath("data", "databases", "cde_dwh.duckdb")
        return DuckDBManager(db_path=db_path)

    def check_that_database_is_set_up(self) -> None:
        nibrs_metadata_tables = self.db_manager.list_tables("nibrs_metadata")
        nibrs_raw_tables = self.db_manager.list_tables("nibrs_raw")
        if "nibrs_metadata" not in self.db_manager.list_schemas():
            raise SchemaMissingError("Schema nibrs_metadata must be created.")
        if ("nibrs_master_metadata" not in nibrs_metadata_tables) or (
            any([ns not in nibrs_raw_tables for ns in self.nibrs_segments.values()])
        ):
            raise TableMissingError("Required tables must be created.")

    def assert_nibrs_year_data_available(self):
        if not self.file_path.is_file():
            raise FileNotFoundError(
                f"No file named {self.file_name} found in directory {self.data_dir}."
            )

    @cached_property
    def file_hash(self) -> str:
        sha256_hash = hashlib.sha256()
        with open(self.file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def get_archive_compression(self) -> str:
        with zipfile.ZipFile(self.file_path, "r") as zf:
            file_info = zf.infolist()
            file_info = sorted(file_info, key=lambda x: x.file_size, reverse=True)
            compression_type = zipfile.compressor_names.get(file_info[0].compress_type, "unknown")
            return compression_type

    def get_file_lines(self, encoding: str = "latin1") -> Generator[str, None, None]:
        compression_type = self.get_archive_compression()
        if compression_type == "deflate64":
            unzip_context_mgr = zipfile.ZipFile
        else:
            unzip_context_mgr = zipfile_deflate64.ZipFile
        with unzip_context_mgr(self.file_path, "r") as zf:
            file_info = zf.infolist()
            file_info = sorted(file_info, key=lambda x: x.file_size, reverse=True)
            if len(file_info) == 0:
                raise ValueError(f"ZIP file {self.file_path} is empty")
            file_name = file_info[0].filename
            with zf.open(file_name, "r") as f:
                text_file = io.TextIOWrapper(f, encoding=encoding)
                for line in text_file:
                    line = line.replace("\x00", " ")
                    yield line

    def get_segment_record_count(self, segment_name) -> int:
        record_counts = self.db_manager.query(f"""
            SELECT count(*) AS record_count
            FROM nibrs_raw.{segment_name}
            WHERE year = {self.nibrs_year}
            ORDER BY ingested_at DESC
            LIMIT 1
        """)["record_count"]
        if len(record_counts) > 0:
            return record_counts.values[0]
        else:
            return 0

    def nibrs_year_already_ingested(self) -> bool:
        prior_year_ingestions = self.db_manager.query(f"""
            SELECT *
            FROM nibrs_metadata.nibrs_master_metadata
            WHERE year = {self.nibrs_year}
        """)
        if len(prior_year_ingestions) == 0:
            return False
        latest_ingestion = (
            prior_year_ingestions.sort_values(by="ingested_at", ascending=False).iloc[0:1].copy()
        )
        expected_hash = latest_ingestion["file_hash"].values[0]
        hash_matches = expected_hash == self.file_hash
        if not hash_matches:
            raise SourceDataChangedError(
                f"Hash of the nibrs-{self.nibrs_year}.zip doesn't match the cached value.\n"
                f"Expected hash: {expected_hash}\n"
                f"Observed hash: {self.file_hash}\n\n. Please investigate."
            )
        for segment_code, segment_name in self.nibrs_segments:
            expected_count = latest_ingestion[segment_name].values[0]
            count_in_table = self.get_segment_record_count(segment_name)
            record_counts_match = expected_count == count_in_table
            if not record_counts_match:
                raise DatabaseRecordCountError(
                    f"The {segment_name} database table doesn't have the expected record count.\n"
                    f"Expected records: {expected_count}\n"
                    f"Observed records: {count_in_table}"
                )
        return True

    def ingest_all_lines(self) -> None:
        for line in self.get_file_lines():
            segment_code = line[0:2]
            segment_name = self.nibrs_segments.get(segment_code, None)
            parser_func = self.nibrs_segment_parsers.get(segment_code, None)
            if parser_func is None or segment_name is None:
                raise KeyError(f"Line has an invalid segment code: {segment_code}. Line: {line}")
            parser = parser_func(line)
            record = parser.record
            insert_stmt = self._format_record_insert_stmt(table_name=segment_name, record=record)
            _ = self.db_manager.query(insert_stmt)
            self.segment_counter[segment_code] += 1

    def _format_record_insert_stmt(self, table_name: str, record: dict[str, str]) -> str:
        column_names = list(record.keys())
        column_values = list(record.values())
        column_names_str = ", ".join(column_names)
        column_values_str = "'" + "', '".join(column_values) + "'"
        insert_stmt = (
            f"INSERT INTO nibrs_raw.{table_name}\n"
            f"    ({column_names_str})\n"
            "VALUES\n"
            f"    ({column_values_str});"
        )
        return insert_stmt


class NIBRSPipeline:
    def __init__(self, project_root: Path, update_oris: bool = False):
        self.project_root = project_root
        self.update_oris = update_oris
        self.cde_api = self.get_cde_api()
        self.db_manager = self.get_db_manager()

    def run_pipeline(self) -> None:
        self.db_manager.create_schema("nibrs_raw")
        self.extract_and_load_all_oris()

    def get_cde_api(self) -> CDEAPI:
        return CDEAPI(dotenv_path=self.project_root.joinpath(".env"))

    def get_db_manager(self) -> DuckDBManager:
        db_path = self.project_root.joinpath("data", "databases", "cde_dwh.duckdb")
        return DuckDBManager(db_path=db_path)

    def extract_and_load_all_oris(
        self, state_abbrs: tuple[str] = tuple(STATE_CODES.values())
    ) -> None:
        tables_in_schema = self.db_manager.list_tables("nibrs_raw")
        non_state_codes = set(("AS", "CZ", "GM", "PR", "VI"))
        check_states = tuple(set(state_abbrs).difference(non_state_codes))
        if "ori" in tables_in_schema:
            ingested_states = self.db_manager.query("""
                select distinct state_abbr
                from nibrs_raw.ori
            """)["state_abbr"].tolist()
            if not self.update_oris:
                check_states = [s for s in state_abbrs if s not in ingested_states]
        for state in check_states:
            state_oris = self.cde_api.get_state_oris(state=state)
            self.db_manager.ingest(
                df=state_oris,
                table_name="ori",
                schema_name="nibrs_raw",
                primary_keys=["ori"],
                if_exists="upsert",
            )


def main(project_root: Path, update_oris: bool) -> None:
    setup_logging()
    print(project_root)
    print(f"update_oris: {update_oris}")
    pipeline = NIBRSPipeline(project_root, update_oris)
    pipeline.run_pipeline()


if __name__ == "__main__":
    print(f"__file__: {__file__}")
    parser = argparse.ArgumentParser(
        description="Runs a pipeline to collect NIBRS data and load it into a duckdb database."
    )
    parser.add_argument(
        "--project_root_dir",
        type=str,
        default=str(Path(__file__).parent.parent),
        help="Path to the project's root dir",
    )
    parser.add_argument("--update_oris", action="store_true", help="Forces redownload of ORI data")
    args = parser.parse_args()
    main(project_root=Path(args.project_root_dir), update_oris=args.update_oris)
