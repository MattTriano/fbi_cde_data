import argparse
from functools import cached_property
import hashlib
import io
from pathlib import Path
from typing import Generator
import zipfile

import zipfile_deflate64

from extract import CDEAPI
from load import DuckDBManager
from parsers import nibrs


state_abbrs = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
)


class NIBRSMasterFilePipeline:
    def __init__(self, project_root: Path, nibrs_year: int):
        self.project_root = project_root
        self.db_manager = self.get_db_manager()

    def setup(self):
        self.setup_schemas()

    def setup_schemas(self):
        self.db_manager.create_schema("nibrs_raw")
        self.db_manager.create_schema("nibrs_metadata")

    def setup_nibrs_metadata_table(self) -> None:
        self.db_manager.query(
            """CREATE SEQUENCE IF NOT EXISTS seq_nibrs_master_ingestion_id START 1"""
        )
        self.db_manager.query("""
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
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _create_segment_01_table(self) -> None:
        int_col_lines = "nibrs_year::INTEGER"
        str_cols = list(nibrs.Segment01Parser(line=" " * 300).record.keys())
        str_col_lines = "::VARCHAR,\n\t\t\t\t".join(str_cols)
        create_query = f"""
            CREATE TABLE IF NOT EXISTS nibrs_raw.administrative AS (
                {int_col_lines},
                {str_col_lines}
            )
        """
        self.db_manager.query(create_query)

    # def setup_segment_tables(self) -> None:


class NIBRSMasterFileIngester:
    def __init__(self, project_root: Path, nibrs_year: int):
        self.project_root = project_root
        self.nibrs_year = nibrs_year
        self.assert_nibrs_year_data_available()
        self.db_manager = self.get_db_manager()
        self.segment_counter = self.init_segment_counter()

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

    # def nibrs_year_already_ingested(self):


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

    def extract_and_load_all_oris(self, state_abbrs: tuple[str] = state_abbrs) -> None:
        tables_in_schema = self.db_manager.list_tables("nibrs_raw")
        check_states = state_abbrs
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
