import argparse
from functools import cached_property
import hashlib
from pathlib import Path

from extract import CDEAPI
from load import DuckDBManager

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

    # def setup_nibrs_metadata_table(self):


class NIBRSMasterFileIngester:
    def __init__(self, project_root: Path, nibrs_year: int):
        self.project_root = project_root
        self.nibrs_year = nibrs_year
        self.assert_nibrs_year_data_available()
        self.db_manager = self.get_db_manager()

    @cached_property
    def file_name(self) -> str:
        return f"nibrs-{str(self.nibrs_year)}.zip"

    @cached_property
    def data_dir(self) -> Path:
        return self.project_root.joinpath("data", "master")

    @cached_property
    def file_path(self) -> Path:
        return self.data_dir.joinpath(self.file_name)

    def get_db_manager(self) -> DuckDBManager:
        db_path = self.project_root.joinpath("data", "databases", "cde_dwh.duckdb")
        return DuckDBManager(db_path=db_path)

    def assert_nibrs_year_data_available(self):
        if not self.file_path.is_file():
            raise FileNotFoundError(
                f"No file named {self.file_name} found in directory {self.data_dir}."
            )

    @cached_property
    def calculate_file_hash(self) -> str:
        sha256_hash = hashlib.sha256()
        with open(self.file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

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
