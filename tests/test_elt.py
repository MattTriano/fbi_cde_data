import logging
from pathlib import Path
import tempfile
import zipfile

import pytest

from elt import NIBRSMasterFilePipeline, NIBRSMasterFileIngester


@pytest.fixture(autouse=True)
def configure_logging():
    """
    Configure logging to suppress INFO level logs during testing.
    This fixture runs automatically for all tests.
    """
    original_levels = {}
    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        original_levels[logger_name] = logger.level
        logger.setLevel(logging.WARNING)

    root_level = logging.root.level
    logging.root.setLevel(logging.WARNING)
    yield
    for logger_name, level in original_levels.items():
        logging.getLogger(logger_name).setLevel(level)
    logging.root.setLevel(root_level)


@pytest.fixture(scope="module")
def temp_project_root():
    """
    Create a temporary directory structure that mimics the project layout
    with data/databases and data/master subdirectories.
    """
    with tempfile.TemporaryDirectory() as tmp_root:
        project_root = Path(tmp_root)
        data_dir = project_root.joinpath("data")
        databases_dir = data_dir.joinpath("databases")
        master_dir = data_dir.joinpath("master")
        databases_dir.mkdir(parents=True)
        master_dir.mkdir(parents=True)
        yield project_root


@pytest.fixture(scope="module")
def test_zip_archive(temp_project_root):
    """Create a test zip archive with sample data in the temporary directory."""

    file_path = temp_project_root.joinpath("data", "master", "nibrs-2021.zip")
    test_data = (
        "BH50AK0010100000000000000                ANCHORAGE                     AK1C941Y         3030020A         00028623800  39000000000000000000      000000000000000000      000000000000000000      000000000000000000      000000000  002021NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN020            \n"
        "BH50AK0010200000000000000        20210101FAIRBANKS                     AK4 941Y         3030020AA        00003059800 258000000000000000000      000000000000000000      000000000000000000      000000000000000000      00000000012122021NYNNYNNYNNYNNYNNYNNYNNYNNYNNYNNYNNYN090            \n"
        "0150AK001020021000004    20210101 03010010100    N                                      N\n"
        "0150AK001020021000009    20210101 03010030100    N                                      N\n"
        "0101AL001000000220526012720211206 12010010100    N                                      N\n"
        "0110GA022020020210400086320210416 13010020100    N                                      N\n"
        "0250AK001020021000004    20210101290CD  14               88\n"
        "0201AL00102002E7F551B0EB02021042623HCN  20               88\n"
        "0204CA0334200MR21-3723   2021071523FCN  13               88\n"
        "0350AK001020021000004    20210101408000000010                                                         \n"
        "0301AL00102007F90C983385D20210709777000000001                                                         \n"
        "0304CA037000021126124    20210618720000000055                                                         \n"
        "0450AK001020021000004    20210101002290                           B                                                                    \n"
        "0401AL0010200451B32E2525C2021051400426F                           I56FWNR                                                              \n"
        "0404CA037000021109994    2021030800123F                           I39FWHR                                                              \n"
        "0550AK001020021000004    202101010164MBN\n"
        "0501AL0010200474AA46F2176202107080116MBN\n"
        "0504CA037000021127384    202106250129MBN\n"
        "0650AK001020021000019    202101020146085       20210102ON13B17    41MWNR \n"
        "0601AL028120075AB42761DE8202101060178EAA9D876E320210106TN35B01    38FWNR \n"
        "0604CA0270000FG2102356   2021042901FG2102356-0120210429ON23G01    34MW   \n"
        "0750AK001020046075       2021010101    O90J01    33MINR \n"
        "0701AL02900002E01CA187E6C2021102001    T90Z01    39FWNN \n"
        "0704CA0360400AR21090586  2021092601    T90Z01    50MWNN \n"
        "W102AZ0111200S20001291   20200609 00000000000    B20210204                              N\n"
        "W107DE00203003219011670  20190205R00000000000    B20210106                              N\n"
        "W302AZ00725002102301             50900000000020210224                                                                               \n"
        "W304CA037000020139593            50300000400020210119  01                                                                           \n"
        "W307DE303SP000519077275          51300000026020210206                                                 23F                           \n"
        "W602AZ0070300201225039           0147541       20210115ON13B      26FWHR N                              \n"
        "W607DE00202003419007198          0121001006AWS 20210125TN23D      32MWNN Y                              \n"
    )
    with zipfile.ZipFile(file_path, "w") as zip_file:
        info = zipfile.ZipInfo("nibrs_data.txt")
        info.date_time = (2021, 1, 1, 0, 0, 0)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o644 << 16
        zip_file.writestr(info, test_data)
    yield file_path


@pytest.fixture(scope="module")
def setup_database(temp_project_root):
    """
    Setup the database schema once at the beginning of the test session.
    This ensures schema creation is done only once and persists across tests.
    """
    # from elt import NIBRSMasterFilePipeline
    pipeline = NIBRSMasterFilePipeline(temp_project_root)
    return pipeline


# def test_schema_setup(temp_project_root):
#     pipeline = NIBRSMasterFilePipeline(temp_project_root)
#     schemas = pipeline.db_manager.list_schemas()
#     assert "nibrs_raw" in schemas
#     assert "nibrs_metadata" in schemas


@pytest.fixture(scope="class")
def class_setup_database(temp_project_root, setup_database):
    """
    This fixture ensures the database is set up at the class level,
    making it available to all test methods in a class.
    """
    return setup_database


@pytest.mark.usefixtures("class_setup_database")
class TestNIBRSPipeline:
    def test_schema_setup(self, temp_project_root):
        pipeline = NIBRSMasterFilePipeline(temp_project_root)
        schemas = pipeline.db_manager.list_schemas()
        assert "nibrs_raw" in schemas
        assert "nibrs_metadata" in schemas

    def test_tables_created(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        metadata_tables = ingester.db_manager.list_tables("nibrs_metadata")
        raw_tables = ingester.db_manager.list_tables("nibrs_raw")
        assert all(
            [
                t in raw_tables
                for t in [
                    "batch_header",
                    "batch_header_p1",
                    "batch_header_p2",
                    "batch_header_p3",
                    "administrative",
                    "offense",
                    "property",
                    "victim",
                    "offender",
                    "arrestee",
                    "arrest",
                    "window_ex_clear",
                    "window_property",
                    "window_arrestee",
                ]
            ]
        )
        assert "nibrs_master_metadata" in metadata_tables

    def test_year_not_ingested_pre_ingestion(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        assert not ingester.nibrs_year_already_ingested()

    def test_segment_counts_after_ingestion(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        assert sum(ingester.segment_counter.values()) == 0
        metadata_df = ingester.db_manager.query(
            "select * from nibrs_metadata.nibrs_master_metadata"
        )
        assert metadata_df.empty
        ingester.run_ingestion()
        segment_counts = ingester.segment_counter
        assert segment_counts["BH"] == 2
        assert segment_counts["01"] == 4
        assert segment_counts["02"] == 3
        assert segment_counts["03"] == 3
        assert segment_counts["04"] == 3
        assert segment_counts["05"] == 3
        assert segment_counts["06"] == 3
        assert segment_counts["07"] == 3
        assert segment_counts["W1"] == 2
        assert segment_counts["W3"] == 3
        assert segment_counts["W6"] == 2

    def test_metadata_record_created_after_ingestion(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        metadata_df = ingester.db_manager.query(
            "select * from nibrs_metadata.nibrs_master_metadata"
        )
        assert not metadata_df.empty
        assert ingester.file_hash in metadata_df["file_hash"].unique()

    def test_file_hash_of_zip_archive_matches_file(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        metadata_df = ingester.db_manager.query(
            "select * from nibrs_metadata.nibrs_master_metadata"
        )
        assert not metadata_df.empty
        assert ingester.file_hash in metadata_df["file_hash"].unique()

    def test_bh_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.batch_header")
        assert data_df["ori"].to_list() == ["AK0010100", "AK0010200"]
        assert data_df["city"].to_list() == [
            "ANCHORAGE                     ",
            "FAIRBANKS                     ",
        ]
        assert data_df["pop_group"].to_list() == ["1C", "4 "]
        assert data_df["nibrs_flag"].to_list() == [" ", "A"]
        assert data_df["fbi_field_office"].to_list() == ["3030", "3030"]

    def test_01_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.administrative")
        assert data_df["ori"].to_list() == ["AK0010200", "AK0010200", "AL0010000", "GA0220200"]
        assert data_df["incident_date"].to_list() == [
            "20210101",
            "20210101",
            "20211206",
            "20210416",
        ]
        assert data_df["incident_hour"].to_list() == ["03", "03", "12", "13"]
        assert data_df["total_victims"].to_list() == ["001", "003", "001", "002"]

    def test_02_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.offense")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0010200", "CA0334200"]
        assert data_df["incident_date"].to_list() == ["20210101", "20210426", "20210715"]
        assert data_df["ucr_offense_code"].to_list() == ["290", "23H", "23F"]
        assert data_df["location_type"].to_list() == ["14", "20", "13"]

    def test_03_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.property")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0010200", "CA0370000"]
        assert data_df["incident_no"].to_list() == ["21000004    ", "7F90C983385D", "21126124    "]
        assert data_df["incident_date"].to_list() == ["20210101", "20210709", "20210618"]
        assert data_df["property_descr"].to_list() == ["08", "77", "20"]

    def test_04_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.victim")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0010200", "CA0370000"]
        assert data_df["incident_no"].to_list() == ["21000004    ", "451B32E2525C", "21109994    "]
        assert data_df["incident_date"].to_list() == ["20210101", "20210514", "20210308"]
        assert data_df["victim_of_ucr_offense1"].to_list() == ["290", "26F", "23F"]
        assert data_df["victim_age"].to_list() == ["  ", "56", "39"]

    def test_05_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.offender")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0010200", "CA0370000"]
        assert data_df["incident_no"].to_list() == ["21000004    ", "474AA46F2176", "21127384    "]
        assert data_df["incident_date"].to_list() == ["20210101", "20210708", "20210625"]
        assert data_df["offender_age"].to_list() == ["64", "16", "29"]

    def test_06_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.arrestee")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0281200", "CA0270000"]
        assert data_df["incident_no"].to_list() == ["21000019    ", "75AB42761DE8", "FG2102356   "]
        assert data_df["incident_date"].to_list() == ["20210102", "20210106", "20210429"]
        assert data_df["arrest_transaction_no"].to_list() == [
            "46085       ",
            "78EAA9D876E3",
            "FG2102356-01",
        ]
        assert data_df["ucr_arrest_offense_code"].to_list() == ["13B", "35B", "23G"]

    def test_07_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.arrest")
        assert data_df["ori"].to_list() == ["AK0010200", "AL0290000", "CA0360400"]
        assert data_df["incident_no"].to_list() == ["46075       ", "2E01CA187E6C", "AR21090586  "]
        assert data_df["arrest_date"].to_list() == ["20210101", "20211020", "20210926"]
        assert data_df["arrestee_age"].to_list() == ["33", "39", "50"]
        assert data_df["arrestee_residency"].to_list() == ["R", "N", "N"]

    def test_W1_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.window_ex_clear")
        assert data_df["ori"].to_list() == ["AZ0111200", "DE0020300"]
        assert data_df["incident_no"].to_list() == ["S20001291   ", "3219011670  "]
        assert data_df["incident_date"].to_list() == ["20200609", "20190205"]
        assert data_df["ex_cleared_date"].to_list() == ["20210204", "20210106"]

    def test_W3_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.window_property")
        assert data_df["ori"].to_list() == ["AZ0072500", "CA0370000", "DE303SP00"]
        assert data_df["incident_no"].to_list() == ["2102301     ", "20139593    ", "0519077275  "]
        assert data_df["date_recovered"].to_list() == ["20210224", "20210119", "20210206"]
        assert data_df["motor_vehicles_recovered"].to_list() == ["  ", "01", "  "]

    def test_W6_data_ingested_correctly(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        data_df = ingester.db_manager.query("select * from nibrs_raw.window_arrestee")
        assert data_df["ori"].to_list() == ["AZ0070300", "DE0020200"]
        assert data_df["incident_no"].to_list() == ["201225039   ", "3419007198  "]
        assert data_df["arrest_transaction_no"].to_list() == ["47541       ", "21001006AWS "]
        assert data_df["arrest_date"].to_list() == ["20210115", "20210125"]
        assert data_df["ucr_arrest_offense_code"].to_list() == ["13B", "23D"]

    def test_check_segment_record_counts(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        assert ingester.get_segment_record_count("batch_header") == 2
        assert ingester.get_segment_record_count("administrative") == 4
        assert ingester.get_segment_record_count("offense") == 3
        assert ingester.get_segment_record_count("property") == 3
        assert ingester.get_segment_record_count("victim") == 3
        assert ingester.get_segment_record_count("offender") == 3
        assert ingester.get_segment_record_count("arrestee") == 3
        assert ingester.get_segment_record_count("arrest") == 3
        assert ingester.get_segment_record_count("window_ex_clear") == 2
        assert ingester.get_segment_record_count("window_property") == 3
        assert ingester.get_segment_record_count("window_arrestee") == 2

    def test_year_ingested_post_ingestion(self, temp_project_root, test_zip_archive):
        ingester = NIBRSMasterFileIngester(project_root=temp_project_root, nibrs_year=2021)
        assert ingester.nibrs_year_already_ingested()
