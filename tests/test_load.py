import pytest
import pandas as pd
import duckdb
from pathlib import Path

from load import DuckDBManager, QueryError, InvalidNameError


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database file path."""
    return tmp_path / "test.db"


@pytest.fixture
def db_manager(db_path):
    """Create a DuckDBManager instance with temporary database."""
    return DuckDBManager(db_path)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "ori": ["WI0230600", "LA0250300", "OH0451700"],
            "counties": ["GREEN", "JACKSON", "FAIRFIELD, LICKING"],
            "is_nibrs": [True, False, True],
            "latitude": [42.677728, 32.30448, 40.093609],
            "longitude": [-89.605639, -92.556672, -82.481251],
            "state_abbr": ["WI", "LA", "OH"],
            "state_name": ["Wisconsin", "Louisiana", "Ohio"],
            "agency_name": [
                "New Glarus Police Department",
                "North Hodge Police Department",
                "Buckeye Lake Police Department",
            ],
            "agency_type_name": ["City", "City", "City"],
            "nibrs_start_date": ["2020-01-01", None, "2021-03-01"],
            "county_key": ["GREEN", "JACKSON", "FAIRFIELD, LICKING"],
        }
    )


def test_init(db_manager, db_path):
    """Test DuckDBManager initialization."""
    assert db_manager.db_path == db_path


def test_get_connection(db_manager):
    """Test connection context manager."""
    with db_manager._get_connection() as conn:
        assert isinstance(conn, duckdb.DuckDBPyConnection)
    # Connection should be closed after context
    with pytest.raises(Exception):
        conn.execute("SELECT 1")


def test_execute_query_success(db_manager):
    """Test successful query execution."""
    with db_manager._get_connection() as conn:
        df = db_manager._execute_query("SELECT 1 as num", conn)
        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["num"] == 1


def test_execute_query_failure(db_manager):
    """Test query execution failure."""
    with db_manager._get_connection() as conn:
        with pytest.raises(QueryError):
            db_manager._execute_query("SELECT * FROM nonexistent_table", conn)


def test_query_with_connection(db_manager):
    """Test query method with provided connection."""
    with db_manager._get_connection() as conn:
        df = db_manager.query("SELECT 1 as num", conn)
        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["num"] == 1


def test_query_without_connection(db_manager):
    """Test query method without provided connection."""
    df = db_manager.query("SELECT 1 as num")
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["num"] == 1


def test_list_schemas(db_manager):
    """Test listing schemas."""
    schemas = db_manager.list_schemas()
    assert isinstance(schemas, list)
    assert "main" in schemas  # DuckDB always has a main schema


def test_create_schema(db_manager):
    """Test schema creation."""
    test_schema = "test_schema"
    db_manager.create_schema(test_schema)
    schemas = db_manager.list_schemas()
    assert "test_schema" in schemas


def test_standardize_name():
    """Test name standardization."""
    db_manager = DuckDBManager(Path("test.db"))
    assert db_manager._standardize_name("Test Name") == "test_name"
    assert db_manager._standardize_name("test-name-123") == "test_name_123"

    with pytest.raises(InvalidNameError):
        db_manager._standardize_name("123test")  # Can't start with number
    with pytest.raises(InvalidNameError):
        db_manager._standardize_name("")  # Empty string


def test_ingest_basic(db_manager, sample_df):
    """Test basic data ingestion."""
    db_manager.ingest(df=sample_df, table_name="test_table", schema_name="test_schema")
    # Verify the data was ingested correctly
    result = db_manager.query("""SELECT * FROM test_schema.test_table""")

    assert len(result) == len(sample_df)
    assert all(result["counties"] == sample_df["counties"])
    assert all(result["is_nibrs"] == sample_df["is_nibrs"])
    assert all(result["latitude"] == sample_df["latitude"])
    assert all(result["longitude"] == sample_df["longitude"])
    assert all(result["state_abbr"] == sample_df["state_abbr"])
    assert all(result["agency_name"] == sample_df["agency_name"])


def test_ingest_if_exists_fail(db_manager, sample_df):
    """Test ingestion with if_exists='fail'."""
    # First ingestion should succeed
    db_manager.ingest(df=sample_df, table_name="test_table", schema_name="test_schema")
    # Second ingestion should fail
    with pytest.raises(ValueError):
        db_manager.ingest(
            df=sample_df, table_name="test_table", schema_name="test_schema", if_exists="fail"
        )


def test_ingest_if_exists_replace(db_manager, sample_df):
    """Test ingestion with if_exists='replace'."""
    # First ingestion
    db_manager.ingest(df=sample_df, table_name="test_table", schema_name="test_schema")
    # Create modified DataFrame
    modified_df = sample_df.copy()
    modified_df["counties"] = modified_df["counties"] * 2
    # Replace with modified data
    db_manager.ingest(
        df=modified_df, table_name="test_table", schema_name="test_schema", if_exists="replace"
    )
    # Verify the data was replaced
    result = db_manager.query("""SELECT * FROM test_schema.test_table""")

    assert len(result) == len(modified_df)
    assert all(result["counties"] == modified_df["counties"])


def test_ingest_if_exists_append(db_manager, sample_df):
    """Test ingestion with if_exists='append'."""
    # First ingestion
    db_manager.ingest(df=sample_df, table_name="test_table", schema_name="test_schema")
    # Append same data
    db_manager.ingest(
        df=sample_df, table_name="test_table", schema_name="test_schema", if_exists="append"
    )
    # Verify the data was appended
    result = db_manager.query("""
        SELECT * FROM test_schema.test_table
        ORDER BY ori
    """)
    assert len(result) == len(sample_df) * 2


def test_ingest_if_exists_upsert(db_manager, sample_df):
    """Test ingestion with if_exists='upsert'."""
    # First ingestion
    db_manager.ingest(
        df=sample_df, table_name="test_table", schema_name="test_schema", primary_keys=["ori"]
    )
    # Modify one record
    modified_df = pd.DataFrame(
        {
            "ori": ["WI0230600"],
            "counties": ["BEER"],
            "is_nibrs": [True],
            "latitude": [42.677728],
            "longitude": [-89.605639],
            "state_abbr": ["WI"],
            "state_name": ["Wisconsin"],
            "agency_name": ["New Glarus Spotted Cow"],
            "agency_type_name": ["City"],
            "nibrs_start_date": ["2020-01-01"],
            "county_key": ["BEER"],
        }
    )
    # Upsert modified record
    db_manager.ingest(
        df=modified_df,
        table_name="test_table",
        schema_name="test_schema",
        primary_keys=["ori"],
        if_exists="upsert",
    )
    # Verify the data was upserted
    result = db_manager.query("""
        SELECT * FROM test_schema.test_table
        WHERE ori = 'WI0230600'
    """)
    assert len(result) == 1
    assert result.iloc[0]["counties"] == "BEER"
    assert result.iloc[0]["agency_name"] == "New Glarus Spotted Cow"


def test_ingest_if_exists_append_new(db_manager, sample_df):
    """Test ingestion with if_exists='append-new'."""
    # First ingestion
    db_manager.ingest(
        df=sample_df, table_name="test_table", schema_name="test_schema", primary_keys=["ori"]
    )
    # Try to append same data
    db_manager.ingest(
        df=sample_df,
        table_name="test_table",
        schema_name="test_schema",
        primary_keys=["ori"],
        if_exists="append-new",
    )
    # Verify no duplicates were added
    result = db_manager.query("""
        SELECT * FROM test_schema.test_table
        ORDER BY ori
    """)
    assert len(result) == len(sample_df)


def test_vacuum(db_manager):
    """Test vacuum operation."""
    # This is mostly a smoke test since vacuum's effects are internal
    db_manager.vacuum()
    # If no exception is raised, consider it successful


def test_ingest_missing_primary_keys(db_manager, sample_df):
    """Test ingestion without primary keys when required."""
    with pytest.raises(ValueError):
        db_manager.ingest(
            df=sample_df,
            table_name="test_table",
            schema_name="test_schema",
            if_exists="upsert",  # Requires primary_keys
        )
