import pytest
import pandas as pd
from unittest.mock import MagicMock
from apps.query_interface import run_sample_query, run_main_query


@pytest.fixture
def mock_db_manager(mocker):
    """Mock DuckDBManager to prevent real database queries."""
    mock_instance = MagicMock()
    mock_instance.query.return_value = pd.DataFrame({"column1": [1, 2], "column2": ["a", "b"]})

    _ = mocker.patch("apps.query_interface.get_db_manager", return_value=mock_instance)
    mocker.patch("apps.query_interface.db_manager", new=mock_instance)
    return mock_instance


def test_run_sample_query(mock_db_manager):
    """Tests sample query execution using a mocked DuckDBManager."""
    query = ["SELECT * FROM test"]
    result = run_sample_query(query)
    mock_db_manager.query.assert_called_once_with("SELECT * FROM test")

    assert isinstance(result, pd.DataFrame)
    assert "column1" in result.columns
    assert "column2" in result.columns


def test_run_main_query(mocker, mock_db_manager):
    """Kinda sorta tests main query execution using a mocked DuckDBManager.
    These tests kind of stink and the mocking makes this tightly coupled and very brittle, but
    at least this touches most of the run_main_query func.
    """
    mock_st = mocker.patch("apps.query_interface.st")
    mock_db_manager.query.side_effect = lambda query: (
        pd.DataFrame({"count_star()": [10]})
        if "count(*)" in query.lower()
        else pd.DataFrame({"column1": [1, 2], "column2": ["a", "b"]})
    )

    query = "SELECT * FROM test"
    run_main_query(query)
    expected_len_check_query = "select count(*) from (SELECT * FROM test)"
    mock_db_manager.query.assert_any_call(expected_len_check_query)
    mock_db_manager.query.assert_any_call(query)
    mock_st.write.assert_any_call("Executed query:")
    mock_st.dataframe.assert_called()
