import pytest

from app_utils import display_query, enforce_query_limit, has_unescaped_newline, match_sql_case


def test_display_query_str_input(mocker):
    # This doesn't really test the behavior; it just tests that stremlit's .code() method is called
    mock_code = mocker.patch("streamlit.code")

    display_query("SELECT * FROM table")
    mock_code.assert_called_once_with("SELECT * FROM table", language="sql")


def test_display_query_list_input(mocker):
    # This doesn't really test the behavior; it just tests that stremlit's .code() method is called
    mock_code = mocker.patch("streamlit.code")

    display_query(["SELECT *", "FROM table"])
    mock_code.assert_called_once_with("SELECT *\nFROM table", language="sql")


def test_enforce_query_limit_adds_limit():
    original_query = "select * from a_schema.a_table"
    limited_query = enforce_query_limit(original_query, 5000)
    assert limited_query == f"""{original_query} limit 5000"""


def test_enforce_query_limit_lowers_existing_limit_above_max():
    original_query = "select * from a_schema.a_table limit 10000"
    limited_query = enforce_query_limit(original_query, 5000)
    assert limited_query == "select * from a_schema.a_table limit 5000"


def test_enforce_query_limit_leaves_existing_limit_below_max():
    original_query = "select * from a_schema.a_table limit 23"
    expected_query = "select * from a_schema.a_table limit 23"
    limited_query = enforce_query_limit(original_query, 5000)
    assert limited_query == expected_query

    original_query = "SELECT * FROM a_schema.a_table limit 231"
    expected_query = "SELECT * FROM a_schema.a_table LIMIT 231"
    limited_query = enforce_query_limit(original_query, 5000)
    assert limited_query == expected_query


def test_enforce_query_limit_matches_modal_case_when_limit_above_max():
    original_query = "select * FROM a_schema.a_table LIMIT 10000"
    expected_query = "select * FROM a_schema.a_table LIMIT 8080"
    limited_query = enforce_query_limit(original_query, 8080)
    assert limited_query == expected_query

    original_query = "SELECT *\nfrom a_schema.a_table\nlimit 987654"
    expected_query = "SELECT *\nfrom a_schema.a_table\nlimit 1005"
    limited_query = enforce_query_limit(original_query, 1005)
    assert limited_query == expected_query

    original_query = "select *\nFROM a_schema.a_table\nLIMIT 987654"
    expected_query = "select *\nFROM a_schema.a_table\nLIMIT 3030"
    limited_query = enforce_query_limit(original_query, 3030)
    assert limited_query == expected_query


def test_enforce_query_limit_matches_modal_case_when_limit_below_max():
    original_query = "select * FROM a_schema.a_table limit 456"
    expected_query = "select * FROM a_schema.a_table limit 456"
    limited_query = enforce_query_limit(original_query, 2510)
    assert limited_query == expected_query

    original_query = "SELECT *\nfrom a_schema.a_table\nlimit 1010"
    expected_query = "SELECT *\nfrom a_schema.a_table\nlimit 1010"
    limited_query = enforce_query_limit(original_query, 3510)
    assert limited_query == expected_query

    original_query = "SELECT *\nFROM a_schema.a_table\nlimit 2010"
    expected_query = "SELECT *\nFROM a_schema.a_table\nLIMIT 2010"
    limited_query = enforce_query_limit(original_query, 4510)
    assert limited_query == expected_query


def test_enforce_query_limit_matches_modal_case_when_adding_limit():
    original_query = "select * FROM a_schema.a_table"
    expected_query = "select * FROM a_schema.a_table limit 2520"
    limited_query = enforce_query_limit(original_query, 2520)
    assert limited_query == expected_query

    original_query = "SELECT *\nfrom a_schema.a_table"
    expected_query = "SELECT *\nfrom a_schema.a_table\nlimit 3520"
    limited_query = enforce_query_limit(original_query, 3520)
    assert limited_query == expected_query

    original_query = "SELECT *\nFROM a_schema.a_table"
    expected_query = "SELECT *\nFROM a_schema.a_table\nLIMIT 7520"
    limited_query = enforce_query_limit(original_query, 7520)
    assert limited_query == expected_query


def test_enforce_query_limit_multiline_query():
    original_query = "select *\nFROM a_schema.a_table"
    expected_query = "select *\nFROM a_schema.a_table\nlimit 2330"
    limited_query = enforce_query_limit(original_query, 2330)
    assert limited_query == expected_query

    original_query = "SELECT *\nfrom a_schema.a_table\nLIMIT 1330"
    expected_query = "SELECT *\nfrom a_schema.a_table\nLIMIT 1330"
    limited_query = enforce_query_limit(original_query, 2330)
    assert limited_query == expected_query

    original_query = "SELECT *\nFROM a_schema.a_table\nlimit 4330"
    expected_query = "SELECT *\nFROM a_schema.a_table\nLIMIT 2330"
    limited_query = enforce_query_limit(original_query, 2330)
    assert limited_query == expected_query


@pytest.mark.parametrize(
    "query, keyword, expected",
    [
        ("SELECT * FROM a_table", "limit", "LIMIT"),
        ("select * from a_table", "limit", "limit"),
        ("Select * From a_table", "limit", "Limit"),
        ("SELECT * from a_table where a_col = 1", "limit", "limit"),
    ],
)
def test_match_sql_case(query, keyword, expected):
    assert match_sql_case(query, keyword) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT * FROM a_table", False),
        ("SELECT *\nFROM a_table", True),
        ("SELECT * FROM a_table WHERE name = 'John\nDoe'", False),  # Newline inside quotes
    ],
)
def test_has_unescaped_newline(query, expected):
    assert has_unescaped_newline(query) == expected


@pytest.mark.parametrize(
    "query, max_rows, expected",
    [
        ("SELECT * FROM a_table", 1000, "SELECT * FROM a_table LIMIT 1000"),
        ("SELECT * FROM a_table LIMIT 5000", 1000, "SELECT * FROM a_table LIMIT 1000"),
        ("SELECT *\nFROM a_table", 500, "SELECT *\nFROM a_table\nLIMIT 500"),
    ],
)
def test_enforce_query_limit(query, max_rows, expected):
    assert enforce_query_limit(query, max_rows) == expected
