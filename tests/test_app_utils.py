from app_utils import enforce_query_limit


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
