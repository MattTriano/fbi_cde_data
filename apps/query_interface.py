from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT.joinpath("data", "databases", "cde_dwh.duckdb")
sys.path.append(str(PROJECT_ROOT.joinpath("src")))

from load import DuckDBManager  # noqa: E402
from app_utils import display_query, enforce_query_limit  # noqa: E402

####################################################
################# Helper Functions #################
####################################################

MAX_ROWS = 5000
SAMPLE_QUERIES = {
    "tables in nibrs_raw schema": [
        "select table_name",
        "from information_schema.tables",
        "where table_schema = 'nibrs_raw'",
    ],
    "schemas in database": ["select distinct schema_name", "from information_schema.schemata"],
    "Column info for nibrs_raw.ori table": [
        "select column_name, data_type",
        "from information_schema.columns",
        "where",
        "    table_schema = 'nibrs_raw'",
        "    and table_name = 'ori'",
    ],
}


@st.cache_resource
def get_db_manager():
    return DuckDBManager(DB_PATH, read_only=True)


def run_sample_query(query: list[str]) -> pd.DataFrame:
    if not isinstance(query, list):
        raise TypeError(f"Sample queries must be provided as a list of strs. Received {query}")
    results = db_manager.query("\n".join(query))
    return results


def run_main_query(query: str) -> pd.DataFrame:
    if not isinstance(query, str):
        raise TypeError(f"The main query must be provided as a str. Received {query}")
    len_check_query = f"""select count(*) from ({query})"""
    len_check_df = db_manager.query(len_check_query)
    total_rows = len_check_df["count_star()"][0]
    if total_rows > MAX_ROWS:
        st.write(
            f"Query would return {total_rows} rows, which exceeds this app's {MAX_ROWS} row limit"
        )
        query = enforce_query_limit(query, max_rows=MAX_ROWS)
    st.write("Executed query:")
    display_query(query)
    with st.spinner("Executing query..."):
        result_df = db_manager.query(query)
    if result_df is not None:
        st.write(f"Returned {len(result_df)} rows")
        st.dataframe(result_df, use_container_width=True, hide_index=True)
        csv = result_df.to_csv(index=False)
        st.download_button(
            label="Download results as CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
        )


####################################################
#################### App Layout ####################
####################################################

st.set_page_config(page_title="FBI CDE Database Query Interface", layout="wide")

st.title("FBI CDE Database Query Interface")
st.markdown("""
This app allows you to execute SQL queries against a DuckDB database.
Write your query in the text area below and click 'Execute Query' to see the results.
""")

db_manager = get_db_manager()

main_query = st.text_area(
    "Enter your SQL query:", height=150, placeholder="select *\nfrom nibrs_raw.ori\nlimit 5"
)


if st.button("Execute Query"):
    if main_query:
        run_main_query(main_query)
    else:
        st.warning("Please enter a query first.")


with st.sidebar:
    st.header("Database Information")
    sample_query = st.selectbox("Sample queries", SAMPLE_QUERIES)
    sample_query_lines = SAMPLE_QUERIES[sample_query]
    display_query(sample_query_lines)
    result = run_sample_query(sample_query_lines)
    st.dataframe(result)

    st.markdown("""
    ### Tips
    - Use SELECT queries to retrieve data
    - Use WHERE clauses to filter to subsets of interest
    - The interface is read-only for safety
    - Results can be downloaded as CSV
    """)
