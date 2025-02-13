from pathlib import Path
import sys

import streamlit as st

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT.joinpath("data", "databases", "cde_dwh.duckdb")
sys.path.append(str(PROJECT_ROOT.joinpath("src")))

from load import DuckDBManager  # noqa: E402


@st.cache_resource
def get_db_manager():
    return DuckDBManager(DB_PATH, read_only=True)


def run_query(conn, query):
    try:
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None


st.set_page_config(page_title="DuckDB Query Interface", layout="wide")

st.title("DuckDB Query Interface")
st.markdown("""
This app allows you to execute SQL queries against a DuckDB database.
Write your query in the text area below and click 'Execute Query' to see the results.
""")

db_manager = get_db_manager()

query = st.text_area(
    "Enter your SQL query:", height=150, placeholder="SELECT * FROM nibrs_raw.ori LIMIT 5;"
)

if st.button("Execute Query"):
    if query:
        with st.spinner("Executing query..."):
            results = db_manager.query(query)
        if results is not None:
            st.write(f"Returned {len(results)} rows")
            st.dataframe(results, use_container_width=True, hide_index=True)
            csv = results.to_csv(index=False)
            st.download_button(
                label="Download results as CSV",
                data=csv,
                file_name="query_results.csv",
                mime="text/csv",
            )
    else:
        st.warning("Please enter a query first.")

with st.sidebar:
    st.header("Database Information")
    st.subheader("Available Tables")
    tables_query = "SELECT * FROM information_schema.tables WHERE table_schema = 'main'"
    tables = db_manager.query(tables_query)
    if tables is not None:
        for _, row in tables.iterrows():
            st.write(f"- {row['table_name']}")

    st.markdown("""
    ### Tips
    - Use SELECT queries to retrieve data
    - The interface is read-only for safety
    - Results can be downloaded as CSV
    - Use LIMIT to restrict large result sets
    """)

st.markdown("---")
st.markdown("Built with Streamlit and DuckDB")
