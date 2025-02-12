from contextlib import contextmanager
from pathlib import Path
import re
from typing import Optional, Union

import duckdb
import pandas as pd


class DBWrapper:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def _get_connection(self):
        """Context manager for handling database connections."""
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            yield conn
        finally:
            if conn:
                conn.close()

    def _execute_query(self, query: str, conn: duckdb.duckdb.DuckDBPyConnection) -> pd.DataFrame:
        try:
            result = conn.execute(query)
            return result.df()
        except Exception as e:
            raise QueryError(f"Query execution failed: {str(e)}")

    def query(
        self, query: str, conn: Optional[duckdb.duckdb.DuckDBPyConnection] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as a pandas DataFrame."""
        if conn:
            return self._execute_query(query, conn)
        else:
            with self._get_connection() as conn:
                return self._execute_query(query, conn)

    @property
    def schemata(self) -> pd.DataFrame:
        return self.query("""select * from information_schema.schemata;""")

    def list_schemas(self) -> list[str]:
        return self.schemata["schema_name"].unique().tolist()

    def _tables(self, schema_name: str) -> pd.DataFrame:
        return self.query(f"""
            select *
            from information_schema.tables
            where table_schema = '{schema_name}'
        """)

    def list_tables(self, schema_name: str) -> list[str]:
        return self._tables(schema_name)["table_name"].unique().tolist()

    def _standardize_name(self, name: str) -> str:
        std_name = re.sub(r"[^a-z0-9_]", "", "_".join(name.lower().split()))
        self._validate_name(std_name)
        return std_name

    def _validate_name(self, name: str) -> None:
        if re.fullmatch(r"[a-z_][a-z0-9_]*", name) is None:
            raise InvalidNameError(f"The provided label, '{name}', is not valid.")

    def create_schema(self, schema_name: str) -> None:
        standardized_name = self._standardize_name(schema_name)
        self.query(f"""create schema if not exists {standardized_name};""")

    def ingest(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema_name: str,
        primary_keys: Optional[Union[str, list[str]]] = None,
        if_exists: str = "append",
    ) -> None:
        """Ingest a pandas DataFrame into the database.

        Args:
            df (pd.DataFrame): DataFrame to ingest
            table_name (str):  Name of the target table
            schema_name (str): Name of the target schema
            if_exists (str):   How to behave if table exists
               options: ['fail', 'replace', 'append', 'append-new', 'upsert']
               If 'upsert' or 'append-new', `primary_keys` must be provided.
        """
        if primary_keys is not None:
            if isinstance(primary_keys, str):
                primary_keys = list(primary_keys)
            elif not isinstance(primary_keys, list):
                raise ValueError("Primary key column name(s) must be provided as a list or a str.")
        else:
            if if_exists in ["upsert", "append-new"]:
                raise ValueError(
                    f"Primary key column name(s) are required if if_exists = '{if_exists}'."
                )
        try:
            schema = self._standardize_name(schema_name)
            table = self._standardize_name(table_name)
            table_exists = table in self.list_tables(schema)
            if table_exists:
                if if_exists == "replace":
                    self.query(f"DROP TABLE IF EXISTS {schema}.{table}")
                elif if_exists == "fail":
                    raise ValueError(f"""Table "{table}" already exists in schema "{schema}".""")
            with self._get_connection() as conn:
                conn.register("temp_df", df)
                if not table_exists:
                    create_query = f"""
                        CREATE TABLE IF NOT EXISTS {schema}.{table}
                        AS SELECT * FROM temp_df
                    """
                    self.query(create_query, conn)
                else:
                    insert_query = f"""
                        INSERT INTO {schema}.{table}
                        SELECT * FROM temp_df
                    """
                    if if_exists in ["upsert", "append-new"]:
                        if if_exists == "append-new":
                            key_conditions = " AND ".join(
                                [f"target.{key} = temp_df.{key}" for key in primary_keys]
                            )
                            where_clause = f"""
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM {schema}.{table} AS target
                                    WHERE {key_conditions}
                                )"""
                            insert_query = insert_query + where_clause
                        elif if_exists == "upsert":
                            key_conditions = " AND ".join(
                                [f"{schema}.{table}.{key} = temp_df.{key}" for key in primary_keys]
                            )
                            delete_query = f"""
                                DELETE FROM {schema}.{table}
                                WHERE EXISTS (
                                    SELECT 1 FROM temp_df
                                    WHERE {key_conditions}
                                )
                            """
                            self.query(delete_query, conn)
                    self.query(insert_query, conn)
                conn.unregister("temp_df")
        except Exception as e:
            raise IngestError(f"Failed to ingest DataFrame: {str(e)}")

    def vacuum(self) -> None:
        self.query("VACUUM")


class QueryError(Exception):
    """Raised when a query execution fails."""

    pass


class IngestError(Exception):
    """Raised when data ingestion fails."""

    pass


class InvalidNameError(Exception):
    """Raised when an invalid duckdb resource name is used."""

    pass
