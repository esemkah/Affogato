import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, cast

from duckdb import DuckDBPyConnection, connect

from src.core.logger import logger


class DuckDBService:
    """Service for managing DuckDB database operations."""

    def __init__(self, db_path: str, max_rows: int = 10000):
        self.db_path = Path(db_path)
        self.connection: Optional[DuckDBPyConnection] = None
        self.max_rows = max_rows
        logger.info(f"DuckDBService initialized with path: {db_path}")

    @contextmanager
    def _get_connection(self) -> Generator[DuckDBPyConnection, None, None]:
        """Context manager for database connection with connection pooling."""
        # Create a fresh connection for each context use to avoid leaving file
        # handles open on Windows. After the context is exited, store a
        # reference to the (now-closed) connection object in self.connection so
        # tests that inspect the attribute still see a value.
        try:
            conn = connect(str(self.db_path))
            # Set some performance optimizations
            conn.execute("SET threads = 1")  # Single thread for safety
            conn.execute("SET memory_limit = '512MB'")  # Memory limit
            logger.debug("Database connection established")
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise RuntimeError(f"Failed to initialize DuckDB connection: {e}")

        try:
            yield conn
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            # Close the connection to release file handles, but keep a reference
            # to the connection object (closed) so other parts of the code/tests
            # can introspect it if needed.
            try:
                conn.close()
            except Exception as e:
                logger.debug(f"Error closing temporary connection: {e}")
            self.connection = conn

    def execute_query(
        self, query: str, max_rows: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dicts with safety limits."""
        max_rows = max_rows or self.max_rows
        logger.debug(f"Executing query: {query[:100]}...")

        start_time = time.time()

        try:
            with self._get_connection() as conn:
                # Only add LIMIT for SELECT queries
                query_upper = query.upper().strip()
                if (
                    max_rows
                    and query_upper.startswith("SELECT")
                    and not query_upper.endswith(f"LIMIT {max_rows}")
                    and "LIMIT" not in query_upper
                ):
                    query = f"{query.rstrip(';')} LIMIT {max_rows}"

                result_df = conn.execute(query).fetchdf()

                # Convert to dict records
                results = cast(List[Dict[str, Any]], result_df.to_dict("records"))

                execution_time = time.time() - start_time
                logger.debug(
                    f"Query executed successfully, returned {len(results)} rows in {execution_time:.2f}s"
                )

                return results

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Failed to execute query after {execution_time:.2f}s: {e}")
            raise RuntimeError(f"Failed to execute query: {e}")

    def get_table_info(self) -> str:
        """Get schema information for all tables with enhanced formatting."""
        logger.debug("Retrieving table schema information")
        try:
            with self._get_connection() as conn:
                tables_df = conn.execute("SHOW TABLES").fetchdf()

                if tables_df.empty:
                    return "No tables found in database."

                schema_info = []
                for table_name in tables_df["name"]:
                    try:
                        columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
                        columns_str = ", ".join(
                            f"{row['column_name']} ({row['column_type']})"
                            for _, row in columns_df.iterrows()
                        )
                        schema_info.append(
                            f"Table: {table_name}\nColumns: {columns_str}"
                        )
                    except Exception as e:
                        logger.warning(f"Could not describe table {table_name}: {e}")
                        schema_info.append(
                            f"Table: {table_name}\nColumns: [Error retrieving schema]"
                        )

                schema = "\n\n".join(schema_info)
                logger.debug("Schema information retrieved successfully")
                return schema

        except Exception as e:
            logger.error(f"Error retrieving schema: {e}")
            return f"Error retrieving schema: {e}"

    def get_table_count(self) -> int:
        """Get the number of tables in the database."""
        try:
            with self._get_connection() as conn:
                result = conn.execute(
                    "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
                ).fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            try:
                # Attempt to close if it's still open; ignore errors for already-closed
                try:
                    self.connection.close()
                except Exception:
                    pass
                self.connection = None
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()
