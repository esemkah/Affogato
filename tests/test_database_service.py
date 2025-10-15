import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.services.database_service import DuckDBService


class TestDuckDBService:
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        import tempfile
        import os
        from pathlib import Path

        # Create a temporary file and immediately delete it to get a unique path
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(path)  # Remove the file so DuckDB can create it

        yield path

        # Clean up
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def db_service(self, temp_db_path):
        """Create a DuckDBService instance."""
        return DuckDBService(temp_db_path, max_rows=100)

    def test_init(self, temp_db_path):
        """Test service initialization."""
        service = DuckDBService(temp_db_path)
        assert service.db_path == Path(temp_db_path)
        assert service.connection is None
        assert service.max_rows == 10000  # default

    def test_execute_query_select(self, db_service):
        """Test executing a SELECT query."""
        # Create a test table
        db_service.execute_query("CREATE TABLE test (id INTEGER, name VARCHAR)")
        db_service.execute_query("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

        results = db_service.execute_query("SELECT * FROM test ORDER BY id")
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["name"] == "Alice"
        assert results[1]["id"] == 2
        assert results[1]["name"] == "Bob"

    def test_execute_query_with_limit(self, db_service):
        """Test query execution with automatic LIMIT."""
        # Create a test table with many rows
        db_service.execute_query("CREATE TABLE test (id INTEGER)")
        values = ", ".join(f"({i})" for i in range(200))
        db_service.execute_query(f"INSERT INTO test VALUES {values}")

        # Query without explicit LIMIT should be limited to max_rows
        results = db_service.execute_query("SELECT * FROM test ORDER BY id")
        assert len(results) == 100  # max_rows

    def test_execute_query_invalid_sql(self, db_service):
        """Test handling of invalid SQL."""
        with pytest.raises(RuntimeError, match="Failed to execute query"):
            db_service.execute_query("INVALID SQL QUERY")

    def test_get_table_info(self, db_service):
        """Test retrieving table schema information."""
        # Create test tables
        db_service.execute_query("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
        db_service.execute_query("CREATE TABLE products (id INTEGER, price DECIMAL)")

        schema = db_service.get_table_info()
        assert "Table: users" in schema
        assert "id (INTEGER)" in schema
        assert "name (VARCHAR)" in schema
        assert "age (INTEGER)" in schema
        assert "Table: products" in schema
        assert "price (DECIMAL" in schema

    def test_get_table_info_empty_db(self, db_service):
        """Test schema info for empty database."""
        schema = db_service.get_table_info()
        assert schema == "No tables found in database."

    def test_get_table_count(self, db_service):
        """Test getting table count."""
        assert db_service.get_table_count() == 0

        db_service.execute_query("CREATE TABLE test1 (id INTEGER)")
        assert db_service.get_table_count() == 1

        db_service.execute_query("CREATE TABLE test2 (id INTEGER)")
        assert db_service.get_table_count() == 2

    def test_close(self, db_service):
        """Test closing database connection."""
        # Execute a query to establish connection
        db_service.execute_query("SELECT 1")
        assert db_service.connection is not None

        db_service.close()
        assert db_service.connection is None

    def test_context_manager(self, db_service):
        """Test that connection is properly managed."""
        with db_service._get_connection() as conn:
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1

    @patch("src.services.database_service.connect")
    def test_connection_error_handling(self, mock_connect, db_service):
        """Test handling of connection errors."""
        mock_connect.side_effect = Exception("Connection failed")

        with pytest.raises(RuntimeError, match="Failed to initialize DuckDB connection"):
            with db_service._get_connection():
                pass
