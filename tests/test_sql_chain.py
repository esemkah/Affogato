import pytest
from unittest.mock import Mock, patch

from src.chains.sql_chain import SQLChainManager


class TestSQLChainManager:
    @pytest.fixture
    def mock_db_service(self):
        """Mock database service."""
        mock_service = Mock()
        mock_service.get_table_info.return_value = """
Table: users
Columns: id (INTEGER), name (VARCHAR), email (VARCHAR)

Table: orders
Columns: id (INTEGER), user_id (INTEGER), amount (DECIMAL)
"""
        return mock_service

    @pytest.fixture
    def mock_chain(self, mock_llm):
        """Mock the chain object."""
        mock_chain_obj = Mock()
        mock_chain_obj.invoke.return_value = "SELECT id, name FROM users WHERE id = 1;"
        return mock_chain_obj

    @pytest.fixture
    def mock_llm(self):
        """Mock LLM for testing."""
        return Mock()

    @pytest.fixture
    def sql_chain(self, mock_llm, mock_db_service, mock_chain):
        """Create SQLChainManager instance."""
        chain = SQLChainManager(mock_llm, mock_db_service)
        chain.chain = mock_chain  # Override the chain with mock
        return chain

    def test_init(self, mock_llm, mock_db_service):
        """Test initialization."""
        chain = SQLChainManager(mock_llm, mock_db_service)
        assert chain.llm == mock_llm
        assert chain.db_service == mock_db_service
        assert chain._cached_schema is None

    def test_get_schema_info_caching(self, sql_chain, mock_db_service):
        """Test schema caching."""
        # First call should retrieve from service
        schema1 = sql_chain._get_schema_info()
        mock_db_service.get_table_info.assert_called_once()
        assert schema1 == mock_db_service.get_table_info.return_value

        # Second call should use cache
        mock_db_service.get_table_info.reset_mock()
        schema2 = sql_chain._get_schema_info()
        mock_db_service.get_table_info.assert_not_called()
        assert schema1 == schema2

    def test_clean_sql_output_code_block(self, sql_chain):
        """Test cleaning SQL from markdown code blocks."""
        raw_output = "Here's the SQL query:\n```sql\nSELECT * FROM users;\n```"
        cleaned = sql_chain._clean_sql_output(raw_output)
        assert cleaned == "SELECT * FROM users;"

    def test_clean_sql_output_generic_code_block(self, sql_chain):
        """Test cleaning SQL from generic code blocks."""
        raw_output = "```\nSELECT id FROM users WHERE active = 1;\n```"
        cleaned = sql_chain._clean_sql_output(raw_output)
        assert cleaned == "SELECT id FROM users WHERE active = 1;"

    def test_clean_sql_output_no_code_block(self, sql_chain):
        """Test cleaning when no code blocks present."""
        raw_output = "SELECT * FROM products ORDER BY price DESC;"
        cleaned = sql_chain._clean_sql_output(raw_output)
        assert cleaned == raw_output

    def test_clean_sql_output_with_thinking(self, sql_chain):
        """Test cleaning SQL with thinking tags."""
        raw_output = "<think>Let me analyze this query...</think>SELECT name FROM users;"
        cleaned = sql_chain._clean_sql_output(raw_output)
        assert cleaned == "SELECT name FROM users;"

    def test_natural_language_to_sql(self, sql_chain, mock_chain, mock_db_service):
        """Test natural language to SQL conversion."""
        question = "Show me all active users"
        result = sql_chain.natural_language_to_sql(question)

        # Verify schema was retrieved
        mock_db_service.get_table_info.assert_called_once()

        # Verify chain was called with correct input
        mock_chain.invoke.assert_called_once()
        call_args = mock_chain.invoke.call_args[0][0]
        assert "schema" in call_args
        assert "question" in call_args
        assert call_args["question"] == question

        # Verify result
        assert result == "SELECT id, name FROM users WHERE id = 1;"

    def test_natural_language_to_sql_error_handling(self, sql_chain, mock_chain):
        """Test error handling in natural language to SQL conversion."""
        mock_chain.invoke.side_effect = Exception("LLM Error")

        with pytest.raises(RuntimeError, match="Failed to generate SQL query"):
            sql_chain.natural_language_to_sql("Show users")

    def test_clear_cache(self, sql_chain):
        """Test cache clearing."""
        # Populate cache
        sql_chain._get_schema_info()
        assert sql_chain._cached_schema is not None

        # Clear cache
        sql_chain.clear_cache()
        assert sql_chain._cached_schema is None

    @patch("src.chains.sql_chain.time")
    def test_processing_timing(self, mock_time, sql_chain, mock_chain):
        """Test that processing time is logged."""
        mock_time.time.side_effect = [100.0, 100.5, 101.0]  # Start, end for processing, end for validation

        with patch("src.chains.sql_chain.logger") as mock_logger:
            sql_chain.natural_language_to_sql("test query")

            # Check that debug logging includes timing
            debug_calls = [call for call in mock_logger.debug.call_args_list if "Generated SQL" in str(call)]
            assert len(debug_calls) > 0

    def test_validate_generated_sql_safe(self, sql_chain):
        """Test validation of safe SQL queries."""
        safe_queries = [
            "SELECT * FROM users;",
            "SELECT id, name FROM users WHERE active = 1;",
            "SELECT COUNT(*) FROM orders GROUP BY user_id;",
        ]

        for query in safe_queries:
            assert sql_chain._validate_generated_sql(query)

    def test_validate_generated_sql_unsafe(self, sql_chain):
        """Test validation of unsafe SQL queries."""
        unsafe_queries = [
            "DROP TABLE users;",
            "DELETE FROM users WHERE 1=1;",
            "UPDATE users SET password = 'hack';",
            "INSERT INTO users VALUES (1, 'hack');",
            "ALTER TABLE users ADD COLUMN hack VARCHAR;",
            "CREATE TABLE hack (id INTEGER);",
            "TRUNCATE TABLE users;",
            "SELECT * FROM users; DROP TABLE users; --",
        ]

        for query in unsafe_queries:
            assert not sql_chain._validate_generated_sql(query)
