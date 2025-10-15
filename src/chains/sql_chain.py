import re
import time
from functools import lru_cache
from typing import TYPE_CHECKING, Optional

from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough

from src.core.logger import logger

if TYPE_CHECKING:
    from src.services.database_service import DuckDBService


class SQLChainManager:
    """Manager for SQL chain operations with caching and optimization."""

    def __init__(self, llm, db_service: "DuckDBService"):
        self.llm = llm
        self.db_service = db_service
        self._cached_schema: Optional[str] = None
        self._schema_cache_time: Optional[float] = None
        self._cache_ttl = 300  # 5 minutes cache TTL

        # Enhanced prompt for better SQL generation
        self.sql_prompt = PromptTemplate(
            input_variables=["schema", "question"],
            template="""You are an expert SQL developer specializing in DuckDB. Convert the following natural language query to a valid DuckDB SQL query.

Database Schema:
{schema}

Instructions:
- Generate only the SQL query, no explanations
- Use proper DuckDB syntax
- Ensure the query is safe and efficient
- Use appropriate JOINs when needed
- Handle NULL values properly
- Limit results to reasonable sizes (use LIMIT if appropriate)

Question: {question}

SQL Query:""",
        )

        self.chain = (
            {"schema": RunnablePassthrough(), "question": RunnablePassthrough()}
            | self.sql_prompt
            | llm
            | (lambda x: str(x))
        )

    def _get_schema_info(self) -> str:
        """Retrieve and cache schema information with TTL."""
        current_time = time.time()
        if (
            self._cached_schema is None
            or self._schema_cache_time is None
            or (current_time - self._schema_cache_time) > self._cache_ttl
        ):
            try:
                self._cached_schema = self.db_service.get_table_info()
                self._schema_cache_time = current_time
                logger.debug("Schema information cached/refreshed")
            except Exception as e:
                logger.error(f"Failed to retrieve schema info: {e}")
                raise RuntimeError(f"Failed to retrieve schema info: {e}")
        return self._cached_schema

    @staticmethod
    @lru_cache(maxsize=100)
    def _clean_sql_output(raw_output: str) -> str:
        """Clean and extract SQL from LLM output with caching for repeated outputs."""
        result = raw_output.strip()

        # Remove thinking tags if present
        if "<think>" in result and "</think>" in result:
            parts = result.split("</think>")
            if len(parts) > 1:
                result = parts[1].strip()

        # Extract SQL from code blocks
        sql_match = re.search(
            r"```sql\s*(.*?)\s*```", result, re.DOTALL | re.IGNORECASE
        )
        if sql_match:
            return sql_match.group(1).strip()

        # Fallback: extract from generic code blocks
        code_match = re.search(r"```\s*(.*?)\s*```", result, re.DOTALL)
        if code_match:
            return code_match.group(1).strip()

        # If no code blocks, assume the entire output is SQL
        return result

    def _validate_generated_sql(self, sql: str) -> bool:
        """Basic validation of generated SQL."""
        sql_lower = sql.lower().strip()

        # Must be SELECT query
        if not sql_lower.startswith("select"):
            return False

        # Check for dangerous patterns (shouldn't happen with proper prompting)
        dangerous = [
            "drop",
            "delete",
            "update",
            "insert",
            "alter",
            "create",
            "truncate",
        ]
        for word in dangerous:
            if re.search(r"\b" + word + r"\b", sql_lower):
                logger.warning(f"Potentially dangerous SQL generated: {sql[:100]}...")
                return False

        return True

    def natural_language_to_sql(self, question: str) -> str:
        """Convert natural language question to SQL query with validation."""
        logger.debug(f"Converting question to SQL: {question[:100]}...")
        start_time = time.time()

        try:
            schema = self._get_schema_info()
            raw_result = self.chain.invoke({"schema": schema, "question": question})
            cleaned_sql = self._clean_sql_output(raw_result)

            # Validate the generated SQL
            if not self._validate_generated_sql(cleaned_sql):
                raise ValueError("Generated SQL failed validation")

            processing_time = time.time() - start_time
            logger.debug(
                f"Generated SQL in {processing_time:.2f}s: {cleaned_sql[:100]}..."
            )
            return cleaned_sql

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(
                f"Failed to generate SQL query after {processing_time:.2f}s: {e}"
            )
            raise RuntimeError(f"Failed to generate SQL query: {e}")

    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._cached_schema = None
        self._schema_cache_time = None
        self._clean_sql_output.cache_clear()
        logger.info("SQL chain cache cleared")
