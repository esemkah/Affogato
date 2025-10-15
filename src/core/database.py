import os
from pathlib import Path
from typing import Tuple

# Load environment variables
from dotenv import load_dotenv
from langchain_groq import ChatGroq

from src.chains.sql_chain import SQLChainManager
from src.core.logger import logger
from src.services.database_service import DuckDBService

load_dotenv()


def create_services(
    db_path: str = "data/database.db",
    groq_model: str = "llama-3.1-8b-instant",
    temperature: float = 0.0,
) -> Tuple[DuckDBService, SQLChainManager]:
    """Create and return database and SQL chain services with configuration."""
    logger.info("Creating services...")

    # Validate database path
    db_path_obj = Path(db_path)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    # Check for required environment variables
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        logger.error("GROQ_API_KEY environment variable is missing")
        raise ValueError(
            "GROQ_API_KEY environment variable is required. Please set it in your .env file."
        )

    # Initialize Groq LLM with configuration
    from langchain_core.utils import convert_to_secret_str

    llm = ChatGroq(
        model=groq_model,
        temperature=temperature,
        api_key=convert_to_secret_str(groq_api_key),
        max_tokens=1024,  # Limit response length for SQL generation
    )
    logger.info(f"Groq LLM initialized with model: {groq_model}")

    # Initialize database service
    db_service = DuckDBService(str(db_path_obj))
    logger.info(f"Database service initialized with path: {db_path_obj}")

    # Initialize SQL chain manager
    sql_chain = SQLChainManager(llm, db_service)
    logger.info("SQL chain manager initialized")

    return db_service, sql_chain


def get_config() -> dict:
    """Get application configuration from environment variables."""
    return {
        "groq_api_key": os.getenv("GROQ_API_KEY"),
        "groq_model": os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
        "db_path": os.getenv("DATABASE_PATH", "data/database.db"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "max_query_rows": int(os.getenv("MAX_QUERY_ROWS", "10000")),
        "rate_limit_requests": int(os.getenv("RATE_LIMIT_REQUESTS", "5")),
        "rate_limit_window": int(os.getenv("RATE_LIMIT_WINDOW", "60")),  # seconds
    }
