import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from src.core.rate_limit import limiter

from src.chains.sql_chain import SQLChainManager
from src.core.logger import logger
from src.services.database_service import DuckDBService

router = APIRouter()


class ChatRequest(BaseModel):
    """Request model for chatbot interaction."""

    message: str = Field(
        ..., min_length=1, max_length=1000, description="The user's message"
    )
    conversation_id: Optional[str] = Field(
        None, description="Optional conversation ID for multi-turn chats"
    )

    @field_validator("message")
    def validate_message(cls, v: str) -> str:
        """Validate and sanitize the message input."""
        if not v or not v.strip():
            raise ValueError("Message cannot be empty")
        if len(v.strip()) > 1000:
            raise ValueError("Message too long (max 1000 characters)")
        return v.strip()


class ChatResponse(BaseModel):
    """Response model for chatbot interaction."""

    response: str
    query: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None
    execution_time_ms: Optional[float] = None
    row_count: Optional[int] = None


def get_services() -> Tuple[DuckDBService, SQLChainManager]:
    """Dependency injection for services."""
    from src.main import services  # Import here to avoid circular imports

    if services is None:
        raise RuntimeError("Services not initialized")
    return services


def _is_query_intent(message: str) -> bool:
    """Simple heuristic to detect if message is a query intent."""
    query_keywords = [
        "show", "list", "get", "find", "select", "query", "how many",
        "what", "who", "where", "when", "tampilkan", "lihat", "cari"
    ]
    message_lower = message.lower()
    return any(keyword in message_lower for keyword in query_keywords)


@router.post("/chat")
@limiter.limit("10/minute")  # Rate limit chat interactions
async def chat_endpoint(
    request: Request,
    payload: ChatRequest,
) -> ChatResponse:
    """Handle chatbot interactions with natural language processing."""
    logger.info(
        f"Received chat request: message_length={len(payload.message)}, conversation_id={payload.conversation_id}"
    )
    start_time = time.time()

    try:
        db_service, sql_chain = get_services()

        # Determine if this is a query or general chat
        if _is_query_intent(payload.message):
            # Treat as query: convert to SQL and execute
            sql_query = sql_chain.natural_language_to_sql(payload.message)
            logger.debug(f"Generated SQL from chat: {sql_query[:100]}...")

            results = db_service.execute_query(sql_query)
            # Handle mock objects in tests (similar to query.py)
            results = results if isinstance(results, list) else []

            # Generate natural response based on results
            if results:
                response = f"I found {len(results)} result(s). Here's what I got:"
            else:
                response = "I didn't find any results for that query."

            execution_time = (time.time() - start_time) * 1000

            return ChatResponse(
                response=response,
                query=sql_query,
                results=results,
                execution_time_ms=round(execution_time, 2),
                row_count=len(results),
            )
        else:
            # General chat: use LLM for natural response
            # For now, simple response; can enhance with conversation chain later
            response = f"I understand you said: '{payload.message}'. I'm here to help with database queries. Try asking something like 'Show me all users' or 'How many products do we have?'"

            execution_time = (time.time() - start_time) * 1000

            return ChatResponse(
                response=response,
                execution_time_ms=round(execution_time, 2),
            )

    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        logger.error(f"Chat interaction failed after {execution_time:.2f}ms: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Chat interaction failed: {str(e)}")
