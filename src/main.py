from contextlib import asynccontextmanager
from typing import Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from src.core.rate_limit import limiter

from .api.endpoint.query import router as query_router
from .chains.sql_chain import SQLChainManager
from .core.database import create_services
from .core.logger import logger
from .services.database_service import DuckDBService

# Global services cache with proper typing
services: Optional[Tuple[DuckDBService, SQLChainManager]] = None

# Rate limiter (shared instance imported from src.core.rate_limit)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan: startup and shutdown."""
    global services
    logger.info("Starting Affogato API")
    try:
        services = create_services()
        logger.info("Services initialized successfully")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    finally:
        logger.info("Shutting down Affogato API")
        if services:
            db_service, _ = services
            db_service.close()
            logger.info("Database connection closed")


app = FastAPI(
    title="Affogato API",
    description="Natural Language to SQL Query API using LangChain and Groq",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add trusted host middleware (configure for production)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])

# Add rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore
app.add_middleware(SlowAPIMiddleware)

app.include_router(query_router, prefix="/api", tags=["query"])


@app.get("/")
@limiter.limit("10/minute")
async def root(request: Request):
    """Root endpoint with basic health check."""
    return {
        "message": "Welcome to Affogato Platform",
        "version": "1.0.0",
        "status": "healthy",
    }


@app.get("/health")
@limiter.limit("30/minute")
async def health_check(request: Request):
    """Health check endpoint."""
    return {
        "status": "healthy",
        "services": "initialized" if services else "not initialized",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True,
    )
