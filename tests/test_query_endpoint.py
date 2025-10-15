import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from fastapi import HTTPException

from src.main import app
from src.services.database_service import DuckDBService
from src.chains.sql_chain import SQLChainManager


@pytest.fixture
def client():
    """Test client for FastAPI app."""
    # Provide a unique header so the shared limiter won't collide across tests
    import uuid

    test_id = str(uuid.uuid4())
    return TestClient(app, headers={"x-test-id": test_id})


@pytest.fixture
def mock_services():
    """Mock services for testing."""
    mock_db = Mock(spec=DuckDBService)
    mock_chain = Mock(spec=SQLChainManager)

    mock_db.execute_query.return_value = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]

    mock_chain.natural_language_to_sql.return_value = "SELECT * FROM users;"

    return mock_db, mock_chain


class TestQueryEndpoint:
    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "Welcome to Affogato Platform" in data["message"]
        assert data["status"] == "healthy"

    def test_health_endpoint(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_query_endpoint_nlq_success(self, mock_services, client):
        """Test successful NLQ query execution."""
        mock_db, mock_chain = mock_services

        with patch("src.api.endpoint.query.get_services", return_value=mock_services):
            response = client.post(
                "/api/query",
                json={"question": "Show me all users", "use_nlq": True}
            )

        assert response.status_code == 200
        data = response.json()
        assert "query" in data
        assert "results" in data
        assert data["query"] == "SELECT * FROM users;"
        assert len(data["results"]) == 2

        mock_chain.natural_language_to_sql.assert_called_once_with("Show me all users")
        mock_db.execute_query.assert_called_once_with("SELECT * FROM users;")

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_query_endpoint_direct_sql_success(self, mock_services, client):
        """Test successful direct SQL query execution."""
        mock_db, mock_chain = mock_services

        with patch("src.api.endpoint.query.get_services", return_value=mock_services):
            response = client.post(
                "/api/query",
                json={"question": "SELECT * FROM users;", "use_nlq": False}
            )

        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "SELECT * FROM users;"
        assert len(data["results"]) == 2

        mock_chain.natural_language_to_sql.assert_not_called()
        mock_db.execute_query.assert_called_once_with("SELECT * FROM users;")

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_query_endpoint_invalid_direct_sql(self, mock_services, client):
        """Test rejection of invalid direct SQL."""
        mock_db, mock_chain = mock_services

        with patch("src.api.endpoint.query.get_services", return_value=mock_services):
            response = client.post(
                "/api/query",
                json={"question": "DROP TABLE users;", "use_nlq": False}
            )

        assert response.status_code == 400
        data = response.json()
        assert "Invalid SQL query" in data["detail"]

        mock_db.execute_query.assert_not_called()

    def test_query_endpoint_empty_question(self, client):
        """Test validation of empty question."""
        response = client.post("/api/query", json={"question": "", "use_nlq": True})
        assert response.status_code == 422  # Validation error

    def test_query_endpoint_whitespace_question(self, client):
        """Test validation of whitespace-only question."""
        response = client.post("/api/query", json={"question": "   ", "use_nlq": True})
        assert response.status_code == 422  # Validation error

    @patch("src.main.services", None)
    def test_query_endpoint_services_not_initialized(self, client):
        """Test handling when services are not initialized."""
        with patch("src.api.endpoint.query.get_services", side_effect=RuntimeError("Services not initialized")):
            response = client.post(
                "/api/query",
                json={"question": "SELECT 1;", "use_nlq": False}
            )

        assert response.status_code == 500
        data = response.json()
        assert "Services not initialized" in data["detail"]

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_query_endpoint_execution_error(self, mock_services, client):
        """Test handling of query execution errors."""
        mock_db, mock_chain = mock_services
        mock_db.execute_query.side_effect = Exception("Database error")

        with patch("src.api.endpoint.query.get_services", return_value=mock_services):
            response = client.post(
                "/api/query",
                json={"question": "SELECT * FROM users;", "use_nlq": False}
            )

        assert response.status_code == 500
        data = response.json()
        assert "Query execution failed" in data["detail"]

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_query_endpoint_nlq_error(self, mock_services, client):
        """Test handling of NLQ conversion errors."""
        mock_db, mock_chain = mock_services
        mock_chain.natural_language_to_sql.side_effect = Exception("LLM error")

        with patch("src.api.endpoint.query.get_services", return_value=mock_services):
            response = client.post(
                "/api/query",
                json={"question": "Show users", "use_nlq": True}
            )

        assert response.status_code == 500
        data = response.json()
        assert "Query execution failed" in data["detail"]

    def test_rate_limiting_root(self, client):
        """Test rate limiting on root endpoint."""
        # Make multiple requests quickly
        response = None
        for i in range(15):  # Exceed the 10/minute limit
            response = client.get("/")

        # Last request should be rate limited
        assert response is not None
        assert response.status_code == 429

    def test_rate_limiting_health(self, client):
        """Test rate limiting on health endpoint."""
        # Make multiple requests quickly
        response = None
        for i in range(35):  # Exceed the 30/minute limit
            response = client.get("/health")

        # Last request should be rate limited
        assert response is not None
        assert response.status_code == 429
