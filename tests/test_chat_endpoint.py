import pytest
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

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


class TestChatEndpoint:
    def test_chat_endpoint_query_intent_success(self, mock_services, client):
        """Test successful chat interaction with query intent."""
        mock_db, mock_chain = mock_services

        with patch("src.api.endpoint.chat.get_services", return_value=mock_services):
            response = client.post(
                "/api/chat",
                json={"message": "Show me all users"}
            )

        assert response.status_code == 200
        data = response.json()
        assert "response" in data
        assert "query" in data
        assert "results" in data
        assert data["query"] == "SELECT * FROM users;"
        assert len(data["results"]) == 2
        assert "I found 2 result(s)" in data["response"]

        mock_chain.natural_language_to_sql.assert_called_once_with("Show me all users")
        mock_db.execute_query.assert_called_once_with("SELECT * FROM users;")

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_chat_endpoint_general_chat(self, mock_services, client):
        """Test chat interaction with general message."""
        mock_db, mock_chain = mock_services

        with patch("src.api.endpoint.chat.get_services", return_value=mock_services):
            response = client.post(
                "/api/chat",
                json={"message": "Hello, how are you?"}
            )

        assert response.status_code == 200
        data = response.json()
        assert "response" in data
        assert "Hello, how are you?" in data["response"]
        assert data["query"] is None
        assert data["results"] is None

        mock_chain.natural_language_to_sql.assert_not_called()
        mock_db.execute_query.assert_not_called()

    def test_chat_endpoint_query_intent_no_results(self, mock_services, client):
        """Test chat interaction with query intent but no results."""
        mock_db, mock_chain = mock_services
        mock_db.execute_query.return_value = []

        with patch("src.api.endpoint.chat.get_services", return_value=mock_services):
            response = client.post(
                "/api/chat",
                json={"message": "Show me all products"}
            )

        assert response.status_code == 200
        data = response.json()
        assert "I didn't find any results" in data["response"]
        assert data["query"] == "SELECT * FROM users;"
        assert len(data["results"]) == 0

    def test_chat_endpoint_empty_message(self, client):
        """Test validation of empty message."""
        response = client.post("/api/chat", json={"message": ""})
        assert response.status_code == 422  # Validation error

    def test_chat_endpoint_whitespace_message(self, client):
        """Test validation of whitespace-only message."""
        response = client.post("/api/chat", json={"message": "   "})
        assert response.status_code == 422  # Validation error

    @patch("src.main.services", None)
    def test_chat_endpoint_services_not_initialized(self, client):
        """Test handling when services are not initialized."""
        with patch("src.api.endpoint.chat.get_services", side_effect=RuntimeError("Services not initialized")):
            response = client.post(
                "/api/chat",
                json={"message": "Hello"}
            )

        assert response.status_code == 500
        data = response.json()
        assert "Services not initialized" in data["detail"]

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_chat_endpoint_execution_error(self, mock_services, client):
        """Test handling of chat execution errors."""
        mock_db, mock_chain = mock_services
        mock_chain.natural_language_to_sql.side_effect = Exception("LLM error")

        with patch("src.api.endpoint.chat.get_services", return_value=mock_services):
            response = client.post(
                "/api/chat",
                json={"message": "Show users"}
            )

        assert response.status_code == 500
        data = response.json()
        assert "Chat interaction failed" in data["detail"]

    @patch("src.main.services", new_callable=lambda: (Mock(), Mock()))
    def test_chat_endpoint_with_conversation_id(self, mock_services, client):
        """Test chat endpoint with conversation ID (for future multi-turn support)."""
        with patch("src.api.endpoint.chat.get_services", return_value=mock_services):
            response = client.post(
                "/api/chat",
                json={"message": "Hello", "conversation_id": "conv123"}
            )
        assert response.status_code == 200
        # Currently conversation_id is accepted but not used in logic
