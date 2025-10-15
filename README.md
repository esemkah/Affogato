# Affogato API

A powerful Natural Language to SQL Query API built with FastAPI, leveraging LangChain and Groq for natural language processing, and DuckDB for efficient database operations.

## Overview

Affogato API allows users to query databases using natural language instead of writing complex SQL queries. The API converts natural language questions into SQL queries, executes them against a DuckDB database, and returns the results in a structured format.

## Features

- **Natural Language to SQL Conversion**: Convert plain English questions into SQL queries using advanced LLM models
- **FastAPI Framework**: High-performance, modern Python web framework
- **DuckDB Integration**: Fast, analytical database engine
- **Rate Limiting**: Built-in rate limiting to prevent abuse
- **CORS Support**: Cross-origin resource sharing enabled
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Input Validation**: Robust validation for SQL injection prevention
- **Health Checks**: Built-in health check endpoints
- **Comprehensive Testing**: Full test coverage with pytest

## Installation

### Prerequisites

- Python 3.8+
- Groq API Key (sign up at [groq.com](https://groq.com))

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd affogato
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory:
```env
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=llama-3.1-8b-instant
DATABASE_PATH=data/database.db
LOG_LEVEL=INFO
MAX_QUERY_ROWS=10000
RATE_LIMIT_REQUESTS=5
RATE_LIMIT_WINDOW=60
```

## Configuration

The application can be configured using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GROQ_API_KEY` | - | Your Groq API key (required) |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | LLM model to use for NLQ conversion |
| `DATABASE_PATH` | `data/database.db` | Path to the DuckDB database file |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `MAX_QUERY_ROWS` | `10000` | Maximum rows to return in query results |
| `RATE_LIMIT_REQUESTS` | `5` | Number of requests allowed per time window |
| `RATE_LIMIT_WINDOW` | `60` | Rate limit time window in seconds |

## Usage

### Running the Application

Start the development server:
```bash
python src/main.py
```

The API will be available at `http://localhost:8000`

### API Endpoints

#### GET /
Basic health check endpoint.

**Response:**
```json
{
  "message": "Welcome to Affogato Platform",
  "version": "1.0.0",
  "status": "healthy"
}
```

#### GET /health
Detailed health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "services": "initialized"
}
```

#### POST /api/query
Execute a query (natural language or direct SQL).

**Request Body:**
```json
{
  "question": "Show me all users with their email addresses",
  "use_nlq": true,
  "max_rows": 1000
}
```

**Parameters:**
- `question` (string, required): The query question or SQL statement
- `use_nlq` (boolean, optional): Whether to use natural language to SQL conversion (default: true)
- `max_rows` (integer, optional): Maximum number of rows to return (1-10000)

**Response:**
```json
{
  "query": "SELECT id, name, email FROM users;",
  "results": [
    {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com"
    }
  ],
  "execution_time_ms": 45.2,
  "row_count": 1
}
```

### Example Usage

#### Natural Language Query
```bash
curl -X POST "http://localhost:8000/api/query" \
     -H "Content-Type: application/json" \
     -d '{
       "question": "How many users do we have?",
       "use_nlq": true
     }'
```

#### Direct SQL Query
```bash
curl -X POST "http://localhost:8000/api/query" \
     -H "Content-Type: application/json" \
     -d '{
       "question": "SELECT COUNT(*) as user_count FROM users;",
       "use_nlq": false
     }'
```

## Testing

Run the test suite:
```bash
pytest tests/
```

Run tests with coverage:
```bash
pytest --cov=src tests/
```

### Test Structure

- `tests/test_database_service.py`: Database service tests
- `tests/test_query_endpoint.py`: API endpoint tests
- `tests/test_sql_chain.py`: SQL chain manager tests

## Project Structure

```
affogato/
├── src/
│   ├── main.py                 # FastAPI application entry point
│   ├── api/
│   │   └── endpoint/
│   │       └── query.py        # Query API endpoint
│   ├── chains/
│   │   └── sql_chain.py        # SQL chain manager for NLQ conversion
│   ├── core/
│   │   ├── database.py         # Database service initialization
│   │   ├── logger.py           # Logging configuration
│   │   └── rate_limit.py       # Rate limiting configuration
│   └── services/
│       └── database_service.py # DuckDB service layer
├── tests/                      # Test suite
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore rules
└── README.md                   # This file
```

## Security Features

- **SQL Injection Prevention**: Input validation and sanitization
- **Rate Limiting**: Prevents abuse with configurable limits
- **CORS Configuration**: Controlled cross-origin access
- **Input Validation**: Pydantic models for request validation
- **Safe SQL Generation**: Validation of generated SQL queries

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add tests for new features
- Update documentation as needed
- Ensure all tests pass before submitting PR

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Open an issue on GitHub
- Check the documentation
- Review the test cases for usage examples

## Acknowledgments

- [FastAPI](https://fastapi.tiangolo.com/) - Modern Python web framework
- [LangChain](https://langchain.com/) - Framework for LLM applications
- [Groq](https://groq.com/) - Fast LLM inference
- [DuckDB](https://duckdb.org/) - Analytical database engine
