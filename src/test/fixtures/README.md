# Test Fixtures

## Test Fixtures Overview

This directory contains reusable test fixtures for the self-healing data pipeline project. Fixtures are organized by domain and provide consistent test environments, mock objects, and test data for unit, integration, and end-to-end testing.

## Directory Structure

- `conftest.py`: Central pytest configuration that imports and exposes all fixtures
- `backend/`: Backend-specific test fixtures
  - `ingestion_fixtures.py`: Fixtures for data ingestion components
  - `quality_fixtures.py`: Fixtures for data quality validation
  - `healing_fixtures.py`: Fixtures for self-healing AI components
  - `monitoring_fixtures.py`: Fixtures for monitoring and alerting
  - `optimization_fixtures.py`: Fixtures for performance optimization
  - `api_fixtures.py`: Fixtures for API testing
  - `airflow_fixtures.py`: Fixtures for Airflow/Composer testing
  - `db_fixtures.py`: Fixtures for database interactions
- `web/`: Frontend-specific test fixtures
  - `component_fixtures.tsx`: React component fixtures
  - `api_fixtures.tsx`: API interaction fixtures
  - `auth_fixtures.tsx`: Authentication fixtures

## Usage Guidelines

### Importing Fixtures

Fixtures are automatically available in test files through pytest's fixture discovery mechanism. Simply declare the fixture as a parameter in your test function:

```python
def test_gcs_connector(mock_gcs_client):
    # mock_gcs_client is automatically provided
    connector = GCSConnector("test-bucket")
    assert connector.client == mock_gcs_client
```

### Creating New Fixtures

1. Add domain-specific fixtures to the appropriate file in `backend/` or `web/`
2. For shared fixtures used across domains, consider adding them to `conftest.py`
3. Document fixtures with clear docstrings explaining their purpose and usage
4. Use appropriate scope (`function`, `class`, `module`, `session`) to optimize test performance

## Mock Data

Test fixtures often use mock data from the `src/test/mock_data/` directory. This data includes:

- Sample GCS files (CSV, JSON, Parquet, Avro)
- Sample database schemas and data
- Sample API requests and responses
- Sample validation rules and results
- Sample healing patterns and corrections

When creating new fixtures, leverage existing mock data or add new mock data to the appropriate subdirectory.

## Best Practices

1. **Isolation**: Fixtures should create isolated test environments that don't interfere with each other
2. **Deterministic**: Fixtures should produce consistent, deterministic results
3. **Performance**: Use appropriate fixture scopes to minimize setup/teardown overhead
4. **Maintainability**: Keep fixtures focused on a single responsibility
5. **Documentation**: Document fixtures with clear docstrings and examples
6. **Reusability**: Design fixtures to be reusable across multiple test cases

## Backend Fixtures

Backend fixtures provide mock objects, test data, and environment setup for testing Python components:

- **Mock Clients**: Mock GCP service clients (BigQuery, GCS, Vertex AI)
- **Mock Connectors**: Mock data source connectors
- **Test Data Generators**: Generate synthetic test data with specific characteristics
- **Environment Managers**: Set up and tear down test environments

## Web Fixtures

Web fixtures provide components, test data, and utilities for testing React components:

- **Mock Components**: Simplified versions of UI components for testing
- **Test Data**: Sample data for tables, charts, and forms
- **Rendering Utilities**: Helpers for rendering components with theme providers
- **Mock Handlers**: Mock event handlers for testing interactions

## Extending the Fixture Library

When adding new features to the pipeline, follow these steps to extend the fixture library:

1. Identify the testing needs for the new feature
2. Create or update fixtures in the appropriate domain-specific file
3. Add any necessary mock data to the mock_data directory
4. Document the new fixtures with clear docstrings
5. Update this README if adding new fixture categories or significant patterns