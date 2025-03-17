# Self-Healing Data Pipeline Testing Framework

This directory contains the comprehensive testing framework for the self-healing data pipeline project. The framework is designed to ensure the reliability, performance, and correctness of all pipeline components through multiple testing layers and approaches.

## Testing Strategy Overview

The testing strategy follows a multi-layered approach to validate different aspects of the pipeline:

- **Unit Testing**: Validates individual components in isolation
- **Integration Testing**: Validates interactions between components
- **End-to-End Testing**: Validates complete workflows across the system
- **Performance Testing**: Validates system performance under various conditions

Each layer has specific goals, tools, and methodologies as detailed in their respective documentation.

## Directory Structure

```
src/test/
├── unit/                  # Unit tests for individual components
├── integration/           # Integration tests for component interactions
├── e2e/                   # End-to-end tests for complete workflows
├── performance/           # Performance and load tests
├── environments/          # Test environment configurations
├── scenarios/             # End-to-end test scenarios
├── fixtures/              # Shared test fixtures
├── utils/                 # Testing utilities and helpers
├── mock_data/             # Mock data for testing
├── scripts/               # Test automation scripts
├── conftest.py            # Global pytest configuration
├── pytest.ini             # Pytest settings
├── requirements.txt       # Testing dependencies
└── README.md              # This file
```

## Test Types

### Unit Tests

Unit tests focus on testing individual functions, classes, and modules in isolation. They use mocks and stubs to replace dependencies and validate component behavior independently.

See [Unit Testing Guidelines](unit/README.md) for details on writing and running unit tests.

### Integration Tests

Integration tests validate the interactions between components, ensuring they work together correctly. They focus on the interfaces and data flows between modules.

See [Integration Testing Framework](integration/README.md) for details on writing and running integration tests.

### End-to-End Tests

End-to-end tests validate complete workflows from start to finish, ensuring the system works as expected from a user's perspective. They interact with the system as a user would.

See [End-to-End Testing Framework](e2e/README.md) for details on writing and running E2E tests.

### Performance Tests

Performance tests validate the system's performance characteristics under various conditions, including load testing, stress testing, and scalability testing.

See [Performance Testing Framework](performance/README.md) for details on writing and running performance tests.

## Test Environments

The testing framework supports multiple environment configurations to balance test fidelity with execution speed:

- **Local Environment**: Docker-based environment for development and unit testing
- **GCP Test Environment**: Isolated GCP project for integration and performance testing

See [Test Environments](environments/README.md) for details on setting up and using test environments.

## Test Scenarios

The `scenarios/` directory contains end-to-end test scenarios that validate the functionality of the pipeline components in realistic scenarios. These scenarios combine multiple components and validate their interactions.

See [Test Scenarios](scenarios/README.md) for details on available scenarios and how to create new ones.

## Test Utilities

The `utils/` directory contains shared utilities for testing, including:

- Test data generators
- Mock service implementations
- Test helpers and assertions
- Resource management utilities

These utilities are designed to make tests more readable, maintainable, and reliable.

## Test Data Management

The `mock_data/` directory contains sample data files for testing, organized by component:

- `gcs/`: Sample files for GCS ingestion testing
- `cloudsql/`: SQL scripts for database testing
- `api/`: Sample API requests and responses
- `bigquery/`: Sample schemas and data for BigQuery
- `quality/`: Sample validation rules and results
- `healing/`: Sample issues and corrections
- `monitoring/`: Sample metrics and alerts

Dynamic test data can be generated using utilities in `utils/test_data_generators.py`.

## Test Fixtures

Shared test fixtures are defined in `fixtures/conftest.py` and imported by the test-specific `conftest.py` files. These fixtures provide:

- Test environment setup and teardown
- Mock service implementations
- Test data generation
- Resource management

Fixtures are organized by test type and component to ensure proper isolation and reuse.

## Running Tests

### Basic Usage

```bash
# Run all tests
python -m pytest

# Run specific test types
python -m pytest src/test/unit
python -m pytest src/test/integration
python -m pytest src/test/e2e
python -m pytest src/test/performance

# Run tests with coverage report
python -m pytest --cov=src/backend --cov-report=html
```

### Test Selection

```bash
# Run tests by marker
python -m pytest -m unit
python -m pytest -m "integration and not slow"

# Run specific test file
python -m pytest src/test/unit/backend/ingestion/test_connectors.py

# Run specific test function
python -m pytest src/test/unit/backend/ingestion/test_connectors.py::test_gcs_connector_extract
```

### Environment Configuration

```bash
# Run with real GCP services
python -m pytest src/test/integration --use-real-services

# Specify test project and location
python -m pytest --test-project-id=my-test-project --test-location=us-central1
```

## CI/CD Integration

Tests are automatically executed as part of the CI/CD pipeline:

- **Pull Request Validation**: Unit tests and critical integration tests
- **Main Branch Builds**: All tests except performance tests
- **Nightly Builds**: Complete test suite including performance tests

Test results are published as build artifacts and reported as GitHub status checks.

The CI configuration is defined in `.github/workflows/ci.yml` and `.github/workflows/quality-checks.yml`.

## Code Coverage Requirements

The project maintains the following code coverage targets:

- Overall coverage target: 85%
- Critical paths: 90%+
- Self-healing components: 90%+
- Monitoring components: 85%+

Coverage reports are generated during CI pipeline execution and available in the build artifacts.

## Test Maintenance

To maintain the quality and reliability of the test suite:

1. **Regular Maintenance**: Schedule regular test maintenance sprints
2. **Flaky Test Management**: Monitor and fix flaky tests promptly
3. **Test Performance**: Optimize slow tests to maintain fast feedback cycles
4. **Coverage Gaps**: Identify and address coverage gaps in critical components
5. **Dependency Updates**: Keep test dependencies up-to-date
6. **Documentation**: Keep test documentation current with implementation changes

## Contributing

When contributing to the test suite:

1. Follow the test organization structure
2. Maintain test independence and isolation
3. Use appropriate fixtures and utilities
4. Ensure proper resource cleanup
5. Document test purpose and approach
6. Include both positive and negative test cases
7. Verify test reliability before submitting

## Dependencies

Test dependencies are defined in `requirements.txt`. Key dependencies include:

- **pytest**: Primary testing framework
- **pytest-cov**: Coverage reporting
- **pytest-mock**: Mocking utilities
- **pytest-xdist**: Parallel test execution
- **locust/k6**: Performance testing tools
- **playwright/selenium**: Browser automation for E2E testing
- **google-cloud-testutils**: GCP service mocking

Install dependencies with:
```bash
pip install -r src/test/requirements.txt
```

This testing framework is designed to ensure the reliability, performance, and correctness of the self-healing data pipeline. By following the guidelines and using the provided tools, we can maintain high quality standards and catch issues early in the development process.