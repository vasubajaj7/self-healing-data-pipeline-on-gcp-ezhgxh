# Unit Testing Guidelines

## Introduction

This directory contains unit tests for the self-healing data pipeline components. Unit tests focus on testing individual functions, classes, and modules in isolation to ensure they work as expected.

## Testing Framework

We use pytest for Python components and Jest for TypeScript/JavaScript components. These frameworks provide a robust foundation for writing and executing unit tests with features like fixtures, parameterization, and mocking.

## Directory Structure

The unit test directory structure mirrors the source code structure to make it easy to locate tests for specific components. For example, tests for `src/backend/ingestion/connectors/gcs_connector.py` would be located at `src/test/unit/backend/ingestion/test_connectors.py`.

## Naming Conventions

- Test files should be prefixed with `test_`
- Test functions should be named `test_<function_name>_<scenario>`
- Test classes should be named `Test<ClassName>`

## Mocking Strategy

We use the following mocking approaches:

- **Python**: `pytest-mock` for general mocking, `google-cloud-testutils` for GCP services
- **TypeScript**: Jest's built-in mocking capabilities

Mocks should be defined at the test function level to ensure test isolation. For common mocks, use fixtures defined in the appropriate `conftest.py` file.

## Test Data Management

- Use small, focused test datasets
- Define test data in the test file or reference files in `src/test/mock_data`
- Use factories and generators for complex test data requirements
- Parameterize tests to run with multiple data variations

## Code Coverage Requirements

- Overall coverage target: 85%
- Critical paths: 90%+
- Self-healing components: 90%+
- Monitoring components: 85%+

Coverage reports are generated during CI pipeline execution and available in the build artifacts.

## Best Practices

1. **Test Independence**: Each test should be independent and not rely on the state from other tests
2. **Fast Execution**: Unit tests should execute quickly to provide rapid feedback
3. **Readable Tests**: Tests should clearly show the setup, action, and assertion phases
4. **Meaningful Assertions**: Test the behavior and output, not the implementation details
5. **Error Cases**: Test both success and failure scenarios
6. **Parameterization**: Use parameterized tests for testing multiple inputs

## Running Tests

To run all unit tests:
```bash
# From project root
python -m pytest src/test/unit

# With coverage report
python -m pytest src/test/unit --cov=src/backend --cov-report=html
```

To run specific tests:
```bash
# Run tests for a specific module
python -m pytest src/test/unit/backend/ingestion/test_connectors.py

# Run a specific test
python -m pytest src/test/unit/backend/ingestion/test_connectors.py::test_gcs_connector_extract
```

## Continuous Integration

Unit tests are automatically executed as part of the CI pipeline for every pull request and merge to main. Tests must pass before code can be merged.

## Troubleshooting

- Use the `-v` flag for verbose output
- Use `--pdb` to drop into debugger on test failures
- Check the test fixtures in `conftest.py` files
- Ensure mocks are properly configured and reset between tests

## Conclusion

Comprehensive unit testing is essential for maintaining the reliability and quality of our self-healing data pipeline. By following these guidelines, we ensure that our components work correctly in isolation, making it easier to identify and fix issues early in the development process.