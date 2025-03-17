# Integration Testing Framework

This directory contains the integration tests for the self-healing data pipeline project. Integration tests verify that different components of the system work together correctly, focusing on the interactions between modules, services, and external dependencies.

The integration testing framework is designed to validate end-to-end workflows, data flows, and component interactions while providing flexibility to use either real or carefully mocked services based on testing requirements.

## Directory Structure

```
src/test/integration/
├── backend/                # Backend component integration tests
│   ├── test_database_interactions.py    # Database integration tests
│   ├── test_ingestion_pipeline.py       # Data ingestion pipeline tests
│   ├── test_quality_validation.py       # Data quality validation tests
│   ├── test_self_healing_process.py     # Self-healing process tests
│   ├── test_monitoring_alerts.py        # Monitoring and alerting tests
│   ├── test_bigquery_optimization.py    # BigQuery optimization tests
│   └── test_api_endpoints.py            # API endpoint integration tests
├── web/                    # Web UI component integration tests
│   ├── test_dashboard_integration.tsx   # Dashboard integration tests
│   ├── test_quality_data_flow.tsx       # Quality UI data flow tests
│   ├── test_alerting_system.tsx         # Alerting UI integration tests
│   └── test_self_healing_interaction.tsx # Self-healing UI tests
├── conftest.py             # Integration test fixtures and configuration
└── README.md               # This documentation file
```

## Test Categories

### Backend Integration Tests
Tests in the `backend/` directory focus on validating the integration between backend components, including:

- **Data flow** between pipeline stages (ingestion, quality, processing, etc.)
- **Service interactions** between components (API calls, event handling)
- **Database operations** and data persistence
- **Error handling and recovery** across component boundaries
- **Self-healing mechanisms** and their integration with other components

### Web UI Integration Tests
Tests in the `web/` directory focus on validating the integration between frontend components and their interaction with backend services, including:

- **Data flow** between UI components and API services
- **State management** across components
- **User workflow** validation across multiple screens
- **Error handling and display** in the UI
- **Real-time updates** and notifications

## Test Environment Options

The integration testing framework supports multiple environment configurations to balance test fidelity with execution speed:

### Mock-Based Environment
Uses carefully crafted mock objects and services to simulate component interactions without requiring actual GCP resources. This approach is:

- **Fast**: Tests run quickly without external dependencies
- **Deterministic**: Behavior is controlled and predictable
- **CI-friendly**: Can run in any environment without GCP access
- **Limited fidelity**: May not catch all integration issues

### Real Service Environment
Uses actual GCP services in a test project for high-fidelity testing. This approach is:

- **High fidelity**: Tests actual service interactions
- **Resource intensive**: Requires GCP resources and incurs costs
- **Slower**: Tests take longer to execute
- **Environment dependent**: Requires GCP access and configuration

### Hybrid Environment
Selectively uses real services for critical components while mocking others. This approach balances:

- **Targeted fidelity**: Real services where most important
- **Reasonable speed**: Mocks for less critical components
- **Flexible resource usage**: Control which services incur costs

The environment is configured through pytest options and environment variables, as described in the "Running Tests" section.

## Test Fixtures

The `conftest.py` file provides fixtures specifically designed for integration testing:

### Environment Fixtures
- `integration_test_environment`: Base test environment configuration
- `gcs_test_environment`: GCS bucket and test files setup
- `bigquery_test_environment`: BigQuery datasets and tables setup
- `vertex_ai_test_environment`: Vertex AI models and endpoints setup

### Component Fixtures
- `real_extraction_orchestrator`: Configured extraction orchestrator
- `real_validation_engine`: Configured validation engine
- `real_self_healing_components`: Self-healing component suite

### Client Fixtures
- `gcs_client`: GCS client (real or mock)
- `bigquery_client`: BigQuery client (real or mock)
- `vertex_ai_client`: Vertex AI client (real or mock)
- `monitoring_client`: Cloud Monitoring client (real or mock)

### Data Fixtures
- `test_validation_data`: Test data for validation scenarios
- `test_healing_data`: Test data for self-healing scenarios
- `test_metric_data`: Test data for monitoring scenarios
- `test_alert_data`: Test data for alerting scenarios

These fixtures handle setup and teardown of test resources, ensuring tests start with a clean environment and clean up after themselves.

## Writing Integration Tests

When writing integration tests, follow these guidelines:

### Test Structure
1. **Arrange**: Set up the test environment and components
2. **Act**: Execute the integrated workflow or operation
3. **Assert**: Verify the expected outcomes across component boundaries
4. **Cleanup**: Ensure all resources are properly cleaned up

### Best Practices
- **Focus on integration points**: Test the interactions between components, not internal component logic
- **Use realistic data**: Test with data that resembles production patterns
- **Test error paths**: Verify error handling and recovery across components
- **Minimize test coupling**: Tests should be independent and not rely on each other's state
- **Control external dependencies**: Use fixtures to manage external services consistently

### Example: Backend Integration Test

```python
@pytest.mark.integration
@pytest.mark.gcp
def test_ingestion_to_quality_flow(gcs_test_environment, real_extraction_orchestrator, real_validation_engine):
    # Arrange: Set up test data in GCS
    test_file_path = gcs_test_environment['test_files']['csv']
    extraction_config = {
        'source_id': 'test-source',
        'source_type': DataSourceType.GCS,
        'file_path': test_file_path,
        'format': FileFormat.CSV
    }
    
    # Act: Execute extraction and pass to validation
    extraction_result = real_extraction_orchestrator.execute_extraction(extraction_config)
    validation_result = real_validation_engine.validate_dataset(
        extraction_result.dataset_ref,
        rule_set_id='basic-rules'
    )
    
    # Assert: Verify end-to-end flow results
    assert extraction_result.status == ExtractionStatus.SUCCESS
    assert extraction_result.record_count > 0
    assert validation_result.status == ValidationStatus.SUCCESS
    assert validation_result.quality_score > 0.9
    
    # Verify metadata was properly tracked
    metadata = real_extraction_orchestrator.metadata_tracker.get_extraction_metadata(
        extraction_result.extraction_id
    )
    assert metadata.source_id == 'test-source'
    assert metadata.validation_id == validation_result.validation_id
```

### Example: Web UI Integration Test

```typescript
describe('Quality Dashboard Integration', () => {
  it('should load quality data and display validation results', async () => {
    // Arrange: Set up mock API responses
    const mockApi = setupApiMocks({
      [endpoints.quality.getDatasetSummaries]: {
        data: mockDatasetSummaries,
        status: 200
      },
      [endpoints.quality.getValidationResults]: {
        data: sampleValidationResults,
        status: 200
      }
    });
    
    // Act: Render the component and interact with it
    render(
      <MockApiProvider client={mockApi}>
        <QualityProvider>
          <QualityDashboard />
        </QualityProvider>
      </MockApiProvider>
    );
    
    // Wait for data to load
    await waitFor(() => {
      expect(screen.getByText('Data Quality Overview')).toBeInTheDocument();
    });
    
    // Select a dataset
    const datasetSelect = screen.getByLabelText('Select Dataset');
    fireEvent.change(datasetSelect, { target: { value: 'customer_data' } });
    
    // Assert: Verify the correct API calls were made
    expect(mockApi.get).toHaveBeenCalledWith(
      endpoints.quality.getValidationResults,
      expect.objectContaining({ params: { datasetId: 'customer_data' } })
    );
    
    // Verify the UI displays the correct data
    await waitFor(() => {
      expect(screen.getByText('98%')).toBeInTheDocument(); // Quality score
      expect(screen.getByText('15')).toBeInTheDocument(); // Issue count
    });
  });
});
```

## Running Tests

### Basic Usage

```bash
# Run all integration tests with mock services
python -m pytest src/test/integration

# Run specific backend integration tests
python -m pytest src/test/integration/backend/test_ingestion_pipeline.py

# Run specific web integration tests
npm test -- src/test/integration/web/test_quality_data_flow.tsx
```

### Environment Configuration

```bash
# Run with real GCP services
python -m pytest src/test/integration --use-real-services

# Specify test project and location
python -m pytest src/test/integration --use-real-services --test-project-id=my-test-project --test-location=us-central1

# Run specific test categories
python -m pytest src/test/integration -m "gcp and not performance"
```

### Test Markers

The following markers are available for selecting specific test categories:

- `integration`: All integration tests (applied automatically)
- `gcp`: Tests that interact with GCP services
- `database`: Tests focused on database interactions
- `api`: Tests focused on API interactions
- `performance`: Performance-focused integration tests
- `web`: Web UI integration tests

Example:
```bash
# Run only database integration tests
python -m pytest src/test/integration -m database
```

## Test Data Management

Integration tests require appropriate test data to validate component interactions. The framework provides several approaches for test data management:

### Static Test Data
Pre-defined test data files are available in the `src/test/mock_data/` directory, organized by component:

- `gcs/`: Sample files for GCS ingestion testing
- `cloudsql/`: SQL scripts for database testing
- `api/`: Sample API requests and responses
- `bigquery/`: Sample schemas and data for BigQuery
- `quality/`: Sample validation rules and results
- `healing/`: Sample issues and corrections
- `monitoring/`: Sample metrics and alerts

### Dynamic Test Data Generation
Test fixtures can generate dynamic test data with specific characteristics:

- `create_test_dataframe()`: Creates pandas DataFrames with configurable schemas
- `generate_test_data_with_issues()`: Generates data with specific quality issues
- `create_test_rule()`: Creates test validation rules

### Test Data Cleanup
All tests should clean up their test data to prevent interference between tests:

- Use fixture finalizers for automatic cleanup
- Use the `cleanup_test_resources()` utility for manual cleanup
- Implement test-specific cleanup in teardown functions

## Mocking Strategy

Integration tests often require controlled test doubles to isolate specific integration points. The framework provides several mocking approaches:

### Service Mocking
For GCP services, the framework provides both high-level and low-level mocks:

- **High-level mocks**: Complete service simulations with realistic behavior
- **Low-level mocks**: Targeted mocks for specific service methods

Example:
```python
# High-level mock for GCS
from src.test.fixtures.backend.ingestion_fixtures import mock_gcs_connector

# Low-level mock for specific GCS operations
with mock.patch('google.cloud.storage.Bucket.blob') as mock_blob:
    mock_blob.return_value.download_as_string.return_value = b'test data'
    # Test code using GCS
```

### API Mocking
For web UI tests, the framework provides API mocking utilities:

```typescript
// Set up mock API responses
const mockApi = setupApiMocks({
  [endpoints.quality.getDatasetSummaries]: {
    data: mockDatasetSummaries,
    status: 200
  }
});

// Use the mock API provider
render(
  <MockApiProvider client={mockApi}>
    <ComponentUnderTest />
  </MockApiProvider>
);
```

### Component Mocking
For testing specific integration points, you can mock adjacent components:

```python
# Mock the self-healing engine for testing integration with quality validation
with mock.patch('src.backend.self_healing.ai.issue_classifier.IssueClassifier') as mock_classifier:
    mock_classifier.return_value.classify_issue.return_value = {
        'issue_type': 'missing_values',
        'confidence': 0.95,
        'recommended_action': 'impute_values'
    }
    # Test code that integrates with the classifier
```

## Troubleshooting

### Common Issues

#### Test Environment Setup Failures
- Verify GCP credentials are properly configured
- Check test project permissions
- Ensure required APIs are enabled in the test project
- Verify network connectivity to GCP services

#### Flaky Tests
- Look for timing issues in asynchronous operations
- Check for resource cleanup failures in previous test runs
- Verify test isolation and independence
- Add additional logging to identify inconsistent behavior

#### Slow Tests
- Consider using mock services instead of real ones
- Optimize test data size
- Use parallel test execution where possible
- Profile tests to identify bottlenecks

### Debugging Tips

- Use the `--verbose` flag for detailed test output
- Add the `-v` flag to show individual test results
- Use `pytest.set_trace()` for interactive debugging
- Add `print` statements or use the logging module for visibility
- Check test logs in the `logs/` directory
- For web tests, use the `screen.debug()` function to view the DOM

## CI/CD Integration

Integration tests are automatically executed as part of the CI/CD pipeline:

### Pull Request Validation
- Critical integration tests are run on every PR
- Tests use mock services by default
- Test results are reported as GitHub status checks

### Main Branch Builds
- All integration tests are run on merges to main
- A subset of tests use real GCP services in a test project
- Test results are published as build artifacts

### Nightly Builds
- Complete integration test suite with real services
- Performance-focused integration tests
- Extended test coverage with larger datasets

The CI configuration is defined in `.github/workflows/ci.yml` and `.github/workflows/quality-checks.yml`.

## Related Documentation

- [Main Testing README](../README.md): Overview of the testing framework
- [Unit Testing Guidelines](../unit/README.md): Guidelines for unit testing
- [End-to-End Testing Framework](../e2e/README.md): Framework for E2E testing
- [Performance Testing Framework](../performance/README.md): Framework for performance testing
- [Test Utilities Documentation](../utils/README.md): Documentation for test utilities
- [Project Testing Strategy](../../docs/development/testing.md): Overall testing strategy