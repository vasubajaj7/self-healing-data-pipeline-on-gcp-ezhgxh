# Test Utilities

This directory contains utility functions and classes to support testing of the self-healing data pipeline components. These utilities are designed to make writing tests easier, more consistent, and more maintainable.

## Available Utilities

### Test Assertions (`test_assertions.py`)

Custom assertion functions that extend the standard Python `unittest` assertions with domain-specific validations:

- `assert_data_quality_matches`: Validates data quality results against expected values
- `assert_pipeline_execution_success`: Verifies pipeline execution completed successfully
- `assert_healing_action_applied`: Confirms self-healing actions were correctly applied
- `assert_bigquery_data_matches`: Compares BigQuery data with expected results

### Test Helpers (`test_helpers.py`)

General helper functions for test setup, teardown, and execution:

- `setup_test_environment`: Prepares the test environment with required configurations
- `teardown_test_environment`: Cleans up resources after tests
- `wait_for_pipeline_completion`: Waits for asynchronous pipeline operations to complete
- `capture_logs`: Captures and returns logs for verification

### Test Data Generators (`test_data_generators.py`)

Functions to generate test data with specific characteristics:

- `generate_sample_data`: Creates sample datasets with configurable properties
- `generate_data_with_errors`: Produces data with intentional errors for testing validation
- `generate_time_series_data`: Creates time-series data for testing anomaly detection
- `generate_schema_drift`: Produces data with schema changes for testing drift detection

### GCP Test Utilities (`gcp_test_utils.py`)

Utilities for interacting with Google Cloud Platform services in tests:

- `create_test_bucket`: Creates a temporary GCS bucket for testing
- `create_test_dataset`: Creates a temporary BigQuery dataset
- `upload_test_file`: Uploads a file to GCS for testing
- `execute_test_query`: Executes a BigQuery query and returns results

### Airflow Test Utilities (`airflow_test_utils.py`)

Utilities for testing Cloud Composer/Airflow components:

- `create_test_dag`: Creates a test DAG instance
- `execute_dag_task`: Executes a specific task in a DAG
- `mock_airflow_context`: Creates a mock Airflow context for testing operators
- `simulate_dag_run`: Simulates a complete DAG run

### BigQuery Test Utilities (`bigquery_test_utils.py`)

Utilities specific to BigQuery testing:

- `create_test_table`: Creates a temporary table for testing
- `load_test_data`: Loads test data into a BigQuery table
- `compare_query_results`: Compares results of two queries
- `verify_table_schema`: Verifies a table's schema matches expectations

### API Test Utilities (`api_test_utils.py`)

Utilities for testing API endpoints:

- `mock_api_response`: Creates mock API responses
- `simulate_api_request`: Simulates API requests
- `validate_api_response`: Validates API response structure and content

### Web Test Utilities (`web_test_utils.ts`)

Utilities for testing web components:

- `renderWithProviders`: Renders React components with necessary providers
- `mockApiCalls`: Mocks API calls for component testing
- `simulateUserInteraction`: Simulates user interactions with components

### Test Cleanup (`test_cleanup.py`)

Utilities for ensuring proper cleanup after tests:

- `register_resource_for_cleanup`: Registers a resource to be cleaned up
- `cleanup_all_resources`: Cleans up all registered resources
- `cleanup_temporary_buckets`: Removes temporary GCS buckets
- `cleanup_temporary_datasets`: Removes temporary BigQuery datasets

### Mock Utilities (`mocks.py` and `mock_services.py`)

Utilities for creating mock objects and services:

- `mock_gcs_client`: Creates a mock GCS client
- `mock_bigquery_client`: Creates a mock BigQuery client
- `mock_composer_environment`: Creates a mock Cloud Composer environment
- `mock_vertex_ai_model`: Creates a mock Vertex AI model

## Usage Examples

### Setting Up a Test Environment

```python
from test.utils.test_helpers import setup_test_environment, teardown_test_environment
from test.utils.gcp_test_utils import create_test_bucket, create_test_dataset

def setUp():
    # Set up the test environment
    self.env = setup_test_environment()
    self.test_bucket = create_test_bucket(prefix="test-ingestion-")
    self.test_dataset = create_test_dataset(prefix="test_quality_")

def tearDown():
    # Clean up resources
    teardown_test_environment(self.env)
```

### Generating Test Data

```python
from test.utils.test_data_generators import generate_sample_data, generate_data_with_errors

# Generate clean sample data
clean_data = generate_sample_data(rows=100, include_nulls=False)

# Generate data with specific errors for quality testing
error_data = generate_data_with_errors(
    rows=100, 
    error_types=["missing_values", "type_mismatch", "outliers"],
    error_percentage=0.15
)
```

### Testing Data Quality Validation

```python
from test.utils.test_assertions import assert_data_quality_matches
from test.utils.bigquery_test_utils import load_test_data

# Load test data with known quality issues
table_id = load_test_data(self.test_dataset, error_data)

# Run quality validation
validation_results = quality_service.validate_data(self.test_dataset, table_id)

# Assert quality results match expectations
assert_data_quality_matches(
    validation_results,
    expected_failures=["missing_values", "type_mismatch", "outliers"],
    expected_pass_rate=0.85
)
```

### Testing Self-Healing Functionality

```python
from test.utils.test_assertions import assert_healing_action_applied
from test.utils.test_helpers import wait_for_pipeline_completion

# Trigger self-healing process
healing_job = self_healing_service.heal_data_issues(validation_results)

# Wait for healing process to complete
wait_for_pipeline_completion(healing_job)

# Verify healing actions were correctly applied
assert_healing_action_applied(
    healing_job,
    expected_actions=["impute_missing_values", "fix_data_types", "remove_outliers"],
    expected_success_rate=0.9
)
```

## Contributing

When adding new test utilities:

1. Follow the existing patterns and naming conventions
2. Add comprehensive docstrings explaining parameters and return values
3. Include usage examples in docstrings
4. Update this README with information about the new utility
5. Write tests for the utility itself in the appropriate test directory

## Best Practices

- Use these utilities to avoid duplicating test code
- Prefer higher-level utilities that encapsulate multiple steps when possible
- Always clean up test resources to avoid orphaned cloud resources
- Use descriptive prefixes for test resources to make them easily identifiable
- Leverage the mock utilities to avoid hitting actual cloud services in unit tests