# Self-Healing Data Pipeline Test Scenarios

This directory contains end-to-end test scenarios for validating the functionality of the self-healing data pipeline components. These scenarios are designed to test complete data flows from ingestion through processing, quality validation, self-healing, and final output.

## Test Scenario Organization

The test scenarios are organized into the following categories:

```
scenarios/
├── happy_path/             # Basic functionality with valid data
├── data_quality_issues/    # Scenarios with various data quality problems
├── system_failures/        # Infrastructure and service failure scenarios
├── performance/            # Load and performance testing scenarios
├── self_healing/           # Specific tests for self-healing capabilities
└── compliance/             # Regulatory and policy compliance scenarios
```

Each scenario directory contains:
- `config/` - Configuration files for the scenario
- `data/` - Test data files
- `expected/` - Expected results for validation
- `scenario.yaml` - Scenario definition and execution parameters

## Purpose and Coverage

### Happy Path Scenarios
Validate that the pipeline functions correctly under normal conditions with valid data. These tests establish baseline functionality.

### Data Quality Issue Scenarios
Test the pipeline's ability to detect and respond to various data quality issues:
- Missing data and null values
- Schema violations and type mismatches
- Referential integrity issues
- Anomalous values and patterns
- Duplicate records

### System Failure Scenarios
Evaluate the pipeline's resilience against various infrastructure and service failures:
- Network connectivity issues
- Service unavailability
- Resource exhaustion
- Timeout conditions
- Quota limitations

### Performance Scenarios
Test the pipeline's behavior under different load conditions:
- Large dataset processing
- High concurrency
- Sustained throughput
- Resource scaling
- Recovery from backlog

### Self-Healing Scenarios
Specifically test the self-healing capabilities of the pipeline:
- Automated error correction
- Smart retry logic
- Parameter optimization
- Failure prediction
- Recovery orchestration

### Compliance Scenarios
Validate that the pipeline meets regulatory and policy requirements:
- Data protection controls
- Access controls
- Audit trail completeness
- Retention policy enforcement
- Data classification handling

## Running Test Scenarios

### Prerequisites
- GCP project with necessary permissions
- Configured authentication credentials
- Python 3.9+ environment with required dependencies

### Execution Instructions

1. **Setup test environment:**
   ```bash
   cd src/test
   ./setup_test_env.sh --project=your-gcp-project-id
   ```

2. **Run a specific scenario:**
   ```bash
   python run_scenario.py --scenario=happy_path/basic_ingestion
   ```

3. **Run all scenarios in a category:**
   ```bash
   python run_scenario.py --category=self_healing
   ```

4. **Run with custom parameters:**
   ```bash
   python run_scenario.py --scenario=performance/large_dataset --scale=10 --duration=30m
   ```

### Test Results
Test results are stored in the `results/` directory with the following structure:
- Logs of all operations
- Metrics collected during execution
- Comparison between expected and actual results
- Screenshots of monitoring dashboards (if applicable)

## Creating New Test Scenarios

1. Create a new directory under the appropriate category
2. Create a `scenario.yaml` file defining:
   - Scenario name and description
   - Required resources
   - Setup steps
   - Execution steps
   - Validation criteria
   - Cleanup steps
3. Add necessary test data in the `data/` subdirectory
4. Define expected results in the `expected/` subdirectory
5. Add any custom configuration in the `config/` subdirectory

### Example `scenario.yaml`
```yaml
name: Basic Data Ingestion
description: Tests the basic ingestion of valid data from GCS to BigQuery
resources:
  - gcs_bucket: {name: "test-source-bucket", region: "us-central1"}
  - bigquery_dataset: {name: "test_dataset", location: "US"}

setup:
  - create_resources: {template: "resources.json"}
  - upload_test_data: {source: "data/valid_data.csv", destination: "gs://${gcs_bucket.name}/input/"}

execution:
  - trigger_pipeline: {pipeline: "ingestion_pipeline", params: {source_bucket: "${gcs_bucket.name}"}}
  - wait_for_completion: {timeout: "10m"}

validation:
  - check_table_exists: {dataset: "${bigquery_dataset.name}", table: "output_table"}
  - verify_row_count: {expected: 1000}
  - compare_data: {actual: "${bigquery_dataset.name}.output_table", expected: "expected/output.csv"}

cleanup:
  - delete_resources: {template: "resources.json"}
```

## Test Maintenance Strategy

### Regular Maintenance Tasks
- Review and update test data quarterly to reflect current patterns
- Validate scenarios against new pipeline versions before release
- Update expected results when pipeline behavior changes intentionally
- Archive obsolete scenarios with documentation on why they were retired

### Extending Test Coverage
When adding new pipeline features:
1. Add happy path scenarios first
2. Add failure scenarios to test error handling
3. Add edge case scenarios to test boundary conditions
4. Update existing scenarios if the new feature affects their behavior

### Test Scenario Reviews
All new test scenarios should undergo code review with special attention to:
- Completeness of test coverage
- Realism of test data
- Clarity of validation criteria
- Proper resource cleanup
- Deterministic execution

### Flaky Test Management
If a test scenario is identified as flaky (inconsistent results):
1. Tag it as `@flaky` in the scenario file
2. Create an issue for investigation
3. Consider isolating it from the main test suite
4. Document known conditions that affect reliability

## Best Practices

1. **Realistic Data**: Use anonymized production-like data patterns
2. **Isolation**: Ensure scenarios don't interfere with each other
3. **Idempotence**: Scenarios should be repeatable with consistent results
4. **Self-Contained**: Include all necessary resources and configuration
5. **Cleanup**: Always include thorough cleanup steps
6. **Documentation**: Clearly document the purpose and expected behavior
7. **Deterministic**: Avoid dependencies on external factors when possible
8. **Parameterization**: Make scenarios configurable for different environments
9. **Performance Aware**: Optimize scenarios to run efficiently
10. **Maintainability**: Prefer modular, reusable components

## Troubleshooting Failed Scenarios

1. Check the execution logs in `results/{scenario_name}/logs/`
2. Verify that all prerequisites were met
3. Inspect resource state in the GCP project
4. Look for environmental differences if the scenario works elsewhere
5. Check for recent pipeline changes that might affect the scenario
6. Run with `--debug` flag for more detailed logging

For additional help, contact the Data Engineering team.