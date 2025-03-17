# Performance Testing Framework

This directory contains the performance testing framework for the self-healing data pipeline. The framework is designed to validate performance characteristics across all pipeline components, including data ingestion, quality validation, self-healing mechanisms, BigQuery operations, and API endpoints.

## Testing Objectives

The performance testing framework aims to validate the following aspects of the pipeline:

- Throughput: Measure data processing rates under various load conditions
- Latency: Validate response times for critical operations
- Resource Utilization: Monitor CPU, memory, and storage usage
- Scalability: Verify linear scaling with increasing data volumes
- Reliability: Ensure consistent performance over extended periods

## Performance Requirements

The pipeline must meet the following key performance requirements:

- Data Ingestion: Process files up to 10GB with <30 min processing time
- Data Processing: Complete processing for 10GB dataset in <30 min
- BigQuery Queries: Perform within 120% of baseline
- API Endpoints: 95th percentile response time <500ms for most endpoints
- Self-Healing: Response time <5 min for issue detection

## Testing Tools

The performance testing framework leverages multiple tools to provide comprehensive coverage:

### Pytest

Used for component-level performance testing with custom fixtures and utilities in the `backend/` directory. Pytest tests focus on isolated component performance with precise measurements.

### k6

Used for API load testing in the `k6/` directory. k6 tests simulate realistic user behavior and measure API performance under various load conditions including:

- Constant load: Fixed number of virtual users
- Ramp-up tests: Gradually increasing load
- Stress tests: Pushing the system to its limits
- Endurance tests: Sustained load over extended periods

### Locust

Used for distributed load testing in the `locust/` directory. Locust provides a web UI for real-time monitoring and supports distributed test execution across multiple machines for high-volume testing.

### JMeter

Used for specialized BigQuery performance testing in the `jmeter/` directory. JMeter tests focus on query performance, optimization effectiveness, and resource utilization.

## Directory Structure

The performance testing framework is organized as follows:

```
performance/
├── README.md                 # This file
├── conftest.py               # Pytest configuration for performance tests
├── backend/                  # Component-level performance tests
│   ├── test_data_ingestion_perf.py
│   ├── test_bigquery_query_perf.py
│   ├── test_self_healing_response_time.py
│   └── test_api_load.py
├── k6/                       # k6 load testing scripts
│   ├── scripts/
│   │   ├── common.js         # Common utilities for k6 tests
│   │   ├── api_load_test.js  # API load testing
│   │   ├── ingestion_load_test.js
│   │   └── healing_load_test.js
│   └── results/              # k6 test results
├── locust/                   # Locust distributed load testing
│   ├── locustfile.py         # Main Locust configuration
│   ├── tasks/                # Task definitions for user behavior
│   │   ├── ingestion_tasks.py
│   │   ├── quality_tasks.py
│   │   ├── healing_tasks.py
│   │   └── api_tasks.py
│   └── results/              # Locust test results
└── jmeter/                   # JMeter test plans
    ├── test_plans/
    │   ├── ingestion_test_plan.jmx
    │   ├── api_test_plan.jmx
    │   └── bigquery_test_plan.jmx
    └── results/              # JMeter test results
```

## Test Data

Performance tests use generated test data of various sizes to simulate realistic workloads:

- Small: 1,000 records
- Medium: 10,000 records
- Large: 100,000 records

Test data is generated in multiple formats (CSV, JSON, Parquet, Avro) to validate format-specific performance characteristics. Reference test data is available in the `src/test/mock_data/` directory.

## Performance Metrics

The framework collects and analyzes the following performance metrics:

- **Execution Time**: Duration of operations in milliseconds
- **Throughput**: Records processed per second
- **Resource Utilization**: CPU, memory, and storage usage
- **Latency Percentiles**: p50, p90, p95, p99 response times
- **Error Rates**: Percentage of failed operations
- **Scalability Ratio**: Performance change relative to data volume increase

Metrics are collected using the `PerformanceMetricsCollector` class and analyzed against defined thresholds.

## Running Performance Tests

Performance tests can be executed using the following methods:

### Component-Level Tests

```bash
# Run all performance tests
pytest src/test/performance/backend/

# Run specific performance test
pytest src/test/performance/backend/test_data_ingestion_perf.py

# Run with specific data size
pytest src/test/performance/backend/ --test-data-size=medium

# Run with custom iterations
pytest src/test/performance/backend/ --test-iterations=10
```

### k6 Load Tests

```bash
# Run API load test
k6 run src/test/performance/k6/scripts/api_load_test.js

# Run with environment variables
k6 run --env BASE_URL=http://localhost:8000 src/test/performance/k6/scripts/api_load_test.js

# Run with specific scenario
k6 run --env SCENARIO=stress_test src/test/performance/k6/scripts/api_load_test.js
```

### Locust Tests

```bash
# Start Locust web UI
locust -f src/test/performance/locust/locustfile.py --host=http://localhost:8000

# Run headless with specified users
locust -f src/test/performance/locust/locustfile.py --host=http://localhost:8000 --users=20 --spawn-rate=2 --run-time=5m --headless

# Run distributed across multiple machines
locust -f src/test/performance/locust/locustfile.py --master
locust -f src/test/performance/locust/locustfile.py --worker --master-host=<master-hostname>
```

### JMeter Tests

```bash
# Run JMeter test plan in GUI mode
jmeter -t src/test/performance/jmeter/test_plans/bigquery_test_plan.jmx

# Run JMeter test plan in non-GUI mode
jmeter -n -t src/test/performance/jmeter/test_plans/bigquery_test_plan.jmx -l results.jtl

# Run with custom properties
jmeter -n -t src/test/performance/jmeter/test_plans/bigquery_test_plan.jmx -Jproject_id=test-project -Jdataset_id=test_dataset -l results.jtl
```

## Test Environments

Performance tests can be executed in different environments:

- **Local**: For development and initial validation
- **Test GCP Project**: For isolated testing with controlled resources
- **Staging**: For pre-production validation with production-like data volumes
- **Production**: For baseline measurements and regression testing (limited scope)

Environment-specific configuration is managed through environment variables or configuration files.

## Performance Thresholds

Performance thresholds are defined in `conftest.py` and categorized by component and data size. Tests validate that measured performance meets or exceeds these thresholds.

Example thresholds:

```python
PERFORMANCE_THRESHOLDS = {
    "ingestion": {
        "small": 1000,  # ms
        "medium": 5000,  # ms
        "large": 30000  # ms
    },
    "quality": {
        "small": 500,  # ms
        "medium": 2000,  # ms
        "large": 10000  # ms
    },
    # Additional thresholds for other components
}
```

Thresholds can be adjusted using command-line options or environment variables for specific test runs.

## Reporting

Performance test results are reported in multiple formats:

- **Console Output**: Summary statistics during test execution
- **JSON Reports**: Detailed metrics and analysis
- **CSV Exports**: For further analysis in spreadsheets or BI tools
- **HTML Reports**: Visual representation of test results
- **Dashboard Integration**: Real-time metrics in monitoring dashboards

Reports include statistical analysis (min, max, avg, median, percentiles) and comparison against defined thresholds.

## Continuous Integration

Performance tests are integrated into the CI/CD pipeline with the following approach:

- **PR Validation**: Basic performance tests on pull requests
- **Nightly Builds**: Comprehensive performance test suite
- **Release Validation**: Full performance testing before production deployment
- **Scheduled Baseline**: Regular baseline measurements for trend analysis

Performance regression beyond defined thresholds will trigger build failures and notifications.

## Best Practices

When writing or executing performance tests:

1. **Isolation**: Ensure tests run in isolation to prevent interference
2. **Warm-up**: Include warm-up periods before measurement to avoid cold-start penalties
3. **Statistical Validity**: Run multiple iterations to ensure statistical significance
4. **Resource Monitoring**: Always monitor resource utilization alongside performance metrics
5. **Realistic Data**: Use representative data volumes and patterns
6. **Consistent Environment**: Control for environmental variables that might affect results
7. **Documentation**: Document test conditions and configuration for reproducibility

## Extending the Framework

To add new performance tests:

1. For component-level tests, add a new test file in the `backend/` directory
2. For API load tests, add a new script in the `k6/scripts/` directory
3. For user behavior tests, add new task classes in the `locust/tasks/` directory
4. For specialized tests, create a new JMeter test plan in the `jmeter/test_plans/` directory

Ensure all new tests follow the established patterns and integrate with the metrics collection framework.

## Troubleshooting

Common issues and solutions:

- **Inconsistent Results**: Increase test iterations and check for environmental interference
- **Resource Limitations**: Adjust test parameters or increase available resources
- **Timeout Errors**: Adjust timeout settings or optimize the component being tested
- **Connection Failures**: Verify network configuration and service availability
- **Data Size Issues**: Use the `--test-data-size` parameter to adjust test data volume

## References

- [Pytest Documentation](https://docs.pytest.org/)
- [k6 Documentation](https://k6.io/docs/)
- [Locust Documentation](https://docs.locust.io/)
- [JMeter Documentation](https://jmeter.apache.org/usermanual/index.html)
- [Google Cloud Performance Testing Best Practices](https://cloud.google.com/architecture/framework/performance-optimization/testing)