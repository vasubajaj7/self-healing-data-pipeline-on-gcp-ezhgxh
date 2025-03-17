# Mock Data Resources

## Overview

This directory contains mock data resources used for testing the self-healing data pipeline. These resources provide consistent, reproducible test data for unit tests, integration tests, and end-to-end testing scenarios.

## Directory Structure

```
mock_data/
├── gcs/                  # Sample files for Google Cloud Storage testing
│   ├── sample_data.csv
│   ├── sample_data.json
│   ├── sample_data.parquet
│   └── sample_data.avro
├── cloudsql/             # Sample database schemas and data
│   ├── sample_schema.sql
│   └── sample_data.sql
├── api/                  # Sample API requests and responses
│   ├── sample_responses.json
│   └── sample_requests.json
├── bigquery/             # Sample BigQuery schemas and data
│   ├── sample_schema.json
│   └── sample_data.json
├── quality/              # Sample quality validation data
│   ├── sample_expectations.json
│   └── sample_validation_results.json
├── healing/              # Sample self-healing test data
│   ├── sample_issues.json
│   └── sample_corrections.json
├── monitoring/           # Sample monitoring and alerting data
│   ├── sample_metrics.json
│   └── sample_alerts.json
└── generator/            # Data generation utilities
    ├── schema_generator.py
    └── data_generator.py
```

## Usage Guidelines

### For Unit Tests

Unit tests should use the smallest possible subset of mock data needed to test specific functionality. Import the required mock data files directly in your test modules:

```python
import json
from pathlib import Path

# Load mock data
MOCK_DATA_DIR = Path(__file__).parent.parent / 'mock_data'
with open(MOCK_DATA_DIR / 'gcs' / 'sample_data.json', 'r') as f:
    sample_data = json.load(f)
```

### For Integration Tests

Integration tests should use more comprehensive datasets that test interactions between components. Use the data generator utilities for creating variations of the base mock data:

```python
from src.test.mock_data.generator.data_generator import generate_test_dataset

# Generate a custom dataset with specific characteristics
test_data = generate_test_dataset(
    size=100,
    include_nulls=True,
    include_anomalies=True
)
```

### For End-to-End Tests

End-to-end tests should use complete datasets that represent realistic data flows. The mock data can be loaded into test environments using the provided scripts:

```bash
# From project root
./src/test/scripts/generate_test_data.sh --target=gcp --size=medium
```

## Data Generation

The `generator` directory contains utilities for generating synthetic test data with specific characteristics:

- `schema_generator.py`: Creates schema definitions for different data sources
- `data_generator.py`: Generates data based on schema definitions with options for:
  - Data volume (small, medium, large)
  - Data quality issues (nulls, duplicates, outliers)
  - Temporal patterns (trends, seasonality, anomalies)
  - Relationship integrity (valid/invalid references)

## Maintenance

### Adding New Mock Data

1. Create appropriate files in the relevant subdirectory
2. Update this README if adding new categories of mock data
3. Ensure all mock data is properly anonymized and contains no sensitive information
4. Add generation logic to the generator utilities if applicable

### Versioning

Mock datasets are versioned alongside the codebase. Major changes to mock data structure should be documented in commit messages and may require updates to tests that depend on specific data patterns.

## Best Practices

1. **Reproducibility**: All randomly generated data should use fixed seeds for reproducibility
2. **Isolation**: Test data should be isolated from production data
3. **Comprehensiveness**: Mock data should cover edge cases and error conditions
4. **Maintainability**: Keep mock data aligned with current schema definitions
5. **Performance**: Consider file size and loading performance for large test datasets