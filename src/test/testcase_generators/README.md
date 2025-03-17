# Test Case Generators

This directory contains specialized test case generators that create comprehensive test scenarios for different components of the self-healing data pipeline. These generators automate the creation of test data, schemas, quality issues, and expected outcomes to facilitate thorough testing of the pipeline's functionality.

## Available Generators

| Generator | Description |
|-----------|-------------|
| `schema_data_generator.py` | Base generator for creating schema and data pairs with various configurations. Provides foundational functionality used by other specialized generators. |
| `quality_testcase_generator.py` | Specialized generator for data quality validation testing. Creates test cases with various quality issues, validation rules, and expected outcomes. |
| `healing_testcase_generator.py` | Specialized generator for self-healing AI testing. Creates test cases with corrupted data, expected corrections, and healing configurations. |
| `monitoring_testcase_generator.py` | Specialized generator for monitoring and alerting testing. Creates test cases with anomalies, expected alerts, and monitoring configurations. |
| `optimization_testcase_generator.py` | Specialized generator for performance optimization testing. Creates test cases with query patterns, optimization opportunities, and expected improvements. |

## Common Features

All test case generators provide:

- Configurable schema and data generation
- Controlled injection of specific issues or anomalies
- Generation of expected outcomes for validation
- File-based persistence of test cases
- Comprehensive test suite generation
- Loading of previously saved test cases

## Usage Examples

### Basic Schema and Data Generation

```python
from src.test.testcase_generators.schema_data_generator import TestCaseGenerator

# Initialize generator
generator = TestCaseGenerator()

# Generate a basic test case
test_case = generator.generate_basic_test_case(
    schema_config={
        'num_columns': 5,
        'include_types': ['string', 'integer', 'float', 'boolean', 'timestamp']
    },
    data_config={
        'num_rows': 100,
        'null_percentage': 0.05
    },
    test_case_name='basic_test',
    save_files=True
)

# Access generated schema and data
schema = test_case['schema']
data = test_case['data']
```

### Quality Validation Test Case

```python
from src.test.testcase_generators.quality_testcase_generator import QualityTestCaseGenerator
from src.backend.constants import ValidationRuleType

# Initialize generator
generator = QualityTestCaseGenerator()

# Generate a test case with content quality issues
test_case = generator.generate_content_validation_test_case(
    schema_config={
        'num_columns': 5,
        'include_types': ['string', 'integer', 'float']
    },
    data_config={
        'num_rows': 100
    },
    content_issues=[
        {'type': ValidationRuleType.CONTENT, 'subtype': 'null_values', 'columns': ['col1', 'col2'], 'percentage': 0.2},
        {'type': ValidationRuleType.CONTENT, 'subtype': 'out_of_range', 'column': 'col3', 'min_value': 0, 'max_value': 100}
    ],
    test_case_name='content_quality_test',
    save_files=True
)

# Access test case components
schema = test_case['schema']
data = test_case['data']
validation_rules = test_case['validation_rules']
expected_results = test_case['expected_results']
```

### Self-Healing Test Case

```python
from src.test.testcase_generators.healing_testcase_generator import HealingTestCaseGenerator
from src.backend.constants import ValidationRuleType

# Initialize generator
generator = HealingTestCaseGenerator()

# Generate a test case for testing schema healing
test_case = generator.generate_schema_healing_test_case(
    schema_config={
        'num_columns': 5,
        'include_types': ['string', 'integer', 'float']
    },
    data_config={
        'num_rows': 100
    },
    schema_issues=[
        {'type': ValidationRuleType.SCHEMA, 'subtype': 'missing_column', 'column': 'col1'},
        {'type': ValidationRuleType.SCHEMA, 'subtype': 'type_mismatch', 'column': 'col2', 'expected_type': 'integer', 'actual_type': 'string'}
    ],
    test_case_name='schema_healing_test',
    save_files=True
)

# Access test case components
schema = test_case['schema']
original_data = test_case['original_data']
corrupted_data = test_case['corrupted_data']
expected_corrected_data = test_case['expected_corrected_data']
expected_corrections = test_case['expected_corrections']
```

### Comprehensive Test Suite Generation

```python
from src.test.testcase_generators.schema_data_generator import TestCaseGenerator

# Initialize generator
generator = TestCaseGenerator()

# Generate a comprehensive test suite
test_suite = generator.generate_comprehensive_test_suite(
    suite_config={
        'basic_cases': 5,
        'schema_evolution_cases': 3,
        'data_quality_cases': 5,
        'self_healing_cases': 5,
        'schema_config': {
            'num_columns': 5,
            'include_types': ['string', 'integer', 'float', 'boolean', 'timestamp']
        },
        'data_config': {
            'num_rows': 100,
            'null_percentage': 0.05
        }
    },
    suite_name='comprehensive_test_suite',
    save_files=True
)

# Access test suite components
basic_cases = test_suite['basic_cases']
schema_evolution_cases = test_suite['schema_evolution_cases']
data_quality_cases = test_suite['data_quality_cases']
self_healing_cases = test_suite['self_healing_cases']
```

## Test Case Directory Structure

Test cases are saved in the `src/test/mock_data/testcases` directory by default, with specialized subdirectories for different types of test cases:

```
src/test/mock_data/
├── testcases/       # Base test cases
├── quality/         # Quality validation test cases
├── healing/         # Self-healing test cases
├── monitoring/      # Monitoring test cases
└── optimization/    # Optimization test cases
```

Each test case typically includes:
- Schema definition (JSON)
- Data files (CSV, JSON, Parquet, or Avro)
- Expected results
- Test case metadata
- Test suite manifests (for comprehensive test suites)

## Extending the Generators

To extend the existing generators or create new ones:

1. Inherit from the base `TestCaseGenerator` class in `schema_data_generator.py`
2. Implement specialized generation methods for your test scenarios
3. Create appropriate test case classes to represent your test data
4. Implement save/load functionality for persistence
5. Add comprehensive documentation and examples

See the existing generators for implementation patterns and best practices.

## Best Practices

- Use descriptive test case names that indicate the purpose of the test
- Generate test cases with varying complexity and edge cases
- Save generated test cases for reproducibility and regression testing
- Use comprehensive test suites for thorough component testing
- Combine multiple issue types to test interaction effects
- Include both healable and non-healable issues in self-healing tests
- Document any custom test case configurations for future reference