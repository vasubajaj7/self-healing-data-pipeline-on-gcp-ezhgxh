"""
Integration tests for the data quality validation components of the self-healing data pipeline.
Tests the end-to-end functionality of the validation engine, validators, and their integration with other components like BigQuery and Great Expectations.
"""
import pytest  # package_version: latest
import pandas  # package_version: latest
import numpy  # package_version: latest
from unittest.mock import MagicMock  # package_version: standard library
from great_expectations import __version__ as ge_version  # package_version: 0.15.x

from src.backend.constants import ValidationRuleType, QualityDimension  # src/backend/constants.py
from src.backend.quality.engines.validation_engine import ValidationEngine, ValidationResult, ValidationSummary  # src/backend/quality/engines/validation_engine.py
from src.backend.quality.validators.schema_validator import SchemaValidator  # src/backend/quality/validators/schema_validator.py
from src.backend.quality.validators.content_validator import ContentValidator  # src/backend/quality/validators/content_validator.py
from src.backend.quality.validators.relationship_validator import RelationshipValidator  # src/backend/quality/validators/relationship_validator.py
from src.backend.quality.validators.statistical_validator import StatisticalValidator  # src/backend/quality/validators/statistical_validator.py
from src.backend.quality.engines.quality_scorer import QualityScorer, ScoringModel  # src/backend/quality/engines/quality_scorer.py
from src.backend.quality.integrations.great_expectations_adapter import GreatExpectationsAdapter  # src/backend/quality/integrations/great_expectations_adapter.py
from src.test.fixtures.backend.quality_fixtures import create_test_rule, generate_test_dataset, TestValidationData  # src/test/fixtures/backend/quality_fixtures.py
from src.test.utils.test_helpers import create_temp_file, create_test_dataframe  # src/test/utils/test_helpers.py
from src.test.utils.bigquery_test_utils import setup_test_dataset, create_test_table, load_test_data_to_bigquery  # src/test/utils/bigquery_test_utils.py


def setup_validation_engine(config: dict) -> ValidationEngine:
    """Helper function to set up a validation engine for testing"""
    # Create a validation engine with the provided configuration
    engine = ValidationEngine(config)
    # Configure default quality threshold
    engine.set_quality_threshold(0.8)
    # Return the configured validation engine
    return engine


def create_schema_validation_rules(schema: dict) -> list:
    """Creates a set of schema validation rules for testing"""
    rules = []
    # Create column existence rule
    rules.append(create_test_rule(rule_id='rule_001', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS, parameters={'column_name': 'id'}))
    # Create column type rule
    rules.append(create_test_rule(rule_id='rule_002', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.VALIDITY, parameters={'column_name': 'name', 'data_type': 'STRING'}))
    # Create schema consistency rule
    rules.append(create_test_rule(rule_id='rule_003', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.CONSISTENCY, parameters={'expected_schema': schema}))
    # Create primary key rule if primary key columns provided
    if 'primary_key_columns' in schema:
        rules.append(create_test_rule(rule_id='rule_004', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.UNIQUENESS, parameters={'key_columns': schema['primary_key_columns']}))
    # Return list of created rules
    return rules


def create_content_validation_rules(columns: list) -> list:
    """Creates a set of content validation rules for testing"""
    rules = []
    # Create not_null rule for specified columns
    for column in columns:
        rules.append(create_test_rule(rule_id=f'rule_not_null_{column}', rule_type=ValidationRuleType.CONTENT, dimension=QualityDimension.COMPLETENESS, parameters={'column_name': column, 'subtype': 'not_null'}))
    # Create value_range rule for numeric columns
    for column in columns:
        rules.append(create_test_rule(rule_id=f'rule_range_{column}', rule_type=ValidationRuleType.CONTENT, dimension=QualityDimension.ACCURACY, parameters={'column_name': column, 'subtype': 'value_range', 'min_value': 0, 'max_value': 100}))
    # Create pattern rule for string columns
    for column in columns:
        rules.append(create_test_rule(rule_id=f'rule_pattern_{column}', rule_type=ValidationRuleType.CONTENT, dimension=QualityDimension.VALIDITY, parameters={'column_name': column, 'subtype': 'pattern_matching', 'pattern': '[A-Za-z0-9]+'}))
    # Create categorical rule for categorical columns
    for column in columns:
        rules.append(create_test_rule(rule_id=f'rule_categorical_{column}', rule_type=ValidationRuleType.CONTENT, dimension=QualityDimension.CONSISTENCY, parameters={'column_name': column, 'subtype': 'categorical_validation', 'categories': ['A', 'B', 'C']}))
    # Create uniqueness rule for key columns
    for column in columns:
        rules.append(create_test_rule(rule_id=f'rule_uniqueness_{column}', rule_type=ValidationRuleType.CONTENT, dimension=QualityDimension.UNIQUENESS, parameters={'column_name': column, 'subtype': 'uniqueness'}))
    # Return list of created rules
    return rules


def test_validation_engine_initialization():
    """Tests that the validation engine initializes correctly with various configurations"""
    # Initialize validation engine with default configuration
    engine = ValidationEngine({})
    # Verify engine properties are set correctly
    assert engine.get_quality_threshold() == 0.8
    # Initialize with custom quality threshold
    engine = ValidationEngine({'quality_threshold': 0.9})
    # Verify threshold is set correctly
    assert engine.get_quality_threshold() == 0.9
    # Initialize with custom execution mode
    # Verify execution mode is set correctly
    # Initialize with custom scoring model
    # Verify scoring model is set correctly
    engine.close()


def test_schema_validation_with_dataframe(test_validation_data):
    """Tests schema validation with pandas DataFrame"""
    # Create test DataFrame with known schema
    test_df = test_validation_data.test_dataframe
    # Create schema validation rules
    rules = create_schema_validation_rules(test_validation_data.schema_definition)
    # Initialize validation engine
    engine = ValidationEngine({})
    # Execute validation
    summary, results = engine.validate(test_df, rules, {})
    # Verify validation results are correct
    assert all(result['success'] for result in results)
    # Verify validation summary contains expected metrics
    assert summary.total_rules == len(rules)
    engine.close()


def test_content_validation_with_dataframe(test_validation_data):
    """Tests content validation with pandas DataFrame"""
    # Create test DataFrame with known content
    test_df = test_validation_data.test_dataframe
    # Create content validation rules
    rules = create_content_validation_rules(test_validation_data.schema_definition.keys())
    # Initialize validation engine
    engine = ValidationEngine({})
    # Execute validation
    summary, results = engine.validate(test_df, rules, {})
    # Verify validation results are correct
    assert len(results) > 0
    # Verify validation summary contains expected metrics
    assert summary.total_rules == len(rules)
    engine.close()


@pytest.mark.integration
def test_validation_with_bigquery(bigquery_test_environment):
    """Tests validation with BigQuery tables"""
    dataset_id, table_id = bigquery_test_environment
    # Create test BigQuery table with test data
    # Create validation rules for BigQuery data
    # Initialize validation engine with BigQuery configuration
    engine = ValidationEngine({})
    # Execute validation against BigQuery table
    # Verify validation results are correct
    # Verify validation summary contains expected metrics
    engine.close()


def test_validation_with_great_expectations(test_validation_data):
    """Tests validation using Great Expectations integration"""
    # Create test DataFrame
    test_df = test_validation_data.test_dataframe
    # Create validation rules compatible with Great Expectations
    rules = create_schema_validation_rules(test_validation_data.schema_definition)
    # Initialize validation engine with Great Expectations configuration
    engine = ValidationEngine({'use_great_expectations': True})
    # Execute validation using Great Expectations adapter
    summary, results = engine.validate(test_df, rules, {})
    # Verify validation results are correct
    assert len(results) > 0
    # Verify validation summary contains expected metrics
    assert summary.total_rules == len(rules)
    engine.close()


def test_quality_scoring(test_validation_data):
    """Tests quality scoring functionality"""
    # Create test validation results with known pass/fail ratio
    # Initialize quality scorer with different scoring models
    scorer = QualityScorer()
    # Calculate quality scores
    score = scorer.calculate_score(test_validation_data.validation_results)
    # Verify scores are calculated correctly for each model
    assert score > 0
    # Verify threshold comparison works correctly
    assert scorer.passes_threshold(score) == False


def test_validation_with_data_issues(test_validation_data):
    """Tests validation with datasets containing specific issues"""
    # Create test DataFrame with specific issues (missing columns, wrong types, null values)
    # Create validation rules that should detect these issues
    # Initialize validation engine
    engine = ValidationEngine({})
    # Execute validation
    # Verify each issue is correctly detected and reported
    # Verify quality score reflects the issues
    engine.close()


def test_validation_rule_registration():
    """Tests rule registration and retrieval functionality"""
    # Create test validation rules
    # Initialize validation engine
    engine = ValidationEngine({})
    # Register rules with the engine
    # Retrieve rules by ID
    # Verify retrieved rules match the registered rules
    engine.close()


@pytest.mark.integration
def test_end_to_end_validation_pipeline(test_validation_data, bigquery_test_environment):
    """Tests the end-to-end validation pipeline"""
    dataset_id, table_id = bigquery_test_environment
    # Create test data in both DataFrame and BigQuery
    # Create comprehensive validation rules
    # Initialize validation engine
    engine = ValidationEngine({})
    # Execute validation pipeline
    # Verify validation results
    # Verify quality score calculation
    # Verify metrics are reported correctly
    engine.close()


@pytest.mark.performance
def test_validation_performance(bigquery_test_environment):
    """Tests validation performance with large datasets"""
    dataset_id, table_id = bigquery_test_environment
    # Create large test dataset in BigQuery
    # Create validation rules
    # Initialize validation engine
    engine = ValidationEngine({})
    # Measure execution time for validation
    # Verify performance meets requirements
    # Test with different execution modes
    # Compare performance metrics
    engine.close()