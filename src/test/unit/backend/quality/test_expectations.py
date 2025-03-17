"""
Unit tests for the expectations functionality in the data quality validation component.
Tests the creation, management, and execution of Great Expectations-based validation rules and expectations for ensuring data quality in the self-healing pipeline.
"""
import pytest  # package_version: 7.3.1
from unittest import mock  # package_version: standard library
import os  # package_version: standard library
import tempfile  # package_version: standard library
import json  # package_version: standard library
import pandas as pd  # package_version: 2.0.x
from great_expectations.core import DataContext  # version 0.15.x

from src.backend.quality.expectations.expectation_manager import ExpectationManager, map_rule_to_expectation, map_expectation_to_rule, create_data_context  # src/backend/quality/expectations/expectation_manager.py
from src.backend.quality.expectations.expectation_suite_builder import ExpectationSuiteBuilder, generate_suite_name, validate_suite_name  # src/backend/quality/expectations/expectation_suite_builder.py
from src.backend.quality.expectations.custom_expectations import register_custom_expectations  # src/backend/quality/expectations/custom_expectations.py
from src.backend.constants import ValidationRuleType, QualityDimension  # src/backend/constants.py
from src.test.fixtures.backend.quality_fixtures import create_test_rule, create_test_validation_result, TestValidationData  # src/test/fixtures/backend/quality_fixtures.py
from src.test.utils.test_helpers import create_temp_file, create_temp_directory, create_test_dataframe  # src/test/utils/test_helpers.py

TEST_CONTEXT_ROOT_DIR = tempfile.mkdtemp(prefix='ge_test_')
SAMPLE_RULES = "[{'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'columns': ['id', 'name', 'value']}}, {'rule_id': 'rule_002', 'rule_type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column': 'value', 'min_value': 0, 'max_value': 100}}]"

def setup_module():
    """Setup function that runs before all tests in the module"""
    # Create a temporary directory for Great Expectations context
    global TEST_CONTEXT_ROOT_DIR
    TEST_CONTEXT_ROOT_DIR = tempfile.mkdtemp(prefix='ge_test_')
    # Register custom expectations
    register_custom_expectations()

def teardown_module():
    """Teardown function that runs after all tests in the module"""
    # Clean up temporary files and directories
    global TEST_CONTEXT_ROOT_DIR
    if os.path.exists(TEST_CONTEXT_ROOT_DIR):
        shutil.rmtree(TEST_CONTEXT_ROOT_DIR)

def create_test_expectation_manager(config: dict = None) -> ExpectationManager:
    """Creates a test ExpectationManager instance

    Args:
        config (dict): config

    Returns:
        ExpectationManager: Configured ExpectationManager instance
    """
    # Create a default configuration if none provided
    if config is None:
        config = {}
    # Set context_root_dir to TEST_CONTEXT_ROOT_DIR
    config['context_root_dir'] = TEST_CONTEXT_ROOT_DIR
    # Create an ExpectationManager with the configuration
    manager = ExpectationManager(config=config)
    # Initialize the manager
    manager.initialize()
    # Return the initialized manager
    return manager

def create_test_expectation_suite_builder(config: dict = None) -> ExpectationSuiteBuilder:
    """Creates a test ExpectationSuiteBuilder instance

    Args:
        config (dict): config

    Returns:
        ExpectationSuiteBuilder: Configured ExpectationSuiteBuilder instance
    """
    # Create a default configuration if none provided
    if config is None:
        config = {}
    # Set context_root_dir to TEST_CONTEXT_ROOT_DIR
    config['context_root_dir'] = TEST_CONTEXT_ROOT_DIR
    # Create an ExpectationSuiteBuilder with the configuration
    builder = ExpectationSuiteBuilder(config=config)
    # Initialize the builder
    builder.initialize()
    # Return the initialized builder
    return builder

def create_sample_dataframe() -> pd.DataFrame:
    """Creates a sample pandas DataFrame for testing expectations

    Returns:
        pandas.DataFrame: Sample DataFrame for testing
    """
    # Define column specifications with appropriate data types
    columns_spec = {
        'id': {'type': 'int', 'min': 1, 'max': 1000},
        'name': {'type': 'str', 'length': 10},
        'score': {'type': 'float', 'min': 0, 'max': 100},
        'active': {'type': 'bool'},
        'created_at': {'type': 'datetime', 'start': '2020-01-01', 'end': '2023-01-01'},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
    }
    # Create a DataFrame using create_test_dataframe helper
    df = create_test_dataframe(columns_spec, num_rows=100)
    # Return the created DataFrame
    return df

@pytest.mark.unit
def test_create_data_context():
    """Tests the creation of a Great Expectations data context"""
    # Create a temporary directory for the context
    temp_dir = tempfile.mkdtemp()
    # Call create_data_context with the directory
    context = create_data_context(temp_dir)
    # Assert that the returned object is a DataContext
    assert isinstance(context, DataContext)
    # Assert that the context has expected properties and methods
    assert hasattr(context, 'add_datasource')
    assert hasattr(context, 'get_expectation_suite')
    # Clean up the temporary directory
    shutil.rmtree(temp_dir)

@pytest.mark.unit
def test_expectation_manager_initialization():
    """Tests the initialization of the ExpectationManager"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Assert that initialization is successful
    assert manager is not None
    # Assert that the manager has expected properties and methods
    assert hasattr(manager, 'create_suite')
    assert hasattr(manager, 'get_suite')
    # Assert that the context is properly created
    assert isinstance(manager._context, DataContext)

@pytest.mark.unit
def test_expectation_suite_creation():
    """Tests the creation of expectation suites"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a suite with a specific name
    suite_name = "test_suite"
    suite = manager.create_suite(suite_name)
    # Assert that the suite is created successfully
    assert suite is not None
    # Assert that the suite has the expected name and properties
    assert suite.expectation_suite_name == suite_name
    assert hasattr(suite, 'expectations')
    # Create another suite with overwrite_existing=True
    new_suite = manager.create_suite(suite_name, overwrite_existing=True)
    # Assert that the suite is overwritten successfully
    assert new_suite is not None

@pytest.mark.unit
def test_map_rule_to_expectation():
    """Tests mapping validation rules to Great Expectations expectations"""
    # Create test rules for different validation types
    schema_rule = {'rule_id': 'schema_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}}
    content_rule = {'rule_id': 'content_001', 'rule_type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column': 'value', 'min_value': 0}}
    # Map each rule to an expectation using map_rule_to_expectation
    schema_expectation = map_rule_to_expectation(schema_rule)
    content_expectation = map_rule_to_expectation(content_rule)
    # Assert that the mapping produces valid expectation configurations
    assert isinstance(schema_expectation, dict)
    assert isinstance(content_expectation, dict)
    # Assert that the expectation type matches the rule type
    assert schema_expectation['expectation_type'] == 'expect_column_to_exist'
    assert content_expectation['expectation_type'] == 'expect_column_values_to_be_between'
    # Assert that the expectation parameters match the rule parameters
    assert schema_expectation['kwargs']['column'] == 'id'
    assert content_expectation['kwargs']['column'] == 'value'
    assert content_expectation['kwargs']['min_value'] == 0

@pytest.mark.unit
def test_map_expectation_to_rule():
    """Tests mapping Great Expectations validation results back to rule results"""
    # Create test rules and expectations
    rule = {'rule_id': 'test_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    expectation = {'success': True, 'result': {'observed_value': 'column_exists'}}
    # Create sample expectation validation results
    # Map each expectation result to a rule result using map_expectation_to_rule
    rule_result = map_expectation_to_rule(expectation, rule)
    # Assert that the mapping produces valid rule results
    assert isinstance(rule_result, dict)
    # Assert that the rule result contains the correct success status
    assert rule_result['success'] is True
    # Assert that the rule result contains the appropriate details
    assert rule_result['details']['observed_value'] == 'column_exists'

@pytest.mark.unit
def test_add_expectation_to_suite():
    """Tests adding expectations to a suite"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a test suite
    suite_name = "test_suite"
    manager.create_suite(suite_name)
    # Create sample expectations
    expectation = {'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'id'}}
    # Add each expectation to the suite
    manager.add_expectation(suite_name, expectation)
    # Assert that the expectations are added successfully
    # Get the suite and verify it contains the added expectations
    suite = manager.get_suite(suite_name)
    assert len(suite.expectations) == 1
    assert suite.expectations[0].expectation_type == 'expect_column_to_exist'

@pytest.mark.unit
def test_add_rule_to_suite():
    """Tests adding validation rules to a suite as expectations"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a test suite
    suite_name = "test_suite"
    manager.create_suite(suite_name)
    # Create sample validation rules
    rule = {'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}}
    # Add each rule to the suite using add_rule
    manager.add_rule(suite_name, rule)
    # Assert that the rules are added successfully
    # Get the suite and verify it contains expectations for the rules
    suite = manager.get_suite(suite_name)
    assert len(suite.expectations) == 1
    assert suite.expectations[0].expectation_type == 'expect_column_to_exist'

@pytest.mark.unit
def test_validate_dataset_with_suite():
    """Tests validating a dataset against an expectation suite"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a test suite with expectations
    suite_name = "test_suite"
    manager.create_suite(suite_name)
    expectation = {'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'id'}}
    manager.add_expectation(suite_name, expectation)
    # Create a sample dataset that should pass validation
    df = pd.DataFrame({'id': [1, 2, 3]})
    # Validate the dataset against the suite
    results = manager.validate_dataset(df, suite_name)
    # Assert that validation completes successfully
    assert results['success'] is True
    # Assert that validation results match expectations
    # Create a sample dataset that should fail validation
    df = pd.DataFrame({'name': ['A', 'B', 'C']})
    # Validate the dataset against the suite
    results = manager.validate_dataset(df, suite_name)
    # Assert that validation identifies the expected failures
    assert results['success'] is False

@pytest.mark.unit
def test_validate_with_rules():
    """Tests validating a dataset against a list of validation rules"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create sample validation rules
    rules = [{'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}}]
    # Create a sample dataset
    df = pd.DataFrame({'id': [1, 2, 3]})
    # Validate the dataset with the rules using validate_with_rules
    results = manager.validate_with_rules(df, rules)
    # Assert that validation completes successfully
    assert isinstance(results, list)
    # Assert that validation results match expectations
    assert results[0]['success'] is True
    # Assert that results are properly mapped back to rule format
    assert results[0]['rule_id'] == 'rule_001'

@pytest.mark.unit
def test_expectation_suite_builder():
    """Tests the ExpectationSuiteBuilder functionality"""
    # Create an ExpectationSuiteBuilder with test configuration
    builder = create_test_expectation_suite_builder()
    # Create a suite using the builder
    suite_name = builder.create_suite("test_suite", overwrite_existing=True)
    # Assert that the suite is created successfully
    assert suite_name == "test_suite"
    # Build a suite from rules using build_suite_from_rules
    rules = [{'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}}]
    suite_name, num_expectations = builder.build_suite_from_rules(rules, "test_suite_2", overwrite_existing=True)
    # Assert that the suite contains expectations for all rules
    assert num_expectations == 1
    # Test adding individual rules to the suite
    builder.add_rule_to_suite(rules[0], "test_suite")
    # Test suite management functions (list, get, delete)
    assert "test_suite" in builder.list_suites()
    assert builder.get_suite("test_suite") is not None
    builder.delete_suite("test_suite")
    assert "test_suite" not in builder.list_suites()

@pytest.mark.unit
def test_custom_expectations():
    """Tests the custom expectations functionality"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a test suite
    suite_name = "test_suite"
    manager.create_suite(suite_name)
    # Create sample data for testing custom expectations
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']})
    # Add custom expectations to the suite
    expectation = {'expectation_type': 'expect_column_values_to_be_in_set', 'kwargs': {'column': 'col2', 'value_set': ['A', 'B', 'C']}}
    manager.add_expectation(suite_name, expectation)
    # Validate the dataset against the custom expectations
    results = manager.validate_dataset(df, suite_name)
    # Assert that validation results are as expected for each custom expectation
    assert results['success'] is True

@pytest.mark.unit
def test_suite_import_export():
    """Tests importing and exporting expectation suites"""
    # Create an ExpectationManager with test configuration
    manager = create_test_expectation_manager()
    # Create a test suite with expectations
    suite_name = "test_suite"
    manager.create_suite(suite_name)
    expectation = {'expectation_type': 'expect_column_to_exist', 'kwargs': {'column': 'id'}}
    manager.add_expectation(suite_name, expectation)
    # Export the suite to a JSON file
    file_path = create_temp_file(suffix=".json")
    manager.export_suite_to_json(suite_name, file_path)
    # Assert that the file is created with correct content
    assert os.path.exists(file_path)
    # Create a new manager instance
    new_manager = create_test_expectation_manager()
    # Import the suite from the JSON file
    imported_suite = new_manager.import_suite_from_json(file_path, suite_name)
    # Assert that the imported suite matches the original
    assert imported_suite is not None
    assert imported_suite.expectation_suite_name == suite_name
    assert len(imported_suite.expectations) == 1
    assert imported_suite.expectations[0].expectation_type == 'expect_column_to_exist'

@pytest.mark.unit
def test_generate_suite_name():
    """Tests the generation of unique suite names"""
    # Call generate_suite_name with different prefixes
    suite_name1 = generate_suite_name("test_prefix")
    suite_name2 = generate_suite_name("test_prefix")
    # Assert that generated names contain the prefix
    assert suite_name1.startswith("test_prefix")
    assert suite_name2.startswith("test_prefix")
    # Assert that generated names are unique
    assert suite_name1 != suite_name2
    # Assert that generated names are valid suite names
    assert validate_suite_name(suite_name1)
    assert validate_suite_name(suite_name2)

@pytest.mark.unit
def test_validate_suite_name():
    """Tests the validation of suite names"""
    # Test valid suite names with validate_suite_name
    valid_name = "test_suite"
    # Assert that valid names return True
    assert validate_suite_name(valid_name) is True
    # Test invalid suite names
    invalid_name = "test suite"
    # Assert that invalid names return False
    assert validate_suite_name(invalid_name) is False