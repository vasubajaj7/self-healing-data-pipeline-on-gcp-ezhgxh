"""
Provides test fixtures and helper functions for testing the data quality validation components of the self-healing data pipeline.
Includes mock objects, test data generators, and utility functions to simplify quality validation testing.
"""
import pytest  # package_version: 7.3.1
from unittest.mock import MagicMock  # package_version: standard library
import pandas as pd  # package_version: 2.0.x
import numpy as np  # package_version: 1.23.x
import datetime  # package_version: standard library
import uuid  # package_version: standard library
from typing import Dict, List, Any

from src.backend.constants import ValidationRuleType, QualityDimension, VALIDATION_STATUS_PASSED, VALIDATION_STATUS_FAILED, VALIDATION_STATUS_WARNING  # src/backend/constants.py
from src.backend.quality.engines.validation_engine import ValidationResult, ValidationSummary, ValidationEngine, create_validation_result  # src/backend/quality/engines/validation_engine.py
from src.backend.quality.engines.quality_scorer import QualityScorer, ScoringModel  # src/backend/quality/engines/quality_scorer.py
from src.backend.quality.validators.schema_validator import SchemaValidator  # src/backend/quality/validators/schema_validator.py
from src.test.utils.test_helpers import create_temp_file  # src/test/utils/test_helpers.py


SAMPLE_VALIDATION_RULES = "[{'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'columns': ['id', 'name', 'value']}}, {'rule_id': 'rule_002', 'rule_type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column': 'value', 'min_value': 0, 'max_value': 100}}]"
SAMPLE_VALIDATION_RESULTS = "[{'rule_id': 'rule_001', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'status': 'PASSED', 'success': True, 'details': {'message': 'All required columns present'}}, {'rule_id': 'rule_002', 'rule_type': 'CONTENT', 'dimension': 'ACCURACY', 'status': 'FAILED', 'success': False, 'details': {'message': 'Values outside acceptable range', 'invalid_count': 5}}]"


def create_test_rule(rule_id: str, rule_type: ValidationRuleType, dimension: QualityDimension, parameters: Dict) -> Dict:
    """Creates a test validation rule with specified parameters

    Args:
        rule_id (str): rule_id
        rule_type (ValidationRuleType): rule_type
        dimension (QualityDimension): dimension
        parameters (Dict): parameters

    Returns:
        Dict: A validation rule dictionary
    """
    # Create a rule dictionary with the provided parameters
    rule = {
        'rule_id': rule_id,
        'rule_type': rule_type.value,
        'dimension': dimension.value,
        'parameters': parameters
    }
    # Set default values for any missing parameters
    rule.setdefault('name', f'Test Rule {rule_id}')
    rule.setdefault('description', 'A test validation rule')
    # Return the complete rule dictionary
    return rule


def create_test_validation_result(rule_id: str, success: bool, details: Dict) -> ValidationResult:
    """Creates a test validation result with specified parameters

    Args:
        rule_id (str): rule_id
        success (bool): success
        details (Dict): details

    Returns:
        ValidationResult: A ValidationResult instance
    """
    # Create a rule dictionary with default values
    rule = {'rule_id': rule_id, 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    # Use create_validation_result to create a ValidationResult
    validation_result = create_validation_result(rule, success, details)
    # Set success status and details
    validation_result.success = success
    validation_result.details = details
    # Return the ValidationResult instance
    return validation_result


def create_test_validation_summary(validation_results: List, quality_score_value: float, passes_threshold: bool) -> ValidationSummary:
    """Creates a test validation summary with specified parameters

    Args:
        validation_results (List): validation_results
        quality_score_value (float): quality_score_value
        passes_threshold (bool): passes_threshold

    Returns:
        ValidationSummary: A ValidationSummary instance
    """
    # Create a ValidationSummary with the provided validation results
    validation_summary = ValidationSummary(validation_results, 0.0)
    # Create a quality score with the provided value
    mock_quality_score = MagicMock()
    mock_quality_score.overall_score = quality_score_value
    # Set the quality score and threshold status
    validation_summary.set_quality_score(mock_quality_score, 0.8)
    validation_summary.passes_threshold = passes_threshold
    # Return the ValidationSummary instance
    return validation_summary


def create_mock_validation_engine(validation_results: List, validation_summary: ValidationSummary) -> MagicMock:
    """Creates a mock ValidationEngine for testing

    Args:
        validation_results (List): validation_results
        validation_summary (ValidationSummary): validation_summary

    Returns:
        MagicMock: A mock ValidationEngine instance
    """
    # Create a mock ValidationEngine object
    mock_engine = MagicMock(spec=ValidationEngine)
    # Configure the validate method to return the provided validation_summary and validation_results
    mock_engine.validate.return_value = validation_summary, validation_results
    # Configure the validate_rule method to return a validation result based on the rule
    mock_engine.validate_rule.side_effect = lambda dataset, rule: create_test_validation_result(rule['rule_id'], True, {})
    # Configure other methods with appropriate return values
    mock_engine.get_quality_threshold.return_value = 0.8
    # Return the configured mock
    return mock_engine


def create_mock_quality_scorer(score_value: float, passes_threshold: bool) -> MagicMock:
    """Creates a mock QualityScorer for testing

    Args:
        score_value (float): score_value
        passes_threshold (bool): passes_threshold

    Returns:
        MagicMock: A mock QualityScorer instance
    """
    # Create a mock QualityScorer object
    mock_scorer = MagicMock(spec=QualityScorer)
    # Configure the calculate_score method to return the provided score_value
    mock_scorer.calculate_score.return_value = score_value
    # Configure the passes_threshold method to return the provided passes_threshold value
    mock_scorer.passes_threshold.return_value = passes_threshold
    # Configure other methods with appropriate return values
    mock_scorer.get_quality_threshold.return_value = 0.8
    # Return the configured mock
    return mock_scorer


def create_mock_schema_validator(validation_results: List) -> MagicMock:
    """Creates a mock SchemaValidator for testing

    Args:
        validation_results (List): validation_results

    Returns:
        MagicMock: A mock SchemaValidator instance
    """
    # Create a mock SchemaValidator object
    mock_validator = MagicMock(spec=SchemaValidator)
    # Configure the validate method to return the provided validation_results
    mock_validator.validate.return_value = validation_results
    # Configure the validate_rule method to return a validation result based on the rule
    mock_validator.validate_rule.side_effect = lambda dataset, rule: create_test_validation_result(rule['rule_id'], True, {})
    # Configure other methods with appropriate return values
    mock_validator.extract_schema.return_value = {'col1': 'INT64', 'col2': 'STRING'}
    # Return the configured mock
    return mock_validator


def create_mock_rule_engine(rules: List) -> MagicMock:
    """Creates a mock rule engine for testing

    Args:
        rules (List): rules

    Returns:
        MagicMock: A mock rule engine instance
    """
    # Create a mock rule engine object
    mock_engine = MagicMock()
    # Configure the get_rules method to return the provided rules
    mock_engine.get_rules.return_value = rules
    # Configure the get_rule method to return a specific rule by ID
    mock_engine.get_rule.side_effect = lambda rule_id: next((rule for rule in rules if rule['rule_id'] == rule_id), None)
    # Configure other methods with appropriate return values
    mock_engine.register_rule.return_value = 'test_rule_id'
    # Return the configured mock
    return mock_engine


def generate_test_dataset(rows: int, columns: List, data_types: Dict, null_percentage: float) -> pd.DataFrame:
    """Generates a test pandas DataFrame with specified characteristics

    Args:
        rows (int): rows
        columns (List): columns
        data_types (Dict): data_types
        null_percentage (float): null_percentage

    Returns:
        pd.DataFrame: A test DataFrame
    """
    # Create an empty DataFrame with the specified columns
    df = pd.DataFrame(columns=columns)
    # Generate random data for each column based on data_types
    for col in columns:
        if col in data_types:
            if data_types[col] == 'int':
                df[col] = np.random.randint(0, 100, size=rows)
            elif data_types[col] == 'float':
                df[col] = np.random.rand(rows)
            elif data_types[col] == 'str':
                df[col] = [''.join(np.random.choice(list('abcde'), size=5)) for _ in range(rows)]
            elif data_types[col] == 'bool':
                df[col] = np.random.choice([True, False], size=rows)
            elif data_types[col] == 'datetime':
                start = datetime.datetime.now() - datetime.timedelta(days=365)
                end = datetime.datetime.now()
                df[col] = [start + (end - start) * np.random.rand() for _ in range(rows)]
    # Introduce null values based on null_percentage
    for col in columns:
        mask = np.random.choice([True, False], size=rows, p=[null_percentage, 1 - null_percentage])
        df.loc[mask, col] = None
    # Return the generated DataFrame
    return df


class TestValidationData:
    """Class providing test data for quality validation tests"""
    rules: List
    validation_results: List
    test_dataframe: pd.DataFrame
    schema_definition: Dict

    def __init__(self):
        """Initialize test validation data"""
        # Initialize default test rules
        self.rules = [
            {'rule_id': 'rule_001', 'name': 'Column id exists', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}},
            {'rule_id': 'rule_002', 'name': 'Column value is positive', 'type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column_name': 'value', 'min_value': 0}}
        ]
        # Initialize default validation results
        self.validation_results = [
            {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
            {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
        ]
        # Generate a test DataFrame with known characteristics
        self.test_dataframe = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', 'D', 'E'],
            'value': [10, 20, -30, 40, 50]
        })
        # Define a schema definition for testing
        self.schema_definition = {'id': 'INT64', 'name': 'STRING', 'value': 'INT64'}

    def get_rule_by_id(self, rule_id: str) -> Dict:
        """Get a test rule by ID

        Args:
            rule_id (str): rule_id

        Returns:
            Dict: The rule with the specified ID or None
        """
        # Search for a rule with the specified ID in the rules list
        for rule in self.rules:
            if rule['rule_id'] == rule_id:
                # Return the rule if found, None otherwise
                return rule
        return None

    def get_result_by_rule_id(self, rule_id: str) -> Dict:
        """Get a test validation result by rule ID

        Args:
            rule_id (str): rule_id

        Returns:
            Dict: The validation result for the specified rule ID or None
        """
        # Search for a validation result with the specified rule ID
        for result in self.validation_results:
            if result['rule_id'] == rule_id:
                # Return the result if found, None otherwise
                return result
        return None

    def create_validation_summary(self, quality_score: float, passes_threshold: bool) -> ValidationSummary:
        """Create a test validation summary

        Args:
            quality_score (float): quality_score
            passes_threshold (bool): passes_threshold

        Returns:
            ValidationSummary: A test validation summary
        """
        # Create a ValidationSummary with the validation_results
        validation_summary = ValidationSummary(self.validation_results, 0.0)
        # Set the quality score and threshold status
        mock_quality_score = MagicMock()
        mock_quality_score.overall_score = quality_score
        validation_summary.set_quality_score(mock_quality_score, 0.8)
        validation_summary.passes_threshold = passes_threshold
        # Return the ValidationSummary
        return validation_summary

    def generate_dataframe_with_issues(self, issue_types: List) -> pd.DataFrame:
        """Generate a test DataFrame with specific quality issues

        Args:
            issue_types (List): issue_types

        Returns:
            pd.DataFrame: A DataFrame with the specified issues
        """
        # Start with a clean test DataFrame
        df = self.test_dataframe.copy()
        # For each issue type, introduce specific issues:
        for issue_type in issue_types:
            if issue_type == 'missing_columns':
                # For 'missing_columns', remove specified columns
                columns_to_remove = ['name', 'value']
                df = df.drop(columns=columns_to_remove, errors='ignore')
            elif issue_type == 'wrong_types':
                # For 'wrong_types', change data types of columns
                df['id'] = df['id'].astype(str)
                df['value'] = df['value'].astype(str)
            elif issue_type == 'null_values':
                # For 'null_values', introduce null values
                df.loc[0:2, 'name'] = None
            elif issue_type == 'out_of_range':
                # For 'out_of_range', add values outside valid ranges
                df.loc[0, 'value'] = -100
                df.loc[1, 'value'] = 200
            elif issue_type == 'duplicates':
                # For 'duplicates', add duplicate rows
                df = pd.concat([df, df.iloc[[0, 1]]], ignore_index=True)
        # Return the DataFrame with issues
        return df


class MockValidationEngine:
    """Mock implementation of ValidationEngine for testing"""
    _config: Dict
    _validation_results: List
    _validation_summary: ValidationSummary
    _rule_registry: Dict

    def __init__(self, validation_results: List, validation_summary: ValidationSummary, config: Dict = None):
        """Initialize mock validation engine

        Args:
            validation_results (List): validation_results
            validation_summary (ValidationSummary): validation_summary
            config (Dict): config
        """
        # Store validation_results for later use
        self._validation_results = validation_results
        # Store validation_summary for later use
        self._validation_summary = validation_summary
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        # Initialize empty rule registry
        self._rule_registry = {}

    def validate(self, dataset: Any, rules: List, validation_config: Dict) -> Tuple[ValidationSummary, List[ValidationResult]]:
        """Mock implementation of validate method

        Args:
            dataset (Any): dataset
            rules (List): rules
            validation_config (Dict): validation_config

        Returns:
            Tuple[ValidationSummary, List[ValidationResult]]: (ValidationSummary, list[ValidationResult])
        """
        # Return the stored validation_summary and validation_results
        return self._validation_summary, self._validation_results

    def validate_rule(self, dataset: Any, rule: Dict) -> ValidationResult:
        """Mock implementation of validate_rule method

        Args:
            dataset (Any): dataset
            rule (Dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Find a matching result in _validation_results by rule_id
        for result in self._validation_results:
            if result.rule_id == rule['rule_id']:
                # If found, return that result
                return result
        # Otherwise, create and return a default result
        return ValidationResult(rule['rule_id'], ValidationRuleType.SCHEMA, QualityDimension.COMPLETENESS)

    def register_rule(self, rule: Dict) -> str:
        """Mock implementation of register_rule method

        Args:
            rule (Dict): rule

        Returns:
            str: Rule ID
        """
        # Store rule in rule registry
        self._rule_registry[rule['rule_id']] = rule
        # Return rule_id
        return rule['rule_id']

    def get_rule(self, rule_id: str) -> Dict:
        """Mock implementation of get_rule method

        Args:
            rule_id (str): rule_id

        Returns:
            Dict: Rule definition
        """
        # Return rule from registry if found, None otherwise
        return self._rule_registry.get(rule_id)


@pytest.fixture
def mock_validation_engine():
    """Pytest fixture providing a mock validation engine"""
    # Create mock validation results
    mock_results = [
        create_test_validation_result(rule_id='rule_001', success=True, details={'message': 'Column exists'}),
        create_test_validation_result(rule_id='rule_002', success=False, details={'message': 'Values out of range'})
    ]
    # Create mock validation summary
    mock_summary = create_test_validation_summary(validation_results=mock_results, quality_score=0.75, passes_threshold=False)
    # Create and return mock validation engine
    return create_mock_validation_engine(mock_results, mock_summary)


@pytest.fixture
def mock_quality_scorer():
    """Pytest fixture providing a mock quality scorer"""
    # Create and return mock quality scorer
    return create_mock_quality_scorer(score_value=0.85, passes_threshold=True)


@pytest.fixture
def mock_schema_validator():
    """Pytest fixture providing a mock schema validator"""
    # Create mock validation results
    mock_results = [
        create_test_validation_result(rule_id='rule_001', success=True, details={'message': 'Column exists'}),
        create_test_validation_result(rule_id='rule_002', success=False, details={'message': 'Values out of range'})
    ]
    # Create and return mock schema validator
    return create_mock_schema_validator(mock_results)


@pytest.fixture
def test_validation_data():
    """Pytest fixture providing test validation data"""
    # Create and return TestValidationData instance
    return TestValidationData()


@pytest.fixture
def sample_validation_rules():
    """Pytest fixture providing sample validation rules"""
    # Return a list of sample validation rules
    return [
        {'rule_id': 'rule_001', 'name': 'Column id exists', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}},
        {'rule_id': 'rule_002', 'name': 'Column value is positive', 'type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column_name': 'value', 'min_value': 0}}
    ]


@pytest.fixture
def sample_validation_results():
    """Pytest fixture providing sample validation results"""
    # Return a list of sample validation results
    return [
        {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
        {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
    ]