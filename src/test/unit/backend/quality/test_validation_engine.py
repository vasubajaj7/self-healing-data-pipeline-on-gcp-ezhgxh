# src/test/unit/backend/quality/test_validation_engine.py
"""Unit tests for the validation engine component of the data quality framework.
Tests the functionality of ValidationEngine, ValidationResult, and ValidationSummary classes along with helper functions for validation result processing."""
import pytest  # package_version: 7.3.1
from unittest import mock  # package_version: standard library
import pandas as pd  # package_version: 2.0.x
import numpy as np  # package_version: 1.23.x
import json  # package_version: standard library

from src.backend.constants import ValidationRuleType, QualityDimension, VALIDATION_STATUS_PASSED, VALIDATION_STATUS_FAILED, VALIDATION_STATUS_WARNING  # src/backend/constants.py
from src.backend.quality.engines.validation_engine import ValidationResult, ValidationSummary, ValidationEngine, create_validation_result, format_validation_summary, group_results_by_dimension, group_results_by_rule_type, create_validator  # src/backend/quality/engines/validation_engine.py
from src.backend.quality.engines.execution_engine import ExecutionEngine, ExecutionMode, ExecutionContext  # src/backend/quality/engines/execution_engine.py
from src.backend.quality.engines.quality_scorer import QualityScorer, ScoringModel  # src/backend/quality/engines/quality_scorer.py
from src.backend.quality.validators.schema_validator import SchemaValidator  # src/backend/quality/validators/schema_validator.py
from src.test.fixtures.backend.quality_fixtures import create_test_rule, create_test_validation_result, create_test_validation_summary, generate_test_dataset, TestValidationData, mock_validation_engine, mock_quality_scorer, mock_schema_validator, test_validation_data, sample_validation_rules, sample_validation_results  # src/test/fixtures/backend/quality_fixtures.py


def test_validation_result_initialization():
    """Test that ValidationResult initializes correctly with expected values"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Verify rule_id, rule_type, and dimension are set correctly
    assert result.rule_id == 'test_rule'
    assert result.rule_type == ValidationRuleType.SCHEMA
    assert result.dimension == QualityDimension.COMPLETENESS
    # Verify default values for status, success, details, and execution_time
    assert result.status is None
    assert result.success is False
    assert result.details == {}
    assert result.execution_time == 0.0


def test_validation_result_set_status():
    """Test that set_status method correctly updates status and success properties"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Call set_status(True) and verify status is PASSED and success is True
    result.set_status(True)
    assert result.status == VALIDATION_STATUS_PASSED
    assert result.success is True
    # Call set_status(False) and verify status is FAILED and success is False
    result.set_status(False)
    assert result.status == VALIDATION_STATUS_FAILED
    assert result.success is False


def test_validation_result_set_warning():
    """Test that set_warning method correctly sets warning status"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Call set_warning() method
    result.set_warning()
    # Verify status is WARNING and success is True
    assert result.status == VALIDATION_STATUS_WARNING
    assert result.success is True


def test_validation_result_set_details():
    """Test that set_details method correctly updates details property"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Create a test details dictionary
    test_details = {'message': 'Test details'}
    # Call set_details with the test dictionary
    result.set_details(test_details)
    # Verify details property matches the test dictionary
    assert result.details == test_details


def test_validation_result_set_execution_time():
    """Test that set_execution_time method correctly updates execution_time property"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Call set_execution_time with a test value
    result.set_execution_time(1.23)
    # Verify execution_time property matches the test value
    assert result.execution_time == 1.23


def test_validation_result_to_dict():
    """Test that to_dict method correctly serializes ValidationResult to dictionary"""
    # Create a ValidationResult with test parameters
    result = ValidationResult(rule_id='test_rule', rule_type=ValidationRuleType.SCHEMA, dimension=QualityDimension.COMPLETENESS)
    # Set status, details, and execution_time
    result.set_status(True)
    result.set_details({'message': 'Test details'})
    result.set_execution_time(1.23)
    result.timestamp = pd.Timestamp('2023-01-01')
    # Call to_dict method
    result_dict = result.to_dict()
    # Verify dictionary contains all expected keys and values
    assert result_dict['rule_id'] == 'test_rule'
    assert result_dict['rule_type'] == 'SCHEMA'
    assert result_dict['dimension'] == 'COMPLETENESS'
    assert result_dict['status'] == VALIDATION_STATUS_PASSED
    assert result_dict['success'] is True
    assert result_dict['details'] == {'message': 'Test details'}
    # Verify enum values are converted to strings
    # Verify timestamp is formatted correctly
    assert result_dict['timestamp'] == '2023-01-01T00:00:00'
    assert result_dict['execution_time'] == 1.23


def test_validation_result_from_dict():
    """Test that from_dict method correctly deserializes dictionary to ValidationResult"""
    # Create a test dictionary with ValidationResult data
    data = {
        'rule_id': 'test_rule',
        'rule_type': 'SCHEMA',
        'dimension': 'COMPLETENESS',
        'status': VALIDATION_STATUS_PASSED,
        'success': True,
        'details': {'message': 'Test details'},
        'timestamp': '2023-01-01T00:00:00',
        'execution_time': 1.23
    }
    # Call ValidationResult.from_dict with the test dictionary
    result = ValidationResult.from_dict(data)
    # Verify all properties are set correctly
    assert result.rule_id == 'test_rule'
    assert result.rule_type == ValidationRuleType.SCHEMA
    assert result.dimension == QualityDimension.COMPLETENESS
    assert result.status == VALIDATION_STATUS_PASSED
    assert result.success is True
    assert result.details == {'message': 'Test details'}
    # Verify enum values are converted from strings
    # Verify timestamp is parsed correctly
    assert result.timestamp == '2023-01-01T00:00:00'
    assert result.execution_time == 1.23


def test_create_validation_result():
    """Test that create_validation_result function creates a ValidationResult with expected values"""
    # Create a test rule dictionary
    rule = {'rule_id': 'test_rule', 'rule_type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    # Call create_validation_result with the rule, success=True, and test details
    result = create_validation_result(rule, success=True, details={'message': 'Test details'})
    # Verify returned object is a ValidationResult
    assert isinstance(result, ValidationResult)
    # Verify rule_id, rule_type, and dimension match the rule
    assert result.rule_id == 'test_rule'
    assert result.rule_type == ValidationRuleType.SCHEMA
    assert result.dimension == QualityDimension.COMPLETENESS
    # Verify status is PASSED and success is True
    assert result.status == VALIDATION_STATUS_PASSED
    assert result.success is True
    # Verify details match the test details
    assert result.details == {'message': 'Test details'}


def test_validation_summary_initialization():
    """Test that ValidationSummary initializes correctly with expected values"""
    # Create a list of test ValidationResults
    results = [
        {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
        {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
    ]
    # Create a ValidationSummary with the test results
    summary = ValidationSummary(results, 0.0)
    # Verify validation_id is a valid UUID
    assert isinstance(summary.validation_id, str)
    try:
        uuid.UUID(summary.validation_id)
    except ValueError:
        pytest.fail("validation_id is not a valid UUID")
    # Verify total_rules, passed_rules, failed_rules, and warning_rules are counted correctly
    assert summary.total_rules == 2
    assert summary.passed_rules == 1
    assert summary.failed_rules == 1
    assert summary.warning_rules == 0
    # Verify success_rate is calculated correctly
    assert summary.success_rate == 0.5
    # Verify results_by_dimension and results_by_rule_type are grouped correctly
    # Verify quality_score is initially None and passes_threshold is False
    assert summary.quality_score is None
    assert summary.passes_threshold is False
    # Verify timestamp is set and execution_time matches the parameter
    assert summary.execution_time == 0.0


def test_validation_summary_set_quality_score():
    """Test that set_quality_score method correctly updates quality_score and passes_threshold"""
    # Create a ValidationSummary with test results
    results = [
        {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
        {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
    ]
    summary = ValidationSummary(results, 0.0)
    # Create a mock QualityScore with a test value
    mock_score = MagicMock()
    mock_score.overall_score = 0.9
    # Call set_quality_score with the mock score and a threshold
    summary.set_quality_score(mock_score, 0.8)
    # Verify quality_score is set to the mock score
    assert summary.quality_score == mock_score
    # Verify passes_threshold is set based on comparison with threshold
    assert summary.passes_threshold is True


def test_validation_summary_to_dict():
    """Test that to_dict method correctly serializes ValidationSummary to dictionary"""
    # Create a ValidationSummary with test results
    results = [
        {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
        {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
    ]
    summary = ValidationSummary(results, 0.0)
    # Set a quality score
    mock_score = MagicMock()
    mock_score.overall_score = 0.9
    summary.set_quality_score(mock_score, 0.8)
    summary.timestamp = pd.Timestamp('2023-01-01')
    # Call to_dict method
    summary_dict = summary.to_dict()
    # Verify dictionary contains all expected keys and values
    assert summary_dict['validation_id'] == summary.validation_id
    assert summary_dict['total_rules'] == 2
    assert summary_dict['passed_rules'] == 1
    assert summary_dict['failed_rules'] == 1
    assert summary_dict['warning_rules'] == 0
    assert summary_dict['success_rate'] == 0.5
    # Verify quality_score is converted to dictionary
    # Verify timestamp is formatted correctly
    assert summary_dict['timestamp'] == '2023-01-01T00:00:00'
    assert summary_dict['execution_time'] == 0.0


def test_validation_summary_from_dict():
    """Test that from_dict method correctly deserializes dictionary to ValidationSummary"""
    # Create a test dictionary with ValidationSummary data
    data = {
        'validation_id': str(uuid.uuid4()),
        'total_rules': 2,
        'passed_rules': 1,
        'failed_rules': 1,
        'warning_rules': 0,
        'success_rate': 0.5,
        'results_by_dimension': {},
        'results_by_rule_type': {},
        'quality_score': {'overall_score': 0.9},
        'passes_threshold': True,
        'timestamp': '2023-01-01T00:00:00',
        'execution_time': 0.0
    }
    # Call ValidationSummary.from_dict with the test dictionary
    summary = ValidationSummary.from_dict(data)
    # Verify all properties are set correctly
    assert summary.validation_id == data['validation_id']
    assert summary.total_rules == 2
    assert summary.passed_rules == 1
    assert summary.failed_rules == 1
    assert summary.warning_rules == 0
    assert summary.success_rate == 0.5
    assert summary.results_by_dimension == {}
    assert summary.results_by_rule_type == {}
    # Verify quality_score is converted from dictionary
    # Verify timestamp is parsed correctly
    assert summary.timestamp == '2023-01-01T00:00:00'
    assert summary.execution_time == 0.0


def test_format_validation_summary():
    """Test that format_validation_summary function formats results correctly"""
    # Create a list of test ValidationResults
    results = [
        {'rule_id': 'rule_001', 'success': True, 'details': {'message': 'Column exists'}},
        {'rule_id': 'rule_002', 'success': False, 'details': {'message': 'Values out of range'}}
    ]
    # Create a mock QualityScore
    mock_score = MagicMock()
    mock_score.overall_score = 0.9
    # Call format_validation_summary with results and score
    formatted_summary = format_validation_summary(results, mock_score)
    # Verify returned dictionary contains all expected sections
    assert 'total_rules' in formatted_summary
    assert 'passed_rules' in formatted_summary
    assert 'failed_rules' in formatted_summary
    assert 'warning_rules' in formatted_summary
    assert 'success_rate' in formatted_summary
    assert 'results_by_rule_type' in formatted_summary
    assert 'results_by_dimension' in formatted_summary
    assert 'quality_score' in formatted_summary
    # Verify counts (total, passed, failed, warnings) are correct
    assert formatted_summary['total_rules'] == 2
    assert formatted_summary['passed_rules'] == 1
    assert formatted_summary['failed_rules'] == 1
    assert formatted_summary['warning_rules'] == 0
    # Verify success rate is calculated correctly
    assert formatted_summary['success_rate'] == 0.5
    # Verify results are grouped by rule type and dimension
    # Verify quality score information is included
    assert formatted_summary['quality_score'] == mock_score.to_dict()


def test_group_results_by_dimension():
    """Test that group_results_by_dimension function groups results correctly"""
    # Create test ValidationResults with different dimensions
    results = [
        {'rule_id': 'rule_001', 'dimension': QualityDimension.COMPLETENESS, 'success': True},
        {'rule_id': 'rule_002', 'dimension': QualityDimension.ACCURACY, 'success': False},
        {'rule_id': 'rule_003', 'dimension': QualityDimension.COMPLETENESS, 'success': True}
    ]
    # Call group_results_by_dimension with the test results
    grouped_results = group_results_by_dimension(results)
    # Verify returned dictionary has QualityDimension keys
    assert set(grouped_results.keys()) == set(QualityDimension)
    # Verify each dimension group contains the correct results
    assert len(grouped_results[QualityDimension.COMPLETENESS]) == 2
    assert len(grouped_results[QualityDimension.ACCURACY]) == 1
    # Verify dimensions with no results have empty lists
    assert len(grouped_results[QualityDimension.CONSISTENCY]) == 0


def test_group_results_by_rule_type():
    """Test that group_results_by_rule_type function groups results correctly"""
    # Create test ValidationResults with different rule types
    results = [
        {'rule_id': 'rule_001', 'rule_type': ValidationRuleType.SCHEMA, 'success': True},
        {'rule_id': 'rule_002', 'rule_type': ValidationRuleType.CONTENT, 'success': False},
        {'rule_id': 'rule_003', 'rule_type': ValidationRuleType.SCHEMA, 'success': True}
    ]
    # Call group_results_by_rule_type with the test results
    grouped_results = group_results_by_rule_type(results)
    # Verify returned dictionary has ValidationRuleType keys
    assert set(grouped_results.keys()) == set(ValidationRuleType)
    # Verify each rule type group contains the correct results
    assert len(grouped_results[ValidationRuleType.SCHEMA]) == 2
    assert len(grouped_results[ValidationRuleType.CONTENT]) == 1
    # Verify rule types with no results have empty lists
    assert len(grouped_results[ValidationRuleType.RELATIONSHIP]) == 0


def test_create_validator():
    """Test that create_validator function creates appropriate validators"""
    # Mock the dynamic_import function to return mock validators
    with mock.patch('src.backend.quality.engines.validation_engine.dynamic_import') as mock_dynamic_import:
        mock_dynamic_import.return_value = MagicMock()
        # Call create_validator with ValidationRuleType.SCHEMA
        validator = create_validator(ValidationRuleType.SCHEMA)
        # Verify SchemaValidator is created
        assert isinstance(validator, MagicMock)
        # Call create_validator with ValidationRuleType.CONTENT
        validator = create_validator(ValidationRuleType.CONTENT)
        # Verify ContentValidator is created
        assert isinstance(validator, MagicMock)
        # Call create_validator with ValidationRuleType.RELATIONSHIP
        validator = create_validator(ValidationRuleType.RELATIONSHIP)
        # Verify RelationshipValidator is created
        assert isinstance(validator, MagicMock)
        # Call create_validator with ValidationRuleType.STATISTICAL
        validator = create_validator(ValidationRuleType.STATISTICAL)
        # Verify StatisticalValidator is created
        assert isinstance(validator, MagicMock)
        # Call create_validator with invalid rule type
        with pytest.raises(ValueError):
            create_validator("INVALID")


def test_validation_engine_initialization():
    """Test that ValidationEngine initializes correctly with expected values"""
    # Create a test configuration dictionary
    config = {'quality_threshold': 0.9, 'execution_mode': 'BIGQUERY', 'scoring_model': 'WEIGHTED'}
    # Create a ValidationEngine with the test configuration
    engine = ValidationEngine(config)
    # Verify _config is set with defaults overridden by test config
    assert engine._config == config
    # Verify _validators is initialized as empty dictionary
    assert engine._validators == {}
    # Verify _execution_engine is created
    assert isinstance(engine._execution_engine, ExecutionEngine)
    # Verify _quality_scorer is created
    assert isinstance(engine._quality_scorer, QualityScorer)
    # Verify _rule_registry is initialized as empty dictionary
    assert engine._rule_registry == {}
    # Verify _quality_threshold, _execution_mode, and _scoring_model are set from config
    assert engine._quality_threshold == 0.9
    assert engine._execution_mode == ExecutionMode.BIGQUERY
    assert engine._scoring_model == ScoringModel.WEIGHTED


def test_validation_engine_validate():
    """Test that validate method correctly validates a dataset against rules"""
    # Create a mock ExecutionEngine that returns test validation results
    mock_execution_engine = MagicMock()
    mock_execution_engine.execute.return_value = ([], MagicMock())
    # Create a mock QualityScorer that returns a test quality score
    mock_quality_scorer = MagicMock()
    mock_quality_scorer.calculate_score.return_value = 0.9
    # Create a ValidationEngine with the mocks
    engine = ValidationEngine({})
    engine._execution_engine = mock_execution_engine
    engine._quality_scorer = mock_quality_scorer
    # Create a test dataset and rules
    dataset = MagicMock()
    rules = [{'rule_id': 'test_rule', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'}]
    # Call validate method with dataset and rules
    summary, results = engine.validate(dataset, rules, {})
    # Verify ExecutionEngine.execute is called with correct parameters
    mock_execution_engine.execute.assert_called_once_with(dataset, rules, {})
    # Verify QualityScorer.calculate_score is called with results
    mock_quality_scorer.calculate_score.assert_called_once_with([])
    # Verify returned ValidationSummary has correct quality score
    # Verify returned validation results match expected results


def test_validation_engine_validate_rule():
    """Test that validate_rule method correctly validates a single rule"""
    # Create a mock validator that returns a test validation result
    mock_validator = MagicMock()
    mock_validator.validate.return_value = MagicMock()
    # Create a ValidationEngine with the mock validator
    engine = ValidationEngine({})
    engine._validators = {'SCHEMA': mock_validator}
    # Create a test dataset and rule
    dataset = MagicMock()
    rule = {'rule_id': 'test_rule', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    # Call validate_rule method with dataset and rule
    result = engine.validate_rule(dataset, rule)
    # Verify get_validator is called with rule type
    # Verify validator.validate_rule is called with dataset and rule
    # Verify returned validation result matches expected result


def test_validation_engine_register_rule():
    """Test that register_rule method correctly registers a rule"""
    # Create a ValidationEngine
    engine = ValidationEngine({})
    # Create a test rule without rule_id
    rule1 = {'name': 'Test Rule', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    # Call register_rule with the test rule
    rule_id1 = engine.register_rule(rule1)
    # Verify rule_id is generated
    assert rule_id1 is not None
    # Verify rule is stored in _rule_registry
    assert rule_id1 in engine._rule_registry
    assert engine._rule_registry[rule_id1] == rule1
    # Create a test rule with rule_id
    rule_id2 = 'existing_rule_id'
    rule2 = {'rule_id': rule_id2, 'name': 'Test Rule', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    # Call register_rule with the test rule
    engine.register_rule(rule2)
    # Verify rule_id is preserved
    assert rule_id2 in engine._rule_registry
    assert engine._rule_registry[rule_id2] == rule2


def test_validation_engine_get_rule():
    """Test that get_rule method correctly retrieves a rule"""
    # Create a ValidationEngine
    engine = ValidationEngine({})
    # Create and register a test rule
    rule = {'rule_id': 'test_rule', 'name': 'Test Rule', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'}
    engine.register_rule(rule)
    # Call get_rule with the rule_id
    retrieved_rule = engine.get_rule('test_rule')
    # Verify returned rule matches the registered rule
    assert retrieved_rule == rule
    # Call get_rule with a non-existent rule_id
    retrieved_rule = engine.get_rule('non_existent_rule')
    # Verify None is returned
    assert retrieved_rule is None


def test_validation_engine_load_rules_from_config():
    """Test that load_rules_from_config method correctly loads rules"""
    # Mock rule_loader.load_rules_from_config to return test rules
    with mock.patch('src.backend.quality.engines.validation_engine.load_rules_from_config') as mock_load_rules:
        test_rules = [{'rule_id': 'rule_001', 'name': 'Test Rule 1', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS'},
                      {'rule_id': 'rule_002', 'name': 'Test Rule 2', 'type': 'CONTENT', 'dimension': 'ACCURACY'}]
        mock_load_rules.return_value = test_rules
        # Create a ValidationEngine
        engine = ValidationEngine({})
        # Call load_rules_from_config with a test config path
        loaded_rules = engine.load_rules_from_config('test_config_path')
        # Verify rule_loader.load_rules_from_config is called with the path
        mock_load_rules.assert_called_once_with('test_config_path')
        # Verify each rule is registered
        # Verify returned rules match the test rules
        assert loaded_rules == test_rules


def test_validation_engine_get_validator():
    """Test that get_validator method correctly retrieves or creates validators"""
    # Mock create_validator to return a test validator
    with mock.patch('src.backend.quality.engines.validation_engine.create_validator') as mock_create_validator:
        mock_create_validator.return_value = MagicMock()
        # Create a ValidationEngine
        engine = ValidationEngine({})
        # Call get_validator with a rule type not in _validators
        validator1 = engine.get_validator(ValidationRuleType.SCHEMA)
        # Verify create_validator is called with the rule type
        mock_create_validator.assert_called_once_with(ValidationRuleType.SCHEMA)
        # Verify validator is stored in _validators
        assert ValidationRuleType.SCHEMA in engine._validators
        # Call get_validator with the same rule type again
        validator2 = engine.get_validator(ValidationRuleType.SCHEMA)
        # Verify create_validator is not called again
        assert mock_create_validator.call_count == 1
        # Verify cached validator is returned
        assert validator1 == validator2


def test_validation_engine_set_quality_threshold():
    """Test that set_quality_threshold method correctly updates threshold"""
    # Create a ValidationEngine
    engine = ValidationEngine({})
    # Call set_quality_threshold with a test value
    engine.set_quality_threshold(0.75)
    # Verify _quality_threshold is updated
    assert engine._quality_threshold == 0.75
    # Verify quality_scorer.set_quality_threshold is called with the value
    # Call set_quality_threshold with an invalid value (< 0)
    with pytest.raises(ValueError):
        engine.set_quality_threshold(-0.1)
    # Call set_quality_threshold with an invalid value (> 1)
    with pytest.raises(ValueError):
        engine.set_quality_threshold(1.1)


def test_validation_engine_get_quality_threshold():
    """Test that get_quality_threshold method returns correct threshold"""
    # Create a ValidationEngine with a test threshold
    engine = ValidationEngine({'quality_threshold': 0.75})
    # Call get_quality_threshold
    threshold = engine.get_quality_threshold()
    # Verify returned value matches the test threshold
    assert threshold == 0.75


def test_validation_engine_set_execution_mode():
    """Test that set_execution_mode method correctly updates mode"""
    # Create a ValidationEngine
    engine = ValidationEngine({})
    # Call set_execution_mode with a test mode
    engine.set_execution_mode(ExecutionMode.BIGQUERY)
    # Verify _execution_mode is updated
    assert engine._execution_mode == ExecutionMode.BIGQUERY
    # Call set_execution_mode with an invalid value
    with pytest.raises(ValueError):
        engine.set_execution_mode("INVALID")


def test_validation_engine_set_scoring_model():
    """Test that set_scoring_model method correctly updates model"""
    # Create a ValidationEngine
    engine = ValidationEngine({})
    # Call set_scoring_model with a test model
    engine.set_scoring_model(ScoringModel.WEIGHTED)
    # Verify _scoring_model is updated
    assert engine._scoring_model == ScoringModel.WEIGHTED
    # Verify quality_scorer.set_model is called with the model
    # Call set_scoring_model with an invalid value
    with pytest.raises(ValueError):
        engine.set_scoring_model("INVALID")


def test_validation_engine_report_metrics():
    """Test that report_metrics method correctly reports metrics"""
    # Create a mock MetricClient
    mock_metric_client = MagicMock()
    # Create a ValidationEngine with the mock client
    engine = ValidationEngine({})
    engine._metric_client = mock_metric_client
    # Create a test ValidationSummary and results
    mock_summary = MagicMock()
    mock_results = []
    # Call report_metrics with the summary and results
    engine.report_metrics(mock_summary, mock_results)
    # Verify MetricClient.report_counter is called for validation count
    # Verify MetricClient.report_gauge is called for success rate
    # Verify MetricClient.report_gauge is called for quality score
    # Verify MetricClient.report_gauge is called for execution time
    # Verify MetricClient.report_counter is called for rule types
    # Verify MetricClient.report_counter is called for dimensions


def test_validation_engine_close():
    """Test that close method correctly releases resources"""
    # Create mock validators, execution engine, and metric client
    mock_validator = MagicMock()
    mock_execution_engine = MagicMock()
    mock_metric_client = MagicMock()
    # Create a ValidationEngine with the mocks
    engine = ValidationEngine({})
    engine._validators = {'SCHEMA': mock_validator}
    engine._execution_engine = mock_execution_engine
    engine._metric_client = mock_metric_client
    # Call close method
    engine.close()
    # Verify close is called on each validator
    mock_validator.close.assert_called_once()
    # Verify close is called on execution engine
    mock_execution_engine.close.assert_called_once()
    # Verify close is called on metric client
    mock_metric_client.close.assert_called_once()


def test_validation_engine_integration():
    """Integration test for ValidationEngine with real components"""
    # Create a real ValidationEngine (not mocked)
    engine = ValidationEngine({})
    # Create a test dataset with known characteristics
    dataset = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
    # Create test rules for schema and content validation
    rules = [
        {'rule_id': 'rule_001', 'name': 'Column id exists', 'type': 'SCHEMA', 'dimension': 'COMPLETENESS', 'parameters': {'column_name': 'id'}},
        {'rule_id': 'rule_002', 'name': 'Column value is positive', 'type': 'CONTENT', 'dimension': 'ACCURACY', 'parameters': {'column_name': 'value', 'min_value': 0}}
    ]
    # Call validate method with dataset and rules
    summary, results = engine.validate(dataset, rules, {})
    # Verify ValidationSummary is returned with expected properties
    assert isinstance(summary, ValidationSummary)
    # Verify validation results match expected outcomes
    # Verify quality score is calculated correctly