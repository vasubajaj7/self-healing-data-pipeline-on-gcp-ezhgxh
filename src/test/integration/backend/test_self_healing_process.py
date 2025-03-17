import pytest
import unittest.mock
import pandas
import numpy
import json
import os
import datetime
from src.backend.constants import HealingActionType, DEFAULT_CONFIDENCE_THRESHOLD
from src.backend.self_healing.ai.issue_classifier import IssueClassification, IssueClassifier
from src.backend.self_healing.ai.pattern_recognizer import Pattern, PatternRecognizer
from src.backend.self_healing.correction.data_corrector import DataCorrector, CorrectionResult
from src.backend.quality.engines.validation_engine import ValidationEngine, ValidationResult
from src.backend.db.repositories.healing_repository import HealingRepository
from src.test.fixtures.backend.healing_fixtures import create_test_issue_classification, create_test_pattern, create_test_correction_result, generate_test_issue_data, TestHealingData
from src.test.fixtures.backend.quality_fixtures import create_test_validation_result
from src.test.utils.test_helpers import create_test_dataframe

@pytest.fixture
def setup_self_healing_components(config):
    """Sets up the self-healing components for integration testing"""
    healing_repository = unittest.mock.MagicMock(HealingRepository)
    issue_classifier = IssueClassifier(config)
    pattern_recognizer = PatternRecognizer(config)
    data_corrector = DataCorrector(config, healing_repository, None, None)
    return issue_classifier, pattern_recognizer, data_corrector, healing_repository

@pytest.fixture
def create_test_data_with_issues(issue_type, num_rows, num_issues):
    """Creates test data with specific issues for testing self-healing"""
    if issue_type == "missing_values":
        columns_spec = {
            'id': {'type': 'int', 'min': 1, 'max': 1000},
            'name': {'type': 'str', 'length': 10},
            'value': {'type': 'float', 'min': 0, 'max': 100},
            'active': {'type': 'bool'},
            'created_at': {'type': 'datetime', 'start': '2020-01-01', 'end': '2023-01-01'},
            'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
        }
    elif issue_type == "outliers":
        columns_spec = {
            'id': {'type': 'int', 'min': 1, 'max': 1000},
            'value': {'type': 'float', 'min': 0, 'max': 100}
        }
    elif issue_type == "format_errors":
        columns_spec = {
            'email': {'type': 'str', 'length': 20}
        }
    else:
        columns_spec = {}

    df = create_test_dataframe(columns_spec, num_rows)
    issue_details = {}

    if issue_type == "missing_values":
        target_column = "value"
        for _ in range(num_issues):
            index = numpy.random.randint(0, num_rows - 1)
            df.at[index, target_column] = None
            if target_column not in issue_details:
                issue_details[target_column] = []
            issue_details[target_column].append(index)

    elif issue_type == "outliers":
        target_column = "value"
        for _ in range(num_issues):
            index = numpy.random.randint(0, num_rows - 1)
            df.at[index, target_column] = 10000
            if target_column not in issue_details:
                issue_details[target_column] = []
            issue_details[target_column].append(index)

    elif issue_type == "format_errors":
        target_column = "email"
        for _ in range(num_issues):
            index = numpy.random.randint(0, num_rows - 1)
            df.at[index, target_column] = "invalid-email"
            if target_column not in issue_details:
                issue_details[target_column] = []
            issue_details[target_column].append(index)

    return df, issue_details

@pytest.mark.integration
def test_issue_classification_integration(test_healing_data):
    """Tests the integration of issue classification with pattern recognition"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    issue_data = test_healing_data.get_sample_issue(issue_type="missing_values")
    classification = issue_classifier.classify_issue(issue_data)
    assert classification.issue_type == "missing_values"
    assert classification.issue_category == "data_quality"
    assert classification.confidence >= DEFAULT_CONFIDENCE_THRESHOLD
    assert classification.recommended_action == HealingActionType.DATA_CORRECTION
    patterns = pattern_recognizer.find_matching_pattern(issue_data, "data_quality", DEFAULT_CONFIDENCE_THRESHOLD)
    assert len(patterns) == 1

@pytest.mark.integration
def test_data_correction_integration(test_healing_data):
    """Tests the integration of issue classification with data correction"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details

@pytest.mark.integration
def test_end_to_end_self_healing_process(test_healing_data):
    """Tests the complete end-to-end self-healing process"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    patterns = pattern_recognizer.find_matching_pattern(issue_data, "data_quality", DEFAULT_CONFIDENCE_THRESHOLD)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details
    healing_repository.record_correction_execution.assert_called()

@pytest.mark.integration
@pytest.mark.parametrize("issue_type", ["missing_values", "outliers", "format_errors", "schema_drift"])
def test_self_healing_with_different_issue_types(test_healing_data, issue_type):
    """Tests self-healing process with different types of data issues"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues(issue_type, num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data(issue_type, "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details

@pytest.mark.integration
@pytest.mark.parametrize("confidence_threshold", [0.5, 0.7, 0.9])
def test_self_healing_confidence_thresholds(test_healing_data, confidence_threshold):
    """Tests how different confidence thresholds affect self-healing decisions"""
    config = {"confidence_threshold": confidence_threshold, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    if classification.confidence >= confidence_threshold:
        success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
        assert success is True
    else:
        success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
        assert success is True

@pytest.mark.integration
def test_pattern_learning_from_corrections(test_healing_data):
    """Tests that the system learns patterns from successful corrections"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details
    healing_repository.record_correction_execution.assert_called()

@pytest.mark.integration
def test_self_healing_with_validation_integration(test_healing_data, mock_validation_engine):
    """Tests integration between validation engine and self-healing process"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details
    healing_repository.record_correction_execution.assert_called()

@pytest.mark.integration
def test_healing_repository_integration(test_healing_data):
    """Tests integration with healing repository for tracking healing actions"""
    config = {"confidence_threshold": 0.8, "learning_enabled": True, "healing_mode": "semi-automatic"}
    issue_classifier, pattern_recognizer, data_corrector, healing_repository = setup_self_healing_components(config)
    df, issue_details = create_test_data_with_issues("missing_values", num_rows=100, num_issues=10)
    issue_data = generate_test_issue_data("missing_values", "test_dataset", "test_table", "value")
    classification = issue_classifier.classify_issue(issue_data)
    success, correction_details = data_corrector.correct_data_issue(issue_data, classification, None)
    assert success is True
    assert "strategy" in correction_details
    healing_repository.record_correction_execution.assert_called()