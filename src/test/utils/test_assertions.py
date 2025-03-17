"""
Provides custom assertion functions and utilities for testing the self-healing data pipeline components.
This module contains specialized assertions for validating data quality, pipeline execution, BigQuery operations, and self-healing behaviors that go beyond standard pytest assertions.
"""

import pytest  # version 7.3.1
import typing  # standard library
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import pandas  # version 2.0.x
import numpy  # version 1.24.x

# Internal imports
from src.test.utils.test_helpers import compare_nested_structures, create_test_validation_result, create_test_validation_summary  # Import helper functions for test data creation and comparison
from src.test.utils.bigquery_test_utils import compare_dataframes_for_bigquery, verify_query_result  # Import BigQuery-specific test utilities
from src.test.utils.airflow_test_utils import assert_dag_expected_structure  # Import Airflow-specific test utilities
from src.backend.constants import ValidationRuleType, QualityDimension, HealingActionType, PipelineStatus  # Import constants for validation types, quality dimensions, and pipeline statuses
from src.backend.quality.engines.validation_engine import ValidationResult, ValidationSummary  # Import validation classes for assertion implementation
from src.backend.self_healing.ai.issue_classifier import IssueClassification  # Import issue classification classes for healing assertions

DEFAULT_FLOAT_TOLERANCE = 1e-6
DEFAULT_COMPARISON_KEYS = []


def assert_validation_result(
    actual: ValidationResult,
    rule_id: str = None,
    rule_name: str = None,
    rule_type: ValidationRuleType = None,
    dimension: QualityDimension = None,
    success: bool = None,
    details: dict = None
) -> None:
    """Asserts that a validation result matches expected values"""
    assert isinstance(actual, ValidationResult), "actual must be a ValidationResult instance"
    if rule_id is not None:
        assert actual.rule_id == rule_id, f"rule_id mismatch: expected {rule_id}, got {actual.rule_id}"
    if rule_name is not None:
        assert actual.rule_name == rule_name, f"rule_name mismatch: expected {rule_name}, got {actual.rule_name}"
    if rule_type is not None:
        assert actual.rule_type == rule_type, f"rule_type mismatch: expected {rule_type}, got {actual.rule_type}"
    if dimension is not None:
        assert actual.dimension == dimension, f"dimension mismatch: expected {dimension}, got {actual.dimension}"
    if success is not None:
        assert actual.success == success, f"success mismatch: expected {success}, got {actual.success}"
    if details is not None:
        assert all(k in actual.details and actual.details[k] == v for k, v in details.items()), f"details mismatch: expected {details}, got {actual.details}"


def assert_validation_summary(
    actual: ValidationSummary,
    expected_total: int = None,
    expected_passed: int = None,
    expected_failed: int = None,
    expected_quality_score: float = None,
    expected_results: list = None
) -> None:
    """Asserts that a validation summary matches expected values"""
    assert isinstance(actual, ValidationSummary), "actual must be a ValidationSummary instance"
    if expected_total is not None:
        assert actual.total_validations == expected_total, f"total_validations mismatch: expected {expected_total}, got {actual.total_validations}"
    if expected_passed is not None:
        assert actual.passed_validations == expected_passed, f"passed_validations mismatch: expected {expected_passed}, got {actual.passed_validations}"
    if expected_failed is not None:
        assert actual.failed_validations == expected_failed, f"failed_validations mismatch: expected {expected_failed}, got {actual.failed_validations}"
    if expected_quality_score is not None:
        assert abs(actual.quality_score - expected_quality_score) < DEFAULT_FLOAT_TOLERANCE, f"quality_score mismatch: expected {expected_quality_score}, got {actual.quality_score}"
    if expected_results is not None:
        assert all(result in actual.validation_results for result in expected_results), f"validation_results mismatch: expected {expected_results}, got {actual.validation_results}"


def assert_issue_classification(
    actual: IssueClassification,
    issue_id: str = None,
    category: str = None,
    issue_type: str = None,
    confidence: float = None,
    recommended_action: HealingActionType = None,
    context: dict = None
) -> None:
    """Asserts that an issue classification matches expected values"""
    assert isinstance(actual, IssueClassification), "actual must be an IssueClassification instance"
    if issue_id is not None:
        assert actual.issue_id == issue_id, f"issue_id mismatch: expected {issue_id}, got {actual.issue_id}"
    if category is not None:
        assert actual.issue_category == category, f"category mismatch: expected {category}, got {actual.issue_category}"
    if issue_type is not None:
        assert actual.issue_type == issue_type, f"issue_type mismatch: expected {issue_type}, got {actual.issue_type}"
    if confidence is not None:
        assert abs(actual.confidence - confidence) < DEFAULT_FLOAT_TOLERANCE, f"confidence mismatch: expected {confidence}, got {actual.confidence}"
    if recommended_action is not None:
        assert actual.recommended_action == recommended_action, f"recommended_action mismatch: expected {recommended_action}, got {actual.recommended_action}"
    if context is not None:
        assert all(k in actual.features and actual.features[k] == v for k, v in context.items()), f"context mismatch: expected {context}, got {actual.features}"


def assert_healing_action(
    actual: dict,
    action_id: str = None,
    action_type: HealingActionType = None,
    successful: bool = None,
    parameters: dict = None,
    result: dict = None
) -> None:
    """Asserts that a healing action matches expected values"""
    assert isinstance(actual, dict), "actual must be a dictionary"
    if action_id is not None:
        assert actual['action_id'] == action_id, f"action_id mismatch: expected {action_id}, got {actual['action_id']}"
    if action_type is not None:
        assert actual['action_type'] == action_type, f"action_type mismatch: expected {action_type}, got {actual['action_type']}"
    if successful is not None:
        assert actual['successful'] == successful, f"successful mismatch: expected {successful}, got {actual['successful']}"
    if parameters is not None:
        assert all(k in actual['parameters'] and actual['parameters'][k] == v for k, v in parameters.items()), f"parameters mismatch: expected {parameters}, got {actual['parameters']}"
    if result is not None:
        assert all(k in actual['result'] and actual['result'][k] == v for k, v in result.items()), f"result mismatch: expected {result}, got {actual['result']}"


def assert_pipeline_execution(
    actual: dict,
    pipeline_id: str = None,
    status: PipelineStatus = None,
    expected_tasks: int = None,
    expected_metadata: dict = None
) -> None:
    """Asserts that a pipeline execution matches expected values"""
    assert isinstance(actual, dict), "actual must be a dictionary"
    if pipeline_id is not None:
        assert actual['pipeline_id'] == pipeline_id, f"pipeline_id mismatch: expected {pipeline_id}, got {actual['pipeline_id']}"
    if status is not None:
        assert actual['status'] == status, f"status mismatch: expected {status}, got {actual['status']}"
    if expected_tasks is not None:
        assert len(actual['tasks']) == expected_tasks, f"number of tasks mismatch: expected {expected_tasks}, got {len(actual['tasks'])}"
    if expected_metadata is not None:
        assert all(k in actual['metadata'] and actual['metadata'][k] == v for k, v in expected_metadata.items()), f"metadata mismatch: expected {expected_metadata}, got {actual['metadata']}"


def assert_dataframes_equal(
    actual: pandas.DataFrame,
    expected: pandas.DataFrame,
    check_dtype: bool = True,
    check_index: bool = True,
    compare_columns: list = None,
    tolerance: float = DEFAULT_FLOAT_TOLERANCE
) -> None:
    """Asserts that two pandas DataFrames are equal with customizable comparison options"""
    if compare_columns:
        actual = actual[compare_columns]
        expected = expected[compare_columns]
    try:
        compare_dataframes_for_bigquery(actual, expected, check_dtype, check_index, tolerance)
    except AssertionError as e:
        raise AssertionError(f"DataFrames are not equal: {e}")


def assert_query_result(
    actual: pandas.DataFrame,
    expected: pandas.DataFrame,
    key_columns: list = None,
    tolerance: float = DEFAULT_FLOAT_TOLERANCE
) -> None:
    """Asserts that a query result matches expected data"""
    try:
        verify_query_result(actual, expected, key_columns, tolerance)
    except AssertionError as e:
        raise AssertionError(f"Query result does not match expected data: {e}")


def assert_dict_contains(
    actual: dict,
    expected: dict,
    exact_match: bool = False
) -> None:
    """Asserts that a dictionary contains all key-value pairs from expected dictionary"""
    for key, value in expected.items():
        assert key in actual, f"Missing key: {key}"
        assert actual[key] == value, f"Value mismatch for key {key}: expected {value}, got {actual[key]}"
    if exact_match:
        assert len(actual) == len(expected), f"Dictionary has extra keys: {set(actual.keys()) - set(expected.keys())}"


def assert_list_contains(
    actual: list,
    expected: list,
    exact_match: bool = False,
    comparison_keys: list = None
) -> None:
    """Asserts that a list contains all items from expected list"""
    if comparison_keys:
        actual = [{k: item[k] for k in comparison_keys if k in item} for item in actual]
        expected = [{k: item[k] for k in comparison_keys if k in item} for item in expected]
    for item in expected:
        assert item in actual, f"Missing item: {item}"
    if exact_match:
        assert len(actual) == len(expected), f"List has extra items: {set(actual) - set(expected)}"


def assert_nested_structure(
    actual: Any,
    expected: Any,
    ignore_keys: bool = False
) -> None:
    """Asserts that a nested structure (dict/list) matches expected structure"""
    match, message = compare_nested_structures(actual, expected, ignore_keys)
    if not match:
        raise AssertionError(f"Nested structure mismatch: {message}")


def assert_dag_structure(
    dag: 'airflow.models.DAG',
    expected_task_ids: list,
    expected_dependencies: dict
) -> None:
    """Asserts that a DAG has the expected structure"""
    try:
        assert_dag_expected_structure(dag, expected_task_ids, expected_dependencies)
    except AssertionError as e:
        raise AssertionError(f"DAG structure mismatch: {e}")


def assert_quality_dimension_scores(
    actual_scores: dict,
    expected_scores: dict,
    tolerance: float = DEFAULT_FLOAT_TOLERANCE
) -> None:
    """Asserts that quality dimension scores match expected values"""
    for dimension, expected_score in expected_scores.items():
        assert dimension in actual_scores, f"Missing dimension: {dimension}"
        actual_score = actual_scores[dimension]
        assert abs(actual_score - expected_score) < tolerance, f"Score mismatch for dimension {dimension}: expected {expected_score}, got {actual_score}"


def assert_healing_success_rate(
    healing_stats: dict,
    min_success_rate: float,
    min_attempts: int
) -> None:
    """Asserts that healing success rate meets expected threshold"""
    total_attempts = healing_stats['total_attempts']
    successful_attempts = healing_stats['successful_attempts']
    assert total_attempts >= min_attempts, f"Not enough healing attempts: expected at least {min_attempts}, got {total_attempts}"
    success_rate = successful_attempts / total_attempts
    assert success_rate >= min_success_rate, f"Healing success rate below threshold: expected at least {min_success_rate}, got {success_rate}"


def assert_performance_within_threshold(
    actual_metrics: dict,
    threshold_metrics: dict,
    lower_is_better: bool
) -> None:
    """Asserts that performance metrics are within expected thresholds"""
    for metric, threshold in threshold_metrics.items():
        assert metric in actual_metrics, f"Missing metric: {metric}"
        actual_value = actual_metrics[metric]
        if lower_is_better:
            assert actual_value <= threshold, f"Metric {metric} exceeds threshold: expected <= {threshold}, got {actual_value}"
        else:
            assert actual_value >= threshold, f"Metric {metric} below threshold: expected >= {threshold}, got {actual_value}"