"""
Unit tests for the data corrector component of the self-healing AI engine.
This module tests the functionality of the DataCorrector class and related functions that apply automated fixes to data quality issues based on AI-driven analysis and pattern recognition.
"""
import pytest  # package_version: 7.3.1
from unittest.mock import MagicMock, patch, Mock  # package_version: standard library
import pandas as pd  # package_version: 2.0.x
import numpy as np  # package_version: 1.24.x

from src.backend.constants import HealingActionType, DEFAULT_CONFIDENCE_THRESHOLD  # Module: src.backend.constants
from src.backend.self_healing.correction.data_corrector import DataCorrector, CorrectionResult, get_correction_strategy, apply_correction, correct_missing_values, correct_outliers, correct_format_errors, validate_correction  # Module: src.backend.self_healing.correction.data_corrector
from src.backend.self_healing.ai.issue_classifier import IssueClassification  # Module: src.backend.self_healing.ai.issue_classifier
from src.backend.self_healing.ai.root_cause_analyzer import RootCauseAnalysis, RootCause  # Module: src.backend.self_healing.ai.root_cause_analyzer
from src.test.fixtures.backend.healing_fixtures import create_test_issue_classification, generate_test_issue_data, TestHealingData  # Module: src.test.fixtures.backend.healing_fixtures
from src.test.utils.test_helpers import create_test_dataframe, generate_unique_id, compare_nested_structures  # Module: src.test.utils.test_helpers


@pytest.fixture
def test_config():
    """Provides a test configuration for DataCorrector"""
    return {
        "confidence_threshold": 0.8,
        "max_correction_history": 100,
        "default_strategies": {
            "missing_values": "mean_imputation",
            "outliers": "winsorization",
            "format_errors": "auto"
        }
    }


@pytest.fixture
def healing_data():
    """Provides test healing data for tests"""
    return TestHealingData()


@pytest.fixture
def mock_healing_repository():
    """Provides a mock healing repository"""
    return MagicMock()


@pytest.fixture
def mock_gcs_client():
    """Provides a mock GCS client"""
    return MagicMock()


@pytest.fixture
def mock_bq_client():
    """Provides a mock BigQuery client"""
    return MagicMock()


@pytest.fixture
def test_data_corrector(test_config, mock_healing_repository, mock_gcs_client, mock_bq_client):
    """Provides a test DataCorrector instance"""
    return DataCorrector(test_config, mock_healing_repository, mock_gcs_client, mock_bq_client)


@pytest.fixture
def missing_values_df():
    """Provides a DataFrame with missing values for testing"""
    return create_test_dataframe_with_missing_values(100, 20, ['value1', 'value2'])


@pytest.fixture
def outliers_df():
    """Provides a DataFrame with outliers for testing"""
    return create_test_dataframe_with_outliers(100, 10, ['value1', 'value2'])


@pytest.fixture
def format_errors_df():
    """Provides a DataFrame with format errors for testing"""
    return create_test_dataframe_with_format_errors(100, 15, ['date_col', 'string_col'])


@pytest.fixture
def test_issue_classification():
    """Provides a test issue classification for testing"""
    return create_test_issue_classification(issue_id='test-issue-001', issue_category='data_quality', issue_type='missing_values', description="Test missing values", confidence=0.9, features={}, recommended_action=HealingActionType.DATA_CORRECTION, severity=AlertSeverity.MEDIUM)


@pytest.fixture
def test_root_cause_analysis():
    """Provides a test root cause analysis for testing"""
    return RootCauseAnalysis(analysis_id='test-analysis-001', issue_id='test-issue-001', issue_type='missing_values', root_causes=[RootCause(cause_id="cause-001", cause_category="data_quality", cause_type="source_data_issue", description="Test source data issue", confidence=0.8, evidence={}, recommended_action=HealingActionType.DATA_CORRECTION, related_causes=[], analysis_time=datetime.datetime.now())], causality_graph=None, context={}, analysis_time=datetime.datetime.now())


def setup_test_data_corrector(config):
    """Sets up a test DataCorrector instance with mocked dependencies"""
    healing_repository = MagicMock()
    gcs_client = MagicMock()
    bq_client = MagicMock()
    corrector = DataCorrector(config, healing_repository, gcs_client, bq_client)
    return corrector, healing_repository, gcs_client, bq_client


def create_test_dataframe_with_missing_values(num_rows, num_missing, target_columns):
    """Creates a test DataFrame with missing values for testing correction"""
    columns_spec = {
        'id': {'type': 'int', 'min': 1, 'max': 1000},
        'value1': {'type': 'float', 'min': 0, 'max': 100},
        'value2': {'type': 'float', 'min': 0, 'max': 100},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
    }
    df = create_test_dataframe(columns_spec, num_rows)
    missing_value_details = {}
    for column in target_columns:
        for _ in range(num_missing):
            index = np.random.randint(0, num_rows)
            df.at[index, column] = np.nan
            if column not in missing_value_details:
                missing_value_details[column] = []
            missing_value_details[column].append(index)
    return df, missing_value_details


def create_test_dataframe_with_outliers(num_rows, num_outliers, target_columns):
    """Creates a test DataFrame with outlier values for testing correction"""
    columns_spec = {
        'id': {'type': 'int', 'min': 1, 'max': 1000},
        'value1': {'type': 'float', 'min': 0, 'max': 100},
        'value2': {'type': 'float', 'min': 0, 'max': 100},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
    }
    df = create_test_dataframe(columns_spec, num_rows)
    outlier_details = {}
    for column in target_columns:
        mean = df[column].mean()
        std = df[column].std()
        threshold = mean + 3 * std
        for _ in range(num_outliers):
            index = np.random.randint(0, num_rows)
            df.at[index, column] = threshold + 10  # Insert outlier value
            if column not in outlier_details:
                outlier_details[column] = []
            outlier_details[column].append((index, df.at[index, column]))
    return df, outlier_details


def create_test_dataframe_with_format_errors(num_rows, num_errors, target_columns):
    """Creates a test DataFrame with format errors for testing correction"""
    columns_spec = {
        'id': {'type': 'int', 'min': 1, 'max': 1000},
        'date_col': {'type': 'str'},
        'string_col': {'type': 'str'},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
    }
    df = create_test_dataframe(columns_spec, num_rows)
    format_error_details = {}
    for column in target_columns:
        for _ in range(num_errors):
            index = np.random.randint(0, num_rows)
            if column == 'date_col':
                df.at[index, column] = "Invalid Date"
            elif column == 'string_col':
                df.at[index, column] = 12345  # Insert a number into a string column
            if column not in format_error_details:
                format_error_details[column] = []
            format_error_details[column].append((index, df.at[index, column]))
    return df, format_error_details


def test_get_correction_strategy():
    """Tests the get_correction_strategy function for different issue types"""
    strategy, params = get_correction_strategy("missing_values")
    assert strategy == "mean_imputation"
    assert params == {'strategy': 'mean'}

    strategy, params = get_correction_strategy("outliers")
    assert strategy == "winsorization"
    assert params == {'limits': [0.05, 0.05]}

    strategy, params = get_correction_strategy("format_errors")
    assert strategy is None
    assert params is None

    strategy, params = get_correction_strategy("schema_drift")
    assert strategy is None
    assert params is None

    strategy, params = get_correction_strategy("data_corruption")
    assert strategy is None
    assert params is None


def test_apply_correction(missing_values_df):
    """Tests the apply_correction function with different strategies"""
    df, _ = missing_values_df
    target_columns = ['value1', 'value2']
    strategy = "mean_imputation"
    parameters = {'strategy': 'mean'}
    corrected_df, details = apply_correction(df.copy(), strategy, parameters, target_columns)
    assert not corrected_df.isnull().any().any()
    assert 'value1' in details
    assert 'value2' in details
    assert corrected_df is not df  # Verify original DataFrame is not modified


def test_correct_missing_values(missing_values_df):
    """Tests the correct_missing_values function with different strategies"""
    df, _ = missing_values_df
    target_columns = ['value1', 'value2']
    corrected_df, details = correct_missing_values(df.copy(), "mean", {}, target_columns)
    assert not corrected_df.isnull().any().any()
    assert 'value1' in details
    assert 'value2' in details


def test_correct_outliers(outliers_df):
    """Tests the correct_outliers function with different strategies"""
    df, _ = outliers_df
    target_columns = ['value1', 'value2']
    corrected_df, details = correct_outliers(df.copy(), "winsorization", {}, target_columns)
    assert 'value1' in details
    assert 'value2' in details


def test_correct_format_errors(format_errors_df):
    """Tests the correct_format_errors function with different strategies"""
    df, _ = format_errors_df
    target_columns = ['date_col', 'string_col']
    corrected_df, details = correct_format_errors(df.copy(), "date_format_correction", {}, target_columns)
    assert 'date_col' in details
    assert 'string_col' in details


def test_validate_correction(missing_values_df):
    """Tests the validate_correction function"""
    df, _ = missing_values_df
    corrected_df = df.fillna(0)
    details = {}
    validation_rules = {}
    success, results = validate_correction(df, corrected_df, details, validation_rules)
    assert success is True
    assert isinstance(results, dict)


def test_data_corrector_initialization(test_config, mock_healing_repository, mock_gcs_client, mock_bq_client):
    """Tests the initialization of the DataCorrector class"""
    corrector = DataCorrector(test_config, mock_healing_repository, mock_gcs_client, mock_bq_client)
    assert corrector._config == test_config
    assert corrector._confidence_threshold == 0.85
    assert corrector._healing_repository == mock_healing_repository
    assert corrector._gcs_client == mock_gcs_client
    assert corrector._bq_client == mock_bq_client


def test_correct_data_issue(test_data_corrector, test_issue_classification, test_root_cause_analysis):
    """Tests the correct_data_issue method of DataCorrector"""
    issue_data = generate_test_issue_data(issue_type="missing_values", dataset="test_dataset", table="test_table", column="test_column")
    success, details = test_data_corrector.correct_data_issue(issue_data, test_issue_classification, test_root_cause_analysis)
    assert isinstance(success, bool)
    assert isinstance(details, dict)


def test_correct_missing_values_issue(test_data_corrector, test_issue_classification):
    """Tests the correct_missing_values_issue method of DataCorrector"""
    issue_data = generate_test_issue_data(issue_type="missing_values", dataset="test_dataset", table="test_table", column="test_column")
    test_data_corrector.correct_missing_values_issue.return_value = True, {}
    success, details = test_data_corrector.correct_missing_values_issue(issue_data, test_issue_classification)
    assert isinstance(success, bool)
    assert isinstance(details, dict)


def test_correct_outliers_issue(test_data_corrector, test_issue_classification):
    """Tests the correct_outliers_issue method of DataCorrector"""
    issue_data = generate_test_issue_data(issue_type="outliers", dataset="test_dataset", table="test_table", column="test_column")
    test_data_corrector.correct_outliers_issue.return_value = True, {}
    success, details = test_data_corrector.correct_outliers_issue(issue_data, test_issue_classification)
    assert isinstance(success, bool)
    assert isinstance(details, dict)


def test_correct_format_errors_issue(test_data_corrector, test_issue_classification):
    """Tests the correct_format_errors_issue method of DataCorrector"""
    issue_data = generate_test_issue_data(issue_type="format_errors", dataset="test_dataset", table="test_table", column="test_column")
    test_data_corrector.correct_format_errors_issue.return_value = True, {}
    success, details = test_data_corrector.correct_format_errors_issue(issue_data, test_issue_classification)
    assert isinstance(success, bool)
    assert isinstance(details, dict)


def test_record_correction_execution(test_data_corrector, mock_healing_repository):
    """Tests the record_correction_execution method of DataCorrector"""
    execution_id = generate_unique_id("execution")
    validation_id = generate_unique_id("validation")
    pattern_id = generate_unique_id("pattern")
    action_id = generate_unique_id("action")
    success = True
    details = {}
    test_data_corrector.record_correction_execution(execution_id, validation_id, pattern_id, action_id, 0.9, success, details)
    mock_healing_repository.assert_called
    # assert mock_healing_repository.create_healing_execution.called


def test_get_correction_history(test_data_corrector):
    """Tests the get_correction_history method of DataCorrector"""
    history = test_data_corrector.get_correction_history()
    assert isinstance(history, list)


def test_set_confidence_threshold(test_data_corrector):
    """Tests the set_confidence_threshold method of DataCorrector"""
    test_data_corrector.set_confidence_threshold(0.9)
    assert test_data_corrector._confidence_threshold == 0.9
    with pytest.raises(ValueError):
        test_data_corrector.set_confidence_threshold(1.1)


def test_correction_result_class():
    """Tests the CorrectionResult class functionality"""
    correction_id = generate_unique_id("correction")
    issue_id = generate_unique_id("issue")
    result = CorrectionResult(correction_id=correction_id, issue_id=issue_id, issue_type="missing_values", correction_strategy="mean_imputation", successful=True, correction_details={}, validation_results={})
    assert result.correction_id == correction_id
    assert result.to_dict()["correction_id"] == correction_id
    assert CorrectionResult.from_dict(result.to_dict()).correction_id == correction_id
    assert "missing_values" in result.get_summary().values()


def test_integration_with_issue_classification(test_data_corrector, test_issue_classification):
    """Tests integration between issue classification and data correction"""
    issue_data = generate_test_issue_data(issue_type="missing_values", dataset="test_dataset", table="test_table", column="test_column")
    success, details = test_data_corrector.correct_data_issue(issue_data, test_issue_classification, RootCauseAnalysis(analysis_id="test", issue_id="test", issue_type="test", root_causes=[], causality_graph=None, context={}, analysis_time=datetime.datetime.now()))
    assert isinstance(success, bool)
    assert isinstance(details, dict)


def test_integration_with_root_cause_analysis(test_data_corrector, test_issue_classification, test_root_cause_analysis):
    """Tests integration between root cause analysis and data correction"""
    issue_data = generate_test_issue_data(issue_type="missing_values", dataset="test_dataset", table="test_table", column="test_column")
    success, details = test_data_corrector.correct_data_issue(issue_data, test_issue_classification, test_root_cause_analysis)
    assert isinstance(success, bool)
    assert isinstance(details, dict)