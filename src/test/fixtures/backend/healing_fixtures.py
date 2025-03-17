# src/test/fixtures/backend/healing_fixtures.py
"""Provides test fixtures for the self-healing AI engine components.
This module contains mock objects, sample data, and utility functions to facilitate testing of issue classification, pattern recognition, and data correction capabilities of the self-healing system.
"""
import pytest  # package_version: 7.3.1
import unittest.mock  # package_version: standard library
import pandas  # package_version: 2.0.x
import numpy  # package_version: 1.24.x
import json  # package_version: standard library
import os  # package_version: standard library
import datetime  # package_version: standard library
from src.backend.constants import HealingActionType, AlertSeverity, DEFAULT_CONFIDENCE_THRESHOLD  # Module: src.backend.constants
from src.backend.self_healing.ai.issue_classifier import IssueClassification, IssueClassifier  # Module: src.backend.self_healing.ai.issue_classifier
from src.backend.self_healing.ai.pattern_recognizer import Pattern, PatternRecognizer  # Module: src.backend.self_healing.ai.pattern_recognizer
from src.backend.self_healing.correction.data_corrector import DataCorrector, CorrectionResult  # Module: src.backend.self_healing.correction.data_corrector
from src.test.utils.test_helpers import create_test_dataframe, create_test_healing_action, generate_unique_id, TestDataGenerator  # Module: src.test.utils.test_helpers

SAMPLE_ISSUES_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'mock_data', 'healing', 'sample_issues.json')
SAMPLE_CORRECTIONS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'mock_data', 'healing', 'sample_corrections.json')


def load_sample_issues():
    """Loads sample issue data from the sample_issues.json file"""
    with open(SAMPLE_ISSUES_PATH, 'r') as f:
        sample_issues = json.load(f)
    return sample_issues


def load_sample_corrections():
    """Loads sample correction data from the sample_corrections.json file"""
    with open(SAMPLE_CORRECTIONS_PATH, 'r') as f:
        sample_corrections = json.load(f)
    return sample_corre


def create_mock_issue_classifier(config, confidence_score, issue_category, issue_type):
    """Creates a mock IssueClassifier for testing"""
    mock_classifier = unittest.mock.MagicMock(spec=IssueClassifier)
    mock_classification = unittest.mock.MagicMock(spec=IssueClassification)
    mock_classification.confidence = confidence_score
    mock_classification.issue_category = issue_category
    mock_classification.issue_type = issue_type
    mock_classifier.classify_issue.return_value = mock_classification
    mock_classifier.get_classification_history.return_value = [mock_classification]
    return mock_classifier


def create_mock_pattern_recognizer(config, confidence_score, patterns):
    """Creates a mock PatternRecognizer for testing"""
    mock_recognizer = unittest.mock.MagicMock(spec=PatternRecognizer)
    mock_pattern = unittest.mock.MagicMock(spec=Pattern)
    mock_recognizer.recognize_pattern.return_value = (mock_pattern, confidence_score)
    mock_recognizer.find_matching_pattern.return_value = [(mock_pattern, confidence_score)]
    mock_recognizer.get_all_patterns.return_value = patterns
    return mock_recognizer


def create_mock_data_corrector(config, success, correction_details):
    """Creates a mock DataCorrector for testing"""
    mock_corrector = unittest.mock.MagicMock(spec=DataCorrector)
    mock_corrector.correct_data_issue.return_value = (success, correction_details)
    mock_corrector.correct_missing_values_issue.return_value = (success, correction_details)
    mock_corrector.correct_outliers_issue.return_value = (success, correction_details)
    mock_corrector.correct_format_errors_issue.return_value = (success, correction_details)
    mock_corrector.correct_schema_drift_issue.return_value = (success, correction_details)
    mock_corrector.get_correction_history.return_value = []
    return mock_corrector


def create_test_issue_classification(issue_id, issue_category, issue_type, description, confidence, features, recommended_action, severity):
    """Creates a test IssueClassification instance for testing"""
    classification_id = generate_unique_id("classification")
    return IssueClassification(
        classification_id=classification_id,
        issue_id=issue_id,
        issue_category=issue_category,
        issue_type=issue_type,
        description=description,
        confidence=confidence,
        features=features,
        recommended_action=recommended_action,
        severity=severity
    )


def create_test_pattern(pattern_id, name, pattern_type, description, features, confidence_threshold, healing_actions, occurrence_count, success_rate):
    """Creates a test Pattern instance for testing"""
    pattern_id = generate_unique_id("pattern")
    return Pattern(
        pattern_id=pattern_id,
        name=name,
        pattern_type=pattern_type,
        description=description,
        features=features,
        confidence_threshold=confidence_threshold,
        occurrence_count=occurrence_count,
        success_rate=success_rate,
        healing_actions=healing_actions
    )


def create_test_correction_result(correction_id, issue_id, issue_type, correction_strategy, successful, correction_details, validation_results):
    """Creates a test CorrectionResult instance for testing"""
    correction_id = generate_unique_id("correction")
    return CorrectionResult(
        correction_id=correction_id,
        issue_id=issue_id,
        issue_type=issue_type,
        correction_strategy=correction_strategy,
        successful=successful,
        correction_details=correction_details,
        validation_results=validation_results
    )


def generate_test_issue_data(issue_type, dataset, table, column):
    """Generates test issue data for a specific issue type"""
    issue_id = generate_unique_id("issue")
    if issue_type in ["missing_values", "format_errors", "outliers"]:
        issue_category = "data_quality"
    else:
        issue_category = "pipeline"
    issue_data = {
        "issue_id": issue_id,
        "dataset": dataset,
        "table": table,
        "column": column,
        "issue_type": issue_type,
        "issue_category": issue_category
    }
    if issue_type == "missing_values":
        issue_data["missing_count"] = 10
    elif issue_type == "format_errors":
        issue_data["invalid_count"] = 5
    elif issue_type == "outliers":
        issue_data["outlier_count"] = 3
    return issue_data


class TestHealingData:
    """Class providing test data for self-healing components"""

    def __init__(self):
        """Initialize the TestHealingData with sample data"""
        self.sample_issues = load_sample_issues()
        self.sample_corrections = load_sample_corrections()
        self.issue_types = {
            "missing_values": "Data Imputation",
            "format_errors": "Data Formatting",
            "outliers": "Outlier Removal"
        }
        self.correction_strategies = {
            "missing_values": ["mean_imputation", "median_imputation"],
            "format_errors": ["standardize_date_format", "standardize_phone_number"],
            "outliers": ["winsorization", "trimming"]
        }

    def get_sample_issue(self, issue_id=None, issue_type=None):
        """Get a sample issue by ID or type"""
        if issue_id:
            for issue in self.sample_issues:
                if issue["issue_id"] == issue_id:
                    return issue.copy()
        elif issue_type:
            for issue in self.sample_issues:
                if issue["type"] == issue_type:
                    return issue.copy()
        if self.sample_issues:
            return self.sample_issues[0].copy()
        return None

    def get_sample_correction(self, correction_id=None, issue_id=None):
        """Get a sample correction by ID or related issue ID"""
        if correction_id:
            for correction in self.sample_corrections:
                if correction["correction_id"] == correction_id:
                    return correction.copy()
        elif issue_id:
            for correction in self.sample_corrections:
                if correction["issue_id"] == issue_id:
                    return correction.copy()
        if self.sample_corrections:
            return self.sample_corrections[0].copy()
        return None

    def get_issues_by_type(self, issue_type):
        """Get all sample issues of a specific type"""
        return [issue.copy() for issue in self.sample_issues if issue["type"] == issue_type]

    def get_corrections_by_type(self, issue_type):
        """Get all sample corrections for a specific issue type"""
        return [correction.copy() for correction in self.sample_corrections if correction["issue_type"] == issue_type]

    def get_correction_strategy(self, issue_type):
        """Get an appropriate correction strategy for an issue type"""
        if issue_type in self.correction_strategies:
            return self.correction_strategies[issue_type][0]
        return None

    def create_test_dataframe_with_issues(self, issue_type, num_rows=100, num_issues=10, columns_spec=None):
        """Create a test DataFrame with embedded data quality issues"""
        if columns_spec is None:
            columns_spec = {
                'id': {'type': 'int', 'min': 1, 'max': 1000},
                'name': {'type': 'str', 'length': 10},
                'score': {'type': 'float', 'min': 0, 'max': 100},
                'active': {'type': 'bool'},
                'created_at': {'type': 'datetime', 'start': '2020-01-01', 'end': '2023-01-01'},
                'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
            }

        df = create_test_dataframe(columns_spec, num_rows)
        issue_details = {}

        if issue_type == "missing_values":
            # Inject missing values into a random column
            target_column = random.choice(list(columns_spec.keys()))
            for _ in range(num_issues):
                index = random.randint(0, num_rows - 1)
                df.at[index, target_column] = None
                if target_column not in issue_details:
                    issue_details[target_column] = []
                issue_details[target_column].append(index)

        elif issue_type == "format_errors":
            # Inject format errors into a string column
            string_columns = [col for col, spec in columns_spec.items() if spec.get('type') == 'str']
            if string_columns:
                target_column = random.choice(string_columns)
                for _ in range(num_issues):
                    index = random.randint(0, num_rows - 1)
                    df.at[index, target_column] = "Invalid Format"
                    if target_column not in issue_details:
                        issue_details[target_column] = []
                    issue_details[target_column].append(index)

        elif issue_type == "outliers":
            # Inject outliers into a numeric column
            numeric_columns = [col for col, spec in columns_spec.items() if spec.get('type') in ['int', 'float']]
            if numeric_columns:
                target_column = random.choice(numeric_columns)
                for _ in range(num_issues):
                    index = random.randint(0, num_rows - 1)
                    df.at[index, target_column] = 10000  # Inject a large outlier
                    if target_column not in issue_details:
                        issue_details[target_column] = []
                    issue_details[target_column].append(index)

        return df, issue_details