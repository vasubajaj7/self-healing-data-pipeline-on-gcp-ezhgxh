"""
Provides pytest fixtures for testing Airflow components in the self-healing data pipeline.
Includes mocks for DAGs, operators, hooks, and task contexts to enable isolated testing of Airflow functionality
without requiring a running Airflow environment.
"""

import pytest
from unittest.mock import MagicMock, patch
import typing
from typing import Dict, List, Optional, Union, Callable, Tuple
import pandas  # package: pandas, version: 2.0.x
import datetime  # package: datetime, version: standard library
import airflow  # package: apache-airflow, version: 2.5.x
from airflow.models import DAG  # package: apache-airflow, version: 2.5.x
from airflow.utils.dates import days_ago  # package: apache-airflow, version: 2.5.x

from src.backend import constants  # Import constants used in Airflow components
from src.backend.constants import FileFormat, DEFAULT_CONFIDENCE_THRESHOLD  # Import constants used in Airflow components
from src.backend.airflow.plugins.hooks.gcs_hooks import EnhancedGCSHook, SelfHealingGCSHook  # Import GCS hooks for mocking
from src.backend.airflow.plugins.custom_operators.gcs_operators import GCSListOperator, SelfHealingGCSToDataFrameOperator, SelfHealingGCSToBigQueryOperator  # Import GCS operators for mocking
from src.backend.airflow.plugins.custom_operators.quality_operators import GCSDataQualityValidationOperator, QualityBasedBranchOperator  # Import quality operators for mocking
from src.backend.self_healing.ai.issue_classifier import IssueClassifier  # Import issue classifier for mocking self-healing functionality
from src.backend.self_healing.correction.data_corrector import DataCorrector  # Import data corrector for mocking self-healing functionality
from src.test.utils.test_helpers import create_temp_file  # Import test helper utilities

# Define default DAG arguments
DEFAULT_DAG_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
    "start_date": airflow.utils.dates.days_ago(1)
}

# Define sample GCS data
SAMPLE_GCS_DATA = {
    "bucket_name": "test-bucket",
    "blob_name": "test-data.csv",
    "content": "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300"
}

# Define sample quality rules
SAMPLE_QUALITY_RULES = [
    {"rule_type": "schema", "field": "id", "expectation": "not_null"},
    {"rule_type": "schema", "field": "name", "expectation": "not_null"},
    {"rule_type": "value", "field": "value", "expectation": "greater_than", "value": 0}
]

@pytest.fixture
def mock_gcs_hook():
    """Pytest fixture providing a mock EnhancedGCSHook for testing"""
    mock = MockGCSHook(gcp_conn_id='test_gcp_conn')
    return mock

@pytest.fixture
def mock_self_healing_gcs_hook():
    """Pytest fixture providing a mock SelfHealingGCSHook for testing"""
    mock = MockSelfHealingGCSHook(gcp_conn_id='test_gcp_conn', confidence_threshold=DEFAULT_CONFIDENCE_THRESHOLD)
    return mock

@pytest.fixture
def mock_airflow_context():
    """Pytest fixture providing a mock Airflow context for testing operators"""
    def _create_mock_airflow_context(task_instance_xcom_values: dict = None, additional_context: dict = None):
        return create_mock_airflow_context(task_instance_xcom_values, additional_context)
    return _create_mock_airflow_context

@pytest.fixture
def mock_task_instance():
    """Pytest fixture providing a mock Airflow TaskInstance for testing"""
    def _create_mock_task_instance(task_id: str = None, xcom_values: dict = None):
        return create_mock_task_instance(task_id, xcom_values)
    return _create_mock_task_instance

@pytest.fixture
def mock_dag():
    """Pytest fixture providing a mock Airflow DAG for testing"""
    mock = MagicMock(spec=DAG)
    return mock

@pytest.fixture
def sample_dataframe():
    """Pytest fixture providing a sample pandas DataFrame for testing"""
    def _create_sample_dataframe(rows: int = 3, columns: list = ['id', 'name', 'value']):
        return create_sample_dataframe(rows, columns)
    return _create_sample_dataframe

@pytest.fixture
def quality_validation_result():
    """Pytest fixture providing a sample quality validation result for testing"""
    def _create_quality_validation_result(quality_score: float = 0.95, rules_passed: int = 10, rules_failed: int = 2):
        return create_quality_validation_result(quality_score, rules_passed, rules_failed)
    return _create_quality_validation_result

@pytest.fixture
def mock_issue_classifier():
    """Pytest fixture providing a mock IssueClassifier for testing self-healing"""
    mock = MagicMock(spec=IssueClassifier)
    return mock

@pytest.fixture
def mock_data_corrector():
    """Pytest fixture providing a mock DataCorrector for testing self-healing"""
    mock = MagicMock(spec=DataCorrector)
    return mock

def create_mock_task_instance(task_id: str = None, xcom_values: dict = None):
    """Creates a mock Airflow TaskInstance for testing

    Args:
        task_id (str): task_id
        xcom_values (dict): xcom_values

    Returns:
        unittest.mock.MagicMock: Mock TaskInstance with xcom_pull functionality
    """
    task_instance = MagicMock()
    task_instance.task_id = task_id

    def xcom_pull(key=None, task_ids=None):
        if task_ids and task_ids in xcom_values and key in xcom_values[task_ids]:
            return xcom_values[task_ids][key]
        return None
    task_instance.xcom_pull = xcom_pull
    return task_instance

def create_mock_airflow_context(task_instance_xcom_values: dict = None, additional_context: dict = None):
    """Creates a mock Airflow context dictionary for testing operators

    Args:
        task_instance_xcom_values (dict): task_instance_xcom_values
        additional_context (dict): additional_context

    Returns:
        dict: Mock Airflow context dictionary
    """
    task_instance = create_mock_task_instance(xcom_values=task_instance_xcom_values)
    context = {
        'dag': MagicMock(),
        'task': MagicMock(),
        'ti': task_instance,
        'dag_run': MagicMock(),
        'execution_date': datetime.datetime.now()
    }
    if additional_context:
        context.update(additional_context)
    return context

def create_sample_dataframe(rows: int = 3, columns: list = ['id', 'name', 'value']):
    """Creates a sample pandas DataFrame for testing

    Args:
        rows (int): rows
        columns (list): columns

    Returns:
        pandas.DataFrame: Sample DataFrame with specified dimensions
    """
    data = {}
    for column in columns:
        data[column] = [i for i in range(rows)]
    df = pandas.DataFrame(data)
    return df

def create_quality_validation_result(quality_score: float = 0.95, rules_passed: int = 10, rules_failed: int = 2):
    """Creates a sample quality validation result for testing

    Args:
        quality_score (float): quality_score
        rules_passed (int): rules_passed
        rules_failed (int): rules_failed

    Returns:
        dict: Sample validation result dictionary
    """
    return {
        'quality_score': quality_score,
        'rules_passed': rules_passed,
        'rules_failed': rules_failed
    }

class MockGCSHook:
    """Mock implementation of EnhancedGCSHook for testing"""

    def __init__(self, gcp_conn_id: str, files: dict = None, buckets: dict = None):
        """Initialize the mock GCS hook

        Args:
            gcp_conn_id (str): gcp_conn_id
            files (dict): files
            buckets (dict): buckets
        """
        self.gcp_conn_id = gcp_conn_id
        self.files = files or {}
        self.buckets = buckets or {}

    def list_files(self, bucket_name: str, prefix: str = None, delimiter: str = '/'):
        """Mock implementation of list_files method

        Args:
            bucket_name (str): bucket_name
            prefix (str): prefix
            delimiter (str): delimiter

        Returns:
            list: List of mock blob objects
        """
        if bucket_name not in self.buckets:
            return []
        files = self.files.get(bucket_name, [])
        if prefix:
            files = [f for f in files if f.startswith(prefix)]
        return [MagicMock(name=f) for f in files]

    def download_file(self, bucket_name: str, blob_name: str, local_file_path: str):
        """Mock implementation of download_file method

        Args:
            bucket_name (str): bucket_name
            blob_name (str): blob_name
            local_file_path (str): local_file_path

        Returns:
            str: Local file path
        """
        if bucket_name not in self.buckets or blob_name not in self.files.get(bucket_name, []):
            raise Exception("File not found")
        with open(local_file_path, 'w') as f:
            f.write(self.files[bucket_name][blob_name])
        return local_file_path

    def upload_file(self, local_file_path: str, bucket_name: str, blob_name: str, content_type: str = None, metadata: dict = None):
        """Mock implementation of upload_file method

        Args:
            local_file_path (str): local_file_path
            bucket_name (str): bucket_name
            blob_name (str): blob_name
            content_type (str): content_type
            metadata (dict): metadata

        Returns:
            unittest.mock.MagicMock: Mock blob object
        """
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = MagicMock()
        if bucket_name not in self.files:
            self.files[bucket_name] = {}
        with open(local_file_path, 'r') as f:
            self.files[bucket_name][blob_name] = f.read()
        return MagicMock()

    def read_file_as_dataframe(self, bucket_name: str, blob_name: str, file_format: FileFormat = None, read_options: dict = None):
        """Mock implementation of read_file_as_dataframe method

        Args:
            bucket_name (str): bucket_name
            blob_name (str): blob_name
            file_format (FileFormat): file_format
            read_options (dict): read_options

        Returns:
            pandas.DataFrame: DataFrame containing file data
        """
        if bucket_name not in self.buckets or blob_name not in self.files.get(bucket_name, []):
            raise Exception("File not found")
        content = self.files[bucket_name][blob_name]
        if file_format == FileFormat.CSV:
            df = pandas.read_csv(io.StringIO(content))
        else:
            df = pandas.DataFrame({'content': [content]})
        return df

    def file_exists(self, bucket_name: str, blob_name: str):
        """Mock implementation of file_exists method

        Args:
            bucket_name (str): bucket_name
            blob_name (str): blob_name

        Returns:
            bool: True if file exists
        """
        return bucket_name in self.buckets and blob_name in self.files.get(bucket_name, [])

class MockSelfHealingGCSHook:
    """Mock implementation of SelfHealingGCSHook for testing"""

    def __init__(self, gcp_conn_id: str, files: dict = None, buckets: dict = None, confidence_threshold: float = 0.85):
        """Initialize the mock self-healing GCS hook

        Args:
            gcp_conn_id (str): gcp_conn_id
            files (dict): files
            buckets (dict): buckets
            confidence_threshold (float): confidence_threshold
        """
        self.gcp_conn_id = gcp_conn_id
        self.files = files or {}
        self.buckets = buckets or {}
        self.confidence_threshold = confidence_threshold
        self.issue_classifier = MagicMock()
        self.data_corrector = MagicMock()

    def read_file_as_dataframe(self, bucket_name: str, blob_name: str, file_format: FileFormat = None, read_options: dict = None, attempt_healing: bool = True):
        """Mock implementation of read_file_as_dataframe method with self-healing

        Args:
            bucket_name (str): bucket_name
            blob_name (str): blob_name
            file_format (FileFormat): file_format
            read_options (dict): read_options
            attempt_healing (bool): attempt_healing

        Returns:
            pandas.DataFrame: DataFrame containing file data
        """
        if bucket_name not in self.buckets or blob_name not in self.files.get(bucket_name, []):
            if attempt_healing:
                # Simulate self-healing by creating the file
                self.buckets[bucket_name] = MagicMock()
                self.files[bucket_name] = {blob_name: "Simulated content"}
            else:
                raise Exception("File not found")
        content = self.files[bucket_name][blob_name]
        if file_format == FileFormat.CSV:
            df = pandas.read_csv(io.StringIO(content))
        else:
            df = pandas.DataFrame({'content': [content]})
        return df

    def get_issue_classifier(self):
        """Returns the mock issue classifier

        Args:

        Returns:
            unittest.mock.MagicMock: Mock issue classifier
        """
        return self.issue_classifier

    def get_data_corrector(self):
        """Returns the mock data corrector

        Args:

        Returns:
            unittest.mock.MagicMock: Mock data corrector
        """
        return self.data_corrector

    def _apply_healing_action(self, error: Exception, context: dict):
        """Mock implementation of _apply_healing_action method

        Args:
            error (Exception): error
            context (dict): context

        Returns:
            dict: Healing result with action taken and confidence
        """
        # Simulate issue classification and correction
        return {"action_taken": "Simulated", "confidence": 0.9}