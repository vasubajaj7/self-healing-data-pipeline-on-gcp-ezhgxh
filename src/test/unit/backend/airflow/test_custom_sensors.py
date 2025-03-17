"""
Unit tests for custom Airflow sensors used in the self-healing data pipeline.
Tests cover API, GCS, Cloud SQL, and Quality sensors, including their self-healing capabilities.
"""
import unittest.mock as mock
import pytest  # version 7.3.1
from requests import Response  # version 2.31.x
from sqlalchemy.exc import SQLAlchemyError  # version 2.0.x
from google.cloud.exceptions import NotFound  # version 2.9.0+

from src.test.utils.airflow_test_utils import AirflowTestCase, create_mock_airflow_context  # Import Airflow test utilities
from src.backend.airflow.plugins.custom_sensors.api_sensors import ApiSensor, ApiAvailabilitySensor, ApiResponseSensor, ApiDataAvailabilitySensor, SelfHealingApiAvailabilitySensor, SelfHealingApiDataAvailabilitySensor  # Import API sensors for testing
from src.backend.airflow.plugins.custom_sensors.gcs_sensors import GCSSensor, GCSFileExistenceSensor, GCSFilePatternSensor, GCSDataAvailabilitySensor, SelfHealingGCSFileExistenceSensor, SelfHealingGCSDataAvailabilitySensor, match_blob_pattern, validate_data_sample  # Import GCS sensors for testing
from src.backend.airflow.plugins.custom_sensors.cloudsql_sensors import CloudSQLSensor, CloudSQLTableExistenceSensor, CloudSQLTableDataAvailabilitySensor, CloudSQLTableValueSensor, SelfHealingCloudSQLSensor, SelfHealingCloudSQLTableExistenceSensor, SelfHealingCloudSQLTableDataAvailabilitySensor  # Import Cloud SQL sensors for testing
from src.backend.airflow.plugins.custom_sensors.quality_sensors import QualitySensor, QualityValidationCompletionSensor, QualityScoreSensor, QualityIssueDetectionSensor, SelfHealingQualitySensor, format_validation_context  # Import Quality sensors for testing
from src.backend.airflow.plugins.hooks.api_hooks import ApiHook, SelfHealingApiHook  # Import API hooks for mocking
from src.backend.airflow.plugins.hooks.gcs_hooks import EnhancedGCSHook, SelfHealingGCSHook  # Import GCS hooks for mocking
from src.backend.airflow.plugins.hooks.cloudsql_hooks import EnhancedCloudSQLHook, SelfHealingCloudSQLHook  # Import Cloud SQL hooks for mocking
from src.backend.constants import FileFormat, DEFAULT_CONFIDENCE_THRESHOLD, VALIDATION_STATUS_PASSED, VALIDATION_STATUS_FAILED  # Import constants used in sensors
from src.backend.quality.engines.validation_engine import ValidationEngine, ValidationResult, ValidationSummary  # Import validation engine for mocking quality tests
from src.backend.self_healing.ai.issue_classifier import IssueClassifier  # Import issue classifier for mocking self-healing tests
from src.backend.self_healing.correction.data_corrector import DataCorrector  # Import data corrector for mocking self-healing tests
from src.backend.db.repositories.quality_repository import QualityRepository  # Import quality repository for mocking quality tests


class TestApiSensors(AirflowTestCase):
    """Test cases for API sensors"""

    def setUp(self):
        """Set up test environment for API sensor tests"""
        super().setUp()
        self.mock_api_hook = mock.MagicMock(spec=ApiHook)
        self.mock_self_healing_api_hook = mock.MagicMock(spec=SelfHealingApiHook)
        self.context = create_mock_airflow_context(task_id='test_task', dag_id='test_dag')
        self.endpoint = 'https://testapi.com/data'
        self.request_params = {'param1': 'value1'}
        self.headers = {'header1': 'header_value'}

    def tearDown(self):
        """Clean up test environment after API sensor tests"""
        self.mock_api_hook = None
        self.mock_self_healing_api_hook = None
        self.context = None
        super().tearDown()

    def test_api_availability_sensor_success(self):
        """Test ApiAvailabilitySensor with successful API response"""
        self.mock_api_hook.get_request.return_value.response.status_code = 200
        sensor = ApiAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers)
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_api_availability_sensor_failure(self):
        """Test ApiAvailabilitySensor with failed API response"""
        self.mock_api_hook.get_request.return_value.response.status_code = 404
        sensor = ApiAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers)
        result = sensor.poke(self.context)
        self.assertFalse(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_api_response_sensor_success(self):
        """Test ApiResponseSensor with successful condition check"""
        self.mock_api_hook.get_request.return_value.json.return_value = {'status': 'success', 'data': [1, 2, 3]}
        sensor = ApiResponseSensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, response_check=lambda response_data: response_data['status'] == 'success')
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_api_response_sensor_failure(self):
        """Test ApiResponseSensor with failed condition check"""
        self.mock_api_hook.get_request.return_value.json.return_value = {'status': 'failed', 'data': []}
        sensor = ApiResponseSensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, response_check=lambda response_data: response_data['status'] == 'success')
        result = sensor.poke(self.context)
        self.assertFalse(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_api_data_availability_sensor_success(self):
        """Test ApiDataAvailabilitySensor with available data"""
        self.mock_api_hook.get_request.return_value.json.return_value = {'data': [1, 2, 3]}
        sensor = ApiDataAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, data_path='data')
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_api_data_availability_sensor_failure(self):
        """Test ApiDataAvailabilitySensor with insufficient data"""
        self.mock_api_hook.get_request.return_value.json.return_value = {'data': []}
        sensor = ApiDataAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, data_path='data')
        result = sensor.poke(self.context)
        self.assertFalse(result)
        self.mock_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_self_healing_api_availability_sensor_success(self):
        """Test SelfHealingApiAvailabilitySensor with successful API response"""
        self.mock_self_healing_api_hook.get_request.return_value.response.status_code = 200
        sensor = SelfHealingApiAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers)
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.mock_self_healing_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_self_healing_api_availability_sensor_with_healing(self):
        """Test SelfHealingApiAvailabilitySensor with failed response but successful healing"""
        self.mock_self_healing_api_hook.get_request.side_effect = [
            mock.MagicMock(status_code=500),  # First call fails
            mock.MagicMock(status_code=200)   # Second call succeeds after healing
        ]
        sensor = SelfHealingApiAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers)
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.assertEqual(self.mock_self_healing_api_hook.get_request.call_count, 2)

    def test_self_healing_api_availability_sensor_failure(self):
        """Test SelfHealingApiAvailabilitySensor with failed response and failed healing"""
        self.mock_self_healing_api_hook.get_request.return_value.status_code = 500
        self.mock_self_healing_api_hook.get_request.side_effect = requests.exceptions.RequestException("API failed")
        sensor = SelfHealingApiAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers)
        result = sensor.poke(self.context)
        self.assertFalse(result)
        self.assertEqual(self.mock_self_healing_api_hook.get_request.call_count, 1)

    def test_self_healing_api_data_availability_sensor_success(self):
        """Test SelfHealingApiDataAvailabilitySensor with available data"""
        self.mock_self_healing_api_hook.get_request.return_value.json.return_value = {'data': [1, 2, 3]}
        sensor = SelfHealingApiDataAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, data_path='data')
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.mock_self_healing_api_hook.get_request.assert_called_once_with(endpoint=self.endpoint, params=self.request_params, headers=self.headers)

    def test_self_healing_api_data_availability_sensor_with_healing(self):
        """Test SelfHealingApiDataAvailabilitySensor with insufficient data but successful healing"""
        self.mock_self_healing_api_hook.get_request.side_effect = [
            mock.MagicMock(json=lambda: {'data': []}),  # First call returns no data
            mock.MagicMock(json=lambda: {'data': [1, 2, 3]})   # Second call returns data after healing
        ]
        sensor = SelfHealingApiDataAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, data_path='data')
        result = sensor.poke(self.context)
        self.assertTrue(result)
        self.assertEqual(self.mock_self_healing_api_hook.get_request.call_count, 2)

    def test_self_healing_api_data_availability_sensor_failure(self):
        """Test SelfHealingApiDataAvailabilitySensor with insufficient data and failed healing"""
        self.mock_self_healing_api_hook.get_request.return_value.json.return_value = {'data': []}
        self.mock_self_healing_api_hook.get_request.side_effect = requests.exceptions.RequestException("API failed")
        sensor = SelfHealingApiDataAvailabilitySensor(task_id='api_sensor', conn_id='api_conn', endpoint=self.endpoint, request_params=self.request_params, headers=self.headers, data_path='data')
        result = sensor.poke(self.context)
        self.assertFalse(result)
        self.assertEqual(self.mock_self_healing_api_hook.get_request.call_count, 1)