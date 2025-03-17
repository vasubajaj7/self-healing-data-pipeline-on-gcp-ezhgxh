"""
Provides pytest fixtures for testing the data ingestion components of the self-healing data pipeline.
This module contains fixtures for mocking connectors, extractors, and orchestration components
to facilitate unit and integration testing of the ingestion framework.
"""
import pytest
from unittest import mock
import typing
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import pandas
import io
import os
import json

from src.backend.constants import DataSourceType, FileFormat  # version: See src/backend/constants.py
from src.backend.ingestion.connectors.base_connector import BaseConnector, ConnectorFactory  # version: See src/backend/ingestion/connectors/base_connector.py
from src.backend.ingestion.connectors.gcs_connector import GCSConnector  # version: See src/backend/ingestion/connectors/gcs_connector.py
from src.backend.ingestion.connectors.cloudsql_connector import CloudSQLConnector  # version: See src/backend/ingestion/connectors/cloudsql_connector.py
from src.backend.ingestion.connectors.api_connector import ApiConnector, ApiAuthType, ApiPaginationType  # version: See src/backend/ingestion/connectors/api_connector.py
from src.backend.ingestion.extractors.file_extractor import FileExtractor, detect_file_format  # version: See src/backend/ingestion/extractors/file_extractor.py
from src.backend.ingestion.metadata.metadata_tracker import MetadataTracker  # version: See src/backend/ingestion/metadata/metadata_tracker.py
from src.backend.ingestion.orchestration.extraction_orchestrator import ExtractionOrchestrator  # version: See src/backend/ingestion/orchestration/extraction_orchestrator.py
from src.backend.utils.storage.gcs_client import GCSClient  # version: See src/backend/utils/storage/gcs_client.py
from src.test.utils.test_helpers import create_temp_file, load_test_data, MockResponseBuilder  # version: See src/test/utils/test_helpers.py

# Define the path to the sample data directory
SAMPLE_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'mock_data')

# Define sample configurations for different data sources
SAMPLE_GCS_CONFIG = {
    'project_id': 'test-project',
    'location': 'us-central1',
    'timeout': 30
}

SAMPLE_CLOUDSQL_CONFIG = {
    'db_type': 'postgres',
    'instance_connection_name': 'test-project:us-central1:test-instance',
    'database': 'test_db',
    'user': 'test_user',
    'password': 'test_password',
    'timeout': 30
}

SAMPLE_API_CONFIG = {
    'base_url': 'https://api.example.com/v1',
    'auth_type': 'API_KEY',
    'auth_config': {
        'api_key': 'test_api_key',
        'header_name': 'X-API-Key'
    },
    'timeout': 30,
    'verify_ssl': True
}


def load_sample_gcs_data(file_name: str) -> dict:
    """Loads sample GCS data from the test data directory"""
    file_path = os.path.join(SAMPLE_DATA_PATH, 'gcs', file_name)
    return load_test_data(file_path)


def load_sample_cloudsql_data(file_name: str) -> dict:
    """Loads sample Cloud SQL data from the test data directory"""
    file_path = os.path.join(SAMPLE_DATA_PATH, 'cloudsql', file_name)
    return load_test_data(file_path)


def load_sample_api_data(file_name: str) -> dict:
    """Loads sample API data from the test data directory"""
    file_path = os.path.join(SAMPLE_DATA_PATH, 'api', file_name)
    return load_test_data(file_path)


def create_mock_gcs_connector(extraction_results: dict, connection_success: bool) -> mock.MagicMock:
    """Creates a mock GCS connector for testing"""
    mock_builder = MockResponseBuilder()
    mock_builder.connection_successful = connection_success
    mock_builder.extraction_results = extraction_results
    return mock_builder.build_mock_gcs_connector()


def create_mock_cloudsql_connector(extraction_results: dict, connection_success: bool) -> mock.MagicMock:
    """Creates a mock Cloud SQL connector for testing"""
    mock_builder = MockResponseBuilder()
    mock_builder.connection_successful = connection_success
    mock_builder.extraction_results = extraction_results
    return mock_builder.build_mock_cloudsql_connector()


def create_mock_api_connector(extraction_results: dict, connection_success: bool) -> mock.MagicMock:
    """Creates a mock API connector for testing"""
    mock_builder = MockResponseBuilder()
    mock_builder.connection_successful = connection_success
    mock_builder.extraction_results = extraction_results
    return mock_builder.build_mock_api_connector()


def create_mock_file_extractor(extraction_results: dict) -> mock.MagicMock:
    """Creates a mock file extractor for testing"""
    mock_builder = MockResponseBuilder()
    mock_builder.extraction_results = extraction_results
    return mock_builder.build_mock_file_extractor()


@pytest.fixture
def mock_gcs_client() -> mock.MagicMock:
    """Pytest fixture providing a mock GCS client for testing"""
    with mock.patch('google.cloud.storage.Client') as mock_client:
        yield mock_client


@pytest.fixture
def mock_gcs_connector() -> mock.MagicMock:
    """Pytest fixture providing a mock GCS connector for testing"""
    mock_connector = mock.MagicMock(spec=GCSConnector)
    mock_connector.source_type = DataSourceType.GCS
    return mock_connector


@pytest.fixture
def mock_cloudsql_connector() -> mock.MagicMock:
    """Pytest fixture providing a mock Cloud SQL connector for testing"""
    mock_connector = mock.MagicMock(spec=CloudSQLConnector)
    mock_connector.source_type = DataSourceType.CLOUD_SQL
    return mock_connector


@pytest.fixture
def mock_api_connector() -> mock.MagicMock:
    """Pytest fixture providing a mock API connector for testing"""
    mock_connector = mock.MagicMock(spec=ApiConnector)
    mock_connector.source_type = DataSourceType.API
    return mock_connector


@pytest.fixture
def mock_file_extractor() -> mock.MagicMock:
    """Pytest fixture providing a mock file extractor for testing"""
    return mock.MagicMock(spec=FileExtractor)


@pytest.fixture
def mock_connector_factory() -> mock.MagicMock:
    """Pytest fixture providing a mock connector factory for testing"""
    return mock.MagicMock(spec=ConnectorFactory)


@pytest.fixture
def mock_metadata_tracker() -> mock.MagicMock:
    """Pytest fixture providing a mock metadata tracker for testing"""
    return mock.MagicMock(spec=MetadataTracker)


@pytest.fixture
def mock_extraction_orchestrator() -> mock.MagicMock:
    """Pytest fixture providing a mock extraction orchestrator for testing"""
    return mock.MagicMock(spec=ExtractionOrchestrator)


@pytest.fixture
def sample_gcs_config() -> dict:
    """Pytest fixture providing sample GCS configuration for testing"""
    return SAMPLE_GCS_CONFIG


@pytest.fixture
def sample_cloudsql_config() -> dict:
    """Pytest fixture providing sample Cloud SQL configuration for testing"""
    return SAMPLE_CLOUDSQL_CONFIG


@pytest.fixture
def sample_api_config() -> dict:
    """Pytest fixture providing sample API configuration for testing"""
    return SAMPLE_API_CONFIG


@pytest.fixture
def sample_extraction_config() -> dict:
    """Pytest fixture providing sample extraction configuration for testing"""
    return {
        'source_id': 'test-source',
        'source_name': 'Test Source',
        'source_type': DataSourceType.GCS,
        'extraction_params': {'bucket': 'test-bucket', 'file': 'test.csv'}
    }


class TestIngestionData:
    """Class for generating test ingestion data"""

    def __init__(self):
        """Initialize the TestIngestionData class"""
        self._gcs_templates = {}
        self._cloudsql_templates = {}
        self._api_templates = {}
        self._load_sample_data()

    def _load_sample_data(self):
        """Load sample data for different source types"""
        self._gcs_templates = {
            'full_load': load_sample_gcs_data('gcs_full_load.json'),
            'incremental_load': load_sample_gcs_data('gcs_incremental_load.json')
        }
        self._cloudsql_templates = {
            'full_load': load_sample_cloudsql_data('cloudsql_full_load.json'),
            'incremental_load': load_sample_cloudsql_data('cloudsql_incremental_load.json')
        }
        self._api_templates = {
            'full_load': load_sample_api_data('api_full_load.json'),
            'incremental_load': load_sample_api_data('api_incremental_load.json')
        }

    def generate_gcs_extraction_params(self, extraction_mode: str, overrides: dict = None) -> dict:
        """Generate GCS extraction parameters"""
        template = self._gcs_templates.get(extraction_mode, self._gcs_templates['full_load'])
        params = template.copy()
        if overrides:
            params.update(overrides)
        return params

    def generate_cloudsql_extraction_params(self, extraction_mode: str, overrides: dict = None) -> dict:
        """Generate Cloud SQL extraction parameters"""
        template = self._cloudsql_templates.get(extraction_mode, self._cloudsql_templates['full_load'])
        params = template.copy()
        if overrides:
            params.update(overrides)
        return params

    def generate_api_extraction_params(self, extraction_mode: str, overrides: dict = None) -> dict:
        """Generate API extraction parameters"""
        template = self._api_templates.get(extraction_mode, self._api_templates['full_load'])
        params = template.copy()
        if overrides:
            params.update(overrides)
        return params

    def generate_test_file(self, file_format: FileFormat, data: dict, file_name: str = None) -> str:
        """Generate a test file with specified format and content"""
        if file_name is None:
            file_name = f"test_file.{file_format.value.lower()}"

        if file_format == FileFormat.CSV:
            content = ",".join(data.keys()) + "\n" + ",".join(data.values())
        elif file_format == FileFormat.JSON:
            content = json.dumps(data)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        return create_temp_file(content=content, suffix=f".{file_format.value.lower()}", prefix="test_", dir_path=None)

    def generate_test_dataset(self, source_type: DataSourceType, num_rows: int, schema: dict = None) -> pandas.DataFrame:
        """Generate a test dataset for ingestion testing"""
        if source_type == DataSourceType.GCS:
            # Generate test data for GCS source
            data = {'col1': ['value1'] * num_rows, 'col2': ['value2'] * num_rows}
        elif source_type == DataSourceType.CLOUD_SQL:
            # Generate test data for Cloud SQL source
            data = {'id': range(1, num_rows + 1), 'name': ['test'] * num_rows}
        elif source_type == DataSourceType.API:
            # Generate test data for API source
            data = {'item_id': range(100, 100 + num_rows), 'description': ['api_data'] * num_rows}
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        return pandas.DataFrame(data)


@pytest.fixture
def test_ingestion_data() -> TestIngestionData:
    """Pytest fixture providing a TestIngestionData instance for testing"""
    return TestIngestionData()