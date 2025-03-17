"""
Pytest configuration file for integration tests in the self-healing data pipeline project.

This module provides fixtures and configurations for integration testing,
focusing on interactions between components with real or emulated GCP services.
It manages test environment setup, resource creation and cleanup, and provides
utilities for consistent and isolated integration testing.
"""

import pytest
import os
import tempfile
import logging
import pandas as pd
from unittest.mock import MagicMock, patch

# Import shared fixtures
from src.test.fixtures.conftest import test_resource_manager, temp_dir

# Import GCP testing utilities
from src.test.utils.gcp_test_utils import (
    GCPTestContext,
    GCSEmulator,
    BigQueryEmulator,
    VertexAIEmulator
)

# Import testing helpers
from src.test.utils.test_helpers import (
    TestResourceManager,
    cleanup_temp_resources,
    create_test_dataframe
)

# Import constants
from src.backend.constants import DataSourceType, FileFormat

# Define global constants
INTEGRATION_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
INTEGRATION_TEST_CONFIG = {
    'use_emulators': True,
    'test_project_id': 'test-project-id',
    'test_location': 'us-central1',
    'cleanup_resources': True
}


def pytest_configure(config):
    """
    Configure pytest for integration tests.
    
    Args:
        config: Pytest configuration object
    """
    # Register custom markers for integration tests
    config.addinivalue_line(
        "markers", "integration: mark a test as an integration test"
    )
    config.addinivalue_line(
        "markers", "gcp: mark a test as requiring GCP services"
    )
    config.addinivalue_line(
        "markers", "slow: mark a test as slow running"
    )
    
    # Configure logging for integration tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up integration test specific config
    if not hasattr(config, 'integration_test_config'):
        config.integration_test_config = INTEGRATION_TEST_CONFIG


def pytest_collection_modifyitems(config, items):
    """
    Modify collected test items for integration tests.
    
    Args:
        config: Pytest configuration object
        items: List of collected test items
    """
    # Add integration marker to all tests in this directory
    for item in items:
        if INTEGRATION_TEST_DIR in os.path.dirname(item.fspath):
            item.add_marker(pytest.mark.integration)
    
    # Skip tests that require real GCP if using emulators
    if config.integration_test_config.get('use_emulators', True):
        skip_real_gcp = pytest.mark.skip(reason="Test requires real GCP services")
        for item in items:
            if "real_gcp" in item.keywords:
                item.add_marker(skip_real_gcp)
    
    # Reorder integration tests to ensure proper execution order
    # Run setup tests first, then main tests, then cleanup tests
    def get_test_priority(item):
        if "setup" in item.nodeid:
            return 0
        elif "cleanup" in item.nodeid:
            return 2
        return 1
    
    items.sort(key=get_test_priority)


def pytest_sessionstart(session):
    """
    Set up resources needed for the entire test session.
    
    Args:
        session: Pytest session object
    """
    # Create global temporary directory for test resources
    temp_dir = tempfile.mkdtemp(prefix="pipeline_integration_test_")
    session.integration_test_temp_dir = temp_dir
    
    # Set up shared test resources
    session.integration_test_resource_manager = TestResourceManager()
    
    # Set up GCP emulators if configured to use them
    if session.config.integration_test_config.get('use_emulators', True):
        session.integration_test_context = GCPTestContext(
            project_id=session.config.integration_test_config.get('test_project_id'),
            location=session.config.integration_test_config.get('test_location')
        )
        session.integration_test_context.__enter__()


def pytest_sessionfinish(session, exitstatus):
    """
    Clean up resources after the entire test session.
    
    Args:
        session: Pytest session object
        exitstatus: Exit status code
    """
    # Clean up GCP emulators if they were used
    if hasattr(session, 'integration_test_context'):
        session.integration_test_context.__exit__(None, None, None)
    
    # Clean up all test resources
    if hasattr(session, 'integration_test_resource_manager'):
        session.integration_test_resource_manager.cleanup()
    
    # Clean up temporary resources
    if hasattr(session, 'integration_test_temp_dir') and INTEGRATION_TEST_CONFIG.get('cleanup_resources', True):
        try:
            cleanup_temp_resources()
            if os.path.exists(session.integration_test_temp_dir):
                os.rmdir(session.integration_test_temp_dir)
        except Exception as e:
            logging.warning(f"Error cleaning up integration test resources: {str(e)}")


@pytest.fixture(scope="session")
def setup_integration_test_environment(request):
    """
    Provide the integration test environment configuration.
    
    Returns:
        Dict: Integration test configuration
    """
    return request.config.integration_test_config


@pytest.fixture(scope="session")
def gcp_test_context(request):
    """
    Provide a GCPTestContext for GCP service testing.
    
    Returns:
        GCPTestContext: Context manager for GCP testing
    """
    if hasattr(request.session, 'integration_test_context'):
        return request.session.integration_test_context
    
    # Create a new context if one doesn't exist
    context = GCPTestContext(
        project_id=INTEGRATION_TEST_CONFIG['test_project_id'],
        location=INTEGRATION_TEST_CONFIG['test_location']
    )
    return context


@pytest.fixture(scope="session")
def gcs_emulator(gcp_test_context):
    """
    Provide a GCS emulator for testing.
    
    Args:
        gcp_test_context: GCP test context
        
    Returns:
        GCSEmulator: GCS emulator instance
    """
    return gcp_test_context.use_gcs_emulator()


@pytest.fixture(scope="session")
def bigquery_emulator(gcp_test_context):
    """
    Provide a BigQuery emulator for testing.
    
    Args:
        gcp_test_context: GCP test context
        
    Returns:
        BigQueryEmulator: BigQuery emulator instance
    """
    return gcp_test_context.use_bigquery_emulator()


@pytest.fixture(scope="session")
def vertex_ai_emulator(gcp_test_context):
    """
    Provide a Vertex AI emulator for testing.
    
    Args:
        gcp_test_context: GCP test context
        
    Returns:
        VertexAIEmulator: Vertex AI emulator instance
    """
    return gcp_test_context.use_vertex_ai_emulator()


@pytest.fixture(scope="session")
def bigquery_test_environment(bigquery_emulator):
    """
    Provide a configured BigQuery test environment.
    
    Args:
        bigquery_emulator: BigQuery emulator fixture
        
    Returns:
        Dict: BigQuery test environment configuration
    """
    # Create a test dataset
    test_dataset_id = f"integration_test_dataset"
    
    try:
        dataset = bigquery_emulator.get_dataset(test_dataset_id)
    except Exception:
        dataset = bigquery_emulator.create_dataset(test_dataset_id)
    
    return {
        'client': bigquery_emulator.get_client(),
        'dataset_id': test_dataset_id,
        'dataset': dataset
    }


@pytest.fixture(scope="session")
def gcs_test_environment(gcs_emulator):
    """
    Provide a configured GCS test environment.
    
    Args:
        gcs_emulator: GCS emulator fixture
        
    Returns:
        Dict: GCS test environment configuration
    """
    # Create a test bucket
    test_bucket_name = f"integration-test-bucket"
    
    try:
        bucket = gcs_emulator.get_bucket(test_bucket_name)
    except Exception:
        bucket = gcs_emulator.create_bucket(test_bucket_name)
    
    return {
        'client': gcs_emulator.get_client(),
        'bucket_name': test_bucket_name,
        'bucket': bucket
    }


@pytest.fixture
def test_dataframe_generator():
    """
    Provide a function to generate test DataFrames.
    
    Returns:
        Function: Function that generates pandas DataFrames for testing
    """
    def generate_dataframe(columns_spec=None, num_rows=100):
        """
        Generate a test DataFrame with specified columns and number of rows.
        
        Args:
            columns_spec: Dictionary specifying column names and types
            num_rows: Number of rows to generate
            
        Returns:
            pandas.DataFrame: Generated DataFrame
        """
        if columns_spec is None:
            columns_spec = {
                'id': {'type': 'int', 'min': 1, 'max': 1000},
                'name': {'type': 'str', 'length': 10},
                'value': {'type': 'float', 'min': 0.0, 'max': 100.0},
                'is_valid': {'type': 'bool'},
                'timestamp': {'type': 'datetime'}
            }
        
        return create_test_dataframe(columns_spec, num_rows)
    
    return generate_dataframe


@pytest.fixture
def test_data_generator():
    """
    Provide a TestDataGenerator instance.
    
    Returns:
        TestDataGenerator: Utility for generating test data
    """
    from src.test.utils.test_helpers import TestDataGenerator
    return TestDataGenerator()


@pytest.fixture
def mock_response_builder():
    """
    Provide a MockResponseBuilder for HTTP response mocking.
    
    Returns:
        MockResponseBuilder: Builder for mock HTTP responses
    """
    from src.test.utils.test_helpers import MockResponseBuilder
    return MockResponseBuilder()


@pytest.fixture
def integration_temp_file(request):
    """
    Provide a temporary file for integration tests.
    
    Args:
        request: Pytest request object
        
    Returns:
        str: Path to temporary file that will be cleaned up
    """
    if hasattr(request.session, 'integration_test_resource_manager'):
        resource_manager = request.session.integration_test_resource_manager
    else:
        resource_manager = TestResourceManager()
    
    temp_file = resource_manager.add_temp_file(
        content="",
        prefix="integration_test_",
        suffix=".tmp"
    )
    
    return temp_file


@pytest.fixture
def integration_temp_dir(request):
    """
    Provide a temporary directory for integration tests.
    
    Args:
        request: Pytest request object
        
    Returns:
        str: Path to temporary directory that will be cleaned up
    """
    if hasattr(request.session, 'integration_test_resource_manager'):
        resource_manager = request.session.integration_test_resource_manager
    else:
        resource_manager = TestResourceManager()
    
    temp_dir = resource_manager.add_temp_directory(
        prefix="integration_test_"
    )
    
    return temp_dir


@pytest.fixture
def test_gcs_bucket(gcs_test_environment, gcp_test_context):
    """
    Provide a test GCS bucket.
    
    Args:
        gcs_test_environment: GCS test environment
        gcp_test_context: GCP test context
        
    Returns:
        MagicMock: Mock bucket object
    """
    # Create a test-specific bucket
    bucket_name = f"test-bucket-{os.urandom(4).hex()}"
    return gcp_test_context.create_temp_gcs_bucket(bucket_name)


@pytest.fixture
def test_bigquery_dataset(bigquery_test_environment, gcp_test_context):
    """
    Provide a test BigQuery dataset.
    
    Args:
        bigquery_test_environment: BigQuery test environment
        gcp_test_context: GCP test context
        
    Returns:
        MagicMock: Mock dataset object
    """
    # Create a test-specific dataset
    dataset_id = f"test_dataset_{os.urandom(4).hex()}"
    return gcp_test_context.create_temp_bigquery_dataset(dataset_id)


@pytest.fixture
def test_bigquery_table(test_bigquery_dataset, test_dataframe_generator, gcp_test_context):
    """
    Provide a test BigQuery table.
    
    Args:
        test_bigquery_dataset: Test BigQuery dataset
        test_dataframe_generator: Function to generate test data
        gcp_test_context: GCP test context
        
    Returns:
        MagicMock: Mock table object
    """
    # Create a test table with sample data
    table_id = f"test_table_{os.urandom(4).hex()}"
    test_data = test_dataframe_generator(num_rows=10)
    
    return gcp_test_context.create_temp_bigquery_table(
        test_bigquery_dataset.dataset_id,
        table_id,
        data=test_data
    )


@pytest.fixture
def test_vertex_ai_model(gcp_test_context, vertex_ai_emulator):
    """
    Provide a test Vertex AI model.
    
    Args:
        gcp_test_context: GCP test context
        vertex_ai_emulator: Vertex AI emulator
        
    Returns:
        MagicMock: Mock Vertex AI model
    """
    model_id = f"test-model-{os.urandom(4).hex()}"
    model = vertex_ai_emulator.create_model(
        model_id=model_id,
        display_name=f"Test Model {model_id}",
        model_type="classification"
    )
    return model


@pytest.fixture
def test_vertex_ai_endpoint(gcp_test_context, vertex_ai_emulator, test_vertex_ai_model):
    """
    Provide a test Vertex AI endpoint.
    
    Args:
        gcp_test_context: GCP test context
        vertex_ai_emulator: Vertex AI emulator
        test_vertex_ai_model: Test Vertex AI model
        
    Returns:
        MagicMock: Mock Vertex AI endpoint
    """
    endpoint_id = f"test-endpoint-{os.urandom(4).hex()}"
    endpoint = vertex_ai_emulator.create_endpoint(
        endpoint_id=endpoint_id,
        display_name=f"Test Endpoint {endpoint_id}",
        deployed_models=[test_vertex_ai_model.name]
    )
    return endpoint


@pytest.fixture
def test_ingestion_pipeline(gcs_test_environment, bigquery_test_environment):
    """
    Provide a configured ingestion pipeline for testing.
    
    Args:
        gcs_test_environment: GCS test environment
        bigquery_test_environment: BigQuery test environment
        
    Returns:
        MagicMock: Mock ingestion pipeline object
    """
    # Mock ingestion pipeline configuration
    from unittest.mock import MagicMock
    
    pipeline = MagicMock()
    pipeline.id = f"test-ingestion-pipeline-{os.urandom(4).hex()}"
    pipeline.name = "Test Ingestion Pipeline"
    pipeline.source_config = {
        "type": DataSourceType.GCS.value,
        "bucket": gcs_test_environment['bucket_name'],
        "prefix": "test-data/",
        "file_format": FileFormat.CSV.value
    }
    pipeline.target_config = {
        "type": "BIGQUERY",
        "dataset": bigquery_test_environment['dataset_id'],
        "table": f"test_table_{os.urandom(4).hex()}"
    }
    
    return pipeline


@pytest.fixture
def test_quality_validation_engine():
    """
    Provide a configured quality validation engine for testing.
    
    Returns:
        MagicMock: Mock quality validation engine
    """
    # Mock quality validation engine
    from unittest.mock import MagicMock
    
    validation_engine = MagicMock()
    validation_engine.id = f"validation-engine-{os.urandom(4).hex()}"
    
    # Configure mock validation methods
    def validate_data(data, rules=None):
        # Create a sample validation result
        from src.test.utils.test_helpers import create_test_validation_summary
        return create_test_validation_summary(
            total_validations=5,
            passed_validations=4
        )
    
    validation_engine.validate_data.side_effect = validate_data
    
    return validation_engine


@pytest.fixture
def test_self_healing_engine(test_vertex_ai_endpoint):
    """
    Provide a configured self-healing engine for testing.
    
    Args:
        test_vertex_ai_endpoint: Test Vertex AI endpoint
        
    Returns:
        MagicMock: Mock self-healing engine
    """
    # Mock self-healing engine
    from unittest.mock import MagicMock
    from src.backend.constants import SelfHealingMode
    
    healing_engine = MagicMock()
    healing_engine.id = f"healing-engine-{os.urandom(4).hex()}"
    healing_engine.mode = SelfHealingMode.SEMI_AUTOMATIC
    healing_engine.confidence_threshold = 0.85
    healing_engine.prediction_endpoint = test_vertex_ai_endpoint.name
    
    # Configure mock healing methods
    def analyze_issue(issue):
        # Create a sample healing action
        from src.test.utils.test_helpers import create_test_healing_action
        return create_test_healing_action(
            successful=True,
            action_type="data_correction"
        )
    
    healing_engine.analyze_issue.side_effect = analyze_issue
    
    return healing_engine


@pytest.fixture
def test_monitoring_system():
    """
    Provide a configured monitoring system for testing.
    
    Returns:
        MagicMock: Mock monitoring system
    """
    # Mock monitoring system
    from unittest.mock import MagicMock
    
    monitoring_system = MagicMock()
    monitoring_system.id = f"monitoring-system-{os.urandom(4).hex()}"
    
    # Configure mock alert methods
    def create_alert(severity, message, context=None):
        alert_id = f"alert-{os.urandom(4).hex()}"
        return {
            "alert_id": alert_id,
            "severity": severity,
            "message": message,
            "context": context or {},
            "timestamp": pd.Timestamp.now().isoformat(),
            "status": "ACTIVE"
        }
    
    monitoring_system.create_alert.side_effect = create_alert
    
    return monitoring_system