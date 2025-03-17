"""
Pytest configuration file for unit tests in the self-healing data pipeline project.

This file defines fixtures specific to unit testing, imports shared fixtures from the main conftest.py,
and configures the unit test environment. It provides mock objects and test data for isolated
testing of individual components.
"""

import pytest  # package_name: pytest, package_version: 7.3.1
import os  # package_name: os, package_version: standard library
import tempfile  # package_name: tempfile, package_version: standard library
import logging  # package_name: logging, package_version: standard library
from unittest import mock  # package_name: unittest.mock, package_version: standard library

# Internal imports
from src.test.conftest import setup_test_environment, root_dir, test_data_dir  # Module: src.test.conftest
from src.test.fixtures.conftest import test_resource_manager, temp_dir  # Module: src.test.fixtures.conftest
from src.test.fixtures.backend.ingestion_fixtures import mock_gcs_connector, mock_cloudsql_connector, mock_api_connector, mock_file_extractor, mock_connector_factory, mock_metadata_tracker, mock_extraction_orchestrator, test_ingestion_data  # Module: src.test.fixtures.backend.ingestion_fixtures
from src.test.fixtures.backend.quality_fixtures import mock_validation_engine, mock_quality_scorer, mock_schema_validator, test_validation_data, sample_validation_rules, sample_validation_results  # Module: src.test.fixtures.backend.quality_fixtures
from src.test.fixtures.backend.healing_fixtures import mock_issue_classifier, mock_pattern_recognizer, mock_data_corrector, test_healing_data, sample_issues, sample_corrections  # Module: src.test.fixtures.backend.healing_fixtures
from src.test.fixtures.backend.monitoring_fixtures import mock_metric_processor, mock_anomaly_detector, mock_alert_generator, mock_notification_router, test_metric_data, test_alert_data, sample_metrics, sample_alerts  # Module: src.test.fixtures.backend.monitoring_fixtures
from src.test.fixtures.backend.optimization_fixtures import mock_query_optimizer, mock_schema_optimizer, mock_resource_optimizer, test_optimization_data  # Module: src.test.fixtures.backend.optimization_fixtures
from src.test.fixtures.backend.api_fixtures import mock_api_client, test_api_app, test_client  # Module: src.test.fixtures.backend.api_fixtures
from src.test.utils.test_helpers import create_temp_file, cleanup_temp_resources, MockResponseBuilder  # Module: src.test.utils.test_helpers

# Define global test paths
UNIT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
UNIT_TEST_CONFIG = {
    'use_mocks': True,
    'mock_external_services': True,
    'skip_slow_tests': False
}


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest for unit tests"""
    # Register unit test specific markers
    config.addinivalue_line("markers", "unit: mark a test as a unit test")

    # Configure logging for unit tests
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('pytest')
    logger.info(f"Configuring pytest for unit tests in: {UNIT_TEST_DIR}")

    # Set up unit test specific configuration
    config.option.markexpr = 'unit'


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    """Modify collected test items for unit tests"""
    # Add 'unit' marker to all tests in this directory
    for item in items:
        item.add_marker(pytest.mark.unit)

    # Skip tests marked as 'slow' if skip_slow_tests is enabled
    if config.getoption("--skip-slow"):
        skip_slow = pytest.mark.skip(reason="skipping due to --skip-slow option")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Reorder tests for optimal execution
    # (Implementation depends on test dependencies and execution time)
    pass


@pytest.fixture(scope='session')
def setup_unit_test_environment(request: pytest.FixtureRequest) -> dict:
    """Set up the unit test environment"""
    # Set up environment variables for unit testing
    os.environ['UNIT_TEST_MODE'] = 'True'

    # Configure mock behavior for unit tests
    use_mocks = request.config.getini('use_mocks')
    mock_external_services = request.config.getini('mock_external_services')

    # Register cleanup function to tear down unit test environment
    def fin():
        print("\n[teardown] tearing down unit test environment")
    request.addfinalizer(fin)

    # Return unit test configuration dictionary
    return {
        'use_mocks': use_mocks,
        'mock_external_services': mock_external_services
    }


@pytest.fixture
def mock_response_builder() -> MockResponseBuilder:
    """Create a MockResponseBuilder for HTTP response mocking"""
    # Create and return a MockResponseBuilder instance
    return MockResponseBuilder()


@pytest.fixture
def unit_test_temp_file() -> callable:
    """Create a temporary file for unit tests"""
    def _create_temp_file(content: str = "", extension: str = None) -> str:
        # Create a temporary file with the specified content and extension
        temp_file_path = create_temp_file(content, extension)
        return temp_file_path

    return _create_temp_file