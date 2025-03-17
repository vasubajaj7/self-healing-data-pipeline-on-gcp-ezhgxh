"""
Pytest configuration file for the self-healing data pipeline project.

This file sets up the testing environment, registers fixtures, and provides hooks for test session
management. It serves as the central configuration point for all test types (unit, integration,
performance, e2e) and provides common utilities and fixtures that can be used across all test modules.
"""

import pytest
import os
import tempfile
import logging
import pathlib
import shutil
from typing import Callable, Dict, Any, Optional, List, Union

# Internal imports
from src.test.utils.test_helpers import (
    TestResourceManager,
    cleanup_temp_resources,
    create_temp_file,
    create_temp_directory,
    create_test_dataframe
)
from src.test.utils.gcp_test_utils import (
    GCPTestContext,
    TEST_PROJECT_ID,
    TEST_LOCATION
)
from src.backend.constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING,
    ENV_PRODUCTION
)

# Define global test paths
ROOT_DIR = pathlib.Path(__file__).parent.parent
TEST_DIR = pathlib.Path(__file__).parent
TEST_DATA_DIR = TEST_DIR / 'mock_data'

# Set test environment from environment variable, default to development
TEST_ENV = os.environ.get('TEST_ENV', ENV_DEVELOPMENT)


def pytest_configure(config):
    """Configure pytest environment before test collection."""
    
    # Register custom markers
    config.addinivalue_line("markers", "unit: mark a test as a unit test")
    config.addinivalue_line("markers", "integration: mark a test as an integration test")
    config.addinivalue_line("markers", "performance: mark a test as a performance test")
    config.addinivalue_line("markers", "e2e: mark a test as an end-to-end test")
    config.addinivalue_line("markers", "slow: mark a test as slow-running")
    config.addinivalue_line("markers", "gcp: mark a test as requiring GCP services")
    config.addinivalue_line("markers", "bigquery: mark a test as requiring BigQuery")
    config.addinivalue_line("markers", "gcs: mark a test as requiring Google Cloud Storage")
    config.addinivalue_line("markers", "vertex_ai: mark a test as requiring Vertex AI")
    config.addinivalue_line("markers", "data_quality: mark a test related to data quality validation")
    config.addinivalue_line("markers", "self_healing: mark a test related to self-healing functionality")
    
    # Configure logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger('pytest')
    logger.info(f"Configuring test environment: {TEST_ENV}")
    
    # Environment-specific configuration
    if TEST_ENV == ENV_DEVELOPMENT:
        # Development-specific settings
        os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', str(TEST_DIR / 'credentials' / 'dev-service-account.json'))
        logger.info("Using development configuration")
    elif TEST_ENV == ENV_STAGING:
        # Staging-specific settings
        os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', str(TEST_DIR / 'credentials' / 'staging-service-account.json'))
        logger.info("Using staging configuration")
    elif TEST_ENV == ENV_PRODUCTION:
        # Production-specific settings - typically we avoid running tests against prod
        logger.warning("Using PRODUCTION environment for testing - this is generally not recommended")
        os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', str(TEST_DIR / 'credentials' / 'prod-service-account.json'))


def pytest_sessionstart(session):
    """Set up resources needed for the entire test session."""
    # Create global test directories
    os.makedirs(TEST_DATA_DIR, exist_ok=True)
    
    # Create credential directory if it doesn't exist
    credentials_dir = TEST_DIR / 'credentials'
    os.makedirs(credentials_dir, exist_ok=True)
    
    # Set up shared test resources based on environment
    if TEST_ENV == ENV_DEVELOPMENT:
        # Create default test credential file if needed for development
        cred_file = credentials_dir / 'dev-service-account.json'
        if not cred_file.exists():
            with open(cred_file, 'w') as f:
                f.write('{"type": "service_account", "project_id": "' + TEST_PROJECT_ID + '"}')


def pytest_sessionfinish(session, exitstatus):
    """Clean up resources after the entire test session."""
    # Clean up temporary resources using cleanup helper
    cleanup_temp_resources()
    
    # Log test session summary
    logger = logging.getLogger('pytest')
    logger.info(f"Test session completed with exit status: {exitstatus}")
    
    # If we're generating HTML reports, create directories for them
    if session.config.getoption("--generate-reports", False):
        reports_dir = TEST_DIR / 'reports'
        os.makedirs(reports_dir, exist_ok=True)
        logger.info(f"Test reports saved to {reports_dir}")


def pytest_addoption(parser):
    """Add custom command line options for pytest."""
    parser.addoption(
        "--env", action="store", default=ENV_DEVELOPMENT,
        help="Test environment: development, staging, or production"
    )
    parser.addoption(
        "--use-emulators", action="store_true", default=False,
        help="Use GCP emulators instead of mock objects for testing"
    )
    parser.addoption(
        "--generate-reports", action="store_true", default=False,
        help="Generate HTML reports for test results"
    )
    parser.addoption(
        "--data-size", action="store", default="small",
        choices=["small", "medium", "large"], 
        help="Size of test datasets to generate"
    )
    parser.addoption(
        "--skip-slow", action="store_true", default=False,
        help="Skip tests marked as slow"
    )
    parser.addoption(
        "--test-gcp-project", action="store", default=TEST_PROJECT_ID,
        help="GCP project ID to use for integration tests"
    )
    parser.addoption(
        "--test-gcp-location", action="store", default=TEST_LOCATION,
        help="GCP location/region to use for integration tests"
    )


# Basic project fixtures
@pytest.fixture(scope="session")
def root_dir():
    """Return the root directory of the project."""
    return ROOT_DIR


@pytest.fixture(scope="session")
def test_dir():
    """Return the test directory of the project."""
    return TEST_DIR


@pytest.fixture(scope="session")
def test_data_dir():
    """Return the test data directory of the project."""
    return TEST_DATA_DIR


# Resource management fixtures
@pytest.fixture(scope="function")
def test_resource_manager():
    """Provide a TestResourceManager for managing test resources."""
    with TestResourceManager() as manager:
        yield manager


@pytest.fixture(scope="function")
def temp_dir():
    """Provide a temporary directory for test file storage."""
    temp_dir = create_temp_directory(prefix="pytest_")
    yield temp_dir
    # Cleanup is handled by cleanup_temp_resources() in pytest_sessionfinish


@pytest.fixture(scope="function")
def temp_file():
    """Provide a function to create temporary files."""
    def _create_temp_file(content="", suffix=None, prefix=None, dir_path=None):
        return create_temp_file(content, suffix, prefix, dir_path)
    
    return _create_temp_file


# Environment configuration
@pytest.fixture(scope="session")
def setup_test_environment(request):
    """Set up the test environment based on TEST_ENV."""
    env = request.config.getoption("--env")
    
    # Override global TEST_ENV with command line option
    global TEST_ENV
    TEST_ENV = env
    
    # Environment-specific setup
    if env == ENV_DEVELOPMENT:
        # Development environment setup
        pass
    elif env == ENV_STAGING:
        # Staging environment setup
        pass
    elif env == ENV_PRODUCTION:
        # Production environment setup
        pass
    
    return env


# GCP Testing fixtures
@pytest.fixture(scope="function")
def gcp_test_context(request):
    """Provide a GCPTestContext for GCP service testing."""
    use_emulators = request.config.getoption("--use-emulators")
    project_id = request.config.getoption("--test-gcp-project")
    location = request.config.getoption("--test-gcp-location")
    
    with GCPTestContext(project_id, location) as context:
        # Configure based on user options
        if use_emulators:
            # Set up emulators instead of mocks
            context.use_gcs_emulator()
            context.use_bigquery_emulator()
            context.use_vertex_ai_emulator()
        else:
            # Set up mocks (default behavior)
            context.mock_gcs_client()
            context.mock_bigquery_client()
            context.mock_vertex_ai_client()
        
        yield context


# Data generation fixtures
@pytest.fixture(scope="function")
def test_dataframe_generator(request):
    """Provide a function to generate test DataFrames."""
    data_size = request.config.getoption("--data-size")
    
    def _create_test_dataframe(columns_spec, num_rows=None):
        # Determine size based on option
        if num_rows is None:
            if data_size == "small":
                num_rows = 100
            elif data_size == "medium":
                num_rows = 10000
            elif data_size == "large":
                num_rows = 100000
        
        return create_test_dataframe(columns_spec, num_rows)
    
    return _create_test_dataframe


# HTTP and API testing fixtures
@pytest.fixture(scope="function")
def mock_response_builder():
    """Provide a builder for mock HTTP responses."""
    from src.test.utils.test_helpers import MockResponseBuilder
    return MockResponseBuilder()