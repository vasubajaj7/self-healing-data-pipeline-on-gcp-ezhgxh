"""
Provides centralized cleanup functionality for test resources created during testing of the self-healing data pipeline.
This module ensures proper cleanup of temporary files, directories, GCP resources, and other test artifacts
to prevent resource leakage and maintain a clean test environment.
"""

import pytest
import logging
import os
import shutil
from typing import List

# Internal imports
from src.test.utils.test_helpers import cleanup_temp_resources, TestResourceManager
from src.test.utils.gcp_test_utils import GCPTestContext
from src.test.utils.bigquery_test_utils import BigQueryTestHelper

# Set up logging
logger = logging.getLogger(__name__)

# Global registries for resources that need cleaning up
RESOURCE_MANAGERS: List[TestResourceManager] = []
GCP_TEST_CONTEXTS: List[GCPTestContext] = []
BQ_TEST_HELPERS: List[BigQueryTestHelper] = []


def register_resource_manager(resource_manager: TestResourceManager) -> None:
    """
    Registers a TestResourceManager instance for cleanup during test teardown.
    
    Args:
        resource_manager: The TestResourceManager instance to register
    """
    RESOURCE_MANAGERS.append(resource_manager)
    logger.debug(f"Registered TestResourceManager for cleanup")


def register_gcp_test_context(gcp_context: GCPTestContext) -> None:
    """
    Registers a GCPTestContext instance for cleanup during test teardown.
    
    Args:
        gcp_context: The GCPTestContext instance to register
    """
    GCP_TEST_CONTEXTS.append(gcp_context)
    logger.debug(f"Registered GCPTestContext for cleanup")


def register_bigquery_test_helper(bq_helper: BigQueryTestHelper) -> None:
    """
    Registers a BigQueryTestHelper instance for cleanup during test teardown.
    
    Args:
        bq_helper: The BigQueryTestHelper instance to register
    """
    BQ_TEST_HELPERS.append(bq_helper)
    logger.debug(f"Registered BigQueryTestHelper for cleanup")


def cleanup_all_test_resources() -> None:
    """
    Cleans up all registered test resources.
    """
    logger.info("Starting cleanup of all test resources")
    
    # Clean up temporary files and directories
    cleanup_temp_resources()
    
    # Clean up TestResourceManager instances
    for resource_manager in RESOURCE_MANAGERS:
        try:
            resource_manager.cleanup()
        except Exception as e:
            logger.error(f"Error cleaning up TestResourceManager: {str(e)}")
    
    # Clean up GCPTestContext instances
    for gcp_context in GCP_TEST_CONTEXTS:
        try:
            # We'll exit the context manager which should handle cleanup
            gcp_context.__exit__(None, None, None)
        except Exception as e:
            logger.error(f"Error cleaning up GCPTestContext: {str(e)}")
    
    # Clean up BigQueryTestHelper instances
    for bq_helper in BQ_TEST_HELPERS:
        try:
            bq_helper.cleanup()
        except Exception as e:
            logger.error(f"Error cleaning up BigQueryTestHelper: {str(e)}")
    
    # Clear all registries
    RESOURCE_MANAGERS.clear()
    GCP_TEST_CONTEXTS.clear()
    BQ_TEST_HELPERS.clear()
    
    logger.info("Completed cleanup of all test resources")


def cleanup_test_directory(directory_path: str, ignore_errors: bool = False) -> bool:
    """
    Cleans up a test directory and all its contents.
    
    Args:
        directory_path: Path to the directory to clean up
        ignore_errors: Whether to ignore errors during cleanup
        
    Returns:
        True if cleanup was successful, False otherwise
    """
    if not os.path.exists(directory_path):
        logger.debug(f"Directory does not exist: {directory_path}")
        return True
    
    try:
        shutil.rmtree(directory_path, ignore_errors=ignore_errors)
        logger.info(f"Successfully cleaned up directory: {directory_path}")
        return True
    except Exception as e:
        error_msg = f"Error cleaning up directory {directory_path}: {str(e)}"
        if ignore_errors:
            logger.warning(error_msg)
            return False
        else:
            logger.error(error_msg)
            raise


def cleanup_test_file(file_path: str, ignore_errors: bool = False) -> bool:
    """
    Cleans up a test file.
    
    Args:
        file_path: Path to the file to clean up
        ignore_errors: Whether to ignore errors during cleanup
        
    Returns:
        True if cleanup was successful, False otherwise
    """
    if not os.path.exists(file_path):
        logger.debug(f"File does not exist: {file_path}")
        return True
    
    try:
        os.remove(file_path)
        logger.info(f"Successfully cleaned up file: {file_path}")
        return True
    except Exception as e:
        error_msg = f"Error cleaning up file {file_path}: {str(e)}"
        if ignore_errors:
            logger.warning(error_msg)
            return False
        else:
            logger.error(error_msg)
            raise


class TestCleanupFixture:
    """
    Pytest fixture class for automatic test resource cleanup.
    """
    
    def setup(self):
        """
        Setup method called before each test.
        """
        # Reset the registration lists to ensure clean state
        RESOURCE_MANAGERS.clear()
        GCP_TEST_CONTEXTS.clear()
        BQ_TEST_HELPERS.clear()
        
        logger.info("TestCleanupFixture: Setup completed")
    
    def teardown(self):
        """
        Teardown method called after each test.
        """
        logger.info("TestCleanupFixture: Starting teardown")
        cleanup_all_test_resources()
        logger.info("TestCleanupFixture: Teardown completed")


class GlobalTestCleanup:
    """
    Class for global test cleanup operations.
    """
    
    @classmethod
    def cleanup_before_session(cls):
        """
        Cleanup method called before test session starts.
        """
        logger.info("GlobalTestCleanup: Starting pre-session cleanup")
        cleanup_all_test_resources()
        logger.info("GlobalTestCleanup: Pre-session cleanup completed")
    
    @classmethod
    def cleanup_after_session(cls):
        """
        Cleanup method called after test session ends.
        """
        logger.info("GlobalTestCleanup: Starting post-session cleanup")
        cleanup_all_test_resources()
        logger.info("GlobalTestCleanup: Post-session cleanup completed")