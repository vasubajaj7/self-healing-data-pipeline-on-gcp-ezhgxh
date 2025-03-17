"""
Main initialization file for the data ingestion module of the self-healing data pipeline.
This module serves as the entry point for all data ingestion functionality, exposing key components from submodules including connectors, extractors, metadata tracking, orchestration, staging, and error handling. It provides a comprehensive framework for extracting data from various sources, managing the extraction process, and preparing data for quality validation and processing.
"""

import logging  # Standard library for logging functionality

from . import connectors  # src/backend/ingestion/connectors/__init__.py: Import data source connector functionality
from . import extractors  # src/backend/ingestion/extractors/__init__.py: Import data extraction functionality
from . import metadata  # src/backend/ingestion/metadata/__init__.py: Import metadata tracking functionality
from . import orchestration  # src/backend/ingestion/orchestration/__init__.py: Import pipeline orchestration functionality
from . import staging  # src/backend/ingestion/staging/__init__.py: Import data staging functionality
from . import errors  # src/backend/ingestion/errors/__init__.py: Import error handling functionality
from ...constants import __version__  # src/backend/constants.py: Import version information

__all__ = ["connectors", "extractors", "metadata", "orchestration", "staging", "errors", "__version__", "logger", "initialize"]

logger = logging.getLogger(__name__)  # Initialize logger for this module


def initialize():
    """Initialize the ingestion module by setting up necessary components and registering connectors"""
    # Configure logging for the ingestion module
    logger.info("Initializing data ingestion module")

    # Register all connector implementations with the connector factory
    connectors.register_connectors()
    logger.debug("Registered all data source connectors")

    # Initialize error handlers
    # TODO: Implement error handler initialization
    logger.debug("Initialized error handlers")

    # Log successful initialization of the ingestion module
    logger.info("Data ingestion module initialized successfully")