# src/backend/ingestion/extractors/api_extractor.py
"""
Implementation of the API extractor component for the self-healing data pipeline.
This extractor is responsible for extracting data from external REST APIs,
handling authentication, pagination, rate limiting, and providing comprehensive
error handling with retry mechanisms and circuit breaker patterns.
"""

import typing
import json
import enum
import urllib.parse
import pandas  # version 2.0.x
from datetime import datetime

from .base_connector import BaseConnector, ConnectorFactory  # Ensure correct usage of BaseConnector methods
from ...constants import DataSourceType, DEFAULT_MAX_RETRY_ATTEMPTS  # Ensure constants are used correctly
from ...config import get_config  # Access application configuration settings
from ...utils.logging.logger import get_logger  # Configure logging for API extractor operations
from ..errors.error_handler import with_error_handling  # Apply error handling decorator to extraction methods
from ..errors.error_handler import retry_with_backoff  # Apply retry logic with backoff to API operations
from ..connectors.api_connector import ApiConnector, ApiAuthType, ApiPaginationType  # Use API connector for establishing connections to external APIs
from .batch_extractor import BatchExtractor  # Use batch extractor for processing large API datasets in batches

# Initialize logger
logger = get_logger(__name__)


class ApiExtractor(BaseConnector):
    """
    Extractor for retrieving data from external REST APIs with support for
    authentication, pagination, and error recovery
    """

    def __init__(
        self,
        source_id: str,
        source_name: str,
        extraction_config: dict
    ):
        """
        Initialize the API extractor with source information and extraction configuration

        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            extraction_config: Configuration parameters for connecting to the source
        """
        # Store source identification information
        self.source_id = source_id
        self.source_name = source_name
        self.extraction_config = extraction_config

        # Initialize extraction configuration with defaults
        self.base_url = extraction_config.get("base_url")
        self.auth_type = ApiAuthType(extraction_config.get("auth_type", ApiAuthType.NONE.value))
        self.auth_config = extraction_config.get("auth_config", {})
        self.timeout = extraction_config.get("timeout")
        self.max_retries = extraction_config.get("max_retries")
        self.verify_ssl = extraction_config.get("verify_ssl", True)
        self.default_headers = extraction_config.get("default_headers", {})
        self.pagination_type = ApiPaginationType(extraction_config.get("pagination_type", ApiPaginationType.NONE.value))
        self.pagination_config = extraction_config.get("pagination_config", {})

        # Set up connector configuration from extraction_config
        connector_config = {
            "base_url": self.base_url,
            "auth_type": self.auth_type.value,
            "auth_config": self.auth_config,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "verify_ssl": self.verify_ssl,
            "default_headers": self.default_headers,
            "pagination_type": self.pagination_type.value,
            "pagination_config": self.pagination_config
        }

        # Initialize connector to None (created during extract)
        self.connector = None

        # Initialize extraction statistics dictionary
        self.extraction_stats = {
            "total_records": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "last_extraction_time": None
        }

        # Determine if batching should be used based on config
        self.use_batching = extraction_config.get("use_batching", False)

        # Initialize batch_extractor if use_batching is True
        if self.use_batching:
            self.batch_extractor = BatchExtractor(source_id, source_name, extraction_config)
        else:
            self.batch_extractor = None

        # Set up logging for this extractor instance
        logger.info(f"Initialized API extractor for {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'ApiExtractor', 'operation': 'extract'}, raise_exception=True)
    def extract(self, extraction_params: dict) -> typing.Tuple[typing.Optional[pandas.DataFrame], dict]:
        """
        Extract data from the API based on extraction parameters

        Args:
            extraction_params: Parameters controlling the extraction process,
                including what data to extract and how to extract it

        Returns:
            tuple: (data, metadata) - Extracted data and associated metadata
        """
        # Validate extraction parameters
        if not self._validate_extraction_params(extraction_params):
            raise ValueError("Invalid extraction parameters")

        # Initialize or get existing API connector
        self.connector = self.initialize_connector()

        # Ensure connector is connected
        self.ensure_connection()

        # Determine extraction method (batch or direct)
        if self.use_batching and self.batch_extractor:
            # If using batching, use batch_extractor.extract_in_batches
            data, metadata = self.batch_extractor.extract_in_batches(extraction_params, self.connector)
        else:
            # Otherwise, use connector.extract_data directly
            data, metadata = self.connector.extract_data(extraction_params)

        # Process and transform the extracted data if needed
        if data is not None and 'transformation' in extraction_params:
            data = self.transform_data(data, extraction_params)

        # Update extraction statistics
        self._update_extraction_stats(success=data is not None, metadata=metadata)

        # Return extracted data and metadata
        return data, metadata

    @with_error_handling(context={'component': 'ApiExtractor', 'operation': 'get_schema'}, raise_exception=True)
    def get_schema(self, endpoint_path: str) -> dict:
        """
        Retrieve the schema information for an API endpoint

        Args:
            endpoint_path: Path to the API endpoint

        Returns:
            dict: Schema definition for the specified endpoint
        """
        # Initialize or get existing API connector
        self.connector = self.initialize_connector()

        # Ensure connector is connected
        self.ensure_connection()

        # Use connector.get_source_schema to retrieve schema
        schema = self.connector.get_source_schema(endpoint_path)

        # Format and return schema definition
        return schema

    def initialize_connector(self) -> ApiConnector:
        """
        Initialize the API connector if not already initialized

        Returns:
            ApiConnector: Initialized API connector
        """
        # Check if connector is already initialized
        if self.connector is None:
            # If not, create new ApiConnector instance
            self.connector = ApiConnector(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=DataSourceType.API,
                connection_config=self.extraction_config
            )
        # Configure connector with source information and connection details
        return self.connector

    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def ensure_connection(self) -> bool:
        """
        Ensure the API connector is connected

        Returns:
            bool: True if connection successful, False otherwise
        """
        # Check if connector is initialized, initialize if needed
        if self.connector is None:
            self.connector = self.initialize_connector()

        # Check if connector is already connected
        if not self.connector.is_connected:
            # If not connected, call connector.connect()
            return self.connector.connect()

        # Return connection status
        return self.connector.is_connected

    def transform_data(self, data: object, extraction_params: dict) -> object:
        """
        Transform extracted data based on transformation configuration

        Args:
            data: Extracted data
            extraction_params: Extraction parameters

        Returns:
            object: Transformed data
        """
        # Check if transformation is required
        if 'transformation' not in extraction_params:
            return data

        # Apply field mappings if specified
        # Apply filters if specified
        # Apply format conversions if specified

        # Return transformed data
        return data

    def get_extraction_stats(self) -> dict:
        """
        Get statistics about API extraction operations

        Returns:
            dict: Extraction statistics
        """
        # Collect current extraction statistics
        stats = self.extraction_stats.copy()

        # Include API-specific statistics from connector if available
        if self.connector:
            api_stats = self.connector.get_api_stats()
            stats.update(api_stats)

        # Include batch statistics if batching is used
        if self.use_batching and self.batch_extractor:
            batch_stats = self.batch_extractor.get_batch_stats()
            stats.update(batch_stats)

        # Calculate derived metrics (success rate, average time, etc.)
        # Format statistics dictionary
        return stats

    def reset_extraction_stats(self) -> None:
        """
        Reset the extraction statistics
        """
        # Reset all extraction statistics to initial values
        self.extraction_stats = {
            "total_records": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "last_extraction_time": None
        }

        # Reset API connector statistics if available
        if self.connector:
            self.connector.reset_api_stats()

        # Reset batch extractor statistics if using batching
        if self.use_batching and self.batch_extractor:
            self.batch_extractor.reset_batch_stats()

    def _update_extraction_stats(self, success: bool, metadata: dict) -> None:
        """
        Update extraction statistics after an operation

        Args:
            success: Whether the extraction was successful
            metadata: Metadata about the extraction
        """
        # Update extraction attempt counters
        self.extraction_stats["total_records"] += metadata.get("record_count", 0)

        # Update success/failure counters
        if success:
            self.extraction_stats["successful_extractions"] += 1
        else:
            self.extraction_stats["failed_extractions"] += 1

        # Update data volume metrics
        # Update timing information
        self.extraction_stats["last_extraction_time"] = datetime.now().isoformat()

        # Log statistics update
        logger.debug(f"Updated extraction statistics: {self.extraction_stats}")

    def close(self) -> bool:
        """
        Close the API connector and clean up resources

        Returns:
            bool: True if close successful, False otherwise
        """
        # Check if connector is initialized
        if self.connector:
            # If initialized, call connector.disconnect()
            status = self.connector.disconnect()
            # Reset connector to None
            self.connector = None
            return status
        return True


# Register the API connector with the connector factory
ConnectorFactory().register_connector(DataSourceType.API, ApiExtractor)