"""
Implementation of a custom connector for the self-healing data pipeline that enables
integration with third-party databases and specialized data sources not covered by
standard connectors. This connector provides a flexible framework for implementing
custom extraction logic while maintaining compatibility with the pipeline's
self-healing capabilities.
"""

import typing
import pandas as pd  # version 2.0.x
import importlib  # standard library
import json  # standard library
import datetime  # standard library

from .base_connector import BaseConnector, ConnectorFactory  # src/backend/ingestion/connectors/base_connector.py
from .base_connector import ConnectorFactory  # src/backend/ingestion/connectors/base_connector.py
from ..errors.error_handler import handle_error, with_error_handling  # src/backend/ingestion/errors/error_handler.py
from ..errors.error_handler import handle_error, with_error_handling  # src/backend/ingestion/errors/error_handler.py
from ...constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS  # src/backend/constants.py
from ...constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS  # src/backend/constants.py
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from ...utils.retry import retry_decorator  # src/backend/utils/retry/retry_decorator.py
from ...utils.retry import retry_decorator  # src/backend/utils/retry/retry_decorator.py

# Initialize module logger
logger = get_logger(__name__)


@ConnectorFactory.register_connector(DataSourceType.CUSTOM, )
class CustomConnector(BaseConnector):
    """
    Flexible connector for integrating with third-party databases and specialized data sources
    through custom implementation logic
    """

    def __init__(
        self,
        source_id: str,
        source_name: str,
        connection_config: dict
    ):
        """
        Initialize the custom connector with source information and connection configuration

        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            connection_config: Configuration parameters for connecting to the source
        """
        # Call parent constructor with source_id, source_name, DataSourceType.CUSTOM, and connection_config
        super().__init__(source_id, source_name, DataSourceType.CUSTOM, connection_config)

        # Extract custom connector configuration parameters
        self.connector_type = connection_config.get('connector_type')
        self.connector_module_path = connection_config.get('connector_module_path')
        self.connector_class_name = connection_config.get('connector_class_name')
        self.custom_config = connection_config.get('custom_config', {})

        # Configure timeout and max_retries from connection_config or defaults
        self.timeout = connection_config.get('timeout', DEFAULT_TIMEOUT_SECONDS)
        self.max_retries = connection_config.get('max_retries', MAX_RETRY_ATTEMPTS)

        # Initialize custom_implementation to None (loaded during connect)
        self.custom_implementation = None

        # Initialize connection_stats dictionary for tracking usage
        self.connection_stats = {
            'custom_stats': {}
        }

        logger.info(f"Initialized custom connector for {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'CustomConnector', 'operation': 'connect'}, raise_exception=False)
    def connect(self) -> bool:
        """
        Establish connection to the custom data source by loading and initializing the custom implementation

        Returns:
            True if connection successful, False otherwise
        """
        # Check if already connected
        if self.is_connected:
            logger.info(f"Already connected to {self.source_name} (ID: {self.source_id})")
            return True

        try:
            # Load custom implementation module using importlib
            self.custom_implementation = self.load_custom_implementation()

            # Call connect method on the custom implementation
            self.custom_implementation.connect()

            # Update connection state and statistics
            self._update_connection_state(connected=True, success=True)
            self.connection_stats['last_connection_time'] = datetime.datetime.now().isoformat()

            # Log successful connection
            logger.info(f"Successfully connected to {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            # Update connection state and statistics
            self._update_connection_state(connected=False, success=False)
            self.connection_stats['last_error'] = str(e)
            logger.error(f"Failed to connect to {self.source_name} (ID: {self.source_id}): {e}")
            return False

    @with_error_handling(context={'component': 'CustomConnector', 'operation': 'disconnect'}, raise_exception=False)
    def disconnect(self) -> bool:
        """
        Close connection to the custom data source

        Returns:
            True if disconnection successful, False otherwise
        """
        # Check if currently connected
        if not self.is_connected:
            logger.info(f"Not connected to {self.source_name} (ID: {self.source_id}), skipping disconnect")
            return True

        try:
            # Call disconnect method on the custom implementation
            self.custom_implementation.disconnect()

            # Set custom_implementation to None
            self.custom_implementation = None

            # Update connection state and statistics
            self._update_connection_state(connected=False, success=True)
            self.connection_stats['last_disconnection_time'] = datetime.datetime.now().isoformat()

            # Log successful disconnection
            logger.info(f"Successfully disconnected from {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            # Update connection state and statistics
            self._update_connection_state(connected=True, success=False)
            self.connection_stats['last_error'] = str(e)
            logger.error(f"Failed to disconnect from {self.source_name} (ID: {self.source_id}): {e}")
            return False

    @with_error_handling(context={'component': 'CustomConnector', 'operation': 'extract_data'}, raise_exception=True)
    def extract_data(self, extraction_params: dict) -> typing.Tuple[typing.Optional[pd.DataFrame], dict]:
        """
        Extract data from the custom data source based on extraction parameters

        Args:
            extraction_params: Parameters controlling the extraction process

        Returns:
            Tuple containing:
                - Extracted data as pandas DataFrame (or None if extraction failed)
                - Metadata dictionary with extraction details
        """
        # Validate extraction parameters
        if not self._validate_extraction_params(extraction_params):
            raise ValueError("Invalid extraction parameters")

        # Ensure connection is established
        if not self.is_connected:
            if not self.connect():
                raise ConnectionError(f"Failed to connect to {self.source_name} (ID: {self.source_id})",
                                      self.source_name, self.connection_config)

        # Delegate extraction to custom implementation
        try:
            # Call extract_data method on custom implementation with extraction_params
            data, custom_metadata = self.custom_implementation.extract_data(extraction_params)

            # Validate and standardize returned data format
            if data is not None and not isinstance(data, pd.DataFrame):
                raise TypeError("Custom implementation must return a pandas DataFrame or None")

            # Enhance metadata with standard connector information
            metadata = self._standardize_metadata(custom_metadata)

            # Update connection statistics
            self.connection_stats['last_extraction_time'] = datetime.datetime.now().isoformat()
            self.connection_stats['rows_extracted'] = len(data) if data is not None else 0

            # Return extracted data and metadata
            return data, metadata

        except Exception as e:
            # Handle extraction errors
            return self._handle_extraction_error(e, extraction_params)

    @with_error_handling(context={'component': 'CustomConnector', 'operation': 'get_source_schema'}, raise_exception=True)
    def get_source_schema(self, object_name: str) -> dict:
        """
        Retrieve the schema information for a custom data source object

        Args:
            object_name: Name of the object (table, file, etc.) to get schema for

        Returns:
            Schema definition for the specified object
        """
        # Ensure connection is established
        if not self.is_connected:
            if not self.connect():
                raise ConnectionError(f"Failed to connect to {self.source_name} (ID: {self.source_id})",
                                      self.source_name, self.connection_config)

        try:
            # Delegate schema retrieval to custom implementation
            custom_schema = self.custom_implementation.get_source_schema(object_name)

            # Validate and standardize returned schema format
            if not isinstance(custom_schema, dict):
                raise TypeError("Custom implementation must return a dictionary for schema")

            # Standardize the schema
            schema = self._standardize_schema(custom_schema)

            # Return standardized schema definition
            return schema

        except Exception as e:
            logger.error(f"Failed to get schema for {object_name} from {self.source_name} (ID: {self.source_id}): {e}")
            raise

    def validate_connection_config(self, config: dict) -> bool:
        """
        Validate the custom connector configuration

        Args:
            config: Connection configuration to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        # Check for required parameters
        if not all(key in config for key in ['connector_type', 'connector_module_path', 'connector_class_name']):
            logger.error("Missing required parameters in connection configuration")
            return False

        # Validate that the specified module can be imported
        try:
            module_path = config['connector_module_path']
            importlib.import_module(module_path)
        except ImportError as e:
            logger.error(f"Failed to import module {config['connector_module_path']}: {e}")
            return False

        # Validate that the specified class exists in the module
        try:
            module = importlib.import_module(config['connector_module_path'])
            class_name = config['connector_class_name']
            getattr(module, class_name)
        except AttributeError as e:
            logger.error(f"Class {config['connector_class_name']} not found in module {config['connector_module_path']}: {e}")
            return False

        # Validate custom_config format if provided
        if 'custom_config' in config and not isinstance(config['custom_config'], dict):
            logger.error("custom_config must be a dictionary")
            return False

        # All validations passed
        return True

    @retry_decorator.retry(max_attempts=MAX_RETRY_ATTEMPTS)
    def load_custom_implementation(self) -> object:
        """
        Load and instantiate the custom connector implementation

        Returns:
            Instance of the custom connector implementation
        """
        try:
            # Import the module specified by connector_module_path
            module_path = self.connector_module_path
            module = importlib.import_module(module_path)

            # Get the class specified by connector_class_name from the module
            class_name = self.connector_class_name
            connector_class = getattr(module, class_name)

            # Verify the class implements required interface methods
            if not self._validate_custom_implementation(connector_class):
                raise TypeError("Custom implementation does not implement required methods")

            # Instantiate the class with custom_config
            instance = connector_class(**self.custom_config)

            # Return the instance
            return instance

        except Exception as e:
            logger.error(f"Failed to load custom implementation from {self.connector_module_path}: {e}")
            raise

    def get_connection_stats(self) -> dict:
        """
        Get statistics about custom connector usage

        Returns:
            Connection and usage statistics
        """
        # Collect standard connection statistics
        stats = super().get_connection_stats()

        # Add custom connector specific statistics if available
        if self.custom_implementation and hasattr(self.custom_implementation, 'get_stats') and callable(self.custom_implementation.get_stats):
            try:
                custom_stats = self.custom_implementation.get_stats()
                if isinstance(custom_stats, dict):
                    stats['custom_stats'] = custom_stats
                else:
                    logger.warning("Custom implementation's get_stats method did not return a dictionary")
            except Exception as e:
                logger.error(f"Error calling custom implementation's get_stats method: {e}")

        # Return formatted statistics dictionary
        return stats

    def reset_connection_stats(self) -> None:
        """
        Reset the custom connector usage statistics
        """
        # Reset standard connection statistics
        super().reset_connection_stats()

        # Reset custom connector specific statistics if applicable
        if self.custom_implementation and hasattr(self.custom_implementation, 'reset_stats') and callable(self.custom_implementation.reset_stats):
            try:
                self.custom_implementation.reset_stats()
                self.connection_stats['custom_stats'] = {}
            except Exception as e:
                logger.error(f"Error calling custom implementation's reset_stats method: {e}")

        logger.info(f"Reset connection statistics for {self.source_name} (ID: {self.source_id})")

    def _validate_custom_implementation(self, implementation: object) -> bool:
        """
        Validate that the custom implementation has required methods

        Args:
            implementation: The custom implementation object

        Returns:
            True if implementation is valid, False otherwise
        """
        # Check for required methods
        required_methods = ['connect', 'disconnect', 'extract_data', 'get_source_schema']
        for method_name in required_methods:
            if not hasattr(implementation, method_name) or not callable(getattr(implementation, method_name)):
                logger.error(f"Custom implementation missing required method: {method_name}")
                return False

        # Verify method signatures match expected interface
        try:
            connect_sig = inspect.signature(implementation.connect)
            extract_data_sig = inspect.signature(implementation.extract_data)
            get_source_schema_sig = inspect.signature(implementation.get_source_schema)

            # Basic signature checks (more detailed checks can be added)
            if len(connect_sig.parameters) != 1:
                logger.error("connect method must have no parameters")
                return False
            if len(extract_data_sig.parameters) != 1:
                logger.error("extract_data method must have one parameter")
                return False
            if len(get_source_schema_sig.parameters) != 1:
                logger.error("get_source_schema method must have one parameter")
                return False

        except Exception as e:
            logger.error(f"Error validating method signatures: {e}")
            return False

        # All validations passed
        return True

    def _standardize_metadata(self, custom_metadata: dict) -> dict:
        """
        Standardize metadata format from custom implementation

        Args:
            custom_metadata: Source-specific metadata to format

        Returns:
            Standardized metadata
        """
        # Create base metadata structure with standard fields
        metadata = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'extraction_time': datetime.datetime.now().isoformat(),
            'extraction_status': 'success',
        }

        # Add source information (source_id, source_name, source_type)

        # Add extraction timestamp

        # Merge custom metadata with standard structure
        if custom_metadata:
            metadata.update(custom_metadata)

        # Ensure all required metadata fields are present

        # Return standardized metadata dictionary
        return metadata

    def _standardize_schema(self, custom_schema: dict) -> dict:
        """
        Standardize schema format from custom implementation

        Args:
            custom_schema: Source-specific schema to format

        Returns:
            Standardized schema
        """
        # Create base schema structure with standard fields
        schema = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'fields': []
        }

        # Ensure fields array is properly formatted

        # Standardize data type representations

        # Add schema metadata (source, timestamp, version)

        # Return standardized schema dictionary
        return schema