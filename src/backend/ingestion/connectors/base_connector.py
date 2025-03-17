"""
Base connector module that defines the abstract interface for all data source connectors
in the self-healing data pipeline.

This module provides:
- An abstract base class that standardizes connector interfaces
- Common functionality for connection management and statistics
- Error handling for extraction operations
- A factory for creating and managing connector instances

All specific data source connectors (GCS, Cloud SQL, API, etc.) should inherit from
the BaseConnector class and implement its abstract methods.
"""

import abc
from typing import Dict, List, Optional, Any, Tuple, Union
import datetime
import pandas as pd

from ...constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS
from ...utils.logging.logger import get_logger
from ..errors.error_handler import handle_error, with_error_handling

# Set up module logger
logger = get_logger(__name__)


class BaseConnector(abc.ABC):
    """
    Abstract base class that defines the interface for all data source connectors.
    
    This class establishes the contract that specific connector implementations must follow,
    including connection management, data extraction, schema retrieval, and error handling.
    
    All connector implementations should inherit from this class and implement
    the abstract methods to provide source-specific functionality.
    """
    
    def __init__(
        self,
        source_id: str,
        source_name: str,
        source_type: DataSourceType,
        connection_config: Dict[str, Any]
    ):
        """
        Initialize the base connector with source information and connection configuration.
        
        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            source_type: Type of data source (from DataSourceType enum)
            connection_config: Configuration parameters for connecting to the source
        """
        # Store source information
        self.source_id = source_id
        self.source_name = source_name
        self.source_type = source_type
        self.connection_config = connection_config
        
        # Initialize connection state
        self.is_connected = False
        self.last_connected_time = None
        self.last_disconnected_time = None
        
        # Initialize connection statistics
        self.connection_attempts = 0
        self.successful_connections = 0
        self.failed_connections = 0
        self.connection_stats = {
            'avg_connection_time_ms': 0,
            'last_error': None,
            'last_connection_time_ms': 0
        }
        
        # Validate connection configuration
        if not self.validate_connection_config(connection_config):
            logger.error(f"Invalid connection configuration for {source_name} (ID: {source_id})")
            raise ValueError(f"Invalid connection configuration for {source_name}")
        
        logger.info(f"Initialized {source_type.value} connector for {source_name} (ID: {source_id})")
    
    @abc.abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Implementations should:
        1. Attempt to connect to the data source using the connection_config
        2. Update connection state and statistics using _update_connection_state()
        3. Handle connection errors appropriately
        4. Return True if connection successful, False otherwise
        
        Returns:
            True if connection was successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def disconnect(self) -> bool:
        """
        Close connection to the data source.
        
        Implementations should:
        1. Attempt to gracefully close any open connections
        2. Update connection state using _update_connection_state()
        3. Handle disconnection errors appropriately
        4. Return True if disconnection successful, False otherwise
        
        Returns:
            True if disconnection was successful, False otherwise
        """
        pass
    
    @abc.abstractmethod
    def extract_data(self, extraction_params: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Extract data from the source based on extraction parameters.
        
        Implementations should:
        1. Validate extraction parameters using _validate_extraction_params()
        2. Connect to the source if not already connected
        3. Extract data according to parameters
        4. Format metadata using _format_metadata()
        5. Handle extraction errors using _handle_extraction_error()
        
        Args:
            extraction_params: Parameters controlling the extraction process,
                including what data to extract and how to extract it
        
        Returns:
            Tuple containing:
                - Extracted data as pandas DataFrame (or None if extraction failed)
                - Metadata dictionary with extraction details
        """
        pass
    
    @abc.abstractmethod
    def get_source_schema(self, object_name: str) -> Dict[str, Any]:
        """
        Retrieve the schema information for a source object.
        
        Implementations should:
        1. Connect to the source if not already connected
        2. Retrieve schema information for the specified object
        3. Format schema in a standardized dictionary structure
        4. Handle schema retrieval errors appropriately
        
        Args:
            object_name: Name of the object (table, file, etc.) to get schema for
            
        Returns:
            Dictionary containing schema definition for the specified object
        """
        pass
    
    @abc.abstractmethod
    def validate_connection_config(self, config: Dict[str, Any]) -> bool:
        """
        Validate the connection configuration.
        
        Implementations should:
        1. Check that all required parameters are present
        2. Validate parameter types and values
        3. Return True if configuration is valid, False otherwise
        
        Args:
            config: Connection configuration to validate
            
        Returns:
            True if configuration is valid, False otherwise
        """
        pass
    
    def get_connection_status(self) -> Dict[str, Any]:
        """
        Get the current connection status and statistics.
        
        Returns:
            Dictionary containing connection state information and statistics
        """
        status = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'is_connected': self.is_connected,
            'connection_attempts': self.connection_attempts,
            'successful_connections': self.successful_connections,
            'failed_connections': self.failed_connections,
            'last_connected_time': self.last_connected_time,
            'last_disconnected_time': self.last_disconnected_time,
        }
        
        # Add additional connection statistics
        status.update(self.connection_stats)
        
        return status
    
    def reset_connection_stats(self) -> None:
        """
        Reset connection statistics.
        
        This method resets all connection counters and statistics while 
        preserving current connection state.
        """
        self.connection_attempts = 0
        self.successful_connections = 0
        self.failed_connections = 0
        self.connection_stats = {
            'avg_connection_time_ms': 0,
            'last_error': None,
            'last_connection_time_ms': 0
        }
        logger.info(f"Reset connection statistics for {self.source_name} (ID: {self.source_id})")
    
    def _update_connection_state(self, connected: bool, success: bool) -> None:
        """
        Update connection state and statistics.
        
        Args:
            connected: Whether the connector is now connected
            success: Whether the connection/disconnection operation was successful
        """
        # Update connected state
        previous_state = self.is_connected
        self.is_connected = connected
        
        # Update timestamps
        if connected and success and (not previous_state or previous_state != connected):
            self.last_connected_time = datetime.datetime.now()
        elif not connected and (previous_state or previous_state != connected):
            self.last_disconnected_time = datetime.datetime.now()
        
        # Update connection attempt statistics
        self.connection_attempts += 1
        if success:
            self.successful_connections += 1
        else:
            self.failed_connections += 1
        
        # Calculate success rate
        success_rate = self.successful_connections / self.connection_attempts if self.connection_attempts > 0 else 0
        self.connection_stats['success_rate'] = success_rate
        
        # Log state change
        if previous_state != connected:
            logger.info(
                f"Connection state changed for {self.source_name} (ID: {self.source_id}): "
                f"{'Connected' if connected else 'Disconnected'}"
            )
    
    def _validate_extraction_params(self, extraction_params: Dict[str, Any]) -> bool:
        """
        Validate extraction parameters.
        
        Base implementation checks that extraction_params is a dictionary.
        Subclasses should extend this with source-specific validation.
        
        Args:
            extraction_params: Parameters controlling the extraction process
            
        Returns:
            True if parameters are valid, False otherwise
        """
        if not isinstance(extraction_params, dict):
            logger.error(f"Extraction parameters must be a dictionary, got {type(extraction_params)}")
            return False
        
        # Basic validation that extraction_params contains something
        if not extraction_params:
            logger.warning(f"Empty extraction parameters for {self.source_name} (ID: {self.source_id})")
            # Empty params might be valid for some connectors that extract everything
            return True
        
        # Subclasses should extend this with source-specific validation
        return True
    
    def _format_metadata(self, raw_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format metadata in a standardized structure.
        
        Args:
            raw_metadata: Source-specific metadata to format
            
        Returns:
            Formatted metadata dictionary with standardized fields
        """
        # Create base metadata structure
        metadata = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'extraction_time': datetime.datetime.now().isoformat(),
            'extraction_status': 'success',
        }
        
        # Add source-specific metadata
        if raw_metadata:
            metadata.update(raw_metadata)
        
        return metadata
    
    @with_error_handling(context={'component': 'BaseConnector'})
    def _handle_extraction_error(self, exception: Exception, extraction_params: Dict[str, Any]) -> Tuple[None, Dict[str, Any]]:
        """
        Handle errors during data extraction.
        
        Args:
            exception: The exception that occurred
            extraction_params: The extraction parameters used
            
        Returns:
            Tuple containing:
                - None (indicating extraction failure)
                - Error metadata dictionary
        """
        # Log the extraction error with context
        logger.error(
            f"Error extracting data from {self.source_name} (ID: {self.source_id}): {str(exception)}",
            exc_info=exception
        )
        
        # Create error metadata
        error_metadata = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'extraction_time': datetime.datetime.now().isoformat(),
            'extraction_status': 'error',
            'error_message': str(exception),
            'error_type': type(exception).__name__,
            'extraction_params': extraction_params
        }
        
        # Pass to error handler for potential self-healing
        handle_error(exception, {
            'source_id': self.source_id,
            'source_type': self.source_type.value,
            'extraction_params': extraction_params
        }, raise_exception=False)
        
        # Return None for data and error metadata
        return None, error_metadata


class ConnectorFactory:
    """
    Factory class for creating connector instances based on source type.
    
    This class manages the registration of connector implementations and
    provides a centralized way to create appropriate connector instances
    for different data source types.
    """
    
    def __init__(self):
        """
        Initialize the connector factory.
        """
        # Registry of connector classes keyed by source type
        self._connector_registry = {}
    
    def register_connector(self, source_type: DataSourceType, connector_class: type) -> None:
        """
        Register a connector class for a specific source type.
        
        Args:
            source_type: The type of data source this connector handles
            connector_class: The connector class (must be a BaseConnector subclass)
        """
        # Validate that connector_class is a subclass of BaseConnector
        if not issubclass(connector_class, BaseConnector):
            raise TypeError(f"Connector class must be a subclass of BaseConnector: {connector_class.__name__}")
        
        # Register the connector class
        self._connector_registry[source_type] = connector_class
        logger.info(f"Registered connector {connector_class.__name__} for source type {source_type.value}")
    
    def create_connector(
        self,
        source_id: str,
        source_name: str,
        source_type: DataSourceType,
        connection_config: Dict[str, Any]
    ) -> BaseConnector:
        """
        Create a connector instance for a specific source.
        
        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            source_type: Type of data source (from DataSourceType enum)
            connection_config: Configuration parameters for connecting to the source
            
        Returns:
            Connector instance for the specified source
            
        Raises:
            ValueError: If no connector is registered for the specified source type
        """
        # Check if source_type is registered
        if source_type not in self._connector_registry:
            logger.error(f"No connector registered for source type {source_type.value}")
            raise ValueError(f"No connector registered for source type {source_type.value}")
        
        # Get connector class from registry
        connector_class = self._connector_registry[source_type]
        
        # Instantiate and return the connector
        logger.info(f"Creating connector for {source_name} (ID: {source_id}, Type: {source_type.value})")
        return connector_class(source_id, source_name, source_type, connection_config)
    
    def get_registered_connectors(self) -> List[DataSourceType]:
        """
        Get a list of registered connector types.
        
        Returns:
            List of registered DataSourceType values
        """
        return list(self._connector_registry.keys())