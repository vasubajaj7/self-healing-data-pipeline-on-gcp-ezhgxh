"""
Incremental Extractor for the self-healing data pipeline.

This module implements an incremental extraction component that enables efficient 
extraction of data changes since the last extraction. It supports various change 
data capture (CDC) patterns, tracks extraction state, and provides optimized data 
loading with minimal source system impact.

The extractor utilizes Firestore for state tracking, supports different incremental 
extraction strategies (timestamp, sequence, version), and provides detailed statistics 
for monitoring and optimization.
"""

import hashlib
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, Union

# Import constants and configuration utilities
from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, DataSourceType
from ...config import get_config
from ...utils.logging.logger import get_logger
from ..errors.error_handler import with_error_handling, retry_with_backoff
from ...utils.storage.firestore_client import get_firestore_client

# Set up logger
logger = get_logger(__name__)

# Default values
DEFAULT_STATE_COLLECTION = "incremental_extraction_state"
DEFAULT_LOOKBACK_WINDOW = 24  # Hours
DEFAULT_WATERMARK_BUFFER = 60  # Seconds


class IncrementalExtractor:
    """
    Extractor for incrementally processing data changes with state tracking and optimized extraction.
    
    This class implements efficient incremental data loading by tracking the high watermark
    of previous extractions and only retrieving new or changed data. It supports various
    Change Data Capture (CDC) patterns and integrates with the self-healing framework.
    """

    def __init__(self, source_id: str, source_name: str, extraction_config: Dict[str, Any]):
        """
        Initialize the incremental extractor with source information and extraction configuration.
        
        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            extraction_config: Configuration for the extraction process
        """
        # Store source information
        self.source_id = source_id
        self.source_name = source_name
        self.extraction_config = extraction_config
        
        # Initialize statistics tracking
        self.incremental_stats = {
            'attempts': 0,
            'successes': 0,
            'failures': 0,
            'total_records': 0,
            'total_processing_time': 0,
            'avg_processing_time': 0,
            'last_extraction_time': None,
            'extractions': []
        }
        
        # Initialize Firestore client for state storage
        self.state_client = get_firestore_client()
        
        # Get state collection name from config or use default
        config = get_config()
        self.state_collection = extraction_config.get(
            'state_collection', 
            config.get('ingestion.state_collection', DEFAULT_STATE_COLLECTION)
        )
        
        # Get lookback window from config or use default
        self.lookback_window_hours = extraction_config.get(
            'lookback_window_hours',
            config.get('ingestion.lookback_window_hours', DEFAULT_LOOKBACK_WINDOW)
        )
        
        # Get watermark buffer from config or use default
        self.watermark_buffer_seconds = extraction_config.get(
            'watermark_buffer_seconds',
            config.get('ingestion.watermark_buffer_seconds', DEFAULT_WATERMARK_BUFFER)
        )
        
        logger.info(
            f"Initialized incremental extractor for source {source_name} (ID: {source_id})"
        )

    @with_error_handling(context={'component': 'IncrementalExtractor', 'operation': 'extract_incremental'}, raise_exception=True)
    def extract_incremental(self, extraction_params: Dict[str, Any], connector: Any) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extract data incrementally using the provided connector.
        
        Args:
            extraction_params: Parameters for the extraction including table_name and incremental_column
            connector: Data source connector object with extraction capabilities
            
        Returns:
            Tuple containing the extracted data as DataFrame and extraction metadata
        """
        # Validate extraction parameters
        if not self._validate_incremental_params(extraction_params):
            raise ValueError(f"Invalid extraction parameters: {extraction_params}")
            
        start_time = datetime.now()
        
        # Extract required parameters
        table_name = extraction_params.get('table_name')
        incremental_column = extraction_params.get('incremental_column')
        column_type = extraction_params.get('column_type', 'timestamp')
        
        # Generate unique key for this extraction
        extraction_key = self.generate_extraction_key(extraction_params)
        
        # Retrieve previous extraction state
        previous_state = self.get_extraction_state(extraction_key)
        logger.info(f"Previous extraction state for {table_name}: {previous_state}")
        
        # Get the last high watermark or set initial value if none exists
        last_value = previous_state.get('last_value')
        if last_value is None:
            last_value = self.determine_initial_value(
                connector,
                table_name,
                incremental_column,
                column_type
            )
            logger.info(f"No previous state found. Using initial value: {last_value}")
        
        # Determine extraction window parameters
        current_time = datetime.now()
        
        # Execute incremental extraction based on column type
        if column_type == 'timestamp':
            # For timestamp columns, we need to handle timezone and format issues
            if isinstance(last_value, str):
                try:
                    last_value = datetime.fromisoformat(last_value.replace('Z', '+00:00'))
                except ValueError:
                    last_value = datetime.strptime(last_value, "%Y-%m-%d %H:%M:%S")
            
            # Add extraction parameters
            extraction_params['from_timestamp'] = last_value
            extraction_params['to_timestamp'] = current_time
            
            # Log extraction window
            logger.info(
                f"Extracting data from {table_name} where {incremental_column} is between "
                f"{last_value} and {current_time}"
            )
        elif column_type in ('sequence', 'numeric'):
            # For numeric sequence columns (IDs, etc.)
            extraction_params['from_value'] = last_value
            
            logger.info(
                f"Extracting data from {table_name} where {incremental_column} > {last_value}"
            )
        elif column_type == 'version':
            # For version-based tracking
            extraction_params['from_version'] = last_value
            
            logger.info(
                f"Extracting data from {table_name} where {incremental_column} > {last_value}"
            )
        else:
            # Default for string or other column types
            extraction_params['from_value'] = last_value
            
            logger.info(
                f"Extracting data from {table_name} where {incremental_column} > '{last_value}'"
            )
        
        # Call connector's extract method with incremental parameters
        data = connector.extract(extraction_params)
        
        # Calculate the new high watermark from the extracted data
        new_high_watermark = self.calculate_high_watermark(data, incremental_column, last_value)
        
        # Update extraction state with new high watermark
        extraction_metadata = {
            'table_name': table_name,
            'incremental_column': incremental_column,
            'column_type': column_type,
            'record_count': len(data),
            'extraction_start': start_time.isoformat(),
            'extraction_end': datetime.now().isoformat()
        }
        
        self.update_extraction_state(extraction_key, new_high_watermark, extraction_metadata)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Update statistics
        self._update_incremental_stats(
            success=True,
            extraction_metadata=extraction_metadata,
            processing_time=processing_time
        )
        
        logger.info(
            f"Successfully extracted {len(data)} records incrementally from {table_name}. "
            f"New high watermark: {new_high_watermark}. "
            f"Processing time: {processing_time:.2f} seconds."
        )
        
        # Return extracted data and metadata
        return data, extraction_metadata

    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def get_extraction_state(self, extraction_key: str) -> Dict[str, Any]:
        """
        Retrieve the previous extraction state for a specific extraction.
        
        Args:
            extraction_key: Unique key for the extraction state
            
        Returns:
            Previous extraction state or default initial state
        """
        try:
            # Generate document ID from extraction key
            doc_id = f"state_{extraction_key}"
            
            # Retrieve state document from Firestore
            doc_ref = self.state_client.collection(self.state_collection).document(doc_id)
            doc = doc_ref.get()
            
            if doc.exists:
                # Return existing state
                state_data = doc.to_dict()
                logger.debug(f"Retrieved extraction state: {state_data}")
                return state_data
            else:
                # Return default initial state
                logger.debug(f"No extraction state found for key: {extraction_key}")
                return {
                    'last_value': None,
                    'last_updated': None,
                    'extraction_history': []
                }
        except Exception as e:
            logger.error(f"Error retrieving extraction state: {e}")
            # Return default initial state on error
            return {
                'last_value': None,
                'last_updated': None,
                'extraction_history': []
            }

    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def update_extraction_state(self, extraction_key: str, new_high_watermark: Any, extraction_metadata: Dict[str, Any]) -> bool:
        """
        Update the extraction state with new high watermark.
        
        Args:
            extraction_key: Unique key for the extraction state
            new_high_watermark: New high watermark value
            extraction_metadata: Additional metadata about the extraction
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            # Generate document ID from extraction key
            doc_id = f"state_{extraction_key}"
            
            # Create state document
            current_time = datetime.now().isoformat()
            
            # Convert watermark to string if it's a datetime object
            watermark_value = new_high_watermark
            if isinstance(new_high_watermark, datetime):
                watermark_value = new_high_watermark.isoformat()
            
            # Get current state to append to history
            current_state = self.get_extraction_state(extraction_key)
            history = current_state.get('extraction_history', [])
            
            # Add current extraction to history (keep last 10)
            history_item = {
                'timestamp': current_time,
                'watermark': watermark_value,
                'record_count': extraction_metadata.get('record_count', 0)
            }
            history.append(history_item)
            if len(history) > 10:
                history = history[-10:]
            
            # Create updated state document
            state_doc = {
                'last_value': watermark_value,
                'last_updated': current_time,
                'source_id': self.source_id,
                'table_name': extraction_metadata.get('table_name', ''),
                'incremental_column': extraction_metadata.get('incremental_column', ''),
                'column_type': extraction_metadata.get('column_type', ''),
                'extraction_history': history,
                'extraction_metadata': extraction_metadata
            }
            
            # Store state document
            doc_ref = self.state_client.collection(self.state_collection).document(doc_id)
            doc_ref.set(state_doc)
            
            logger.debug(f"Updated extraction state with new watermark: {watermark_value}")
            return True
        except Exception as e:
            logger.error(f"Error updating extraction state: {e}")
            return False

    def generate_extraction_key(self, extraction_params: Dict[str, Any]) -> str:
        """
        Generate a unique key for tracking extraction state.
        
        Args:
            extraction_params: Extraction parameters including table and column info
            
        Returns:
            Unique extraction state key
        """
        # Extract key components
        source_id = self.source_id
        table_name = extraction_params.get('table_name', '')
        incremental_column = extraction_params.get('incremental_column', '')
        
        if not source_id or not table_name or not incremental_column:
            logger.warning("Missing required parameters for generating extraction key")
            # Use available values with fallbacks for missing ones
            component_str = f"{source_id or 'unknown'}_{table_name or 'unknown'}_{incremental_column or 'unknown'}"
        else:
            # Combine components into a string
            component_str = f"{source_id}_{table_name}_{incremental_column}"
        
        # Generate hash for consistent key
        key_hash = hashlib.md5(component_str.encode()).hexdigest()
        
        return key_hash

    def determine_initial_value(self, connector: Any, table_name: str, incremental_column: str, column_type: str) -> Any:
        """
        Determine the initial value for incremental extraction if no state exists.
        
        Args:
            connector: Data source connector object
            table_name: Name of the table being extracted
            incremental_column: Column used for incremental extraction
            column_type: Type of the incremental column (timestamp, numeric, etc.)
            
        Returns:
            Initial value for extraction
        """
        # For timestamp columns, use current time minus lookback window
        if column_type == 'timestamp':
            initial_time = datetime.now() - timedelta(hours=self.lookback_window_hours)
            logger.info(
                f"Using initial timestamp value: {initial_time} "
                f"(current time - {self.lookback_window_hours} hours)"
            )
            return initial_time
        
        # For numeric columns, use 0 as initial value
        elif column_type in ('sequence', 'numeric'):
            logger.info("Using initial numeric value: 0")
            return 0
        
        # For version columns, use 0 as initial value
        elif column_type == 'version':
            logger.info("Using initial version value: 0")
            return 0
        
        # For string columns, use empty string
        elif column_type == 'string':
            logger.info("Using initial string value: ''")
            return ''
        
        # Default case
        else:
            logger.info(f"Using default initial value None for column type: {column_type}")
            return None

    def calculate_high_watermark(self, data: pd.DataFrame, incremental_column: str, previous_high_watermark: Any) -> Any:
        """
        Calculate the new high watermark value from extracted data.
        
        Args:
            data: Extracted data as DataFrame
            incremental_column: Column used for incremental extraction
            previous_high_watermark: Previous high watermark value
            
        Returns:
            New high watermark value
        """
        # If data is empty, return previous high watermark
        if data is None or len(data) == 0:
            logger.info("No data extracted, keeping previous high watermark")
            return previous_high_watermark
        
        # Ensure the incremental column exists in the data
        if incremental_column not in data.columns:
            logger.warning(
                f"Incremental column '{incremental_column}' not found in extracted data, "
                f"keeping previous high watermark"
            )
            return previous_high_watermark
        
        try:
            # Find maximum value in the incremental column
            max_value = data[incremental_column].max()
            
            # For timestamp columns, add buffer to avoid missing data
            if isinstance(max_value, (pd.Timestamp, datetime)):
                # Add buffer to timestamp watermark
                if self.watermark_buffer_seconds > 0:
                    max_value = max_value + timedelta(seconds=self.watermark_buffer_seconds)
                    logger.debug(
                        f"Added buffer of {self.watermark_buffer_seconds} seconds to timestamp watermark"
                    )
            
            # Ensure new watermark is at least equal to previous watermark
            if previous_high_watermark is not None:
                # For timestamp comparison
                if isinstance(max_value, (pd.Timestamp, datetime)) and isinstance(previous_high_watermark, (pd.Timestamp, datetime, str)):
                    # Convert string to datetime if needed
                    if isinstance(previous_high_watermark, str):
                        try:
                            previous_high_watermark = datetime.fromisoformat(
                                previous_high_watermark.replace('Z', '+00:00')
                            )
                        except ValueError:
                            previous_high_watermark = datetime.strptime(
                                previous_high_watermark, "%Y-%m-%d %H:%M:%S"
                            )
                    
                    if max_value < previous_high_watermark:
                        logger.warning(
                            f"New timestamp watermark {max_value} is earlier than previous "
                            f"watermark {previous_high_watermark}, using previous watermark"
                        )
                        return previous_high_watermark
                # For numeric comparison
                elif isinstance(max_value, (int, float)) and isinstance(previous_high_watermark, (int, float)):
                    if max_value < previous_high_watermark:
                        logger.warning(
                            f"New numeric watermark {max_value} is less than previous "
                            f"watermark {previous_high_watermark}, using previous watermark"
                        )
                        return previous_high_watermark
                # For string comparison
                elif isinstance(max_value, str) and isinstance(previous_high_watermark, str):
                    if max_value < previous_high_watermark:
                        logger.warning(
                            f"New string watermark '{max_value}' is less than previous "
                            f"watermark '{previous_high_watermark}', using previous watermark"
                        )
                        return previous_high_watermark
            
            logger.info(f"New high watermark calculated: {max_value}")
            return max_value
            
        except Exception as e:
            logger.error(f"Error calculating high watermark: {e}")
            return previous_high_watermark

    def get_incremental_stats(self) -> Dict[str, Any]:
        """
        Get statistics about incremental extraction operations.
        
        Returns:
            Incremental extraction statistics
        """
        # Calculate derived metrics
        if self.incremental_stats['successes'] > 0:
            self.incremental_stats['avg_processing_time'] = (
                self.incremental_stats['total_processing_time'] / 
                self.incremental_stats['successes']
            )
            
            self.incremental_stats['avg_records_per_extraction'] = (
                self.incremental_stats['total_records'] / 
                self.incremental_stats['successes']
            )
        
        # Return a copy of the statistics
        return self.incremental_stats.copy()

    def reset_incremental_stats(self) -> None:
        """
        Reset the incremental extraction statistics.
        """
        self.incremental_stats = {
            'attempts': 0,
            'successes': 0,
            'failures': 0,
            'total_records': 0,
            'total_processing_time': 0,
            'avg_processing_time': 0,
            'last_extraction_time': None,
            'extractions': []
        }
        logger.info("Incremental extraction statistics reset")

    def _update_incremental_stats(self, success: bool, extraction_metadata: Dict[str, Any], processing_time: float) -> None:
        """
        Update incremental statistics after an extraction.
        
        Args:
            success: Whether the extraction was successful
            extraction_metadata: Metadata about the extraction
            processing_time: Time taken for extraction in seconds
        """
        # Update counters
        self.incremental_stats['attempts'] += 1
        
        if success:
            self.incremental_stats['successes'] += 1
            # Add record count to total
            record_count = extraction_metadata.get('record_count', 0)
            self.incremental_stats['total_records'] += record_count
        else:
            self.incremental_stats['failures'] += 1
        
        # Update timing information
        self.incremental_stats['total_processing_time'] += processing_time
        self.incremental_stats['last_extraction_time'] = datetime.now().isoformat()
        
        # Add extraction details
        extraction_entry = {
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'record_count': extraction_metadata.get('record_count', 0),
            'processing_time': processing_time,
            'table_name': extraction_metadata.get('table_name', ''),
            'incremental_column': extraction_metadata.get('incremental_column', '')
        }
        
        # Keep last 10 extractions
        self.incremental_stats['extractions'].append(extraction_entry)
        if len(self.incremental_stats['extractions']) > 10:
            self.incremental_stats['extractions'] = self.incremental_stats['extractions'][-10:]
        
        logger.debug(f"Updated incremental stats: {success=}, {record_count=}, {processing_time=:.2f}s")

    def _validate_incremental_params(self, extraction_params: Dict[str, Any]) -> bool:
        """
        Validate parameters required for incremental extraction.
        
        Args:
            extraction_params: Extraction parameters to validate
            
        Returns:
            True if parameters are valid, False otherwise
        """
        # Check if extraction_params is a dictionary
        if not isinstance(extraction_params, dict):
            logger.error("Extraction parameters must be a dictionary")
            return False
        
        # Check for required parameters
        required_params = ['table_name', 'incremental_column']
        for param in required_params:
            if param not in extraction_params:
                logger.error(f"Missing required parameter: {param}")
                return False
        
        # Validate optional parameters if present
        if 'column_type' in extraction_params:
            valid_types = ['timestamp', 'sequence', 'numeric', 'version', 'string']
            if extraction_params['column_type'] not in valid_types:
                logger.error(
                    f"Invalid column_type: {extraction_params['column_type']}. "
                    f"Must be one of {valid_types}"
                )
                return False
        
        logger.debug("Validated incremental extraction parameters successfully")
        return True