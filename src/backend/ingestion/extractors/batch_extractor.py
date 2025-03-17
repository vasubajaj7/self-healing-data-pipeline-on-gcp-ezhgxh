"""
Batch Extractor for the self-healing data pipeline.

This module implements functionality for processing large datasets in manageable
batches to optimize memory usage and enable efficient processing of big data sources.
It includes capabilities for batch-based extraction, progress tracking, memory optimization,
and integration with the self-healing system.
"""

import time
import math
import random
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Union

# Internal imports
from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, DataSourceType
from ...config import get_config
from ...utils.logging.logger import get_logger
from ..errors.error_handler import with_error_handling, retry_with_backoff

# Set up logging
logger = get_logger(__name__)

# Default batch processing constants
DEFAULT_BATCH_SIZE = 10000
DEFAULT_MAX_BATCHES = None  # No limit by default
DEFAULT_BATCH_TIMEOUT_SECONDS = 3600  # 1 hour


class BatchExtractor:
    """
    Extractor for processing large datasets in manageable batches with progress tracking
    and error handling. Enables efficient memory usage and provides integration points
    for self-healing capabilities.
    """

    def __init__(self, source_id: str, source_name: str, extraction_config: Dict[str, Any]):
        """
        Initialize the batch extractor with source information and extraction configuration.

        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            extraction_config: Configuration parameters for the extraction process
        """
        # Store source identification information
        self.source_id = source_id
        self.source_name = source_name
        self.extraction_config = extraction_config

        # Initialize batch processing configuration
        self.default_batch_size = extraction_config.get('batch_size', DEFAULT_BATCH_SIZE)
        self.max_batches = extraction_config.get('max_batches', DEFAULT_MAX_BATCHES)
        self.batch_timeout_seconds = extraction_config.get('batch_timeout_seconds', DEFAULT_BATCH_TIMEOUT_SECONDS)

        # Initialize batch statistics tracking
        self.batch_stats = {
            'total_batches_attempted': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_records_processed': 0,
            'total_processing_time': 0.0,
            'start_time': None,
            'end_time': None,
            'batch_history': [],
            'avg_batch_time': 0.0,
            'avg_records_per_batch': 0,
            'records_per_second': 0.0
        }

        logger.info(f"Initialized BatchExtractor for source: {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'BatchExtractor', 'operation': 'extract_in_batches'}, raise_exception=True)
    def extract_in_batches(self, extraction_params: Dict[str, Any], connector: Any) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extract data in batches using the provided connector.

        Args:
            extraction_params: Parameters for the extraction process
            connector: Data source connector object that implements batch extraction

        Returns:
            Tuple containing:
                - Combined DataFrame from all batches
                - Aggregated metadata about the extraction process
        """
        # Validate extraction parameters
        if not extraction_params:
            extraction_params = {}

        # Reset batch statistics for this extraction run
        self.reset_batch_stats()
        
        # Determine batch size to use
        batch_size = extraction_params.get('batch_size', self.default_batch_size)
        
        # Initialize data collection
        batch_results = []
        batch_metadata = []
        
        # Record start time and initialize stats
        self.batch_stats['start_time'] = time.time()
        
        # Try to estimate total records, if possible
        estimated_total_records = self.estimate_total_records(connector, extraction_params)
        
        if estimated_total_records:
            logger.info(f"Estimated total records to process: {estimated_total_records}")
            # Optimize batch size based on data volume
            batch_size = self.calculate_optimal_batch_size(connector, extraction_params, estimated_total_records)
        
        # Initialize batch processing
        batch_number = 1
        offset = 0
        records_processed = 0
        
        logger.info(f"Beginning batch extraction with batch size: {batch_size}")
        
        # Process data in batches
        while self._should_continue_processing(
            batch_number, records_processed, estimated_total_records, self.batch_stats['start_time']
        ):
            logger.info(f"Processing batch {batch_number} (offset: {offset})")
            
            try:
                # Process batch and track timing
                batch_start_time = time.time()
                batch_data, batch_meta = self.process_batch(
                    connector, extraction_params, batch_number, batch_size, offset
                )
                batch_processing_time = time.time() - batch_start_time
                
                # Append results if batch returned data
                if batch_data is not None and not batch_data.empty:
                    batch_results.append(batch_data)
                    batch_metadata.append(batch_meta)
                    
                    # Update statistics
                    batch_record_count = len(batch_data)
                    records_processed += batch_record_count
                    offset += batch_record_count
                    
                    self._update_batch_stats(
                        True,  # Success
                        {
                            'batch_number': batch_number,
                            'record_count': batch_record_count,
                            'offset': offset - batch_record_count
                        },
                        batch_processing_time
                    )
                    
                    logger.info(f"Batch {batch_number} processed successfully: {batch_record_count} records in {batch_processing_time:.2f}s")
                else:
                    # Empty batch means we've reached the end of data
                    logger.info(f"Batch {batch_number} returned no data, extraction complete")
                    break
                
                # Increment batch counter for next iteration
                batch_number += 1
                
            except Exception as e:
                # Log error but don't raise - the @with_error_handling decorator will handle it
                logger.error(f"Error processing batch {batch_number}: {str(e)}")
                
                # Update statistics for failed batch
                self._update_batch_stats(
                    False,  # Failure
                    {'batch_number': batch_number, 'error': str(e)},
                    time.time() - batch_start_time if 'batch_start_time' in locals() else 0
                )
                
                # Re-raise to allow error handling decorator to process it
                raise
        
        # Record end time
        self.batch_stats['end_time'] = time.time()
        
        # Combine results from all batches
        combined_data = self.combine_batch_data(batch_results)
        
        # Aggregate metadata from all batches
        aggregated_metadata = self.aggregate_batch_metadata(batch_metadata, batch_number - 1)
        
        # Final statistics
        total_time = self.batch_stats['end_time'] - self.batch_stats['start_time']
        logger.info(
            f"Extraction complete: {records_processed} records in {total_time:.2f}s "
            f"({records_processed / total_time:.2f} records/sec)"
        )
        
        return combined_data, aggregated_metadata

    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def process_batch(self, connector: Any, extraction_params: Dict[str, Any], 
                     batch_number: int, batch_size: int, offset: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process a single batch of data using the provided connector.

        Args:
            connector: Data source connector object
            extraction_params: Base extraction parameters
            batch_number: Current batch number (1-based)
            batch_size: Number of records to extract in this batch
            offset: Starting offset for this batch

        Returns:
            Tuple containing batch data and batch metadata
        """
        # Prepare batch-specific parameters
        batch_params = self.prepare_batch_params(extraction_params, batch_number, batch_size, offset)
        
        logger.debug(f"Processing batch {batch_number} with parameters: size={batch_size}, offset={offset}")
        
        # Track start time for performance monitoring
        start_time = time.time()
        
        # Call connector's extract method with batch parameters
        batch_data = connector.extract(batch_params)
        
        # Capture execution time
        execution_time = time.time() - start_time
        
        # Create metadata about this batch
        batch_metadata = {
            'batch_number': batch_number,
            'batch_size': batch_size,
            'offset': offset,
            'record_count': len(batch_data) if isinstance(batch_data, pd.DataFrame) else 0,
            'execution_time': execution_time,
            'timestamp': datetime.now().isoformat()
        }
        
        # Check for empty batch
        if batch_metadata['record_count'] == 0:
            logger.info(f"Batch {batch_number} returned no records")
        else:
            logger.info(
                f"Batch {batch_number} processed: {batch_metadata['record_count']} records "
                f"in {execution_time:.2f}s ({batch_metadata['record_count'] / execution_time:.2f} records/sec)"
            )
        
        return batch_data, batch_metadata

    def prepare_batch_params(self, extraction_params: Dict[str, Any], 
                           batch_number: int, batch_size: int, offset: int) -> Dict[str, Any]:
        """
        Prepare extraction parameters for a specific batch.

        Args:
            extraction_params: Base extraction parameters
            batch_number: Current batch number
            batch_size: Size of the current batch
            offset: Starting offset for this batch

        Returns:
            Batch-specific extraction parameters
        """
        # Create a copy of the original parameters to avoid modifying them
        batch_params = extraction_params.copy()
        
        # Add batch-specific parameters
        batch_params.update({
            'batch_number': batch_number,
            'batch_size': batch_size,
            'offset': offset,
            'is_batch': True,
            'batch_context': {
                'source_id': self.source_id,
                'source_name': self.source_name,
                'extraction_start_time': self.batch_stats['start_time'],
                'batch_start_time': time.time()
            }
        })
        
        return batch_params

    def combine_batch_data(self, batch_results: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine data from multiple batches into a single dataset.

        Args:
            batch_results: List of DataFrames from individual batches

        Returns:
            Combined DataFrame containing all records
        """
        if not batch_results:
            logger.warning("No batch results to combine")
            return pd.DataFrame()
        
        # Check if we have DataFrames
        if not all(isinstance(batch, pd.DataFrame) for batch in batch_results):
            logger.error("Not all batch results are pandas DataFrames")
            non_df_batches = [i for i, batch in enumerate(batch_results) 
                             if not isinstance(batch, pd.DataFrame)]
            logger.error(f"Non-DataFrame batches: {non_df_batches}")
            # Filter to only include DataFrames
            batch_results = [batch for batch in batch_results if isinstance(batch, pd.DataFrame)]
            
            if not batch_results:
                return pd.DataFrame()
        
        # Combine all DataFrames
        try:
            combined_df = pd.concat(batch_results, ignore_index=True)
            logger.info(f"Combined {len(batch_results)} batches into DataFrame with {len(combined_df)} records")
            return combined_df
        except Exception as e:
            logger.error(f"Error combining batch results: {str(e)}")
            # Try to combine batches one by one to identify problematic batches
            result_df = batch_results[0].copy()
            for i, batch_df in enumerate(batch_results[1:], 1):
                try:
                    result_df = pd.concat([result_df, batch_df], ignore_index=True)
                except Exception as e2:
                    logger.error(f"Error combining batch {i}: {str(e2)}")
                    # Continue with next batch
            return result_df

    def aggregate_batch_metadata(self, batch_metadata: List[Dict[str, Any]], 
                               total_batches: int) -> Dict[str, Any]:
        """
        Aggregate metadata from multiple batches.

        Args:
            batch_metadata: List of metadata dictionaries from individual batches
            total_batches: Total number of batches processed

        Returns:
            Aggregated metadata dictionary
        """
        if not batch_metadata:
            return {
                'total_batches': 0,
                'total_records': 0,
                'total_time': 0,
                'records_per_second': 0,
                'timestamp': datetime.now().isoformat(),
                'source_id': self.source_id,
                'source_name': self.source_name
            }
        
        # Calculate aggregated statistics
        total_records = sum(meta.get('record_count', 0) for meta in batch_metadata)
        total_time = sum(meta.get('execution_time', 0) for meta in batch_metadata)
        records_per_second = total_records / total_time if total_time > 0 else 0
        
        # Create the aggregated metadata
        aggregated = {
            'total_batches': total_batches,
            'total_records': total_records,
            'total_time': total_time,
            'records_per_second': records_per_second,
            'avg_batch_size': total_records / len(batch_metadata) if batch_metadata else 0,
            'avg_batch_time': total_time / len(batch_metadata) if batch_metadata else 0,
            'timestamp': datetime.now().isoformat(),
            'source_id': self.source_id,
            'source_name': self.source_name,
            'batch_details': batch_metadata if self.extraction_config.get('include_batch_details', False) else None
        }
        
        # Add batch execution statistics
        aggregated.update({
            'successful_batches': self.batch_stats['successful_batches'],
            'failed_batches': self.batch_stats['failed_batches'],
            'total_batches_attempted': self.batch_stats['total_batches_attempted']
        })
        
        return aggregated

    def estimate_total_records(self, connector: Any, extraction_params: Dict[str, Any]) -> Optional[int]:
        """
        Estimate the total number of records to be processed.

        Args:
            connector: Data source connector object
            extraction_params: Extraction parameters

        Returns:
            Estimated total records or None if unknown
        """
        # Check if connector provides count estimation method
        if hasattr(connector, 'estimate_record_count'):
            try:
                estimated_count = connector.estimate_record_count(extraction_params)
                logger.info(f"Estimated record count: {estimated_count}")
                return estimated_count
            except Exception as e:
                logger.warning(f"Failed to estimate record count: {str(e)}")
                return None
        
        # If there's a metadata-based way to get the count
        if hasattr(connector, 'get_metadata') and 'total_records' in (connector.get_metadata(extraction_params) or {}):
            try:
                metadata = connector.get_metadata(extraction_params)
                estimated_count = metadata.get('total_records')
                logger.info(f"Metadata-based record count: {estimated_count}")
                return estimated_count
            except Exception as e:
                logger.warning(f"Failed to get metadata record count: {str(e)}")
                return None
        
        # No way to estimate, return None
        logger.info("No method available to estimate record count")
        return None

    def calculate_optimal_batch_size(self, connector: Any, extraction_params: Dict[str, Any], 
                                   estimated_total_records: int) -> int:
        """
        Calculate the optimal batch size based on data characteristics.

        Args:
            connector: Data source connector object
            extraction_params: Extraction parameters
            estimated_total_records: Estimated total number of records

        Returns:
            Calculated optimal batch size
        """
        # Start with the default batch size
        batch_size = self.default_batch_size
        
        # Consider source type if provided
        source_type = extraction_params.get('source_type', self.extraction_config.get('source_type'))
        
        if source_type:
            # Adjust based on source type
            if isinstance(source_type, str):
                try:
                    source_type = DataSourceType(source_type)
                except ValueError:
                    # Not a valid enum value, use default
                    source_type = None
            
            if source_type == DataSourceType.GCS:
                # For file-based sources, larger batches often work better
                batch_size = max(batch_size, 25000)
            elif source_type == DataSourceType.CLOUD_SQL:
                # For database sources, moderate batches to avoid locking
                batch_size = min(batch_size, 15000)
            elif source_type == DataSourceType.API:
                # For API sources, smaller batches to respect rate limits
                batch_size = min(batch_size, 5000)
        
        # Adjust based on estimated total records
        if estimated_total_records:
            # For very small datasets, process in one batch
            if estimated_total_records < batch_size / 2:
                batch_size = estimated_total_records
            # For very large datasets, adjust to get a reasonable number of batches
            elif estimated_total_records > 1000000:
                # Aim for at most 100 batches
                batch_size = max(batch_size, estimated_total_records // 100)
        
        # Consider memory constraints
        memory_limit_mb = extraction_params.get('memory_limit_mb', 
                                              self.extraction_config.get('memory_limit_mb'))
        if memory_limit_mb:
            # Estimate memory per record (very rough estimate)
            record_size_bytes = extraction_params.get('avg_record_size_bytes', 1000)  # Default 1KB per record
            
            # Calculate max records that fit in memory limit (with 50% buffer)
            memory_limit_bytes = memory_limit_mb * 1024 * 1024 * 0.5
            memory_based_limit = int(memory_limit_bytes / record_size_bytes)
            
            # Apply memory-based limit
            batch_size = min(batch_size, memory_based_limit)
        
        # Apply min and max bounds to ensure reasonable batch size
        min_batch_size = extraction_params.get('min_batch_size', 100)
        max_batch_size = extraction_params.get('max_batch_size', 100000)
        
        batch_size = max(min_batch_size, min(batch_size, max_batch_size))
        
        logger.info(f"Calculated optimal batch size: {batch_size}")
        return batch_size

    def get_batch_stats(self) -> Dict[str, Any]:
        """
        Get statistics about batch processing operations.

        Returns:
            Dictionary containing batch processing statistics
        """
        # Calculate derived metrics
        stats = self.batch_stats.copy()
        
        # Calculate processing duration
        if stats['start_time'] and stats['end_time']:
            stats['total_duration'] = stats['end_time'] - stats['start_time']
        else:
            stats['total_duration'] = stats['total_processing_time']
        
        # Calculate averages if we have successful batches
        if stats['successful_batches'] > 0:
            stats['avg_batch_time'] = stats['total_processing_time'] / stats['successful_batches']
            stats['avg_records_per_batch'] = (stats['total_records_processed'] / 
                                             stats['successful_batches'])
        
        # Calculate records per second
        if stats['total_processing_time'] > 0:
            stats['records_per_second'] = stats['total_records_processed'] / stats['total_processing_time']
        
        # Calculate success rate
        if stats['total_batches_attempted'] > 0:
            stats['batch_success_rate'] = (stats['successful_batches'] / 
                                          stats['total_batches_attempted'])
        else:
            stats['batch_success_rate'] = 1.0
        
        # Format timestamps to ISO format
        if stats['start_time']:
            stats['start_time_iso'] = datetime.fromtimestamp(stats['start_time']).isoformat()
        if stats['end_time']:
            stats['end_time_iso'] = datetime.fromtimestamp(stats['end_time']).isoformat()
        
        # Return a clean copy without internal details
        return {k: v for k, v in stats.items() if not k.startswith('_')}

    def reset_batch_stats(self) -> None:
        """
        Reset the batch processing statistics.
        """
        # Reset to initial values
        self.batch_stats = {
            'total_batches_attempted': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_records_processed': 0,
            'total_processing_time': 0.0,
            'start_time': None,
            'end_time': None,
            'batch_history': [],
            'avg_batch_time': 0.0,
            'avg_records_per_batch': 0,
            'records_per_second': 0.0
        }
        logger.debug("Batch statistics reset")

    def _update_batch_stats(self, success: bool, batch_metadata: Dict[str, Any], 
                          processing_time: float) -> None:
        """
        Update batch statistics after processing a batch.

        Args:
            success: Whether the batch was processed successfully
            batch_metadata: Metadata about the batch
            processing_time: Time taken to process the batch in seconds
        """
        # Update attempt counter
        self.batch_stats['total_batches_attempted'] += 1
        
        # Update success/failure counters
        if success:
            self.batch_stats['successful_batches'] += 1
            # Update record count for successful batches
            self.batch_stats['total_records_processed'] += batch_metadata.get('record_count', 0)
        else:
            self.batch_stats['failed_batches'] += 1
        
        # Update timing information
        self.batch_stats['total_processing_time'] += processing_time
        
        # Store batch-specific statistics
        batch_stats = {
            'batch_number': batch_metadata.get('batch_number'),
            'success': success,
            'record_count': batch_metadata.get('record_count', 0) if success else 0,
            'processing_time': processing_time,
            'timestamp': time.time()
        }
        self.batch_stats['batch_history'].append(batch_stats)
        
        # Log update if debug is enabled
        logger.debug(
            f"Updated batch stats: total={self.batch_stats['total_batches_attempted']}, "
            f"success={self.batch_stats['successful_batches']}, "
            f"fail={self.batch_stats['failed_batches']}, "
            f"records={self.batch_stats['total_records_processed']}"
        )

    def _should_continue_processing(self, current_batch: int, records_processed: int, 
                                 estimated_total_records: Optional[int], start_time: float) -> bool:
        """
        Determine if batch processing should continue.

        Args:
            current_batch: Current batch number
            records_processed: Number of records processed so far
            estimated_total_records: Estimated total number of records (if known)
            start_time: Timestamp when processing started

        Returns:
            True if processing should continue, False otherwise
        """
        # Check if max_batches limit is reached
        if self.max_batches is not None and current_batch > self.max_batches:
            logger.info(f"Reached maximum batch limit ({self.max_batches}), stopping processing")
            return False
        
        # Check if we've processed all records (if total is known)
        if estimated_total_records is not None and records_processed >= estimated_total_records:
            logger.info(f"Processed all estimated records ({records_processed}/{estimated_total_records}), stopping processing")
            return False
        
        # Check if timeout has been reached
        elapsed_time = time.time() - start_time
        if elapsed_time > self.batch_timeout_seconds:
            logger.warning(
                f"Batch processing timeout reached ({elapsed_time:.2f}s > {self.batch_timeout_seconds}s), "
                f"stopping after processing {records_processed} records"
            )
            return False
        
        # Continue processing
        return True