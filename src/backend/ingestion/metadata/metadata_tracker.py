"""
Metadata tracking system for the self-healing data pipeline.

This module provides functionality to capture, store, and manage metadata about 
data sources, pipeline executions, tasks, and data quality metrics. It supports
tracking pipeline execution history, data lineage, and providing metadata context
for self-healing processes.

The metadata tracking system uses Firestore for operational metadata storage and
BigQuery for long-term analysis and historical metadata storage.
"""

import uuid
import datetime
import json
from typing import Dict, List, Any, Optional, Union

from ...constants import (
    DataSourceType,
    PIPELINE_STATUS_RUNNING,
    PIPELINE_STATUS_SUCCESS,
    PIPELINE_STATUS_FAILED,
    PIPELINE_STATUS_HEALING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCESS,
    TASK_STATUS_FAILED
)
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.firestore_client import FirestoreClient
from ...utils.storage.bigquery_client import BigQueryClient

# Configure logger for this module
logger = get_logger(__name__)

# Default collection name for Firestore
DEFAULT_METADATA_COLLECTION = "pipeline_metadata"

# Default table name for BigQuery
DEFAULT_METADATA_TABLE = "pipeline_metadata"


def create_metadata_record(record_type: str, metadata: dict) -> str:
    """
    Creates a new metadata record with a unique identifier.
    
    Args:
        record_type: Type of metadata record (e.g., 'source', 'pipeline', 'execution')
        metadata: Dictionary containing metadata fields and values
        
    Returns:
        The unique identifier for the created metadata record
    """
    # Generate a unique metadata_id
    metadata_id = str(uuid.uuid4())
    
    # Create metadata tracker instance
    metadata_tracker = MetadataTracker()
    
    # Create record with timestamp
    record = {
        'created_at': datetime.datetime.utcnow().isoformat(),
        'updated_at': datetime.datetime.utcnow().isoformat(),
        'record_type': record_type,
        **metadata
    }
    
    # Store the record
    metadata_tracker._store_metadata_record({
        'metadata_id': metadata_id,
        **record
    })
    
    return metadata_id


class MetadataTracker:
    """
    Tracks and manages metadata information throughout the pipeline.
    
    This class provides methods to track metadata for various pipeline components
    including data sources, pipeline definitions, pipeline executions, tasks,
    data quality validations, and self-healing actions.
    
    The metadata is stored in both Firestore (for operational use) and BigQuery 
    (for long-term analysis) depending on configuration.
    """
    
    def __init__(self):
        """Initialize the MetadataTracker with storage clients."""
        # Load configuration
        config = get_config()
        
        # Initialize Firestore client
        self._firestore_client = FirestoreClient()
        
        # Initialize BigQuery client if enabled
        self._enable_bigquery_storage = config.get("metadata.enable_bigquery_storage", False)
        if self._enable_bigquery_storage:
            self._bigquery_client = BigQueryClient()
        else:
            self._bigquery_client = None
            
        # Set collection and table names from configuration
        self._metadata_collection = config.get(
            "metadata.collection_name", 
            DEFAULT_METADATA_COLLECTION
        )
        self._metadata_table = config.get(
            "metadata.table_name", 
            DEFAULT_METADATA_TABLE
        )
        
        logger.debug(f"Initialized MetadataTracker with collection: {self._metadata_collection}")
    
    def track_source_system(
        self, source_id: str, source_name: str, source_type: DataSourceType, 
        connection_details: Dict[str, Any], schema_version: str
    ) -> str:
        """
        Records metadata for a data source system.
        
        Args:
            source_id: Unique identifier for the source system
            source_name: Descriptive name of the source system
            source_type: Type of source (GCS, Cloud SQL, API, etc.)
            connection_details: Dictionary containing connection parameters
            schema_version: Version of the source schema
            
        Returns:
            The metadata record ID
        """
        # Mask sensitive connection details
        masked_connection = self._mask_sensitive_details(connection_details)
        
        # Create source metadata record
        metadata = {
            'source_id': source_id,
            'source_name': source_name,
            'source_type': source_type.value,
            'connection_details': masked_connection,
            'schema_version': schema_version,
            'last_updated': datetime.datetime.utcnow().isoformat()
        }
        
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'source_system', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked source system metadata: {source_id}, type: {source_type.value}")
        return record_id
    
    def track_pipeline_definition(
        self, pipeline_id: str, pipeline_name: str, source_id: str, 
        target_dataset: str, target_table: str, dag_id: str, 
        pipeline_config: Dict[str, Any]
    ) -> str:
        """
        Records metadata for a pipeline definition.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            pipeline_name: Descriptive name of the pipeline
            source_id: Source system identifier
            target_dataset: Target dataset in BigQuery
            target_table: Target table in BigQuery
            dag_id: Airflow DAG identifier
            pipeline_config: Configuration parameters for the pipeline
            
        Returns:
            The metadata record ID
        """
        # Create pipeline definition metadata
        metadata = {
            'pipeline_id': pipeline_id,
            'pipeline_name': pipeline_name,
            'source_id': source_id,
            'target_dataset': target_dataset,
            'target_table': target_table,
            'dag_id': dag_id,
            'pipeline_config': pipeline_config,
            'created_at': datetime.datetime.utcnow().isoformat(),
            'last_updated': datetime.datetime.utcnow().isoformat()
        }
        
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'pipeline_definition', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked pipeline definition metadata: {pipeline_id}")
        return record_id
    
    def track_pipeline_execution(
        self, execution_id: str, pipeline_id: str, status: str, 
        execution_params: Dict[str, Any]
    ) -> str:
        """
        Records metadata for a pipeline execution.
        
        Args:
            execution_id: Unique identifier for the execution
            pipeline_id: Pipeline identifier
            status: Current status of the execution
            execution_params: Parameters used for this execution
            
        Returns:
            The metadata record ID
        """
        # Create execution metadata
        metadata = {
            'execution_id': execution_id,
            'pipeline_id': pipeline_id,
            'status': status,
            'execution_params': execution_params,
            'created_at': datetime.datetime.utcnow().isoformat()
        }
        
        # Set start time if status is RUNNING
        if status == PIPELINE_STATUS_RUNNING:
            metadata['start_time'] = datetime.datetime.utcnow().isoformat()
            
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'pipeline_execution', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked pipeline execution start: {execution_id}, status: {status}")
        return record_id
    
    def update_pipeline_execution(
        self, execution_id: str, status: str, 
        execution_metrics: Optional[Dict[str, Any]] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Updates metadata for an existing pipeline execution.
        
        Args:
            execution_id: Execution identifier
            status: Updated status of the execution
            execution_metrics: Performance metrics for the execution
            error_details: Details about any errors that occurred
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Query the existing record
            record = self.search_metadata({'execution_id': execution_id}, 'pipeline_execution')
            
            if not record or len(record) == 0:
                logger.error(f"Execution record not found for update: {execution_id}")
                return False
                
            # Get the record to update
            execution_record = record[0]
            
            # Update fields
            execution_record['status'] = status
            execution_record['updated_at'] = datetime.datetime.utcnow().isoformat()
            
            # Add completion time for terminal statuses
            if status in [PIPELINE_STATUS_SUCCESS, PIPELINE_STATUS_FAILED]:
                execution_record['end_time'] = datetime.datetime.utcnow().isoformat()
                
                # Calculate duration if start time exists
                if 'start_time' in execution_record:
                    start_time = datetime.datetime.fromisoformat(execution_record['start_time'])
                    end_time = datetime.datetime.fromisoformat(execution_record['end_time'])
                    duration = (end_time - start_time).total_seconds()
                    execution_record['duration_seconds'] = duration
            
            # Add metrics if provided
            if execution_metrics:
                execution_record['execution_metrics'] = execution_metrics
                
            # Add error details if failed
            if status == PIPELINE_STATUS_FAILED and error_details:
                execution_record['error_details'] = error_details
                
            # Store updated record
            self._store_metadata_record(execution_record)
            
            logger.info(f"Updated pipeline execution: {execution_id}, status: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating execution metadata: {str(e)}")
            return False
    
    def track_task_execution(
        self, execution_id: str, task_id: str, task_type: str, 
        status: str, task_params: Dict[str, Any]
    ) -> str:
        """
        Records metadata for a task execution within a pipeline.
        
        Args:
            execution_id: Pipeline execution identifier
            task_id: Unique identifier for the task
            task_type: Type of task (extract, transform, load, etc.)
            status: Current status of the task
            task_params: Parameters used for this task
            
        Returns:
            The metadata record ID
        """
        # Create task metadata
        metadata = {
            'execution_id': execution_id,
            'task_id': task_id,
            'task_type': task_type,
            'status': status,
            'task_params': task_params,
            'created_at': datetime.datetime.utcnow().isoformat()
        }
        
        # Set start time if status is RUNNING
        if status == TASK_STATUS_RUNNING:
            metadata['start_time'] = datetime.datetime.utcnow().isoformat()
            
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'task_execution', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked task execution: {task_id} for execution: {execution_id}")
        return record_id
    
    def update_task_execution(
        self, execution_id: str, task_id: str, status: str, 
        task_metrics: Optional[Dict[str, Any]] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Updates metadata for an existing task execution.
        
        Args:
            execution_id: Pipeline execution identifier
            task_id: Task identifier
            status: Updated status of the task
            task_metrics: Performance metrics for the task
            error_details: Details about any errors that occurred
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Query the existing record
            record = self.search_metadata(
                {'execution_id': execution_id, 'task_id': task_id}, 
                'task_execution'
            )
            
            if not record or len(record) == 0:
                logger.error(f"Task record not found for update: {task_id} in execution: {execution_id}")
                return False
                
            # Get the record to update
            task_record = record[0]
            
            # Update fields
            task_record['status'] = status
            task_record['updated_at'] = datetime.datetime.utcnow().isoformat()
            
            # Add completion time for terminal statuses
            if status in [TASK_STATUS_SUCCESS, TASK_STATUS_FAILED]:
                task_record['end_time'] = datetime.datetime.utcnow().isoformat()
                
                # Calculate duration if start time exists
                if 'start_time' in task_record:
                    start_time = datetime.datetime.fromisoformat(task_record['start_time'])
                    end_time = datetime.datetime.fromisoformat(task_record['end_time'])
                    duration = (end_time - start_time).total_seconds()
                    task_record['duration_seconds'] = duration
            
            # Add metrics if provided
            if task_metrics:
                task_record['task_metrics'] = task_metrics
                
            # Add error details if failed
            if status == TASK_STATUS_FAILED and error_details:
                task_record['error_details'] = error_details
                
            # Store updated record
            self._store_metadata_record(task_record)
            
            logger.info(f"Updated task execution: {task_id}, status: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating task metadata: {str(e)}")
            return False
    
    def track_schema_metadata(
        self, dataset: str, table: str, schema: Dict[str, Any], 
        schema_version: str, source_id: Optional[str] = None
    ) -> str:
        """
        Records metadata about a dataset schema.
        
        Args:
            dataset: Dataset name
            table: Table name
            schema: Schema definition
            schema_version: Version of the schema
            source_id: Optional source system identifier
            
        Returns:
            The metadata record ID
        """
        # Create schema metadata
        metadata = {
            'dataset': dataset,
            'table': table,
            'schema': schema,
            'schema_version': schema_version,
            'created_at': datetime.datetime.utcnow().isoformat()
        }
        
        # Add source reference if provided
        if source_id:
            metadata['source_id'] = source_id
            
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'schema_metadata', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked schema metadata for {dataset}.{table}, version: {schema_version}")
        return record_id
    
    def track_data_quality_metadata(
        self, execution_id: str, validation_id: str, dataset: str, 
        table: str, validation_results: Dict[str, Any], quality_score: float
    ) -> str:
        """
        Records metadata about data quality validation.
        
        Args:
            execution_id: Pipeline execution identifier
            validation_id: Unique identifier for the validation
            dataset: Dataset name
            table: Table name
            validation_results: Results of the validation
            quality_score: Overall quality score (0-1)
            
        Returns:
            The metadata record ID
        """
        # Create data quality metadata
        metadata = {
            'execution_id': execution_id,
            'validation_id': validation_id,
            'dataset': dataset,
            'table': table,
            'validation_results': validation_results,
            'quality_score': quality_score,
            'validation_time': datetime.datetime.utcnow().isoformat()
        }
            
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'data_quality', metadata)
        self._store_metadata_record(metadata_record)
        
        logger.info(f"Tracked data quality metadata for {dataset}.{table}, score: {quality_score}")
        return record_id
    
    def track_self_healing_metadata(
        self, execution_id: str, healing_id: str, issue_type: str, 
        action_type: str, action_details: Dict[str, Any], 
        confidence_score: float, success: bool
    ) -> str:
        """
        Records metadata about a self-healing action.
        
        Args:
            execution_id: Pipeline execution identifier
            healing_id: Unique identifier for the healing action
            issue_type: Type of issue being addressed
            action_type: Type of healing action taken
            action_details: Details about the healing action
            confidence_score: Confidence in the healing action (0-1)
            success: Whether the healing action was successful
            
        Returns:
            The metadata record ID
        """
        # Create self-healing metadata
        metadata = {
            'execution_id': execution_id,
            'healing_id': healing_id,
            'issue_type': issue_type,
            'action_type': action_type,
            'action_details': action_details,
            'confidence_score': confidence_score,
            'success': success,
            'healing_time': datetime.datetime.utcnow().isoformat()
        }
            
        # Create and store the record
        record_id = str(uuid.uuid4())
        metadata_record = self._create_metadata_record(record_id, 'self_healing', metadata)
        self._store_metadata_record(metadata_record)
        
        log_msg = f"Tracked self-healing metadata for {execution_id}, "
        log_msg += f"action: {action_type}, success: {success}"
        logger.info(log_msg)
        
        return record_id
    
    def get_metadata_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a specific metadata record by ID.
        
        Args:
            record_id: Unique identifier for the metadata record
            
        Returns:
            The metadata record or None if not found
        """
        try:
            # Query Firestore for the record
            record = self._firestore_client.get_document(
                self._metadata_collection,
                record_id
            )
            return record
        except Exception as e:
            logger.error(f"Error retrieving metadata record {record_id}: {str(e)}")
            return None
    
    def get_pipeline_metadata(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific pipeline.
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            Pipeline metadata including definition and recent executions
        """
        try:
            # Get pipeline definition
            pipeline_def = self.search_metadata(
                {'pipeline_id': pipeline_id}, 
                'pipeline_definition'
            )
            
            if not pipeline_def or len(pipeline_def) == 0:
                logger.warning(f"Pipeline definition not found: {pipeline_id}")
                return {}
                
            # Get recent executions
            executions = self.search_metadata(
                {'pipeline_id': pipeline_id}, 
                'pipeline_execution', 
                limit=10
            )
            
            # Compile complete metadata
            result = {
                'definition': pipeline_def[0],
                'recent_executions': executions
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving pipeline metadata: {str(e)}")
            return {}
    
    def get_execution_metadata(
        self, execution_id: str, include_tasks: bool = True,
        include_quality: bool = True, include_healing: bool = True
    ) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific pipeline execution.
        
        Args:
            execution_id: Execution identifier
            include_tasks: Whether to include task executions
            include_quality: Whether to include quality validations
            include_healing: Whether to include self-healing actions
            
        Returns:
            Execution metadata including tasks and related data if requested
        """
        try:
            # Get execution record
            execution = self.search_metadata(
                {'execution_id': execution_id}, 
                'pipeline_execution'
            )
            
            if not execution or len(execution) == 0:
                logger.warning(f"Execution record not found: {execution_id}")
                return {}
                
            result = {
                'execution': execution[0]
            }
            
            # Include tasks if requested
            if include_tasks:
                tasks = self.search_metadata(
                    {'execution_id': execution_id}, 
                    'task_execution'
                )
                result['tasks'] = tasks
                
            # Include quality data if requested
            if include_quality:
                quality = self.search_metadata(
                    {'execution_id': execution_id}, 
                    'data_quality'
                )
                result['quality_validations'] = quality
                
            # Include healing actions if requested
            if include_healing:
                healing = self.search_metadata(
                    {'execution_id': execution_id}, 
                    'self_healing'
                )
                result['healing_actions'] = healing
                
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving execution metadata: {str(e)}")
            return {}
    
    def search_metadata(
        self, search_criteria: Dict[str, Any], record_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Searches metadata records based on criteria.
        
        Args:
            search_criteria: Dictionary of field-value pairs to search for
            record_type: Optional type of records to search
            limit: Maximum number of records to return
            
        Returns:
            List of matching metadata records
        """
        try:
            query = search_criteria.copy()
            
            # Add record type to query if specified
            if record_type:
                query['record_type'] = record_type
                
            # Query Firestore
            results = self._firestore_client.query_documents(
                self._metadata_collection,
                query,
                limit=limit
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching metadata: {str(e)}")
            return []
    
    def export_metadata_to_bigquery(
        self, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> bool:
        """
        Exports metadata to BigQuery for long-term storage and analysis.
        
        Args:
            start_date: Start date for metadata to export
            end_date: End date for metadata to export
            
        Returns:
            True if export successful, False otherwise
        """
        if not self._enable_bigquery_storage or not self._bigquery_client:
            logger.warning("BigQuery storage not enabled for metadata export")
            return False
            
        try:
            # Convert dates to ISO format for Firestore query
            start_str = start_date.isoformat()
            end_str = end_date.isoformat()
            
            # Query metadata records created within date range
            records = self._firestore_client.query_documents(
                self._metadata_collection,
                {
                    'created_at': {'$gte': start_str, '$lte': end_str}
                }
            )
            
            if not records:
                logger.info(f"No metadata records found between {start_str} and {end_str}")
                return True
                
            # Prepare records for BigQuery insertion
            bq_records = []
            for record in records:
                # Convert to JSON string for nested fields
                for field in ['execution_params', 'pipeline_config', 'schema', 
                             'validation_results', 'action_details', 'error_details',
                             'task_params', 'task_metrics', 'execution_metrics', 
                             'connection_details']:
                    if field in record and isinstance(record[field], dict):
                        record[field] = json.dumps(record[field])
                
                bq_records.append(record)
            
            # Insert into BigQuery
            dataset = self._bigquery_client.get_default_dataset()
            result = self._bigquery_client.insert_records(
                dataset,
                self._metadata_table,
                bq_records
            )
            
            logger.info(f"Exported {len(bq_records)} metadata records to BigQuery")
            return result
            
        except Exception as e:
            logger.error(f"Error exporting metadata to BigQuery: {str(e)}")
            return False
    
    def _store_metadata_record(self, metadata_record: Dict[str, Any]) -> bool:
        """
        Internal method to store a metadata record in the appropriate storage.
        
        Args:
            metadata_record: The metadata record to store
            
        Returns:
            True if storage successful, False otherwise
        """
        try:
            # Store in Firestore
            result = self._firestore_client.set_document(
                self._metadata_collection,
                metadata_record['metadata_id'],
                metadata_record
            )
            
            # Store in BigQuery if enabled and configured for real-time storage
            if (self._enable_bigquery_storage and self._bigquery_client and 
                get_config().get("metadata.realtime_bigquery_storage", False)):
                
                record_copy = metadata_record.copy()
                
                # Convert complex fields to JSON strings for BigQuery
                for field in ['execution_params', 'pipeline_config', 'schema', 
                            'validation_results', 'action_details', 'error_details',
                            'task_params', 'task_metrics', 'execution_metrics', 
                            'connection_details']:
                    if field in record_copy and isinstance(record_copy[field], dict):
                        record_copy[field] = json.dumps(record_copy[field])
                
                # Insert into BigQuery
                dataset = self._bigquery_client.get_default_dataset()
                self._bigquery_client.insert_records(
                    dataset,
                    self._metadata_table,
                    [record_copy]
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error storing metadata record: {str(e)}")
            return False
    
    def _create_metadata_record(
        self, record_id: str, record_type: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Internal method to create a standardized metadata record.
        
        Args:
            record_id: Unique identifier for the record
            record_type: Type of metadata record
            metadata: Dictionary containing metadata fields and values
            
        Returns:
            Formatted metadata record
        """
        # Create base record with ID and type
        record = {
            'metadata_id': record_id,
            'record_type': record_type,
            'created_at': datetime.datetime.utcnow().isoformat(),
            'updated_at': datetime.datetime.utcnow().isoformat(),
        }
        
        # Get environment info
        config = get_config()
        environment = config.get_environment()
        
        # Add environment info
        record['environment'] = environment
        
        # Add provided metadata
        record.update(metadata)
        
        # Add system info
        record['system_info'] = {
            'app_version': config.get("app.version", "1.0.0"),
            'hostname': config.get("app.hostname", "unknown")
        }
        
        return record
    
    def _mask_sensitive_details(self, connection_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Masks sensitive information in connection details.
        
        Args:
            connection_details: Connection details that may contain sensitive info
            
        Returns:
            Masked connection details
        """
        # Create a copy to avoid modifying the original
        masked = connection_details.copy()
        
        # Mask sensitive fields
        sensitive_fields = ['password', 'secret', 'key', 'token', 'credential']
        
        for field in masked:
            for sensitive in sensitive_fields:
                if sensitive in field.lower() and isinstance(masked[field], str):
                    # Preserve first and last character, mask the rest
                    value = masked[field]
                    if len(value) > 2:
                        masked[field] = value[0] + '*' * (len(value) - 2) + value[-1]
                    else:
                        masked[field] = '****'  # For very short values
        
        return masked


class MetadataQuery:
    """
    Utility class for querying and analyzing pipeline metadata.
    
    This class provides methods for extracting insights from metadata,
    including pipeline statistics, quality trends, and failure patterns.
    It uses both Firestore for operational queries and BigQuery for
    more complex analytical queries on historical data.
    """
    
    def __init__(self, metadata_tracker: MetadataTracker):
        """
        Initialize the MetadataQuery with a MetadataTracker instance.
        
        Args:
            metadata_tracker: MetadataTracker instance for basic queries
        """
        self._metadata_tracker = metadata_tracker
        
        # Initialize BigQuery client for complex queries
        config = get_config()
        if config.get("metadata.enable_bigquery_storage", False):
            self._bigquery_client = BigQueryClient()
        else:
            self._bigquery_client = None
    
    def get_pipeline_statistics(
        self, pipeline_id: str, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime
    ) -> Dict[str, Any]:
        """
        Calculates statistics for pipeline executions.
        
        Args:
            pipeline_id: Pipeline identifier
            start_date: Start date for the analysis period
            end_date: End date for the analysis period
            
        Returns:
            Pipeline statistics including success rate, average duration, etc.
        """
        try:
            # Convert dates to ISO format for query
            start_str = start_date.isoformat()
            end_str = end_date.isoformat()
            
            # Query executions within date range
            executions = self._metadata_tracker.search_metadata(
                {
                    'pipeline_id': pipeline_id,
                    'created_at': {'$gte': start_str, '$lte': end_str}
                }, 
                'pipeline_execution'
            )
            
            if not executions:
                logger.warning(f"No executions found for pipeline {pipeline_id} in specified period")
                return {
                    'pipeline_id': pipeline_id,
                    'period_start': start_str,
                    'period_end': end_str,
                    'execution_count': 0
                }
            
            # Initialize counters and aggregators
            total_count = len(executions)
            success_count = 0
            failed_count = 0
            healing_count = 0
            durations = []
            
            # Process execution records
            for execution in executions:
                status = execution.get('status')
                
                if status == PIPELINE_STATUS_SUCCESS:
                    success_count += 1
                elif status == PIPELINE_STATUS_FAILED:
                    failed_count += 1
                elif status == PIPELINE_STATUS_HEALING:
                    healing_count += 1
                
                # Track duration if available
                if 'duration_seconds' in execution:
                    durations.append(execution['duration_seconds'])
            
            # Calculate statistics
            stats = {
                'pipeline_id': pipeline_id,
                'period_start': start_str,
                'period_end': end_str,
                'execution_count': total_count,
                'success_count': success_count,
                'failed_count': failed_count,
                'healing_count': healing_count,
                'success_rate': success_count / total_count if total_count > 0 else 0,
                'failure_rate': failed_count / total_count if total_count > 0 else 0,
                'healing_rate': healing_count / total_count if total_count > 0 else 0
            }
            
            # Add duration statistics if available
            if durations:
                stats['avg_duration_seconds'] = sum(durations) / len(durations)
                stats['min_duration_seconds'] = min(durations)
                stats['max_duration_seconds'] = max(durations)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating pipeline statistics: {str(e)}")
            return {
                'pipeline_id': pipeline_id,
                'period_start': start_date.isoformat(),
                'period_end': end_date.isoformat(),
                'error': str(e)
            }
    
    def get_quality_trends(
        self, dataset: str, table: str, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime
    ) -> Dict[str, Any]:
        """
        Analyzes trends in data quality over time.
        
        Args:
            dataset: Dataset name
            table: Table name
            start_date: Start date for the analysis period
            end_date: End date for the analysis period
            
        Returns:
            Quality trend analysis including scores over time and issue patterns
        """
        try:
            # Convert dates to ISO format for query
            start_str = start_date.isoformat()
            end_str = end_date.isoformat()
            
            # Query quality validations within date range
            validations = self._metadata_tracker.search_metadata(
                {
                    'dataset': dataset,
                    'table': table,
                    'validation_time': {'$gte': start_str, '$lte': end_str}
                }, 
                'data_quality'
            )
            
            if not validations:
                logger.warning(f"No quality validations found for {dataset}.{table} in specified period")
                return {
                    'dataset': dataset,
                    'table': table,
                    'period_start': start_str,
                    'period_end': end_str,
                    'validation_count': 0
                }
            
            # Sort validations by time
            validations.sort(key=lambda x: x.get('validation_time', ''))
            
            # Extract quality scores over time
            quality_scores = []
            common_issues = {}
            
            for validation in validations:
                # Get quality score
                score = validation.get('quality_score', 0)
                time = validation.get('validation_time', '')
                quality_scores.append({
                    'time': time,
                    'score': score
                })
                
                # Analyze validation results for issues
                results = validation.get('validation_results', {})
                if isinstance(results, str):
                    # Parse JSON string if stored that way
                    results = json.loads(results)
                
                for rule_name, rule_result in results.items():
                    if isinstance(rule_result, dict) and not rule_result.get('success', True):
                        # Count failed validations by rule
                        if rule_name not in common_issues:
                            common_issues[rule_name] = 0
                        common_issues[rule_name] += 1
            
            # Sort issues by frequency
            sorted_issues = sorted(
                common_issues.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            # Calculate overall statistics
            avg_score = sum(item['score'] for item in quality_scores) / len(quality_scores)
            min_score = min(item['score'] for item in quality_scores)
            max_score = max(item['score'] for item in quality_scores)
            
            # Compile trend analysis
            trends = {
                'dataset': dataset,
                'table': table,
                'period_start': start_str,
                'period_end': end_str,
                'validation_count': len(validations),
                'average_score': avg_score,
                'min_score': min_score,
                'max_score': max_score,
                'quality_scores': quality_scores,
                'common_issues': [
                    {'rule': rule, 'count': count} 
                    for rule, count in sorted_issues[:10]  # Top 10 issues
                ]
            }
            
            # Calculate trend direction (improving, declining, stable)
            if len(quality_scores) >= 2:
                first_scores = quality_scores[:len(quality_scores)//2]
                second_scores = quality_scores[len(quality_scores)//2:]
                avg_first = sum(item['score'] for item in first_scores) / len(first_scores)
                avg_second = sum(item['score'] for item in second_scores) / len(second_scores)
                
                if avg_second - avg_first > 0.05:
                    trends['trend_direction'] = 'improving'
                elif avg_first - avg_second > 0.05:
                    trends['trend_direction'] = 'declining'
                else:
                    trends['trend_direction'] = 'stable'
            
            return trends
            
        except Exception as e:
            logger.error(f"Error analyzing quality trends: {str(e)}")
            return {
                'dataset': dataset,
                'table': table,
                'period_start': start_date.isoformat(),
                'period_end': end_date.isoformat(),
                'error': str(e)
            }
    
    def get_self_healing_effectiveness(
        self, pipeline_id: str, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime
    ) -> Dict[str, Any]:
        """
        Analyzes the effectiveness of self-healing actions.
        
        Args:
            pipeline_id: Pipeline identifier
            start_date: Start date for the analysis period
            end_date: End date for the analysis period
            
        Returns:
            Self-healing effectiveness metrics including success rate by issue type
        """
        try:
            # First get all executions for this pipeline in the period
            start_str = start_date.isoformat()
            end_str = end_date.isoformat()
            
            executions = self._metadata_tracker.search_metadata(
                {
                    'pipeline_id': pipeline_id,
                    'created_at': {'$gte': start_str, '$lte': end_str}
                }, 
                'pipeline_execution'
            )
            
            if not executions:
                logger.warning(f"No executions found for pipeline {pipeline_id} in specified period")
                return {
                    'pipeline_id': pipeline_id,
                    'period_start': start_str,
                    'period_end': end_str,
                    'healing_actions_count': 0
                }
            
            # Extract execution IDs
            execution_ids = [exec.get('execution_id') for exec in executions if 'execution_id' in exec]
            
            # Query all healing actions for these executions
            all_healing_actions = []
            for exec_id in execution_ids:
                actions = self._metadata_tracker.search_metadata(
                    {'execution_id': exec_id}, 
                    'self_healing'
                )
                all_healing_actions.extend(actions)
            
            if not all_healing_actions:
                logger.info(f"No healing actions found for pipeline {pipeline_id} in specified period")
                return {
                    'pipeline_id': pipeline_id,
                    'period_start': start_str,
                    'period_end': end_str,
                    'execution_count': len(executions),
                    'healing_actions_count': 0
                }
            
            # Analyze healing actions
            total_actions = len(all_healing_actions)
            successful_actions = sum(1 for action in all_healing_actions if action.get('success', False))
            
            # Group by issue type
            issue_types = {}
            for action in all_healing_actions:
                issue_type = action.get('issue_type', 'unknown')
                if issue_type not in issue_types:
                    issue_types[issue_type] = {
                        'total': 0,
                        'successful': 0
                    }
                
                issue_types[issue_type]['total'] += 1
                if action.get('success', False):
                    issue_types[issue_type]['successful'] += 1
            
            # Calculate success rates by issue type
            issue_success_rates = {}
            for issue_type, counts in issue_types.items():
                success_rate = counts['successful'] / counts['total'] if counts['total'] > 0 else 0
                issue_success_rates[issue_type] = {
                    'total_actions': counts['total'],
                    'successful_actions': counts['successful'],
                    'success_rate': success_rate
                }
            
            # Group by action type
            action_types = {}
            for action in all_healing_actions:
                action_type = action.get('action_type', 'unknown')
                if action_type not in action_types:
                    action_types[action_type] = {
                        'total': 0,
                        'successful': 0
                    }
                
                action_types[action_type]['total'] += 1
                if action.get('success', False):
                    action_types[action_type]['successful'] += 1
            
            # Calculate success rates by action type
            action_success_rates = {}
            for action_type, counts in action_types.items():
                success_rate = counts['successful'] / counts['total'] if counts['total'] > 0 else 0
                action_success_rates[action_type] = {
                    'total_actions': counts['total'],
                    'successful_actions': counts['successful'],
                    'success_rate': success_rate
                }
            
            # Analyze confidence score correlation with success
            confidence_bins = {
                'low': {'min': 0.0, 'max': 0.5, 'total': 0, 'successful': 0},
                'medium': {'min': 0.5, 'max': 0.8, 'total': 0, 'successful': 0},
                'high': {'min': 0.8, 'max': 1.0, 'total': 0, 'successful': 0}
            }
            
            for action in all_healing_actions:
                confidence = action.get('confidence_score', 0)
                success = action.get('success', False)
                
                for bin_name, bin_range in confidence_bins.items():
                    if bin_range['min'] <= confidence < bin_range['max']:
                        confidence_bins[bin_name]['total'] += 1
                        if success:
                            confidence_bins[bin_name]['successful'] += 1
                        break
            
            # Calculate success rates by confidence bin
            for bin_name, bin_data in confidence_bins.items():
                if bin_data['total'] > 0:
                    bin_data['success_rate'] = bin_data['successful'] / bin_data['total']
                else:
                    bin_data['success_rate'] = 0
            
            # Compile effectiveness analysis
            effectiveness = {
                'pipeline_id': pipeline_id,
                'period_start': start_str,
                'period_end': end_str,
                'execution_count': len(executions),
                'healing_actions_count': total_actions,
                'overall_success_rate': successful_actions / total_actions if total_actions > 0 else 0,
                'by_issue_type': issue_success_rates,
                'by_action_type': action_success_rates,
                'by_confidence': confidence_bins
            }
            
            return effectiveness
            
        except Exception as e:
            logger.error(f"Error analyzing healing effectiveness: {str(e)}")
            return {
                'pipeline_id': pipeline_id,
                'period_start': start_date.isoformat(),
                'period_end': end_date.isoformat(),
                'error': str(e)
            }
    
    def find_related_executions(
        self, execution_id: str, relation_type: str
    ) -> List[Dict[str, Any]]:
        """
        Finds pipeline executions related to a specific execution.
        
        Args:
            execution_id: Execution identifier
            relation_type: Type of relation to look for ('similar', 'same_day', 'same_source', etc.)
            
        Returns:
            List of related execution records
        """
        try:
            # First get the execution details
            execution = self._metadata_tracker.get_execution_metadata(
                execution_id,
                include_tasks=False,
                include_quality=False,
                include_healing=False
            )
            
            if not execution or 'execution' not in execution:
                logger.warning(f"Execution not found: {execution_id}")
                return []
            
            execution_data = execution['execution']
            pipeline_id = execution_data.get('pipeline_id')
            
            if not pipeline_id:
                logger.warning(f"Execution {execution_id} has no pipeline_id")
                return []
            
            # Build query based on relation type
            if relation_type == 'same_day':
                # Get execution date (YYYY-MM-DD part of ISO string)
                if 'created_at' in execution_data:
                    execution_date = execution_data['created_at'][:10]
                    
                    # Find executions from same day
                    return self._metadata_tracker.search_metadata(
                        {
                            'pipeline_id': pipeline_id,
                            'created_at': {'$regex': f"^{execution_date}"}  # Starts with date
                        }, 
                        'pipeline_execution'
                    )
            
            elif relation_type == 'same_pipeline':
                # Find other executions of the same pipeline
                return self._metadata_tracker.search_metadata(
                    {'pipeline_id': pipeline_id}, 
                    'pipeline_execution',
                    limit=20
                )
            
            elif relation_type == 'similar_errors':
                # First check if this execution has errors
                if execution_data.get('status') != PIPELINE_STATUS_FAILED:
                    logger.info(f"Execution {execution_id} is not failed, no similar errors to find")
                    return []
                
                # Get error details
                error_details = execution_data.get('error_details', {})
                if not error_details:
                    logger.info(f"Execution {execution_id} has no error details")
                    return []
                
                # For string-based search, look for error message patterns
                if isinstance(error_details, str):
                    error_details = json.loads(error_details)
                
                error_message = error_details.get('message', '')
                if not error_message:
                    logger.info(f"Execution {execution_id} has no error message")
                    return []
                
                # Find executions with similar error messages
                # This is a simplified approach - in a real system, you might use
                # more sophisticated text matching or classification
                return self._metadata_tracker.search_metadata(
                    {
                        'status': PIPELINE_STATUS_FAILED,
                        'error_details.message': {'$regex': error_message[:50]}  # Use first part of message
                    }, 
                    'pipeline_execution',
                    limit=10
                )
            
            # Default case: just return a few recent executions of the same pipeline
            return self._metadata_tracker.search_metadata(
                {'pipeline_id': pipeline_id}, 
                'pipeline_execution',
                limit=5
            )
            
        except Exception as e:
            logger.error(f"Error finding related executions: {str(e)}")
            return []
    
    def analyze_failure_patterns(
        self, pipeline_id: str, 
        start_date: datetime.datetime, 
        end_date: datetime.datetime
    ) -> Dict[str, Any]:
        """
        Analyzes patterns in pipeline failures.
        
        Args:
            pipeline_id: Pipeline identifier
            start_date: Start date for the analysis period
            end_date: End date for the analysis period
            
        Returns:
            Failure pattern analysis including common causes and correlations
        """
        try:
            # Convert dates to ISO format for query
            start_str = start_date.isoformat()
            end_str = end_date.isoformat()
            
            # Query failed executions within date range
            failed_executions = self._metadata_tracker.search_metadata(
                {
                    'pipeline_id': pipeline_id,
                    'status': PIPELINE_STATUS_FAILED,
                    'created_at': {'$gte': start_str, '$lte': end_str}
                }, 
                'pipeline_execution'
            )
            
            if not failed_executions:
                logger.info(f"No failed executions found for pipeline {pipeline_id} in specified period")
                return {
                    'pipeline_id': pipeline_id,
                    'period_start': start_str,
                    'period_end': end_str,
                    'failed_execution_count': 0
                }
            
            # Count all executions in period for failure rate
            all_executions = self._metadata_tracker.search_metadata(
                {
                    'pipeline_id': pipeline_id,
                    'created_at': {'$gte': start_str, '$lte': end_str}
                }, 
                'pipeline_execution'
            )
            
            # Get execution IDs for task analysis
            execution_ids = [exec.get('execution_id') for exec in failed_executions if 'execution_id' in exec]
            
            # Analyze error patterns
            error_categories = {}
            common_tasks = {}
            
            for execution in failed_executions:
                # Categorize by error details
                error_details = execution.get('error_details', {})
                if isinstance(error_details, str):
                    try:
                        error_details = json.loads(error_details)
                    except:
                        error_details = {'message': error_details}
                
                error_type = error_details.get('type', 'unknown')
                if error_type not in error_categories:
                    error_categories[error_type] = 0
                error_categories[error_type] += 1
            
            # Analyze failed tasks
            for exec_id in execution_ids:
                tasks = self._metadata_tracker.search_metadata(
                    {
                        'execution_id': exec_id,
                        'status': TASK_STATUS_FAILED
                    }, 
                    'task_execution'
                )
                
                for task in tasks:
                    task_type = task.get('task_type', 'unknown')
                    task_id = task.get('task_id', 'unknown')
                    
                    # Track by task type
                    if task_type not in common_tasks:
                        common_tasks[task_type] = {
                            'count': 0,
                            'task_ids': set()
                        }
                    
                    common_tasks[task_type]['count'] += 1
                    common_tasks[task_type]['task_ids'].add(task_id)
            
            # Sort error categories by frequency
            sorted_errors = sorted(
                error_categories.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            # Convert task IDs to list for JSON serialization
            for task_type in common_tasks:
                common_tasks[task_type]['task_ids'] = list(common_tasks[task_type]['task_ids'])
            
            # Sort tasks by failure frequency
            sorted_tasks = sorted(
                common_tasks.items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )
            
            # Look for temporal patterns
            time_patterns = self._analyze_time_patterns(failed_executions)
            
            # Compile failure analysis
            analysis = {
                'pipeline_id': pipeline_id,
                'period_start': start_str,
                'period_end': end_str,
                'execution_count': len(all_executions),
                'failed_execution_count': len(failed_executions),
                'failure_rate': len(failed_executions) / len(all_executions) if all_executions else 0,
                'common_error_types': [
                    {'type': error_type, 'count': count}
                    for error_type, count in sorted_errors
                ],
                'failed_task_types': [
                    {
                        'task_type': task_type,
                        'count': data['count'],
                        'task_ids': data['task_ids'][:5]  # Limit to first 5 for brevity
                    }
                    for task_type, data in sorted_tasks
                ],
                'temporal_patterns': time_patterns
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing failure patterns: {str(e)}")
            return {
                'pipeline_id': pipeline_id,
                'period_start': start_date.isoformat(),
                'period_end': end_date.isoformat(),
                'error': str(e)
            }
    
    def export_metadata_report(
        self, report_type: str, report_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generates a comprehensive metadata report for analysis.
        
        Args:
            report_type: Type of report to generate
            report_params: Parameters for the report
            
        Returns:
            Metadata report based on specified type and parameters
        """
        try:
            # Process different report types
            if report_type == 'pipeline_health':
                # Report on pipeline health metrics
                pipeline_id = report_params.get('pipeline_id')
                if not pipeline_id:
                    return {'error': 'Missing pipeline_id parameter'}
                
                # Get date range from params or default to last 30 days
                end_date = datetime.datetime.utcnow()
                start_date = end_date - datetime.timedelta(days=report_params.get('days', 30))
                
                # Get pipeline definition
                pipeline_def = self._metadata_tracker.get_pipeline_metadata(pipeline_id)
                
                # Get pipeline statistics
                pipeline_stats = self.get_pipeline_statistics(
                    pipeline_id, start_date, end_date
                )
                
                # Get self-healing effectiveness
                healing_stats = self.get_self_healing_effectiveness(
                    pipeline_id, start_date, end_date
                )
                
                # Get failure patterns
                failure_patterns = self.analyze_failure_patterns(
                    pipeline_id, start_date, end_date
                )
                
                # Compile report
                return {
                    'report_type': 'pipeline_health',
                    'generated_at': datetime.datetime.utcnow().isoformat(),
                    'pipeline_id': pipeline_id,
                    'pipeline_definition': pipeline_def.get('definition', {}),
                    'period_start': start_date.isoformat(),
                    'period_end': end_date.isoformat(),
                    'execution_statistics': pipeline_stats,
                    'healing_effectiveness': healing_stats,
                    'failure_patterns': failure_patterns
                }
                
            elif report_type == 'data_quality':
                # Report on data quality trends
                dataset = report_params.get('dataset')
                table = report_params.get('table')
                
                if not dataset or not table:
                    return {'error': 'Missing dataset or table parameter'}
                
                # Get date range from params or default to last 30 days
                end_date = datetime.datetime.utcnow()
                start_date = end_date - datetime.timedelta(days=report_params.get('days', 30))
                
                # Get quality trends
                quality_trends = self.get_quality_trends(
                    dataset, table, start_date, end_date
                )
                
                # Get schema metadata
                schema_records = self._metadata_tracker.search_metadata(
                    {'dataset': dataset, 'table': table},
                    'schema_metadata',
                    limit=1
                )
                
                # Compile report
                return {
                    'report_type': 'data_quality',
                    'generated_at': datetime.datetime.utcnow().isoformat(),
                    'dataset': dataset,
                    'table': table,
                    'period_start': start_date.isoformat(),
                    'period_end': end_date.isoformat(),
                    'quality_trends': quality_trends,
                    'current_schema': schema_records[0] if schema_records else None
                }
                
            elif report_type == 'self_healing':
                # Report on self-healing effectiveness across pipelines
                # Get date range from params or default to last 30 days
                end_date = datetime.datetime.utcnow()
                start_date = end_date - datetime.timedelta(days=report_params.get('days', 30))
                
                # Get all pipelines with self-healing actions
                healing_actions = self._metadata_tracker.search_metadata(
                    {
                        'healing_time': {
                            '$gte': start_date.isoformat(),
                            '$lte': end_date.isoformat()
                        }
                    },
                    'self_healing'
                )
                
                if not healing_actions:
                    return {
                        'report_type': 'self_healing',
                        'generated_at': datetime.datetime.utcnow().isoformat(),
                        'period_start': start_date.isoformat(),
                        'period_end': end_date.isoformat(),
                        'healing_actions_count': 0
                    }
                
                # Get execution IDs from healing actions
                execution_ids = set(action.get('execution_id') for action in healing_actions 
                                if 'execution_id' in action)
                
                # Get pipeline IDs from execution IDs
                pipeline_ids = set()
                for exec_id in execution_ids:
                    execution = self._metadata_tracker.search_metadata(
                        {'execution_id': exec_id},
                        'pipeline_execution',
                        limit=1
                    )
                    if execution and 'pipeline_id' in execution[0]:
                        pipeline_ids.add(execution[0]['pipeline_id'])
                
                # Get healing effectiveness for each pipeline
                pipeline_healing_stats = {}
                for pipeline_id in pipeline_ids:
                    stats = self.get_self_healing_effectiveness(
                        pipeline_id, start_date, end_date
                    )
                    pipeline_healing_stats[pipeline_id] = stats
                
                # Calculate overall effectiveness
                total_actions = sum(stats.get('healing_actions_count', 0) 
                                for stats in pipeline_healing_stats.values())
                
                successful_actions = sum(
                    stats.get('healing_actions_count', 0) * stats.get('overall_success_rate', 0)
                    for stats in pipeline_healing_stats.values()
                )
                
                overall_success_rate = successful_actions / total_actions if total_actions > 0 else 0
                
                # Compile report
                return {
                    'report_type': 'self_healing',
                    'generated_at': datetime.datetime.utcnow().isoformat(),
                    'period_start': start_date.isoformat(),
                    'period_end': end_date.isoformat(),
                    'healing_actions_count': total_actions,
                    'overall_success_rate': overall_success_rate,
                    'pipeline_count': len(pipeline_ids),
                    'pipeline_details': pipeline_healing_stats
                }
                
            else:
                return {'error': f'Unknown report type: {report_type}'}
                
        except Exception as e:
            logger.error(f"Error generating metadata report: {str(e)}")
            return {
                'report_type': report_type,
                'generated_at': datetime.datetime.utcnow().isoformat(),
                'error': str(e)
            }
    
    def _analyze_time_patterns(self, executions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyzes temporal patterns in a set of executions.
        
        Args:
            executions: List of execution records to analyze
            
        Returns:
            Analysis of temporal patterns in the executions
        """
        # Initialize counters
        day_of_week = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}  # Monday = 0, Sunday = 6
        hour_of_day = {h: 0 for h in range(24)}
        
        for execution in executions:
            if 'created_at' in execution:
                try:
                    # Parse timestamp
                    timestamp = datetime.datetime.fromisoformat(execution['created_at'])
                    
                    # Increment day of week counter
                    day_of_week[timestamp.weekday()] += 1
                    
                    # Increment hour of day counter
                    hour_of_day[timestamp.hour] += 1
                except (ValueError, TypeError):
                    # Skip if timestamp can't be parsed
                    continue
        
        # Find peak times
        max_day = max(day_of_week.items(), key=lambda x: x[1]) if day_of_week else (0, 0)
        max_hour = max(hour_of_day.items(), key=lambda x: x[1]) if hour_of_day else (0, 0)
        
        # Convert day of week to name
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Format results
        return {
            'by_day_of_week': [
                {'day': day_names[day], 'count': count}
                for day, count in day_of_week.items()
            ],
            'by_hour_of_day': [
                {'hour': hour, 'count': count}
                for hour, count in hour_of_day.items()
            ],
            'peak_day': day_names[max_day[0]] if max_day[1] > 0 else None,
            'peak_hour': max_hour[0] if max_hour[1] > 0 else None
        }