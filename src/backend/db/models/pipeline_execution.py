import datetime
import uuid
import typing
import json
import enum

from ...constants import PipelineStatus
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Initialize logger
logger = get_logger(__name__)

# Define BigQuery table name
PIPELINE_EXECUTION_TABLE_NAME = "pipeline_executions"


def generate_execution_id() -> str:
    """
    Generates a unique identifier for a pipeline execution.
    
    Returns:
        str: Unique execution ID with 'exec_' prefix
    """
    return f"exec_{str(uuid.uuid4())}"


def get_pipeline_execution_table_schema() -> list:
    """
    Returns the BigQuery table schema for the pipeline executions table.
    
    Returns:
        list: List of SchemaField objects defining the table schema
    """
    return [
        get_schema_field("execution_id", "STRING", "REQUIRED", "Unique identifier for the pipeline execution"),
        get_schema_field("pipeline_id", "STRING", "REQUIRED", "Identifier of the pipeline definition"),
        get_schema_field("dag_run_id", "STRING", "NULLABLE", "Airflow DAG run identifier"),
        get_schema_field("status", "STRING", "REQUIRED", "Current status of the pipeline execution"),
        get_schema_field("start_time", "TIMESTAMP", "REQUIRED", "Time when the execution started"),
        get_schema_field("end_time", "TIMESTAMP", "NULLABLE", "Time when the execution completed"),
        get_schema_field("execution_params", "STRING", "NULLABLE", "JSON string of execution parameters"),
        get_schema_field("execution_metrics", "STRING", "NULLABLE", "JSON string of execution metrics"),
        get_schema_field("error_details", "STRING", "NULLABLE", "JSON string of error details if execution failed"),
        get_schema_field("retry_count", "INTEGER", "REQUIRED", "Number of retry attempts"),
        get_schema_field("retry_history", "STRING", "NULLABLE", "JSON string of retry history"),
        get_schema_field("self_healing_attempts", "STRING", "NULLABLE", "JSON string of self-healing attempts"),
        get_schema_field("metadata", "STRING", "NULLABLE", "JSON string of additional metadata"),
        get_schema_field("records_processed", "INTEGER", "REQUIRED", "Number of records processed"),
        get_schema_field("records_failed", "INTEGER", "REQUIRED", "Number of records that failed processing"),
        get_schema_field("quality_score", "FLOAT", "NULLABLE", "Data quality score for the execution")
    ]


class PipelineExecution:
    """
    Model class representing an execution instance of a data pipeline.
    
    This class tracks execution status, metrics, and metadata for pipeline runs,
    enabling monitoring, self-healing, and performance analysis.
    """
    
    def __init__(
        self,
        pipeline_id: str,
        execution_id: str = None,
        dag_run_id: str = None,
        execution_params: dict = None,
        status: PipelineStatus = PipelineStatus.PENDING
    ):
        """
        Initialize a new pipeline execution with provided parameters.
        
        Args:
            pipeline_id: Identifier of the pipeline definition
            execution_id: Unique identifier for this execution (generated if not provided)
            dag_run_id: Airflow DAG run identifier
            execution_params: Parameters for this execution
            status: Initial status of the execution
        """
        self.execution_id = execution_id or generate_execution_id()
        self.pipeline_id = pipeline_id
        self.dag_run_id = dag_run_id
        self.status = status
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.execution_params = execution_params or {}
        self.execution_metrics = {}
        self.error_details = {}
        self.retry_count = 0
        self.retry_history = {}
        self.self_healing_attempts = []
        self.metadata = {}
        self.records_processed = 0
        self.records_failed = 0
        self.quality_score = None
        
        logger.info(f"Created new pipeline execution {self.execution_id} for pipeline {self.pipeline_id}")
    
    def to_dict(self) -> dict:
        """
        Convert the pipeline execution to a dictionary representation.
        
        Returns:
            dict: Dictionary representation of the pipeline execution
        """
        result = {
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "dag_run_id": self.dag_run_id,
            "status": self.status.value if isinstance(self.status, enum.Enum) else self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_params": self.execution_params,
            "execution_metrics": self.execution_metrics,
            "error_details": self.error_details,
            "retry_count": self.retry_count,
            "retry_history": self.retry_history,
            "self_healing_attempts": self.self_healing_attempts,
            "metadata": self.metadata,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "quality_score": self.quality_score
        }
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PipelineExecution':
        """
        Create a PipelineExecution instance from a dictionary.
        
        Args:
            data: Dictionary containing pipeline execution data
            
        Returns:
            PipelineExecution: New PipelineExecution instance
        """
        # Extract required parameters
        pipeline_id = data.get('pipeline_id')
        execution_id = data.get('execution_id')
        dag_run_id = data.get('dag_run_id')
        execution_params = data.get('execution_params', {})
        
        # Convert status string to enum if needed
        status_value = data.get('status')
        status = None
        if status_value:
            try:
                status = PipelineStatus(status_value)
            except ValueError:
                # If string doesn't match enum, keep the string value
                status = status_value
        
        # Create instance with required parameters
        instance = cls(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            dag_run_id=dag_run_id,
            execution_params=execution_params,
            status=status
        )
        
        # Set additional properties
        if 'start_time' in data and data['start_time']:
            if isinstance(data['start_time'], str):
                instance.start_time = datetime.datetime.fromisoformat(data['start_time'])
            else:
                instance.start_time = data['start_time']
                
        if 'end_time' in data and data['end_time']:
            if isinstance(data['end_time'], str):
                instance.end_time = datetime.datetime.fromisoformat(data['end_time'])
            else:
                instance.end_time = data['end_time']
        
        if 'execution_metrics' in data:
            instance.execution_metrics = data['execution_metrics']
            
        if 'error_details' in data:
            instance.error_details = data['error_details']
            
        if 'retry_count' in data:
            instance.retry_count = data['retry_count']
            
        if 'retry_history' in data:
            instance.retry_history = data['retry_history']
            
        if 'self_healing_attempts' in data:
            instance.self_healing_attempts = data['self_healing_attempts']
            
        if 'metadata' in data:
            instance.metadata = data['metadata']
            
        if 'records_processed' in data:
            instance.records_processed = data['records_processed']
            
        if 'records_failed' in data:
            instance.records_failed = data['records_failed']
            
        if 'quality_score' in data:
            instance.quality_score = data['quality_score']
            
        return instance
    
    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'PipelineExecution':
        """
        Create a PipelineExecution instance from a BigQuery row.
        
        Args:
            row: BigQuery row data
            
        Returns:
            PipelineExecution: New PipelineExecution instance
        """
        # Parse JSON fields
        execution_params = json.loads(row.get('execution_params', '{}')) if row.get('execution_params') else {}
        execution_metrics = json.loads(row.get('execution_metrics', '{}')) if row.get('execution_metrics') else {}
        error_details = json.loads(row.get('error_details', '{}')) if row.get('error_details') else {}
        retry_history = json.loads(row.get('retry_history', '{}')) if row.get('retry_history') else {}
        self_healing_attempts = json.loads(row.get('self_healing_attempts', '[]')) if row.get('self_healing_attempts') else []
        metadata = json.loads(row.get('metadata', '{}')) if row.get('metadata') else {}
        
        # Create instance
        instance = cls(
            pipeline_id=row.get('pipeline_id'),
            execution_id=row.get('execution_id'),
            dag_run_id=row.get('dag_run_id'),
            execution_params=execution_params,
            status=PipelineStatus(row.get('status')) if row.get('status') else PipelineStatus.PENDING
        )
        
        # Set additional properties
        if 'start_time' in row and row['start_time']:
            instance.start_time = row['start_time']
            
        if 'end_time' in row and row['end_time']:
            instance.end_time = row['end_time']
            
        instance.execution_metrics = execution_metrics
        instance.error_details = error_details
        instance.retry_count = row.get('retry_count', 0)
        instance.retry_history = retry_history
        instance.self_healing_attempts = self_healing_attempts
        instance.metadata = metadata
        instance.records_processed = row.get('records_processed', 0)
        instance.records_failed = row.get('records_failed', 0)
        instance.quality_score = row.get('quality_score')
        
        return instance
    
    def to_bigquery_row(self) -> dict:
        """
        Convert the pipeline execution to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        return {
            "execution_id": self.execution_id,
            "pipeline_id": self.pipeline_id,
            "dag_run_id": self.dag_run_id,
            "status": self.status.value if isinstance(self.status, enum.Enum) else self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "execution_params": json.dumps(self.execution_params),
            "execution_metrics": json.dumps(self.execution_metrics),
            "error_details": json.dumps(self.error_details),
            "retry_count": self.retry_count,
            "retry_history": json.dumps(self.retry_history),
            "self_healing_attempts": json.dumps(self.self_healing_attempts),
            "metadata": json.dumps(self.metadata),
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "quality_score": self.quality_score
        }
    
    def update_status(self, status: PipelineStatus) -> None:
        """
        Update the status of the pipeline execution.
        
        Args:
            status: New status value
        """
        self.status = status
        
        # If status is terminal, set end_time if not already set
        if status in [PipelineStatus.SUCCESS, PipelineStatus.FAILED, PipelineStatus.PARTIALLY_SUCCEEDED] and not self.end_time:
            self.end_time = datetime.datetime.now()
            
        logger.info(f"Pipeline execution {self.execution_id} status updated to {status}")
    
    def update_metrics(self, metrics: dict) -> None:
        """
        Update the execution metrics for the pipeline execution.
        
        Args:
            metrics: Dictionary of metrics to update
        """
        self.execution_metrics.update(metrics)
        logger.info(f"Updated metrics for pipeline execution {self.execution_id}")
    
    def set_error(self, error_type: str, error_message: str, error_context: dict = None) -> None:
        """
        Set error details for a failed pipeline execution.
        
        Args:
            error_type: Type of error
            error_message: Error message
            error_context: Additional error context
        """
        self.error_details = {
            "type": error_type,
            "message": error_message,
            "timestamp": datetime.datetime.now().isoformat(),
            "context": error_context or {}
        }
        
        self.update_status(PipelineStatus.FAILED)
        logger.error(f"Pipeline execution {self.execution_id} failed: {error_type} - {error_message}")
    
    def record_retry(self, retry_params: dict = None) -> None:
        """
        Record a retry attempt for the pipeline execution.
        
        Args:
            retry_params: Parameters for the retry attempt
        """
        self.retry_count += 1
        
        retry_record = {
            "attempt": self.retry_count,
            "timestamp": datetime.datetime.now().isoformat(),
            "params": retry_params or {}
        }
        
        if "attempts" not in self.retry_history:
            self.retry_history["attempts"] = []
            
        self.retry_history["attempts"].append(retry_record)
        
        # Reset status to pending for retry
        self.status = PipelineStatus.PENDING
        
        logger.info(f"Recorded retry #{self.retry_count} for pipeline execution {self.execution_id}")
    
    def add_self_healing_attempt(
        self,
        healing_id: str,
        issue_type: str,
        action_taken: str,
        successful: bool,
        details: dict = None
    ) -> None:
        """
        Record a self-healing attempt for the pipeline execution.
        
        Args:
            healing_id: Unique identifier for this healing attempt
            issue_type: Type of issue that was detected
            action_taken: Description of the healing action taken
            successful: Whether the healing attempt was successful
            details: Additional details about the healing attempt
        """
        healing_record = {
            "healing_id": healing_id,
            "issue_type": issue_type,
            "action_taken": action_taken,
            "timestamp": datetime.datetime.now().isoformat(),
            "successful": successful,
            "details": details or {}
        }
        
        self.self_healing_attempts.append(healing_record)
        
        if successful:
            # Update status to indicate healing is in progress
            self.status = PipelineStatus.HEALING
            
        logger.info(
            f"Added self-healing attempt for pipeline execution {self.execution_id}: "
            f"{issue_type} - {action_taken} - {'successful' if successful else 'failed'}"
        )
    
    def complete(self, status: PipelineStatus, final_metrics: dict = None) -> None:
        """
        Mark the pipeline execution as complete with the specified status.
        
        Args:
            status: Final status of the execution
            final_metrics: Final metrics to add to execution metrics
        """
        if final_metrics:
            self.update_metrics(final_metrics)
            
        self.update_status(status)
        
        # Ensure end_time is set
        if not self.end_time:
            self.end_time = datetime.datetime.now()
            
        # Calculate and add execution duration to metrics
        duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.execution_metrics["duration_seconds"] = duration_seconds
        
        logger.info(f"Pipeline execution {self.execution_id} completed with status {status}")
    
    def update_record_counts(self, processed: int, failed: int) -> None:
        """
        Update the record processing counts for the pipeline execution.
        
        Args:
            processed: Number of records processed to add
            failed: Number of records failed to add
        """
        self.records_processed += processed
        self.records_failed += failed
        
        logger.info(
            f"Updated record counts for pipeline execution {self.execution_id}: "
            f"+{processed} processed, +{failed} failed"
        )
    
    def set_quality_score(self, score: float) -> None:
        """
        Set the data quality score for the pipeline execution.
        
        Args:
            score: Quality score value (0-100)
        """
        self.quality_score = score
        logger.info(f"Set quality score to {score} for pipeline execution {self.execution_id}")
    
    def get_duration(self) -> typing.Optional[float]:
        """
        Get the duration of the pipeline execution in seconds.
        
        Returns:
            float: Duration in seconds or None if execution is not complete
        """
        if not self.end_time:
            return None
            
        return (self.end_time - self.start_time).total_seconds()
    
    def is_complete(self) -> bool:
        """
        Check if the pipeline execution is complete.
        
        Returns:
            bool: True if execution is complete, False otherwise
        """
        return self.status in [PipelineStatus.SUCCESS, PipelineStatus.FAILED, PipelineStatus.PARTIALLY_SUCCEEDED]
    
    def is_successful(self) -> bool:
        """
        Check if the pipeline execution was successful.
        
        Returns:
            bool: True if execution was successful, False otherwise
        """
        return self.status == PipelineStatus.SUCCESS
    
    def has_retried(self) -> bool:
        """
        Check if the pipeline has been retried.
        
        Returns:
            bool: True if pipeline has been retried, False otherwise
        """
        return self.retry_count > 0
    
    def has_self_healed(self) -> bool:
        """
        Check if the pipeline has undergone self-healing.
        
        Returns:
            bool: True if pipeline has self-healing attempts, False otherwise
        """
        return len(self.self_healing_attempts) > 0
    
    def get_success_rate(self) -> typing.Optional[float]:
        """
        Calculate the success rate of record processing.
        
        Returns:
            float: Success rate as a percentage or None if no records processed
        """
        if self.records_processed == 0:
            return None
            
        success_count = self.records_processed - self.records_failed
        return (success_count / self.records_processed) * 100