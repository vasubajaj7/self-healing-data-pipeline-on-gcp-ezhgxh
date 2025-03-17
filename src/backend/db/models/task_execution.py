"""
Task Execution model for the self-healing data pipeline.

This module defines the TaskExecution class that represents individual task
executions within a pipeline, tracking status, metrics, and execution details
to enable monitoring, troubleshooting, and self-healing at the task level.
"""

import datetime
import json
import typing
import uuid

from ...constants import (
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCESS,
    TASK_STATUS_FAILED,
    TASK_STATUS_SKIPPED,
    TASK_STATUS_UPSTREAM_FAILED
)
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Set up logger
logger = get_logger(__name__)

# Table name constant
TASK_EXECUTION_TABLE_NAME = "task_executions"


def generate_task_execution_id() -> str:
    """
    Generate a unique identifier for a task execution.
    
    Returns:
        str: Unique task execution ID with 'task_' prefix
    """
    return f"task_{str(uuid.uuid4())}"


def get_task_execution_table_schema() -> list:
    """
    Define the BigQuery table schema for task executions.
    
    Returns:
        list: List of SchemaField objects defining the table schema
    """
    return [
        get_schema_field("task_execution_id", "STRING", "REQUIRED", "Unique identifier for the task execution"),
        get_schema_field("execution_id", "STRING", "REQUIRED", "Reference to parent pipeline execution"),
        get_schema_field("task_id", "STRING", "REQUIRED", "Identifier of the task"),
        get_schema_field("task_type", "STRING", "REQUIRED", "Type of the task"),
        get_schema_field("status", "STRING", "REQUIRED", "Current status of the task execution"),
        get_schema_field("start_time", "TIMESTAMP", "REQUIRED", "Time when the task execution started"),
        get_schema_field("end_time", "TIMESTAMP", "NULLABLE", "Time when the task execution ended"),
        get_schema_field("task_params", "JSON", "NULLABLE", "Parameters used for the task execution"),
        get_schema_field("task_metrics", "JSON", "NULLABLE", "Metrics collected during task execution"),
        get_schema_field("error_details", "JSON", "NULLABLE", "Details about errors if task failed"),
        get_schema_field("retry_count", "INTEGER", "REQUIRED", "Number of retry attempts"),
        get_schema_field("retry_history", "JSON", "NULLABLE", "History of retry attempts"),
        get_schema_field("metadata", "JSON", "NULLABLE", "Additional metadata about the task execution"),
        get_schema_field("records_processed", "INTEGER", "REQUIRED", "Number of records processed by the task"),
        get_schema_field("records_failed", "INTEGER", "REQUIRED", "Number of records failed during processing"),
        get_schema_field("duration_seconds", "FLOAT", "NULLABLE", "Duration of task execution in seconds")
    ]


class TaskExecution:
    """
    Model representing an execution instance of a task within a pipeline.
    
    This class tracks the execution status, timing, parameters, metrics, and other
    details about a specific task execution to support monitoring, troubleshooting,
    and self-healing capabilities.
    """

    def __init__(
        self, 
        execution_id: str, 
        task_id: str, 
        task_type: str, 
        task_execution_id: str = None, 
        task_params: dict = None, 
        status: str = None
    ):
        """
        Initialize a new task execution.
        
        Args:
            execution_id: Reference to the parent pipeline execution
            task_id: Identifier of the task
            task_type: Type of the task
            task_execution_id: Unique identifier, generated if not provided
            task_params: Parameters for task execution
            status: Initial status, defaults to PENDING
        """
        self.task_execution_id = task_execution_id or generate_task_execution_id()
        self.execution_id = execution_id
        self.task_id = task_id
        self.task_type = task_type
        self.status = status or TASK_STATUS_PENDING
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.task_params = task_params or {}
        self.task_metrics = {}
        self.error_details = {}
        self.retry_count = 0
        self.retry_history = {}
        self.metadata = {}
        self.records_processed = 0
        self.records_failed = 0

        logger.info(
            f"Created task execution {self.task_execution_id} for task {self.task_id} "
            f"in execution {self.execution_id}"
        )

    def to_dict(self) -> dict:
        """
        Convert the task execution to a dictionary representation.
        
        Returns:
            dict: Dictionary with all task execution attributes
        """
        result = {
            "task_execution_id": self.task_execution_id,
            "execution_id": self.execution_id,
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "task_params": self.task_params,
            "task_metrics": self.task_metrics,
            "error_details": self.error_details,
            "retry_count": self.retry_count,
            "retry_history": self.retry_history,
            "metadata": self.metadata,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "duration_seconds": self.get_duration()
        }
        return result

    @classmethod
    def from_dict(cls, data: dict) -> 'TaskExecution':
        """
        Create a TaskExecution instance from a dictionary.
        
        Args:
            data: Dictionary containing task execution attributes
            
        Returns:
            TaskExecution: New TaskExecution instance
        """
        task_execution = cls(
            execution_id=data["execution_id"],
            task_id=data["task_id"],
            task_type=data["task_type"],
            task_execution_id=data.get("task_execution_id"),
            task_params=data.get("task_params", {}),
            status=data.get("status", TASK_STATUS_PENDING)
        )
        
        # Handle datetime fields
        if "start_time" in data and data["start_time"]:
            if isinstance(data["start_time"], str):
                task_execution.start_time = datetime.datetime.fromisoformat(data["start_time"])
            else:
                task_execution.start_time = data["start_time"]
                
        if "end_time" in data and data["end_time"]:
            if isinstance(data["end_time"], str):
                task_execution.end_time = datetime.datetime.fromisoformat(data["end_time"])
            else:
                task_execution.end_time = data["end_time"]
        
        # Set additional properties if present
        for prop in ["task_metrics", "error_details", "retry_history", "metadata"]:
            if prop in data:
                setattr(task_execution, prop, data.get(prop, {}))
                
        if "retry_count" in data:
            task_execution.retry_count = data["retry_count"]
            
        if "records_processed" in data:
            task_execution.records_processed = data["records_processed"]
            
        if "records_failed" in data:
            task_execution.records_failed = data["records_failed"]
        
        return task_execution

    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'TaskExecution':
        """
        Create a TaskExecution instance from a BigQuery row.
        
        Args:
            row: BigQuery row data
            
        Returns:
            TaskExecution: New TaskExecution instance
        """
        # Create a dictionary to pass to from_dict
        task_dict = {
            "task_execution_id": row.get("task_execution_id"),
            "execution_id": row.get("execution_id"),
            "task_id": row.get("task_id"),
            "task_type": row.get("task_type"),
            "status": row.get("status"),
            "start_time": row.get("start_time"),
            "end_time": row.get("end_time"),
            "retry_count": row.get("retry_count", 0),
            "records_processed": row.get("records_processed", 0),
            "records_failed": row.get("records_failed", 0)
        }
        
        # Parse JSON fields
        for json_field in ["task_params", "task_metrics", "error_details", "retry_history", "metadata"]:
            if json_field in row and row[json_field]:
                if isinstance(row[json_field], str):
                    task_dict[json_field] = json.loads(row[json_field])
                else:
                    task_dict[json_field] = row[json_field]
            else:
                task_dict[json_field] = {}
        
        return cls.from_dict(task_dict)

    def to_bigquery_row(self) -> dict:
        """
        Convert the task execution to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        # Start with regular dictionary representation
        bq_row = self.to_dict()
        
        # Convert datetime objects to strings if not already
        if isinstance(bq_row["start_time"], datetime.datetime):
            bq_row["start_time"] = bq_row["start_time"].isoformat()
            
        if bq_row["end_time"] and isinstance(bq_row["end_time"], datetime.datetime):
            bq_row["end_time"] = bq_row["end_time"].isoformat()
        
        # Convert dict fields to JSON strings
        for json_field in ["task_params", "task_metrics", "error_details", "retry_history", "metadata"]:
            if json_field in bq_row and bq_row[json_field]:
                bq_row[json_field] = json.dumps(bq_row[json_field])
        
        return bq_row

    def update_status(self, status: str) -> None:
        """
        Update the status of the task execution.
        
        Args:
            status: New status value
        """
        self.status = status
        
        # If status is terminal, set end_time if not already set
        if status in [TASK_STATUS_SUCCESS, TASK_STATUS_FAILED, TASK_STATUS_SKIPPED, TASK_STATUS_UPSTREAM_FAILED]:
            if not self.end_time:
                self.end_time = datetime.datetime.now()
        
        logger.info(
            f"Updated task execution {self.task_execution_id} status to {status}"
        )

    def update_metrics(self, metrics: dict) -> None:
        """
        Update the execution metrics for the task.
        
        Args:
            metrics: Dictionary of metrics to update
        """
        self.task_metrics.update(metrics)
        logger.debug(
            f"Updated metrics for task execution {self.task_execution_id}: {metrics}"
        )

    def set_error(self, error_type: str, error_message: str, error_context: dict = None) -> None:
        """
        Set error details for a failed task execution.
        
        Args:
            error_type: Type of error
            error_message: Error message
            error_context: Additional context about the error
        """
        self.error_details = {
            "type": error_type,
            "message": error_message,
            "timestamp": datetime.datetime.now().isoformat(),
            "context": error_context or {}
        }
        
        # Update status to FAILED
        self.update_status(TASK_STATUS_FAILED)
        
        logger.error(
            f"Task execution {self.task_execution_id} failed with error: {error_type} - {error_message}"
        )

    def record_retry(self, retry_params: dict = None) -> None:
        """
        Record a retry attempt for the task execution.
        
        Args:
            retry_params: Parameters for the retry attempt
        """
        self.retry_count += 1
        
        retry_timestamp = datetime.datetime.now().isoformat()
        retry_key = f"retry_{self.retry_count}"
        
        # Record retry details
        self.retry_history[retry_key] = {
            "timestamp": retry_timestamp,
            "params": retry_params or {},
            "previous_status": self.status
        }
        
        # Reset status to pending
        self.status = TASK_STATUS_PENDING
        
        logger.info(
            f"Recorded retry #{self.retry_count} for task execution {self.task_execution_id}"
        )

    def complete(self, status: str = TASK_STATUS_SUCCESS, final_metrics: dict = None) -> None:
        """
        Mark the task execution as complete with the specified status.
        
        Args:
            status: Final status (default: SUCCESS)
            final_metrics: Final metrics to update
        """
        # Update metrics if provided
        if final_metrics:
            self.update_metrics(final_metrics)
        
        # Update status and set end time
        self.update_status(status)
        
        # Ensure end_time is set
        if not self.end_time:
            self.end_time = datetime.datetime.now()
        
        # Calculate duration and add to metrics
        duration = self.get_duration()
        if duration is not None:
            self.task_metrics["duration_seconds"] = duration
        
        logger.info(
            f"Completed task execution {self.task_execution_id} with status {status}"
        )

    def update_record_counts(self, processed: int = 0, failed: int = 0) -> None:
        """
        Update the record processing counts for the task execution.
        
        Args:
            processed: Number of successfully processed records to add
            failed: Number of failed records to add
        """
        self.records_processed += processed
        self.records_failed += failed
        
        logger.debug(
            f"Updated record counts for task execution {self.task_execution_id}: "
            f"+{processed} processed, +{failed} failed"
        )

    def get_duration(self) -> typing.Optional[float]:
        """
        Get the duration of the task execution in seconds.
        
        Returns:
            float: Duration in seconds or None if execution is not complete
        """
        if not self.end_time:
            return None
            
        delta = self.end_time - self.start_time
        return delta.total_seconds()

    def is_complete(self) -> bool:
        """
        Check if the task execution is complete.
        
        Returns:
            bool: True if the task execution is complete
        """
        return self.status in [
            TASK_STATUS_SUCCESS, 
            TASK_STATUS_FAILED,
            TASK_STATUS_SKIPPED,
            TASK_STATUS_UPSTREAM_FAILED
        ]

    def is_successful(self) -> bool:
        """
        Check if the task execution was successful.
        
        Returns:
            bool: True if the task execution was successful
        """
        return self.status == TASK_STATUS_SUCCESS

    def has_retried(self) -> bool:
        """
        Check if the task has been retried.
        
        Returns:
            bool: True if the task has been retried
        """
        return self.retry_count > 0

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