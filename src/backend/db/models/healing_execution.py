import datetime
import json
import typing
import uuid

from ...constants import HealingActionType
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Initialize logger
logger = get_logger(__name__)

# Table name constant
HEALING_EXECUTION_TABLE_NAME = "healing_executions"

def generate_healing_id(prefix: str = "heal_") -> str:
    """
    Generates a unique identifier for a healing execution.
    
    Args:
        prefix: Prefix for the ID (default 'heal_')
        
    Returns:
        Unique healing execution ID with the specified prefix
    """
    return f"{prefix}{str(uuid.uuid4())}"

class HealingExecution:
    """Model class representing a self-healing execution attempt."""
    
    def __init__(self, 
                 healing_id: str, 
                 execution_id: str, 
                 pattern_id: str, 
                 action_id: str, 
                 validation_id: str = None):
        """
        Initialize a new healing execution with provided parameters.
        
        Args:
            healing_id: Unique identifier for this healing execution
            execution_id: ID of the pipeline execution
            pattern_id: ID of the detected issue pattern
            action_id: ID of the healing action being applied
            validation_id: Optional ID of the validation that triggered healing
        """
        self.healing_id = healing_id or generate_healing_id()
        self.execution_id = execution_id
        self.pattern_id = pattern_id
        self.action_id = action_id
        self.validation_id = validation_id
        
        # Initialize status and timestamps
        self.status = "PENDING"
        self.execution_time = datetime.datetime.now()
        self.completion_time = None
        
        # Initialize outcome tracking
        self.successful = None  # None until execution completes
        self.confidence_score = None
        
        # Initialize detail and metric tracking
        self.issue_details = {}
        self.execution_details = {}
        self.metrics = {}
        
        logger.info(f"Created new healing execution {self.healing_id} for execution {execution_id}")
    
    def to_dict(self) -> dict:
        """
        Convert the healing execution to a dictionary representation.
        
        Returns:
            Dictionary representation of the healing execution
        """
        return {
            "healing_id": self.healing_id,
            "execution_id": self.execution_id,
            "pattern_id": self.pattern_id,
            "action_id": self.action_id,
            "validation_id": self.validation_id,
            "status": self.status,
            "execution_time": self.execution_time.isoformat() if self.execution_time else None,
            "completion_time": self.completion_time.isoformat() if self.completion_time else None,
            "successful": self.successful,
            "confidence_score": self.confidence_score,
            "issue_details": self.issue_details,
            "execution_details": self.execution_details,
            "metrics": self.metrics
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'HealingExecution':
        """
        Create a HealingExecution instance from a dictionary.
        
        Args:
            data: Dictionary containing healing execution data
            
        Returns:
            New HealingExecution instance
        """
        # Create instance with required fields
        instance = cls(
            healing_id=data.get("healing_id"),
            execution_id=data.get("execution_id"),
            pattern_id=data.get("pattern_id"),
            action_id=data.get("action_id"),
            validation_id=data.get("validation_id")
        )
        
        # Set additional fields
        instance.status = data.get("status", "PENDING")
        
        # Convert ISO format strings to datetime objects
        if "execution_time" in data and data["execution_time"]:
            instance.execution_time = datetime.datetime.fromisoformat(data["execution_time"])
        if "completion_time" in data and data["completion_time"]:
            instance.completion_time = datetime.datetime.fromisoformat(data["completion_time"])
            
        instance.successful = data.get("successful")
        instance.confidence_score = data.get("confidence_score")
        instance.issue_details = data.get("issue_details", {})
        instance.execution_details = data.get("execution_details", {})
        instance.metrics = data.get("metrics", {})
        
        return instance
    
    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'HealingExecution':
        """
        Create a HealingExecution instance from a BigQuery row.
        
        Args:
            row: BigQuery result row
            
        Returns:
            New HealingExecution instance
        """
        # Parse JSON fields
        issue_details = json.loads(row.get("issue_details", "{}"))
        execution_details = json.loads(row.get("execution_details", "{}"))
        metrics = json.loads(row.get("metrics", "{}"))
        
        # Create instance with required fields
        instance = cls(
            healing_id=row.get("healing_id"),
            execution_id=row.get("execution_id"),
            pattern_id=row.get("pattern_id"),
            action_id=row.get("action_id"),
            validation_id=row.get("validation_id")
        )
        
        # Set additional fields
        instance.status = row.get("status", "PENDING")
        
        # Convert timestamp strings to datetime objects
        if "execution_time" in row and row["execution_time"]:
            instance.execution_time = row["execution_time"].replace(tzinfo=None)
        if "completion_time" in row and row["completion_time"]:
            instance.completion_time = row["completion_time"].replace(tzinfo=None)
            
        instance.successful = row.get("successful")
        instance.confidence_score = row.get("confidence_score")
        instance.issue_details = issue_details
        instance.execution_details = execution_details
        instance.metrics = metrics
        
        return instance
    
    def to_bigquery_row(self) -> dict:
        """
        Convert the healing execution to a format suitable for BigQuery insertion.
        
        Returns:
            Dictionary formatted for BigQuery insertion
        """
        return {
            "healing_id": self.healing_id,
            "execution_id": self.execution_id,
            "pattern_id": self.pattern_id,
            "action_id": self.action_id,
            "validation_id": self.validation_id,
            "status": self.status,
            "execution_time": self.execution_time.isoformat() if self.execution_time else None,
            "completion_time": self.completion_time.isoformat() if self.completion_time else None,
            "successful": self.successful,
            "confidence_score": self.confidence_score,
            "issue_details": json.dumps(self.issue_details),
            "execution_details": json.dumps(self.execution_details),
            "metrics": json.dumps(self.metrics)
        }
    
    def start_execution(self, confidence_score: float) -> None:
        """
        Mark the healing execution as started.
        
        Args:
            confidence_score: Confidence score for the healing action
        """
        self.status = "IN_PROGRESS"
        self.confidence_score = confidence_score
        logger.info(f"Started healing execution {self.healing_id} with confidence {confidence_score}")
    
    def complete(self, successful: bool, execution_details: dict = None, metrics: dict = None) -> None:
        """
        Mark the healing execution as complete with the specified outcome.
        
        Args:
            successful: Whether the healing was successful
            execution_details: Optional execution details to add
            metrics: Optional metrics to add
        """
        self.successful = successful
        self.status = "SUCCESS" if successful else "FAILED"
        self.completion_time = datetime.datetime.now()
        
        if execution_details:
            self.execution_details.update(execution_details)
        
        if metrics:
            self.metrics.update(metrics)
            
        logger.info(f"Completed healing execution {self.healing_id} with result: {'success' if successful else 'failure'}")
    
    def update_metrics(self, new_metrics: dict) -> None:
        """
        Update the metrics for the healing execution.
        
        Args:
            new_metrics: New metrics to add
        """
        self.metrics.update(new_metrics)
        logger.info(f"Updated metrics for healing execution {self.healing_id}")
    
    def update_execution_details(self, details: dict) -> None:
        """
        Update the execution details for the healing execution.
        
        Args:
            details: Execution details to add
        """
        self.execution_details.update(details)
        logger.info(f"Updated execution details for healing execution {self.healing_id}")
    
    def require_approval(self, approval_context: dict) -> None:
        """
        Mark the healing execution as requiring approval.
        
        Args:
            approval_context: Context information for the approval
        """
        self.status = "APPROVAL_REQUIRED"
        self.execution_details["approval_context"] = approval_context
        logger.info(f"Healing execution {self.healing_id} requires approval")
    
    def approve(self, approver_id: str, approval_notes: str = None) -> None:
        """
        Mark the healing execution as approved for execution.
        
        Args:
            approver_id: ID of the user who approved
            approval_notes: Optional notes about the approval
        """
        self.status = "APPROVED"
        self.execution_details["approval"] = {
            "approver_id": approver_id,
            "approval_time": datetime.datetime.now().isoformat(),
            "notes": approval_notes
        }
        logger.info(f"Healing execution {self.healing_id} approved by {approver_id}")
    
    def reject(self, rejector_id: str, rejection_reason: str) -> None:
        """
        Mark the healing execution as rejected.
        
        Args:
            rejector_id: ID of the user who rejected
            rejection_reason: Reason for rejection
        """
        self.status = "REJECTED"
        self.successful = False
        self.completion_time = datetime.datetime.now()
        self.execution_details["rejection"] = {
            "rejector_id": rejector_id,
            "rejection_time": datetime.datetime.now().isoformat(),
            "reason": rejection_reason
        }
        logger.info(f"Healing execution {self.healing_id} rejected by {rejector_id}: {rejection_reason}")
    
    def get_duration(self) -> typing.Optional[float]:
        """
        Get the duration of the healing execution in seconds.
        
        Returns:
            Duration in seconds or None if execution is not complete
        """
        if not self.completion_time:
            return None
        
        duration = (self.completion_time - self.execution_time).total_seconds()
        return duration
    
    def is_complete(self) -> bool:
        """
        Check if the healing execution is complete.
        
        Returns:
            True if execution is complete, False otherwise
        """
        return self.status in ["SUCCESS", "FAILED", "REJECTED"]
    
    def is_successful(self) -> bool:
        """
        Check if the healing execution was successful.
        
        Returns:
            True if execution was successful, False otherwise
        """
        return self.successful is True
    
    def needs_approval(self) -> bool:
        """
        Check if the healing execution requires approval.
        
        Returns:
            True if execution requires approval, False otherwise
        """
        return self.status == "APPROVAL_REQUIRED"

def create_healing_execution(execution_id: str, pattern_id: str, action_id: str, 
                            issue_details: dict, validation_id: str = None) -> HealingExecution:
    """
    Creates a new healing execution record.
    
    Args:
        execution_id: ID of the pipeline execution
        pattern_id: ID of the detected issue pattern
        action_id: ID of the healing action being applied
        issue_details: Details about the issue being addressed
        validation_id: Optional ID of the validation that triggered healing
        
    Returns:
        Newly created healing execution instance
    """
    healing_id = generate_healing_id()
    
    healing_exec = HealingExecution(
        healing_id=healing_id,
        execution_id=execution_id,
        pattern_id=pattern_id,
        action_id=action_id,
        validation_id=validation_id
    )
    
    healing_exec.issue_details = issue_details
    
    logger.info(f"Created new healing execution {healing_id} for execution {execution_id}")
    
    return healing_exec

def get_healing_execution(healing_id: str) -> typing.Optional[HealingExecution]:
    """
    Retrieves a healing execution by its ID.
    
    Args:
        healing_id: ID of the healing execution
        
    Returns:
        Retrieved healing execution or None if not found
    """
    # In a real implementation, this would query a database
    # Here we just show the function signature
    logger.info(f"Retrieving healing execution {healing_id}")
    return None

def get_healing_executions_by_execution(execution_id: str) -> list:
    """
    Retrieves all healing executions for a specific pipeline execution.
    
    Args:
        execution_id: ID of the pipeline execution
        
    Returns:
        List of HealingExecution instances for the execution
    """
    # In a real implementation, this would query a database
    # Here we just show the function signature
    logger.info(f"Retrieving healing executions for pipeline execution {execution_id}")
    return []

def get_healing_executions_by_pattern(pattern_id: str) -> list:
    """
    Retrieves all healing executions for a specific issue pattern.
    
    Args:
        pattern_id: ID of the issue pattern
        
    Returns:
        List of HealingExecution instances for the pattern
    """
    # In a real implementation, this would query a database
    # Here we just show the function signature
    logger.info(f"Retrieving healing executions for pattern {pattern_id}")
    return []

def get_healing_executions_by_action(action_id: str) -> list:
    """
    Retrieves all healing executions for a specific healing action.
    
    Args:
        action_id: ID of the healing action
        
    Returns:
        List of HealingExecution instances for the action
    """
    # In a real implementation, this would query a database
    # Here we just show the function signature
    logger.info(f"Retrieving healing executions for action {action_id}")
    return []

def get_healing_execution_table_schema() -> list:
    """
    Returns the BigQuery table schema for healing executions.
    
    Returns:
        List of BigQuery SchemaField objects defining the table schema
    """
    return [
        get_schema_field("healing_id", "STRING", "REQUIRED", "Unique identifier for the healing execution"),
        get_schema_field("execution_id", "STRING", "REQUIRED", "ID of the pipeline execution"),
        get_schema_field("pattern_id", "STRING", "REQUIRED", "ID of the detected issue pattern"),
        get_schema_field("action_id", "STRING", "REQUIRED", "ID of the healing action being applied"),
        get_schema_field("validation_id", "STRING", "NULLABLE", "ID of the validation that triggered healing"),
        get_schema_field("status", "STRING", "REQUIRED", "Current status of the healing execution"),
        get_schema_field("execution_time", "TIMESTAMP", "REQUIRED", "Time when healing execution started"),
        get_schema_field("completion_time", "TIMESTAMP", "NULLABLE", "Time when healing execution completed"),
        get_schema_field("successful", "BOOLEAN", "NULLABLE", "Whether the healing execution was successful"),
        get_schema_field("confidence_score", "FLOAT", "NULLABLE", "Confidence score for the healing action"),
        get_schema_field("issue_details", "STRING", "NULLABLE", "JSON string containing issue details"),
        get_schema_field("execution_details", "STRING", "NULLABLE", "JSON string containing execution details"),
        get_schema_field("metrics", "STRING", "NULLABLE", "JSON string containing execution metrics")
    ]