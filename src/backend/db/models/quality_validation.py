"""
Quality validation model for the self-healing data pipeline.

This module defines the QualityValidation model class that represents data quality
validation results in the self-healing data pipeline. It tracks validation outcomes,
metrics, and relationships to pipeline executions and quality rules, enabling the
self-healing system to identify and address data quality issues.
"""

import datetime
import uuid
import typing
import json

from ...constants import (
    ValidationRuleType, 
    QualityDimension,
    VALIDATION_STATUS_PASSED,
    VALIDATION_STATUS_FAILED,
    VALIDATION_STATUS_WARNING
)
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Get module logger
logger = get_logger(__name__)

# Define table name constant
QUALITY_VALIDATION_TABLE_NAME = "quality_validations"


def generate_validation_id() -> str:
    """
    Generates a unique identifier for a quality validation.
    
    Returns:
        str: Unique validation ID with 'val_' prefix
    """
    return f"val_{str(uuid.uuid4())}"


def get_quality_validation_table_schema() -> list:
    """
    Returns the BigQuery table schema for the quality validations table.
    
    Returns:
        list: List of SchemaField objects defining the table schema
    """
    return [
        get_schema_field("validation_id", "STRING", "REQUIRED", "Unique identifier for the validation"),
        get_schema_field("execution_id", "STRING", "REQUIRED", "Identifier of the pipeline execution"),
        get_schema_field("rule_id", "STRING", "REQUIRED", "Identifier of the quality rule"),
        get_schema_field("status", "STRING", "REQUIRED", "Validation status (PASSED, FAILED, WARNING)"),
        get_schema_field("validation_time", "TIMESTAMP", "REQUIRED", "Time when validation was performed"),
        get_schema_field("execution_time", "FLOAT", "NULLABLE", "Time taken to execute validation in seconds"),
        get_schema_field("details", "STRING", "NULLABLE", "Detailed validation results as JSON"),
        get_schema_field("metrics", "STRING", "NULLABLE", "Validation metrics as JSON"),
        get_schema_field("issue_count", "INTEGER", "NULLABLE", "Number of issues found during validation"),
        get_schema_field("dimension", "STRING", "NULLABLE", "Quality dimension being validated"),
        get_schema_field("rule_type", "STRING", "NULLABLE", "Type of validation rule"),
        get_schema_field("severity", "STRING", "NULLABLE", "Severity of issues (HIGH, MEDIUM, LOW)"),
        get_schema_field("requires_healing", "BOOLEAN", "NULLABLE", "Whether this validation requires self-healing"),
        get_schema_field("healing_id", "STRING", "NULLABLE", "Identifier of the healing execution addressing this validation")
    ]


def create_quality_validation(execution_id: str, rule_id: str, status: str, details: dict = None) -> 'QualityValidation':
    """
    Creates a new quality validation record.
    
    Args:
        execution_id: Identifier of the pipeline execution
        rule_id: Identifier of the quality rule
        status: Validation status (PASSED, FAILED, WARNING)
        details: Detailed validation results
        
    Returns:
        QualityValidation: Newly created quality validation instance
    """
    validation_id = generate_validation_id()
    validation = QualityValidation(validation_id, execution_id, rule_id, status)
    validation.validation_time = datetime.datetime.now()
    validation.details = details or {}
    validation.metrics = {}
    validation.issue_count = 0
    
    logger.info(f"Created quality validation {validation_id} for execution {execution_id}, rule {rule_id}")
    return validation


def get_quality_validation(validation_id: str) -> typing.Optional['QualityValidation']:
    """
    Retrieves a quality validation by its ID.
    
    Args:
        validation_id: Unique identifier of the validation
        
    Returns:
        QualityValidation: Retrieved quality validation or None if not found
    """
    # This is a placeholder function that should be implemented with actual database retrieval
    # In a real implementation, this would query the database for the validation
    logger.debug(f"Retrieving quality validation {validation_id}")
    return None


def get_validations_by_execution(execution_id: str) -> list:
    """
    Retrieves all quality validations for a specific pipeline execution.
    
    Args:
        execution_id: Identifier of the pipeline execution
        
    Returns:
        list: List of QualityValidation instances for the execution
    """
    # This is a placeholder function that should be implemented with actual database retrieval
    # In a real implementation, this would query the database for all validations matching the execution_id
    logger.debug(f"Retrieving validations for execution {execution_id}")
    return []


def get_validations_by_rule(rule_id: str) -> list:
    """
    Retrieves all quality validations for a specific rule.
    
    Args:
        rule_id: Identifier of the quality rule
        
    Returns:
        list: List of QualityValidation instances for the rule
    """
    # This is a placeholder function that should be implemented with actual database retrieval
    # In a real implementation, this would query the database for all validations matching the rule_id
    logger.debug(f"Retrieving validations for rule {rule_id}")
    return []


class QualityValidation:
    """
    Model class representing a data quality validation result.
    
    This class tracks information about a data quality validation, including its
    status, metrics, issues found, and relationship to healing actions.
    """
    
    def __init__(self, validation_id: str, execution_id: str, rule_id: str, status: str):
        """
        Initialize a new quality validation with provided parameters.
        
        Args:
            validation_id: Unique identifier for the validation
            execution_id: Identifier of the pipeline execution
            rule_id: Identifier of the quality rule
            status: Validation status (PASSED, FAILED, WARNING)
        """
        self.validation_id = validation_id or generate_validation_id()
        self.execution_id = execution_id
        self.rule_id = rule_id
        self.status = status
        self.validation_time = datetime.datetime.now()
        self.execution_time = 0.0
        self.details = {}
        self.metrics = {}
        self.issue_count = 0
        self.dimension = None  # QualityDimension enum
        self.rule_type = None  # ValidationRuleType enum
        self.severity = "MEDIUM"
        self.requires_healing = False
        self.healing_id = None
        
        logger.debug(f"Initialized quality validation {self.validation_id}")
    
    def to_dict(self) -> dict:
        """
        Convert the quality validation to a dictionary representation.
        
        Returns:
            dict: Dictionary representation of the quality validation
        """
        # Convert the validation to a dictionary for serialization
        result = {
            "validation_id": self.validation_id,
            "execution_id": self.execution_id,
            "rule_id": self.rule_id,
            "status": self.status,
            "validation_time": self.validation_time.isoformat() if self.validation_time else None,
            "execution_time": self.execution_time,
            "details": self.details,
            "metrics": self.metrics,
            "issue_count": self.issue_count,
            "dimension": self.dimension.value if self.dimension else None,
            "rule_type": self.rule_type.value if self.rule_type else None,
            "severity": self.severity,
            "requires_healing": self.requires_healing,
            "healing_id": self.healing_id
        }
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'QualityValidation':
        """
        Create a QualityValidation instance from a dictionary.
        
        Args:
            data: Dictionary containing validation properties
            
        Returns:
            QualityValidation: New QualityValidation instance
        """
        # Extract required fields from the dictionary
        validation_id = data.get("validation_id")
        execution_id = data.get("execution_id")
        rule_id = data.get("rule_id")
        status = data.get("status")
        
        # Create a new QualityValidation instance with the extracted values
        validation = cls(validation_id, execution_id, rule_id, status)
        
        # Set properties from dictionary
        if "validation_time" in data and data["validation_time"]:
            if isinstance(data["validation_time"], str):
                validation.validation_time = datetime.datetime.fromisoformat(data["validation_time"])
            else:
                validation.validation_time = data["validation_time"]
                
        if "execution_time" in data:
            validation.execution_time = float(data["execution_time"])
            
        if "details" in data:
            validation.details = data["details"]
            
        if "metrics" in data:
            validation.metrics = data["metrics"]
            
        if "issue_count" in data:
            validation.issue_count = int(data["issue_count"])
            
        if "dimension" in data and data["dimension"]:
            try:
                validation.dimension = QualityDimension(data["dimension"])
            except ValueError:
                logger.warning(f"Invalid quality dimension: {data['dimension']}")
                
        if "rule_type" in data and data["rule_type"]:
            try:
                validation.rule_type = ValidationRuleType(data["rule_type"])
            except ValueError:
                logger.warning(f"Invalid validation rule type: {data['rule_type']}")
                
        if "severity" in data:
            validation.severity = data["severity"]
            
        if "requires_healing" in data:
            validation.requires_healing = bool(data["requires_healing"])
            
        if "healing_id" in data:
            validation.healing_id = data["healing_id"]
            
        return validation
    
    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'QualityValidation':
        """
        Create a QualityValidation instance from a BigQuery row.
        
        Args:
            row: Dictionary containing a BigQuery table row
            
        Returns:
            QualityValidation: New QualityValidation instance
        """
        # Extract validation properties from BigQuery row
        validation_id = row.get("validation_id")
        execution_id = row.get("execution_id")
        rule_id = row.get("rule_id")
        status = row.get("status")
        
        # Create and return new QualityValidation instance with extracted properties
        validation = cls(validation_id, execution_id, rule_id, status)
        
        if "validation_time" in row and row["validation_time"]:
            validation.validation_time = row["validation_time"]
            
        if "execution_time" in row:
            validation.execution_time = float(row["execution_time"])
            
        if "details" in row and row["details"]:
            # Parse JSON fields
            validation.details = json.loads(row["details"])
            
        if "metrics" in row and row["metrics"]:
            validation.metrics = json.loads(row["metrics"])
            
        if "issue_count" in row:
            validation.issue_count = int(row["issue_count"])
            
        if "dimension" in row and row["dimension"]:
            try:
                validation.dimension = QualityDimension(row["dimension"])
            except ValueError:
                logger.warning(f"Invalid quality dimension in BigQuery row: {row['dimension']}")
                
        if "rule_type" in row and row["rule_type"]:
            try:
                validation.rule_type = ValidationRuleType(row["rule_type"])
            except ValueError:
                logger.warning(f"Invalid validation rule type in BigQuery row: {row['rule_type']}")
                
        if "severity" in row:
            validation.severity = row["severity"]
            
        if "requires_healing" in row:
            validation.requires_healing = bool(row["requires_healing"])
            
        if "healing_id" in row:
            validation.healing_id = row["healing_id"]
            
        return validation
    
    def to_bigquery_row(self) -> dict:
        """
        Convert the quality validation to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        return {
            "validation_id": self.validation_id,
            "execution_id": self.execution_id,
            "rule_id": self.rule_id,
            "status": self.status,
            "validation_time": self.validation_time.isoformat() if self.validation_time else None,
            "execution_time": self.execution_time,
            "details": json.dumps(self.details) if self.details else None,
            "metrics": json.dumps(self.metrics) if self.metrics else None,
            "issue_count": self.issue_count,
            "dimension": self.dimension.value if self.dimension else None,
            "rule_type": self.rule_type.value if self.rule_type else None,
            "severity": self.severity,
            "requires_healing": self.requires_healing,
            "healing_id": self.healing_id
        }
    
    def set_details(self, details: dict) -> None:
        """
        Set the validation details.
        
        Args:
            details: Detailed validation results
        """
        self.details = details
        logger.debug(f"Updated details for validation {self.validation_id}")
    
    def set_metrics(self, metrics: dict) -> None:
        """
        Set the validation metrics.
        
        Args:
            metrics: Validation metrics dictionary
        """
        self.metrics = metrics
        logger.debug(f"Updated metrics for validation {self.validation_id}")
    
    def set_execution_time(self, execution_time: float) -> None:
        """
        Set the execution time for the validation.
        
        Args:
            execution_time: Time taken to execute validation in seconds
        """
        self.execution_time = execution_time
        logger.debug(f"Updated execution time for validation {self.validation_id}: {execution_time}s")
    
    def set_rule_metadata(self, rule_type: ValidationRuleType, dimension: QualityDimension, severity: str = "MEDIUM") -> None:
        """
        Set rule type, dimension, and severity from rule metadata.
        
        Args:
            rule_type: Type of validation rule
            dimension: Quality dimension being validated
            severity: Severity of issues (defaults to MEDIUM)
        """
        self.rule_type = rule_type
        self.dimension = dimension
        self.severity = severity
        logger.debug(f"Updated rule metadata for validation {self.validation_id}")
    
    def set_issue_count(self, count: int) -> None:
        """
        Set the number of issues found during validation.
        
        Args:
            count: Number of issues found
        """
        self.issue_count = count
        logger.debug(f"Updated issue count for validation {self.validation_id}: {count} issues")
    
    def mark_for_healing(self) -> None:
        """
        Mark the validation as requiring self-healing.
        """
        self.requires_healing = True
        logger.info(f"Validation {self.validation_id} marked for self-healing")
    
    def set_healing_id(self, healing_id: str) -> None:
        """
        Set the ID of the healing execution addressing this validation.
        
        Args:
            healing_id: Identifier of the healing execution
        """
        self.healing_id = healing_id
        logger.info(f"Validation {self.validation_id} associated with healing execution {healing_id}")
    
    def is_passed(self) -> bool:
        """
        Check if the validation passed.
        
        Returns:
            bool: True if validation passed, False otherwise
        """
        return self.status == VALIDATION_STATUS_PASSED
    
    def is_failed(self) -> bool:
        """
        Check if the validation failed.
        
        Returns:
            bool: True if validation failed, False otherwise
        """
        return self.status == VALIDATION_STATUS_FAILED
    
    def is_warning(self) -> bool:
        """
        Check if the validation resulted in a warning.
        
        Returns:
            bool: True if validation is a warning, False otherwise
        """
        return self.status == VALIDATION_STATUS_WARNING
    
    def needs_healing(self) -> bool:
        """
        Check if the validation requires self-healing.
        
        Returns:
            bool: True if validation requires healing, False otherwise
        """
        return self.requires_healing
    
    def has_been_healed(self) -> bool:
        """
        Check if the validation has been addressed by a healing execution.
        
        Returns:
            bool: True if validation has been healed, False otherwise
        """
        return self.healing_id is not None