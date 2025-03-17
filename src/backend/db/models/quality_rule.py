"""
Model class for data quality validation rules in the self-healing data pipeline.

This module defines the QualityRule model that represents data quality rules
used for validating data across different datasets. Quality rules have specific
types, dimensions, and configurable parameters that determine validation behavior.

The model supports serialization to/from dictionaries and BigQuery rows, making it
compatible with both application logic and data storage layers.
"""

import datetime
import json
import typing
import uuid

from ...constants import ValidationRuleType, QualityDimension
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Setup module logger
logger = get_logger(__name__)

# BigQuery table name for quality rules
QUALITY_RULE_TABLE_NAME = "quality_rules"


def generate_rule_id() -> str:
    """
    Generates a unique identifier for a quality rule.
    
    Returns:
        str: Unique rule ID with 'rule_' prefix
    """
    return f"rule_{str(uuid.uuid4())}"


def get_quality_rule_table_schema() -> list:
    """
    Returns the BigQuery table schema for the quality rules table.
    
    Returns:
        list: List of SchemaField objects defining the table schema
    """
    schema = [
        get_schema_field("rule_id", "STRING", "REQUIRED", "Unique identifier for the rule"),
        get_schema_field("name", "STRING", "REQUIRED", "Name of the quality rule"),
        get_schema_field("rule_type", "STRING", "REQUIRED", "Type of validation rule"),
        get_schema_field("subtype", "STRING", "NULLABLE", "Specific rule subtype"),
        get_schema_field("dimension", "STRING", "REQUIRED", "Quality dimension the rule addresses"),
        get_schema_field("description", "STRING", "NULLABLE", "Description of the rule"),
        get_schema_field("parameters", "STRING", "NULLABLE", "Rule parameters as JSON string"),
        get_schema_field("metadata", "STRING", "NULLABLE", "Additional metadata as JSON string"),
        get_schema_field("created_at", "TIMESTAMP", "REQUIRED", "Creation timestamp"),
        get_schema_field("updated_at", "TIMESTAMP", "REQUIRED", "Last updated timestamp"),
        get_schema_field("version", "STRING", "REQUIRED", "Rule version"),
        get_schema_field("enabled", "BOOLEAN", "REQUIRED", "Whether the rule is enabled")
    ]
    return schema


class QualityRule:
    """
    Model class representing a data quality validation rule.
    
    Quality rules define expected qualities of data that should be validated 
    during pipeline execution. These rules have specific types (schema, completeness,
    anomaly detection, referential integrity) and dimensions (completeness, accuracy, etc.).
    """
    
    def __init__(self, 
                 name: str, 
                 rule_type: ValidationRuleType, 
                 subtype: str, 
                 dimension: QualityDimension, 
                 description: str, 
                 parameters: dict = None, 
                 rule_id: str = None):
        """
        Initialize a new quality rule with provided parameters.
        
        Args:
            name: Name of the rule
            rule_type: Type of validation rule
            subtype: Specific rule subtype
            dimension: Quality dimension the rule addresses
            description: Description of the rule
            parameters: Rule parameters dictionary
            rule_id: Unique identifier (generated if None)
        """
        self.rule_id = rule_id or generate_rule_id()
        self.name = name
        self.rule_type = rule_type
        self.subtype = subtype
        self.dimension = dimension
        self.description = description
        self.parameters = parameters or {}
        self.metadata = {}
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()
        self.version = "1.0"
        self.enabled = True
        
        logger.info(f"Created quality rule: {self.name} ({self.rule_id})")

    def to_dict(self) -> dict:
        """
        Convert the quality rule to a dictionary representation.
        
        Returns:
            dict: Dictionary representation of the quality rule
        """
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "rule_type": self.rule_type.value,
            "subtype": self.subtype,
            "dimension": self.dimension.value,
            "description": self.description,
            "parameters": self.parameters,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version,
            "enabled": self.enabled
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'QualityRule':
        """
        Create a QualityRule instance from a dictionary.
        
        Args:
            data: Dictionary containing rule data
        
        Returns:
            QualityRule: New QualityRule instance
        """
        # Extract required fields with appropriate enum conversions
        rule_id = data.get("rule_id")
        name = data.get("name")
        rule_type = ValidationRuleType(data.get("rule_type"))
        subtype = data.get("subtype")
        dimension = QualityDimension(data.get("dimension"))
        description = data.get("description")
        parameters = data.get("parameters", {})
        
        # Create the rule instance
        rule = cls(
            name=name,
            rule_type=rule_type,
            subtype=subtype,
            dimension=dimension,
            description=description,
            parameters=parameters,
            rule_id=rule_id
        )
        
        # Set additional fields if present
        if "metadata" in data:
            rule.metadata = data["metadata"]
        
        if "created_at" in data:
            if isinstance(data["created_at"], str):
                rule.created_at = datetime.datetime.fromisoformat(data["created_at"])
            else:
                rule.created_at = data["created_at"]
        
        if "updated_at" in data:
            if isinstance(data["updated_at"], str):
                rule.updated_at = datetime.datetime.fromisoformat(data["updated_at"])
            else:
                rule.updated_at = data["updated_at"]
        
        if "version" in data:
            rule.version = data["version"]
        
        if "enabled" in data:
            rule.enabled = data["enabled"]
        
        return rule
    
    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'QualityRule':
        """
        Create a QualityRule instance from a BigQuery row.
        
        Args:
            row: BigQuery table row as dictionary
        
        Returns:
            QualityRule: New QualityRule instance
        """
        # Parse JSON strings from BigQuery
        parameters = json.loads(row.get("parameters", "{}"))
        metadata = json.loads(row.get("metadata", "{}"))
        
        # Create rule with required parameters
        rule = cls(
            name=row.get("name"),
            rule_type=ValidationRuleType(row.get("rule_type")),
            subtype=row.get("subtype"),
            dimension=QualityDimension(row.get("dimension")),
            description=row.get("description"),
            parameters=parameters,
            rule_id=row.get("rule_id")
        )
        
        # Set additional properties
        rule.metadata = metadata
        rule.created_at = row.get("created_at")
        rule.updated_at = row.get("updated_at")
        rule.version = row.get("version")
        rule.enabled = row.get("enabled")
        
        return rule
    
    def to_bigquery_row(self) -> dict:
        """
        Convert the quality rule to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "rule_type": self.rule_type.value,
            "subtype": self.subtype,
            "dimension": self.dimension.value,
            "description": self.description,
            "parameters": json.dumps(self.parameters),
            "metadata": json.dumps(self.metadata),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "version": self.version,
            "enabled": self.enabled
        }
    
    def update(self, updates: dict) -> None:
        """
        Update the quality rule with new values.
        
        Args:
            updates: Dictionary containing fields to update
        """
        for key, value in updates.items():
            if key == "rule_id":
                # rule_id should not be updated
                continue
            
            if key == "rule_type" and isinstance(value, str):
                self.__setattr__(key, ValidationRuleType(value))
            elif key == "dimension" and isinstance(value, str):
                self.__setattr__(key, QualityDimension(value))
            else:
                self.__setattr__(key, value)
        
        # Update the timestamp and version
        self.updated_at = datetime.datetime.now()
        
        # Increment version (assuming semantic versioning)
        version_parts = self.version.split(".")
        if len(version_parts) >= 2:
            minor = int(version_parts[1]) + 1
            self.version = f"{version_parts[0]}.{minor}"
        
        logger.info(f"Updated quality rule: {self.name} ({self.rule_id}) to version {self.version}")
    
    def update_parameters(self, parameters: dict) -> None:
        """
        Update the rule parameters.
        
        Args:
            parameters: New parameters dictionary
        """
        self.parameters = parameters
        self.updated_at = datetime.datetime.now()
        
        # Increment version
        version_parts = self.version.split(".")
        if len(version_parts) >= 2:
            minor = int(version_parts[1]) + 1
            self.version = f"{version_parts[0]}.{minor}"
        
        logger.info(f"Updated parameters for rule: {self.name} ({self.rule_id})")
    
    def update_metadata(self, metadata: dict) -> None:
        """
        Update the rule metadata.
        
        Args:
            metadata: New metadata dictionary
        """
        self.metadata = metadata
        self.updated_at = datetime.datetime.now()
        logger.info(f"Updated metadata for rule: {self.name} ({self.rule_id})")
    
    def disable(self) -> None:
        """
        Disable the quality rule.
        """
        self.enabled = False
        self.updated_at = datetime.datetime.now()
        logger.info(f"Disabled rule: {self.name} ({self.rule_id})")
    
    def enable(self) -> None:
        """
        Enable the quality rule.
        """
        self.enabled = True
        self.updated_at = datetime.datetime.now()
        logger.info(f"Enabled rule: {self.name} ({self.rule_id})")
    
    def is_enabled(self) -> bool:
        """
        Check if the rule is enabled.
        
        Returns:
            bool: True if rule is enabled, False otherwise
        """
        return self.enabled
    
    def get_parameter(self, key: str, default: typing.Any = None) -> typing.Any:
        """
        Get a parameter value with optional default.
        
        Args:
            key: Parameter key
            default: Default value if parameter not found
        
        Returns:
            Any: Parameter value or default if not found
        """
        return self.parameters.get(key, default)
    
    def get_metadata(self, key: str, default: typing.Any = None) -> typing.Any:
        """
        Get a metadata value with optional default.
        
        Args:
            key: Metadata key
            default: Default value if metadata not found
        
        Returns:
            Any: Metadata value or default if not found
        """
        return self.metadata.get(key, default)
    
    def get_severity(self) -> str:
        """
        Get the rule severity from metadata.
        
        Returns:
            str: Severity level or 'MEDIUM' if not specified
        """
        return self.metadata.get("severity", "MEDIUM")