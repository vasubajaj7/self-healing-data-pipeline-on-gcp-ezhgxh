"""
Defines the database schema structures for the self-healing data pipeline.

This module serves as the central schema definition for database migrations, providing
constants and utilities for creating and managing database objects including:
- BigQuery table schemas
- Firestore collection schemas
- Partitioning and clustering configurations
- Schema versioning and evolution support

The schemas defined here are used throughout the application to ensure consistent
data structure and to enable the self-healing capabilities of the pipeline.
"""

import typing
import datetime
from google.cloud.bigquery import SchemaField

from ...constants import (
    DataSourceType,
    PipelineStatus,
    ValidationRuleType,
    QualityDimension,
    HealingActionType,
    AlertSeverity
)
from ..schema.bigquery_schema import (
    SchemaField as BQSchemaField,
    get_schema_field,
    create_schema_from_dict
)
from ..schema.firestore_schema import (
    CollectionSchemas,
    FirestoreSchema
)
from ...utils.logging.logger import get_logger

# Configure module-level logger
logger = get_logger(__name__)

# Current schema version
SCHEMA_VERSION = "1.0.0"

# BigQuery table names
BIGQUERY_TABLES = {
    "source_systems": "source_systems",
    "pipeline_definitions": "pipeline_definitions",
    "pipeline_executions": "pipeline_executions",
    "task_executions": "task_executions",
    "quality_rules": "quality_rules",
    "quality_validations": "quality_validations",
    "issue_patterns": "issue_patterns",
    "healing_actions": "healing_actions",
    "healing_executions": "healing_executions",
    "pipeline_metrics": "pipeline_metrics",
    "alerts": "alerts",
}

# Firestore collection names
COLLECTION_NAMES = {
    "source_systems": "source_systems",
    "pipeline_definitions": "pipeline_definitions",
    "issue_patterns": "issue_patterns",
    "healing_actions": "healing_actions",
    "configuration": "configuration",
}


def define_source_systems_schema() -> list:
    """
    Defines the schema for the source_systems table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("source_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the source system"),
        SchemaField("source_name", "STRING", mode="REQUIRED", 
                   description="Descriptive name of the source"),
        SchemaField("source_type", "STRING", mode="REQUIRED", 
                   description="Type of source (GCS, CLOUD_SQL, API, etc.)"),
        SchemaField("connection_details", "JSON", mode="NULLABLE", 
                   description="Connection parameters and credentials as JSON"),
        SchemaField("schema_version", "STRING", mode="NULLABLE", 
                   description="Current schema version of the source"),
        SchemaField("extraction_method", "STRING", mode="NULLABLE", 
                   description="Method used for extraction"),
        SchemaField("last_successful_extraction", "TIMESTAMP", mode="NULLABLE", 
                   description="Timestamp of last successful extraction"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this source was created"),
        SchemaField("created_by", "STRING", mode="REQUIRED", 
                   description="User who created this source"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When this source was last updated"),
        SchemaField("updated_by", "STRING", mode="NULLABLE", 
                   description="User who last updated this source"),
        SchemaField("is_active", "BOOLEAN", mode="REQUIRED", 
                   description="Whether this source is active")
    ]


def define_pipeline_definitions_schema() -> list:
    """
    Defines the schema for the pipeline_definitions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("pipeline_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the pipeline definition"),
        SchemaField("source_id", "STRING", mode="REQUIRED", 
                   description="Source system identifier this pipeline uses"),
        SchemaField("pipeline_name", "STRING", mode="REQUIRED", 
                   description="Descriptive name of the pipeline"),
        SchemaField("description", "STRING", mode="NULLABLE", 
                   description="Detailed description of the pipeline"),
        SchemaField("target_dataset", "STRING", mode="REQUIRED", 
                   description="Target BigQuery dataset"),
        SchemaField("target_table", "STRING", mode="REQUIRED", 
                   description="Target BigQuery table"),
        SchemaField("dag_id", "STRING", mode="REQUIRED", 
                   description="Cloud Composer DAG identifier"),
        SchemaField("schedule_interval", "STRING", mode="NULLABLE", 
                   description="Cron expression for pipeline scheduling"),
        SchemaField("pipeline_config", "JSON", mode="NULLABLE", 
                   description="Pipeline configuration parameters as JSON"),
        SchemaField("quality_rules", "JSON", mode="NULLABLE", 
                   description="Quality validation rules for this pipeline"),
        SchemaField("quality_threshold", "FLOAT", mode="NULLABLE", 
                   description="Minimum quality score threshold"),
        SchemaField("self_healing_enabled", "BOOLEAN", mode="REQUIRED", 
                   description="Whether self-healing is enabled for this pipeline"),
        SchemaField("self_healing_config", "JSON", mode="NULLABLE", 
                   description="Self-healing configuration for this pipeline"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this pipeline was created"),
        SchemaField("created_by", "STRING", mode="REQUIRED", 
                   description="User who created this pipeline"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When this pipeline was last updated"),
        SchemaField("updated_by", "STRING", mode="NULLABLE", 
                   description="User who last updated this pipeline"),
        SchemaField("is_active", "BOOLEAN", mode="REQUIRED", 
                   description="Whether this pipeline is active")
    ]


def define_pipeline_executions_schema() -> list:
    """
    Defines the schema for the pipeline_executions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("execution_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the execution"),
        SchemaField("pipeline_id", "STRING", mode="REQUIRED", 
                   description="Reference to pipeline definition"),
        SchemaField("dag_run_id", "STRING", mode="REQUIRED", 
                   description="Airflow DAG run identifier"),
        SchemaField("start_time", "TIMESTAMP", mode="REQUIRED", 
                   description="When execution started"),
        SchemaField("end_time", "TIMESTAMP", mode="NULLABLE", 
                   description="When execution completed"),
        SchemaField("status", "STRING", mode="REQUIRED", 
                   description="Status (PENDING, RUNNING, SUCCESS, FAILED, HEALING)"),
        SchemaField("records_processed", "INTEGER", mode="NULLABLE", 
                   description="Number of records processed"),
        SchemaField("error_details", "JSON", mode="NULLABLE", 
                   description="Details of any errors encountered"),
        SchemaField("execution_params", "JSON", mode="NULLABLE", 
                   description="Parameters used for this execution"),
        SchemaField("quality_score", "FLOAT", mode="NULLABLE", 
                   description="Overall data quality score for this execution"),
        SchemaField("self_healing_attempts", "INTEGER", mode="NULLABLE", 
                   description="Number of self-healing attempts"),
        SchemaField("self_healing_success", "BOOLEAN", mode="NULLABLE", 
                   description="Whether self-healing was successful")
    ]


def define_task_executions_schema() -> list:
    """
    Defines the schema for the task_executions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("task_execution_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the task execution"),
        SchemaField("execution_id", "STRING", mode="REQUIRED", 
                   description="Reference to pipeline execution"),
        SchemaField("task_id", "STRING", mode="REQUIRED", 
                   description="Task identifier from Airflow"),
        SchemaField("task_type", "STRING", mode="REQUIRED", 
                   description="Type of task (extraction, validation, transformation, etc.)"),
        SchemaField("start_time", "TIMESTAMP", mode="REQUIRED", 
                   description="When task started"),
        SchemaField("end_time", "TIMESTAMP", mode="NULLABLE", 
                   description="When task completed"),
        SchemaField("status", "STRING", mode="REQUIRED", 
                   description="Status (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED)"),
        SchemaField("retry_count", "INTEGER", mode="NULLABLE", 
                   description="Number of retry attempts"),
        SchemaField("error_details", "JSON", mode="NULLABLE", 
                   description="Details of any errors encountered"),
        SchemaField("task_params", "JSON", mode="NULLABLE", 
                   description="Parameters used for this task"),
        SchemaField("task_metrics", "JSON", mode="NULLABLE", 
                   description="Performance metrics for this task")
    ]


def define_quality_rules_schema() -> list:
    """
    Defines the schema for the quality_rules table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("rule_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the rule"),
        SchemaField("rule_name", "STRING", mode="REQUIRED", 
                   description="Descriptive name of the rule"),
        SchemaField("rule_type", "STRING", mode="REQUIRED", 
                   description="Type of rule (schema, null check, etc.)"),
        SchemaField("description", "STRING", mode="NULLABLE", 
                   description="Detailed description of the rule"),
        SchemaField("target_dataset", "STRING", mode="NULLABLE", 
                   description="Target dataset this rule applies to"),
        SchemaField("target_table", "STRING", mode="NULLABLE", 
                   description="Target table this rule applies to"),
        SchemaField("target_column", "STRING", mode="NULLABLE", 
                   description="Target column this rule applies to"),
        SchemaField("expectation_type", "STRING", mode="REQUIRED", 
                   description="Great Expectations expectation type"),
        SchemaField("expectation_config", "JSON", mode="REQUIRED", 
                   description="Configuration for the expectation"),
        SchemaField("severity", "STRING", mode="REQUIRED", 
                   description="Rule severity (CRITICAL, HIGH, MEDIUM, LOW)"),
        SchemaField("dimension", "STRING", mode="REQUIRED", 
                   description="Quality dimension (COMPLETENESS, ACCURACY, etc.)"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this rule was created"),
        SchemaField("created_by", "STRING", mode="REQUIRED", 
                   description="User who created this rule"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When this rule was last updated"),
        SchemaField("updated_by", "STRING", mode="NULLABLE", 
                   description="User who last updated this rule"),
        SchemaField("is_active", "BOOLEAN", mode="REQUIRED", 
                   description="Whether this rule is active")
    ]


def define_quality_validations_schema() -> list:
    """
    Defines the schema for the quality_validations table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("validation_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the validation"),
        SchemaField("execution_id", "STRING", mode="REQUIRED", 
                   description="Reference to pipeline execution"),
        SchemaField("rule_id", "STRING", mode="REQUIRED", 
                   description="Reference to quality rule"),
        SchemaField("validation_time", "TIMESTAMP", mode="REQUIRED", 
                   description="When validation was performed"),
        SchemaField("passed", "BOOLEAN", mode="REQUIRED", 
                   description="Whether validation passed"),
        SchemaField("validation_results", "JSON", mode="NULLABLE", 
                   description="Detailed validation results"),
        SchemaField("validation_metrics", "JSON", mode="NULLABLE", 
                   description="Metrics from the validation"),
        SchemaField("quality_score", "FLOAT", mode="NULLABLE", 
                   description="Quality score for this validation"),
        SchemaField("self_healing_applied", "BOOLEAN", mode="NULLABLE", 
                   description="Whether self-healing was applied"),
        SchemaField("self_healing_action_id", "STRING", mode="NULLABLE", 
                   description="ID of the self-healing action applied"),
        SchemaField("self_healing_success", "BOOLEAN", mode="NULLABLE", 
                   description="Whether self-healing was successful")
    ]


def define_issue_patterns_schema() -> list:
    """
    Defines the schema for the issue_patterns table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("pattern_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the pattern"),
        SchemaField("name", "STRING", mode="REQUIRED", 
                   description="Descriptive name of the pattern"),
        SchemaField("issue_type", "STRING", mode="REQUIRED", 
                   description="Type of issue this pattern detects"),
        SchemaField("description", "STRING", mode="NULLABLE", 
                   description="Detailed description of the pattern"),
        SchemaField("detection_pattern", "JSON", mode="REQUIRED", 
                   description="Pattern definition for detection"),
        SchemaField("confidence_threshold", "FLOAT", mode="REQUIRED", 
                   description="Minimum confidence threshold for detection"),
        SchemaField("occurrence_count", "INTEGER", mode="NULLABLE", 
                   description="Number of times this pattern was detected"),
        SchemaField("success_count", "INTEGER", mode="NULLABLE", 
                   description="Number of successful remediations"),
        SchemaField("success_rate", "FLOAT", mode="NULLABLE", 
                   description="Success rate of remediations"),
        SchemaField("is_active", "BOOLEAN", mode="REQUIRED", 
                   description="Whether this pattern is active"),
        SchemaField("last_seen", "TIMESTAMP", mode="NULLABLE", 
                   description="When this pattern was last detected"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this pattern was created"),
        SchemaField("created_by", "STRING", mode="REQUIRED", 
                   description="User who created this pattern"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When this pattern was last updated"),
        SchemaField("updated_by", "STRING", mode="NULLABLE", 
                   description="User who last updated this pattern")
    ]


def define_healing_actions_schema() -> list:
    """
    Defines the schema for the healing_actions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("action_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the action"),
        SchemaField("name", "STRING", mode="REQUIRED", 
                   description="Descriptive name of the action"),
        SchemaField("action_type", "STRING", mode="REQUIRED", 
                   description="Type of healing action (correction, retry, etc.)"),
        SchemaField("description", "STRING", mode="NULLABLE", 
                   description="Detailed description of the action"),
        SchemaField("pattern_id", "STRING", mode="REQUIRED", 
                   description="Pattern this action is associated with"),
        SchemaField("action_parameters", "JSON", mode="REQUIRED", 
                   description="Parameters for executing this action"),
        SchemaField("execution_count", "INTEGER", mode="NULLABLE", 
                   description="Number of times this action was executed"),
        SchemaField("success_count", "INTEGER", mode="NULLABLE", 
                   description="Number of successful executions"),
        SchemaField("success_rate", "FLOAT", mode="NULLABLE", 
                   description="Success rate of this action"),
        SchemaField("is_active", "BOOLEAN", mode="REQUIRED", 
                   description="Whether this action is active"),
        SchemaField("last_executed", "TIMESTAMP", mode="NULLABLE", 
                   description="When this action was last executed"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this action was created"),
        SchemaField("created_by", "STRING", mode="REQUIRED", 
                   description="User who created this action"),
        SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When this action was last updated"),
        SchemaField("updated_by", "STRING", mode="NULLABLE", 
                   description="User who last updated this action")
    ]


def define_healing_executions_schema() -> list:
    """
    Defines the schema for the healing_executions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("healing_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the healing execution"),
        SchemaField("execution_id", "STRING", mode="REQUIRED", 
                   description="Reference to pipeline execution"),
        SchemaField("validation_id", "STRING", mode="NULLABLE", 
                   description="Reference to quality validation (if applicable)"),
        SchemaField("pattern_id", "STRING", mode="REQUIRED", 
                   description="Pattern that was detected"),
        SchemaField("action_id", "STRING", mode="REQUIRED", 
                   description="Action that was applied"),
        SchemaField("issue_type", "STRING", mode="REQUIRED", 
                   description="Type of issue being healed"),
        SchemaField("action_taken", "STRING", mode="REQUIRED", 
                   description="Description of the action taken"),
        SchemaField("execution_time", "TIMESTAMP", mode="REQUIRED", 
                   description="When healing was executed"),
        SchemaField("successful", "BOOLEAN", mode="REQUIRED", 
                   description="Whether healing was successful"),
        SchemaField("execution_details", "JSON", mode="NULLABLE", 
                   description="Detailed information about execution"),
        SchemaField("confidence_score", "FLOAT", mode="NULLABLE", 
                   description="Confidence score for pattern detection"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When this record was created")
    ]


def define_pipeline_metrics_schema() -> list:
    """
    Defines the schema for the pipeline_metrics table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("metric_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the metric"),
        SchemaField("execution_id", "STRING", mode="REQUIRED", 
                   description="Reference to pipeline execution"),
        SchemaField("metric_category", "STRING", mode="REQUIRED", 
                   description="Category of metric (performance, quality, resource, etc.)"),
        SchemaField("metric_name", "STRING", mode="REQUIRED", 
                   description="Name of the metric"),
        SchemaField("metric_value", "FLOAT", mode="REQUIRED", 
                   description="Value of the metric"),
        SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED", 
                   description="When metric was collected"),
        SchemaField("metric_details", "JSON", mode="NULLABLE", 
                   description="Additional details about the metric")
    ]


def define_alerts_schema() -> list:
    """
    Defines the schema for the alerts table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        SchemaField("alert_id", "STRING", mode="REQUIRED", 
                   description="Unique identifier for the alert"),
        SchemaField("execution_id", "STRING", mode="NULLABLE", 
                   description="Reference to pipeline execution (if applicable)"),
        SchemaField("alert_type", "STRING", mode="REQUIRED", 
                   description="Type of alert (data quality, pipeline failure, etc.)"),
        SchemaField("severity", "STRING", mode="REQUIRED", 
                   description="Alert severity (CRITICAL, HIGH, MEDIUM, LOW)"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", 
                   description="When alert was created"),
        SchemaField("alert_details", "JSON", mode="NULLABLE", 
                   description="Detailed information about the alert"),
        SchemaField("acknowledged", "BOOLEAN", mode="REQUIRED", 
                   description="Whether alert has been acknowledged"),
        SchemaField("acknowledged_at", "TIMESTAMP", mode="NULLABLE", 
                   description="When alert was acknowledged"),
        SchemaField("acknowledged_by", "STRING", mode="NULLABLE", 
                   description="Who acknowledged the alert"),
        SchemaField("related_entity_id", "STRING", mode="NULLABLE", 
                   description="ID of entity related to this alert"),
        SchemaField("related_entity_type", "STRING", mode="NULLABLE", 
                   description="Type of entity related to this alert")
    ]


def get_bigquery_schema(table_name: str) -> typing.List[SchemaField]:
    """
    Retrieves the schema definition for a specific BigQuery table.
    
    Args:
        table_name: The name of the table to get the schema for
        
    Returns:
        List of SchemaField objects for the specified table
    """
    # Check if table_name is in our predefined tables
    if table_name not in BIGQUERY_TABLES:
        logger.warning(f"Requested schema for unknown table: {table_name}")
        return None
        
    # Call the appropriate schema definition function
    if table_name == "source_systems":
        return define_source_systems_schema()
    elif table_name == "pipeline_definitions":
        return define_pipeline_definitions_schema()
    elif table_name == "pipeline_executions":
        return define_pipeline_executions_schema()
    elif table_name == "task_executions":
        return define_task_executions_schema()
    elif table_name == "quality_rules":
        return define_quality_rules_schema()
    elif table_name == "quality_validations":
        return define_quality_validations_schema()
    elif table_name == "issue_patterns":
        return define_issue_patterns_schema()
    elif table_name == "healing_actions":
        return define_healing_actions_schema()
    elif table_name == "healing_executions":
        return define_healing_executions_schema()
    elif table_name == "pipeline_metrics":
        return define_pipeline_metrics_schema()
    elif table_name == "alerts":
        return define_alerts_schema()
    
    return None


def get_all_bigquery_schemas() -> typing.Dict[str, typing.List[SchemaField]]:
    """
    Retrieves schema definitions for all BigQuery tables.
    
    Returns:
        Dictionary mapping table names to their schema definitions
    """
    schemas = {}
    for table_name in BIGQUERY_TABLES.values():
        schema = get_bigquery_schema(table_name)
        if schema:
            schemas[table_name] = schema
    return schemas


def get_partitioning_config(table_name: str) -> typing.Dict:
    """
    Determines the appropriate partitioning configuration for a table.
    
    Args:
        table_name: The name of the table to get partitioning config for
        
    Returns:
        Partitioning configuration for the specified table
    """
    partition_config = None
    
    # Time-series tables with frequent updates
    if table_name == "pipeline_executions":
        partition_config = {
            "type": "time",
            "field": "start_time",
            "expiration_days": 90
        }
    elif table_name == "task_executions":
        partition_config = {
            "type": "time",
            "field": "start_time",
            "expiration_days": 30
        }
    elif table_name == "quality_validations":
        partition_config = {
            "type": "time",
            "field": "validation_time",
            "expiration_days": 90
        }
    elif table_name == "healing_executions":
        partition_config = {
            "type": "time",
            "field": "execution_time",
            "expiration_days": 90
        }
    elif table_name == "pipeline_metrics":
        partition_config = {
            "type": "time",
            "field": "collection_time",
            "expiration_days": 30
        }
    elif table_name == "alerts":
        partition_config = {
            "type": "time",
            "field": "created_at",
            "expiration_days": 90
        }
        
    return partition_config


def get_clustering_fields(table_name: str) -> typing.List[str]:
    """
    Determines the appropriate clustering fields for a table.
    
    Args:
        table_name: The name of the table to get clustering fields for
        
    Returns:
        List of field names to use for clustering
    """
    clustering_fields = None
    
    # Define clustering fields based on query patterns
    if table_name == "pipeline_executions":
        clustering_fields = ["pipeline_id", "status"]
    elif table_name == "task_executions":
        clustering_fields = ["execution_id", "task_type"]
    elif table_name == "quality_validations":
        clustering_fields = ["execution_id", "rule_id"]
    elif table_name == "healing_executions":
        clustering_fields = ["execution_id", "pattern_id"]
    elif table_name == "pipeline_metrics":
        clustering_fields = ["execution_id", "metric_category"]
    elif table_name == "alerts":
        clustering_fields = ["severity", "alert_type"]
        
    return clustering_fields


class SchemaRegistry:
    """
    Registry for database schemas with versioning and evolution capabilities.
    
    This class provides a central registry for all database schemas used in the
    application, including BigQuery tables and Firestore collections. It supports
    schema versioning, retrieval, and evolution to enable database migrations and
    schema management.
    """
    
    def __init__(self):
        """
        Initialize a new SchemaRegistry with the current schema version.
        """
        self._bigquery_schemas = get_all_bigquery_schemas()
        self._firestore_schemas = {}
        self._version = SCHEMA_VERSION
        logger.info(f"Initialized SchemaRegistry with version {self._version}")
        
    def get_bigquery_schema(self, table_name: str) -> typing.List[SchemaField]:
        """
        Get BigQuery schema for a specific table.
        
        Args:
            table_name: The name of the table to get the schema for
            
        Returns:
            List of SchemaField objects for the table
        """
        return self._bigquery_schemas.get(table_name)
        
    def get_firestore_schema(self, collection_name: str) -> FirestoreSchema:
        """
        Get Firestore schema for a specific collection.
        
        Args:
            collection_name: The name of the collection to get the schema for
            
        Returns:
            FirestoreSchema object for the collection
        """
        # Return cached schema if available
        if collection_name in self._firestore_schemas:
            return self._firestore_schemas[collection_name]
            
        # Create new schema based on predefined collection schemas
        schema = None
        if collection_name == "source_systems":
            schema = CollectionSchemas.pipeline_execution_schema()
        elif collection_name == "pipeline_definitions":
            schema = CollectionSchemas.pipeline_execution_schema()
        elif collection_name == "issue_patterns":
            schema = CollectionSchemas.issue_pattern_schema()
        elif collection_name == "healing_actions":
            schema = CollectionSchemas.healing_action_schema()
        elif collection_name == "configuration":
            schema = CollectionSchemas.configuration_schema()
            
        # Cache the schema if found
        if schema:
            self._firestore_schemas[collection_name] = schema
            
        return schema
        
    def get_partitioning_config(self, table_name: str) -> typing.Dict:
        """
        Get partitioning configuration for a BigQuery table.
        
        Args:
            table_name: The name of the table to get partitioning config for
            
        Returns:
            Partitioning configuration
        """
        return get_partitioning_config(table_name)
        
    def get_clustering_fields(self, table_name: str) -> typing.List[str]:
        """
        Get clustering fields for a BigQuery table.
        
        Args:
            table_name: The name of the table to get clustering fields for
            
        Returns:
            List of clustering field names
        """
        return get_clustering_fields(table_name)
        
    def get_version(self) -> str:
        """
        Get the current schema version.
        
        Returns:
            Schema version string
        """
        return self._version
        
    def register_custom_schema(self, name: str, schema: object, schema_type: str) -> bool:
        """
        Register a custom schema for a table or collection.
        
        Args:
            name: Name of the table or collection
            schema: Schema definition
            schema_type: Type of schema ('bigquery' or 'firestore')
            
        Returns:
            Success status
        """
        if schema_type not in ['bigquery', 'firestore']:
            logger.error(f"Invalid schema_type: {schema_type}")
            return False
            
        if schema_type == 'bigquery':
            if not isinstance(schema, list) or not all(isinstance(field, SchemaField) for field in schema):
                logger.error("BigQuery schema must be a list of SchemaField objects")
                return False
                
            self._bigquery_schemas[name] = schema
            logger.info(f"Registered custom BigQuery schema for {name}")
            return True
            
        elif schema_type == 'firestore':
            if not isinstance(schema, FirestoreSchema):
                logger.error("Firestore schema must be a FirestoreSchema object")
                return False
                
            self._firestore_schemas[name] = schema
            logger.info(f"Registered custom Firestore schema for {name}")
            return True
            
        return False