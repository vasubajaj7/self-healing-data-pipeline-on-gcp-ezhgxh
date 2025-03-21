"""
Initializes the database package for the self-healing data pipeline.
This module provides a unified interface for database operations in the self-healing data pipeline. 
This module exports models, repositories, schema utilities, and migration tools for working with BigQuery and Firestore databases.
"""

# Import database model classes and constants
from .models import (
    Alert,
    AlertNotification,
    PipelineExecution,
    PipelineStatus,
    QualityRule,
    HealingAction,
    HealingExecution,
    IssuePattern,
    PipelineDefinition,
    PipelineMetric,
    QualityValidation,
    SourceSystem,
    TaskExecution,
    ALERT_TABLE_NAME,
    ALERT_STATUS_NEW,
    ALERT_STATUS_ACKNOWLEDGED,
    ALERT_STATUS_RESOLVED,
    ALERT_STATUS_SUPPRESSED,
    PIPELINE_EXECUTION_TABLE_NAME,
    QUALITY_RULE_TABLE_NAME,
    HEALING_ACTION_TABLE_NAME,
    HEALING_EXECUTION_TABLE_NAME,
    ISSUE_PATTERN_TABLE_NAME,
    PIPELINE_DEFINITION_TABLE_NAME,
    PIPELINE_METRIC_TABLE_NAME,
    QUALITY_VALIDATION_TABLE_NAME,
    SOURCE_SYSTEM_TABLE_NAME,
    TASK_EXECUTION_TABLE_NAME,
    generate_alert_id,
    generate_execution_id,
    generate_rule_id,
    generate_healing_id,
    generate_pattern_id,
    generate_pipeline_id,
    generate_metric_id,
    generate_validation_id,
    generate_source_id,
    generate_task_id,
    get_alert_table_schema,
    get_pipeline_execution_table_schema,
    get_quality_rule_table_schema,
    get_healing_action_table_schema,
    get_healing_execution_table_schema,
    get_issue_pattern_table_schema,
    get_pipeline_definition_table_schema,
    get_pipeline_metric_table_schema,
    get_quality_validation_table_schema,
    get_source_system_table_schema,
    get_task_execution_table_schema,
    create_healing_action,
    get_healing_action,
    get_healing_actions_by_type,
    get_healing_actions_by_pattern,
    update_healing_action,
    delete_healing_action,
)

# Import repository classes for database operations
from .repositories import (
    AlertRepository,
    ExecutionRepository,
    HealingRepository,
    MetricsRepository,
    PipelineRepository,
    QualityRepository,
    SourceRepository,
)

# Import schema utilities for BigQuery and Firestore
from .schema import (
    BigQuerySchemaField,
    BigQuerySchemaManager,
    FirestoreSchema,
    FirestoreSchemaManager,
    CollectionSchemas,
    bq_get_schema_field,
    bq_schema_to_json,
    bq_json_to_schema,
    bq_merge_schemas,
    bq_validate_schema_compatibility,
    bq_get_field_by_name,
    bq_create_schema_from_dict,
    bq_infer_schema_from_data,
    bq_compare_schemas,
    fs_get_schema_field,
    fs_schema_to_json,
    fs_json_to_schema,
    fs_validate_document,
    fs_create_schema_from_dict,
    fs_infer_schema_from_document,
    fs_merge_schemas,
    fs_validate_schema_compatibility,
    fs_get_field_by_name,
)

# Import database migration and initialization utilities
from .migrations import initialize_database, MIGRATION_VERSION, SCHEMA_VERSION

# Configure logging for the database package
from ..utils.logging.logger import get_logger

# Initialize logger for this module
logger = get_logger(__name__)

# Get the current schema version
VERSION = SCHEMA_VERSION

# Define the public interface for this module
__all__ = [
    "models",
    "repositories",
    "schema",
    "migrations",
    "initialize_database",
    "BigQuerySchemaField",
    "BigQuerySchemaManager",
    "FirestoreSchema",
    FirestoreSchemaManager",
    "CollectionSchemas",
    "bq_get_schema_field",
    "bq_schema_to_json",
    "bq_json_to_schema",
    "bq_merge_schemas",
    "bq_validate_schema_compatibility",
    "bq_get_field_by_name",
    "bq_create_schema_from_dict",
    "bq_infer_schema_from_data",
    "bq_compare_schemas",
    "fs_get_schema_field",
    "fs_schema_to_json",
    "fs_json_to_schema",
    "fs_validate_document",
    "fs_create_schema_from_dict",
    "fs_infer_schema_from_document",
    "fs_merge_schemas",
    "fs_validate_schema_compatibility",
    "fs_get_field_by_name",
    "Alert",
    "AlertNotification",
    "PipelineExecution",
    "PipelineStatus",
    "QualityRule",
    "HealingAction",
    "HealingExecution",
    "IssuePattern",
    "PipelineDefinition",
    "PipelineMetric",
    "QualityValidation",
    "SourceSystem",
    "TaskExecution",
    "AlertRepository",
    "ExecutionRepository",
    "HealingRepository",
    "MetricsRepository",
    "PipelineRepository",
    "QualityRepository",
    "SourceRepository",
    "ALERT_TABLE_NAME",
    "ALERT_STATUS_NEW",
    "ALERT_STATUS_ACKNOWLEDGED",
    "ALERT_STATUS_RESOLVED",
    "ALERT_STATUS_SUPPRESSED",
    "PIPELINE_EXECUTION_TABLE_NAME",
    "QUALITY_RULE_TABLE_NAME",
    "HEALING_ACTION_TABLE_NAME",
    "HEALING_EXECUTION_TABLE_NAME",
    "ISSUE_PATTERN_TABLE_NAME",
    "PIPELINE_DEFINITION_TABLE_NAME",
    "PIPELINE_METRIC_TABLE_NAME",
    "QUALITY_VALIDATION_TABLE_NAME",
    "SOURCE_SYSTEM_TABLE_NAME",
    "TASK_EXECUTION_TABLE_NAME",
    "generate_alert_id",
    "generate_execution_id",
    "generate_rule_id",
    "generate_healing_id",
    "generate_pattern_id",
    "generate_pipeline_id",
    "generate_metric_id",
    "generate_validation_id",
    "generate_source_id",
    "generate_task_id",
    "get_alert_table_schema",
    "get_pipeline_execution_table_schema",
    "get_quality_rule_table_schema",
    "get_healing_action_table_schema",
    "get_healing_execution_table_schema",
    "get_issue_pattern_table_schema",
    "get_pipeline_definition_table_schema",
    "get_pipeline_metric_table_schema",
    "get_quality_validation_table_schema",
    "get_source_system_table_schema",
    "get_task_execution_table_schema",
    "create_healing_action",
    "get_healing_action",
    "get_healing_actions_by_type",
    "get_healing_actions_by_pattern",
    "update_healing_action",
    "delete_healing_action",
    "VERSION",
    "MIGRATION_VERSION",
    "SCHEMA_VERSION",
]