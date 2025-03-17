"""
Initializes the database models package for the self-healing data pipeline.
This file imports and exposes all model classes, constants, and utility functions from individual model files,
providing a centralized access point for database models throughout the application.
"""

from .alert import (
    Alert,
    AlertNotification,
    ALERT_TABLE_NAME,
    ALERT_STATUS_NEW,
    ALERT_STATUS_ACKNOWLEDGED,
    ALERT_STATUS_RESOLVED,
    ALERT_STATUS_SUPPRESSED,
    get_alert_table_schema,
    generate_alert_id,
)
from .pipeline_execution import (
    PipelineExecution,
    PipelineStatus,
    PIPELINE_EXECUTION_TABLE_NAME,
    generate_execution_id,
    get_pipeline_execution_table_schema,
)
from .quality_rule import (
    QualityRule,
    QUALITY_RULE_TABLE_NAME,
    generate_rule_id,
    get_quality_rule_table_schema,
)
from .healing_action import (
    HealingAction,
    HEALING_ACTION_TABLE_NAME,
    create_healing_action,
    get_healing_action,
    get_healing_actions_by_type,
    get_healing_actions_by_pattern,
    update_healing_action,
    delete_healing_action,
    get_healing_action_table_schema,
)
from .healing_execution import (
    HealingExecution,
    HEALING_EXECUTION_TABLE_NAME,
    generate_healing_id,
    get_healing_execution_table_schema,
)
from .issue_pattern import (
    IssuePattern,
    ISSUE_PATTERN_TABLE_NAME,
    generate_pattern_id,
    get_issue_pattern_table_schema,
)
from .pipeline_definition import (
    PipelineDefinition,
    PIPELINE_DEFINITION_TABLE_NAME,
    generate_pipeline_id,
    get_pipeline_definition_table_schema,
)
from .pipeline_metric import (
    PipelineMetric,
    PIPELINE_METRIC_TABLE_NAME,
    generate_metric_id,
    get_pipeline_metric_table_schema,
)
from .quality_validation import (
    QualityValidation,
    QUALITY_VALIDATION_TABLE_NAME,
    generate_validation_id,
    get_quality_validation_table_schema,
)
from .source_system import (
    SourceSystem,
    SOURCE_SYSTEM_TABLE_NAME,
    generate_source_id,
    get_source_system_table_schema,
)
from .task_execution import (
    TaskExecution,
    TASK_EXECUTION_TABLE_NAME,
    generate_task_id,
    get_task_execution_table_schema,
)

__all__ = [
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
]