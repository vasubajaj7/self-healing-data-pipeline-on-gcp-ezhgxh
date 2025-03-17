"""
Data models and enumerations for the self-healing data pipeline API.

This module defines Pydantic models for core entities of the system such as:
- Source systems and connections
- Pipeline definitions and executions
- Task executions and status
- Data quality rules and validation results
- Self-healing patterns, actions, and execution records
- Metrics, alerts, and configuration settings

These models provide a standardized structure for data exchange between
API endpoints and underlying services, with built-in validation.

The models use Pydantic for validation, which provides:
- Type checking and coercion
- Complex validation rules via validators
- Clear error messages
- JSON Schema generation
"""

from pydantic import BaseModel, Field, validator, UUID4
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from enum import Enum

from ...constants import SelfHealingMode, HealingActionType, AlertSeverity, DataSourceType, FileFormat, QualityDimension


class SourceSystemType(Enum):
    """Enumeration of possible source system types."""
    GCS = "GCS"
    CLOUD_SQL = "CLOUD_SQL"
    BIGQUERY = "BIGQUERY"
    API = "API"
    SFTP = "SFTP"
    CUSTOM = "CUSTOM"


class ConnectionStatus(Enum):
    """Enumeration of possible connection statuses."""
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    ERROR = "ERROR"
    UNKNOWN = "UNKNOWN"


class PipelineStatus(Enum):
    """Enumeration of possible pipeline execution statuses."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    HEALING = "HEALING"


class TaskStatus(Enum):
    """Enumeration of possible task execution statuses."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"


class ValidationStatus(Enum):
    """Enumeration of possible data validation statuses."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


class HealingStatus(Enum):
    """Enumeration of possible healing execution statuses."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class MetricType(Enum):
    """Enumeration of possible metric types."""
    COUNTER = "COUNTER"
    GAUGE = "GAUGE"
    HISTOGRAM = "HISTOGRAM"
    SUMMARY = "SUMMARY"


class RuleType(Enum):
    """Enumeration of possible rule types."""
    THRESHOLD = "THRESHOLD"
    TREND = "TREND"
    ANOMALY = "ANOMALY"
    COMPOUND = "COMPOUND"
    EVENT = "EVENT"
    PATTERN = "PATTERN"


class SourceSystem(BaseModel):
    """Pydantic model for source system data."""
    source_id: str
    name: str
    source_type: SourceSystemType
    connection_details: Dict[str, Any]
    schema_version: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    schema_definition: Optional[Dict[str, Any]] = None
    extraction_settings: Optional[Dict[str, Any]] = None
    status: Optional[ConnectionStatus] = ConnectionStatus.UNKNOWN

    @validator('connection_details')
    def validate_connection_details(cls, v, values):
        """Validates that connection details are appropriate for the source type."""
        source_type = values.get('source_type')
        if source_type == SourceSystemType.GCS:
            if 'bucket_name' not in v or 'file_pattern' not in v:
                raise ValueError("GCS source requires 'bucket_name' and 'file_pattern' in connection details")
        elif source_type == SourceSystemType.CLOUD_SQL:
            required_fields = ['instance_name', 'database', 'credentials']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Cloud SQL source requires {required_fields} in connection details")
        elif source_type == SourceSystemType.API:
            required_fields = ['endpoint_url', 'auth_method']
            if not all(field in v for field in required_fields):
                raise ValueError(f"API source requires {required_fields} in connection details")
        elif source_type == SourceSystemType.CUSTOM:
            required_fields = ['connector_class', 'parameters']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Custom source requires {required_fields} in connection details")
        return v


class PipelineDefinition(BaseModel):
    """Pydantic model for pipeline definition data."""
    pipeline_id: str
    pipeline_name: str
    source_id: str
    target_dataset: str
    target_table: str
    dag_id: Optional[str] = None
    configuration: Dict[str, Any]
    description: Optional[str] = None
    is_active: Optional[bool] = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @validator('configuration')
    def validate_configuration(cls, v):
        """Validates that configuration contains required fields."""
        required_fields = ['schedule', 'extraction_params']
        if not all(field in v for field in required_fields):
            raise ValueError(f"Configuration must include {required_fields}")
        
        # Validate schedule format - assuming cron string
        schedule = v.get('schedule')
        if schedule and not isinstance(schedule, str):
            raise ValueError("Schedule must be a string (cron format)")
        
        return v


class PipelineExecution(BaseModel):
    """Pydantic model for pipeline execution data."""
    execution_id: str
    pipeline_id: str
    status: PipelineStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    dag_run_id: Optional[str] = None
    records_processed: Optional[int] = None
    execution_params: Optional[Dict[str, Any]] = None
    error_details: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None
    task_ids: Optional[List[str]] = None


class TaskExecution(BaseModel):
    """Pydantic model for task execution data."""
    task_id: str
    execution_id: str
    task_name: str
    task_type: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    task_params: Optional[Dict[str, Any]] = None
    error_details: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None
    log_url: Optional[str] = None


class QualityRule(BaseModel):
    """Pydantic model for data quality rule."""
    rule_id: str
    rule_name: str
    target_dataset: str
    target_table: str
    rule_type: str
    expectation_type: str
    rule_definition: Dict[str, Any]
    severity: AlertSeverity
    is_active: Optional[bool] = True
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @validator('rule_definition')
    def validate_rule_definition(cls, v, values):
        """Validates that rule definition contains required fields for the expectation type."""
        expectation_type = values.get('expectation_type')
        if not expectation_type:
            return v
        
        # Different validation based on expectation type
        if expectation_type == 'expect_column_values_to_not_be_null':
            if 'column' not in v:
                raise ValueError(f"Rule definition for {expectation_type} must include 'column'")
        elif expectation_type == 'expect_column_values_to_be_between':
            required_fields = ['column', 'min_value', 'max_value']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Rule definition for {expectation_type} must include {required_fields}")
        elif expectation_type == 'expect_column_values_to_match_regex':
            required_fields = ['column', 'regex']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Rule definition for {expectation_type} must include {required_fields}")
        
        return v


class QualityValidation(BaseModel):
    """Pydantic model for data quality validation result."""
    validation_id: str
    rule_id: str
    execution_id: str
    status: ValidationStatus
    validation_time: datetime
    success_percent: Optional[float] = None
    records_validated: Optional[int] = None
    records_failed: Optional[int] = None
    validation_results: Optional[Dict[str, Any]] = None
    error_details: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class IssuePattern(BaseModel):
    """Pydantic model for issue pattern definition."""
    pattern_id: str
    issue_type: str
    detection_pattern: Dict[str, Any]
    confidence_threshold: float
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @validator('confidence_threshold')
    def validate_confidence_threshold(cls, v):
        """Validates that confidence threshold is between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError("Confidence threshold must be between 0 and 1")
        return v


class HealingAction(BaseModel):
    """Pydantic model for healing action definition."""
    action_id: str
    pattern_id: str
    action_type: HealingActionType
    action_definition: Dict[str, Any]
    is_active: Optional[bool] = True
    success_rate: Optional[float] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @validator('action_definition')
    def validate_action_definition(cls, v, values):
        """Validates that action definition contains required fields for the action type."""
        action_type = values.get('action_type')
        if not action_type:
            return v
        
        if action_type == HealingActionType.DATA_CORRECTION:
            required_fields = ['correction_type', 'parameters']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        elif action_type == HealingActionType.PIPELINE_RETRY:
            required_fields = ['max_retries', 'backoff_factor']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
            required_fields = ['parameters_to_adjust', 'adjustment_strategy']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        elif action_type == HealingActionType.RESOURCE_SCALING:
            required_fields = ['resource_type', 'scaling_factor']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        elif action_type == HealingActionType.SCHEMA_EVOLUTION:
            required_fields = ['schema_changes', 'compatibility_mode']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        elif action_type == HealingActionType.DEPENDENCY_RESOLUTION:
            required_fields = ['dependency_type', 'resolution_strategy']
            if not all(field in v for field in required_fields):
                raise ValueError(f"Action definition for {action_type} must include {required_fields}")
        
        return v


class HealingExecution(BaseModel):
    """Pydantic model for healing execution data."""
    healing_id: str
    execution_id: str
    validation_id: Optional[str] = None
    pattern_id: str
    action_id: str
    status: HealingStatus
    execution_time: datetime
    completion_time: Optional[datetime] = None
    confidence_score: Optional[float] = None
    successful: bool
    execution_details: Optional[Dict[str, Any]] = None
    error_details: Optional[Dict[str, Any]] = None
    approved_by: Optional[str] = None
    approval_time: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class PipelineMetric(BaseModel):
    """Pydantic model for pipeline metric data."""
    metric_id: str
    pipeline_id: str
    execution_id: Optional[str] = None
    metric_name: str
    metric_type: MetricType
    metric_value: float
    collection_time: datetime
    metric_unit: Optional[str] = None
    dimensions: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class Alert(BaseModel):
    """Pydantic model for alert data."""
    alert_id: str
    alert_type: str
    severity: AlertSeverity
    message: str
    created_at: datetime
    source_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    execution_id: Optional[str] = None
    status: Optional[str] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    context: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class HealingConfig(BaseModel):
    """Pydantic model for global healing configuration."""
    healing_mode: SelfHealingMode
    global_confidence_threshold: float
    max_retry_attempts: int
    approval_required_high_impact: bool
    learning_mode_active: bool
    additional_settings: Optional[Dict[str, Any]] = None

    @validator('global_confidence_threshold')
    def validate_global_confidence_threshold(cls, v):
        """Validates that global confidence threshold is between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError("Global confidence threshold must be between 0 and 1")
        return v

    @validator('max_retry_attempts')
    def validate_max_retry_attempts(cls, v):
        """Validates that max retry attempts is a positive integer."""
        if v < 0:
            raise ValueError("Max retry attempts must be a positive integer")
        return v


class AlertConfig(BaseModel):
    """Pydantic model for alert configuration."""
    teams_webhook_url: Dict[str, Any]
    email_config: Dict[str, Any]
    alert_thresholds: Dict[str, Any]
    enabled_channels: Dict[str, bool]


class OptimizationConfig(BaseModel):
    """Pydantic model for optimization configuration."""
    query_optimization_settings: Dict[str, Any]
    schema_optimization_settings: Dict[str, Any]
    resource_optimization_settings: Dict[str, Any]
    auto_implementation_enabled: bool


class OptimizationRecommendation(BaseModel):
    """Pydantic model for optimization recommendation."""
    recommendation_id: str
    optimization_type: str
    target_resource: str
    recommendations: List[Dict[str, Any]]
    impact_assessment: Dict[str, Any]
    implementation_script: Optional[str] = None
    created_at: Optional[datetime] = None
    status: Optional[str] = None
    implemented_at: Optional[datetime] = None
    implemented_by: Optional[str] = None
    implementation_results: Optional[Dict[str, Any]] = None