"""
Pydantic models for API request validation and data parsing in the self-healing data pipeline.

These models ensure consistent request handling, validation, and documentation across all API endpoints.
"""

from datetime import datetime
from typing import List, Dict, Optional, Any, Union

from pydantic import BaseModel, Field, validator, UUID4  # pydantic v1.9.0

from ...constants import (
    SelfHealingMode, 
    HealingActionType, 
    AlertSeverity, 
    DataSourceType,
    FileFormat,
    QualityDimension
)


class PaginationParams(BaseModel):
    """Common pagination parameters for list endpoints."""
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(20, ge=1, le=100, description="Number of items per page")
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    descending: Optional[bool] = Field(False, description="Sort in descending order if true")
    
    @validator('page', 'page_size', pre=True)
    def validate_pagination(cls, v, values):
        if isinstance(v, str):
            try:
                v = int(v)
            except ValueError:
                raise ValueError(f"Invalid pagination parameter, must be an integer")
        
        if 'page' in values and v < 1:
            raise ValueError("Page must be at least 1")
        
        if 'page_size' in values and (v < 1 or v > 100):
            raise ValueError("Page size must be between 1 and 100")
            
        return v


class DateRangeParams(BaseModel):
    """Date range parameters for filtering time-based data."""
    start_date: Optional[datetime] = Field(None, description="Start date for filtering")
    end_date: Optional[datetime] = Field(None, description="End date for filtering")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        start_date = values.get('start_date')
        if start_date and v and start_date > v:
            raise ValueError("Start date must be before end date")
        return v


class SourceSystemCreateRequest(BaseModel):
    """Request model for creating a new data source system."""
    name: str = Field(..., min_length=1, max_length=100, description="Name of the source system")
    source_type: DataSourceType = Field(..., description="Type of data source")
    connection_details: Dict[str, Any] = Field(..., description="Connection details specific to the source type")
    schema_version: Optional[str] = Field(None, description="Schema version identifier")
    description: Optional[str] = Field(None, description="Description of the source system")
    is_active: Optional[bool] = Field(True, description="Whether the source is active")
    schema_definition: Optional[Dict[str, Any]] = Field(None, description="Schema definition for the source data")
    extraction_settings: Optional[Dict[str, Any]] = Field(None, description="Settings for data extraction")
    
    @validator('connection_details')
    def validate_connection_details(cls, v, values):
        source_type = values.get('source_type')
        if not source_type:
            return v
            
        if source_type == DataSourceType.GCS:
            if 'bucket_name' not in v:
                raise ValueError("GCS connection requires 'bucket_name'")
            if 'file_pattern' not in v:
                raise ValueError("GCS connection requires 'file_pattern'")
                
        elif source_type == DataSourceType.CLOUD_SQL:
            required_fields = ['instance_name', 'database', 'credentials']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Cloud SQL connection requires {', '.join(missing)}")
                
        elif source_type == DataSourceType.API:
            if 'endpoint_url' not in v:
                raise ValueError("API connection requires 'endpoint_url'")
            if 'auth_method' not in v:
                raise ValueError("API connection requires 'auth_method'")
            if 'headers' not in v:
                raise ValueError("API connection requires 'headers'")
                
        elif source_type == DataSourceType.CUSTOM:
            if 'connector_class' not in v:
                raise ValueError("Custom connection requires 'connector_class'")
            if 'parameters' not in v:
                raise ValueError("Custom connection requires 'parameters'")
                
        return v


class SourceSystemUpdateRequest(BaseModel):
    """Request model for updating an existing data source system."""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Name of the source system")
    connection_details: Optional[Dict[str, Any]] = Field(None, description="Connection details specific to the source type")
    schema_version: Optional[str] = Field(None, description="Schema version identifier")
    description: Optional[str] = Field(None, description="Description of the source system")
    is_active: Optional[bool] = Field(None, description="Whether the source is active")
    schema_definition: Optional[Dict[str, Any]] = Field(None, description="Schema definition for the source data")
    extraction_settings: Optional[Dict[str, Any]] = Field(None, description="Settings for data extraction")


class SourceSystemTestRequest(BaseModel):
    """Request model for testing a data source connection."""
    source_type: DataSourceType = Field(..., description="Type of data source")
    connection_details: Dict[str, Any] = Field(..., description="Connection details specific to the source type")
    test_parameters: Optional[Dict[str, Any]] = Field(None, description="Additional parameters for testing")
    
    @validator('connection_details')
    def validate_connection_details(cls, v, values):
        source_type = values.get('source_type')
        if not source_type:
            return v
            
        if source_type == DataSourceType.GCS:
            if 'bucket_name' not in v:
                raise ValueError("GCS connection requires 'bucket_name'")
            if 'file_pattern' not in v:
                raise ValueError("GCS connection requires 'file_pattern'")
                
        elif source_type == DataSourceType.CLOUD_SQL:
            required_fields = ['instance_name', 'database', 'credentials']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Cloud SQL connection requires {', '.join(missing)}")
                
        elif source_type == DataSourceType.API:
            if 'endpoint_url' not in v:
                raise ValueError("API connection requires 'endpoint_url'")
            if 'auth_method' not in v:
                raise ValueError("API connection requires 'auth_method'")
            if 'headers' not in v:
                raise ValueError("API connection requires 'headers'")
                
        elif source_type == DataSourceType.CUSTOM:
            if 'connector_class' not in v:
                raise ValueError("Custom connection requires 'connector_class'")
            if 'parameters' not in v:
                raise ValueError("Custom connection requires 'parameters'")
                
        return v


class PipelineCreateRequest(BaseModel):
    """Request model for creating a new pipeline definition."""
    pipeline_name: str = Field(..., min_length=1, max_length=100, description="Name of the pipeline")
    source_id: str = Field(..., description="ID of the source system")
    target_dataset: str = Field(..., description="Target BigQuery dataset")
    target_table: str = Field(..., description="Target BigQuery table")
    configuration: Dict[str, Any] = Field(..., description="Pipeline configuration parameters")
    description: Optional[str] = Field(None, description="Description of the pipeline")
    is_active: Optional[bool] = Field(True, description="Whether the pipeline is active")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('configuration')
    def validate_configuration(cls, v):
        if 'schedule' not in v:
            raise ValueError("Configuration must include 'schedule'")
        
        if 'extraction_params' not in v:
            raise ValueError("Configuration must include 'extraction_params'")
            
        # Validate schedule format
        schedule = v.get('schedule')
        if not isinstance(schedule, str) and not isinstance(schedule, dict):
            raise ValueError("Schedule must be a string (cron format) or a dictionary")
            
        # Validate extraction_params structure
        extraction_params = v.get('extraction_params')
        if not isinstance(extraction_params, dict):
            raise ValueError("Extraction parameters must be a dictionary")
            
        return v


class PipelineUpdateRequest(BaseModel):
    """Request model for updating an existing pipeline definition."""
    pipeline_name: Optional[str] = Field(None, min_length=1, max_length=100, description="Name of the pipeline")
    target_dataset: Optional[str] = Field(None, description="Target BigQuery dataset")
    target_table: Optional[str] = Field(None, description="Target BigQuery table")
    configuration: Optional[Dict[str, Any]] = Field(None, description="Pipeline configuration parameters")
    description: Optional[str] = Field(None, description="Description of the pipeline")
    is_active: Optional[bool] = Field(None, description="Whether the pipeline is active")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('configuration')
    def validate_configuration(cls, v):
        if v is None:
            return v
            
        if 'schedule' not in v:
            raise ValueError("Configuration must include 'schedule'")
        
        if 'extraction_params' not in v:
            raise ValueError("Configuration must include 'extraction_params'")
            
        # Validate schedule format
        schedule = v.get('schedule')
        if not isinstance(schedule, str) and not isinstance(schedule, dict):
            raise ValueError("Schedule must be a string (cron format) or a dictionary")
            
        # Validate extraction_params structure
        extraction_params = v.get('extraction_params')
        if not isinstance(extraction_params, dict):
            raise ValueError("Extraction parameters must be a dictionary")
            
        return v


class PipelineExecuteRequest(BaseModel):
    """Request model for executing a pipeline."""
    execution_params: Optional[Dict[str, Any]] = Field(None, description="Override parameters for this execution")
    async_execution: Optional[bool] = Field(True, description="Execute asynchronously if true")
    force_execution: Optional[bool] = Field(False, description="Execute even if pipeline is inactive")


class QualityRuleCreateRequest(BaseModel):
    """Request model for creating a new quality rule."""
    rule_name: str = Field(..., min_length=1, max_length=100, description="Name of the quality rule")
    target_dataset: str = Field(..., description="Target BigQuery dataset")
    target_table: str = Field(..., description="Target BigQuery table")
    rule_type: str = Field(..., description="Type of rule (schema, content, relationship, etc.)")
    expectation_type: str = Field(..., description="Great Expectations expectation type")
    rule_definition: Dict[str, Any] = Field(..., description="Rule definition parameters")
    severity: AlertSeverity = Field(..., description="Severity if rule fails")
    is_active: Optional[bool] = Field(True, description="Whether the rule is active")
    description: Optional[str] = Field(None, description="Description of the rule")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('rule_definition')
    def validate_rule_definition(cls, v, values):
        expectation_type = values.get('expectation_type')
        if not expectation_type:
            return v
            
        # Validation logic depends on expectation_type
        if expectation_type == 'expect_column_values_to_not_be_null':
            if 'column' not in v:
                raise ValueError("Rule definition must include 'column' for not-null expectation")
                
        elif expectation_type == 'expect_column_values_to_be_between':
            required_fields = ['column', 'min_value', 'max_value']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Rule definition requires {', '.join(missing)} for range expectation")
                
        return v


class QualityRuleUpdateRequest(BaseModel):
    """Request model for updating an existing quality rule."""
    rule_name: Optional[str] = Field(None, min_length=1, max_length=100, description="Name of the quality rule")
    rule_type: Optional[str] = Field(None, description="Type of rule (schema, content, relationship, etc.)")
    expectation_type: Optional[str] = Field(None, description="Great Expectations expectation type")
    rule_definition: Optional[Dict[str, Any]] = Field(None, description="Rule definition parameters")
    severity: Optional[AlertSeverity] = Field(None, description="Severity if rule fails")
    is_active: Optional[bool] = Field(None, description="Whether the rule is active")
    description: Optional[str] = Field(None, description="Description of the rule")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('rule_definition')
    def validate_rule_definition(cls, v, values):
        if v is None:
            return v
            
        expectation_type = values.get('expectation_type')
        if not expectation_type:
            # If no expectation_type is provided, we can't validate
            # the rule definition properly
            return v
            
        # Validation logic depends on expectation_type
        if expectation_type == 'expect_column_values_to_not_be_null':
            if 'column' not in v:
                raise ValueError("Rule definition must include 'column' for not-null expectation")
                
        elif expectation_type == 'expect_column_values_to_be_between':
            required_fields = ['column', 'min_value', 'max_value']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Rule definition requires {', '.join(missing)} for range expectation")
                
        return v


class HealingPatternCreateRequest(BaseModel):
    """Request model for creating a new healing pattern."""
    issue_type: str = Field(..., description="Type of issue this pattern detects")
    detection_pattern: Dict[str, Any] = Field(..., description="Pattern for issue detection")
    confidence_threshold: float = Field(..., ge=0.0, le=1.0, description="Minimum confidence threshold for pattern matching")
    description: Optional[str] = Field(None, description="Description of the pattern")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('confidence_threshold')
    def validate_confidence_threshold(cls, v):
        if v < 0.0 or v > 1.0:
            raise ValueError("Confidence threshold must be between 0 and 1")
        return v
        
    @validator('detection_pattern')
    def validate_detection_pattern(cls, v, values):
        issue_type = values.get('issue_type')
        if not issue_type:
            return v
            
        # Validation logic depends on issue_type
        if issue_type == 'data_quality':
            if 'quality_dimension' not in v:
                raise ValueError("Detection pattern for data quality issues must include 'quality_dimension'")
                
        elif issue_type == 'pipeline_failure':
            if 'error_pattern' not in v:
                raise ValueError("Detection pattern for pipeline failures must include 'error_pattern'")
                
        return v


class HealingPatternUpdateRequest(BaseModel):
    """Request model for updating an existing healing pattern."""
    issue_type: Optional[str] = Field(None, description="Type of issue this pattern detects")
    detection_pattern: Optional[Dict[str, Any]] = Field(None, description="Pattern for issue detection")
    confidence_threshold: Optional[float] = Field(None, ge=0.0, le=1.0, description="Minimum confidence threshold for pattern matching")
    description: Optional[str] = Field(None, description="Description of the pattern")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('confidence_threshold')
    def validate_confidence_threshold(cls, v):
        if v is None:
            return v
            
        if v < 0.0 or v > 1.0:
            raise ValueError("Confidence threshold must be between 0 and 1")
        return v


class HealingActionCreateRequest(BaseModel):
    """Request model for creating a new healing action."""
    pattern_id: str = Field(..., description="ID of the healing pattern this action is associated with")
    action_type: HealingActionType = Field(..., description="Type of healing action")
    action_definition: Dict[str, Any] = Field(..., description="Definition of the healing action")
    is_active: Optional[bool] = Field(True, description="Whether the action is active")
    description: Optional[str] = Field(None, description="Description of the healing action")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('action_definition')
    def validate_action_definition(cls, v, values):
        action_type = values.get('action_type')
        if not action_type:
            return v
            
        # Validation logic depends on action_type
        if action_type == HealingActionType.DATA_CORRECTION:
            required_fields = ['correction_type', 'parameters']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for data correction")
                
        elif action_type == HealingActionType.PIPELINE_RETRY:
            required_fields = ['max_retries', 'backoff_factor']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for pipeline retry")
                
        elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
            required_fields = ['parameters_to_adjust', 'adjustment_strategy']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for parameter adjustment")
                
        elif action_type == HealingActionType.RESOURCE_SCALING:
            required_fields = ['resource_type', 'scaling_factor']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for resource scaling")
                
        elif action_type == HealingActionType.SCHEMA_EVOLUTION:
            required_fields = ['schema_changes', 'compatibility_mode']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for schema evolution")
                
        elif action_type == HealingActionType.DEPENDENCY_RESOLUTION:
            required_fields = ['dependency_type', 'resolution_strategy']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for dependency resolution")
                
        return v


class HealingActionUpdateRequest(BaseModel):
    """Request model for updating an existing healing action."""
    action_type: Optional[HealingActionType] = Field(None, description="Type of healing action")
    action_definition: Optional[Dict[str, Any]] = Field(None, description="Definition of the healing action")
    is_active: Optional[bool] = Field(None, description="Whether the action is active")
    description: Optional[str] = Field(None, description="Description of the healing action")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('action_definition')
    def validate_action_definition(cls, v, values):
        if v is None:
            return v
            
        action_type = values.get('action_type')
        if not action_type:
            # If no action_type is provided, we can't validate
            # the action definition properly
            return v
            
        # Validation logic similar to HealingActionCreateRequest
        if action_type == HealingActionType.DATA_CORRECTION:
            required_fields = ['correction_type', 'parameters']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for data correction")
                
        elif action_type == HealingActionType.PIPELINE_RETRY:
            required_fields = ['max_retries', 'backoff_factor']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for pipeline retry")
                
        elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
            required_fields = ['parameters_to_adjust', 'adjustment_strategy']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for parameter adjustment")
                
        elif action_type == HealingActionType.RESOURCE_SCALING:
            required_fields = ['resource_type', 'scaling_factor']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for resource scaling")
                
        elif action_type == HealingActionType.SCHEMA_EVOLUTION:
            required_fields = ['schema_changes', 'compatibility_mode']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for schema evolution")
                
        elif action_type == HealingActionType.DEPENDENCY_RESOLUTION:
            required_fields = ['dependency_type', 'resolution_strategy']
            missing = [f for f in required_fields if f not in v]
            if missing:
                raise ValueError(f"Action definition requires {', '.join(missing)} for dependency resolution")
                
        return v


class HealingConfigUpdateRequest(BaseModel):
    """Request model for updating the global healing configuration."""
    healing_mode: SelfHealingMode = Field(..., description="Operating mode for self-healing")
    global_confidence_threshold: float = Field(..., ge=0.0, le=1.0, description="Global minimum confidence threshold")
    max_retry_attempts: int = Field(..., ge=1, description="Maximum number of retry attempts")
    approval_required_high_impact: bool = Field(..., description="Whether high-impact actions require approval")
    learning_mode_active: bool = Field(..., description="Whether learning mode is active")
    additional_settings: Optional[Dict[str, Any]] = Field(None, description="Additional settings")
    
    @validator('global_confidence_threshold')
    def validate_global_confidence_threshold(cls, v):
        if v < 0.0 or v > 1.0:
            raise ValueError("Global confidence threshold must be between 0 and 1")
        return v
        
    @validator('max_retry_attempts')
    def validate_max_retry_attempts(cls, v):
        if v < 1:
            raise ValueError("Maximum retry attempts must be at least 1")
        return v


class ManualHealingRequest(BaseModel):
    """Request model for manually triggering a healing action."""
    issue_id: str = Field(..., description="ID of the issue to heal")
    action_id: str = Field(..., description="ID of the healing action to apply")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Override parameters for the healing action")
    notes: Optional[str] = Field(None, description="Notes about this manual healing action")


class AlertConfigUpdateRequest(BaseModel):
    """Request model for updating alert configuration."""
    teams_webhook_url: Dict[str, Any] = Field(..., description="Microsoft Teams webhook configuration")
    email_config: Dict[str, Any] = Field(..., description="Email notification configuration")
    alert_thresholds: Dict[str, Any] = Field(..., description="Alert threshold configuration")
    enabled_channels: Dict[str, bool] = Field(..., description="Enabled notification channels")
    
    @validator('teams_webhook_url')
    def validate_teams_webhook_url(cls, v):
        if 'webhook_url' not in v:
            raise ValueError("Teams webhook configuration must include 'webhook_url'")
        
        webhook_url = v.get('webhook_url')
        if not webhook_url or not webhook_url.startswith('https://'):
            raise ValueError("Teams webhook URL must be a valid HTTPS URL")
            
        return v
        
    @validator('email_config')
    def validate_email_config(cls, v):
        required_fields = ['smtp_server', 'sender_email', 'recipients']
        missing = [f for f in required_fields if f not in v]
        if missing:
            raise ValueError(f"Email configuration requires {', '.join(missing)}")
            
        return v


class AlertAcknowledgeRequest(BaseModel):
    """Request model for acknowledging an alert."""
    notes: Optional[str] = Field(None, description="Notes about the acknowledgment")
    suppress_similar: Optional[bool] = Field(False, description="Whether to suppress similar alerts")


class OptimizationConfigUpdateRequest(BaseModel):
    """Request model for updating optimization configuration."""
    query_optimization_settings: Dict[str, Any] = Field(..., description="Query optimization settings")
    schema_optimization_settings: Dict[str, Any] = Field(..., description="Schema optimization settings")
    resource_optimization_settings: Dict[str, Any] = Field(..., description="Resource optimization settings")
    auto_implementation_enabled: bool = Field(..., description="Whether automatic implementation is enabled")