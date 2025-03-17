"""
Standardized response models for the self-healing data pipeline API.

This module defines consistent response structures for all API endpoints,
ensuring standardized formatting, pagination, and error handling throughout
the application. Response models are built with Pydantic for validation and
type safety.

The models include:
- Base response structures (BaseResponse, DataResponse, PaginatedResponse)
- Entity-specific response models for all API resources
- Pagination metadata and handling
- Standard status and message formatting

Using these response models ensures a consistent API experience and helps
both API consumers and developers maintain predictable integration patterns.
"""

from typing import List, Dict, Optional, Any, Generic, TypeVar, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator  # pydantic ^1.9.0

# Import response status models and metadata
from .error_models import ResponseStatus, ResponseMetadata

# Import data models for response content
from .data_models import (
    SourceSystem, PipelineDefinition, PipelineExecution, TaskExecution,
    QualityRule, QualityValidation, IssuePattern, HealingAction, 
    HealingExecution, PipelineMetric, Alert, HealingConfig, AlertConfig,
    OptimizationConfig, OptimizationRecommendation
)

# Define a generic type variable for use in generic response models
T = TypeVar('T')

# Base response models
class BaseResponse(BaseModel):
    """Base response model for all API responses."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Operation completed successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)

# Generic data response for single item responses
class DataResponse(Generic[T], BaseModel):
    """Generic response model for single data item responses."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata) 
    data: T

# Pagination metadata model
class PaginationMetadata(BaseModel):
    """Metadata for paginated responses."""
    page: int
    page_size: int
    total_items: int
    total_pages: int
    next_page: Optional[str] = None
    previous_page: Optional[str] = None

# Generic paginated response for list endpoints
class PaginatedResponse(Generic[T], BaseModel):
    """Generic response model for paginated list responses."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[T]
    pagination: PaginationMetadata

# Source System Response Models
class SourceSystemResponse(BaseModel):
    """Response model for source system data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Source system data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: SourceSystem

class SourceSystemListResponse(BaseModel):
    """Response model for paginated list of source systems."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Source systems retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[SourceSystem]
    pagination: PaginationMetadata

class SourceSystemTestResponse(BaseModel):
    """Response model for source system connection test."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Connection test completed"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    connection_successful: bool
    connection_details: Optional[Dict[str, Any]] = None
    test_results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

# Pipeline Response Models
class PipelineResponse(BaseModel):
    """Response model for pipeline definition data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipeline data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: PipelineDefinition

class PipelineListResponse(BaseModel):
    """Response model for paginated list of pipeline definitions."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipelines retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[PipelineDefinition]
    pagination: PaginationMetadata

# Pipeline Execution Response Models
class PipelineExecutionResponse(BaseModel):
    """Response model for pipeline execution data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipeline execution data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: PipelineExecution

class PipelineExecutionListResponse(BaseModel):
    """Response model for paginated list of pipeline executions."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipeline executions retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[PipelineExecution]
    pagination: PaginationMetadata

# Task Execution Response Models
class TaskExecutionResponse(BaseModel):
    """Response model for task execution data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Task execution data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: TaskExecution

class TaskExecutionListResponse(BaseModel):
    """Response model for paginated list of task executions."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Task executions retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[TaskExecution]
    pagination: PaginationMetadata

# Quality Rule Response Models
class QualityRuleResponse(BaseModel):
    """Response model for quality rule data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Quality rule data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: QualityRule

class QualityRuleListResponse(BaseModel):
    """Response model for paginated list of quality rules."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Quality rules retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[QualityRule]
    pagination: PaginationMetadata

# Quality Validation Response Models
class QualityValidationResponse(BaseModel):
    """Response model for quality validation data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Quality validation data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: QualityValidation

class QualityValidationListResponse(BaseModel):
    """Response model for paginated list of quality validations."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Quality validations retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[QualityValidation]
    pagination: PaginationMetadata

class QualityScoreResponse(BaseModel):
    """Response model for quality score data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Quality score calculated successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    overall_score: float
    dimension_scores: Dict[str, float]
    quality_metrics: Dict[str, Any]
    calculation_time: Optional[datetime] = None

# Issue Pattern Response Models
class IssuePatternResponse(BaseModel):
    """Response model for issue pattern data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Issue pattern data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: IssuePattern

class IssuePatternListResponse(BaseModel):
    """Response model for paginated list of issue patterns."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Issue patterns retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[IssuePattern]
    pagination: PaginationMetadata

# Healing Action Response Models
class HealingActionResponse(BaseModel):
    """Response model for healing action data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Healing action data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: HealingAction

class HealingActionListResponse(BaseModel):
    """Response model for paginated list of healing actions."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Healing actions retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[HealingAction]
    pagination: PaginationMetadata

# Healing Execution Response Models
class HealingExecutionResponse(BaseModel):
    """Response model for healing execution data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Healing execution data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: HealingExecution

class HealingExecutionListResponse(BaseModel):
    """Response model for paginated list of healing executions."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Healing executions retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[HealingExecution]
    pagination: PaginationMetadata

# Healing Configuration Response
class HealingConfigResponse(BaseModel):
    """Response model for healing configuration data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Healing configuration retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: HealingConfig

# Manual Healing Response
class ManualHealingResponse(BaseModel):
    """Response model for manual healing action execution."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Manual healing action executed"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    healing_id: str
    success: bool
    execution_details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

# Alert Response Models
class AlertResponse(BaseModel):
    """Response model for alert data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Alert data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: Alert

class AlertListResponse(BaseModel):
    """Response model for paginated list of alerts."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Alerts retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[Alert]
    pagination: PaginationMetadata

class AlertConfigResponse(BaseModel):
    """Response model for alert configuration data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Alert configuration retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: AlertConfig

class AlertAcknowledgeResponse(BaseModel):
    """Response model for alert acknowledgement."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Alert acknowledged successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    alert_id: str
    acknowledged: bool
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    similar_suppressed: Optional[bool] = None
    suppressed_count: Optional[int] = None

# Metric Response Models
class PipelineMetricResponse(BaseModel):
    """Response model for pipeline metric data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipeline metric data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: PipelineMetric

class PipelineMetricListResponse(BaseModel):
    """Response model for paginated list of pipeline metrics."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Pipeline metrics retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[PipelineMetric]
    pagination: PaginationMetadata

class MetricTimeSeriesResponse(BaseModel):
    """Response model for time series metric data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Metric time series data retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    metric_name: str
    metric_unit: Optional[str] = None
    data_points: List[Dict[str, Any]]
    statistics: Optional[Dict[str, Any]] = None
    annotations: Optional[Dict[str, Any]] = None

# Optimization Response Models
class OptimizationConfigResponse(BaseModel):
    """Response model for optimization configuration data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Optimization configuration retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: OptimizationConfig

class OptimizationRecommendationResponse(BaseModel):
    """Response model for optimization recommendation data."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Optimization recommendation retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    data: OptimizationRecommendation

class OptimizationRecommendationListResponse(BaseModel):
    """Response model for paginated list of optimization recommendations."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Optimization recommendations retrieved successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    items: List[OptimizationRecommendation]
    pagination: PaginationMetadata

class OptimizationImplementationResponse(BaseModel):
    """Response model for optimization implementation result."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "Optimization implemented successfully"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    recommendation_id: str
    implemented: bool
    implementation_results: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

# Health Check Response Model
class HealthCheckResponse(BaseModel):
    """Response model for system health check."""
    status: ResponseStatus = ResponseStatus.SUCCESS
    message: str = "System is healthy"
    metadata: ResponseMetadata = Field(default_factory=ResponseMetadata)
    version: str
    components: Dict[str, Dict[str, Any]]
    system_metrics: Dict[str, Any]