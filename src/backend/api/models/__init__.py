"""
API Models Package

This package provides all data models, request models, response models, and error models
for the self-healing data pipeline API. These models are used throughout the application
for request validation, response formatting, and data representation.

The models are organized into four categories:
- Data models: Core entity representations
- Error models: Standardized error response structures
- Request models: Request validation and parsing
- Response models: Standardized API response structures

All models use Pydantic for validation, providing type checking, validation rules,
clear error messages, and automatic documentation.
"""

# Import all models from the sub-modules
from .data_models import *
from .error_models import *
from .request_models import *
from .response_models import *

# Define what should be exported when using "from api.models import *"
__all__ = [
    # Data model enums
    'SourceSystemType', 'ConnectionStatus', 'PipelineStatus', 'TaskStatus',
    'ValidationStatus', 'HealingStatus', 'MetricType', 'RuleType',
    
    # Core data models
    'SourceSystem', 'PipelineDefinition', 'PipelineExecution', 'TaskExecution',
    'QualityRule', 'QualityValidation', 'IssuePattern', 'HealingAction',
    'HealingExecution', 'PipelineMetric', 'Alert', 'HealingConfig',
    'AlertConfig', 'OptimizationConfig', 'OptimizationRecommendation',
    
    # Error model enums
    'ResponseStatus', 'ResponseMetadata', 'ErrorCategory', 'ErrorSeverity',
    
    # Error models
    'ErrorDetail', 'ErrorResponse', 'ValidationErrorResponse', 'PipelineError',
    'ValidationError', 'ResourceNotFoundError', 'AuthenticationError',
    'AuthorizationError', 'ConfigurationError', 'ExternalServiceError',
    'DataQualityError', 'PipelineExecutionError', 'SystemError',
    
    # Request models
    'PaginationParams', 'DateRangeParams', 'SourceSystemCreateRequest',
    'SourceSystemUpdateRequest', 'SourceSystemTestRequest', 'PipelineCreateRequest',
    'PipelineUpdateRequest', 'PipelineExecuteRequest', 'QualityRuleCreateRequest',
    'QualityRuleUpdateRequest', 'HealingPatternCreateRequest',
    'HealingPatternUpdateRequest', 'HealingActionCreateRequest',
    'HealingActionUpdateRequest', 'HealingConfigUpdateRequest',
    'ManualHealingRequest', 'AlertConfigUpdateRequest', 'AlertAcknowledgeRequest',
    'OptimizationConfigUpdateRequest',
    
    # Response models
    'BaseResponse', 'DataResponse', 'PaginationMetadata', 'PaginatedResponse',
    'SourceSystemResponse', 'SourceSystemListResponse', 'SourceSystemTestResponse',
    'PipelineResponse', 'PipelineListResponse', 'PipelineExecutionResponse',
    'PipelineExecutionListResponse', 'TaskExecutionResponse',
    'TaskExecutionListResponse', 'QualityRuleResponse', 'QualityRuleListResponse',
    'QualityValidationResponse', 'QualityValidationListResponse',
    'QualityScoreResponse', 'IssuePatternResponse', 'IssuePatternListResponse',
    'HealingActionResponse', 'HealingActionListResponse',
    'HealingExecutionResponse', 'HealingExecutionListResponse',
    'HealingConfigResponse', 'ManualHealingResponse', 'AlertResponse',
    'AlertListResponse', 'AlertConfigResponse', 'AlertAcknowledgeResponse',
    'PipelineMetricResponse', 'PipelineMetricListResponse',
    'MetricTimeSeriesResponse', 'OptimizationConfigResponse',
    'OptimizationRecommendationResponse', 'OptimizationRecommendationListResponse',
    'OptimizationImplementationResponse', 'HealthCheckResponse',
]