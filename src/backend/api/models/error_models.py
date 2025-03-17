from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field, validator
from fastapi import HTTPException  # FastAPI exception class for reference and conversion

class ResponseStatus(Enum):
    """Enumeration of possible response statuses"""
    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class ResponseMetadata(BaseModel):
    """Model for response metadata information"""
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    version: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None

class ErrorCategory(Enum):
    """Enumeration of error categories for classification and handling"""
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    RESOURCE_NOT_FOUND = "resource_not_found"
    CONFIGURATION = "configuration"
    EXTERNAL_SERVICE = "external_service"
    DATA_QUALITY = "data_quality"
    PIPELINE_EXECUTION = "pipeline_execution"
    SYSTEM = "system"

class ErrorSeverity(Enum):
    """Enumeration of error severity levels for prioritization"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class ErrorDetail(BaseModel):
    """Model for detailed error information"""
    field: str
    message: str
    code: Optional[str] = None
    context: Optional[Any] = None

class ErrorResponse(BaseModel):
    """Standard error response model for API errors"""
    status: ResponseStatus = ResponseStatus.ERROR
    message: str
    metadata: ResponseMetadata
    status_code: int
    error_type: str
    category: ErrorCategory
    severity: ErrorSeverity
    details: Optional[List[ErrorDetail]] = None
    debug_info: Optional[Dict[str, Any]] = None

class ValidationErrorResponse(BaseModel):
    """Specialized error response for validation errors"""
    status: ResponseStatus = ResponseStatus.ERROR
    message: str
    metadata: ResponseMetadata
    status_code: int = 422
    error_type: str = "validation_error"
    validation_errors: List[ErrorDetail]

class PipelineError(Exception):
    """Base exception class for pipeline-specific errors"""
    def __init__(
        self, 
        message: str,
        category: ErrorCategory,
        severity: ErrorSeverity,
        details: Optional[List[ErrorDetail]] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the pipeline error"""
        # Call Exception.__init__ with message
        super().__init__(message)
        # Store message, category, severity, details, and context
        self.message = message
        self.category = category
        self.severity = severity
        self.details = details
        self.context = context
    
    def to_response(self, request_id: Optional[str] = None) -> ErrorResponse:
        """Convert the error to an ErrorResponse"""
        # Create metadata with request_id if provided
        metadata = ResponseMetadata(request_id=request_id)
        # Create and return an ErrorResponse with the error's properties
        # Include request_id in metadata if provided
        return ErrorResponse(
            message=self.message,
            metadata=metadata,
            status_code=self._get_status_code(),
            error_type=self.category.name,
            category=self.category,
            severity=self.severity,
            details=self.details,
            debug_info=self.context
        )
    
    def _get_status_code(self) -> int:
        """Determine the appropriate HTTP status code based on the error category"""
        status_codes = {
            ErrorCategory.VALIDATION: 422,
            ErrorCategory.AUTHENTICATION: 401,
            ErrorCategory.AUTHORIZATION: 403,
            ErrorCategory.RESOURCE_NOT_FOUND: 404,
            ErrorCategory.CONFIGURATION: 500,
            ErrorCategory.EXTERNAL_SERVICE: 502,
            ErrorCategory.DATA_QUALITY: 422,
            ErrorCategory.PIPELINE_EXECUTION: 500,
            ErrorCategory.SYSTEM: 500
        }
        return status_codes.get(self.category, 500)

class ValidationError(Exception):
    """Exception for data validation errors"""
    def __init__(self, message: str, validation_errors: List[ErrorDetail]):
        """Initialize the validation error"""
        # Call Exception.__init__ with message
        super().__init__(message)
        # Store message and validation_errors
        self.message = message
        self.validation_errors = validation_errors
    
    def to_response(self, request_id: Optional[str] = None) -> ValidationErrorResponse:
        """Convert the error to a ValidationErrorResponse"""
        # Create metadata with request_id if provided
        metadata = ResponseMetadata(request_id=request_id)
        # Create and return a ValidationErrorResponse with the error's properties
        return ValidationErrorResponse(
            message=self.message,
            metadata=metadata,
            validation_errors=self.validation_errors
        )

class ResourceNotFoundError(PipelineError):
    """Exception for resource not found errors"""
    def __init__(
        self, 
        resource_type: str,
        resource_id: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the resource not found error"""
        # Store resource_type and resource_id
        self.resource_type = resource_type
        self.resource_id = resource_id
        # Create message with resource_type and resource_id
        message = f"{resource_type} with ID '{resource_id}' not found"
        # Call PipelineError.__init__ with message, RESOURCE_NOT_FOUND category, MEDIUM severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.RESOURCE_NOT_FOUND,
            severity=ErrorSeverity.MEDIUM,
            context=context
        )

class AuthenticationError(PipelineError):
    """Exception for authentication failures"""
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the authentication error"""
        # Call PipelineError.__init__ with message, AUTHENTICATION category, HIGH severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            context=context
        )

class AuthorizationError(PipelineError):
    """Exception for authorization failures"""
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the authorization error"""
        # Call PipelineError.__init__ with message, AUTHORIZATION category, HIGH severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.AUTHORIZATION,
            severity=ErrorSeverity.HIGH,
            context=context
        )

class ConfigurationError(PipelineError):
    """Exception for configuration-related errors"""
    def __init__(
        self,
        message: str,
        severity: ErrorSeverity,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the configuration error"""
        # Call PipelineError.__init__ with message, CONFIGURATION category, provided severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.CONFIGURATION,
            severity=severity,
            context=context
        )

class ExternalServiceError(PipelineError):
    """Exception for external service integration failures"""
    def __init__(
        self,
        service_name: str,
        message: str,
        severity: ErrorSeverity,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the external service error"""
        # Store service_name
        self.service_name = service_name
        # Call PipelineError.__init__ with message, EXTERNAL_SERVICE category, provided severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.EXTERNAL_SERVICE,
            severity=severity,
            context=context
        )

class DataQualityError(PipelineError):
    """Exception for data quality validation failures"""
    def __init__(
        self,
        message: str,
        dataset: str,
        table: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        details: Optional[List[ErrorDetail]] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the data quality error"""
        # Store dataset and table
        self.dataset = dataset
        self.table = table
        # Call PipelineError.__init__ with message, DATA_QUALITY category, provided severity, details, and context
        super().__init__(
            message=message,
            category=ErrorCategory.DATA_QUALITY,
            severity=severity,
            details=details,
            context=context
        )

class PipelineExecutionError(PipelineError):
    """Exception for pipeline execution failures"""
    def __init__(
        self,
        message: str,
        pipeline_id: str,
        execution_id: Optional[str] = None,
        task_id: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the pipeline execution error"""
        # Store pipeline_id, execution_id, and task_id
        self.pipeline_id = pipeline_id
        self.execution_id = execution_id
        self.task_id = task_id
        # Call PipelineError.__init__ with message, PIPELINE_EXECUTION category, provided severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.PIPELINE_EXECUTION,
            severity=severity,
            context=context
        )

class SystemError(PipelineError):
    """Exception for system-level errors"""
    def __init__(
        self,
        message: str,
        component: str,
        severity: ErrorSeverity = ErrorSeverity.CRITICAL,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the system error"""
        # Store component
        self.component = component
        # Call PipelineError.__init__ with message, SYSTEM category, provided severity, and context
        super().__init__(
            message=message,
            category=ErrorCategory.SYSTEM,
            severity=severity,
            context=context
        )