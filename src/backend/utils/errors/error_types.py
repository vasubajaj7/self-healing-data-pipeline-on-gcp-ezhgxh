"""
Error type hierarchy and classification system for the self-healing data pipeline.

This module defines standardized error classes with metadata for error
categorization, severity assessment, and self-healing capabilities.
These error types are used throughout the pipeline to enable consistent
error handling, intelligent retry strategies, and automated self-healing.
"""

import enum
from typing import Dict, Optional, Any

from ...constants import AlertSeverity


class ErrorCategory(enum.Enum):
    """Enumeration of error categories for classification and handling."""
    VALIDATION_ERROR = "VALIDATION_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    RESOURCE_ERROR = "RESOURCE_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    RATE_LIMIT_ERROR = "RATE_LIMIT_ERROR"
    DATA_ERROR = "DATA_ERROR"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNKNOWN = "UNKNOWN"


class ErrorRecoverability(enum.Enum):
    """Enumeration indicating whether an error is recoverable and how."""
    AUTO_RECOVERABLE = "AUTO_RECOVERABLE"
    MANUAL_RECOVERABLE = "MANUAL_RECOVERABLE"
    NON_RECOVERABLE = "NON_RECOVERABLE"


class PipelineError(Exception):
    """Base class for all pipeline-specific errors with metadata for classification and handling."""
    
    def __init__(
        self,
        message: str,
        category: Optional[ErrorCategory] = None,
        recoverability: Optional[ErrorRecoverability] = None,
        severity: Optional[AlertSeverity] = None,
        retryable: Optional[bool] = None,
        self_healable: Optional[bool] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize a pipeline error with classification metadata.
        
        Args:
            message: Human-readable error description
            category: Error category for classification
            recoverability: Indication of how the error can be recovered
            severity: Alert severity level for monitoring
            retryable: Whether the operation should be retried
            self_healable: Whether the error can be handled by self-healing
            context: Additional contextual information about the error
        """
        super().__init__(message)
        self.category = category or ErrorCategory.UNKNOWN
        self.recoverability = recoverability or ErrorRecoverability.MANUAL_RECOVERABLE
        self.severity = severity or AlertSeverity.MEDIUM
        
        # Set default retryable based on category if not specified
        if retryable is None:
            # By default, connection, timeout, rate limit, and service unavailable are retryable
            retryable_categories = [
                ErrorCategory.CONNECTION_ERROR,
                ErrorCategory.TIMEOUT_ERROR,
                ErrorCategory.RATE_LIMIT_ERROR,
                ErrorCategory.SERVICE_UNAVAILABLE,
                ErrorCategory.RESOURCE_ERROR
            ]
            self.retryable = self.category in retryable_categories
        else:
            self.retryable = retryable
            
        # Set default self_healable based on recoverability if not specified
        if self_healable is None:
            self.self_healable = self.recoverability == ErrorRecoverability.AUTO_RECOVERABLE
        else:
            self.self_healable = self_healable
            
        self.context = context or {}
    
    def is_retryable(self) -> bool:
        """Determine if this error should be retried.
        
        Returns:
            True if the error should be retried
        """
        return self.retryable
    
    def is_self_healable(self) -> bool:
        """Determine if this error can be handled by self-healing.
        
        Returns:
            True if the error can be self-healed
        """
        return self.self_healable
    
    def get_context(self) -> Dict[str, Any]:
        """Get the error context dictionary.
        
        Returns:
            Error context information
        """
        return self.context
    
    def add_context(self, additional_context: Dict[str, Any]) -> None:
        """Add additional context information to the error.
        
        Args:
            additional_context: Dictionary of additional context to add
        """
        self.context.update(additional_context)


class ValidationError(PipelineError):
    """Error raised when data validation fails."""
    
    def __init__(
        self,
        message: str,
        validation_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        retryable: bool = False,
        self_healable: bool = True
    ):
        """Initialize a validation error.
        
        Args:
            message: Human-readable error description
            validation_details: Details about the validation failure
            severity: Alert severity level
            retryable: Whether the operation should be retried
            self_healable: Whether the error can be self-healed
        """
        super().__init__(
            message=message,
            category=ErrorCategory.VALIDATION_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if self_healable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=retryable,
            self_healable=self_healable,
            context={"validation_details": validation_details}
        )


class ConfigurationError(PipelineError):
    """Error raised when there is an issue with configuration."""
    
    def __init__(
        self,
        message: str,
        config_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH
    ):
        """Initialize a configuration error.
        
        Args:
            message: Human-readable error description
            config_details: Details about the configuration issue
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.CONFIGURATION_ERROR,
            recoverability=ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Configuration errors typically shouldn't be retried without changes
            context={"config_details": config_details}
        )


class ConnectionError(PipelineError):
    """Error raised when connection to a service or resource fails."""
    
    def __init__(
        self,
        message: str,
        service_name: str,
        connection_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH,
        retryable: bool = True
    ):
        """Initialize a connection error.
        
        Args:
            message: Human-readable error description
            service_name: Name of the service connection failed for
            connection_details: Connection details and failure information
            severity: Alert severity level
            retryable: Whether the connection should be retried
        """
        super().__init__(
            message=message,
            category=ErrorCategory.CONNECTION_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if retryable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=retryable,
            context={
                "service_name": service_name,
                "connection_details": connection_details
            }
        )


class AuthenticationError(PipelineError):
    """Error raised when authentication fails."""
    
    def __init__(
        self,
        message: str,
        service_name: str,
        auth_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH
    ):
        """Initialize an authentication error.
        
        Args:
            message: Human-readable error description
            service_name: Name of the service authentication failed for
            auth_details: Authentication details and failure information
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.AUTHENTICATION_ERROR,
            recoverability=ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Authentication errors typically shouldn't be retried without changes
            context={
                "service_name": service_name,
                "auth_details": auth_details
            }
        )


class AuthorizationError(PipelineError):
    """Error raised when authorization fails (insufficient permissions)."""
    
    def __init__(
        self,
        message: str,
        service_name: str,
        resource: str,
        action: str,
        severity: AlertSeverity = AlertSeverity.HIGH
    ):
        """Initialize an authorization error.
        
        Args:
            message: Human-readable error description
            service_name: Name of the service authorization failed for
            resource: Resource that was being accessed
            action: Action that was being attempted
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.AUTHORIZATION_ERROR,
            recoverability=ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Authorization errors typically shouldn't be retried without changes
            context={
                "service_name": service_name,
                "resource": resource,
                "action": action
            }
        )


class ResourceError(PipelineError):
    """Error raised when a required resource is unavailable or insufficient."""
    
    def __init__(
        self,
        message: str,
        resource_type: str,
        resource_name: str,
        resource_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH,
        retryable: bool = True
    ):
        """Initialize a resource error.
        
        Args:
            message: Human-readable error description
            resource_type: Type of resource that is problematic
            resource_name: Name of the specific resource
            resource_details: Details about the resource issue
            severity: Alert severity level
            retryable: Whether the operation should be retried
        """
        super().__init__(
            message=message,
            category=ErrorCategory.RESOURCE_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if retryable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=retryable,
            context={
                "resource_type": resource_type,
                "resource_name": resource_name,
                "resource_details": resource_details
            }
        )


class TimeoutError(PipelineError):
    """Error raised when an operation times out."""
    
    def __init__(
        self,
        message: str,
        operation: str,
        timeout_seconds: float,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        retryable: bool = True
    ):
        """Initialize a timeout error.
        
        Args:
            message: Human-readable error description
            operation: Name of the operation that timed out
            timeout_seconds: Timeout duration in seconds
            severity: Alert severity level
            retryable: Whether the operation should be retried
        """
        super().__init__(
            message=message,
            category=ErrorCategory.TIMEOUT_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if retryable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=retryable,
            context={
                "operation": operation,
                "timeout_seconds": timeout_seconds
            }
        )


class RateLimitError(PipelineError):
    """Error raised when a service rate limit is exceeded."""
    
    def __init__(
        self,
        message: str,
        service_name: str,
        rate_limit_details: Dict[str, Any],
        retry_after: float,
        severity: AlertSeverity = AlertSeverity.MEDIUM
    ):
        """Initialize a rate limit error.
        
        Args:
            message: Human-readable error description
            service_name: Name of the service with rate limiting
            rate_limit_details: Details about the rate limit
            retry_after: Suggested time to wait before retry (seconds)
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.RATE_LIMIT_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE,
            severity=severity,
            retryable=True,  # Rate limit errors are typically retryable after waiting
            context={
                "service_name": service_name,
                "rate_limit_details": rate_limit_details,
                "retry_after": retry_after
            }
        )


class DataError(PipelineError):
    """Error raised when there is an issue with data content or format."""
    
    def __init__(
        self,
        message: str,
        data_source: str,
        data_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.MEDIUM,
        self_healable: bool = True
    ):
        """Initialize a data error.
        
        Args:
            message: Human-readable error description
            data_source: Source of the problematic data
            data_details: Details about the data issue
            severity: Alert severity level
            self_healable: Whether the error can be self-healed
        """
        super().__init__(
            message=message,
            category=ErrorCategory.DATA_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if self_healable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Data content errors typically shouldn't be retried without changes
            self_healable=self_healable,
            context={
                "data_source": data_source,
                "data_details": data_details
            }
        )


class SchemaError(PipelineError):
    """Error raised when there is a schema mismatch or violation."""
    
    def __init__(
        self,
        message: str,
        data_source: str,
        schema_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH,
        self_healable: bool = True
    ):
        """Initialize a schema error.
        
        Args:
            message: Human-readable error description
            data_source: Source of the data with schema issues
            schema_details: Details about the schema issue
            severity: Alert severity level
            self_healable: Whether the error can be self-healed
        """
        super().__init__(
            message=message,
            category=ErrorCategory.SCHEMA_ERROR,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if self_healable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Schema errors typically shouldn't be retried without changes
            self_healable=self_healable,
            context={
                "data_source": data_source,
                "schema_details": schema_details
            }
        )


class DependencyError(PipelineError):
    """Error raised when a dependency is missing or incompatible."""
    
    def __init__(
        self,
        message: str,
        dependency_name: str,
        dependency_type: str,
        dependency_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH
    ):
        """Initialize a dependency error.
        
        Args:
            message: Human-readable error description
            dependency_name: Name of the problematic dependency
            dependency_type: Type of dependency (library, service, etc.)
            dependency_details: Details about the dependency issue
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.DEPENDENCY_ERROR,
            recoverability=ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Dependency errors typically shouldn't be retried without changes
            context={
                "dependency_name": dependency_name,
                "dependency_type": dependency_type,
                "dependency_details": dependency_details
            }
        )


class ServiceUnavailableError(PipelineError):
    """Error raised when a required service is unavailable."""
    
    def __init__(
        self,
        message: str,
        service_name: str,
        service_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH,
        retryable: bool = True
    ):
        """Initialize a service unavailable error.
        
        Args:
            message: Human-readable error description
            service_name: Name of the unavailable service
            service_details: Details about the service unavailability
            severity: Alert severity level
            retryable: Whether the operation should be retried
        """
        super().__init__(
            message=message,
            category=ErrorCategory.SERVICE_UNAVAILABLE,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE if retryable else ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=retryable,
            context={
                "service_name": service_name,
                "service_details": service_details
            }
        )


class InternalError(PipelineError):
    """Error raised when an internal system error occurs."""
    
    def __init__(
        self,
        message: str,
        component: str,
        error_details: Dict[str, Any],
        severity: AlertSeverity = AlertSeverity.HIGH
    ):
        """Initialize an internal error.
        
        Args:
            message: Human-readable error description
            component: Name of the component with the error
            error_details: Details about the internal error
            severity: Alert severity level
        """
        super().__init__(
            message=message,
            category=ErrorCategory.INTERNAL_ERROR,
            recoverability=ErrorRecoverability.MANUAL_RECOVERABLE,
            severity=severity,
            retryable=False,  # Internal errors typically shouldn't be retried without investigation
            context={
                "component": component,
                "error_details": error_details
            }
        )


class CircuitBreakerOpenError(PipelineError):
    """Error raised when a circuit breaker is open and prevents an operation."""
    
    def __init__(
        self,
        service_name: str,
        open_since: float,
        reset_timeout: float,
        failure_history: Dict[str, Any]
    ):
        """Initialize a circuit breaker open error.
        
        Args:
            service_name: Name of the service protected by the circuit breaker
            open_since: Timestamp when the circuit breaker opened
            reset_timeout: Time until the circuit breaker will try to reset
            failure_history: History of failures that caused the circuit to open
        """
        message = f"Circuit breaker for service '{service_name}' is open and preventing operations"
        
        super().__init__(
            message=message,
            category=ErrorCategory.SERVICE_UNAVAILABLE,
            recoverability=ErrorRecoverability.AUTO_RECOVERABLE,
            severity=AlertSeverity.HIGH,
            retryable=False,  # Operations should not be retried while circuit is open
            context={
                "service_name": service_name,
                "open_since": open_since,
                "reset_timeout": reset_timeout,
                "failure_history": failure_history
            }
        )