"""
Entry point for the error handling utilities in the self-healing data pipeline.

Provides a centralized import location for all error types, error reporting functionality,
and exception handling mechanisms. Simplifies error handling across the application by
exposing a consistent API.
"""

# Import error types
from .error_types import (
    ErrorCategory,
    ErrorRecoverability,
    PipelineError,
    ValidationError,
    ConfigurationError,
    ConnectionError,
    AuthenticationError,
    AuthorizationError,
    ResourceError,
    TimeoutError,
    RateLimitError,
    DataError,
    SchemaError,
    DependencyError,
    ServiceUnavailableError,
    InternalError,
    CircuitBreakerOpenError
)

# Import error reporting functionality
from .error_reporter import ErrorReporter

# Import exception handling functionality
from .exception_handler import (
    initialize_exception_handler,
    handle_exception,
    wrap_exception,
    get_error_reporter,
    ExceptionHandler,
    exception_handler
)