"""
Logging utilities for the self-healing data pipeline.

This package provides a comprehensive logging framework with features including:
- Standardized logging across all components
- Context-aware logging with correlation IDs for request tracing
- Environment-specific formatters (colored for development, JSON for production)
- Multiple output handlers (console, file, Google Cloud Logging)
- Thread-local context storage for adding metadata to logs

The logging utilities ensure consistent instrumentation throughout the application
while supporting different deployment environments.
"""

# Import components from logger module
from .logger import (
    get_logger,
    set_correlation_id,
    get_correlation_id,
    clear_correlation_id,
    set_context,
    get_context,
    clear_context,
    log_with_context,
    configure_root_logger,
    reset_logging,
    LoggingContext,
    CorrelationIdContext
)

# Import components from log_formatter module
from .log_formatter import (
    JsonFormatter,
    ColoredFormatter,
    StructuredFormatter,
    format_exception_info,
    get_formatter_for_environment
)

# Import components from log_handler module
from .log_handler import (
    get_console_handler,
    get_file_handler,
    get_cloud_logging_handler,
    create_default_handlers,
    ContextualHandler,
    is_running_in_cloud
)

# Export all components
__all__ = [
    # Logger functions
    'get_logger',
    'set_correlation_id',
    'get_correlation_id',
    'clear_correlation_id',
    'set_context',
    'get_context',
    'clear_context',
    'log_with_context',
    'configure_root_logger',
    'reset_logging',
    
    # Context managers
    'LoggingContext',
    'CorrelationIdContext',
    
    # Formatters
    'JsonFormatter',
    'ColoredFormatter',
    'StructuredFormatter',
    'format_exception_info',
    'get_formatter_for_environment',
    
    # Handlers
    'get_console_handler',
    'get_file_handler',
    'get_cloud_logging_handler',
    'create_default_handlers',
    'ContextualHandler',
    'is_running_in_cloud'
]