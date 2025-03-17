"""
Core logging utility module that provides a standardized logging interface for the self-healing data pipeline.

This module implements context-aware logging, thread-local storage for correlation IDs, and configurable loggers
with appropriate handlers based on the execution environment. It provides a consistent logging interface
across all components of the application.

Key features:
- Thread-local context storage for adding metadata to logs
- Correlation ID management for request tracing across components
- Environment-specific configuration of log handlers and formatters
- Contextual logging with decorators and context managers
"""

import logging
import os
import sys
import typing
import uuid
import threading
import functools
import contextlib

from ...constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING, 
    ENV_PRODUCTION,
    DEFAULT_LOG_LEVEL
)
from ...config import get_config
from .log_formatter import (
    JsonFormatter,
    ColoredFormatter, 
    StructuredFormatter,
    get_formatter_for_environment
)
from .log_handler import (
    get_console_handler,
    get_file_handler,
    get_cloud_logging_handler,
    create_default_handlers,
    ContextualHandler
)

# Default format strings
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Thread-local storage for contextual logging
_thread_local = threading.local()

# Logger cache for reusing configured loggers
_loggers = {}


def get_logger(name: str, log_level: str = None, environment: str = None) -> logging.Logger:
    """
    Gets or creates a logger with the specified name and configuration.
    
    Args:
        name: The name of the logger
        log_level: The logging level (DEBUG, INFO, etc.), or None to use from config
        environment: The execution environment, or None to use from config
        
    Returns:
        Configured logger instance
    """
    # Check if logger already exists in cache
    if name in _loggers:
        return _loggers[name]
    
    # Create new logger
    logger = logging.getLogger(name)
    
    # Determine log level from parameter, config, or default
    if log_level is None:
        config = get_config()
        log_level = config.get("logging.level", DEFAULT_LOG_LEVEL)
    
    # Determine environment from parameter, config, or default
    if environment is None:
        config = get_config()
        environment = config.get_environment()
    
    # Set logger level
    logger.setLevel(getattr(logging, log_level))
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create appropriate handlers based on environment
    handlers = create_default_handlers(log_level, environment)
    
    # Wrap handlers with contextual handler for context enrichment
    for handler in handlers:
        wrapped_handler = ContextualHandler(handler)
        logger.addHandler(wrapped_handler)
    
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False
    
    # Cache logger in _loggers dictionary
    _loggers[name] = logger
    
    return logger


def set_correlation_id(correlation_id: str = None) -> str:
    """
    Sets the correlation ID for the current thread.
    
    Args:
        correlation_id: The correlation ID to set, or None to generate a new one
        
    Returns:
        The correlation ID that was set
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
    
    setattr(_thread_local, 'correlation_id', correlation_id)
    return correlation_id


def get_correlation_id() -> str:
    """
    Gets the correlation ID for the current thread.
    
    Returns:
        Current correlation ID or None if not set
    """
    return getattr(_thread_local, 'correlation_id', None)


def clear_correlation_id() -> None:
    """
    Clears the correlation ID for the current thread.
    """
    if hasattr(_thread_local, 'correlation_id'):
        delattr(_thread_local, 'correlation_id')


def set_context(context: dict) -> dict:
    """
    Sets context values for the current thread's logging context.
    
    Args:
        context: Dictionary of context values to set
        
    Returns:
        The updated context dictionary
    """
    # Initialize context in thread-local storage if not exists
    if not hasattr(_thread_local, 'context'):
        _thread_local.context = {}
    
    # Update thread-local context with provided context dictionary
    _thread_local.context.update(context)
    
    return _thread_local.context


def get_context() -> dict:
    """
    Gets the current thread's logging context.
    
    Returns:
        Current context dictionary or empty dict if not set
    """
    return getattr(_thread_local, 'context', {})


def clear_context() -> None:
    """
    Clears the context for the current thread.
    """
    if hasattr(_thread_local, 'context'):
        delattr(_thread_local, 'context')


def log_with_context(context: dict):
    """
    Decorator that adds context to all logs within a function.
    
    Args:
        context: Dictionary of context values to add to logs
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get existing context
            original_context = get_context().copy()
            
            # Update context with provided context dictionary
            set_context(context)
            
            try:
                # Execute the original function
                return func(*args, **kwargs)
            finally:
                # Restore original context after function execution
                if original_context:
                    setattr(_thread_local, 'context', original_context)
                else:
                    clear_context()
        
        return wrapper
    
    return decorator


class LoggingContext:
    """
    Context manager for temporarily setting logging context.
    """
    
    def __init__(self, context: dict):
        """
        Initializes the logging context manager.
        
        Args:
            context: Dictionary of context values to set
        """
        self._context = context
        self._previous_context = None
    
    def __enter__(self):
        """
        Sets the context when entering the context manager.
        
        Returns:
            Self reference for context manager
        """
        self._previous_context = get_context().copy()
        set_context(self._context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Restores the previous context when exiting the context manager.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        if self._previous_context:
            setattr(_thread_local, 'context', self._previous_context)
        else:
            clear_context()
        
        # Return None to propagate any exceptions
        return None


class CorrelationIdContext:
    """
    Context manager for temporarily setting correlation ID.
    """
    
    def __init__(self, correlation_id: str = None):
        """
        Initializes the correlation ID context manager.
        
        Args:
            correlation_id: Correlation ID to set, or None to generate a new one
        """
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._previous_correlation_id = None
    
    def __enter__(self):
        """
        Sets the correlation ID when entering the context manager.
        
        Returns:
            Self reference for context manager
        """
        self._previous_correlation_id = get_correlation_id()
        set_correlation_id(self._correlation_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Restores the previous correlation ID when exiting the context manager.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        if self._previous_correlation_id:
            set_correlation_id(self._previous_correlation_id)
        else:
            clear_correlation_id()
        
        # Return None to propagate any exceptions
        return None


def configure_root_logger(log_level: str = None, environment: str = None) -> logging.Logger:
    """
    Configures the root logger with appropriate handlers.
    
    Args:
        log_level: The logging level (DEBUG, INFO, etc.), or None to use from config
        environment: The execution environment, or None to use from config
        
    Returns:
        Configured root logger
    """
    # Get root logger
    root_logger = logging.getLogger()
    
    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Determine log level from parameter, config, or default
    if log_level is None:
        config = get_config()
        log_level = config.get("logging.level", DEFAULT_LOG_LEVEL)
    
    # Determine environment from parameter, config, or default
    if environment is None:
        config = get_config()
        environment = config.get_environment()
    
    # Set root logger level
    root_logger.setLevel(getattr(logging, log_level))
    
    # Create appropriate handlers based on environment
    handlers = create_default_handlers(log_level, environment)
    
    # Wrap handlers with ContextualHandler for context enrichment
    for handler in handlers:
        wrapped_handler = ContextualHandler(handler)
        root_logger.addHandler(wrapped_handler)
    
    return root_logger


def reset_logging() -> None:
    """
    Resets all loggers to default state.
    """
    # Get root logger
    root_logger = logging.getLogger()
    
    # Remove all handlers from root logger
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Reset log level to default
    root_logger.setLevel(logging.WARNING)
    
    # Add a NullHandler to prevent 'No handlers found' warnings
    root_logger.addHandler(logging.NullHandler())
    
    # Clear _loggers cache
    _loggers.clear()