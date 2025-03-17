# src/backend/utils/errors/exception_handler.py
"""Provides a centralized exception handling mechanism for the self-healing data pipeline.
Implements global exception handling, contextual error handling, and integration with the self-healing system.
Ensures consistent error handling across all pipeline components and facilitates automated recovery from failures."""

import traceback  # standard library
import sys  # standard library
import functools  # standard library
import contextlib  # standard library
import typing  # standard library

from . import error_types  # src/backend/utils/errors/error_types.py
from .utils.errors import error_reporter  # src/backend/utils/errors/error_reporter.py
from .utils.logging import logger  # src/backend/utils/logging/logger.py
from .utils.config import config_loader  # src/backend/utils/config/config_loader.py
from src.backend import constants  # src/backend/constants.py

# Initialize logger for this module
logger = logger.get_logger(__name__)

# Global error reporter instance
_error_reporter: error_reporter.ErrorReporter = None

# Store original exception hook
_original_excepthook = sys.excepthook


def initialize_exception_handler(config_override: typing.Optional[dict] = None) -> None:
    """Initializes the global exception handling system

    Args:
        config_override: Configuration overrides for the error reporter
    """
    global _error_reporter
    try:
        # Initialize error reporter with configuration
        _error_reporter = error_reporter.ErrorReporter(config_override)

        # Set up global exception hook
        sys.excepthook = global_exception_handler

        # Configure logging for exception handler
        logger.debug("Global exception handler initialized")
    except Exception as e:
        logger.error(f"Failed to initialize exception handler: {e}")


def global_exception_handler(exc_type, exc_value: Exception, exc_traceback) -> None:
    """Global exception handler for uncaught exceptions

    Args:
        exc_type: Exception type
        exc_value: Exception instance
        exc_traceback: Exception traceback
    """
    try:
        # Check if exception is a PipelineError
        if not isinstance(exc_value, error_types.PipelineError):
            # If not, wrap in appropriate PipelineError type
            exc_value = wrap_exception(exc_value, {"exc_type": exc_type.__name__})

        # Report exception to error reporter
        handle_exception(exception=exc_value, context={"exc_type": exc_type.__name__}, severity=exc_value.severity, reraise=False)

        # Log exception with appropriate severity
        logger.log(getattr(logger, exc_value.severity.name.lower()), f"Uncaught exception: {exc_value}")

        # Call original exception hook for standard handling
        if _original_excepthook:
            _original_excepthook(exc_type, exc_value, exc_traceback)
        else:
            # Fallback to default exception handling if original hook is not available
            sys.__excepthook__(exc_type, exc_value, exc_traceback)

    except Exception as e:
        logger.error(f"Error in global exception handler: {e}")
        # Attempt to use the original excepthook, even if it failed before
        if _original_excepthook:
            _original_excepthook(exc_type, e, exc_traceback)
        else:
            sys.__excepthook__(exc_type, e, exc_traceback)


def handle_exception(exception: Exception, context: dict = None, severity: constants.AlertSeverity = None, reraise: bool = True) -> str:
    """Handles an exception with appropriate logging and reporting

    Args:
        exception: The exception to handle
        context: Contextual information about the error
        severity: Severity level of the alert
        reraise: Whether to re-raise the exception after handling

    Returns:
        Error report ID
    """
    try:
        # Ensure error reporter is initialized
        error_reporter_instance = get_error_reporter()

        # Determine severity if not provided
        if severity is None:
            if isinstance(exception, error_types.PipelineError):
                severity = exception.severity
            else:
                severity = constants.AlertSeverity.MEDIUM

        # Report exception to error reporter
        report_id = error_reporter_instance.report_exception(exception, context, severity)

        # Log exception with appropriate level
        logger.log(getattr(logger, severity.name.lower()), f"Exception handled: {exception} (Report ID: {report_id})")

        # If reraise is True, re-raise the exception
        if reraise:
            raise exception

        # Return the error report ID
        return report_id
    except Exception as e:
        logger.error(f"Error handling exception: {e}")
        return None


def wrap_exception(exception: Exception, context: dict = None) -> error_types.PipelineError:
    """Wraps a standard exception in an appropriate PipelineError type

    Args:
        exception: The exception to wrap
        context: Contextual information about the error

    Returns:
        Wrapped exception as PipelineError
    """
    try:
        # If exception is already a PipelineError, add context and return
        if isinstance(exception, error_types.PipelineError):
            exception.add_context(context)
            return exception

        # Determine appropriate PipelineError subclass based on exception type
        if isinstance(exception, ValueError):
            pipeline_error_type = error_types.DataError
        elif isinstance(exception, OSError):
            pipeline_error_type = error_types.ConnectionError
        else:
            pipeline_error_type = error_types.InternalError

        # Create new PipelineError instance with original exception message
        pipeline_error = pipeline_error_type(str(exception), data_source="unknown", data_details={}, component="unknown")

        # Add original exception to context
        pipeline_error.add_context({"original_exception": exception})

        # Add stack trace to context
        pipeline_error.add_context({"stack_trace": traceback.format_exc()})

        # Add context to the exception
        pipeline_error.add_context(context)

        # Return the wrapped exception
        return pipeline_error
    except Exception as e:
        logger.error(f"Error wrapping exception: {e}")
        return error_types.PipelineError(f"Error wrapping exception: {e}")


def get_error_reporter() -> error_reporter.ErrorReporter:
    """Gets the initialized error reporter instance

    Returns:
        Initialized error reporter
    """
    global _error_reporter
    try:
        # Check if error reporter is initialized
        if _error_reporter is None:
            # If not, initialize with default configuration
            initialize_exception_handler()

        # Return the error reporter instance
        return _error_reporter
    except Exception as e:
        logger.error(f"Error getting error reporter: {e}")
        return None


def reset_exception_handler() -> None:
    """Resets the exception handler to its default state (primarily for testing)"""
    global _error_reporter, _original_excepthook
    try:
        # Restore original sys.excepthook
        sys.excepthook = _original_excepthook

        # Reset error reporter to None
        _error_reporter = None

        # Log reset operation
        logger.info("Exception handler reset to default state")
    except Exception as e:
        logger.error(f"Error resetting exception handler: {e}")


class ExceptionHandler:
    """Context manager for handling exceptions in a specific context"""

    def __init__(self, context: dict = None, severity: constants.AlertSeverity = constants.AlertSeverity.MEDIUM, reraise: bool = True, default_category: error_types.ErrorCategory = error_types.ErrorCategory.UNKNOWN):
        """Initialize the exception handler context manager

        Args:
            context: Contextual information about the error
            severity: Severity level of the alert
            reraise: Whether to re-raise the exception after handling
            default_category: Default error category for uncategorized exceptions
        """
        # Store context dictionary or initialize empty dict
        self.context = context or {}

        # Store severity or use default (MEDIUM)
        self.severity = severity

        # Store reraise flag (default True)
        self.reraise = reraise

        # Store default error category for uncategorized exceptions
        self.default_category = default_category

    def __enter__(self) -> "ExceptionHandler":
        """Enter the exception handling context

        Returns:
            Self reference
        """
        return self

    def __exit__(self, exc_type, exc_value: Exception, exc_traceback) -> bool:
        """Exit the exception handling context and handle any exceptions

        Args:
            exc_type: Exception type
            exc_value: Exception instance
            exc_traceback: Exception traceback

        Returns:
            True if exception was handled, False to propagate
        """
        try:
            # If no exception occurred, return False
            if exc_type is None:
                return False

            # If exception is not a PipelineError, wrap it
            if not isinstance(exc_value, error_types.PipelineError):
                exc_value = wrap_exception(exc_value, self.context)

            # Handle the exception with context and severity
            handle_exception(exception=exc_value, context=self.context, severity=self.severity, reraise=False)

            # Return not reraise (True to suppress, False to propagate)
            return not self.reraise
        except Exception as e:
            logger.error(f"Error in ExceptionHandler context manager: {e}")
            return False


class exception_handler:
    """Decorator for handling exceptions in functions"""

    def __init__(self, context: dict = None, severity: constants.AlertSeverity = constants.AlertSeverity.MEDIUM, reraise: bool = True, default_category: error_types.ErrorCategory = error_types.ErrorCategory.UNKNOWN):
        """Initialize the exception handler decorator

        Args:
            context: Contextual information about the error
            severity: Severity level of the alert
            reraise: Whether to re-raise the exception after handling
            default_category: Default error category for uncategorized exceptions
        """
        # Store context dictionary or initialize empty dict
        self.context = context or {}

        # Store severity or use default (MEDIUM)
        self.severity = severity

        # Store reraise flag (default True)
        self.reraise = reraise

        # Store default error category for uncategorized exceptions
        self.default_category = default_category

    def __call__(self, func):
        """Call method to make the class work as a decorator

        Args:
            func: Function to decorate

        Returns:
            Wrapped function with exception handling
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """Wrapper function that catches exceptions"""
            # Define wrapper function that catches exceptions
            with ExceptionHandler(context=self.context, severity=self.severity, reraise=self.reraise, default_category=self.default_category):
                try:
                    # Add function name and arguments to context
                    local_context = self.context.copy()
                    local_context["function_name"] = func.__name__
                    
                    # Inspect arguments and add them to the context
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()
                    
                    # Add arguments to the context
                    for name, value in bound_args.arguments.items():
                        local_context[f"arg_{name}"] = value
                    
                    # Execute function within exception handler
                    return func(*args, **kwargs)
                except Exception as e:
                    # Exception is handled by the context manager
                    pass
        return wrapper