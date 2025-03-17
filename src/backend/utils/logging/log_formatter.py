"""
Custom log formatters for the self-healing data pipeline application.

This module provides various formatters for different output formats including:
- JSON formatter for machine processing
- Colored formatter for console display
- Structured formatter for consistent field formatting

These formatters support context-rich logging with correlation IDs and execution metadata.
"""

import logging
import json
import datetime
from typing import Dict, Any, Optional, Tuple
import traceback
import colorama

# Import environment constants for environment-specific formatting
from ...constants import ENV_DEVELOPMENT, ENV_STAGING, ENV_PRODUCTION

# Default format strings
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Color mapping for different log levels
LEVEL_COLORS = {
    logging.DEBUG: colorama.Fore.CYAN,
    logging.INFO: colorama.Fore.GREEN,
    logging.WARNING: colorama.Fore.YELLOW,
    logging.ERROR: colorama.Fore.RED,
    logging.CRITICAL: colorama.Fore.MAGENTA
}


def format_exception_info(exc_info: Optional[Tuple]) -> str:
    """
    Format exception information into a string representation.
    
    Args:
        exc_info: Exception information tuple (type, value, traceback)
        
    Returns:
        Formatted exception string or empty string if no exception
    """
    if not exc_info:
        return ""
    
    return "".join(traceback.format_exception(*exc_info))


def get_formatter_for_environment(environment: str, 
                                log_format: str = DEFAULT_FORMAT, 
                                date_format: str = DEFAULT_DATE_FORMAT) -> logging.Formatter:
    """
    Return the appropriate formatter for the specified environment.
    
    Args:
        environment: The deployment environment (development, staging, production)
        log_format: The log format string
        date_format: The date format string
        
    Returns:
        Formatter instance appropriate for the environment
    """
    if environment == ENV_DEVELOPMENT:
        return ColoredFormatter(log_format, date_format)
    elif environment == ENV_PRODUCTION:
        return JsonFormatter(date_format)
    elif environment == ENV_STAGING:
        return StructuredFormatter(log_format, date_format)
    else:
        # Default to standard formatter for unknown environments
        return logging.Formatter(log_format, date_format)


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs log records as JSON objects for machine processing.
    
    This formatter is particularly useful for production environments where logs
    are consumed by log aggregation and analysis tools.
    """
    
    def __init__(self, date_format: str = DEFAULT_DATE_FORMAT):
        """
        Initialize the JSON formatter.
        
        Args:
            date_format: Format string for timestamps
        """
        super().__init__()  # No format string needed for JSON formatter
        self.date_format = date_format
        
    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record as a JSON string.
        
        Args:
            record: The log record to format
            
        Returns:
            JSON-formatted log message
        """
        # Create base log record with standard fields
        log_data = {
            'timestamp': self.formatTime(record, self.date_format),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
            
        # Add any extra attributes that were passed to the logger
        for key, value in record.__dict__.items():
            if key not in ('args', 'asctime', 'created', 'exc_info', 'exc_text', 
                          'filename', 'funcName', 'id', 'levelname', 'levelno',
                          'lineno', 'module', 'msecs', 'message', 'msg', 'name', 
                          'pathname', 'process', 'processName', 'relativeCreated', 
                          'stack_info', 'thread', 'threadName'):
                log_data[key] = value
        
        # Convert to JSON string
        return json.dumps(log_data)
    
    def formatTime(self, record: logging.LogRecord, date_format: Optional[str] = None) -> str:
        """
        Format the record timestamp according to date_format.
        
        Args:
            record: The log record
            date_format: Format string for the timestamp
            
        Returns:
            Formatted timestamp string
        """
        # Convert record creation time to datetime
        dt = datetime.datetime.fromtimestamp(record.created)
        # Format using the specified date_format or the default
        return dt.strftime(date_format or self.date_format)
    
    def formatException(self, exc_info: Tuple) -> str:
        """
        Format exception information as a string.
        
        Args:
            exc_info: Exception information tuple
            
        Returns:
            Formatted exception information
        """
        return format_exception_info(exc_info)


class ColoredFormatter(logging.Formatter):
    """
    Formatter that outputs colored log messages for console display in development.
    
    This formatter applies colors to log messages based on their log level,
    making it easier to distinguish between different types of log messages.
    """
    
    def __init__(self, fmt: str = DEFAULT_FORMAT, 
                date_fmt: str = DEFAULT_DATE_FORMAT, 
                level_colors: Dict[int, str] = None):
        """
        Initialize the colored formatter.
        
        Args:
            fmt: Format string for log messages
            date_fmt: Format string for timestamps
            level_colors: Mapping of log levels to colors
        """
        super().__init__(fmt, date_fmt)
        # Initialize colorama if not already initialized
        if not hasattr(colorama, 'initialized') or not colorama.initialized:
            colorama.init()
            
        self.level_colors = level_colors or LEVEL_COLORS
        
    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with appropriate colors based on level.
        
        Args:
            record: The log record to format
            
        Returns:
            Colored log message
        """
        # Get color for this log level
        color = self.level_colors.get(record.levelno, '')
        
        # Format the record using the parent formatter
        formatted_message = super().format(record)
        
        # Apply color and reset at the end
        return f"{color}{formatted_message}{colorama.Style.RESET_ALL}"
    
    def formatException(self, exc_info: Tuple) -> str:
        """
        Format exception information with error color.
        
        Args:
            exc_info: Exception information tuple
            
        Returns:
            Colored exception information
        """
        exception_text = format_exception_info(exc_info)
        # Use the error color for exceptions
        return f"{colorama.Fore.RED}{exception_text}{colorama.Style.RESET_ALL}"


class StructuredFormatter(logging.Formatter):
    """
    Formatter that outputs structured log messages with consistent fields and formatting.
    
    This formatter is particularly useful for staging environments where logs need
    to be readable by both humans and machines.
    """
    
    def __init__(self, fmt: str = DEFAULT_FORMAT, 
                date_fmt: str = DEFAULT_DATE_FORMAT,
                include_context: bool = True):
        """
        Initialize the structured formatter.
        
        Args:
            fmt: Format string for log messages
            date_fmt: Format string for timestamps
            include_context: Whether to include context information
        """
        super().__init__(fmt, date_fmt)
        self.include_context = include_context
        
    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record in a structured format with consistent fields.
        
        Args:
            record: The log record to format
            
        Returns:
            Structured log message
        """
        # Format the record using the parent formatter
        formatted_message = super().format(record)
        
        # Add context information if enabled
        if self.include_context:
            context_info = self.format_context(record)
            if context_info:
                formatted_message = f"{formatted_message} | {context_info}"
        
        # Include any extra attributes in a consistent format
        extras = []
        for key, value in record.__dict__.items():
            if key not in ('args', 'asctime', 'created', 'exc_info', 'exc_text', 
                          'filename', 'funcName', 'id', 'levelname', 'levelno',
                          'lineno', 'module', 'msecs', 'message', 'msg', 'name', 
                          'pathname', 'process', 'processName', 'relativeCreated', 
                          'stack_info', 'thread', 'threadName', 'correlation_id',
                          'execution_id', 'pipeline_id', 'component'):
                extras.append(f"{key}={value}")
        
        if extras:
            formatted_message = f"{formatted_message} | {' '.join(extras)}"
        
        return formatted_message
    
    def formatException(self, exc_info: Tuple) -> str:
        """
        Format exception information in a structured way.
        
        Args:
            exc_info: Exception information tuple
            
        Returns:
            Structured exception information
        """
        exception_text = format_exception_info(exc_info)
        return f"Exception: {exception_text}"
    
    def format_context(self, record: logging.LogRecord) -> str:
        """
        Format context information from the log record.
        
        Args:
            record: The log record to format
            
        Returns:
            Formatted context information
        """
        context_items = []
        
        # Check for common context attributes
        if hasattr(record, 'correlation_id'):
            context_items.append(f"correlation_id={record.correlation_id}")
        
        if hasattr(record, 'execution_id'):
            context_items.append(f"execution_id={record.execution_id}")
            
        if hasattr(record, 'pipeline_id'):
            context_items.append(f"pipeline_id={record.pipeline_id}")
            
        if hasattr(record, 'component'):
            context_items.append(f"component={record.component}")
        
        return " ".join(context_items)