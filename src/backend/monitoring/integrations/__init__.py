"""
Entry point for the monitoring integrations module that provides a unified
interface for external service integrations such as Cloud Monitoring,
Microsoft Teams notifications, email notifications, and log analysis.
This module exposes key classes and functions from submodules to simplify
imports for consumers.
"""

import logging  # Standard library for logging

from .cloud_monitoring import (  # Internal module for Cloud Monitoring integration
    CloudMonitoringClient,  # Class for interacting with Cloud Monitoring
    CloudMonitoringException,  # Exception class for Cloud Monitoring errors
    convert_to_monitoring_type,  # Function to convert metric types
    format_resource_labels,  # Function to format resource labels
    get_monitored_resource,  # Function to create monitored resource objects
    DEFAULT_MONITORING_SCOPES,  # Default OAuth scopes for Cloud Monitoring API
    DEFAULT_METRIC_PREFIX  # Default prefix for custom metrics in Cloud Monitoring
)
from .teams_notifier import (  # Internal module for Microsoft Teams notifications
    TeamsNotifier,  # Class for sending notifications to Microsoft Teams
    TeamsDeliveryResult,  # Class representing Teams notification delivery results
    format_teams_card  # Function to format Teams adaptive cards
)
from .email_notifier import (  # Internal module for email notifications
    EmailNotifier,  # Class for sending email notifications
    EmailDeliveryResult,  # Class representing email notification delivery results
    format_email_subject,  # Function to format email subjects
    format_email_body  # Function to format email bodies
)
from .logs_analyzer import (  # Internal module for log analysis
    LogsAnalyzer,  # Class for analyzing logs to extract insights and identify issues
    LogPattern,  # Class representing a pattern identified in log entries
    RootCauseAnalysis,  # Class representing the results of a root cause analysis
    extract_error_patterns,  # Function to extract common error patterns from logs
    identify_root_cause  # Function to identify the root cause of a pipeline failure
)
from ...logging_config import get_logger  # Internal module for logging configuration

# Initialize logger for this module
logger = get_logger(__name__)

# Define module version
__version__ = "1.0.0"

# Define what to expose from the module
__all__ = [
    "CloudMonitoringClient",
    "CloudMonitoringException",
    "convert_to_monitoring_type",
    "format_resource_labels",
    "get_monitored_resource",
    "DEFAULT_MONITORING_SCOPES",
    "DEFAULT_METRIC_PREFIX",
    "TeamsNotifier",
    "TeamsDeliveryResult",
    "format_teams_card",
    "EmailNotifier",
    "EmailDeliveryResult",
    "format_email_subject",
    "format_email_body",
    "LogsAnalyzer",
    "LogPattern",
    "RootCauseAnalysis",
    "extract_error_patterns",
    "identify_root_cause"
]