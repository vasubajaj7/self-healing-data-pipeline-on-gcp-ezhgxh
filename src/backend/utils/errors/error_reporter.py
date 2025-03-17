"""
Implements a centralized error reporting system for the self-healing data pipeline.
Responsible for capturing, formatting, and routing error information to appropriate
monitoring systems, logging services, and notification channels. Provides integration
with Cloud Monitoring for metrics and alerting.
"""

import datetime  # standard library
import json  # standard library
import traceback  # standard library
import typing  # standard library
import uuid  # standard library

from . import constants  # src/backend/constants.py
from . import config  # src/backend/config.py
from .utils.errors import error_types  # src/backend/utils/errors/error_types.py
from .utils.logging import logger  # src/backend/utils/logging/logger.py
from .utils.monitoring import metric_client  # src/backend/utils/monitoring/metric_client.py

# Initialize logger for this module
logger = logger.get_logger(__name__)


class ErrorReporter:
    """
    Central class for reporting errors to various monitoring and notification systems
    """

    def __init__(self, config_override: typing.Optional[dict] = None):
        """
        Initialize the error reporter with configuration and clients

        Args:
            config_override (dict, optional): Override configuration settings. Defaults to None.
        """
        # Load configuration from application settings
        self._config = config.get_config().get("error_reporting", {})

        # Apply any configuration overrides provided
        if config_override:
            self._config.update(config_override)

        # Initialize metric client for Cloud Monitoring
        self._metric_client = metric_client.MetricClient()

        # Set up notification channels based on configuration
        self._notification_channels = self._config.get("notification_channels", {})

        # Initialize error metrics tracking
        self._error_metrics = {}

        # Log successful initialization
        logger.info("ErrorReporter initialized successfully")

    def report_exception(
        self,
        exception: Exception,
        context: typing.Dict[str, typing.Any],
        severity: constants.AlertSeverity = constants.AlertSeverity.MEDIUM,
    ) -> str:
        """
        Report an exception to monitoring systems and notification channels

        Args:
            exception (Exception): The exception to report
            context (typing.Dict[str, typing.Any]): Contextual information about the error
            severity (constants.AlertSeverity, optional): Severity level of the alert. Defaults to constants.AlertSeverity.MEDIUM.

        Returns:
            str: Error report ID
        """
        # Generate unique error report ID
        report_id = self.generate_report_id()

        # Extract exception details (type, message, traceback)
        exception_type = type(exception).__name__
        exception_message = str(exception)
        exception_traceback = traceback.format_exc()

        # Format error report with all details
        error_report = self.format_error_report(
            report_id=report_id,
            exception=exception,
            context=context,
            severity=severity,
        )

        # Log the error with appropriate level based on severity
        log_level = severity.name
        logger.log(getattr(logger, log_level.lower()), f"Error reported: {error_report}")

        # Send error metrics to Cloud Monitoring
        self.send_error_metrics(error_report)

        # Send notifications based on severity and configuration
        self.send_notifications(error_report)

        # Return the error report ID
        return report_id

    def report_error(
        self,
        error_message: str,
        category: error_types.ErrorCategory,
        context: typing.Dict[str, typing.Any],
        severity: constants.AlertSeverity = constants.AlertSeverity.MEDIUM,
    ) -> str:
        """
        Report an error without an exception object

        Args:
            error_message (str): The error message to report
            category (error_types.ErrorCategory): The category of the error
            context (typing.Dict[str, typing.Any]): Contextual information about the error
            severity (constants.AlertSeverity, optional): Severity level of the alert. Defaults to constants.AlertSeverity.MEDIUM.

        Returns:
            str: Error report ID
        """
        # Generate unique error report ID
        report_id = self.generate_report_id()

        # Format error report with message, category, and context
        error_report = {
            "report_id": report_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": error_message,
            "category": category.value,
            "severity": severity.value,
            "context": context,
        }

        # Log the error with appropriate level based on severity
        log_level = severity.name
        logger.log(getattr(logger, log_level.lower()), f"Error reported: {error_report}")

        # Send error metrics to Cloud Monitoring
        self.send_error_metrics(error_report)

        # Send notifications based on severity and configuration
        self.send_notifications(error_report)

        # Return the error report ID
        return report_id

    def format_error_report(
        self,
        report_id: str,
        exception: Exception,
        context: typing.Dict[str, typing.Any],
        severity: constants.AlertSeverity,
    ) -> typing.Dict[str, typing.Any]:
        """
        Format an error report with all relevant details

        Args:
            report_id (str): Unique identifier for the error report
            exception (Exception): The exception object
            context (typing.Dict[str, typing.Any]): Contextual information about the error
            severity (constants.AlertSeverity): Severity level of the alert

        Returns:
            typing.Dict[str, typing.Any]: Formatted error report
        """
        # Create base report structure with ID and timestamp
        error_report = {
            "report_id": report_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "severity": severity.value,
            "context": context,
        }

        # Add exception details if exception is provided
        if exception:
            error_report["exception_type"] = type(exception).__name__
            error_report["message"] = str(exception)
            error_report["traceback"] = traceback.format_exc()

            # Add error category from PipelineError or default to UNKNOWN
            if isinstance(exception, error_types.PipelineError):
                error_report["category"] = exception.category.value
            else:
                error_report["category"] = error_types.ErrorCategory.UNKNOWN.value

        # Return the complete error report dictionary
        return error_report

    def send_error_metrics(self, error_report: typing.Dict[str, typing.Any]) -> bool:
        """
        Send error metrics to Cloud Monitoring

        Args:
            error_report (typing.Dict[str, typing.Any]): The formatted error report

        Returns:
            bool: Success status
        """
        try:
            # Extract category and severity from error report
            category = error_report.get("category", error_types.ErrorCategory.UNKNOWN.value)
            severity = error_report.get("severity", constants.AlertSeverity.MEDIUM.value)

            # Increment error count metric by category
            metric_type_category = f"error_count_by_category/{category}"
            self._metric_client.create_gauge_metric(
                metric_type=metric_type_category, value=1, labels={"severity": severity}
            )

            # Increment error count metric by severity
            metric_type_severity = f"error_count_by_severity/{severity}"
            self._metric_client.create_gauge_metric(metric_type=metric_type_severity, value=1, labels={"category": category})

            # Track error rate metrics
            # TODO: Implement error rate tracking (requires more complex logic)

            # Send custom metrics for specific error types if configured
            # TODO: Implement custom metrics based on configuration

            return True
        except Exception as e:
            logger.error(f"Error sending error metrics: {str(e)}")
            return False

    def send_notifications(self, error_report: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        """
        Send error notifications to configured channels

        Args:
            error_report (typing.Dict[str, typing.Any]): The formatted error report

        Returns:
            typing.Dict[str, typing.Any]: Notification results by channel
        """
        results = {}
        try:
            # Determine which channels to notify based on severity and configuration
            severity = constants.AlertSeverity(error_report["severity"])
            channels = self.get_notification_channels(severity)

            # Format notification for each channel type
            if constants.NOTIFICATION_TYPE_TEAMS in channels:
                teams_message = self.format_teams_notification(error_report)
                # TODO: Implement sending to Microsoft Teams
                results[constants.NOTIFICATION_TYPE_TEAMS] = "Not Implemented"

            if constants.NOTIFICATION_TYPE_EMAIL in channels:
                email_message = self.format_email_notification(error_report)
                # TODO: Implement sending email notifications
                results[constants.NOTIFICATION_TYPE_EMAIL] = "Not Implemented"

            # TODO: Implement creating tickets in ticketing system if configured
            results["ticketing_system"] = "Not Implemented"

            return results
        except Exception as e:
            logger.error(f"Error sending notifications: {str(e)}")
            return results

    def format_teams_notification(self, error_report: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        """
        Format an error notification for Microsoft Teams

        Args:
            error_report (typing.Dict[str, typing.Any]): The formatted error report

        Returns:
            typing.Dict[str, typing.Any]: Teams message card payload
        """
        # Create Teams message card structure
        teams_message = {
            "title": f"Error: {error_report.get('category', 'Unknown')}",
            "text": error_report.get("message", "No message provided"),
            "themeColor": "FF0000",  # Red for errors
            "sections": [],
        }

        # Set color based on severity
        severity = error_report.get("severity", constants.AlertSeverity.MEDIUM.value)
        if severity == constants.AlertSeverity.CRITICAL.value:
            teams_message["themeColor"] = "FF0000"  # Red
        elif severity == constants.AlertSeverity.HIGH.value:
            teams_message["themeColor"] = "FFA500"  # Orange
        elif severity == constants.AlertSeverity.MEDIUM.value:
            teams_message["themeColor"] = "FFFF00"  # Yellow
        else:
            teams_message["themeColor"] = "00FF00"  # Green

        # Add error details section
        teams_message["sections"].append(
            {
                "title": "Error Details",
                "facts": [
                    {"name": "Category", "value": error_report.get("category", "Unknown")},
                    {"name": "Severity", "value": error_report.get("severity", "Unknown")},
                    {"name": "Message", "value": error_report.get("message", "N/A")},
                ],
            }
        )

        # Add context section if context exists
        context = error_report.get("context")
        if context:
            context_facts = []
            for key, value in context.items():
                context_facts.append({"name": key, "value": str(value)})
            teams_message["sections"].append({"title": "Context", "facts": context_facts})

        # Add stack trace section if available
        traceback_text = error_report.get("traceback")
        if traceback_text:
            teams_message["sections"].append(
                {"title": "Stack Trace", "text": traceback_text}
            )

        # Add timestamp and report ID
        teams_message["sections"].append(
            {
                "facts": [
                    {"name": "Timestamp", "value": error_report.get("timestamp", "N/A")},
                    {"name": "Report ID", "value": error_report.get("report_id", "N/A")},
                ]
            }
        )

        # Return formatted Teams message card
        return teams_message

    def format_email_notification(self, error_report: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
        """
        Format an error notification for email

        Args:
            error_report (typing.Dict[str, typing.Any]): The formatted error report

        Returns:
            typing.Dict[str, typing.Any]: Email message payload
        """
        # Create email structure with subject and body
        email_message = {
            "subject": f"Error: {error_report.get('category', 'Unknown')} - {error_report.get('severity', 'N/A')}",
            "body": "",
            "recipients": [],
        }

        # Set subject based on error category and severity
        email_message["subject"] = f"Error: {error_report.get('category', 'Unknown')} - {error_report.get('severity', 'N/A')}"

        # Format HTML body with error details
        html_body = f"""
        <h1>Error Report</h1>
        <p><strong>Report ID:</strong> {error_report.get('report_id', 'N/A')}</p>
        <p><strong>Timestamp:</strong> {error_report.get('timestamp', 'N/A')}</p>
        <p><strong>Category:</strong> {error_report.get('category', 'Unknown')}</p>
        <p><strong>Severity:</strong> {error_report.get('severity', 'N/A')}</p>
        <p><strong>Message:</strong> {error_report.get('message', 'N/A')}</p>
        """

        # Include context and stack trace in body
        context = error_report.get("context")
        if context:
            html_body += "<h2>Context</h2><ul>"
            for key, value in context.items():
                html_body += f"<li><strong>{key}:</strong> {value}</li>"
            html_body += "</ul>"

        traceback_text = error_report.get("traceback")
        if traceback_text:
            html_body += f"<h2>Stack Trace</h2><pre>{traceback_text}</pre>"

        email_message["body"] = html_body

        # Add recipients based on severity and configuration
        # TODO: Implement recipient logic based on configuration

        # Return formatted email payload
        return email_message

    def get_notification_channels(self, severity: constants.AlertSeverity) -> typing.List[str]:
        """
        Get appropriate notification channels based on severity

        Args:
            severity (constants.AlertSeverity): The severity level of the alert

        Returns:
            typing.List[str]: List of notification channels to use
        """
        # Check configuration for severity-based channel mapping
        channel_mapping = self._notification_channels.get("severity_mapping", {})

        # For CRITICAL severity, use all available channels
        if severity == constants.AlertSeverity.CRITICAL:
            return list(self._notification_channels.keys())

        # For HIGH severity, use Teams and email
        elif severity == constants.AlertSeverity.HIGH:
            return [constants.NOTIFICATION_TYPE_TEAMS, constants.NOTIFICATION_TYPE_EMAIL]

        # For MEDIUM severity, use Teams only by default
        elif severity == constants.AlertSeverity.MEDIUM:
            return [constants.NOTIFICATION_TYPE_TEAMS]

        # For LOW severity, use minimal or no notifications
        else:
            return []  # No notifications for LOW and INFO

    def generate_report_id(self) -> str:
        """
        Generate a unique identifier for an error report

        Returns:
            str: Unique error report ID
        """
        # Generate a UUID4
        report_id = uuid.uuid4()

        # Return the UUID as a string
        return str(report_id)