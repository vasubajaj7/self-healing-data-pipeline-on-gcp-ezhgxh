"""
Email notification service for the self-healing data pipeline.

Handles formatting, sending, and tracking email notifications for various pipeline events,
errors, and status updates. Supports different email templates based on alert severity and type.
"""

import smtplib
import uuid
import datetime
from typing import Dict, List, Optional, Any, Union
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate, make_msgid, formataddr

import jinja2  # version 3.1.2

from ...constants import AlertSeverity, NotificationChannel
from ...config import get_config
from ...logging_config import get_logger

# Configure logger
logger = get_logger(__name__)

# Email template constants
TEMPLATE_DIR = "src/backend/monitoring/templates/email"
DEFAULT_TEMPLATE = "generic_alert.html"
ALERT_TEMPLATES = {
    "pipeline_failure": "pipeline_failure.html",
    "data_quality": "data_quality_alert.html",
    "performance": "performance_alert.html",
    "resource": "resource_alert.html",
    "security": "security_alert.html",
}


def format_email_subject(message: Dict[str, Any]) -> str:
    """
    Formats the email subject based on alert severity and type.
    
    Args:
        message: Alert message data containing severity and alert details
        
    Returns:
        Formatted email subject
    """
    severity = message.get("severity", AlertSeverity.INFO.value)
    alert_type = message.get("alert_type", "notification")
    pipeline_name = message.get("pipeline_name", "")
    
    # Add severity prefix
    if severity == AlertSeverity.CRITICAL.value:
        prefix = "[CRITICAL]"
    elif severity == AlertSeverity.HIGH.value:
        prefix = "[HIGH]"
    elif severity == AlertSeverity.MEDIUM.value:
        prefix = "[MEDIUM]"
    elif severity == AlertSeverity.LOW.value:
        prefix = "[LOW]"
    else:
        prefix = "[INFO]"
    
    # Format the subject
    if pipeline_name:
        subject = f"{prefix} {pipeline_name} - {message.get('title', alert_type)}"
    else:
        subject = f"{prefix} {message.get('title', alert_type)}"
    
    return subject


def get_template_for_alert_type(alert_type: str) -> str:
    """
    Determines the appropriate email template based on alert type.
    
    Args:
        alert_type: Type of alert
        
    Returns:
        Template name to use
    """
    return ALERT_TEMPLATES.get(alert_type, DEFAULT_TEMPLATE)


def format_email_body(message: Dict[str, Any], template_name: str) -> str:
    """
    Formats the email body using the appropriate template.
    
    Args:
        message: Alert message data
        template_name: Name of the template to use
        
    Returns:
        Formatted email body (HTML)
    """
    try:
        # Set up Jinja2 environment
        template_loader = jinja2.FileSystemLoader(searchpath=TEMPLATE_DIR)
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(template_name)
        
        # Prepare context for template
        context = {
            "alert": message,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "pipeline_name": message.get("pipeline_name", ""),
            "message": message.get("message", ""),
            "details": message.get("details", {}),
            "severity": message.get("severity", AlertSeverity.INFO.value),
            "alert_type": message.get("alert_type", "notification"),
        }
        
        # Add severity-specific styling
        severity = message.get("severity", AlertSeverity.INFO.value)
        if severity == AlertSeverity.CRITICAL.value:
            context["severity_color"] = "#ff0000"  # Red
            context["severity_icon"] = "🚨"
        elif severity == AlertSeverity.HIGH.value:
            context["severity_color"] = "#ff7700"  # Orange
            context["severity_icon"] = "⚠️"
        elif severity == AlertSeverity.MEDIUM.value:
            context["severity_color"] = "#ffcc00"  # Yellow
            context["severity_icon"] = "⚠️"
        elif severity == AlertSeverity.LOW.value:
            context["severity_color"] = "#3777ff"  # Blue
            context["severity_icon"] = "ℹ️"
        else:
            context["severity_color"] = "#37af87"  # Green
            context["severity_icon"] = "ℹ️"
            
        # Render the template
        return template.render(**context)
    except Exception as e:
        logger.error(f"Error formatting email body: {e}")
        # Fallback to basic formatting
        return f"""
        <html>
        <body>
            <h2>{message.get('title', 'Alert Notification')}</h2>
            <p>{message.get('message', '')}</p>
            <p>Timestamp: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        </body>
        </html>
        """


class EmailDeliveryResult:
    """
    Data class representing the result of an email notification delivery attempt.
    """
    
    def __init__(self, message_id: str, recipients: List[str], success: bool, 
                 error_message: Optional[str] = None, delivery_details: Optional[Dict] = None):
        """
        Initializes a new EmailDeliveryResult instance.
        
        Args:
            message_id: Unique identifier for the message
            recipients: List of email recipients
            success: Whether delivery was successful
            error_message: Error message if delivery failed
            delivery_details: Additional delivery details
        """
        self.message_id = message_id
        self.recipients = recipients
        self.success = success
        self.error_message = error_message
        self.timestamp = datetime.datetime.now()
        self.delivery_details = delivery_details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the delivery result to a dictionary representation.
        
        Returns:
            Dictionary representation of the delivery result
        """
        # Create dictionary with all properties
        result = {
            "message_id": self.message_id,
            "recipients": [self._mask_email(r) for r in self.recipients],  # Mask emails for privacy
            "success": self.success,
            "timestamp": self.timestamp.isoformat(),
            "delivery_details": self.delivery_details
        }
        
        # Add error message if present
        if self.error_message:
            result["error_message"] = self.error_message
            
        return result
    
    @staticmethod
    def _mask_email(email: str) -> str:
        """
        Masks an email address for privacy in logs.
        
        Args:
            email: Email address to mask
            
        Returns:
            Masked email address
        """
        if not email or '@' not in email:
            return email
            
        username, domain = email.split('@', 1)
        if len(username) <= 2:
            masked_username = username[0] + '*'
        else:
            masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
            
        domain_parts = domain.split('.')
        masked_domain = '.'.join([
            part[0] + '*' * (len(part) - 1) if i < len(domain_parts) - 1 else part
            for i, part in enumerate(domain_parts)
        ])
        
        return f"{masked_username}@{masked_domain}"
    
    @classmethod
    def from_dict(cls, result_dict: Dict[str, Any]) -> 'EmailDeliveryResult':
        """
        Creates an EmailDeliveryResult instance from a dictionary.
        
        Args:
            result_dict: Dictionary containing delivery result data
            
        Returns:
            EmailDeliveryResult instance
        """
        message_id = result_dict.get("message_id", "")
        recipients = result_dict.get("recipients", [])
        success = result_dict.get("success", False)
        error_message = result_dict.get("error_message")
        delivery_details = result_dict.get("delivery_details", {})
        
        result = cls(message_id, recipients, success, error_message, delivery_details)
        
        # Set timestamp if present in dictionary
        timestamp_str = result_dict.get("timestamp")
        if timestamp_str:
            try:
                result.timestamp = datetime.datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError):
                pass
                
        return result


class EmailNotifier:
    """
    Handles sending email notifications for pipeline events and alerts.
    """
    
    def __init__(self):
        """
        Initializes the EmailNotifier with necessary configuration.
        """
        # Get configuration
        config = get_config()
        self._config = config.get("notifications", {}).get("email", {})
        
        # Set up Jinja2 template environment
        self._template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(TEMPLATE_DIR),
            autoescape=True
        )
        
        # Initialize delivery history tracking
        self._delivery_history = {}
        
        # Configure SMTP settings
        self._smtp_server = self._config.get("smtp_server", "")
        self._smtp_port = int(self._config.get("smtp_port", 587))
        self._smtp_username = self._config.get("username", "")
        self._smtp_password = self._config.get("password", "")
        self._use_tls = self._config.get("use_tls", True)
        
        # Set sender and default recipients
        self._sender_email = self._config.get("sender_email", "")
        self._default_recipients = self._config.get("default_recipients", [])
        
        # Validate configuration
        if not self.validate_config():
            logger.warning("Email notifier not properly configured")
        else:
            logger.info("Email notifier initialized")
    
    def validate_config(self) -> bool:
        """
        Validates the email configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        # Check required configuration values
        if not self._smtp_server:
            logger.error("SMTP server not configured")
            return False
            
        if not self._smtp_port:
            logger.error("SMTP port not configured")
            return False
            
        if not self._sender_email:
            logger.error("Sender email not configured")
            return False
            
        # Validate email format for sender
        if '@' not in self._sender_email:
            logger.error(f"Invalid sender email format: {self._sender_email}")
            return False
            
        # Validate recipient email formats if default recipients provided
        for recipient in self._default_recipients:
            if '@' not in recipient:
                logger.error(f"Invalid recipient email format: {recipient}")
                return False
                
        return True
    
    def test_connection(self) -> bool:
        """
        Tests the SMTP connection to verify configuration.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Connect to SMTP server
            smtp = smtplib.SMTP(self._smtp_server, self._smtp_port)
            
            # Use TLS if configured
            if self._use_tls:
                smtp.starttls()
            
            # Authenticate if credentials provided
            if self._smtp_username and self._smtp_password:
                smtp.login(self._smtp_username, self._smtp_password)
            
            # Verify connection with NOOP command
            smtp.noop()
            
            # Close connection
            smtp.quit()
            
            logger.info("SMTP connection test successful")
            return True
        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False
    
    def send_notification(self, message: Dict[str, Any], recipients: Optional[List[str]] = None) -> bool:
        """
        Sends an email notification.
        
        Args:
            message: Message data containing alert details
            recipients: List of email recipients (falls back to default recipients if not provided)
            
        Returns:
            True if email was sent successfully, False otherwise
        """
        # Generate unique message ID
        message_id = str(uuid.uuid4())
        
        # Use provided recipients or fall back to default
        actual_recipients = recipients or self._default_recipients
        if not actual_recipients:
            logger.error("No recipients specified and no default recipients configured")
            return False
        
        try:
            # Validate message format and required fields
            if not isinstance(message, dict):
                logger.error("Message must be a dictionary")
                return False
                
            # Determine appropriate template for the alert type
            alert_type = message.get("alert_type", "notification")
            template_name = get_template_for_alert_type(alert_type)
            
            # Format email subject based on alert severity and type
            subject = format_email_subject(message)
            
            # Format email body using the template
            html_body = format_email_body(message, template_name)
            
            # Create MIME multipart message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self._sender_email
            msg['To'] = ", ".join(actual_recipients)
            msg['Date'] = formatdate(localtime=True)
            msg['Message-ID'] = make_msgid(domain=self._sender_email.split('@')[-1])
            
            # Add HTML and plain text parts to the message
            # First create a simple text version
            text_body = f"Alert: {message.get('title', '')}\n\n"
            text_body += f"Message: {message.get('message', '')}\n"
            text_body += f"Severity: {message.get('severity', '')}\n"
            text_body += f"Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            
            # Add details if available
            details = message.get("details", {})
            if details:
                text_body += "\nDetails:\n"
                for key, value in details.items():
                    text_body += f"- {key}: {value}\n"
            
            # Attach parts
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            # Add attachments if provided in the message
            attachments = message.get("attachments", [])
            for attachment in attachments:
                self.add_attachment(
                    msg, 
                    attachment.get("content", b""), 
                    attachment.get("filename", "attachment.txt"),
                    attachment.get("content_type", "application/octet-stream")
                )
            
            # Connect to SMTP server with appropriate security
            smtp = smtplib.SMTP(self._smtp_server, self._smtp_port)
            
            # Use TLS if configured
            if self._use_tls:
                smtp.starttls()
            
            # Authenticate if credentials provided
            if self._smtp_username and self._smtp_password:
                smtp.login(self._smtp_username, self._smtp_password)
            
            # Send email to all recipients
            smtp.sendmail(self._sender_email, actual_recipients, msg.as_string())
            
            # Close SMTP connection
            smtp.quit()
            
            # Log delivery status
            logger.info(f"Email notification sent successfully to {len(actual_recipients)} recipients")
            
            # Update delivery history
            delivery_result = EmailDeliveryResult(
                message_id=message_id,
                recipients=actual_recipients,
                success=True,
                delivery_details={
                    "subject": subject,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "alert_type": alert_type,
                    "severity": message.get("severity", AlertSeverity.INFO.value)
                }
            )
            self._delivery_history[message_id] = delivery_result.to_dict()
            
            return True
        except Exception as e:
            # Log error
            logger.error(f"Failed to send email notification: {e}")
            
            # Update delivery history
            delivery_result = EmailDeliveryResult(
                message_id=message_id,
                recipients=actual_recipients,
                success=False,
                error_message=str(e),
                delivery_details={
                    "subject": subject if 'subject' in locals() else "Error occurred before subject creation",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "alert_type": message.get("alert_type", "notification"),
                    "severity": message.get("severity", AlertSeverity.INFO.value)
                }
            )
            self._delivery_history[message_id] = delivery_result.to_dict()
            
            return False
    
    def send_batch_notifications(self, message: Dict[str, Any], recipient_groups: List[List[str]]) -> Dict[str, bool]:
        """
        Sends the same notification to multiple recipient groups.
        
        Args:
            message: Message data containing alert details
            recipient_groups: List of recipient groups (each a list of email addresses)
            
        Returns:
            Dictionary of delivery results by recipient group
        """
        # Validate message and recipient groups list
        if not isinstance(message, dict):
            logger.error("Message must be a dictionary")
            return {}
            
        if not isinstance(recipient_groups, list):
            logger.error("Recipient groups must be a list")
            return {}
            
        # Initialize results dictionary
        results = {}
        
        # Loop through recipient groups and send individual notifications
        for i, recipients in enumerate(recipient_groups):
            group_key = f"group_{i}"
            if isinstance(recipients, list) and recipients:
                results[group_key] = self.send_notification(message, recipients)
            else:
                logger.warning(f"Skipping invalid recipient group: {recipients}")
                results[group_key] = False
                
        # Track success/failure for each group
        return results
    
    def get_delivery_status(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the delivery status for a specific message.
        
        Args:
            message_id: Message ID to check
            
        Returns:
            Delivery status details or None if not found
        """
        # Check if message exists in delivery history
        return self._delivery_history.get(message_id)
    
    def add_attachment(self, message: MIMEMultipart, content: bytes, filename: str, content_type: str) -> None:
        """
        Adds an attachment to an email message.
        
        Args:
            message: MIME message to add attachment to
            content: Attachment content as bytes
            filename: Attachment filename
            content_type: MIME content type
        """
        # Create MIME application part with content
        part = MIMEApplication(content)
        
        # Set content type and attachment filename
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        part.add_header('Content-Type', content_type)
        
        # Add the attachment to the multipart message
        message.attach(part)
    
    def cleanup_delivery_history(self) -> int:
        """
        Cleans up old delivery history records.
        
        Returns:
            Number of records cleaned up
        """
        # Identify delivery records older than retention period
        retention_days = self._config.get("history_retention_days", 7)
        retention_delta = datetime.timedelta(days=retention_days)
        cutoff_time = datetime.datetime.now() - retention_delta
        
        records_to_remove = []
        for message_id, record in self._delivery_history.items():
            try:
                timestamp_str = record.get("timestamp")
                if timestamp_str:
                    timestamp = datetime.datetime.fromisoformat(timestamp_str)
                    if timestamp < cutoff_time:
                        records_to_remove.append(message_id)
            except (ValueError, TypeError):
                # If timestamp is invalid, consider it for removal
                records_to_remove.append(message_id)
        
        # Remove them from delivery history dictionary
        for message_id in records_to_remove:
            del self._delivery_history[message_id]
            
        logger.info(f"Cleaned up {len(records_to_remove)} old delivery history records")
        return len(records_to_remove)