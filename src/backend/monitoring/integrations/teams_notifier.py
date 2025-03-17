"""
Module for sending notifications to Microsoft Teams channels.

This module provides functionality to send formatted notifications to Microsoft Teams
channels using webhooks. It supports rich formatting with adaptive cards and tracks
delivery status for monitoring purposes.
"""

import json
import uuid
import logging
import requests
import datetime
from typing import Dict, List, Any, Optional, Union

from backend.constants import AlertSeverity, NotificationChannel
from backend.config import get_config
from backend.logging_config import get_logger

# Initialize module logger
logger = get_logger(__name__)


def format_teams_card(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formats a Teams adaptive card based on alert severity and type.
    
    Args:
        message: Dictionary containing alert information including severity,
                 type, title, and details.
                
    Returns:
        Dictionary containing the formatted Teams adaptive card payload.
    """
    severity = message.get('severity', AlertSeverity.INFO)
    alert_type = message.get('alert_type', 'general')
    title = message.get('title', 'Pipeline Alert')
    details = message.get('details', {})
    
    # Get styling based on severity
    styling = get_severity_styling(severity)
    
    # Get base template for the alert type
    card_template = get_card_template_for_alert_type(alert_type)
    
    # Build adaptive card
    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "msteams": {
                        "width": "full"
                    },
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "medium",
                            "weight": "bolder",
                            "color": styling["color"],
                            "text": f"{styling['icon']} {styling['prefix']}: {title}"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Time",
                                    "value": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                },
                                {
                                    "title": "Severity",
                                    "value": severity.value if isinstance(severity, AlertSeverity) else str(severity)
                                },
                                {
                                    "title": "Type",
                                    "value": alert_type
                                }
                            ]
                        }
                    ],
                    "actions": []
                }
            }
        ]
    }
    
    # Add alert type specific sections
    card_content = card["attachments"][0]["content"]
    
    # Add message body
    if message.get('message'):
        card_content["body"].append({
            "type": "TextBlock",
            "text": message["message"],
            "wrap": True
        })
    
    # Add details section based on template
    if details:
        facts = []
        for key, value in details.items():
            # Format duration and timestamp values
            if key.endswith('_time') and isinstance(value, (int, float)):
                value = datetime.datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
            elif key.endswith('_duration') and isinstance(value, (int, float)):
                # Format as minutes and seconds
                minutes, seconds = divmod(int(value), 60)
                value = f"{minutes}m {seconds}s"
                
            facts.append({
                "title": key.replace('_', ' ').title(),
                "value": str(value)
            })
            
        if facts:
            card_content["body"].append({
                "type": "FactSet",
                "facts": facts
            })
    
    # Add actions based on alert type
    if alert_type == "pipeline_failure":
        card_content["actions"].append({
            "type": "Action.OpenUrl",
            "title": "View Pipeline",
            "url": details.get("pipeline_url", "#")
        })
    elif alert_type == "quality_issue":
        card_content["actions"].append({
            "type": "Action.OpenUrl",
            "title": "View Data Quality Report",
            "url": details.get("quality_report_url", "#")
        })
    
    # Add acknowledge button if message is acknowledgeable
    if message.get("acknowledgeable", False):
        card_content["actions"].append({
            "type": "Action.OpenUrl",
            "title": "Acknowledge",
            "url": message.get("acknowledge_url", "#")
        })
    
    # Add view details button if URL provided
    if message.get("details_url"):
        card_content["actions"].append({
            "type": "Action.OpenUrl",
            "title": "View Details",
            "url": message["details_url"]
        })
    
    return card


def get_card_template_for_alert_type(alert_type: str) -> Dict[str, Any]:
    """
    Determines the appropriate Teams card template based on alert type.
    
    Args:
        alert_type: The type of alert (e.g., pipeline_failure, quality_issue)
        
    Returns:
        Dictionary containing the base card template to use
    """
    templates = {
        "pipeline_failure": {
            "sections": ["pipeline_info", "error_details", "execution_stats"]
        },
        "quality_issue": {
            "sections": ["dataset_info", "quality_metrics", "validation_details"]
        },
        "performance_issue": {
            "sections": ["resource_info", "performance_metrics", "historical_comparison"]
        },
        "healing_action": {
            "sections": ["issue_info", "action_details", "result_summary"]
        },
        "system_warning": {
            "sections": ["component_info", "warning_details"]
        }
    }
    
    return templates.get(alert_type, {"sections": ["general_info"]})


def get_severity_styling(severity: AlertSeverity) -> Dict[str, str]:
    """
    Gets styling elements based on alert severity.
    
    Args:
        severity: The severity level of the alert
        
    Returns:
        Dictionary containing styling elements including color, icon, and prefix
    """
    # Default to INFO if severity is not an AlertSeverity enum
    if not isinstance(severity, AlertSeverity):
        try:
            severity = AlertSeverity(severity)
        except (ValueError, TypeError):
            severity = AlertSeverity.INFO
    
    styles = {
        AlertSeverity.CRITICAL: {
            "color": "attention",
            "icon": "🔴",
            "prefix": "CRITICAL ALERT"
        },
        AlertSeverity.HIGH: {
            "color": "warning",
            "icon": "⚠️",
            "prefix": "HIGH ALERT"
        },
        AlertSeverity.MEDIUM: {
            "color": "accent",
            "icon": "ℹ️",
            "prefix": "ALERT"
        },
        AlertSeverity.LOW: {
            "color": "good",
            "icon": "📌",
            "prefix": "Notification"
        },
        AlertSeverity.INFO: {
            "color": "default",
            "icon": "📋",
            "prefix": "Info"
        }
    }
    
    return styles.get(severity, styles[AlertSeverity.INFO])


class TeamsDeliveryResult:
    """
    Represents the result of a Teams notification delivery attempt.
    
    This class stores information about the delivery status, any errors encountered,
    and metadata about the message and webhook.
    """
    
    def __init__(self, message_id: str, webhook_key: str, success: bool, 
                 error_message: Optional[str] = None, delivery_details: Optional[Dict[str, Any]] = None):
        """
        Initializes a new TeamsDeliveryResult instance.
        
        Args:
            message_id: Unique identifier for the message
            webhook_key: Key identifying the webhook used for delivery
            success: Whether the delivery was successful
            error_message: Error message if delivery failed, None otherwise
            delivery_details: Additional details about the delivery
        """
        self.message_id = message_id
        self.webhook_key = webhook_key
        self.success = success
        self.error_message = error_message
        self.timestamp = datetime.datetime.now()
        self.delivery_details = delivery_details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the delivery result to a dictionary representation.
        
        Returns:
            Dictionary containing all delivery result properties
        """
        return {
            "message_id": self.message_id,
            "webhook_key": self.webhook_key,
            "success": self.success,
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat(),
            "delivery_details": self.delivery_details
        }
    
    @classmethod
    def from_dict(cls, result_dict: Dict[str, Any]) -> 'TeamsDeliveryResult':
        """
        Creates a TeamsDeliveryResult instance from a dictionary.
        
        Args:
            result_dict: Dictionary containing delivery result data
            
        Returns:
            New TeamsDeliveryResult instance
        """
        result = cls(
            message_id=result_dict["message_id"],
            webhook_key=result_dict["webhook_key"],
            success=result_dict["success"],
            error_message=result_dict.get("error_message"),
            delivery_details=result_dict.get("delivery_details", {})
        )
        
        # Convert timestamp string to datetime object
        if "timestamp" in result_dict:
            try:
                result.timestamp = datetime.datetime.fromisoformat(result_dict["timestamp"])
            except (ValueError, TypeError):
                pass
                
        return result


class TeamsNotifier:
    """
    Handles sending notifications to Microsoft Teams channels via webhooks.
    
    This class manages webhook configurations, formats messages as adaptive cards,
    sends notifications to Teams channels, and tracks delivery status.
    """
    
    def __init__(self):
        """
        Initializes the TeamsNotifier with necessary configuration.
        
        Loads webhook URLs from configuration, initializes tracking structures,
        and sets up HTTP session for connection pooling.
        """
        self._config = get_config().get("notifications", {}).get("teams", {})
        self._webhook_urls = self._config.get("webhooks", {})
        self._delivery_history = {}
        self._session = requests.Session()
        
        if not self.validate_config():
            logger.warning("Teams notification configuration is invalid or incomplete")
        else:
            logger.info(f"TeamsNotifier initialized with {len(self._webhook_urls)} webhooks")
    
    def send_notification(self, message: Dict[str, Any], webhook_key: str = "default") -> TeamsDeliveryResult:
        """
        Sends a notification to a Microsoft Teams channel.
        
        Args:
            message: Dictionary containing message details including title, message, 
                    severity, alert_type, and other notification-specific fields
            webhook_key: Key identifying which webhook URL to use
            
        Returns:
            TeamsDeliveryResult containing delivery status and details
        """
        message_id = str(uuid.uuid4())
        
        # Add message ID to message for tracking
        message["message_id"] = message_id
        
        # Get webhook URL
        webhook_url = self._webhook_urls.get(webhook_key)
        if not webhook_url:
            logger.error(f"No webhook URL found for key: {webhook_key}")
            return TeamsDeliveryResult(
                message_id=message_id,
                webhook_key=webhook_key,
                success=False,
                error_message=f"Webhook URL not found for key: {webhook_key}"
            )
        
        # Format message as Teams card
        try:
            teams_card = format_teams_card(message)
        except Exception as e:
            logger.exception(f"Error formatting Teams card: {e}")
            return TeamsDeliveryResult(
                message_id=message_id,
                webhook_key=webhook_key,
                success=False,
                error_message=f"Error formatting Teams card: {str(e)}"
            )
        
        # Send to Teams webhook
        try:
            response = self._session.post(
                webhook_url, 
                json=teams_card,
                timeout=10  # 10 second timeout
            )
            
            success = 200 <= response.status_code < 300
            error_message = None if success else f"HTTP {response.status_code}: {response.text}"
            
            delivery_details = {
                "status_code": response.status_code,
                "response_text": response.text[:100] if response.text else "",
                "headers": dict(response.headers)
            }
            
            if not success:
                logger.error(f"Failed to send Teams notification: {error_message}")
            else:
                logger.info(f"Teams notification sent successfully: {message_id}")
                
        except requests.RequestException as e:
            logger.exception(f"Error sending Teams notification: {e}")
            success = False
            error_message = f"Request error: {str(e)}"
            delivery_details = {"exception": str(e)}
        
        # Create delivery result
        result = TeamsDeliveryResult(
            message_id=message_id,
            webhook_key=webhook_key,
            success=success,
            error_message=error_message,
            delivery_details=delivery_details
        )
        
        # Store in delivery history
        self._delivery_history[message_id] = result.to_dict()
        
        return result
    
    def send_batch_notifications(self, message: Dict[str, Any], webhook_keys: List[str]) -> Dict[str, TeamsDeliveryResult]:
        """
        Sends the same notification to multiple Teams channels.
        
        Args:
            message: Dictionary containing message details
            webhook_keys: List of webhook keys to send to
            
        Returns:
            Dictionary mapping webhook keys to their delivery results
        """
        results = {}
        
        for key in webhook_keys:
            results[key] = self.send_notification(message, key)
            
        return results
    
    def get_delivery_status(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the delivery status for a specific message.
        
        Args:
            message_id: ID of the message to check
            
        Returns:
            Dictionary containing delivery status details or None if not found
        """
        return self._delivery_history.get(message_id)
    
    def validate_config(self) -> bool:
        """
        Validates the Teams webhook configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        if not self._webhook_urls:
            logger.error("No Teams webhooks configured")
            return False
        
        # Check if at least one webhook URL is valid
        for key, url in self._webhook_urls.items():
            if url and isinstance(url, str) and url.startswith(("https://", "http://")):
                return True
        
        logger.error("No valid Teams webhook URLs found in configuration")
        return False
    
    def test_webhook(self, webhook_key: str = "default") -> bool:
        """
        Tests a webhook connection by sending a test message.
        
        Args:
            webhook_key: Key identifying which webhook URL to test
            
        Returns:
            True if test successful, False otherwise
        """
        test_message = {
            "title": "Test Notification",
            "message": "This is a test notification from the self-healing pipeline.",
            "severity": AlertSeverity.INFO,
            "alert_type": "test",
            "details": {
                "test_id": str(uuid.uuid4()),
                "timestamp": datetime.datetime.now().isoformat()
            }
        }
        
        result = self.send_notification(test_message, webhook_key)
        return result.success
    
    def add_webhook(self, key: str, url: str, description: str = "") -> bool:
        """
        Adds a new webhook URL to the configuration.
        
        Args:
            key: Key to identify the webhook
            url: Webhook URL
            description: Optional description of the webhook
            
        Returns:
            True if added successfully, False otherwise
        """
        if not url or not isinstance(url, str) or not url.startswith(("https://", "http://")):
            logger.error(f"Invalid webhook URL: {url}")
            return False
        
        self._webhook_urls[key] = url
        logger.info(f"Added webhook with key '{key}': {description}")
        return True
    
    def remove_webhook(self, key: str) -> bool:
        """
        Removes a webhook URL from the configuration.
        
        Args:
            key: Key identifying the webhook to remove
            
        Returns:
            True if removed, False if not found
        """
        if key in self._webhook_urls:
            del self._webhook_urls[key]
            logger.info(f"Removed webhook with key '{key}'")
            return True
        return False
    
    def cleanup_delivery_history(self, max_age_hours: int = 24) -> int:
        """
        Cleans up old delivery history records.
        
        Args:
            max_age_hours: Maximum age in hours to keep records
            
        Returns:
            Number of records cleaned up
        """
        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=max_age_hours)
        keys_to_remove = []
        
        for msg_id, delivery in self._delivery_history.items():
            try:
                timestamp = datetime.datetime.fromisoformat(delivery["timestamp"])
                if timestamp < cutoff_time:
                    keys_to_remove.append(msg_id)
            except (KeyError, ValueError):
                # If we can't parse the timestamp, consider it for removal
                keys_to_remove.append(msg_id)
        
        for key in keys_to_remove:
            del self._delivery_history[key]
            
        logger.info(f"Cleaned up {len(keys_to_remove)} old delivery records")
        return len(keys_to_remove)