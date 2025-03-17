"""
Alert model for the self-healing data pipeline.

This module defines the Alert model, which represents alerts generated from
monitoring events, quality issues, and pipeline failures. It includes functionality
for alert lifecycle management, notification tracking, and correlation with other
alerts.
"""

import datetime
import json
import logging
import typing
import uuid
from typing import Dict, List, Optional, Any, Union

from ...constants import AlertSeverity, NotificationChannel
from ...logging_config import get_logger

# Configure logger
logger = get_logger(__name__)

# Constants
ALERT_TABLE_NAME = "alerts"
ALERT_STATUS_NEW = "NEW"
ALERT_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
ALERT_STATUS_RESOLVED = "RESOLVED"
ALERT_STATUS_SUPPRESSED = "SUPPRESSED"


def get_alert_table_schema() -> List[Dict[str, Any]]:
    """
    Returns the BigQuery table schema for the alerts table.
    
    Returns:
        List of BigQuery SchemaField objects defining the alerts table schema
    """
    return [
        {"name": "alert_id", "type": "STRING", "mode": "REQUIRED", "description": "Unique identifier for the alert"},
        {"name": "alert_type", "type": "STRING", "mode": "REQUIRED", "description": "Type of alert (e.g., pipeline_failure, data_quality, etc.)"},
        {"name": "description", "type": "STRING", "mode": "REQUIRED", "description": "Human-readable description of the alert"},
        {"name": "severity", "type": "STRING", "mode": "REQUIRED", "description": "Alert severity level (CRITICAL, HIGH, MEDIUM, LOW, INFO)"},
        {"name": "context", "type": "STRING", "mode": "NULLABLE", "description": "JSON-encoded contextual information about the alert"},
        {"name": "component", "type": "STRING", "mode": "NULLABLE", "description": "Component that generated the alert"},
        {"name": "execution_id", "type": "STRING", "mode": "NULLABLE", "description": "Pipeline execution ID related to the alert"},
        {"name": "status", "type": "STRING", "mode": "REQUIRED", "description": "Current alert status (NEW, ACKNOWLEDGED, RESOLVED, SUPPRESSED)"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Timestamp when the alert was created"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "Timestamp when the alert was last updated"},
        {"name": "acknowledged_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Timestamp when the alert was acknowledged"},
        {"name": "resolved_at", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Timestamp when the alert was resolved"},
        {"name": "related_alerts", "type": "STRING", "mode": "NULLABLE", "description": "JSON-encoded list of related alert IDs"},
        {"name": "notifications", "type": "STRING", "mode": "NULLABLE", "description": "JSON-encoded list of notification records"},
        {"name": "acknowledgment_details", "type": "STRING", "mode": "NULLABLE", "description": "JSON-encoded details about the acknowledgment"},
        {"name": "resolution_details", "type": "STRING", "mode": "NULLABLE", "description": "JSON-encoded details about the resolution"},
    ]


def generate_alert_id() -> str:
    """
    Generates a unique ID for a new alert.
    
    Returns:
        Unique alert identifier
    """
    return str(uuid.uuid4())


class AlertNotification:
    """
    Model representing a notification sent for an alert.
    
    This class tracks the details of alert notifications, including the channel,
    recipient, success status, and timestamp.
    """
    
    def __init__(self, channel: NotificationChannel, recipient: str, success: bool, details: str):
        """
        Initializes a new AlertNotification instance.
        
        Args:
            channel: The notification channel used (e.g., EMAIL, TEAMS)
            recipient: The recipient of the notification
            success: Whether the notification was successfully delivered
            details: Additional details about the notification
        """
        self.channel = channel
        self.recipient = recipient
        self.success = success
        self.details = details
        self.timestamp = datetime.datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the notification to a dictionary representation.
        
        Returns:
            Dictionary representation of the notification
        """
        return {
            "channel": self.channel.value,
            "recipient": self.recipient,
            "success": self.success,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, notification_dict: Dict[str, Any]) -> 'AlertNotification':
        """
        Creates an AlertNotification instance from a dictionary.
        
        Args:
            notification_dict: Dictionary containing notification data
            
        Returns:
            AlertNotification instance
        """
        # Create notification with required fields
        notification = cls(
            channel=NotificationChannel(notification_dict["channel"]),
            recipient=notification_dict["recipient"],
            success=notification_dict["success"],
            details=notification_dict["details"]
        )
        
        # Set timestamp if present
        if "timestamp" in notification_dict:
            notification.timestamp = datetime.datetime.fromisoformat(notification_dict["timestamp"])
            
        return notification


class Alert:
    """
    Model representing an alert in the self-healing data pipeline.
    
    This class encapsulates all alert information and provides methods to manage
    the alert lifecycle, including acknowledgment, resolution, and notification tracking.
    """
    
    def __init__(
        self,
        alert_type: str,
        description: str,
        severity: AlertSeverity,
        context: Dict[str, Any],
        component: str = None,
        execution_id: str = None,
        alert_id: str = None
    ):
        """
        Initializes a new Alert instance.
        
        Args:
            alert_type: Type of alert (e.g., pipeline_failure, data_quality)
            description: Human-readable description of the alert
            severity: Severity level of the alert
            context: Additional contextual information about the alert
            component: Component that generated the alert
            execution_id: Pipeline execution ID related to the alert
            alert_id: Unique identifier for the alert (generated if not provided)
        """
        self.alert_id = alert_id or generate_alert_id()
        self.alert_type = alert_type
        self.description = description
        self.severity = severity
        self.context = context or {}
        self.component = component
        self.execution_id = execution_id
        self.status = ALERT_STATUS_NEW
        
        # Set timestamps
        self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        self.acknowledged_at = None
        self.resolved_at = None
        
        # Initialize tracking lists
        self.related_alerts = []
        self.notifications = []
        
        # Initialize details dictionaries
        self.acknowledgment_details = {}
        self.resolution_details = {}
        
        logger.info(f"Created new alert: {self.alert_id} - {self.alert_type} - {self.severity.value}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the alert to a dictionary representation.
        
        Returns:
            Dictionary representation of the alert
        """
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type,
            "description": self.description,
            "severity": self.severity.value,
            "context": self.context,
            "component": self.component,
            "execution_id": self.execution_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "related_alerts": self.related_alerts,
            "notifications": [notification.to_dict() for notification in self.notifications],
            "acknowledgment_details": self.acknowledgment_details,
            "resolution_details": self.resolution_details
        }
    
    @classmethod
    def from_dict(cls, alert_dict: Dict[str, Any]) -> 'Alert':
        """
        Creates an Alert instance from a dictionary.
        
        Args:
            alert_dict: Dictionary containing alert data
            
        Returns:
            Alert instance
        """
        # Create alert with required fields
        alert = cls(
            alert_type=alert_dict["alert_type"],
            description=alert_dict["description"],
            severity=AlertSeverity(alert_dict["severity"]),
            context=alert_dict.get("context", {}),
            component=alert_dict.get("component"),
            execution_id=alert_dict.get("execution_id"),
            alert_id=alert_dict.get("alert_id")
        )
        
        # Set additional fields if present
        if "status" in alert_dict:
            alert.status = alert_dict["status"]
            
        if "created_at" in alert_dict:
            alert.created_at = datetime.datetime.fromisoformat(alert_dict["created_at"])
            
        if "updated_at" in alert_dict:
            alert.updated_at = datetime.datetime.fromisoformat(alert_dict["updated_at"])
            
        if "acknowledged_at" in alert_dict and alert_dict["acknowledged_at"]:
            alert.acknowledged_at = datetime.datetime.fromisoformat(alert_dict["acknowledged_at"])
            
        if "resolved_at" in alert_dict and alert_dict["resolved_at"]:
            alert.resolved_at = datetime.datetime.fromisoformat(alert_dict["resolved_at"])
            
        if "related_alerts" in alert_dict:
            alert.related_alerts = alert_dict["related_alerts"]
            
        if "notifications" in alert_dict:
            alert.notifications = [
                AlertNotification.from_dict(notification) 
                for notification in alert_dict["notifications"]
            ]
            
        if "acknowledgment_details" in alert_dict:
            alert.acknowledgment_details = alert_dict["acknowledgment_details"]
            
        if "resolution_details" in alert_dict:
            alert.resolution_details = alert_dict["resolution_details"]
            
        return alert
    
    def to_bigquery_row(self) -> Dict[str, Any]:
        """
        Converts the alert to a format suitable for BigQuery insertion.
        
        Returns:
            Dictionary formatted for BigQuery insertion
        """
        alert_dict = self.to_dict()
        
        # Convert complex types to JSON strings for BigQuery
        alert_dict["context"] = json.dumps(alert_dict["context"])
        alert_dict["related_alerts"] = json.dumps(alert_dict["related_alerts"]) if alert_dict["related_alerts"] else None
        alert_dict["notifications"] = json.dumps(alert_dict["notifications"]) if alert_dict["notifications"] else None
        alert_dict["acknowledgment_details"] = json.dumps(alert_dict["acknowledgment_details"]) if alert_dict["acknowledgment_details"] else None
        alert_dict["resolution_details"] = json.dumps(alert_dict["resolution_details"]) if alert_dict["resolution_details"] else None
        
        return alert_dict
    
    @classmethod
    def from_bigquery_row(cls, row: Dict[str, Any]) -> 'Alert':
        """
        Creates an Alert instance from a BigQuery row.
        
        Args:
            row: BigQuery row data
            
        Returns:
            Alert instance
        """
        # Make a copy to avoid modifying the input
        row_copy = dict(row)
        
        # Parse JSON fields
        if row_copy.get("context"):
            row_copy["context"] = json.loads(row_copy["context"])
            
        if row_copy.get("related_alerts"):
            row_copy["related_alerts"] = json.loads(row_copy["related_alerts"])
        else:
            row_copy["related_alerts"] = []
            
        if row_copy.get("notifications"):
            notifications_data = json.loads(row_copy["notifications"])
            row_copy["notifications"] = notifications_data
        else:
            row_copy["notifications"] = []
            
        if row_copy.get("acknowledgment_details"):
            row_copy["acknowledgment_details"] = json.loads(row_copy["acknowledgment_details"])
        else:
            row_copy["acknowledgment_details"] = {}
            
        if row_copy.get("resolution_details"):
            row_copy["resolution_details"] = json.loads(row_copy["resolution_details"])
        else:
            row_copy["resolution_details"] = {}
            
        # Create and return Alert instance
        alert = cls.from_dict(row_copy)
        return alert
    
    def acknowledge(self, acknowledged_by: str, notes: str = None) -> bool:
        """
        Acknowledges the alert, updating its status.
        
        Args:
            acknowledged_by: User or service that acknowledged the alert
            notes: Optional notes about the acknowledgment
            
        Returns:
            True if acknowledgment was successful
        """
        if self.status == ALERT_STATUS_RESOLVED:
            logger.warning(f"Cannot acknowledge already resolved alert: {self.alert_id}")
            return False
            
        if self.status == ALERT_STATUS_ACKNOWLEDGED:
            logger.warning(f"Alert {self.alert_id} already acknowledged")
            return False
            
        self.status = ALERT_STATUS_ACKNOWLEDGED
        self.acknowledged_at = datetime.datetime.now()
        self.updated_at = self.acknowledged_at
        
        self.acknowledgment_details = {
            "acknowledged_by": acknowledged_by,
            "acknowledged_at": self.acknowledged_at.isoformat(),
            "notes": notes
        }
        
        logger.info(f"Alert {self.alert_id} acknowledged by {acknowledged_by}")
        return True
    
    def resolve(self, resolved_by: str, resolution_details: Dict[str, Any] = None) -> bool:
        """
        Resolves the alert, updating its status.
        
        Args:
            resolved_by: User or service that resolved the alert
            resolution_details: Details about the resolution
            
        Returns:
            True if resolution was successful
        """
        if self.status == ALERT_STATUS_RESOLVED:
            logger.warning(f"Alert {self.alert_id} already resolved")
            return False
            
        self.status = ALERT_STATUS_RESOLVED
        self.resolved_at = datetime.datetime.now()
        self.updated_at = self.resolved_at
        
        self.resolution_details = resolution_details or {}
        self.resolution_details["resolved_by"] = resolved_by
        self.resolution_details["resolved_at"] = self.resolved_at.isoformat()
        
        logger.info(f"Alert {self.alert_id} resolved by {resolved_by}")
        return True
    
    def suppress(self, reason: str) -> bool:
        """
        Suppresses the alert, updating its status.
        
        Args:
            reason: Reason for suppressing the alert
            
        Returns:
            True if suppression was successful
        """
        if self.status == ALERT_STATUS_RESOLVED or self.status == ALERT_STATUS_SUPPRESSED:
            logger.warning(f"Cannot suppress alert {self.alert_id} with status {self.status}")
            return False
            
        self.status = ALERT_STATUS_SUPPRESSED
        self.updated_at = datetime.datetime.now()
        
        # Add suppression info to context
        self.context["suppression"] = {
            "reason": reason,
            "timestamp": self.updated_at.isoformat()
        }
        
        logger.info(f"Alert {self.alert_id} suppressed: {reason}")
        return True
    
    def add_related_alert(self, related_alert_id: str) -> bool:
        """
        Adds a related alert to this alert.
        
        Args:
            related_alert_id: ID of the related alert
            
        Returns:
            True if addition was successful
        """
        if related_alert_id in self.related_alerts:
            logger.debug(f"Alert {related_alert_id} already related to {self.alert_id}")
            return False
            
        self.related_alerts.append(related_alert_id)
        self.updated_at = datetime.datetime.now()
        
        logger.debug(f"Added related alert {related_alert_id} to {self.alert_id}")
        return True
    
    def add_notification(
        self,
        channel: NotificationChannel,
        recipient: str,
        success: bool,
        details: str = None
    ) -> bool:
        """
        Records a notification sent for this alert.
        
        Args:
            channel: Notification channel used
            recipient: Recipient of the notification
            success: Whether the notification was successfully delivered
            details: Additional details about the notification
            
        Returns:
            True if addition was successful
        """
        notification = AlertNotification(
            channel=channel,
            recipient=recipient,
            success=success,
            details=details or ""
        )
        
        self.notifications.append(notification)
        self.updated_at = datetime.datetime.now()
        
        log_level = logging.INFO if success else logging.WARNING
        logger.log(
            log_level,
            f"Alert {self.alert_id} notification via {channel.value} to {recipient}: {'Success' if success else 'Failed'}"
        )
        return True
    
    def is_active(self) -> bool:
        """
        Checks if the alert is active (not resolved or suppressed).
        
        Returns:
            True if alert is active
        """
        return self.status not in [ALERT_STATUS_RESOLVED, ALERT_STATUS_SUPPRESSED]
    
    def update_context(self, additional_context: Dict[str, Any]) -> None:
        """
        Updates the alert context with additional information.
        
        Args:
            additional_context: Additional context information to add
        """
        self.context.update(additional_context)
        self.updated_at = datetime.datetime.now()
    
    def get_notification_status(self, channel: NotificationChannel) -> Optional[Dict[str, Any]]:
        """
        Gets the notification status for a specific channel.
        
        Args:
            channel: The notification channel to check
            
        Returns:
            Notification status for the channel or None if not found
        """
        # Find notifications for the specified channel, latest first
        channel_notifications = [
            notification for notification in self.notifications
            if notification.channel == channel
        ]
        
        if not channel_notifications:
            return None
            
        # Return the most recent notification
        latest = max(channel_notifications, key=lambda n: n.timestamp)
        return latest.to_dict()