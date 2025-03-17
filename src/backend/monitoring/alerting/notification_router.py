"""
Implements a notification routing system for the self-healing data pipeline.

This component routes alerts to appropriate notification channels (Microsoft Teams, Email, etc.)
based on alert properties, manages delivery tracking, and handles notification failures.
It serves as a central hub for all outbound notifications from the monitoring system.
"""

import uuid
import datetime
from typing import Dict, List, Any, Optional, Union
from concurrent.futures import ThreadPoolExecutor

from ...constants import AlertSeverity, NotificationChannel
from ...config import get_config
from ...logging_config import get_logger
from ..integrations.teams_notifier import TeamsNotifier
from ..integrations.email_notifier import EmailNotifier

# Set up logger
logger = get_logger(__name__)


def determine_notification_channels(alert_data: Dict[str, Any], specified_channels: List[NotificationChannel] = None) -> List[NotificationChannel]:
    """
    Determines appropriate notification channels based on alert properties.

    Args:
        alert_data: The alert data including severity, type, and other properties
        specified_channels: List of specifically requested channels, if any

    Returns:
        List of notification channels to use
    """
    # If specific channels are provided, use them
    if specified_channels and len(specified_channels) > 0:
        return specified_channels

    # Otherwise, determine based on alert properties
    severity = alert_data.get('severity')
    if isinstance(severity, str):
        try:
            severity = AlertSeverity(severity)
        except ValueError:
            severity = AlertSeverity.INFO
    
    channels = []
    
    # Critical alerts go to all available channels
    if severity == AlertSeverity.CRITICAL:
        channels = [
            NotificationChannel.TEAMS,
            NotificationChannel.EMAIL
        ]
    # High alerts go to Teams and Email
    elif severity == AlertSeverity.HIGH:
        channels = [
            NotificationChannel.TEAMS,
            NotificationChannel.EMAIL
        ]
    # Medium alerts go to Teams
    elif severity == AlertSeverity.MEDIUM:
        channels = [NotificationChannel.TEAMS]
    # Low alerts go to Teams
    elif severity == AlertSeverity.LOW:
        channels = [NotificationChannel.TEAMS]
    # Info alerts go to Teams
    else:
        channels = [NotificationChannel.TEAMS]
    
    # Get routing rules from configuration
    config = get_config()
    routing_config = config.get('notifications.routing', {})
    
    # Apply configuration overrides if available
    alert_type = alert_data.get('alert_type', 'general')
    type_specific_config = routing_config.get(alert_type, {})
    if type_specific_config:
        type_channels = type_specific_config.get('channels', [])
        if type_channels:
            # Convert string channel names to enum values
            channels = [
                NotificationChannel(channel) if isinstance(channel, str) else channel
                for channel in type_channels
            ]
    
    return channels


def format_notification_for_channel(message: Dict[str, Any], channel: NotificationChannel) -> Dict[str, Any]:
    """
    Formats notification message for a specific channel type.

    Args:
        message: The notification message to format
        channel: The channel type to format for

    Returns:
        Channel-specific formatted message
    """
    # Create a copy of the original message to avoid modifying it
    formatted_message = message.copy()
    
    # Add channel-specific formatting
    if channel == NotificationChannel.TEAMS:
        # Format message for Microsoft Teams
        if 'title' in formatted_message and 'message' in formatted_message:
            # Ensure Teams-specific formatting is applied
            formatted_message['title'] = formatted_message['title']
            # No special formatting needed for now, but could be added if needed
        
    elif channel == NotificationChannel.EMAIL:
        # Format message for Email
        if 'title' in formatted_message:
            # Ensure Email-specific formatting is applied
            formatted_message['subject'] = formatted_message.get('subject', formatted_message['title'])
    
    # Add channel type to the message for tracking
    formatted_message['channel'] = channel.value if isinstance(channel, NotificationChannel) else channel
    
    return formatted_message


class NotificationDeliveryResult:
    """
    Represents the result of a notification delivery attempt.
    
    This class stores information about the delivery status, any errors encountered,
    and metadata about the message and channel used.
    """
    
    def __init__(self, notification_id: str, channel: NotificationChannel, 
                 success: bool, error_message: str = None, 
                 delivery_details: Dict[str, Any] = None):
        """
        Initializes a new NotificationDeliveryResult instance.
        
        Args:
            notification_id: Unique identifier for the notification
            channel: The notification channel used
            success: Whether delivery was successful
            error_message: Error message if delivery failed, None otherwise
            delivery_details: Additional details about the delivery
        """
        self.notification_id = notification_id
        self.channel = channel
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
        return {
            "notification_id": self.notification_id,
            "channel": self.channel.value if isinstance(self.channel, NotificationChannel) else self.channel,
            "success": self.success,
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat(),
            "delivery_details": self.delivery_details
        }
    
    @classmethod
    def from_dict(cls, result_dict: Dict[str, Any]) -> 'NotificationDeliveryResult':
        """
        Creates a NotificationDeliveryResult instance from a dictionary.
        
        Args:
            result_dict: Dictionary containing delivery result data
            
        Returns:
            NotificationDeliveryResult instance
        """
        # Extract basic delivery result properties
        notification_id = result_dict.get("notification_id", "")
        channel_value = result_dict.get("channel", "")
        
        # Convert channel string to NotificationChannel enum
        if isinstance(channel_value, str):
            try:
                channel = NotificationChannel(channel_value)
            except ValueError:
                channel = channel_value
        else:
            channel = channel_value
            
        success = result_dict.get("success", False)
        error_message = result_dict.get("error_message")
        delivery_details = result_dict.get("delivery_details", {})
        
        # Create new instance
        result = cls(
            notification_id=notification_id,
            channel=channel,
            success=success,
            error_message=error_message,
            delivery_details=delivery_details
        )
        
        # Convert ISO format strings to datetime objects
        timestamp_str = result_dict.get("timestamp")
        if timestamp_str:
            try:
                result.timestamp = datetime.datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError):
                pass
                
        return result


class NotificationRouter:
    """
    Routes notifications to appropriate channels based on alert properties and configuration.
    
    This class serves as the central hub for outbound notifications, determining which
    channels to use, formatting messages appropriately, and tracking delivery status.
    """
    
    def __init__(self, teams_notifier: TeamsNotifier = None, email_notifier: EmailNotifier = None):
        """
        Initializes the NotificationRouter with necessary components.
        
        Args:
            teams_notifier: TeamsNotifier instance for sending Teams notifications
            email_notifier: EmailNotifier instance for sending email notifications
        """
        # Load notification configuration
        self._config = get_config().get("notifications", {})
        
        # Store or create notifiers
        self._teams_notifier = teams_notifier or TeamsNotifier()
        self._email_notifier = email_notifier or EmailNotifier()
        
        # Initialize channel configuration and routing rules
        self._channel_config = self._config.get("channels", {})
        self._routing_rules = self._config.get("routing_rules", {})
        
        # Initialize delivery history tracking
        self._delivery_history = {}
        
        # Set up thread pool for parallel notification processing
        max_workers = self._config.get("max_concurrent_notifications", 10)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        logger.info("NotificationRouter initialized")
    
    def send_notification(self, message: Dict[str, Any], channels: List[NotificationChannel] = None) -> Dict[NotificationChannel, NotificationDeliveryResult]:
        """
        Sends a notification to specified channels or automatically determines appropriate channels.
        
        Args:
            message: Dictionary containing notification details (title, message, 
                    severity, etc.)
            channels: List of channels to send to (if None, determined from message)
            
        Returns:
            Dictionary mapping channels to their delivery results
        """
        # Validate message format
        if not isinstance(message, dict):
            logger.error("Message must be a dictionary")
            return {}
        
        # Ensure required fields are present
        if 'title' not in message:
            message['title'] = "Pipeline Notification"
        
        # Generate a unique notification ID if not provided
        notification_id = message.get('notification_id', str(uuid.uuid4()))
        message['notification_id'] = notification_id
        
        # Determine channels to use if not specified
        if not channels:
            channels = determine_notification_channels(message)
        
        # Format message for each channel and send in parallel
        futures = {}
        delivery_results = {}
        
        for channel in channels:
            # Format message for this channel
            formatted_message = format_notification_for_channel(message, channel)
            
            # Submit to thread pool for parallel processing
            futures[channel] = self._executor.submit(
                self.send_to_channel, formatted_message, channel
            )
        
        # Collect results from all channels
        for channel, future in futures.items():
            try:
                result = future.result(timeout=30)  # 30-second timeout
                delivery_results[channel] = result
            except Exception as e:
                logger.error(f"Error sending to {channel}: {e}")
                delivery_results[channel] = NotificationDeliveryResult(
                    notification_id=notification_id,
                    channel=channel,
                    success=False,
                    error_message=f"Exception during delivery: {str(e)}"
                )
        
        # Update delivery history
        self._delivery_history[notification_id] = {
            "timestamp": datetime.datetime.now().isoformat(),
            "channels": {
                channel.value if isinstance(channel, NotificationChannel) else str(channel): 
                result.to_dict()
                for channel, result in delivery_results.items()
            },
            "message": {
                "title": message.get("title"),
                "severity": message.get("severity"),
                "alert_type": message.get("alert_type", "general")
            }
        }
        
        # Log overall delivery status
        success_count = sum(1 for result in delivery_results.values() if result.success)
        if success_count == len(delivery_results):
            logger.info(f"Notification {notification_id} delivered successfully to all channels")
        elif success_count > 0:
            logger.warning(f"Notification {notification_id} delivered to {success_count}/{len(delivery_results)} channels")
        else:
            logger.error(f"Notification {notification_id} failed to deliver to any channel")
        
        return delivery_results
    
    def send_to_channel(self, message: Dict[str, Any], channel: NotificationChannel) -> NotificationDeliveryResult:
        """
        Sends a notification to a specific channel.
        
        Args:
            message: Formatted message for the channel
            channel: The channel to send to
            
        Returns:
            Delivery result for the channel
        """
        notification_id = message.get('notification_id', str(uuid.uuid4()))
        
        try:
            logger.debug(f"Sending notification {notification_id} to {channel}")
            
            if channel == NotificationChannel.TEAMS:
                # Send to Microsoft Teams
                result = self._teams_notifier.send_notification(message)
                success = result.success if hasattr(result, 'success') else False
                error_message = result.error_message if hasattr(result, 'error_message') else None
                delivery_details = result.to_dict() if hasattr(result, 'to_dict') else {}
                
            elif channel == NotificationChannel.EMAIL:
                # Send via email
                success = self._email_notifier.send_notification(message)
                error_message = None if success else "Email delivery failed"
                delivery_details = {}
                
            else:
                # Unsupported channel
                logger.warning(f"Unsupported notification channel: {channel}")
                return NotificationDeliveryResult(
                    notification_id=notification_id,
                    channel=channel,
                    success=False,
                    error_message=f"Unsupported channel: {channel}",
                    delivery_details={}
                )
            
            # Create and return delivery result
            return NotificationDeliveryResult(
                notification_id=notification_id,
                channel=channel,
                success=success,
                error_message=error_message,
                delivery_details=delivery_details
            )
            
        except Exception as e:
            # Log and return failure result
            logger.exception(f"Error sending notification to {channel}: {e}")
            return NotificationDeliveryResult(
                notification_id=notification_id,
                channel=channel,
                success=False,
                error_message=f"Exception: {str(e)}",
                delivery_details={}
            )
    
    def send_batch_notifications(self, messages: List[Dict[str, Any]], channels: List[NotificationChannel] = None) -> Dict[str, Dict[NotificationChannel, NotificationDeliveryResult]]:
        """
        Sends multiple notifications in parallel.
        
        Args:
            messages: List of notification messages
            channels: List of channels to send to (if None, determined from each message)
            
        Returns:
            Dictionary mapping message IDs to their channel delivery results
        """
        if not isinstance(messages, list):
            logger.error("Messages must be a list of dictionaries")
            return {}
        
        results = {}
        futures = {}
        
        # Submit each message to thread pool for parallel processing
        for message in messages:
            if not isinstance(message, dict):
                logger.warning(f"Skipping invalid message (not a dictionary): {message}")
                continue
                
            notification_id = message.get('notification_id', str(uuid.uuid4()))
            message['notification_id'] = notification_id
            
            futures[notification_id] = self._executor.submit(
                self.send_notification, message, channels
            )
        
        # Collect results
        for notification_id, future in futures.items():
            try:
                results[notification_id] = future.result(timeout=60)  # 60-second timeout
            except Exception as e:
                logger.error(f"Error processing notification {notification_id}: {e}")
                results[notification_id] = {}
        
        return results
    
    def get_delivery_status(self, notification_id: str) -> Dict[str, Any]:
        """
        Retrieves delivery status for a specific notification.
        
        Args:
            notification_id: ID of the notification to check
            
        Returns:
            Delivery status details by channel or empty dict if not found
        """
        return self._delivery_history.get(notification_id, {})
    
    def get_channel_config(self, channel: NotificationChannel) -> Dict[str, Any]:
        """
        Retrieves configuration for a specific notification channel.
        
        Args:
            channel: The channel to get configuration for
            
        Returns:
            Channel configuration dictionary
        """
        channel_key = channel.value if isinstance(channel, NotificationChannel) else str(channel)
        return self._channel_config.get(channel_key, {})
    
    def update_channel_config(self, channel: NotificationChannel, config: Dict[str, Any]) -> bool:
        """
        Updates configuration for a specific notification channel.
        
        Args:
            channel: The channel to update configuration for
            config: New configuration for the channel
            
        Returns:
            True if update successful, False otherwise
        """
        if not isinstance(config, dict):
            logger.error("Channel configuration must be a dictionary")
            return False
        
        try:
            channel_key = channel.value if isinstance(channel, NotificationChannel) else str(channel)
            
            # Update channel configuration
            if channel_key not in self._channel_config:
                self._channel_config[channel_key] = {}
                
            self._channel_config[channel_key].update(config)
            
            # Apply changes to relevant notifier if applicable
            if channel == NotificationChannel.TEAMS and hasattr(self._teams_notifier, 'update_config'):
                self._teams_notifier.update_config(config)
            elif channel == NotificationChannel.EMAIL and hasattr(self._email_notifier, 'update_config'):
                self._email_notifier.update_config(config)
                
            logger.info(f"Updated configuration for channel: {channel_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating channel configuration: {e}")
            return False
    
    def add_routing_rule(self, rule: Dict[str, Any]) -> str:
        """
        Adds a new notification routing rule.
        
        Args:
            rule: Rule definition dictionary with conditions and actions
            
        Returns:
            Rule ID if added successfully
        """
        # Validate rule structure
        if not isinstance(rule, dict):
            logger.error("Rule must be a dictionary")
            return ""
            
        if 'conditions' not in rule or 'channels' not in rule:
            logger.error("Rule must contain 'conditions' and 'channels' keys")
            return ""
        
        # Generate unique rule ID
        rule_id = str(uuid.uuid4())
        rule['rule_id'] = rule_id
        
        # Add timestamp
        rule['created_at'] = datetime.datetime.now().isoformat()
        
        # Add to routing rules
        self._routing_rules[rule_id] = rule
        
        logger.info(f"Added routing rule: {rule_id}")
        return rule_id
    
    def remove_routing_rule(self, rule_id: str) -> bool:
        """
        Removes a notification routing rule.
        
        Args:
            rule_id: ID of the rule to remove
            
        Returns:
            True if removed, False if not found
        """
        if rule_id in self._routing_rules:
            del self._routing_rules[rule_id]
            logger.info(f"Removed routing rule: {rule_id}")
            return True
            
        logger.warning(f"Routing rule not found: {rule_id}")
        return False
    
    def get_routing_rules(self) -> List[Dict[str, Any]]:
        """
        Retrieves all notification routing rules.
        
        Returns:
            List of routing rules
        """
        return list(self._routing_rules.values())
    
    def apply_routing_rules(self, message: Dict[str, Any]) -> List[NotificationChannel]:
        """
        Applies routing rules to determine appropriate channels for a message.
        
        Args:
            message: The message to route
            
        Returns:
            List of channels to use
        """
        # Extract message properties for rule evaluation
        severity = message.get('severity')
        alert_type = message.get('alert_type', 'general')
        
        # Convert string severity to enum if needed
        if isinstance(severity, str):
            try:
                severity = AlertSeverity(severity)
            except ValueError:
                severity = AlertSeverity.INFO
        
        matching_channels = set()
        
        # Evaluate each rule
        for rule in self._routing_rules.values():
            conditions = rule.get('conditions', {})
            
            # Check if rule matches
            if self._evaluate_conditions(message, conditions):
                # Add channels from matching rule
                rule_channels = rule.get('channels', [])
                for channel in rule_channels:
                    # Convert string channel to enum if needed
                    if isinstance(channel, str):
                        try:
                            channel_enum = NotificationChannel(channel)
                            matching_channels.add(channel_enum)
                        except ValueError:
                            logger.warning(f"Invalid channel in rule: {channel}")
                    else:
                        matching_channels.add(channel)
        
        # If no rules matched, apply default routing
        if not matching_channels:
            default_channels = determine_notification_channels(message)
            matching_channels.update(default_channels)
        
        return list(matching_channels)
    
    def _evaluate_conditions(self, message: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
        """
        Evaluates whether a message matches rule conditions.
        
        Args:
            message: The message to evaluate
            conditions: Conditions to check against
            
        Returns:
            True if all conditions match, False otherwise
        """
        # Check each condition
        for field, expected_value in conditions.items():
            if field not in message:
                return False
                
            actual_value = message[field]
            
            # Handle special case for severity (enum comparison)
            if field == 'severity':
                if isinstance(actual_value, str):
                    try:
                        actual_value = AlertSeverity(actual_value)
                    except ValueError:
                        actual_value = AlertSeverity.INFO
                        
                if isinstance(expected_value, str):
                    try:
                        expected_value = AlertSeverity(expected_value)
                    except ValueError:
                        expected_value = AlertSeverity.INFO
            
            # If values don't match, rule doesn't apply
            if actual_value != expected_value:
                return False
        
        # All conditions matched
        return True
    
    def cleanup_delivery_history(self) -> int:
        """
        Cleans up old delivery history records.
        
        Returns:
            Number of records cleaned up
        """
        # Get retention period from config
        retention_hours = self._config.get("history_retention_hours", 24)
        cutoff_time = datetime.datetime.now() - datetime.timedelta(hours=retention_hours)
        
        # Find records to remove
        keys_to_remove = []
        for notification_id, record in self._delivery_history.items():
            try:
                timestamp_str = record.get("timestamp")
                timestamp = datetime.datetime.fromisoformat(timestamp_str)
                if timestamp < cutoff_time:
                    keys_to_remove.append(notification_id)
            except (ValueError, TypeError, KeyError):
                # If can't parse timestamp, consider it for removal
                keys_to_remove.append(notification_id)
        
        # Remove records
        for key in keys_to_remove:
            del self._delivery_history[key]
            
        logger.info(f"Cleaned up {len(keys_to_remove)} old delivery history records")
        return len(keys_to_remove)