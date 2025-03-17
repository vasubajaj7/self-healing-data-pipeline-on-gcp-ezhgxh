"""
Manages the escalation process for alerts in the self-healing data pipeline.
This component handles alert escalation based on severity, response time, and configured escalation policies. It ensures critical issues are properly escalated to the appropriate teams or individuals when they are not acknowledged or resolved within defined timeframes.
"""

import typing
import datetime
import time
import threading
import json

from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta

from ...constants import AlertSeverity, NotificationChannel, ALERT_STATUS_ACKNOWLEDGED, ALERT_STATUS_RESOLVED
from ...config import get_config
from ...logging_config import get_logger
from .notification_router import NotificationRouter
from ...db.repositories.alert_repository import AlertRepository
from ...db.models.alert import Alert

# Initialize module logger
logger = get_logger(__name__)


class EscalationManager:
    """
    Manages the escalation process for alerts based on severity and response time
    """

    def __init__(self, alert_repository: AlertRepository = None, notification_router: NotificationRouter = None):
        """
        Initializes the EscalationManager with necessary components
        """
        # LD1: Store provided components or create new instances if not provided
        self._alert_repository = alert_repository or AlertRepository()
        self._notification_router = notification_router or NotificationRouter()

        # LD1: Load escalation configuration from application config
        config = get_config()
        self._escalation_policies = config.get('escalation_policies', {})
        self._escalation_targets = config.get('escalation_targets', {})

        # LD1: Initialize escalation state tracking dictionary
        self._escalation_state: Dict[str, Dict[str, Union[int, datetime]]] = {}

        # LD1: Set check interval from configuration (default 60 seconds)
        self._check_interval_seconds = config.get('escalation.check_interval_seconds', 60)

        # LD1: Initialize but don't start the escalation monitoring thread
        self._escalation_thread: Optional[threading.Thread] = None

        # LD1: Set running flag to False
        self._running = False

        # LD1: Log successful initialization
        logger.info("EscalationManager initialized")

    def start_monitoring(self) -> bool:
        """
        Starts the escalation monitoring thread
        """
        # LD1: Check if already running, return False if so
        if self._running:
            logger.warning("Escalation monitoring already running")
            return False

        # LD1: Set running flag to True
        self._running = True

        # LD1: Create and start escalation monitoring thread
        self._escalation_thread = threading.Thread(target=self._escalation_monitor_loop)
        self._escalation_thread.daemon = True  # Allow main thread to exit even if this thread is running
        self._escalation_thread.start()

        # LD1: Log monitoring start
        logger.info("Escalation monitoring started")

        # LD1: Return True
        return True

    def stop_monitoring(self) -> bool:
        """
        Stops the escalation monitoring thread
        """
        # LD1: Set running flag to False
        self._running = False

        # LD1: Wait for escalation thread to terminate if it exists
        if self._escalation_thread and self._escalation_thread.is_alive():
            self._escalation_thread.join()

        # LD1: Log monitoring stop
        logger.info("Escalation monitoring stopped")

        # LD1: Return True
        return True

    def _escalation_monitor_loop(self) -> None:
        """
        Background thread function that periodically checks for alerts requiring escalation
        """
        # LD1: Log thread start
        logger.info("Escalation monitor thread started")

        # LD1: While running flag is True:
        while self._running:
            try:
                # LD1: Fetch active alerts that need escalation check
                alerts_to_check = self._alert_repository.get_active_alerts()

                # LD1: For each alert, check if escalation is needed
                for alert in alerts_to_check:
                    if self.check_alert_escalation(alert):
                        # LD1: If escalation needed, perform escalation
                        escalation_level = self.get_escalation_policy(alert.severity).get_escalation_level(
                            int(((datetime.datetime.now() - alert.created_at).total_seconds()) / 60)
                        )
                        self.escalate_alert(alert, escalation_level)

                # LD1: Sleep for check_interval_seconds
                time.sleep(self._check_interval_seconds)

            except Exception as e:
                logger.error(f"Error in escalation monitor loop: {e}")

        # LD1: Log thread termination
        logger.info("Escalation monitor thread terminated")

    def check_alert_escalation(self, alert: Alert) -> bool:
        """
        Checks if an alert needs escalation based on severity and time since creation
        """
        # LD1: Check if alert is already acknowledged or resolved
        if alert.status in [ALERT_STATUS_ACKNOWLEDGED, ALERT_STATUS_RESOLVED]:
            return False

        # LD1: Get escalation policy for alert severity
        escalation_policy = self.get_escalation_policy(alert.severity)

        # LD1: Calculate time elapsed since alert creation
        elapsed_minutes = int(((datetime.datetime.now() - alert.created_at).total_seconds()) / 60)

        # LD1: Determine current escalation level based on elapsed time
        escalation_level = escalation_policy.get_escalation_level(elapsed_minutes)

        # LD1: Check if alert has already been escalated to this level
        alert_state = self.get_alert_escalation_state(alert.alert_id)
        if alert_state and alert_state.get('level', 0) >= escalation_level:
            return False

        # LD1: Return True if new escalation is needed, False otherwise
        return True

    def escalate_alert(self, alert: Alert, escalation_level: int) -> bool:
        """
        Escalates an alert to the next level based on severity and elapsed time
        """
        # LD1: Get escalation targets for the current level and severity
        escalation_targets = self.get_escalation_targets(escalation_level, alert.severity)

        # LD1: Format escalation notification with alert details and level
        message = self.format_escalation_message(alert, escalation_level)

        # LD1: Send notifications to escalation targets
        delivery_results = self._notification_router.send_notification(message, escalation_targets.keys())

        # LD1: Update alert with escalation information
        success = all(result.success for result in delivery_results.values())
        if success:
            self.update_escalation_state(alert.alert_id, escalation_level, datetime.datetime.now())
            logger.info(f"Alert {alert.alert_id} escalated to level {escalation_level}")
        else:
            logger.warning(f"Failed to escalate alert {alert.alert_id} to level {escalation_level}")

        # LD1: Return True if notifications were sent successfully
        return success

    def get_escalation_policy(self, severity: AlertSeverity) -> 'EscalationPolicy':
        """
        Retrieves the escalation policy for a given alert severity
        """
        # LD1: Get severity-specific policy from escalation_policies
        policy_dict = self._escalation_policies.get(severity.value, self._escalation_policies.get('default', {}))

        # LD1: Fall back to default policy if not found
        if not policy_dict:
            logger.warning(f"No escalation policy found for severity {severity.value}, using default")
            policy_dict = self._escalation_policies.get('default', {})

        # LD1: Return the policy dictionary with timeframes and levels
        return EscalationPolicy.from_dict({'severity': severity.value, **policy_dict})

    def get_escalation_targets(self, level: int, severity: AlertSeverity) -> Dict[NotificationChannel, List[str]]:
        """
        Retrieves the notification targets for a given escalation level and severity
        """
        # LD1: Get severity-specific targets from escalation_targets
        severity_targets = self._escalation_targets.get(severity.value, {})

        # LD1: Get level-specific targets within severity
        level_targets = severity_targets.get(str(level), severity_targets.get('default', {}))

        # LD1: Fall back to default targets if not found
        if not level_targets:
            logger.warning(f"No escalation targets found for level {level} and severity {severity.value}, using default")
            level_targets = severity_targets.get('default', {})

        # LD1: Return the targets dictionary with channels and recipients
        return level_targets

    def format_escalation_message(self, alert: Alert, escalation_level: int) -> Dict[str, Any]:
        """
        Formats an escalation notification message
        """
        # LD1: Create base message with alert details
        message = {
            "title": f"Escalated Alert: {alert.alert_type}",
            "message": alert.description,
            "severity": alert.severity.value,
            "alert_type": "escalation",
            "details": alert.context,
            "pipeline_name": alert.execution_id,
        }

        # LD1: Add escalation-specific information (level, reason)
        message["escalation_level"] = escalation_level
        message["escalation_reason"] = f"Alert has not been acknowledged or resolved within the defined timeframe for severity {alert.severity.value}"

        # LD1: Format message based on severity
        if alert.severity == AlertSeverity.CRITICAL:
            message["message"] = f"CRITICAL: {message['message']}. Immediate action required!"
        elif alert.severity == AlertSeverity.HIGH:
            message["message"] = f"HIGH: {message['message']}. Please investigate ASAP."
        else:
            message["message"] = f"{alert.severity.value}: {message['message']}"

        # LD1: Add links to alert dashboard
        message["details_url"] = f"/alerts/{alert.alert_id}"

        # LD1: Add response instructions
        message["response_instructions"] = "Please review the alert details and take appropriate action."

        # LD1: Return formatted message dictionary
        return message

    def get_alert_escalation_state(self, alert_id: str) -> Optional[Dict[str, Union[int, datetime]]]:
        """
        Retrieves the current escalation state for an alert
        """
        # LD1: Check if alert exists in escalation_state
        if alert_id in self._escalation_state:
            # LD1: Return escalation state if found, None otherwise
            return self._escalation_state[alert_id]
        else:
            return None

    def update_escalation_state(self, alert_id: str, level: int, timestamp: datetime) -> None:
        """
        Updates the escalation state for an alert
        """
        # LD1: Create or update escalation state entry for alert
        self._escalation_state[alert_id] = {
            "level": level,
            "timestamp": timestamp
        }

        # LD1: Clean up old state entries if needed
        self.cleanup_escalation_state()

    def cleanup_escalation_state(self) -> int:
        """
        Removes escalation state entries for resolved alerts
        """
        # LD1: Get list of resolved alert IDs from repository
        resolved_alerts = self._alert_repository.get_alerts_by_status(ALERT_STATUS_RESOLVED)
        resolved_alert_ids = {alert.alert_id for alert in resolved_alerts}

        # LD1: Remove those IDs from escalation_state dictionary
        removed_count = 0
        for alert_id in list(self._escalation_state.keys()):
            if alert_id in resolved_alert_ids:
                del self._escalation_state[alert_id]
                removed_count += 1

        # LD1: Return count of removed entries
        return removed_count

    def get_escalation_statistics(self, time_window_hours: int) -> Dict[str, Any]:
        """
        Retrieves statistics about alert escalations
        """
        # LD1: Calculate statistics for escalations in the time window
        # LD1: Count escalations by severity
        # LD1: Count escalations by level
        # LD1: Calculate average time to acknowledgment
        # LD1: Calculate average time to resolution
        # LD1: Return statistics dictionary
        return {}


class EscalationPolicy:
    """
    Data class representing an escalation policy configuration
    """

    def __init__(self, severity: AlertSeverity, levels: List[int], timeframes: Dict[int, int], channels: Dict[int, List[NotificationChannel]]):
        """
        Initializes a new EscalationPolicy instance
        """
        # LD1: Set severity level
        self.severity = severity

        # LD1: Set escalation levels list
        self.levels = levels

        # LD1: Set timeframes dictionary mapping levels to minutes
        self.timeframes = timeframes

        # LD1: Set channels dictionary mapping levels to notification channels
        self.channels = channels

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the policy to a dictionary representation
        """
        # LD1: Create dictionary with all policy properties
        policy_dict = {
            'severity': self.severity.value if isinstance(self.severity, AlertSeverity) else self.severity,
            'levels': self.levels,
            'timeframes': self.timeframes,
            'channels': self.channels
        }

        # LD1: Convert enum values to strings
        # Already handled by severity

        # LD1: Return the dictionary
        return policy_dict

    @classmethod
    def from_dict(cls, policy_dict: Dict[str, Any]) -> 'EscalationPolicy':
        """
        Creates an EscalationPolicy instance from a dictionary
        """
        # LD1: Extract policy properties from dictionary
        severity = policy_dict.get('severity')
        levels = policy_dict.get('levels', [])
        timeframes = policy_dict.get('timeframes', {})
        channels = policy_dict.get('channels', {})

        # LD1: Convert string severity to AlertSeverity enum
        if isinstance(severity, str):
            severity = AlertSeverity(severity)

        # LD1: Create and return new EscalationPolicy instance
        return cls(severity, levels, timeframes, channels)

    def get_escalation_level(self, elapsed_minutes: int) -> int:
        """
        Determines the appropriate escalation level based on elapsed time
        """
        # LD1: Iterate through timeframes in ascending order
        sorted_timeframes = sorted(self.timeframes.items())

        # LD1: Return the highest level where elapsed time exceeds the threshold
        for level, timeframe in sorted_timeframes:
            if elapsed_minutes >= timeframe:
                continue
            else:
                return level - 1

        # LD1: Return 0 if no escalation needed yet
        return 0