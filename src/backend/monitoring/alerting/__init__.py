"""
Package initialization file for the monitoring alerting system.
This module exposes the key classes and functions from the alerting submodules, providing a unified interface for alert generation, notification routing, rule evaluation, and escalation management. It serves as the entry point for the alerting functionality in the self-healing data pipeline.
"""

from .alert_generator import AlertGenerator, AlertNotification, create_alert_from_rule_result  # Import alert generation functionality
from .notification_router import NotificationRouter, NotificationDeliveryResult, determine_notification_channels, format_notification_for_channel  # Import notification routing functionality
from .rule_engine import RuleEngine, Rule, RuleEvaluationResult, evaluate_condition  # Import rule evaluation functionality
from .escalation_manager import EscalationManager, EscalationPolicy  # Import alert escalation functionality
from ...constants import AlertSeverity, NotificationChannel  # Import alert severity levels and notification channel types

__all__ = [
    "AlertGenerator",  # Main class for generating and managing alerts in the self-healing pipeline
    "AlertNotification",  # Class representing an alert notification with delivery tracking
    "NotificationRouter",  # Class for routing notifications to appropriate channels
    "NotificationDeliveryResult",  # Class representing notification delivery results
    "RuleEngine",  # Class for managing and evaluating alert rules
    "Rule",  # Class representing an individual alert rule
    "RuleEvaluationResult",  # Class representing the result of a rule evaluation
    "EscalationManager",  # Class for managing alert escalations based on severity and response time
    "EscalationPolicy",  # Class representing escalation policy configuration
    "create_alert_from_rule_result",  # Utility function to create alerts from rule evaluation results
    "determine_notification_channels",  # Utility function to determine appropriate notification channels
    "format_notification_for_channel",  # Utility function to format notifications for specific channels
    "evaluate_condition",  # Utility function to evaluate a single condition
    "AlertSeverity",  # Enum defining alert severity levels
    "NotificationChannel"  # Enum defining notification channel types
]