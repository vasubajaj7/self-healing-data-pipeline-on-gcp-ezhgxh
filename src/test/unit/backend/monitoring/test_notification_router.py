# src/test/unit/backend/monitoring/test_notification_router.py
"""
Unit tests for the NotificationRouter component of the monitoring system.
Tests the routing of notifications to appropriate channels (Microsoft Teams, Email) based on alert properties, delivery tracking, and error handling capabilities.
"""

import pytest  # package_version: latest
import unittest.mock  # package_version: standard library
import datetime  # package_version: standard library
import uuid  # package_version: standard library
import concurrent.futures  # package_version: standard library

from src.backend.constants import AlertSeverity, NotificationChannel  # Module(src.backend.constants)
from src.backend.monitoring.alerting.notification_router import NotificationRouter, NotificationDeliveryResult, determine_notification_channels, format_notification_for_channel  # Module(src.backend.monitoring.alerting.notification_router)
from src.backend.monitoring.integrations.teams_notifier import TeamsNotifier  # Module(src.backend.monitoring.integrations.teams_notifier)
from src.backend.monitoring.integrations.email_notifier import EmailNotifier  # Module(src.backend.monitoring.integrations.email_notifier)
from src.test.fixtures.backend.monitoring_fixtures import create_test_notification, create_test_delivery_result, mock_notification_router  # Module(src.test.fixtures.backend.monitoring_fixtures)


def create_test_message(message_id: str = None, title: str = "Test Notification", severity: AlertSeverity = AlertSeverity.INFO, additional_data: dict = None) -> dict:
    """Creates a test notification message for testing"""
    if message_id is None:
        message_id = str(uuid.uuid4())
    base_message = {
        "message_id": message_id,
        "title": title,
        "severity": severity.value if isinstance(severity, AlertSeverity) else severity,
    }
    if additional_data:
        base_message.update(additional_data)
    return base_message


class TestNotificationRouter:
    """Test class for the NotificationRouter component"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.mock_teams_notifier = unittest.mock.MagicMock(spec=TeamsNotifier)
        self.mock_email_notifier = unittest.mock.MagicMock(spec=EmailNotifier)
        self.router = NotificationRouter(teams_notifier=self.mock_teams_notifier, email_notifier=self.mock_email_notifier)
        self.test_message = create_test_message()

    def test_init(self):
        """Test NotificationRouter initialization"""
        assert self.router._teams_notifier == self.mock_teams_notifier
        assert self.router._email_notifier == self.mock_email_notifier
        assert self.router._channel_config is not None

    def test_send_notification(self):
        """Test send_notification method"""
        self.mock_teams_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.TEAMS)
        self.mock_email_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.EMAIL)

        channels = [NotificationChannel.TEAMS, NotificationChannel.EMAIL]
        results = self.router.send_notification(self.test_message, channels)

        self.mock_teams_notifier.send_notification.assert_called_once()
        self.mock_email_notifier.send_notification.assert_called_once()
        assert NotificationChannel.TEAMS in results
        assert NotificationChannel.EMAIL in results
        assert results[NotificationChannel.TEAMS].success is True
        assert results[NotificationChannel.EMAIL].success is True

    def test_send_to_channel(self):
        """Test send_to_channel method"""
        self.mock_teams_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.TEAMS)
        self.mock_email_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.EMAIL)

        teams_result = self.router.send_to_channel(self.test_message, NotificationChannel.TEAMS)
        email_result = self.router.send_to_channel(self.test_message, NotificationChannel.EMAIL)

        self.mock_teams_notifier.send_notification.assert_called_once()
        self.mock_email_notifier.send_notification.assert_called_once()
        assert teams_result.success is True
        assert email_result.success is True

    def test_send_batch_notifications(self):
        """Test send_batch_notifications method"""
        self.mock_teams_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.TEAMS)
        self.mock_email_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.EMAIL)

        messages = [create_test_message(message_id=str(i)) for i in range(3)]
        channels = [NotificationChannel.TEAMS, NotificationChannel.EMAIL]
        results = self.router.send_batch_notifications(messages, channels)

        assert len(results) == 3
        for message_id, channel_results in results.items():
            assert NotificationChannel.TEAMS in channel_results
            assert NotificationChannel.EMAIL in channel_results
            assert channel_results[NotificationChannel.TEAMS].success is True
            assert channel_results[NotificationChannel.EMAIL].success is True
        assert self.mock_teams_notifier.send_notification.call_count == 3
        assert self.mock_email_notifier.send_notification.call_count == 3

    def test_get_delivery_status(self):
        """Test get_delivery_status method"""
        self.mock_teams_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.TEAMS)
        self.mock_email_notifier.send_notification.return_value = create_test_delivery_result(channel=NotificationChannel.EMAIL)

        channels = [NotificationChannel.TEAMS, NotificationChannel.EMAIL]
        results = self.router.send_notification(self.test_message, channels)
        message_id = self.test_message["message_id"]
        status = self.router.get_delivery_status(message_id)

        assert status is not None
        assert "channels" in status
        assert len(status["channels"]) == 2
        assert status["message"]["title"] == self.test_message["title"]

    def test_update_channel_config(self):
        """Test update_channel_config method"""
        new_config = {"webhook_url": "https://new-webhook.example.com"}
        self.router.update_channel_config(NotificationChannel.TEAMS, new_config)

        self.mock_teams_notifier.update_config.assert_called_once_with(new_config)
        assert self.router._channel_config[NotificationChannel.TEAMS.value] == new_config

    def test_routing_rules(self):
        """Test routing rule management methods"""
        rule = {"conditions": {"alert_type": "test"}, "channels": [NotificationChannel.TEAMS.value]}
        rule_id = self.router.add_routing_rule(rule)
        assert rule_id is not None
        assert self.router.get_routing_rules() == [rule]
        self.router.remove_routing_rule(rule_id)
        assert self.router.get_routing_rules() == []

    def test_apply_routing_rules(self):
        """Test apply_routing_rules method"""
        rule1 = {"conditions": {"alert_type": "test"}, "channels": [NotificationChannel.TEAMS.value]}
        rule2 = {"conditions": {"severity": AlertSeverity.CRITICAL.value}, "channels": [NotificationChannel.EMAIL.value]}
        self.router.add_routing_rule(rule1)
        self.router.add_routing_rule(rule2)

        message1 = create_test_message(additional_data={"alert_type": "test"})
        channels1 = self.router.apply_routing_rules(message1)
        assert NotificationChannel.TEAMS in channels1
        assert NotificationChannel.EMAIL not in channels1

        message2 = create_test_message(severity=AlertSeverity.CRITICAL)
        channels2 = self.router.apply_routing_rules(message2)
        assert NotificationChannel.TEAMS in channels2
        assert NotificationChannel.EMAIL in channels2

    def test_cleanup_delivery_history(self):
        """Test cleanup_delivery_history method"""
        # Add some delivery history records
        now = datetime.datetime.now()
        self.router._delivery_history["old1"] = {"timestamp": (now - datetime.timedelta(days=2)).isoformat()}
        self.router._delivery_history["old2"] = {"timestamp": (now - datetime.timedelta(days=1)).isoformat()}
        self.router._delivery_history["recent"] = {"timestamp": now.isoformat()}

        # Clean up history
        removed_count = self.router.cleanup_delivery_history()
        assert removed_count == 2
        assert "old1" not in self.router._delivery_history
        assert "old2" not in self.router._delivery_history
        assert "recent" in self.router._delivery_history


class TestNotificationDeliveryResult:
    """Test class for the NotificationDeliveryResult class"""

    def test_init(self):
        """Test NotificationDeliveryResult initialization"""
        result = NotificationDeliveryResult(
            notification_id="test_notification",
            channel=NotificationChannel.EMAIL,
            success=True,
            error_message="Test error",
            delivery_details={"key": "value"},
        )
        assert result.notification_id == "test_notification"
        assert result.channel == NotificationChannel.EMAIL
        assert result.success is True
        assert result.error_message == "Test error"
        assert result.delivery_details == {"key": "value"}

    def test_to_dict(self):
        """Test to_dict method"""
        result = NotificationDeliveryResult(
            notification_id="test_notification",
            channel=NotificationChannel.EMAIL,
            success=True,
            error_message="Test error",
            delivery_details={"key": "value"},
        )
        result_dict = result.to_dict()
        assert result_dict["notification_id"] == "test_notification"
        assert result_dict["channel"] == NotificationChannel.EMAIL.value
        assert result_dict["success"] is True
        assert result_dict["error_message"] == "Test error"
        assert result_dict["delivery_details"] == {"key": "value"}

    def test_from_dict(self):
        """Test from_dict class method"""
        result = NotificationDeliveryResult(
            notification_id="test_notification",
            channel=NotificationChannel.EMAIL,
            success=True,
            error_message="Test error",
            delivery_details={"key": "value"},
        )
        result_dict = result.to_dict()
        new_result = NotificationDeliveryResult.from_dict(result_dict)
        assert new_result.notification_id == "test_notification"
        assert new_result.channel == NotificationChannel.EMAIL
        assert new_result.success is True
        assert new_result.error_message == "Test error"
        assert new_result.delivery_details == {"key": "value"}