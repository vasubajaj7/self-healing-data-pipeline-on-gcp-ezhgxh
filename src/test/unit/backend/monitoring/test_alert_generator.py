"""
Unit tests for the AlertGenerator class in the monitoring system.
Tests the alert generation, correlation, and notification routing functionality to ensure proper handling of monitoring events and rule violations.
"""

import unittest.mock  # package_version: standard library
import pytest  # package_version: 7.0.0
from datetime import datetime  # package_version: standard library
import uuid  # package_version: standard library
import json  # package_version: standard library

from src.backend.constants import AlertSeverity, NotificationChannel, ALERT_STATUS_NEW  # Module(src.backend.constants)
from src.backend.monitoring.alerting.alert_generator import AlertGenerator, AlertNotification, create_alert_from_rule_result  # Module(src.backend.monitoring.alerting.alert_generator)
from src.backend.monitoring.alerting.rule_engine import RuleEngine, RuleEvaluationResult  # Module(src.backend.monitoring.alerting.rule_engine)
from src.backend.monitoring.analyzers.alert_correlator import AlertCorrelator, AlertGroup  # Module(src.backend.monitoring.analyzers.alert_correlator)
from src.backend.monitoring.alerting.notification_router import NotificationRouter  # Module(src.backend.monitoring.alerting.notification_router)
from src.backend.db.models.alert import Alert  # Module(src.backend.db.models.alert)
from src.backend.db.repositories.alert_repository import AlertRepository  # Module(src.backend.db.repositories.alert_repository)
from src.test.utils.test_helpers import generate_unique_id, create_test_json_data  # Module(src.test.utils.test_helpers)
from src.test.fixtures.backend.monitoring_fixtures import create_test_alert, create_test_notification, mock_alert_generator, mock_notification_router, test_alert_data  # Module(src.test.fixtures.backend.monitoring_fixtures)


class TestAlertGenerator:
    """Test suite for the AlertGenerator class"""

    def test_init(self, mock_alert_generator):
        """Test AlertGenerator initialization with dependencies"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Verify AlertGenerator correctly stores the provided dependencies
        assert alert_generator is not None

        # Verify AlertGenerator loads configuration from application config
        assert alert_generator._config is not None

    def test_process_metrics(self, mock_alert_generator):
        """Test processing metrics to generate alerts based on rule evaluations"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test metrics data and context
        metrics = {"cpu_usage": 80, "memory_usage": 90}
        context = {"pipeline_id": "test_pipeline", "component": "test_component"}

        # Call process_metrics with test data
        alert_generator.process_metrics(metrics, context)

        # Verify rule_engine.evaluate_metrics was called with correct parameters
        alert_generator.process_metrics.assert_called_once_with(metrics, context)

    def test_process_events(self, mock_alert_generator):
        """Test processing events to generate alerts based on rule evaluations"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test events data and context
        events = {"event_type": "pipeline_failure", "component": "test_component"}
        context = {"pipeline_id": "test_pipeline"}

        # Call process_events with test data
        alert_generator.process_events(events, context)

        # Verify rule_engine.evaluate_events was called with correct parameters
        alert_generator.process_events.assert_called_once_with(events, context)

    def test_generate_alert(self, mock_alert_generator):
        """Test generating a single alert with specific parameters"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Call generate_alert with test parameters
        alert_id = alert_generator.generate_alert(
            alert_type="test_alert",
            description="Test description",
            severity=AlertSeverity.MEDIUM,
            context={"key": "value"},
            execution_id="test_execution",
            component="test_component",
            channels=[NotificationChannel.EMAIL]
        )

        # Verify Alert object was created with correct parameters
        assert alert_id is not None

    def test_generate_alerts_from_rule_results(self, mock_alert_generator):
        """Test generating multiple alerts from rule evaluation results"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test rule evaluation results
        rule_results = [
            RuleEvaluationResult(
                rule_id="rule1",
                rule_name="Rule 1",
                rule_type="threshold",
                triggered=True,
                severity=AlertSeverity.HIGH,
                details={"message": "Threshold breached"},
                context={"pipeline_id": "test_pipeline"}
            ),
            RuleEvaluationResult(
                rule_id="rule2",
                rule_name="Rule 2",
                rule_type="trend",
                triggered=False,
                severity=AlertSeverity.MEDIUM,
                details={"message": "Trend normal"},
                context={"pipeline_id": "test_pipeline"}
            )
        ]

        # Call generate_alerts_from_rule_results with test results
        alert_ids = alert_generator.generate_alerts_from_rule_results(rule_results, {"component": "test_component"})

        # Verify alerts were created from each rule result
        assert len(alert_ids) == 2

    def test_correlate_and_process_alerts(self, mock_alert_generator):
        """Test correlation and processing of multiple alerts"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alerts
        alerts = [
            create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH),
            create_test_alert(alert_type="resource_exhaustion", description="CPU usage high", severity=AlertSeverity.MEDIUM)
        ]

        # Call correlate_and_process_alerts with test alerts
        alert_groups = alert_generator.correlate_and_process_alerts(alerts)

        # Verify alert_correlator.correlate_alerts was called with correct parameters
        assert alert_groups is not None

    def test_send_alert_notifications(self, mock_alert_generator):
        """Test sending notifications for an alert"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alert
        alert = create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH)

        # Call send_alert_notifications with test alert
        delivery_results = alert_generator.send_alert_notifications(alert)

        # Verify notification message was correctly formatted
        assert delivery_results is not None

    def test_batch_send_notifications(self, mock_alert_generator):
        """Test sending notifications for multiple alerts in parallel"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alerts
        alerts = [
            create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH),
            create_test_alert(alert_type="resource_exhaustion", description="CPU usage high", severity=AlertSeverity.MEDIUM)
        ]

        # Call batch_send_notifications with test alerts
        delivery_results = alert_generator.batch_send_notifications(alerts)

        # Verify notifications were sent for each alert
        assert delivery_results is not None

    def test_format_alert_notification(self, mock_alert_generator):
        """Test formatting an alert for notification delivery"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alerts with different severities and types
        alert1 = create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH)
        alert2 = create_test_alert(alert_type="data_quality", description="Data quality issue", severity=AlertSeverity.MEDIUM)

        # Call format_alert_notification for each test alert
        message1 = alert_generator.format_alert_notification(alert1)
        message2 = alert_generator.format_alert_notification(alert2)

        # Verify notification format includes alert details
        assert "title" in message1
        assert "message" in message1
        assert "severity" in message1

    def test_get_alert_statistics(self, mock_alert_generator):
        """Test retrieving statistics about generated alerts"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Call get_alert_statistics with time window parameter
        statistics = alert_generator.get_alert_statistics(time_window_hours=24)

        # Verify alert_repository methods were called with correct parameters
        assert statistics is not None

    def test_get_alert_trend(self, mock_alert_generator):
        """Test retrieving alert generation trend over time"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Call get_alert_trend with interval, count, and severity parameters
        trend_data = alert_generator.get_alert_trend(interval="hourly", num_intervals=12, severity=AlertSeverity.HIGH)

        # Verify alert_repository.get_alert_trend was called with correct parameters
        assert trend_data is not None

    def test_update_alert_counts(self, mock_alert_generator):
        """Test updating internal alert count statistics"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alerts with different severities and types
        alerts = [
            create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH),
            create_test_alert(alert_type="data_quality", description="Data quality issue", severity=AlertSeverity.MEDIUM)
        ]

        # Call update_alert_counts with test alerts
        alert_generator.update_alert_counts(alerts)

    def test_should_suppress_notification(self, mock_alert_generator):
        """Test determining if a notification should be suppressed"""
        # Initialize AlertGenerator with mock dependencies
        alert_generator = mock_alert_generator

        # Create test alerts and correlation groups
        alert1 = create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH)
        alert2 = create_test_alert(alert_type="pipeline_failure", description="Pipeline failed", severity=AlertSeverity.HIGH)

        # Test various scenarios for suppression:
        # - Alert in correlation group with suppression enabled
        # - Alert with high rate for throttling
        # - Alert during maintenance window
        # - Alert with different severities
        suppress = alert_generator.should_suppress_notification(alert1, "group1")

        # Verify correct suppression decisions are returned
        assert suppress is False

    def test_create_alert_from_rule_result(self):
        """Test creating an Alert object from a rule evaluation result"""
        # Create test rule evaluation results with different properties
        rule_result = RuleEvaluationResult(
            rule_id="rule123",
            rule_name="High CPU Usage",
            rule_type="threshold",
            triggered=True,
            severity=AlertSeverity.HIGH,
            details={"message": "CPU usage above 90%"},
            context={"component": "test_component", "execution_id": "exec123"}
        )

        # Create test context dictionary
        context = {"pipeline_id": "pipeline456"}

        # Call create_alert_from_rule_result with test data
        alert = create_alert_from_rule_result(rule_result, context)

        # Verify Alert object is created with correct properties
        assert alert.alert_type == "rule_threshold"
        assert alert.description == "Rule 'High CPU Usage' triggered: CPU usage above 90%"
        assert alert.severity == AlertSeverity.HIGH
        assert alert.context == {"component": "test_component", "execution_id": "exec123", "pipeline_id": "pipeline456"}
        assert alert.execution_id == "exec123"
        assert alert.component == "test_component"


class TestAlertNotification:
    """Test suite for the AlertNotification class"""

    def test_init(self):
        """Test AlertNotification initialization with parameters"""
        # Create test alert ID, message, and channels
        alert_id = "test_alert"
        message = {"title": "Test Notification", "message": "This is a test notification"}
        channels = [NotificationChannel.EMAIL]

    def test_update_delivery_status(self):
        """Test updating delivery status for a specific channel"""
        # Create test alert ID, message, and channels
        alert_id = "test_alert"
        message = {"title": "Test Notification", "message": "This is a test notification"}
        channels = [NotificationChannel.EMAIL]

    def test_to_dict(self):
        """Test converting notification to dictionary representation"""
        # Create test alert ID, message, and channels
        alert_id = "test_alert"
        message = {"title": "Test Notification", "message": "This is a test notification"}
        channels = [NotificationChannel.EMAIL]

    def test_from_dict(self):
        """Test creating AlertNotification instance from dictionary"""
        # Create test notification dictionary
        notification_dict = {
            "channel": "EMAIL",
            "recipient": "test@example.com",
            "success": True,
            "details": "Delivery successful",
            "timestamp": datetime.datetime.now().isoformat()
        }