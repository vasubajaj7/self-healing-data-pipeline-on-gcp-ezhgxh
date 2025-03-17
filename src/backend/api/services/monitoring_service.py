# src/backend/api/services/monitoring_service.py
"""Service layer for monitoring and alerting functionality in the self-healing data pipeline.
Provides methods for retrieving metrics, alerts, anomalies, and dashboard data, as well as managing alert configurations and notifications. Acts as an intermediary between API controllers and the underlying monitoring components."""

import typing
import datetime
import pandas  # version 2.0.0+
import json
import uuid

from typing import Dict, List, Any, Optional, Union, Tuple

from ...constants import AlertSeverity, NotificationChannel, ALERT_STATUS_NEW, ALERT_STATUS_ACKNOWLEDGED, ALERT_STATUS_RESOLVED
from ...config import get_config
from ...logging_config import get_logger
from ...monitoring.alerting.alert_generator import AlertGenerator
from ...monitoring.alerting.notification_router import NotificationRouter
from ...monitoring.analyzers.metric_processor import MetricProcessor
from ...monitoring.analyzers.anomaly_detector import AnomalyDetector, StatisticalAnomalyDetector, MLAnomalyDetector, AnomalyRecord
from ...db.repositories.alert_repository import AlertRepository
from ...db.models.alert import Alert
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.monitoring.metric_client import MetricClient
from ...utils.errors.error_types import ResourceNotFoundError, ValidationError

# Initialize module logger
logger = get_logger(__name__)

# Default values
DEFAULT_PAGE_SIZE = 20
DEFAULT_METRICS_LIMIT = 1000
DEFAULT_TIME_WINDOW_HOURS = 24


class MonitoringService:
    """Service class for monitoring and alerting functionality"""

    def __init__(
        self,
        alert_repository: AlertRepository,
        metric_processor: MetricProcessor,
        anomaly_detector: AnomalyDetector,
        alert_generator: AlertGenerator,
        notification_router: NotificationRouter,
        bigquery_client: BigQueryClient,
        metric_client: MetricClient
    ):
        """Initializes the MonitoringService with necessary components"""
        # Store provided components or create new instances if not provided
        self._alert_repository = alert_repository
        self._metric_processor = metric_processor
        self._anomaly_detector = anomaly_detector
        self._alert_generator = alert_generator
        self._notification_router = notification_router
        self._bigquery_client = bigquery_client
        self._metric_client = metric_client

        # Load monitoring configuration from application config
        self._config = get_config().get("monitoring", {})

        # Log successful initialization
        logger.info("MonitoringService initialized")

    def get_metrics(self, page: int, page_size: int, metric_category: str = None, component: str = None, pipeline_id: str = None) -> Tuple[List[Dict[str, Any]], int]:
        """Retrieves a paginated list of pipeline metrics with optional filtering"""
        # Build query parameters based on filters
        query_params = {
            "metric_category": metric_category,
            "component": component,
            "pipeline_id": pipeline_id,
        }

        # Calculate pagination parameters
        offset = (page - 1) * page_size
        limit = page_size

        # Use metric processor to query metrics
        metrics, total_count = self._metric_processor.query_metrics(
            offset=offset, limit=limit, **query_params
        )

        # Return tuple of metrics list and total count
        return metrics, total_count

    def get_metric_by_id(self, metric_id: str) -> Dict[str, Any]:
        """Retrieves a specific metric by ID"""
        # Query metric by ID using metric processor
        metric = self._metric_processor.get_metric(metric_id)

        # If metric not found, raise ResourceNotFoundError
        if not metric:
            raise ResourceNotFoundError(f"Metric not found with ID: {metric_id}")

        # Return metric details as dictionary
        return metric

    def get_metric_time_series(self, metric_name: str, start_time: datetime.datetime, end_time: datetime.datetime, aggregation: str = None, component: str = None, pipeline_id: str = None) -> Dict[str, Any]:
        """Retrieves time series data for a specific metric"""
        # Validate time range parameters
        if start_time >= end_time:
            raise ValidationError("Start time must be before end time")

        # Build query parameters for time series data
        query_params = {
            "metric_name": metric_name,
            "start_time": start_time,
            "end_time": end_time,
            "aggregation": aggregation,
            "component": component,
            "pipeline_id": pipeline_id,
        }

        # Use metric processor to query time series data
        time_series_data = self._metric_processor.get_time_series_data(**query_params)

        # Apply aggregation if specified
        if aggregation:
            time_series_data = self._metric_processor.aggregate_time_series(
                time_series_data, aggregation
            )

        # Format result as time series data points
        formatted_data = self._metric_processor.format_time_series(time_series_data)

        # Return time series data dictionary
        return formatted_data

    def get_alerts(self, page: int, page_size: int, start_time: datetime.datetime = None, end_time: datetime.datetime = None, severity: str = None, status: str = None, component: str = None, pipeline_id: str = None) -> Tuple[List[Alert], int]:
        """Retrieves a paginated list of alerts with optional filtering"""
        # Build search criteria based on filters
        search_criteria = {
            "start_time": start_time,
            "end_time": end_time,
            "severity": severity,
            "status": status,
            "component": component,
            "pipeline_id": pipeline_id,
        }

        # Calculate pagination parameters
        offset = (page - 1) * page_size
        limit = page_size

        # Use alert repository to search alerts
        alerts, total_count = self._alert_repository.search_alerts(
            search_criteria=search_criteria, offset=offset, limit=limit
        )

        # Return tuple of alerts list and total count
        return alerts, total_count

    def get_alert_by_id(self, alert_id: str) -> Alert:
        """Retrieves a specific alert by ID"""
        # Query alert by ID using alert repository
        alert = self._alert_repository.get_alert(alert_id)

        # If alert not found, raise ResourceNotFoundError
        if not alert:
            raise ResourceNotFoundError(f"Alert not found with ID: {alert_id}")

        # Return Alert object
        return alert

    def acknowledge_alert(self, alert_id: str, user_id: str, notes: str = None) -> bool:
        """Acknowledges an alert, updating its status"""
        # Call alert repository's acknowledge_alert method
        success = self._alert_repository.acknowledge_alert(
            alert_id=alert_id, acknowledged_by=user_id, notes=notes
        )

        # If alert not found, raise ResourceNotFoundError
        if not success:
            raise ResourceNotFoundError(f"Alert not found with ID: {alert_id}")

        # Log alert acknowledgment with user information
        logger.info(f"Alert {alert_id} acknowledged by user {user_id}")

        # Return success status
        return success

    def resolve_alert(self, alert_id: str, user_id: str, resolution_details: Dict[str, Any] = None) -> bool:
        """Resolves an alert, updating its status"""
        # Call alert repository's resolve_alert method
        success = self._alert_repository.resolve_alert(
            alert_id=alert_id, resolved_by=user_id, resolution_details=resolution_details
        )

        # If alert not found, raise ResourceNotFoundError
        if not success:
            raise ResourceNotFoundError(f"Alert not found with ID: {alert_id}")

        # Log alert resolution with user information
        logger.info(f"Alert {alert_id} resolved by user {user_id}")

        # Return success status
        return success

    def get_anomalies(self, page: int, page_size: int, start_time: datetime.datetime = None, end_time: datetime.datetime = None, metric_name: str = None, severity: str = None, component: str = None, min_confidence: float = 0.0) -> Tuple[List[AnomalyRecord], int]:
        """Retrieves a paginated list of detected anomalies with optional filtering"""
        # Build query parameters based on filters
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "metric_name": metric_name,
            "severity": severity,
            "component": component,
            "min_confidence": min_confidence,
        }

        # Use anomaly detector to retrieve anomalies
        anomalies = self._anomaly_detector.get_anomalies(**query_params)

        # Filter anomalies based on criteria
        filtered_anomalies = [
            anomaly for anomaly in anomalies if anomaly.confidence >= min_confidence
        ]

        # Apply pagination to results
        offset = (page - 1) * page_size
        paginated_anomalies = filtered_anomalies[offset: offset + page_size]
        total_count = len(filtered_anomalies)

        # Return tuple of anomalies list and total count
        return paginated_anomalies, total_count

    def get_anomaly_by_id(self, anomaly_id: str) -> Dict[str, Any]:
        """Retrieves a specific anomaly by ID"""
        # Query anomaly by ID using anomaly detector
        anomaly = self._anomaly_detector.get_anomaly(anomaly_id)

        # If anomaly not found, raise ResourceNotFoundError
        if not anomaly:
            raise ResourceNotFoundError(f"Anomaly not found with ID: {anomaly_id}")

        # Return anomaly details as dictionary
        return anomaly

    def get_alert_config(self) -> Dict[str, Any]:
        """Retrieves the current alert configuration"""
        # Extract alert configuration from service config
        alert_config = self._config.get("alerting", {})

        # Format configuration for API response
        formatted_config = {"rules": alert_config.get("rules", [])}

        # Return configuration dictionary
        return formatted_config

    def update_alert_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Updates the alert configuration"""
        # Validate configuration data structure
        if not isinstance(config_data, dict) or "rules" not in config_data:
            raise ValidationError("Invalid alert configuration format")

        # Update alert configuration in service config
        self._config["alerting"]["rules"] = config_data["rules"]

        # Apply changes to alert generator and notification router
        # (This might involve reloading rules, updating thresholds, etc.)
        # For example:
        # self._alert_generator.load_rules()
        # self._notification_router.update_routing_rules(config_data["routing_rules"])

        # Return updated configuration dictionary
        return self.get_alert_config()

    def get_system_metrics(self) -> Dict[str, Any]:
        """Retrieves current system-level metrics for monitoring"""
        # Use metric client to collect system-level metrics
        metrics = self._metric_client.collect_system_metrics()

        # Format metrics for API response
        formatted_metrics = {"cpu_usage": metrics.get("cpu_usage"), "memory_usage": metrics.get("memory_usage")}

        # Return system metrics dictionary
        return formatted_metrics

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Retrieves a summary of monitoring data for the dashboard"""
        # Collect pipeline health metrics
        pipeline_health = self._metric_processor.get_pipeline_health_metrics()

        # Collect alert statistics
        alert_stats = self.get_alert_statistics()

        # Collect anomaly statistics
        anomaly_stats = self._anomaly_detector.get_anomaly_statistics()

        # Collect recent executions data
        recent_executions = self._metric_processor.get_recent_executions()

        # Combine data into dashboard summary
        dashboard_summary = {
            "pipeline_health": pipeline_health,
            "alert_statistics": alert_stats,
            "anomaly_statistics": anomaly_stats,
            "recent_executions": recent_executions,
        }

        # Return dashboard summary dictionary
        return dashboard_summary

    def create_alert(self, alert_type: str, description: str, severity: AlertSeverity, context: Dict, component: str = None, execution_id: str = None, channels: List[NotificationChannel] = None) -> str:
        """Creates a new alert in the system"""
        # Call alert generator's generate_alert method
        alert_id = self._alert_generator.generate_alert(alert_type, description, severity, context, execution_id, component, channels)

        # Log alert creation with details
        logger.info(f"Created new alert: {alert_id} - {alert_type} - {severity.value}")

        # Return the alert ID
        return alert_id

    def process_metrics_for_anomalies(self, metrics_data: Dict, sensitivity: float = 2.0) -> Tuple[List[AnomalyRecord], List[str]]:
        """Processes metrics to detect anomalies and generate alerts"""
        # Use anomaly detector to detect anomalies in metrics data
        anomalies = self._anomaly_detector.detect_anomalies(metrics_data, sensitivity)

        # Generate alerts for detected anomalies using alert generator
        alert_ids = self._alert_generator.generate_alerts_from_anomalies(anomalies)

        # Return tuple of anomalies list and alert IDs
        return anomalies, alert_ids

    def send_notification(self, message: Dict, channels: List[NotificationChannel]) -> Dict[NotificationChannel, NotificationDeliveryResult]:
        """Sends a notification to specified channels"""
        # Call notification router's send_notification method
        delivery_results = self._notification_router.send_notification(message, channels)

        # Log notification delivery results
        logger.info(f"Sent notification to channels: {channels} - Results: {delivery_results}")

        # Return delivery results dictionary
        return delivery_results

    def get_notification_channels(self) -> Dict[str, Any]:
        """Retrieves configured notification channels"""
        # Retrieve channel configurations from notification router
        channel_configs = self._notification_router.get_channel_configs()

        # Format configurations for API response
        formatted_configs = {"channels": channel_configs}

        # Return channel configurations dictionary
        return formatted_configs

    def update_notification_channel(self, channel: NotificationChannel, config: Dict) -> bool:
        """Updates configuration for a notification channel"""
        # Call notification router's update_channel_config method
        success = self._notification_router.update_channel_config(channel, config)

        # Log channel configuration update
        logger.info(f"Updated configuration for channel: {channel.value} - Success: {success}")

        # Return success status
        return success

    def get_alert_statistics(self, time_window_hours: int = None) -> Dict[str, Any]:
        """Retrieves statistics about alerts"""
        # Calculate time window for statistics
        if time_window_hours is None:
            time_window_hours = DEFAULT_TIME_WINDOW_HOURS

        # Use alert repository to collect alert counts by different dimensions
        alert_counts_by_severity = self._alert_repository.get_alert_count_by_severity(time_window_hours)
        alert_counts_by_component = self._alert_repository.get_alert_count_by_component(time_window_hours)
        alert_counts_by_status = self._alert_repository.get_alert_count_by_status(time_window_hours)

        # Combine statistics into result dictionary
        alert_statistics = {
            "counts_by_severity": alert_counts_by_severity,
            "counts_by_component": alert_counts_by_component,
            "counts_by_status": alert_counts_by_status,
        }

        # Return alert statistics dictionary
        return alert_statistics

    def get_alert_trend(self, interval: str, num_intervals: int, severity: AlertSeverity = None) -> Dict[str, Any]:
        """Retrieves alert generation trend over time"""
        # Call alert repository's get_alert_trend method
        trend_data = self._alert_repository.get_alert_trend(interval, num_intervals, severity)

        # Format trend data for API response
        formatted_trend_data = {"trend": trend_data}

        # Return trend data dictionary
        return formatted_trend_data
# Placeholder for NotificationDeliveryResult class
class NotificationDeliveryResult:
    pass