"""
Package initialization file for the monitoring module of the self-healing data pipeline. 
This module serves as the entry point for the monitoring system, exposing key components from submodules including alerting, analyzers, collectors, integrations, and visualization. It provides a unified interface for monitoring pipeline health, detecting anomalies, generating alerts, and visualizing performance metrics.
"""

from .alerting import AlertGenerator, AlertNotification, NotificationRouter, RuleEngine, EscalationManager  # Import alerting components for alert generation and notification
from .analyzers import MetricProcessor, AnomalyDetector, AlertCorrelator, MetricForecaster  # Import analyzers for metric processing, anomaly detection, and forecasting
from .collectors import EventCapture, MetricCollector, LogIngestion, StateTracker  # Import collectors for metrics, events, logs, and state tracking
from .integrations import CloudMonitoringClient, TeamsNotifier, EmailNotifier, LogsAnalyzer  # Import integrations with external services like Cloud Monitoring and notification channels
from .visualization import DashboardEngine, DiagnosticTools, HistoricalAnalyzer, ReportGenerator  # Import visualization components for dashboards and reports
from ..logging_config import get_logger  # Configure logging for the module

# Initialize module logger
logger = get_logger(__name__)

# Define module version
__version__ = "1.0.0"

# Define what to expose from the module
__all__ = [
    "AlertGenerator", 
    "AlertNotification", 
    "NotificationRouter", 
    "RuleEngine", 
    "EscalationManager",
    "MetricProcessor", 
    "AnomalyDetector", 
    "AlertCorrelator", 
    "MetricForecaster",
    "EventCapture", 
    "MetricCollector", 
    "LogIngestion", 
    "StateTracker",
    "CloudMonitoringClient", 
    "TeamsNotifier", 
    "EmailNotifier",
    "LogsAnalyzer",
    "DashboardEngine", 
    "DiagnosticTools", 
    "HistoricalAnalyzer", 
    "ReportGenerator"
]