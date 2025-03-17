"""
Provides test fixtures and helper functions for testing the monitoring and alerting components of the self-healing data pipeline.

Includes mock objects, sample data generators, and utility functions to simplify testing of metric processing, anomaly detection, alert generation, and notification routing.
"""

import pytest  # package_version: 7.3.1
import unittest.mock  # package_version: standard library
import pandas  # package_version: 2.0.x
import numpy  # package_version: 1.24.x
import datetime  # package_version: standard library
import json  # package_version: standard library
import os  # package_version: standard library
from typing import Dict, List, Any, Optional, Union, Callable, Tuple

from src.backend.constants import AlertSeverity, NotificationChannel, METRIC_TYPE_GAUGE, METRIC_TYPE_COUNTER, METRIC_TYPE_HISTOGRAM, ALERT_STATUS_NEW  # Module(src.backend.constants)
from src.backend.monitoring.analyzers.metric_processor import MetricProcessor, MetricTransformer, MetricAggregator  # Module(src.backend.monitoring.analyzers.metric_processor)
from src.backend.monitoring.analyzers.anomaly_detector import AnomalyDetector, StatisticalAnomalyDetector, MLAnomalyDetector, AnomalyRecord  # Module(src.backend.monitoring.analyzers.anomaly_detector)
from src.backend.monitoring.alerting.alert_generator import AlertGenerator, AlertNotification  # Module(src.backend.monitoring.alerting.alert_generator)
from src.backend.monitoring.alerting.notification_router import NotificationRouter, NotificationDeliveryResult  # Module(src.backend.monitoring.alerting.notification_router)
from src.backend.db.models.alert import Alert  # Module(src.backend.db.models.alert)
from src.backend.db.repositories.alert_repository import AlertRepository  # Module(src.backend.db.repositories.alert_repository)
from src.test.utils.test_helpers import generate_unique_id, create_temp_file  # Module(src.test.utils.test_helpers)

SAMPLE_METRICS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'mock_data', 'monitoring', 'sample_metrics.json')
SAMPLE_ALERTS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'mock_data', 'monitoring', 'sample_alerts.json')


def load_sample_metrics() -> List[Dict]:
    """Loads sample metric data from the sample_metrics.json file"""
    with open(SAMPLE_METRICS_PATH, 'r') as f:
        sample_metrics = json.load(f)
    return sample_metrics


def load_sample_alerts() -> List[Dict]:
    """Loads sample alert data from the sample_alerts.json file"""
    with open(SAMPLE_ALERTS_PATH, 'r') as f:
        sample_alerts = json.load(f)
    return sample_alerts


def create_mock_metric_processor(config: Dict = None, processed_metrics: List = None) -> unittest.mock.MagicMock:
    """Creates a mock MetricProcessor for testing"""
    mock_processor = unittest.mock.MagicMock(spec=MetricProcessor)
    mock_processor.process_metrics.return_value = processed_metrics or []
    mock_processor.normalize_metrics.return_value = processed_metrics or []
    mock_processor.transform_metric.return_value = {}
    mock_processor.aggregate_metrics.return_value = []
    mock_processor.filter_metrics.return_value = processed_metrics or []
    return mock_processor


def create_mock_anomaly_detector(config: Dict = None, detected_anomalies: List = None, confidence_score: float = 0.9) -> unittest.mock.MagicMock:
    """Creates a mock AnomalyDetector for testing"""
    mock_detector = unittest.mock.MagicMock(spec=AnomalyDetector)
    mock_detector.detect_anomalies.return_value = detected_anomalies or []
    mock_detector.update_baseline.return_value = {}
    mock_detector.train_detection_model.return_value = True
    mock_detector.get_recent_anomalies.return_value = detected_anomalies or []
    mock_detector.calculate_anomaly_severity.return_value = AlertSeverity.MEDIUM
    mock_detector.get_anomaly_explanation.return_value = "Sample explanation"
    return mock_detector


def create_mock_alert_generator(config: Dict = None, generated_alerts: List = None) -> unittest.mock.MagicMock:
    """Creates a mock AlertGenerator for testing"""
    mock_generator = unittest.mock.MagicMock(spec=AlertGenerator)
    mock_generator.process_metrics.return_value = [generate_unique_id() for _ in range(len(generated_alerts or []))]
    mock_generator.process_events.return_value = [generate_unique_id() for _ in range(len(generated_alerts or []))]
    mock_generator.generate_alert.return_value = generate_unique_id()
    mock_generator.get_alert_statistics.return_value = {}
    mock_generator.get_alert_trend.return_value = {}
    return mock_generator


def create_mock_notification_router(config: Dict = None, delivery_results: Dict = None) -> unittest.mock.MagicMock:
    """Creates a mock NotificationRouter for testing"""
    mock_router = unittest.mock.MagicMock(spec=NotificationRouter)
    mock_router.send_notification.return_value = delivery_results or {}
    mock_router.send_to_channel.return_value = {}
    mock_router.send_batch_notifications.return_value = {}
    mock_router.get_delivery_status.return_value = {}
    return mock_router


def create_test_metric(metric_name: str = "test_metric", metric_type: str = METRIC_TYPE_GAUGE, value: float = 1.0, labels: Dict = None, timestamp: datetime.datetime = None) -> Dict:
    """Creates a test metric dictionary for testing"""
    if labels is None:
        labels = {}
    metric = {
        "name": metric_name,
        "type": metric_type,
        "value": value,
        "labels": labels,
    }
    if timestamp:
        metric["timestamp"] = timestamp.isoformat()
    return metric


def create_test_anomaly_record(anomaly_id: str = None, metric_name: str = "test_metric", anomaly_type: str = "point", anomaly_score: float = 0.9, value: float = 1.0, expected_value: float = 0.5, timestamp: datetime.datetime = None, severity: AlertSeverity = AlertSeverity.MEDIUM, context: Dict = None) -> AnomalyRecord:
    """Creates a test AnomalyRecord instance for testing"""
    if anomaly_id is None:
        anomaly_id = generate_unique_id("anomaly")
    if timestamp is None:
        timestamp = datetime.datetime.now()
    if context is None:
        context = {}
    anomaly = AnomalyRecord(
        anomaly_id=anomaly_id,
        metric_name=metric_name,
        anomaly_type=anomaly_type,
        anomaly_score=anomaly_score,
        value=value,
        expected_value=expected_value,
        timestamp=timestamp,
    )
    anomaly.severity = severity
    anomaly.context = context
    return anomaly


def create_test_alert(alert_id: str = None, alert_type: str = "test_alert", description: str = "Test alert", severity: AlertSeverity = AlertSeverity.MEDIUM, context: Dict = None, status: str = ALERT_STATUS_NEW, component: str = "test_component", execution_id: str = "test_execution") -> Alert:
    """Creates a test Alert instance for testing"""
    if alert_id is None:
        alert_id = generate_unique_id("alert")
    if context is None:
        context = {}
    alert = Alert(
        alert_type=alert_type,
        description=description,
        severity=severity,
        context=context,
        component=component,
        execution_id=execution_id,
        alert_id=alert_id,
    )
    alert.status = status
    return alert


def create_test_notification(alert_id: str = "test_alert", message: Dict = None, channels: List = None) -> AlertNotification:
    """Creates a test AlertNotification instance for testing"""
    if message is None:
        message = {"title": "Test Notification", "message": "This is a test notification"}
    if channels is None:
        channels = [NotificationChannel.EMAIL]
    return AlertNotification(
        alert_id=alert_id,
        message=message,
        channels=channels,
    )


def create_test_delivery_result(notification_id: str = "test_notification", channel: NotificationChannel = NotificationChannel.EMAIL, success: bool = True, error_message: str = None, delivery_details: Dict = None) -> NotificationDeliveryResult:
    """Creates a test NotificationDeliveryResult instance for testing"""
    if notification_id is None:
        notification_id = generate_unique_id("notification")
    if delivery_details is None:
        delivery_details = {}
    return NotificationDeliveryResult(
        notification_id=notification_id,
        channel=channel,
        success=success,
        error_message=error_message,
        delivery_details=delivery_details,
    )


def generate_test_metrics(count: int = 5, metric_name_prefix: str = "test_metric", metric_type: str = METRIC_TYPE_GAUGE, label_template: Dict = None, min_value: float = 0.0, max_value: float = 1.0, start_time: datetime.datetime = None, time_interval: datetime.timedelta = datetime.timedelta(minutes=1)) -> List[Dict]:
    """Generates a list of test metrics for testing"""
    if label_template is None:
        label_template = {}
    if start_time is None:
        start_time = datetime.datetime.now()

    metrics = []
    for i in range(count):
        metric_name = f"{metric_name_prefix}_{i}"
        value = numpy.random.uniform(min_value, max_value)
        labels = {k: f"{v}_{i}" for k, v in label_template.items()}
        timestamp = start_time + i * time_interval
        metric = create_test_metric(metric_name, metric_type, value, labels, timestamp)
        metrics.append(metric)
    return metrics


def generate_test_time_series(num_points: int = 100, metric_name: str = "test_metric", pattern: str = "linear", noise_level: float = 0.1, start_time: datetime.datetime = None, time_interval: datetime.timedelta = datetime.timedelta(minutes=1)) -> pandas.DataFrame:
    """Generates a pandas DataFrame with time series data for testing"""
    if start_time is None:
        start_time = datetime.datetime.now()

    date_range = pandas.date_range(start=start_time, periods=num_points, freq=time_interval)
    values = []

    if pattern == "linear":
        values = numpy.linspace(0, 1, num_points)
    elif pattern == "seasonal":
        values = numpy.sin(numpy.linspace(0, 10 * numpy.pi, num_points))
    elif pattern == "spike":
        values = numpy.random.normal(0, 0.2, num_points)
        spike_indices = numpy.random.choice(num_points, size=int(num_points * 0.05), replace=False)
        values[spike_indices] += numpy.random.uniform(1, 3, len(spike_indices))
    elif pattern == "step":
        values = numpy.zeros(num_points)
        step_index = num_points // 2
        values[step_index:] = 1
    elif pattern == "random":
        values = numpy.random.rand(num_points)
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    # Add random noise
    noise = numpy.random.normal(0, noise_level, num_points)
    values += noise

    df = pandas.DataFrame({'timestamp': date_range, 'value': values})
    df['metric_name'] = metric_name
    df['metric_type'] = METRIC_TYPE_GAUGE
    return df


def generate_anomalous_time_series(num_points: int = 100, metric_name: str = "test_metric", base_pattern: str = "linear", anomaly_type: str = "point", num_anomalies: int = 5, anomaly_magnitude: float = 3.0) -> Tuple[pandas.DataFrame, List]:
    """Generates a time series with embedded anomalies for testing"""
    df = generate_test_time_series(num_points, metric_name, base_pattern)
    anomaly_indices = []

    if anomaly_type == "point":
        anomaly_indices = numpy.random.choice(num_points, size=num_anomalies, replace=False)
        df.loc[anomaly_indices, 'value'] += numpy.random.uniform(anomaly_magnitude, 2 * anomaly_magnitude, num_anomalies)
    elif anomaly_type == "collective":
        start_index = numpy.random.randint(0, num_points - num_anomalies)
        anomaly_indices = numpy.arange(start_index, start_index + num_anomalies)
        df.loc[anomaly_indices, 'value'] += numpy.random.uniform(anomaly_magnitude / 2, anomaly_magnitude, num_anomalies)
    elif anomaly_type == "contextual":
        # Anomalies occur at specific times (e.g., weekends)
        weekend_indices = [i for i, dt in enumerate(df['timestamp']) if dt.weekday() in [5, 6]]
        if len(weekend_indices) < num_anomalies:
            num_anomalies = len(weekend_indices)
        anomaly_indices = numpy.random.choice(weekend_indices, size=num_anomalies, replace=False)
        df.loc[anomaly_indices, 'value'] += numpy.random.uniform(anomaly_magnitude, 2 * anomaly_magnitude, num_anomalies)
    elif anomaly_type == "trend":
        # Simulate a sudden trend change
        trend_change_index = num_points // 2
        df.loc[trend_change_index:, 'value'] += numpy.linspace(0, anomaly_magnitude, num_points - trend_change_index)
        anomaly_indices = list(range(trend_change_index, num_points))

    return df, list(anomaly_indices)


class TestMetricData:
    """Class providing test metric data for monitoring component tests"""

    def __init__(self):
        """Initialize the TestMetricData with sample metrics"""
        self.sample_metrics = load_sample_metrics()
        self.metrics_df = pandas.DataFrame(self.sample_metrics)
        self.metric_types = {m['name']: m['type'] for m in self.sample_metrics}
        self.metric_patterns = {
            'linear': {'type': 'linear', 'noise_level': 0.1},
            'seasonal': {'type': 'seasonal', 'noise_level': 0.2},
            'spike': {'type': 'spike', 'noise_level': 0.3}
        }

    def get_sample_metric(self, metric_name: str = None, metric_type: str = None) -> Dict:
        """Get a sample metric by name or type"""
        if metric_name:
            for metric in self.sample_metrics:
                if metric['name'] == metric_name:
                    return metric.copy()
        if metric_type:
            for metric in self.sample_metrics:
                if metric['type'] == metric_type:
                    return metric.copy()
        return self.sample_metrics[0].copy()

    def get_metrics_by_type(self, metric_type: str) -> List:
        """Get all sample metrics of a specific type"""
        return [metric.copy() for metric in self.sample_metrics if metric['type'] == metric_type]

    def get_metrics_dataframe(self, metric_type: str = None, metric_name: str = None) -> pandas.DataFrame:
        """Get metrics as a pandas DataFrame"""
        df = self.metrics_df.copy()
        if metric_type:
            df = df[df['type'] == metric_type]
        if metric_name:
            df = df[df['name'] == metric_name]
        return df

    def create_time_series(self, pattern_name: str, num_points: int = 100, metric_name: str = "test_metric") -> pandas.DataFrame:
        """Create a time series for a specific metric pattern"""
        pattern = self.metric_patterns[pattern_name]
        return generate_test_time_series(num_points, metric_name, pattern['type'], pattern['noise_level'])

    def create_anomalous_series(self, pattern_name: str, anomaly_type: str, num_points: int = 100, num_anomalies: int = 5) -> Tuple[pandas.DataFrame, List]:
        """Create a time series with anomalies"""
        pattern = self.metric_patterns[pattern_name]
        return generate_anomalous_time_series(num_points, metric_name="test_metric", base_pattern=pattern['type'], anomaly_type=anomaly_type, num_anomalies=num_anomalies)


class TestAlertData:
    """Class providing test alert data for alerting component tests"""

    def __init__(self):
        """Initialize the TestAlertData with sample alerts"""
        self.sample_alerts = load_sample_alerts()
        self.alert_types = {a['alert_type']: a for a in self.sample_alerts}
        self.alert_severities = {a['severity']: a for a in self.sample_alerts}

    def get_sample_alert(self, alert_id: str = None, alert_type: str = None, severity: AlertSeverity = None) -> Dict:
        """Get a sample alert by ID, type, or severity"""
        if alert_id:
            for alert in self.sample_alerts:
                if alert['alert_id'] == alert_id:
                    return alert.copy()
        if alert_type:
            for alert in self.sample_alerts:
                if alert['alert_type'] == alert_type:
                    return alert.copy()
        if severity:
            for alert in self.sample_alerts:
                if alert['severity'] == severity.value:
                    return alert.copy()
        return self.sample_alerts[0].copy()

    def get_alerts_by_type(self, alert_type: str) -> List:
        """Get all sample alerts of a specific type"""
        return [alert.copy() for alert in self.sample_alerts if alert['alert_type'] == alert_type]

    def get_alerts_by_severity(self, severity: AlertSeverity) -> List:
        """Get all sample alerts of a specific severity"""
        return [alert.copy() for alert in self.sample_alerts if alert['severity'] == severity.value]

    def create_alert_batch(self, count: int = 5, alert_type: str = "test_alert", severity: AlertSeverity = AlertSeverity.MEDIUM) -> List[Alert]:
        """Create a batch of test alerts"""
        alerts = []
        for i in range(count):
            alert_id = generate_unique_id("alert")
            description = f"Test alert of type {alert_type} - {i}"
            context = {"key1": f"value1_{i}", "key2": f"value2_{i}"}
            alert = Alert(
                alert_type=alert_type,
                description=description,
                severity=severity,
                context=context,
                alert_id=alert_id
            )
            alerts.append(alert)
        return alerts


class MockMetricProcessor:
    """Mock implementation of MetricProcessor for testing"""

    def __init__(self, processed_metrics: List = None, config: Dict = None):
        """Initialize mock metric processor"""
        self._processed_metrics = processed_metrics or []
        self._config = config or {}
        self._transformers = {}
        self._aggregators = {}

    def process_metrics(self, metrics: List, processing_options: Dict = None) -> List:
        """Mock implementation of process_metrics method"""
        return self._processed_metrics

    def normalize_metrics(self, metrics: List) -> List:
        """Mock implementation of normalize_metrics method"""
        return metrics

    def transform_metric(self, metric: Dict, transformation: str, transformation_params: Dict = None) -> Dict:
        """Mock implementation of transform_metric method"""
        return metric

    def aggregate_metrics(self, metrics: List, dimensions: List, method: str, aggregation_params: Dict = None) -> List:
        """Mock implementation of aggregate_metrics method"""
        return metrics

    def register_transformer(self, name: str, transformer_func: Callable) -> None:
        """Mock implementation of register_transformer method"""
        self._transformers[name] = transformer_func

    def register_aggregator(self, name: str, aggregator_func: Callable) -> None:
        """Mock implementation of register_aggregator method"""
        self._aggregators[name] = aggregator_func


class MockAnomalyDetector:
    """Mock implementation of AnomalyDetector for testing"""

    def __init__(self, detected_anomalies: List = None, confidence_score: float = 0.9, config: Dict = None):
        """Initialize mock anomaly detector"""
        self._detected_anomalies = detected_anomalies or []
        self._confidence_score = confidence_score
        self._config = config or {}
        self._baselines = {}

    def detect_anomalies(self, metrics_data: List, sensitivity: float = 2.0) -> List:
        """Mock implementation of detect_anomalies method"""
        return self._detected_anomalies

    def update_baseline(self, metric_name: str, time_series: pandas.Series) -> Dict:
        """Mock implementation of update_baseline method"""
        return {}

    def train_detection_model(self, historical_data: Dict, metric_names: List) -> bool:
        """Mock implementation of train_detection_model method"""
        return True

    def get_recent_anomalies(self, metric_name: str, limit: int = 10) -> List:
        """Mock implementation of get_recent_anomalies method"""
        if metric_name:
            return [a for a in self._detected_anomalies if a.metric_name == metric_name][:limit]
        return self._detected_anomalies[:limit]

    def calculate_anomaly_severity(self, anomaly: AnomalyRecord) -> AlertSeverity:
        """Mock implementation of calculate_anomaly_severity method"""
        if anomaly.anomaly_score > 0.8:
            return AlertSeverity.CRITICAL
        elif anomaly.anomaly_score > 0.5:
            return AlertSeverity.WARNING
        else:
            return AlertSeverity.INFO

    def get_anomaly_explanation(self, anomaly: AnomalyRecord) -> str:
        """Mock implementation of get_anomaly_explanation method"""
        return f"Anomaly detected in {anomaly.metric_name} with score {anomaly.anomaly_score}"


class MockAlertGenerator:
    """Mock implementation of AlertGenerator for testing"""

    def __init__(self, generated_alerts: List = None, config: Dict = None):
        """Initialize mock alert generator"""
        self._generated_alerts = generated_alerts or []
        self._config = config or {}
        self._alert_counts = {}

    def process_metrics(self, metrics: Dict, context: Dict) -> List:
        """Mock implementation of process_metrics method"""
        return [generate_unique_id() for _ in range(len(self._generated_alerts))]

    def process_events(self, events: Dict, context: Dict) -> List:
        """Mock implementation of process_events method"""
        return [generate_unique_id() for _ in range(len(self._generated_alerts))]

    def generate_alert(self, alert_type: str, description: str, severity: AlertSeverity, context: Dict, execution_id: str = None, component: str = None, channels: List = None) -> str:
        """Mock implementation of generate_alert method"""
        return generate_unique_id()

    def get_alert_statistics(self, time_window_hours: int = None) -> Dict:
        """Mock implementation of get_alert_statistics method"""
        return {}

    def get_alert_trend(self, interval: str, num_intervals: int, severity: AlertSeverity = None) -> Dict:
        """Mock implementation of get_alert_trend method"""
        return {}


class MockNotificationRouter:
    """Mock implementation of NotificationRouter for testing"""

    def __init__(self, delivery_results: Dict = None, config: Dict = None):
        """Initialize mock notification router"""
        self._delivery_results = delivery_results or {}
        self._config = config or {}
        self._delivery_history = {}

    def send_notification(self, message: Dict, channels: List = None) -> Dict:
        """Mock implementation of send_notification method"""
        return self._delivery_results

    def send_to_channel(self, message: Dict, channel: NotificationChannel) -> Dict:
        """Mock implementation of send_to_channel method"""
        return {}

    def send_batch_notifications(self, messages: List, channels: List = None) -> Dict:
        """Mock implementation of send_batch_notifications method"""
        return {}

    def get_delivery_status(self, notification_id: str) -> Dict:
        """Mock implementation of get_delivery_status method"""
        return {}


@pytest.fixture
def mock_metric_processor():
    """Pytest fixture providing a mock metric processor"""
    return create_mock_metric_processor()


@pytest.fixture
def mock_anomaly_detector():
    """Pytest fixture providing a mock anomaly detector"""
    return create_mock_anomaly_detector()


@pytest.fixture
def mock_alert_generator():
    """Pytest fixture providing a mock alert generator"""
    return create_mock_alert_generator()


@pytest.fixture
def mock_notification_router():
    """Pytest fixture providing a mock notification router"""
    return create_mock_notification_router()


@pytest.fixture
def test_metric_data():
    """Pytest fixture providing test metric data"""
    return TestMetricData()


@pytest.fixture
def test_alert_data():
    """Pytest fixture providing test alert data"""
    return TestAlertData()


@pytest.fixture
def sample_metrics():
    """Pytest fixture providing sample metrics"""
    return load_sample_metrics()


@pytest.fixture
def sample_alerts():
    """Pytest fixture providing sample alerts"""
    return load_sample_alerts()