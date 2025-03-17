"""
Specialized test case generator for monitoring and alerting testing.

This module provides tools to create test cases with various metrics, anomalies, and alerts
to facilitate thorough testing of the monitoring system, anomaly detection, and
alert generation components.
"""

import os
import json
import random
import uuid
import datetime
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import numpy as np

from src.test.testcase_generators.schema_data_generator import TestCaseGenerator, SchemaDataTestCase
from src.backend.constants import (
    AlertSeverity, 
    NotificationChannel, 
    METRIC_TYPE_GAUGE, 
    METRIC_TYPE_COUNTER, 
    METRIC_TYPE_HISTOGRAM
)
from src.test.utils.test_helpers import create_test_dataframe, generate_unique_id

# Constants
MONITORING_TEST_CASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'monitoring')
DEFAULT_NUM_VARIATIONS = 5
DEFAULT_NUM_METRICS = 10
DEFAULT_NUM_ANOMALIES = 3
DEFAULT_NUM_ALERTS = 5

# Define patterns for generating different types of time series
METRIC_PATTERNS = ['linear', 'seasonal', 'spike', 'step', 'random']
ANOMALY_TYPES = ['point', 'contextual', 'collective', 'trend']
ALERT_TYPES = ['anomaly', 'threshold', 'system', 'data_quality', 'pipeline']


def generate_test_metric(
    metric_name: str,
    metric_type: str,
    value: float,
    labels: Dict = None,
    timestamp: datetime.datetime = None
) -> Dict:
    """
    Generates a test metric dictionary with specified properties.
    
    Args:
        metric_name: Name of the metric
        metric_type: Type of metric (counter, gauge, histogram)
        value: Metric value
        labels: Labels/dimensions for the metric
        timestamp: Timestamp for the metric
        
    Returns:
        Test metric dictionary
    """
    if labels is None:
        labels = {}
    
    if timestamp is None:
        timestamp = datetime.datetime.now()
        
    # Convert timestamp to ISO format if it's a datetime object
    timestamp_str = timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else timestamp
    
    return {
        'metric_id': f"metric-{uuid.uuid4()}",
        'name': metric_name,
        'type': metric_type,
        'value': value,
        'labels': labels,
        'timestamp': timestamp_str
    }


def generate_test_metrics_batch(
    num_metrics: int = DEFAULT_NUM_METRICS,
    metric_names: List[str] = None,
    metric_types: List[str] = None,
    start_time: datetime.datetime = None,
    time_interval: datetime.timedelta = None
) -> List[Dict]:
    """
    Generates a batch of test metrics with various patterns.
    
    Args:
        num_metrics: Number of metrics to generate
        metric_names: List of metric names to use (random if not provided)
        metric_types: List of metric types to use (random if not provided)
        start_time: Starting timestamp for metrics
        time_interval: Time interval between metrics
        
    Returns:
        List of test metric dictionaries
    """
    if metric_names is None:
        metric_names = [f"test_metric_{i}" for i in range(1, 11)]
    
    if metric_types is None:
        metric_types = [METRIC_TYPE_GAUGE, METRIC_TYPE_COUNTER, METRIC_TYPE_HISTOGRAM]
    
    if start_time is None:
        start_time = datetime.datetime.now() - datetime.timedelta(hours=1)
    
    if time_interval is None:
        time_interval = datetime.timedelta(minutes=1)
    
    metrics = []
    
    for i in range(num_metrics):
        metric_name = random.choice(metric_names) if metric_names else f"metric_{i}"
        metric_type = random.choice(metric_types) if metric_types else METRIC_TYPE_GAUGE
        
        # Generate appropriate values based on metric type
        if metric_type == METRIC_TYPE_COUNTER:
            value = random.randint(0, 1000)
        elif metric_type == METRIC_TYPE_GAUGE:
            value = random.uniform(0, 100)
        elif metric_type == METRIC_TYPE_HISTOGRAM:
            value = random.uniform(0, 1000)
        else:
            value = random.uniform(0, 100)
        
        # Create labels with some context
        labels = {
            'environment': random.choice(['development', 'staging', 'production']),
            'component': random.choice(['ingestion', 'processing', 'storage', 'api']),
            'instance': f"instance-{random.randint(1, 5)}"
        }
        
        # Calculate timestamp based on start_time and index
        timestamp = start_time + (time_interval * i)
        
        metrics.append(generate_test_metric(
            metric_name=metric_name,
            metric_type=metric_type,
            value=value,
            labels=labels,
            timestamp=timestamp
        ))
    
    return metrics


def generate_time_series_data(
    pattern: str = 'random',
    num_points: int = 100,
    min_value: float = 0,
    max_value: float = 100,
    noise_level: float = 0.1,
    start_time: datetime.datetime = None,
    time_interval: datetime.timedelta = None
) -> pd.DataFrame:
    """
    Generates time series data with specified pattern.
    
    Args:
        pattern: Pattern type ('linear', 'seasonal', 'spike', 'step', 'random')
        num_points: Number of data points to generate
        min_value: Minimum value in the series
        max_value: Maximum value in the series
        noise_level: Level of random noise to add (0.0 to 1.0)
        start_time: Starting timestamp for the series
        time_interval: Time interval between points
        
    Returns:
        DataFrame with time series data
    """
    if start_time is None:
        start_time = datetime.datetime.now() - datetime.timedelta(hours=num_points / 60)
    
    if time_interval is None:
        time_interval = datetime.timedelta(minutes=1)
    
    # Create timestamp column
    timestamps = [start_time + (time_interval * i) for i in range(num_points)]
    
    # Generate values based on the specified pattern
    values = []
    
    if pattern == 'linear':
        # Linear increasing or decreasing trend
        slope = random.uniform(-1, 1)
        base_values = [i * slope for i in range(num_points)]
        
    elif pattern == 'seasonal':
        # Sinusoidal pattern with seasonality
        period = random.randint(10, 30)
        amplitude = random.uniform(0.5, 1.0)
        base_values = [amplitude * np.sin(2 * np.pi * i / period) for i in range(num_points)]
        
    elif pattern == 'spike':
        # Normal pattern with occasional spikes
        base_values = [0] * num_points
        
        # Add occasional spikes
        num_spikes = random.randint(1, max(2, num_points // 20))
        for _ in range(num_spikes):
            spike_idx = random.randint(0, num_points - 1)
            spike_value = random.choice([-1, 1]) * random.uniform(0.7, 1.0)
            base_values[spike_idx] = spike_value
            
    elif pattern == 'step':
        # Step function with sudden level changes
        base_values = [0] * num_points
        current_level = 0
        
        # Add steps
        num_steps = random.randint(1, max(2, num_points // 25))
        step_points = sorted(random.sample(range(num_points), num_steps))
        
        for i in range(num_points):
            if i in step_points:
                step_change = random.choice([-1, 1]) * random.uniform(0.3, 0.7)
                current_level += step_change
            base_values[i] = current_level
            
    else:  # 'random' or default
        # Random walk pattern
        base_values = [0]
        for i in range(1, num_points):
            step = random.uniform(-0.1, 0.1)
            base_values.append(base_values[-1] + step)
    
    # Add noise
    noise = np.random.normal(0, noise_level, num_points)
    
    # Combine base values with noise
    raw_values = [base_values[i] + noise[i] for i in range(num_points)]
    
    # Scale to fit within min_value and max_value
    min_raw = min(raw_values)
    max_raw = max(raw_values)
    
    if max_raw > min_raw:
        values = [
            min_value + (max_value - min_value) * (v - min_raw) / (max_raw - min_raw)
            for v in raw_values
        ]
    else:
        # Handle constant case
        values = [(min_value + max_value) / 2] * num_points
    
    # Create DataFrame
    return pd.DataFrame({
        'timestamp': timestamps,
        'value': values
    })


def inject_anomalies(
    time_series: pd.DataFrame,
    anomaly_type: str = 'point',
    num_anomalies: int = DEFAULT_NUM_ANOMALIES,
    anomaly_magnitude: float = 3.0
) -> Tuple[pd.DataFrame, List[int]]:
    """
    Injects anomalies into time series data.
    
    Args:
        time_series: DataFrame with time series data
        anomaly_type: Type of anomaly to inject
        num_anomalies: Number of anomalies to inject
        anomaly_magnitude: Magnitude of anomalies relative to normal variation
        
    Returns:
        Tuple of (DataFrame with anomalies, list of anomaly indices)
    """
    # Create a copy to avoid modifying original
    df = time_series.copy()
    anomaly_indices = []
    
    # Get basic statistics of the time series
    mean_val = df['value'].mean()
    std_val = df['value'].std() if df['value'].std() > 0 else 1.0
    
    # Maximum number of anomalies based on series length
    max_possible_anomalies = min(num_anomalies, len(df) // 5)
    num_anomalies = max(1, max_possible_anomalies)
    
    if anomaly_type == 'point':
        # Individual outlier points
        anomaly_indices = random.sample(range(len(df)), num_anomalies)
        
        for idx in anomaly_indices:
            # Decide whether to go high or low
            direction = random.choice([-1, 1])
            df.loc[idx, 'value'] = mean_val + direction * anomaly_magnitude * std_val
            
    elif anomaly_type == 'contextual':
        # Values that are unusual in specific contexts (e.g., time of day)
        # For simplicity, we'll create anomalies that are unusual relative to nearby points
        
        window_size = len(df) // 10
        if window_size < 3:
            window_size = 3
            
        anomaly_indices = random.sample(range(window_size, len(df) - window_size), num_anomalies)
        
        for idx in anomaly_indices:
            # Get local mean and std
            local_window = df.iloc[idx-window_size:idx+window_size]
            local_mean = local_window['value'].mean()
            local_std = local_window['value'].std() if local_window['value'].std() > 0 else std_val
            
            # Create contextual anomaly
            direction = random.choice([-1, 1])
            df.loc[idx, 'value'] = local_mean + direction * anomaly_magnitude * local_std
            
    elif anomaly_type == 'collective':
        # Sequence of unusual values
        collective_length = random.randint(3, min(10, len(df) // 10))
        
        # Number of collective anomaly groups
        num_groups = max(1, num_anomalies // collective_length)
        
        for _ in range(num_groups):
            # Choose starting point for collective anomaly
            start_idx = random.randint(0, len(df) - collective_length - 1)
            
            # Generate anomaly pattern (e.g., sudden level shift)
            direction = random.choice([-1, 1])
            shift = direction * anomaly_magnitude * std_val
            
            # Apply the pattern
            for i in range(collective_length):
                idx = start_idx + i
                df.loc[idx, 'value'] = df.loc[idx, 'value'] + shift
                anomaly_indices.append(idx)
                
    elif anomaly_type == 'trend':
        # Unusual trend change or slope
        trend_length = random.randint(5, min(20, len(df) // 5))
        
        # Choose starting point for trend anomaly
        start_idx = random.randint(0, len(df) - trend_length - 1)
        
        # Generate trend pattern
        direction = random.choice([-1, 1])
        slope = direction * anomaly_magnitude * std_val / trend_length
        
        # Apply the trend
        for i in range(trend_length):
            idx = start_idx + i
            df.loc[idx, 'value'] = df.loc[idx, 'value'] + (i * slope)
            anomaly_indices.append(idx)
            
    return df, anomaly_indices


def generate_test_anomaly_record(
    metric_name: str,
    anomaly_type: str,
    anomaly_score: float,
    value: float,
    expected_value: float,
    timestamp: datetime.datetime = None,
    severity: AlertSeverity = None,
    context: Dict = None
) -> Dict:
    """
    Generates a test anomaly record dictionary.
    
    Args:
        metric_name: Name of the metric with anomaly
        anomaly_type: Type of anomaly detected
        anomaly_score: Confidence score of the anomaly (0-1)
        value: Actual value that was anomalous
        expected_value: Expected value or range
        timestamp: When the anomaly occurred
        severity: Severity level of the anomaly
        context: Additional context information
        
    Returns:
        Test anomaly record dictionary
    """
    anomaly_id = f"anomaly-{uuid.uuid4()}"
    
    if timestamp is None:
        timestamp = datetime.datetime.now()
        
    if severity is None:
        # Assign severity based on anomaly score
        if anomaly_score > 0.9:
            severity = AlertSeverity.CRITICAL
        elif anomaly_score > 0.7:
            severity = AlertSeverity.HIGH
        elif anomaly_score > 0.5:
            severity = AlertSeverity.MEDIUM
        else:
            severity = AlertSeverity.LOW
    
    if context is None:
        context = {}
        
    # Convert severity to string representation
    severity_str = severity.value if isinstance(severity, AlertSeverity) else severity
    
    # Calculate deviation
    deviation = abs(value - expected_value)
    
    return {
        'anomaly_id': anomaly_id,
        'metric_name': metric_name,
        'anomaly_type': anomaly_type,
        'anomaly_score': anomaly_score,
        'value': value,
        'expected_value': expected_value,
        'deviation': deviation,
        'timestamp': timestamp.isoformat() if isinstance(timestamp, datetime.datetime) else timestamp,
        'severity': severity_str,
        'context': context
    }


def generate_test_alert(
    alert_type: str,
    description: str,
    severity: AlertSeverity = None,
    context: Dict = None,
    status: str = None,
    component: str = None,
    execution_id: str = None,
    created_at: datetime.datetime = None
) -> Dict:
    """
    Generates a test alert dictionary.
    
    Args:
        alert_type: Type of alert
        description: Description of the alert
        severity: Severity level of the alert
        context: Additional context information
        status: Alert status
        component: Component that generated the alert
        execution_id: Associated execution ID if applicable
        created_at: When the alert was created
        
    Returns:
        Test alert dictionary
    """
    alert_id = f"alert-{uuid.uuid4()}"
    
    if severity is None:
        severity = random.choice(list(AlertSeverity))
        
    if context is None:
        context = {}
        
    if status is None:
        status = random.choice(['ACTIVE', 'ACKNOWLEDGED', 'RESOLVED'])
        
    if component is None:
        component = random.choice(['ingestion', 'processing', 'quality', 'storage', 'api'])
        
    if created_at is None:
        created_at = datetime.datetime.now()
        
    # Convert severity to string representation
    severity_str = severity.value if isinstance(severity, AlertSeverity) else severity
    
    return {
        'alert_id': alert_id,
        'alert_type': alert_type,
        'description': description,
        'severity': severity_str,
        'context': context,
        'status': status,
        'component': component,
        'execution_id': execution_id,
        'created_at': created_at.isoformat() if isinstance(created_at, datetime.datetime) else created_at
    }


def generate_test_notification(
    alert_id: str = None,
    message: Dict = None,
    channels: List = None,
    delivery_status: Dict = None
) -> Dict:
    """
    Generates a test notification dictionary.
    
    Args:
        alert_id: ID of the associated alert
        message: Message content
        channels: Notification channels
        delivery_status: Delivery status for each channel
        
    Returns:
        Test notification dictionary
    """
    notification_id = f"notification-{uuid.uuid4()}"
    
    if alert_id is None:
        alert_id = f"alert-{uuid.uuid4()}"
        
    if message is None:
        message = {
            'title': 'Test Notification',
            'body': 'This is a test notification message',
            'details_url': f"https://example.com/alerts/{alert_id}"
        }
        
    if channels is None:
        channels = ['EMAIL', 'TEAMS']
        
    if delivery_status is None:
        delivery_status = {
            channel: {'status': 'DELIVERED', 'timestamp': datetime.datetime.now().isoformat()}
            for channel in channels
        }
        
    return {
        'notification_id': notification_id,
        'alert_id': alert_id,
        'message': message,
        'channels': channels,
        'delivery_status': delivery_status,
        'created_at': datetime.datetime.now().isoformat()
    }


def save_monitoring_test_case(
    test_case: Dict,
    test_case_name: str,
    output_dir: str = MONITORING_TEST_CASE_DIR
) -> Dict:
    """
    Saves a monitoring test case to files.
    
    Args:
        test_case: Test case with metrics, time series, anomalies, alerts, etc.
        test_case_name: Name for the test case
        output_dir: Directory to save test case files
        
    Returns:
        Paths to saved test case files
    """
    # Create test case directory
    test_case_dir = os.path.join(output_dir, test_case_name)
    os.makedirs(test_case_dir, exist_ok=True)
    
    file_paths = {
        'test_case_name': test_case_name
    }
    
    # Save metrics to JSON file if present
    if 'metrics' in test_case:
        metrics_path = os.path.join(test_case_dir, 'metrics.json')
        with open(metrics_path, 'w') as f:
            json.dump(test_case['metrics'], f, indent=2)
        file_paths['metrics_path'] = metrics_path
    
    # Save time series data to CSV file if present
    if 'time_series' in test_case:
        time_series_path = os.path.join(test_case_dir, 'time_series.csv')
        if isinstance(test_case['time_series'], pd.DataFrame):
            test_case['time_series'].to_csv(time_series_path, index=False)
        else:
            pd.DataFrame(test_case['time_series']).to_csv(time_series_path, index=False)
        file_paths['time_series_path'] = time_series_path
    
    # Save anomalies to JSON file if present
    if 'anomalies' in test_case:
        anomalies_path = os.path.join(test_case_dir, 'anomalies.json')
        with open(anomalies_path, 'w') as f:
            json.dump(test_case['anomalies'], f, indent=2)
        file_paths['anomalies_path'] = anomalies_path
    
    # Save alerts to JSON file if present
    if 'alerts' in test_case:
        alerts_path = os.path.join(test_case_dir, 'alerts.json')
        with open(alerts_path, 'w') as f:
            json.dump(test_case['alerts'], f, indent=2)
        file_paths['alerts_path'] = alerts_path
        
    # Save notifications to JSON file if present
    if 'notifications' in test_case:
        notifications_path = os.path.join(test_case_dir, 'notifications.json')
        with open(notifications_path, 'w') as f:
            json.dump(test_case['notifications'], f, indent=2)
        file_paths['notifications_path'] = notifications_path
        
    # Save expected results to JSON file if present
    if 'expected_results' in test_case:
        expected_path = os.path.join(test_case_dir, 'expected_results.json')
        with open(expected_path, 'w') as f:
            json.dump(test_case['expected_results'], f, indent=2)
        file_paths['expected_results_path'] = expected_path
    
    return file_paths


def load_monitoring_test_case(
    test_case_name: str,
    input_dir: str = MONITORING_TEST_CASE_DIR
) -> Dict:
    """
    Loads a previously saved monitoring test case.
    
    Args:
        test_case_name: Name of the test case
        input_dir: Directory containing the test case
        
    Returns:
        Loaded test case with all components
    """
    test_case_dir = os.path.join(input_dir, test_case_name)
    
    if not os.path.exists(test_case_dir):
        raise FileNotFoundError(f"Test case directory not found: {test_case_dir}")
    
    test_case = {
        'test_case_name': test_case_name
    }
    
    # Load metrics if exists
    metrics_path = os.path.join(test_case_dir, 'metrics.json')
    if os.path.exists(metrics_path):
        with open(metrics_path, 'r') as f:
            test_case['metrics'] = json.load(f)
    
    # Load time series if exists
    time_series_path = os.path.join(test_case_dir, 'time_series.csv')
    if os.path.exists(time_series_path):
        test_case['time_series'] = pd.read_csv(time_series_path)
    
    # Load anomalies if exists
    anomalies_path = os.path.join(test_case_dir, 'anomalies.json')
    if os.path.exists(anomalies_path):
        with open(anomalies_path, 'r') as f:
            test_case['anomalies'] = json.load(f)
    
    # Load alerts if exists
    alerts_path = os.path.join(test_case_dir, 'alerts.json')
    if os.path.exists(alerts_path):
        with open(alerts_path, 'r') as f:
            test_case['alerts'] = json.load(f)
            
    # Load notifications if exists
    notifications_path = os.path.join(test_case_dir, 'notifications.json')
    if os.path.exists(notifications_path):
        with open(notifications_path, 'r') as f:
            test_case['notifications'] = json.load(f)
            
    # Load expected results if exists
    expected_path = os.path.join(test_case_dir, 'expected_results.json')
    if os.path.exists(expected_path):
        with open(expected_path, 'r') as f:
            test_case['expected_results'] = json.load(f)
    
    return test_case


class MonitoringTestCaseGenerator(TestCaseGenerator):
    """
    Generator for creating test cases specifically for monitoring and alerting testing.
    """
    
    def __init__(self, output_dir: str = MONITORING_TEST_CASE_DIR):
        """
        Initialize the MonitoringTestCaseGenerator.
        
        Args:
            output_dir: Directory to save generated test cases
        """
        super().__init__(output_dir)
        
        # Initialize specialized generators for different metric types
        self._metric_generators = {
            'gauge': lambda: random.uniform(0, 100),
            'counter': lambda: random.randint(0, 1000),
            'histogram': lambda: random.uniform(0, 1000)
        }
        
        # Initialize specialized generators for different anomaly types
        self._anomaly_generators = {
            'point': lambda ts: inject_anomalies(ts, 'point'),
            'contextual': lambda ts: inject_anomalies(ts, 'contextual'),
            'collective': lambda ts: inject_anomalies(ts, 'collective'),
            'trend': lambda ts: inject_anomalies(ts, 'trend')
        }
        
        # Initialize specialized generators for different alert types
        self._alert_generators = {
            'anomaly': lambda: generate_test_alert(
                alert_type='anomaly',
                description=f"Anomaly detected in metric {random.choice(['cpu', 'memory', 'latency'])}",
                severity=random.choice([AlertSeverity.HIGH, AlertSeverity.MEDIUM])
            ),
            'threshold': lambda: generate_test_alert(
                alert_type='threshold',
                description=f"Threshold exceeded for {random.choice(['disk_usage', 'error_rate', 'queue_depth'])}",
                severity=random.choice([AlertSeverity.CRITICAL, AlertSeverity.HIGH])
            ),
            'system': lambda: generate_test_alert(
                alert_type='system',
                description=f"System issue detected: {random.choice(['service_down', 'high_load', 'network_error'])}",
                severity=random.choice([AlertSeverity.CRITICAL, AlertSeverity.HIGH, AlertSeverity.MEDIUM])
            ),
            'data_quality': lambda: generate_test_alert(
                alert_type='data_quality',
                description=f"Data quality issue: {random.choice(['schema_drift', 'null_values', 'duplicates'])}",
                severity=random.choice([AlertSeverity.HIGH, AlertSeverity.MEDIUM, AlertSeverity.LOW])
            ),
            'pipeline': lambda: generate_test_alert(
                alert_type='pipeline',
                description=f"Pipeline issue: {random.choice(['task_failure', 'timeout', 'dependency_error'])}",
                severity=random.choice([AlertSeverity.HIGH, AlertSeverity.MEDIUM])
            )
        }
    
    def generate_metrics_test_case(
        self,
        metrics_config: Dict = None,
        test_case_name: str = None,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case with various metrics for testing.
        
        Args:
            metrics_config: Configuration for metrics generation
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with metrics data
        """
        if metrics_config is None:
            metrics_config = {}
            
        if test_case_name is None:
            test_case_name = f"metrics_test_{generate_unique_id()}"
            
        # Extract configuration parameters
        num_metrics = metrics_config.get('num_metrics', DEFAULT_NUM_METRICS)
        metric_names = metrics_config.get('metric_names')
        metric_types = metrics_config.get('metric_types', [METRIC_TYPE_GAUGE, METRIC_TYPE_COUNTER, METRIC_TYPE_HISTOGRAM])
        start_time = metrics_config.get('start_time')
        time_interval = metrics_config.get('time_interval')
        metric_patterns = metrics_config.get('patterns', METRIC_PATTERNS)
        
        # Generate batch of test metrics
        metrics = generate_test_metrics_batch(
            num_metrics=num_metrics,
            metric_names=metric_names,
            metric_types=metric_types,
            start_time=start_time,
            time_interval=time_interval
        )
        
        # Generate time series data for a subset of metrics
        selected_metrics = random.sample(metrics, min(3, len(metrics)))
        time_series_data = {}
        
        for metric in selected_metrics:
            pattern = random.choice(metric_patterns)
            ts_data = generate_time_series_data(
                pattern=pattern,
                num_points=100,
                min_value=0,
                max_value=100,
                noise_level=0.1
            )
            
            metric_id = metric['metric_id']
            time_series_data[metric_id] = {
                'metric': metric,
                'pattern': pattern,
                'data': ts_data.to_dict(orient='records')
            }
        
        # Create test case
        test_case = {
            'test_case_name': test_case_name,
            'metrics': metrics,
            'time_series': time_series_data,
            'config': metrics_config
        }
        
        # Save to files if requested
        if save_files:
            file_paths = save_monitoring_test_case(test_case, test_case_name, self._output_dir)
            test_case['file_paths'] = file_paths
        
        return test_case
    
    def generate_anomaly_detection_test_case(
        self,
        metrics_config: Dict = None,
        anomaly_config: Dict = None,
        test_case_name: str = None,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for anomaly detection testing.
        
        Args:
            metrics_config: Configuration for metrics generation
            anomaly_config: Configuration for anomaly injection
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with metrics, time series, and anomalies
        """
        if metrics_config is None:
            metrics_config = {}
            
        if anomaly_config is None:
            anomaly_config = {}
            
        if test_case_name is None:
            test_case_name = f"anomaly_detection_test_{generate_unique_id()}"
            
        # Generate base metrics test case
        base_case = self.generate_metrics_test_case(
            metrics_config=metrics_config,
            test_case_name=test_case_name,
            save_files=False
        )
        
        # Extract anomaly configuration parameters
        anomaly_types = anomaly_config.get('anomaly_types', ANOMALY_TYPES)
        num_anomalies = anomaly_config.get('num_anomalies', DEFAULT_NUM_ANOMALIES)
        anomaly_magnitude = anomaly_config.get('anomaly_magnitude', 3.0)
        
        # Inject anomalies into time series data
        anomalies = []
        time_series_with_anomalies = {}
        
        for metric_id, ts_info in base_case['time_series'].items():
            metric = ts_info['metric']
            pattern = ts_info['pattern']
            
            # Convert to DataFrame if it's not already
            if isinstance(ts_info['data'], list):
                orig_df = pd.DataFrame(ts_info['data'])
            else:
                orig_df = ts_info['data']
            
            # Select anomaly type for this metric
            anomaly_type = random.choice(anomaly_types)
            
            # Inject anomalies
            df_with_anomalies, anomaly_indices = inject_anomalies(
                time_series=orig_df,
                anomaly_type=anomaly_type,
                num_anomalies=num_anomalies,
                anomaly_magnitude=anomaly_magnitude
            )
            
            # Record the anomalies
            for idx in anomaly_indices:
                if idx < len(df_with_anomalies):
                    anomaly = generate_test_anomaly_record(
                        metric_name=metric['name'],
                        anomaly_type=anomaly_type,
                        anomaly_score=random.uniform(0.7, 0.99),
                        value=df_with_anomalies.iloc[idx]['value'],
                        expected_value=orig_df.iloc[idx]['value'] if idx < len(orig_df) else 0,
                        timestamp=df_with_anomalies.iloc[idx]['timestamp'],
                        context={
                            'metric_id': metric_id,
                            'pattern': pattern,
                            'index': idx
                        }
                    )
                    anomalies.append(anomaly)
            
            # Store time series with anomalies
            time_series_with_anomalies[metric_id] = {
                'metric': metric,
                'pattern': pattern,
                'anomaly_type': anomaly_type,
                'original_data': orig_df.to_dict(orient='records'),
                'data_with_anomalies': df_with_anomalies.to_dict(orient='records'),
                'anomaly_indices': anomaly_indices
            }
        
        # Create test case
        test_case = {
            'test_case_name': test_case_name,
            'metrics': base_case['metrics'],
            'time_series': time_series_with_anomalies,
            'anomalies': anomalies,
            'config': {
                'metrics_config': metrics_config,
                'anomaly_config': anomaly_config
            }
        }
        
        # Save to files if requested
        if save_files:
            file_paths = save_monitoring_test_case(test_case, test_case_name, self._output_dir)
            test_case['file_paths'] = file_paths
        
        return test_case
    
    def generate_alert_generation_test_case(
        self,
        metrics_config: Dict = None,
        anomaly_config: Dict = None,
        alert_config: Dict = None,
        test_case_name: str = None,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for alert generation testing.
        
        Args:
            metrics_config: Configuration for metrics generation
            anomaly_config: Configuration for anomaly injection
            alert_config: Configuration for alert generation
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with metrics, anomalies, and alerts
        """
        if metrics_config is None:
            metrics_config = {}
            
        if anomaly_config is None:
            anomaly_config = {}
            
        if alert_config is None:
            alert_config = {}
            
        if test_case_name is None:
            test_case_name = f"alert_generation_test_{generate_unique_id()}"
            
        # Generate anomaly detection test case
        anomaly_case = self.generate_anomaly_detection_test_case(
            metrics_config=metrics_config,
            anomaly_config=anomaly_config,
            test_case_name=test_case_name,
            save_files=False
        )
        
        # Extract alert configuration parameters
        alert_types = alert_config.get('alert_types', ALERT_TYPES)
        num_alerts = alert_config.get('num_alerts', DEFAULT_NUM_ALERTS)
        include_notifications = alert_config.get('include_notifications', True)
        
        # Generate alerts based on anomalies
        alerts = []
        notifications = []
        
        # Create anomaly-based alerts
        for anomaly in anomaly_case['anomalies']:
            alert = generate_test_alert(
                alert_type='anomaly',
                description=f"Anomaly detected in metric {anomaly['metric_name']}",
                severity=anomaly['severity'],
                context={
                    'anomaly_id': anomaly['anomaly_id'],
                    'anomaly_type': anomaly['anomaly_type'],
                    'anomaly_score': anomaly['anomaly_score'],
                    'metric_name': anomaly['metric_name']
                },
                status='ACTIVE',
                component='monitoring'
            )
            alerts.append(alert)
            
            # Generate notification for this alert
            if include_notifications:
                notification = generate_test_notification(
                    alert_id=alert['alert_id'],
                    message={
                        'title': f"Anomaly Alert: {anomaly['metric_name']}",
                        'body': f"Anomaly detected in metric {anomaly['metric_name']} with score {anomaly['anomaly_score']}",
                        'details_url': f"https://example.com/alerts/{alert['alert_id']}"
                    },
                    channels=['TEAMS', 'EMAIL']
                )
                notifications.append(notification)
        
        # Generate additional alerts of various types
        additional_alerts_count = max(0, num_alerts - len(alerts))
        for _ in range(additional_alerts_count):
            alert_type = random.choice(alert_types)
            alert = self._alert_generators.get(alert_type, self._alert_generators['system'])()
            alerts.append(alert)
            
            # Generate notification for this alert
            if include_notifications:
                notification = generate_test_notification(
                    alert_id=alert['alert_id'],
                    message={
                        'title': f"{alert_type.capitalize()} Alert",
                        'body': alert['description'],
                        'details_url': f"https://example.com/alerts/{alert['alert_id']}"
                    },
                    channels=['TEAMS', 'EMAIL']
                )
                notifications.append(notification)
        
        # Create test case
        test_case = {
            'test_case_name': test_case_name,
            'metrics': anomaly_case['metrics'],
            'time_series': anomaly_case['time_series'],
            'anomalies': anomaly_case['anomalies'],
            'alerts': alerts,
            'notifications': notifications if include_notifications else [],
            'config': {
                'metrics_config': metrics_config,
                'anomaly_config': anomaly_config,
                'alert_config': alert_config
            }
        }
        
        # Save to files if requested
        if save_files:
            file_paths = save_monitoring_test_case(test_case, test_case_name, self._output_dir)
            test_case['file_paths'] = file_paths
        
        return test_case
    
    def generate_notification_test_case(
        self,
        alert_config: Dict = None,
        notification_config: Dict = None,
        test_case_name: str = None,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for notification system testing.
        
        Args:
            alert_config: Configuration for alert generation
            notification_config: Configuration for notification generation
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with alerts and notifications
        """
        if alert_config is None:
            alert_config = {}
            
        if notification_config is None:
            notification_config = {}
            
        if test_case_name is None:
            test_case_name = f"notification_test_{generate_unique_id()}"
            
        # Extract alert and notification configuration parameters
        alert_types = alert_config.get('alert_types', ALERT_TYPES)
        num_alerts = alert_config.get('num_alerts', DEFAULT_NUM_ALERTS)
        notification_channels = notification_config.get('channels', ['TEAMS', 'EMAIL'])
        include_failures = notification_config.get('include_failures', True)
        
        # Generate alerts
        alerts = []
        for _ in range(num_alerts):
            alert_type = random.choice(alert_types)
            alert = self._alert_generators.get(alert_type, self._alert_generators['system'])()
            alerts.append(alert)
        
        # Generate notifications with various delivery statuses
        notifications = []
        for alert in alerts:
            # Determine channels for this notification
            channels = notification_channels.copy()
            
            # Create delivery status - mix of successes and failures
            delivery_status = {}
            for channel in channels:
                if include_failures and random.random() < 0.2:  # 20% chance of failure
                    status = 'FAILED'
                    reason = random.choice(['TIMEOUT', 'CONNECTION_ERROR', 'UNAUTHORIZED', 'SERVICE_UNAVAILABLE'])
                else:
                    status = 'DELIVERED'
                    reason = None
                    
                delivery_status[channel] = {
                    'status': status,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'reason': reason
                }
            
            notification = generate_test_notification(
                alert_id=alert['alert_id'],
                message={
                    'title': f"{alert['alert_type'].capitalize()} Alert",
                    'body': alert['description'],
                    'details_url': f"https://example.com/alerts/{alert['alert_id']}"
                },
                channels=channels,
                delivery_status=delivery_status
            )
            notifications.append(notification)
        
        # Create test case
        test_case = {
            'test_case_name': test_case_name,
            'alerts': alerts,
            'notifications': notifications,
            'config': {
                'alert_config': alert_config,
                'notification_config': notification_config
            }
        }
        
        # Save to files if requested
        if save_files:
            file_paths = save_monitoring_test_case(test_case, test_case_name, self._output_dir)
            test_case['file_paths'] = file_paths
        
        return test_case
    
    def generate_comprehensive_monitoring_test_suite(
        self,
        suite_config: Dict = None,
        suite_name: str = None,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a comprehensive test suite with multiple monitoring test cases.
        
        Args:
            suite_config: Configuration for the test suite
            suite_name: Name for the test suite
            save_files: Whether to save the test cases to files
            
        Returns:
            Complete test suite with multiple test cases
        """
        if suite_config is None:
            suite_config = {}
            
        if suite_name is None:
            suite_name = f"monitoring_test_suite_{generate_unique_id()}"
            
        # Create output directory for test suite
        suite_dir = os.path.join(self._output_dir, suite_name)
        os.makedirs(suite_dir, exist_ok=True)
        
        suite_results = {
            'suite_name': suite_name,
            'test_cases': {},
            'manifest': {
                'metrics_test_cases': [],
                'anomaly_detection_test_cases': [],
                'alert_generation_test_cases': [],
                'notification_test_cases': []
            }
        }
        
        # Generate metrics test cases
        if 'metrics_test_cases' in suite_config:
            for tc_config in suite_config['metrics_test_cases']:
                tc_name = tc_config.get('name', f"metrics_test_{generate_unique_id()}")
                
                test_case = self.generate_metrics_test_case(
                    metrics_config=tc_config.get('config', {}),
                    test_case_name=tc_name,
                    save_files=save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['metrics_test_cases'].append(tc_name)
        
        # Generate anomaly detection test cases
        if 'anomaly_detection_test_cases' in suite_config:
            for tc_config in suite_config['anomaly_detection_test_cases']:
                tc_name = tc_config.get('name', f"anomaly_test_{generate_unique_id()}")
                
                test_case = self.generate_anomaly_detection_test_case(
                    metrics_config=tc_config.get('metrics_config', {}),
                    anomaly_config=tc_config.get('anomaly_config', {}),
                    test_case_name=tc_name,
                    save_files=save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['anomaly_detection_test_cases'].append(tc_name)
        
        # Generate alert generation test cases
        if 'alert_generation_test_cases' in suite_config:
            for tc_config in suite_config['alert_generation_test_cases']:
                tc_name = tc_config.get('name', f"alert_test_{generate_unique_id()}")
                
                test_case = self.generate_alert_generation_test_case(
                    metrics_config=tc_config.get('metrics_config', {}),
                    anomaly_config=tc_config.get('anomaly_config', {}),
                    alert_config=tc_config.get('alert_config', {}),
                    test_case_name=tc_name,
                    save_files=save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['alert_generation_test_cases'].append(tc_name)
        
        # Generate notification test cases
        if 'notification_test_cases' in suite_config:
            for tc_config in suite_config['notification_test_cases']:
                tc_name = tc_config.get('name', f"notification_test_{generate_unique_id()}")
                
                test_case = self.generate_notification_test_case(
                    alert_config=tc_config.get('alert_config', {}),
                    notification_config=tc_config.get('notification_config', {}),
                    test_case_name=tc_name,
                    save_files=save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['notification_test_cases'].append(tc_name)
        
        # Generate test suite manifest
        if save_files:
            manifest_path = os.path.join(suite_dir, 'manifest.json')
            with open(manifest_path, 'w') as f:
                json.dump(suite_results['manifest'], f, indent=2)
            
            suite_results['manifest_path'] = manifest_path
        
        return suite_results
        
    def save_monitoring_test_case(self, test_case: Dict, test_case_name: str) -> Dict:
        """
        Saves a monitoring test case to files.
        
        Args:
            test_case: Test case to save
            test_case_name: Name for the test case
            
        Returns:
            Updated test case with file paths
        """
        return save_monitoring_test_case(test_case, test_case_name, self._output_dir)
    
    def load_monitoring_test_case(self, test_case_name: str) -> Dict:
        """
        Loads a previously saved monitoring test case.
        
        Args:
            test_case_name: Name of the test case
            
        Returns:
            Loaded test case
        """
        return load_monitoring_test_case(test_case_name, self._output_dir)


class MonitoringTestCase:
    """
    Class representing a test case for monitoring and alerting testing.
    """
    
    def __init__(
        self,
        metrics: List = None,
        time_series: pd.DataFrame = None,
        anomalies: List = None,
        alerts: List = None,
        notifications: List = None,
        expected_results: Dict = None,
        metadata: Dict = None
    ):
        """
        Initialize a MonitoringTestCase.
        
        Args:
            metrics: List of test metrics
            time_series: DataFrame with time series data
            anomalies: List of anomaly records
            alerts: List of alert records
            notifications: List of notification records
            expected_results: Expected results for validation
            metadata: Additional metadata about the test case
        """
        self.metrics = metrics if metrics is not None else []
        self.time_series = time_series
        self.anomalies = anomalies if anomalies is not None else []
        self.alerts = alerts if alerts is not None else []
        self.notifications = notifications if notifications is not None else []
        self.expected_results = expected_results if expected_results is not None else {}
        self.metadata = metadata if metadata is not None else {}
        self.file_paths = {}
    
    def save(
        self,
        test_case_name: str,
        output_dir: str = MONITORING_TEST_CASE_DIR
    ) -> Dict:
        """
        Save the test case to files.
        
        Args:
            test_case_name: Name for the test case
            output_dir: Directory to save the test case
            
        Returns:
            Paths to saved files
        """
        test_case_dict = self.to_dict()
        file_paths = save_monitoring_test_case(test_case_dict, test_case_name, output_dir)
        self.file_paths = file_paths
        return file_paths
    
    def to_dict(self) -> Dict:
        """
        Convert the test case to a dictionary representation.
        
        Returns:
            Dictionary representation of the test case
        """
        test_case_dict = {
            'metrics': self.metrics,
            'anomalies': self.anomalies,
            'alerts': self.alerts,
            'notifications': self.notifications,
            'expected_results': self.expected_results,
            'metadata': self.metadata,
            'file_paths': self.file_paths
        }
        
        # Handle time_series DataFrame
        if self.time_series is not None:
            if isinstance(self.time_series, pd.DataFrame):
                test_case_dict['time_series'] = self.time_series.to_dict(orient='records')
            else:
                test_case_dict['time_series'] = self.time_series
        
        return test_case_dict
    
    @classmethod
    def from_dict(cls, test_case_dict: Dict) -> 'MonitoringTestCase':
        """
        Create a MonitoringTestCase from a dictionary.
        
        Args:
            test_case_dict: Dictionary representation of a test case
            
        Returns:
            MonitoringTestCase instance
        """
        # Convert time_series back to DataFrame if present
        time_series = None
        if 'time_series' in test_case_dict:
            if isinstance(test_case_dict['time_series'], list):
                time_series = pd.DataFrame(test_case_dict['time_series'])
            else:
                time_series = test_case_dict['time_series']
        
        # Create instance
        instance = cls(
            metrics=test_case_dict.get('metrics', []),
            time_series=time_series,
            anomalies=test_case_dict.get('anomalies', []),
            alerts=test_case_dict.get('alerts', []),
            notifications=test_case_dict.get('notifications', []),
            expected_results=test_case_dict.get('expected_results', {}),
            metadata=test_case_dict.get('metadata', {})
        )
        
        # Set file_paths if present
        if 'file_paths' in test_case_dict:
            instance.file_paths = test_case_dict['file_paths']
        
        return instance
    
    @classmethod
    def load(
        cls,
        test_case_name: str,
        input_dir: str = MONITORING_TEST_CASE_DIR
    ) -> 'MonitoringTestCase':
        """
        Load a test case from files.
        
        Args:
            test_case_name: Name of the test case
            input_dir: Directory containing the test case
            
        Returns:
            MonitoringTestCase instance
        """
        test_case_dict = load_monitoring_test_case(test_case_name, input_dir)
        return cls.from_dict(test_case_dict)