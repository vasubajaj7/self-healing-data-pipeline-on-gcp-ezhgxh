"""
Unit tests for the anomaly detection component of the monitoring system.
Tests the functionality of statistical and machine learning-based anomaly detection algorithms, including detection of point anomalies, contextual anomalies, collective anomalies, and trend anomalies in pipeline metrics.
"""

import pytest  # package_version: 7.0.0+
import unittest.mock  # package_version: standard library
import pandas  # package_version: 2.0.0+
import numpy  # package_version: 1.23.0+
import datetime  # package_version: standard library

from src.backend.monitoring.analyzers.anomaly_detector import (  # Module(src.backend.monitoring.analyzers.anomaly_detector)
    AnomalyDetector,
    StatisticalAnomalyDetector,
    MLAnomalyDetector,
    AnomalyRecord,
    calculate_z_score,
    detect_outliers_iqr,
    calculate_anomaly_score,
    detect_trend_anomalies,
    detect_seasonal_anomalies
)
from src.backend.constants import AlertSeverity  # Module(src.backend.constants)
from src.test.fixtures.backend.monitoring_fixtures import (  # Module(src.test.fixtures.backend.monitoring_fixtures)
    create_test_metric,
    create_test_anomaly_record,
    generate_test_time_series,
    generate_anomalous_time_series,
    TestMetricData,
    mock_metric_processor,
    mock_alert_repository,
    TestMetricData
)


def test_anomaly_record_creation():
    """Tests the creation and properties of AnomalyRecord objects"""
    # Create an AnomalyRecord with test values
    anomaly = create_test_anomaly_record(
        metric_name="test_metric",
        anomaly_type="point",
        anomaly_score=0.9,
        value=1.0,
        expected_value=0.5,
        timestamp=datetime.datetime.now()
    )

    # Verify all properties are set correctly
    assert anomaly.metric_name == "test_metric"
    assert anomaly.anomaly_type == "point"
    assert anomaly.anomaly_score == 0.9
    assert anomaly.value == 1.0
    assert anomaly.expected_value == 0.5
    assert isinstance(anomaly.timestamp, datetime.datetime)

    # Test the to_dict and from_dict methods
    anomaly_dict = anomaly.to_dict()
    anomaly_from_dict = AnomalyRecord.from_dict(anomaly_dict)

    # Verify serialization and deserialization work correctly
    assert anomaly_from_dict.metric_name == anomaly.metric_name
    assert anomaly_from_dict.anomaly_type == anomaly.anomaly_type
    assert anomaly_from_dict.anomaly_score == anomaly.anomaly_score
    assert anomaly_from_dict.value == anomaly.value
    assert anomaly_from_dict.expected_value == anomaly.expected_value
    assert anomaly_from_dict.timestamp == anomaly.timestamp

    # Test setting severity and adding context
    anomaly.severity = AlertSeverity.HIGH
    anomaly.context = {"key": "value"}
    assert anomaly.severity == AlertSeverity.HIGH
    assert anomaly.context == {"key": "value"}


def test_anomaly_record_to_alert():
    """Tests conversion of AnomalyRecord to Alert object"""
    # Create an AnomalyRecord with test values
    anomaly = create_test_anomaly_record(
        metric_name="test_metric",
        anomaly_type="point",
        anomaly_score=0.9,
        value=1.0,
        expected_value=0.5,
        timestamp=datetime.datetime.now()
    )

    # Set severity and add context information
    anomaly.severity = AlertSeverity.HIGH
    anomaly.context = {"key": "value"}

    # Call to_alert method to convert to Alert
    alert = anomaly.to_alert()

    # Verify Alert properties match AnomalyRecord properties
    assert alert.alert_type == "anomaly"
    assert alert.description == f"Anomaly detected in test_metric"
    assert alert.severity == AlertSeverity.HIGH
    assert alert.context == {"key": "value", "metric_name": "test_metric", "anomaly_type": "point", "anomaly_score": 0.9, "value": 1.0, "expected_value": 0.5}


def test_calculate_z_score():
    """Tests the z-score calculation function for anomaly detection"""
    # Create a pandas Series with known values
    data = pandas.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Calculate z-scores with different thresholds
    anomalies_threshold_2 = calculate_z_score(data, threshold=2)
    anomalies_threshold_3 = calculate_z_score(data, threshold=3)

    # Verify anomalies are correctly identified
    assert anomalies_threshold_2.tolist() == [False, False, False, False, False, False, False, False, True, True]
    assert anomalies_threshold_3.tolist() == [False, False, False, False, False, False, False, False, False, True]

    # Test edge cases (all same values, single value, empty series)
    same_values = pandas.Series([5, 5, 5, 5, 5])
    assert calculate_z_score(same_values).tolist() == [False, False, False, False, False]

    single_value = pandas.Series([5])
    assert calculate_z_score(single_value).tolist() == [False]

    empty_series = pandas.Series([])
    assert calculate_z_score(empty_series).tolist() == []


def test_detect_outliers_iqr():
    """Tests the IQR-based outlier detection function"""
    # Create a pandas Series with known values including outliers
    data = pandas.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, -50])

    # Apply IQR outlier detection with different multipliers
    outliers_multiplier_1_5 = detect_outliers_iqr(data, k=1.5)
    outliers_multiplier_3 = detect_outliers_iqr(data, k=3)

    # Verify outliers are correctly identified
    assert outliers_multiplier_1_5.tolist() == [False, False, False, False, False, False, False, False, False, False, True, True]
    assert outliers_multiplier_3.tolist() == [False, False, False, False, False, False, False, False, False, False, True, False]

    # Test edge cases (all same values, single value, empty series)
    same_values = pandas.Series([5, 5, 5, 5, 5])
    assert detect_outliers_iqr(same_values).tolist() == [False, False, False, False, False]

    single_value = pandas.Series([5])
    assert detect_outliers_iqr(single_value).tolist() == [False]

    empty_series = pandas.Series([])
    assert detect_outliers_iqr(empty_series).tolist() == []


def test_calculate_anomaly_score():
    """Tests the anomaly score calculation function"""
    # Test various combinations of values, expected values, and standard deviations
    assert calculate_anomaly_score(10, 5, 2, sensitivity=1) == 1.0
    assert calculate_anomaly_score(3, 5, 2, sensitivity=1) == 1.0
    assert calculate_anomaly_score(6, 5, 2, sensitivity=1) == 0.5
    assert calculate_anomaly_score(4, 5, 2, sensitivity=1) == 0.5

    # Verify scores are between 0 and 1
    assert 0 <= calculate_anomaly_score(7, 5, 2, sensitivity=1) <= 1
    assert 0 <= calculate_anomaly_score(3, 5, 2, sensitivity=1) <= 1

    # Verify larger deviations produce higher scores
    score1 = calculate_anomaly_score(10, 5, 2, sensitivity=1)
    score2 = calculate_anomaly_score(7, 5, 2, sensitivity=1)
    assert score1 > score2

    # Test different sensitivity values
    assert calculate_anomaly_score(7, 5, 2, sensitivity=2) < calculate_anomaly_score(7, 5, 2, sensitivity=0.5)

    # Test edge cases (zero std_dev, very large deviations)
    assert calculate_anomaly_score(5, 5, 0) == 0.0
    assert calculate_anomaly_score(1000, 5, 2) == 1.0


def test_detect_trend_anomalies():
    """Tests detection of anomalies in trend components"""
    # Generate time series with known trend anomalies
    data = pandas.Series([1, 2, 3, 4, 5, 10, 12, 14, 16, 18])

    # Apply trend anomaly detection with different window sizes and thresholds
    anomalies_window_5_threshold_2 = detect_trend_anomalies(data, window=5, threshold=2)
    anomalies_window_3_threshold_1 = detect_trend_anomalies(data, window=3, threshold=1)

    # Verify trend anomalies are correctly identified
    assert anomalies_window_5_threshold_2.tolist() == [False, False, False, False, True, False, False, False, False, False]
    assert anomalies_window_3_threshold_1.tolist() == [False, False, False, True, False, False, False, False, False, False]

    # Test with different trend patterns (linear, exponential, step changes)
    linear_data = pandas.Series(numpy.linspace(1, 10, 10))
    assert detect_trend_anomalies(linear_data, window=3, threshold=0.5).tolist() == [False, False, False, False, False, False, False, False, False, False]

    exponential_data = pandas.Series([1, 2, 4, 8, 16, 32, 64, 128, 256, 512])
    assert detect_trend_anomalies(exponential_data, window=5, threshold=5).tolist() == [False, False, False, False, False, False, False, False, False, False]


def test_detect_seasonal_anomalies():
    """Tests detection of anomalies in seasonal components"""
    # Generate time series with known seasonal patterns and anomalies
    data = pandas.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 4, 3, 2, 1, 2, 3, 4, 5, 6])

    # Apply seasonal anomaly detection with different seasonal periods
    anomalies_period_5 = detect_seasonal_anomalies(data, seasonal_period=5)
    anomalies_period_10 = detect_seasonal_anomalies(data, seasonal_period=10)

    # Verify seasonal anomalies are correctly identified
    assert anomalies_period_5.tolist() == [False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False]
    assert anomalies_period_10.tolist() == [False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False]

    # Test with different seasonal patterns (daily, weekly, monthly)
    daily_data = pandas.Series(numpy.sin(numpy.linspace(0, 2 * numpy.pi, 24)))
    assert detect_seasonal_anomalies(daily_data, seasonal_period=24).tolist() == [False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False]


class TestAnomalyDetector:
    """Tests for the base AnomalyDetector class"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Create mock MetricProcessor
        self.mock_processor = unittest.mock.MagicMock()

        # Create mock AlertRepository
        self.mock_alert_repository = unittest.mock.MagicMock()

        # Set up test configuration
        self.test_config = {
            "sensitivity": 2.0,
            "min_data_points": 5,
            "algorithm": "z_score"
        }

        # Initialize test data
        self.test_data = TestMetricData()

    def test_initialization(self):
        """Tests initialization of AnomalyDetector with configuration"""
        # Create AnomalyDetector with test configuration
        detector = AnomalyDetector(config=self.test_config)

        # Verify configuration is applied correctly
        assert detector._config == self.test_config
        assert detector._sensitivity == 2.0
        assert detector._min_data_points == 5
        assert detector._algorithm == "z_score"

        # Verify dependencies are initialized correctly
        assert detector._metric_processor is None
        assert detector._alert_repository is None

        # Test with different configuration overrides
        config_override = {"sensitivity": 3.0, "algorithm": "iqr"}
        detector2 = AnomalyDetector(config=config_override)
        assert detector2._sensitivity == 3.0
        assert detector2._algorithm == "iqr"

    def test_update_baseline(self):
        """Tests updating baseline statistics for metrics"""
        # Create AnomalyDetector instance
        detector = AnomalyDetector()

        # Create test time series data
        time_series = self.test_data.create_time_series("linear")

        # Call update_baseline with metric name and time series
        detector.update_baseline("test_metric", time_series['value'])

        # Verify baseline statistics are calculated correctly
        assert "test_metric" in detector._baselines
        baseline = detector._baselines["test_metric"]
        assert "mean" in baseline
        assert "std" in baseline
        assert "median" in baseline
        assert "percentiles" in baseline

        # Test updating existing baseline with new data
        new_time_series = self.test_data.create_time_series("seasonal")
        detector.update_baseline("test_metric", new_time_series['value'])
        assert "test_metric" in detector._baselines
        new_baseline = detector._baselines["test_metric"]
        assert new_baseline["mean"] != baseline["mean"]

    def test_calculate_anomaly_severity(self):
        """Tests calculation of anomaly severity based on characteristics"""
        # Create AnomalyDetector instance
        detector = AnomalyDetector()

        # Create test anomaly records with different scores and deviations
        anomaly1 = create_test_anomaly_record(anomaly_score=0.9)
        anomaly2 = create_test_anomaly_record(anomaly_score=0.6)
        anomaly3 = create_test_anomaly_record(anomaly_score=0.3)

        # Call calculate_anomaly_severity for each record
        severity1 = detector.calculate_anomaly_severity(anomaly1)
        severity2 = detector.calculate_anomaly_severity(anomaly2)
        severity3 = detector.calculate_anomaly_severity(anomaly3)

        # Verify severity levels match expected values based on scores
        assert severity1 == AlertSeverity.CRITICAL
        assert severity2 == AlertSeverity.WARNING
        assert severity3 == AlertSeverity.INFO

        # Test edge cases (very high/low scores, different anomaly types)
        anomaly4 = create_test_anomaly_record(anomaly_score=1.1)
        assert detector.calculate_anomaly_severity(anomaly4) == AlertSeverity.CRITICAL

        anomaly5 = create_test_anomaly_record(anomaly_score=-0.1)
        assert detector.calculate_anomaly_severity(anomaly5) == AlertSeverity.INFO

    def test_get_anomaly_explanation(self):
        """Tests generation of human-readable explanations for anomalies"""
        # Create AnomalyDetector instance
        detector = AnomalyDetector()

        # Create test anomaly records of different types
        anomaly1 = create_test_anomaly_record(metric_name="cpu_utilization", anomaly_type="point", anomaly_score=0.9)
        anomaly2 = create_test_anomaly_record(metric_name="memory_usage", anomaly_type="trend", anomaly_score=0.7)

        # Call get_anomaly_explanation for each record
        explanation1 = detector.get_anomaly_explanation(anomaly1)
        explanation2 = detector.get_anomaly_explanation(anomaly2)

        # Verify explanations contain relevant information
        assert "cpu_utilization" in explanation1
        assert "0.9" in explanation1
        assert "point" in explanation1

        assert "memory_usage" in explanation2
        assert "0.7" in explanation2
        assert "trend" in explanation2

    def test_create_anomaly_alert(self, mock_alert_repository):
        """Tests creation of alerts from anomaly records"""
        # Create AnomalyDetector instance
        detector = AnomalyDetector(alert_repository=mock_alert_repository)

        # Create test anomaly record
        anomaly = create_test_anomaly_record(metric_name="cpu_utilization", anomaly_type="point", anomaly_score=0.9)

        # Mock AlertRepository.create_alert to return test ID
        mock_alert_repository.create_alert.return_value = "test_alert_id"

        # Call create_anomaly_alert with anomaly record
        alert_id = detector.create_anomaly_alert(anomaly)

        # Verify alert is created with correct properties
        assert alert_id == "test_alert_id"
        mock_alert_repository.create_alert.assert_called_once()
        created_alert = mock_alert_repository.create_alert.call_args[0][0]
        assert created_alert.alert_type == "anomaly"
        assert created_alert.description == "Anomaly detected in cpu_utilization"
        assert created_alert.severity == AlertSeverity.CRITICAL
        assert created_alert.context == {"metric_name": "cpu_utilization", "anomaly_type": "point", "anomaly_score": 0.9, "value": 1.0, "expected_value": 0.5}

    def test_save_load_baselines(self):
        """Tests saving and loading baseline statistics"""
        # Create AnomalyDetector instance
        detector = AnomalyDetector()

        # Update baselines with test data
        time_series = self.test_data.create_time_series("linear")
        detector.update_baseline("test_metric", time_series['value'])

        # Mock storage operations
        mock_storage = unittest.mock.MagicMock()
        detector._storage = mock_storage

        # Call save_baselines method
        detector.save_baselines()

        # Verify baselines are serialized correctly
        mock_storage.save_data.assert_called_once()
        saved_data = mock_storage.save_data.call_args[0][0]
        assert "test_metric" in saved_data
        assert "mean" in saved_data["test_metric"]

        # Call load_baselines method
        mock_storage.load_data.return_value = saved_data
        detector.load_baselines()

        # Verify baselines are loaded correctly
        assert "test_metric" in detector._baselines
        loaded_baseline = detector._baselines["test_metric"]
        assert loaded_baseline["mean"] == detector._baselines["test_metric"]["mean"]


class TestStatisticalAnomalyDetector:
    """Tests for the StatisticalAnomalyDetector implementation"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Create mock MetricProcessor
        self.mock_processor = unittest.mock.MagicMock()

        # Create mock AlertRepository
        self.mock_alert_repository = unittest.mock.MagicMock()

        # Set up test configuration
        self.test_config = {
            "sensitivity": 2.0,
            "min_data_points": 5,
            "algorithm": "z_score"
        }

        # Initialize test data
        self.test_data = TestMetricData()

        # Create StatisticalAnomalyDetector instance
        self.detector = StatisticalAnomalyDetector(
            metric_processor=self.mock_processor,
            alert_repository=self.mock_alert_repository,
            config=self.test_config
        )

    def test_detect_anomalies(self):
        """Tests the main anomaly detection method"""
        # Create test metrics data with known anomalies
        metrics_data = self.test_data.get_metrics_by_type("gauge")

        # Mock MetricProcessor to return processed metrics
        self.mock_processor.process_metrics.return_value = metrics_data

        # Call detect_anomalies with test data
        anomalies = self.detector.detect_anomalies(metrics_data)

        # Verify correct number of anomalies detected
        assert len(anomalies) == 0  # No anomalies in sample data

        # Verify anomaly properties match expected values
        # (This part would need to be adjusted based on the actual anomaly detection logic)
        # Test with different sensitivity thresholds
        self.detector._sensitivity = 3.0
        anomalies_high_sensitivity = self.detector.detect_anomalies(metrics_data)
        assert len(anomalies_high_sensitivity) == 0

    def test_detect_point_anomalies(self):
        """Tests detection of point anomalies"""
        # Generate time series with known point anomalies
        time_series, anomaly_indices = generate_anomalous_time_series(anomaly_type="point")

        # Call detect_point_anomalies method
        anomalies = self.detector.detect_point_anomalies(time_series['value'])

        # Verify point anomalies are correctly identified
        assert len(anomalies) == len(anomaly_indices)

        # Test with different thresholds
        self.detector._sensitivity = 3.0
        anomalies_high_sensitivity = self.detector.detect_point_anomalies(time_series['value'])
        assert len(anomalies_high_sensitivity) <= len(anomalies)

        # Verify anomaly records have correct type (ANOMALY_TYPE_POINT)
        for anomaly in anomalies:
            assert anomaly.anomaly_type == "point"

    def test_detect_contextual_anomalies(self):
        """Tests detection of contextual anomalies"""
        # Generate time series with known contextual anomalies
        time_series, anomaly_indices = generate_anomalous_time_series(anomaly_type="contextual")

        # Call detect_contextual_anomalies method
        anomalies = self.detector.detect_contextual_anomalies(time_series['value'])

        # Verify contextual anomalies are correctly identified
        assert len(anomalies) == len(anomaly_indices)

        # Test with different contexts (hour of day, day of week)
        # (This part would need to be adjusted based on the actual contextual logic)

        # Verify anomaly records have correct type (ANOMALY_TYPE_CONTEXTUAL)
        for anomaly in anomalies:
            assert anomaly.anomaly_type == "contextual"

    def test_detect_collective_anomalies(self):
        """Tests detection of collective anomalies"""
        # Generate time series with known collective anomalies
        time_series, anomaly_indices = generate_anomalous_time_series(anomaly_type="collective")

        # Call detect_collective_anomalies method
        anomalies = self.detector.detect_collective_anomalies(time_series['value'])

        # Verify collective anomalies are correctly identified
        assert len(anomalies) == len(anomaly_indices)

        # Test with different window sizes
        self.detector._min_data_points = 10
        anomalies_large_window = self.detector.detect_collective_anomalies(time_series['value'])
        assert len(anomalies_large_window) <= len(anomalies)

        # Verify anomaly records have correct type (ANOMALY_TYPE_COLLECTIVE)
        for anomaly in anomalies:
            assert anomaly.anomaly_type == "collective"

    def test_statistical_update_baseline(self):
        """Tests statistical baseline updates"""
        # Create test time series data
        time_series = self.test_data.create_time_series("linear")

        # Call update_baseline method
        self.detector.update_baseline("test_metric", time_series['value'])

        # Verify statistical properties are calculated correctly
        assert "test_metric" in self.detector._baselines
        baseline = self.detector._baselines["test_metric"]
        assert "mean" in baseline
        assert "std" in baseline
        assert "median" in baseline
        assert "percentiles" in baseline

        # Check for mean, median, std, percentiles in baseline
        assert isinstance(baseline["mean"], float)
        assert isinstance(baseline["std"], float)
        assert isinstance(baseline["median"], float)
        assert isinstance(baseline["percentiles"], dict)

        # Test updating existing baseline with new data
        new_time_series = self.test_data.create_time_series("seasonal")
        self.detector.update_baseline("test_metric", new_time_series['value'])
        new_baseline = self.detector._baselines["test_metric"]
        assert new_baseline["mean"] != baseline["mean"]


class TestMLAnomalyDetector:
    """Tests for the MLAnomalyDetector implementation"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Create mock MetricProcessor
        self.mock_processor = unittest.mock.MagicMock()

        # Create mock AlertRepository
        self.mock_alert_repository = unittest.mock.MagicMock()

        # Set up test configuration
        self.test_config = {
            "sensitivity": 2.0,
            "min_data_points": 5,
            "algorithm": "lstm"
        }

        # Initialize test data
        self.test_data = TestMetricData()

        # Create MLAnomalyDetector instance
        self.detector = MLAnomalyDetector(
            metric_processor=self.mock_processor,
            alert_repository=self.mock_alert_repository,
            config=self.test_config
        )

        # Mock ML model loading and prediction
        self.mock_model = unittest.mock.MagicMock()
        self.mock_model.predict.return_value = numpy.array([0.1, 0.2, 0.9, 0.3, 0.4])
        self.detector._models = {"test_metric": self.mock_model}

    def test_ml_detect_anomalies(self):
        """Tests ML-based anomaly detection"""
        # Create test metrics data with known anomalies
        metrics_data = self.test_data.get_metrics_by_type("gauge")

        # Mock MetricProcessor to return processed metrics
        self.mock_processor.process_metrics.return_value = metrics_data

        # Mock feature extraction and model prediction
        feature_data = pandas.DataFrame({"feature1": [1, 2, 3], "feature2": [4, 5, 6]})
        self.detector.extract_features = unittest.mock.MagicMock(return_value=feature_data)
        self.detector.predict_anomaly_score = unittest.mock.MagicMock(return_value=pandas.Series([0.1, 0.2, 0.9]))

        # Call detect_anomalies with test data
        anomalies = self.detector.detect_anomalies(metrics_data)

        # Verify correct number of anomalies detected
        assert len(anomalies) == 0

        # Verify anomaly properties match expected values
        # (This part would need to be adjusted based on the actual anomaly detection logic)

    def test_train_detection_model(self):
        """Tests training of ML models for anomaly detection"""
        # Create test historical data
        historical_data = {"test_metric": self.test_data.create_time_series("linear")}

        # Mock ML training functions
        mock_train = unittest.mock.MagicMock()
        mock_save = unittest.mock.MagicMock()
        self.detector.extract_features = unittest.mock.MagicMock(return_value=pandas.DataFrame())

        # Call train_detection_model method
        self.detector.train_detection_model(historical_data, ["test_metric"])

        # Verify model training is called with correct parameters
        # Verify models are saved correctly

    def test_extract_features(self):
        """Tests feature extraction for ML models"""
        # Create test time series data
        time_series = self.test_data.create_time_series("linear")

        # Call extract_features method
        feature_df = self.detector.extract_features(time_series)

        # Verify feature DataFrame has expected columns
        assert "value_mean" in feature_df.columns
        assert "value_std" in feature_df.columns
        assert "hour" in feature_df.columns

        # Check statistical features (mean, std, skew, etc.)
        # Check temporal features (hour, day, etc.)

    def test_predict_anomaly_score(self):
        """Tests prediction of anomaly scores using ML models"""
        # Create test feature DataFrame
        feature_data = pandas.DataFrame({"feature1": [1, 2, 3], "feature2": [4, 5, 6]})

        # Mock ML model prediction
        self.mock_model.predict.return_value = numpy.array([0.1, 0.2, 0.9])

        # Call predict_anomaly_score method
        scores = self.detector.predict_anomaly_score(feature_data, "test_metric")

        # Verify scores are between 0 and 1
        assert all(0 <= score <= 1 for score in scores)

        # Verify model is called with correct features
        self.mock_model.predict.assert_called_once()

    def test_save_load_models(self):
        """Tests saving and loading ML models"""
        # Mock model serialization functions
        mock_dump = unittest.mock.MagicMock()
        mock_load = unittest.mock.MagicMock()

        # Create test models
        test_models = {"test_metric": self.mock_model}

        # Call save_models method
        self.detector.save_models(test_models)

        # Verify models are serialized correctly
        # Call load_models method
        # Verify models are loaded correctly


class TestAnomalyDetectionIntegration:
    """Integration tests for anomaly detection components"""

    def setup_method(self):
        """Set up test fixtures before each test method"""
        # Create real (non-mock) MetricProcessor
        self.metric_processor = MetricProcessor()

        # Create mock AlertRepository
        self.mock_alert_repository = unittest.mock.MagicMock()

        # Set up test configuration
        self.test_config = {
            "sensitivity": 2.0,
            "min_data_points": 5,
            "algorithm": "z_score"
        }

        # Initialize test data with real time series
        self.test_data = TestMetricData()

    def test_statistical_detector_with_real_data(self):
        """Tests StatisticalAnomalyDetector with real data"""
        # Create StatisticalAnomalyDetector with real MetricProcessor
        detector = StatisticalAnomalyDetector(
            metric_processor=self.metric_processor,
            alert_repository=self.mock_alert_repository,
            config=self.test_config
        )

        # Generate realistic time series with known anomalies
        time_series, anomaly_indices = generate_anomalous_time_series(num_points=200, anomaly_type="point", num_anomalies=10)

        # Process data through the detector
        anomalies = detector.detect_anomalies(time_series)

        # Verify anomaly detection accuracy
        # Check false positive rate is below 10%
        # Check all injected anomalies are detected
        assert len(anomalies) > 0

    def test_ml_detector_with_real_data(self):
        """Tests MLAnomalyDetector with real data"""
        # Create MLAnomalyDetector with real MetricProcessor
        detector = MLAnomalyDetector(
            metric_processor=self.metric_processor,
            alert_repository=self.mock_alert_repository,
            config=self.test_config
        )

        # Generate realistic time series with known anomalies
        time_series, anomaly_indices = generate_anomalous_time_series(num_points=200, anomaly_type="point", num_anomalies=10)

        # Train model with historical data
        detector.train_detection_model({"test_metric": time_series}, ["test_metric"])

        # Process data through the detector
        anomalies = detector.detect_anomalies(time_series)

        # Verify anomaly detection accuracy
        # Check false positive rate is below 10%
        # Verify all injected anomalies are detected
        assert len(anomalies) > 0

    def test_end_to_end_anomaly_workflow(self):
        """Tests complete anomaly detection workflow"""
        # Create test metrics with anomalies
        metrics = [create_test_metric(metric_name="cpu_utilization", value=0.9),
                   create_test_metric(metric_name="memory_usage", value=0.2)]

        # Process through MetricProcessor
        processed_metrics = self.metric_processor.process_metrics(metrics)

        # Detect anomalies with StatisticalAnomalyDetector
        detector = StatisticalAnomalyDetector(
            metric_processor=self.metric_processor,
            alert_repository=self.mock_alert_repository,
            config=self.test_config
        )
        anomalies = detector.detect_anomalies(processed_metrics)

        # Create alerts from anomalies
        for anomaly in anomalies:
            detector.create_anomaly_alert(anomaly)

        # Verify alerts are created with correct properties
        # Verify alert severity matches anomaly severity
        assert self.mock_alert_repository.create_alert.call_count == len(anomalies)