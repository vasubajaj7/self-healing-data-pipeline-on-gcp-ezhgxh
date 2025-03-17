"""
Entry point for the monitoring analyzers module that exposes key classes and functions for metric processing,
anomaly detection, alert correlation, and forecasting.

This module provides a collection of components for analyzing monitoring data in the self-healing data pipeline:
- Metric processing: Tools for transforming, normalizing, and preparing metrics
- Anomaly detection: Statistical and ML-based methods for identifying unusual patterns
- Alert correlation: Algorithms for grouping related alerts and reducing noise
- Forecasting: Techniques for predicting future metric values and potential issues

These components form the core analytical engine behind the self-healing capabilities of the pipeline.
"""

# Import and re-export metric processing components
from .metric_processor import (
    MetricProcessor,
    normalize_metric_name,
    calculate_rate,
    resample_time_series,
    apply_rolling_window
)

# Import and re-export anomaly detection components
from .anomaly_detector import (
    AnomalyDetector,
    StatisticalAnomalyDetector,
    MLAnomalyDetector,
    AnomalyRecord,
    calculate_z_score,
    detect_outliers_iqr
)

# Import and re-export alert correlation components
from .alert_correlator import (
    AlertCorrelator,
    AlertGroup,
    calculate_similarity_score
)

# Import and re-export forecasting components
from .forecaster import (
    MetricForecaster,
    StatisticalForecaster,
    MLForecaster,
    ProphetForecaster,
    ForecastResult
)

# Define __all__ to specify exactly what is exported
__all__ = [
    # Metric processing
    'MetricProcessor',
    'normalize_metric_name',
    'calculate_rate',
    'resample_time_series',
    'apply_rolling_window',
    
    # Anomaly detection
    'AnomalyDetector',
    'StatisticalAnomalyDetector',
    'MLAnomalyDetector',
    'AnomalyRecord',
    'calculate_z_score',
    'detect_outliers_iqr',
    
    # Alert correlation
    'AlertCorrelator',
    'AlertGroup',
    'calculate_similarity_score',
    
    # Forecasting
    'MetricForecaster',
    'StatisticalForecaster',
    'MLForecaster',
    'ProphetForecaster',
    'ForecastResult'
]