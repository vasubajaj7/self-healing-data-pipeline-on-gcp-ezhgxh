"""
Custom expectations for the Great Expectations framework.

This module defines custom expectations that extend the Great Expectations framework
to provide specialized data quality validation capabilities for the self-healing
data pipeline.
"""

from typing import Dict, List, Any, Optional, Union
import pandas as pd
import numpy as np

from great_expectations.expectations.expectation import Expectation
from great_expectations.expectations.core import (
    ExpectColumnValuesToBeInSet,
    ColumnExpectation,
    TableExpectation
)
from great_expectations.expectations.registry import register_expectation
from great_expectations.core import ExpectationConfiguration

from backend.constants import ValidationRuleType, QualityDimension
from backend.utils.logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


class ExpectColumnValuesInReferenceTable(ColumnExpectation):
    """
    Custom expectation that validates column values exist in a reference table.
    
    This expectation checks if all values in a column exist in a specified
    reference table column, which is useful for enforcing referential integrity
    across datasets.
    """
    
    expectation_type = "expect_column_values_in_reference_table"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
        }

    def _validate(self, series: pd.Series, **kwargs) -> dict:
        """
        Validates that column values exist in a reference table column.
        
        Args:
            series: The pandas Series to validate
            **kwargs: Additional parameters including:
                - reference_table: The reference table name
                - reference_column: The column in the reference table to check against
                
        Returns:
            Validation result with success status and details
        """
        # Get reference table and column from kwargs
        reference_table = kwargs.get("reference_table")
        reference_column = kwargs.get("reference_column")
        
        if reference_table is None or reference_column is None:
            raise ValueError("reference_table and reference_column must be provided")
            
        # In a real implementation, we would query the reference table
        # For this example, we'll simulate this with a mock reference set
        logger.debug(f"Validating values against {reference_table}.{reference_column}")
        
        try:
            # Mock implementation - this would be replaced with actual BigQuery query
            # reference_values = fetch_reference_values(reference_table, reference_column)
            reference_values = kwargs.get("_mock_reference_values", set())
            
            # Check if each value in series exists in reference values
            series_values = set(series.dropna().unique())
            missing_values = series_values - reference_values
            
            # Calculate success percentage
            success_count = len(series_values) - len(missing_values)
            if len(series_values) > 0:
                success_percentage = success_count / len(series_values)
            else:
                success_percentage = 1.0
                
            # Get unexpected indices (positions of values not in reference set)
            unexpected_indices_list = []
            if len(missing_values) > 0:
                unexpected_values_set = set(missing_values)
                for idx, val in enumerate(series):
                    if val in unexpected_values_set:
                        unexpected_indices_list.append(idx)
            
            # Return validation result
            return {
                "success": len(missing_values) == 0,
                "result": {
                    "unexpected_count": len(missing_values),
                    "unexpected_percent": 100 * (1 - success_percentage),
                    "unexpected_values": list(missing_values)[:10],  # Limit to first 10 for brevity
                    "unexpected_index_list": unexpected_indices_list,
                }
            }
        except Exception as e:
            logger.error(f"Error validating reference values: {e}")
            return {
                "success": False,
                "result": {
                    "unexpected_count": None,
                    "unexpected_percent": None,
                    "unexpected_values": None,
                    "unexpected_index_list": None,
                    "error": str(e)
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Check that reference_table and reference_column are present
        if "reference_table" not in configuration.kwargs:
            raise ValueError("reference_table must be provided")
        
        if "reference_column" not in configuration.kwargs:
            raise ValueError("reference_column must be provided")
            
        # Additional validation could be added here
        
        return True


class ExpectColumnValuesTrendIncreasing(ColumnExpectation):
    """
    Custom expectation that validates column values follow an increasing trend.
    
    This expectation verifies that numeric values in a time series or ordered column
    follow a generally increasing trend, with an optional tolerance parameter to
    allow for minor fluctuations.
    """
    
    expectation_type = "expect_column_values_trend_increasing"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
            "tolerance": 0.1  # Default tolerance for trend direction
        }
    
    def _validate(self, series: pd.Series, **kwargs) -> dict:
        """
        Validates that values in the series follow an increasing trend.
        
        Args:
            series: The pandas Series to validate (should be numeric and ordered)
            **kwargs: Additional parameters including:
                - tolerance: Allowed tolerance for trend violations (0-1)
                
        Returns:
            Validation result with success status and details
        """
        # Get tolerance from kwargs or use default
        tolerance = kwargs.get("tolerance", 0.1)
        
        try:
            # Convert series to numeric if necessary
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Remove NaN values
            clean_series = numeric_series.dropna()
            
            if len(clean_series) <= 1:
                # Not enough data points to determine trend
                return {
                    "success": True,  # Default to success with insufficient data
                    "result": {
                        "observed_value": None,
                        "details": {
                            "message": "Insufficient data points to determine trend",
                            "data_points": len(clean_series)
                        }
                    }
                }
            
            # Calculate differences between consecutive values
            differences = clean_series.diff().dropna()
            
            # Count positive differences (increases)
            positive_diffs = sum(differences > 0)
            total_diffs = len(differences)
            
            # Calculate percentage of increasing steps
            if total_diffs > 0:
                increasing_percentage = positive_diffs / total_diffs
            else:
                increasing_percentage = 1.0
            
            # Check if increasing percentage is above (1 - tolerance)
            is_increasing = increasing_percentage >= (1 - tolerance)
            
            # Find indices where trend is violated
            violations_indices = []
            for i in range(1, len(clean_series)):
                if clean_series.iloc[i] < clean_series.iloc[i-1]:
                    violations_indices.append(clean_series.index[i])
            
            return {
                "success": is_increasing,
                "result": {
                    "observed_value": increasing_percentage,
                    "details": {
                        "increasing_steps": positive_diffs,
                        "total_steps": total_diffs,
                        "tolerance": tolerance,
                        "threshold": 1 - tolerance,
                        "violations_indices": violations_indices[:10]  # Limit to first 10
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error validating increasing trend: {e}")
            return {
                "success": False,
                "result": {
                    "observed_value": None,
                    "details": {
                        "error": str(e)
                    }
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Validate tolerance if provided
        if "tolerance" in configuration.kwargs:
            tolerance = configuration.kwargs["tolerance"]
            
            if not isinstance(tolerance, (int, float)):
                raise ValueError("tolerance must be a number")
                
            if tolerance < 0 or tolerance > 1:
                raise ValueError("tolerance must be between 0 and 1")
        
        return True


class ExpectColumnValuesTrendDecreasing(ColumnExpectation):
    """
    Custom expectation that validates column values follow a decreasing trend.
    
    This expectation verifies that numeric values in a time series or ordered column
    follow a generally decreasing trend, with an optional tolerance parameter to
    allow for minor fluctuations.
    """
    
    expectation_type = "expect_column_values_trend_decreasing"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
            "tolerance": 0.1  # Default tolerance for trend direction
        }
    
    def _validate(self, series: pd.Series, **kwargs) -> dict:
        """
        Validates that values in the series follow a decreasing trend.
        
        Args:
            series: The pandas Series to validate (should be numeric and ordered)
            **kwargs: Additional parameters including:
                - tolerance: Allowed tolerance for trend violations (0-1)
                
        Returns:
            Validation result with success status and details
        """
        # Get tolerance from kwargs or use default
        tolerance = kwargs.get("tolerance", 0.1)
        
        try:
            # Convert series to numeric if necessary
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Remove NaN values
            clean_series = numeric_series.dropna()
            
            if len(clean_series) <= 1:
                # Not enough data points to determine trend
                return {
                    "success": True,  # Default to success with insufficient data
                    "result": {
                        "observed_value": None,
                        "details": {
                            "message": "Insufficient data points to determine trend",
                            "data_points": len(clean_series)
                        }
                    }
                }
            
            # Calculate differences between consecutive values
            differences = clean_series.diff().dropna()
            
            # Count negative differences (decreases)
            negative_diffs = sum(differences < 0)
            total_diffs = len(differences)
            
            # Calculate percentage of decreasing steps
            if total_diffs > 0:
                decreasing_percentage = negative_diffs / total_diffs
            else:
                decreasing_percentage = 1.0
            
            # Check if decreasing percentage is above (1 - tolerance)
            is_decreasing = decreasing_percentage >= (1 - tolerance)
            
            # Find indices where trend is violated
            violations_indices = []
            for i in range(1, len(clean_series)):
                if clean_series.iloc[i] > clean_series.iloc[i-1]:
                    violations_indices.append(clean_series.index[i])
            
            return {
                "success": is_decreasing,
                "result": {
                    "observed_value": decreasing_percentage,
                    "details": {
                        "decreasing_steps": negative_diffs,
                        "total_steps": total_diffs,
                        "tolerance": tolerance,
                        "threshold": 1 - tolerance,
                        "violations_indices": violations_indices[:10]  # Limit to first 10
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error validating decreasing trend: {e}")
            return {
                "success": False,
                "result": {
                    "observed_value": None,
                    "details": {
                        "error": str(e)
                    }
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Validate tolerance if provided
        if "tolerance" in configuration.kwargs:
            tolerance = configuration.kwargs["tolerance"]
            
            if not isinstance(tolerance, (int, float)):
                raise ValueError("tolerance must be a number")
                
            if tolerance < 0 or tolerance > 1:
                raise ValueError("tolerance must be between 0 and 1")
        
        return True


class ExpectColumnValuesSeasonalPattern(ColumnExpectation):
    """
    Custom expectation that validates column values follow a seasonal pattern.
    
    This expectation verifies that time series data exhibits a seasonal pattern
    with the specified periodicity, using decomposition techniques to identify
    and measure the strength of seasonality.
    """
    
    expectation_type = "expect_column_values_seasonal_pattern"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
            "period": 12,  # Default seasonality period (e.g., 12 for monthly data with yearly seasonality)
            "strength_threshold": 0.5  # Default threshold for seasonal strength
        }
    
    def _validate(self, series: pd.Series, **kwargs) -> dict:
        """
        Validates that the time series in the column follows a seasonal pattern.
        
        Args:
            series: The pandas Series to validate (should be numeric time series)
            **kwargs: Additional parameters including:
                - period: The seasonality period to check for
                - strength_threshold: Minimum strength of seasonality to consider valid
                
        Returns:
            Validation result with success status and details
        """
        # Get parameters from kwargs or use defaults
        period = kwargs.get("period", 12)
        strength_threshold = kwargs.get("strength_threshold", 0.5)
        
        try:
            # Convert series to numeric if necessary
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Remove NaN values
            clean_series = numeric_series.dropna()
            
            if len(clean_series) <= 2 * period:
                # Not enough data points to determine seasonality
                return {
                    "success": True,  # Default to success with insufficient data
                    "result": {
                        "observed_value": None,
                        "details": {
                            "message": f"Insufficient data points to determine seasonality. Need at least {2 * period} points, got {len(clean_series)}",
                            "period": period
                        }
                    }
                }
            
            # Create index-based series if not time-indexed
            if not isinstance(clean_series.index, pd.DatetimeIndex):
                clean_series = pd.Series(clean_series.values)
            
            # Calculate rolling averages for trend
            trend = clean_series.rolling(window=period, center=True).mean()
            
            # Calculate detrended series
            detrended = clean_series - trend
            
            # Create seasonal averages
            seasonal = np.zeros_like(clean_series)
            for i in range(period):
                indices = [j for j in range(i, len(clean_series), period)]
                if indices:
                    seasonal_mean = detrended.iloc[indices].mean()
                    for idx in indices:
                        if idx < len(seasonal):
                            seasonal[idx] = seasonal_mean
            
            # Calculate residuals
            residuals = detrended - seasonal
            
            # Calculate variance of components
            var_seasonal = np.var(seasonal[~np.isnan(seasonal)])
            var_residual = np.var(residuals[~np.isnan(residuals)])
            var_total = np.var(clean_series[~np.isnan(clean_series)])
            
            # Calculate seasonal strength (variance explained by seasonality)
            if var_total > 0:
                seasonal_strength = var_seasonal / var_total
            else:
                seasonal_strength = 0
                
            # Check if seasonal strength exceeds threshold
            is_seasonal = seasonal_strength >= strength_threshold
            
            return {
                "success": is_seasonal,
                "result": {
                    "observed_value": seasonal_strength,
                    "details": {
                        "period": period,
                        "threshold": strength_threshold,
                        "seasonal_variance": var_seasonal,
                        "residual_variance": var_residual,
                        "total_variance": var_total
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error validating seasonal pattern: {e}")
            return {
                "success": False,
                "result": {
                    "observed_value": None,
                    "details": {
                        "error": str(e)
                    }
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Validate period if provided
        if "period" in configuration.kwargs:
            period = configuration.kwargs["period"]
            
            if not isinstance(period, int):
                raise ValueError("period must be an integer")
                
            if period < 2:
                raise ValueError("period must be at least 2")
        
        # Validate strength_threshold if provided
        if "strength_threshold" in configuration.kwargs:
            threshold = configuration.kwargs["strength_threshold"]
            
            if not isinstance(threshold, (int, float)):
                raise ValueError("strength_threshold must be a number")
                
            if threshold < 0 or threshold > 1:
                raise ValueError("strength_threshold must be between 0 and 1")
        
        return True


class ExpectColumnValuesAnomalyScore(ColumnExpectation):
    """
    Custom expectation that validates column values have anomaly scores below threshold.
    
    This expectation computes anomaly scores for values in the column and validates
    that they are below a specified threshold, using various anomaly detection methods
    like z-score, IQR, or median absolute deviation.
    """
    
    expectation_type = "expect_column_values_anomaly_score"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
            "threshold": 3.0,  # Default threshold for anomaly scores (e.g., 3 sigma)
            "method": "zscore"  # Default anomaly detection method
        }
    
    def _validate(self, series: pd.Series, **kwargs) -> dict:
        """
        Validates that anomaly scores for values in the series are below threshold.
        
        Args:
            series: The pandas Series to validate
            **kwargs: Additional parameters including:
                - threshold: Maximum allowed anomaly score
                - method: Anomaly detection method ('zscore', 'iqr', 'mad')
                
        Returns:
            Validation result with success status and details
        """
        # Get parameters from kwargs or use defaults
        threshold = kwargs.get("threshold", 3.0)
        method = kwargs.get("method", "zscore")
        
        try:
            # Convert series to numeric if necessary
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            # Remove NaN values
            clean_series = numeric_series.dropna()
            
            if len(clean_series) < 4:  # Minimum required for meaningful anomaly detection
                return {
                    "success": True,  # Default to success with insufficient data
                    "result": {
                        "observed_value": None,
                        "details": {
                            "message": "Insufficient data points for anomaly detection",
                            "data_points": len(clean_series)
                        }
                    }
                }
            
            # Calculate anomaly scores based on selected method
            if method == "zscore":
                # Z-score method (how many standard deviations from mean)
                mean = clean_series.mean()
                std = clean_series.std()
                if std == 0:  # Handle case where all values are the same
                    anomaly_scores = pd.Series(0, index=clean_series.index)
                else:
                    anomaly_scores = abs((clean_series - mean) / std)
                    
            elif method == "iqr":
                # IQR method (based on interquartile range)
                q1 = clean_series.quantile(0.25)
                q3 = clean_series.quantile(0.75)
                iqr = q3 - q1
                if iqr == 0:  # Handle case where IQR is zero
                    anomaly_scores = pd.Series(0, index=clean_series.index)
                else:
                    anomaly_scores = abs((clean_series - clean_series.median()) / iqr)
                    
            elif method == "mad":
                # Median Absolute Deviation method
                median = clean_series.median()
                mad = np.median(abs(clean_series - median))
                if mad == 0:  # Handle case where MAD is zero
                    anomaly_scores = pd.Series(0, index=clean_series.index)
                else:
                    anomaly_scores = abs((clean_series - median) / mad)
                    
            else:
                raise ValueError(f"Unsupported anomaly detection method: {method}")
            
            # Identify anomalies (scores above threshold)
            anomalies = anomaly_scores[anomaly_scores > threshold]
            
            # Calculate percentage of anomalies
            anomaly_percentage = len(anomalies) / len(clean_series) if len(clean_series) > 0 else 0
            
            # Get indices of anomalous values
            anomaly_indices = anomalies.index.tolist()
            
            # Get values of anomalies
            anomaly_values = [(idx, clean_series.loc[idx], anomaly_scores.loc[idx]) 
                             for idx in anomaly_indices]
            
            return {
                "success": len(anomalies) == 0,
                "result": {
                    "observed_value": anomaly_percentage,
                    "details": {
                        "anomaly_count": len(anomalies),
                        "total_count": len(clean_series),
                        "threshold": threshold,
                        "method": method,
                        "anomaly_indices": anomaly_indices[:10],  # Limit to first 10
                        "anomaly_info": anomaly_values[:10]  # Limit to first 10
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error calculating anomaly scores: {e}")
            return {
                "success": False,
                "result": {
                    "observed_value": None,
                    "details": {
                        "error": str(e)
                    }
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Validate threshold if provided
        if "threshold" in configuration.kwargs:
            threshold = configuration.kwargs["threshold"]
            
            if not isinstance(threshold, (int, float)):
                raise ValueError("threshold must be a number")
                
            if threshold <= 0:
                raise ValueError("threshold must be positive")
        
        # Validate method if provided
        if "method" in configuration.kwargs:
            method = configuration.kwargs["method"]
            
            if not isinstance(method, str):
                raise ValueError("method must be a string")
                
            valid_methods = ["zscore", "iqr", "mad"]
            if method.lower() not in valid_methods:
                raise ValueError(f"method must be one of {valid_methods}")
        
        return True


class ExpectTableRowCountToBeBetweenDates(TableExpectation):
    """
    Custom expectation that validates table row count is within expected range for date range.
    
    This expectation filters the table by a specified date range and validates that
    the filtered row count is within the expected minimum and maximum values.
    """
    
    expectation_type = "expect_table_row_count_to_be_between_dates"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_kwarg_values = {
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": False,
            "meta": None,
        }
    
    def _validate(self, dataset: pd.DataFrame, **kwargs) -> dict:
        """
        Validates that the row count in the date range is within expected range.
        
        Args:
            dataset: The pandas DataFrame to validate
            **kwargs: Additional parameters including:
                - date_column: Column containing dates to filter by
                - start_date: Start date for filtering (inclusive)
                - end_date: End date for filtering (inclusive)
                - min_count: Minimum expected row count
                - max_count: Maximum expected row count
                
        Returns:
            Validation result with success status and details
        """
        # Get parameters from kwargs
        date_column = kwargs.get("date_column")
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        min_count = kwargs.get("min_count")
        max_count = kwargs.get("max_count")
        
        # Validate required parameters
        if date_column is None:
            raise ValueError("date_column must be provided")
            
        if start_date is None:
            raise ValueError("start_date must be provided")
            
        if end_date is None:
            raise ValueError("end_date must be provided")
            
        if min_count is None and max_count is None:
            raise ValueError("at least one of min_count or max_count must be provided")
            
        try:
            # Check if date_column exists
            if date_column not in dataset.columns:
                return {
                    "success": False,
                    "result": {
                        "observed_value": None,
                        "details": {
                            "error": f"Column '{date_column}' not found in dataset"
                        }
                    }
                }
            
            # Convert string dates to datetime if necessary
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date)
                
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
                
            # Convert column to datetime if necessary
            if dataset[date_column].dtype != 'datetime64[ns]':
                date_series = pd.to_datetime(dataset[date_column], errors='coerce')
            else:
                date_series = dataset[date_column]
            
            # Filter dataset by date range
            filtered_df = dataset[(date_series >= start_date) & (date_series <= end_date)]
            
            # Count rows in filtered dataset
            row_count = len(filtered_df)
            
            # Check if count is within expected range
            if min_count is not None and max_count is not None:
                success = min_count <= row_count <= max_count
            elif min_count is not None:
                success = row_count >= min_count
            else:  # max_count is not None
                success = row_count <= max_count
            
            return {
                "success": success,
                "result": {
                    "observed_value": row_count,
                    "details": {
                        "start_date": start_date.strftime("%Y-%m-%d") if hasattr(start_date, "strftime") else start_date,
                        "end_date": end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else end_date,
                        "min_count": min_count,
                        "max_count": max_count
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error validating row count between dates: {e}")
            return {
                "success": False,
                "result": {
                    "observed_value": None,
                    "details": {
                        "error": str(e)
                    }
                }
            }
    
    def validate_configuration(self, configuration: Dict) -> bool:
        """
        Validates that the configuration for this expectation is valid.
        
        Args:
            configuration: The expectation configuration
            
        Returns:
            True if configuration is valid
        """
        # Validate required parameters
        required_params = ["date_column", "start_date", "end_date"]
        for param in required_params:
            if param not in configuration.kwargs:
                raise ValueError(f"{param} must be provided")
                
        # Validate that at least one of min_count or max_count is provided
        if "min_count" not in configuration.kwargs and "max_count" not in configuration.kwargs:
            raise ValueError("at least one of min_count or max_count must be provided")
            
        # Validate numeric parameters
        for param in ["min_count", "max_count"]:
            if param in configuration.kwargs:
                value = configuration.kwargs[param]
                if not isinstance(value, (int, float)):
                    raise ValueError(f"{param} must be a number")
                    
                if value < 0:
                    raise ValueError(f"{param} must be non-negative")
        
        # Validate that min_count <= max_count if both are provided
        if ("min_count" in configuration.kwargs and 
            "max_count" in configuration.kwargs and
            configuration.kwargs["min_count"] > configuration.kwargs["max_count"]):
            raise ValueError("min_count must be less than or equal to max_count")
        
        return True


def register_custom_expectations() -> None:
    """
    Registers all custom expectations with the Great Expectations registry.
    """
    logger.info("Registering custom expectations")
    
    try:
        # Register each custom expectation class
        register_expectation(ExpectColumnValuesInReferenceTable)
        register_expectation(ExpectColumnValuesTrendIncreasing)
        register_expectation(ExpectColumnValuesTrendDecreasing)
        register_expectation(ExpectColumnValuesSeasonalPattern)
        register_expectation(ExpectColumnValuesAnomalyScore)
        register_expectation(ExpectTableRowCountToBeBetweenDates)
        
        logger.info("Successfully registered custom expectations")
    except Exception as e:
        logger.error(f"Error registering custom expectations: {e}")
        raise