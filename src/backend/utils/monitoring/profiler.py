"""
Performance profiling utility for the self-healing data pipeline. Provides tools to measure,
record, and analyze execution time and resource usage of code blocks and functions.
Supports both decorator-based and context manager-based profiling with configurable
detail levels and metric reporting.
"""

import time
import functools
import contextlib
import typing
import psutil  # version 5.9.0+
import cProfile
import pstats

from src.backend.config import get_config  # Access application configuration settings for profiling
from src.backend.constants import DEFAULT_MAX_RETRY_ATTEMPTS  # Import constant values for configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for the module
from src.backend.utils.monitoring.metric_client import MetricClient, METRIC_KIND_GAUGE, VALUE_TYPE_DOUBLE, VALUE_TYPE_INT64  # Send profiling metrics to Cloud Monitoring

# Initialize logger for this module
logger = get_logger(__name__)

# Configuration keys for profiling settings
PROFILING_ENABLED_CONFIG_KEY = "monitoring.profiling.enabled"
PROFILING_LEVEL_CONFIG_KEY = "monitoring.profiling.level"
PROFILING_METRIC_REPORTING_CONFIG_KEY = "monitoring.profiling.report_metrics"

# Default values for profiling settings
DEFAULT_PROFILING_ENABLED = True
DEFAULT_PROFILING_LEVEL = "BASIC"
DEFAULT_METRIC_REPORTING = True

# Profiling levels
PROFILING_LEVEL_BASIC = "BASIC"
PROFILING_LEVEL_DETAILED = "DETAILED"
PROFILING_LEVEL_ADVANCED = "ADVANCED"

# Global variable to store the MetricClient instance
_metric_client = None


def get_profiling_config() -> dict:
    """Retrieves profiling configuration settings from application config

    Returns:
        dict: Dictionary of profiling configuration settings
    """
    # Get application configuration
    config = get_config()

    # Check if profiling is enabled (default to DEFAULT_PROFILING_ENABLED if not set)
    profiling_enabled = config.get(PROFILING_ENABLED_CONFIG_KEY, DEFAULT_PROFILING_ENABLED)

    # Get profiling level (default to DEFAULT_PROFILING_LEVEL if not set)
    profiling_level = config.get(PROFILING_LEVEL_CONFIG_KEY, DEFAULT_PROFILING_LEVEL)

    # Check if metric reporting is enabled (default to DEFAULT_METRIC_REPORTING if not set)
    report_metrics = config.get(PROFILING_METRIC_REPORTING_CONFIG_KEY, DEFAULT_METRIC_REPORTING)

    # Return dictionary with profiling settings
    return {
        "enabled": profiling_enabled,
        "level": profiling_level,
        "report_metrics": report_metrics
    }


def is_profiling_enabled() -> bool:
    """Checks if profiling is enabled in the configuration

    Returns:
        bool: True if profiling is enabled
    """
    # Get profiling configuration
    profiling_config = get_profiling_config()

    # Return the 'enabled' value from configuration
    return profiling_config.get("enabled", False)


def get_metric_client() -> typing.Optional[MetricClient]:
    """Gets or creates the MetricClient singleton instance

    Returns:
        MetricClient: MetricClient instance or None if metrics disabled
    """
    global _metric_client

    # Check if _metric_client global exists
    if _metric_client is None:
        # Check if metric reporting is enabled in config
        profiling_config = get_profiling_config()
        if profiling_config.get("report_metrics", False):
            # If enabled, create new MetricClient instance
            try:
                _metric_client = MetricClient()
            except Exception as e:
                logger.error(f"Failed to create MetricClient: {e}")
                return None
        else:
            return None

    # Return the metric client instance or None if disabled
    return _metric_client


def format_duration(seconds: float) -> str:
    """Formats a duration in seconds to a human-readable string

    Args:
        seconds (float): Duration in seconds

    Returns:
        str: Formatted duration string
    """
    # If duration is less than 1 millisecond, format as microseconds
    if seconds < 0.001:
        return f"{seconds * 1000000:.2f} us"
    # If duration is less than 1 second, format as milliseconds
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    # If duration is less than 60 seconds, format as seconds
    elif seconds < 60:
        return f"{seconds:.2f} s"
    # If duration is less than 3600 seconds, format as minutes and seconds
    elif seconds < 3600:
        minutes = int(seconds // 60)
        seconds = seconds % 60
        return f"{minutes} m {seconds:.2f} s"
    # Otherwise format as hours, minutes, and seconds
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        return f"{hours} h {minutes} m {seconds:.2f} s"


def report_metric(metric_name: str, value: float, labels: dict) -> bool:
    """Reports a profiling metric to Cloud Monitoring

    Args:
        metric_name (str): Name of the metric
        value (float): Value of the metric
        labels (dict): Dictionary of labels for the metric

    Returns:
        bool: True if successful, False otherwise
    """
    # Check if metric reporting is enabled
    if not get_profiling_config().get("report_metrics", False):
        logger.debug("Metric reporting is disabled")
        return False

    # Get metric client instance
    metric_client = get_metric_client()
    if metric_client is None:
        logger.debug("Metric client is not available")
        return False

    # Prepare metric labels with profiling information
    metric_labels = labels.copy()
    metric_labels["profiling"] = "true"

    # Create gauge metric with provided value and labels
    success = metric_client.create_gauge_metric(
        metric_type=metric_name,
        value=value,
        labels=metric_labels
    )

    # Log metric creation at debug level
    if success:
        logger.debug(f"Reported metric {metric_name} with value {value} and labels {labels}")
    else:
        logger.warning(f"Failed to report metric {metric_name}")

    # Return success status
    return success


def profile(name: str = None, labels: dict = None, level: str = None) -> callable:
    """Decorator for profiling function execution time and resource usage

    Args:
        name (str): Name of the profile (defaults to function name)
        labels (dict): Dictionary of labels to add to metrics
        level (str): Profiling level (BASIC, DETAILED, ADVANCED)

    Returns:
        callable: Decorated function
    """
    # Check if profiling is enabled in configuration
    if not is_profiling_enabled():
        # If not enabled, return original function unchanged
        return lambda func: func

    # Create a wrapper function that profiles the function execution
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Set profile name to function name if name not provided
            profile_name = name or func.__name__

            # Create Profiler instance with provided parameters
            profiler = Profiler(name=profile_name, labels=labels, level=level)

            # Use profiler.profile_function to profile the function execution
            return profiler.profile_function(func, args, kwargs)

        return wrapper

    return decorator


class Profiler:
    """Main class for performance profiling of code execution"""

    def __init__(self, name: str = None, labels: dict = None, level: str = None):
        """Initializes a new Profiler instance

        Args:
            name (str): Name of the profile (defaults to 'unnamed_profile')
            labels (dict): Dictionary of labels to add to metrics
            level (str): Profiling level (BASIC, DETAILED, ADVANCED)
        """
        # Set name to provided name or 'unnamed_profile'
        self.name = name or "unnamed_profile"
        # Initialize labels dictionary with provided labels or empty dict
        self.labels = labels or {}
        # Set profiling level from parameter or config default
        self.level = level or get_profiling_config().get("level", DEFAULT_PROFILING_LEVEL)
        # Initialize timing and resource variables
        self.start_time = None
        self.end_time = None
        self.start_resources = None
        self.end_resources = None
        # Initialize metrics dictionary
        self.metrics = {}
        # Set active flag to False (not started yet)
        self.active = False
        # Get metric client if metric reporting is enabled
        self._metric_client = get_metric_client()

        # Log profiler initialization at debug level
        logger.debug(f"Initialized Profiler: name={self.name}, level={self.level}")

    def start_timer(self):
        """Starts the profiling timer and captures initial resource usage"""
        # Set start_time to current time
        self.start_time = time.time()

        # If profiling level is DETAILED or ADVANCED, capture initial resource usage
        if self.level in [PROFILING_LEVEL_DETAILED, PROFILING_LEVEL_ADVANCED]:
            self.start_resources = self.capture_resource_usage()

        # Set active flag to True
        self.active = True

        # Log profiling start at debug level
        logger.debug(f"Started profiling: {self.name}")

    def stop_timer(self) -> float:
        """Stops the profiling timer and captures final resource usage

        Returns:
            float: Duration in seconds
        """
        # Check if profiler is active, return 0 if not
        if not self.active:
            return 0

        # Set end_time to current time
        self.end_time = time.time()

        # Calculate duration in seconds
        duration = self.end_time - self.start_time

        # If profiling level is DETAILED or ADVANCED, capture final resource usage
        if self.level in [PROFILING_LEVEL_DETAILED, PROFILING_LEVEL_ADVANCED]:
            self.end_resources = self.capture_resource_usage()

            # Calculate resource usage differences
            self.metrics = self.calculate_resource_usage()

        # Set active flag to False
        self.active = False

        # Log profiling results at info level
        logger.info(
            f"Profiling {self.name} completed in {format_duration(duration)}"
        )

        # If metric reporting is enabled, report metrics to Cloud Monitoring
        if self._metric_client:
            self.report_metrics()

        # Return duration in seconds
        return duration

    def profile_function(self, func: callable, args: tuple, kwargs: dict) -> typing.Any:
        """Profiles the execution of a function

        Args:
            func (callable): Function to profile
            args (tuple): Function arguments
            kwargs (dict): Function keyword arguments

        Returns:
            typing.Any: Result of the function execution
        """
        # Start the profiling timer
        self.start_timer()

        # If profiling level is ADVANCED, use cProfile for detailed profiling
        if self.level == PROFILING_LEVEL_ADVANCED:
            profiler = cProfile.Profile()
            try:
                # Execute the function with cProfile
                profiler.enable()
                result = func(*args, **kwargs)
                profiler.disable()
            except Exception as e:
                # If exception occurs, log error and stop timer
                logger.error(f"Error during profiling of {self.name}: {e}")
                self.stop_timer()
                raise
            finally:
                # Stop the profiling timer
                duration = self.stop_timer()

                # Process and log cProfile results
                s = pstats.Stats(profiler)
                s.sort_stats("cumulative").print_stats(10)
                logger.info(f"cProfile results for {self.name} (duration: {format_duration(duration)})")
        # Otherwise, execute function normally with timing
        else:
            try:
                # Try to execute the function and capture result
                result = func(*args, **kwargs)
            except Exception as e:
                # If exception occurs, log error and stop timer
                logger.error(f"Error during execution of {self.name}: {e}")
                self.stop_timer()
                raise
            finally:
                # Stop the profiling timer
                self.stop_timer()

        # Return the function result or re-raise exception
        return result

    def capture_resource_usage(self) -> dict:
        """Captures current system resource usage

        Returns:
            dict: Dictionary of resource usage metrics
        """
        # Use psutil to get current process
        process = psutil.Process()

        # Capture CPU usage percentage
        cpu_usage = process.cpu_percent(interval=None)

        # Capture memory usage (RSS, VMS)
        memory_info = process.memory_info()
        memory_rss = memory_info.rss
        memory_vms = memory_info.vms

        # Capture IO counters if available
        io_counters = process.io_counters()
        read_count = io_counters.read_count if hasattr(io_counters, "read_count") else None
        write_count = io_counters.write_count if hasattr(io_counters, "write_count") else None
        read_bytes = io_counters.read_bytes if hasattr(io_counters, "read_bytes") else None
        write_bytes = io_counters.write_bytes if hasattr(io_counters, "write_bytes") else None

        # Capture thread count
        thread_count = process.num_threads()

        # Return dictionary with resource metrics
        return {
            "cpu_usage": cpu_usage,
            "memory_rss": memory_rss,
            "memory_vms": memory_vms,
            "io_read_count": read_count,
            "io_write_count": write_count,
            "io_read_bytes": read_bytes,
            "io_write_bytes": write_bytes,
            "thread_count": thread_count
        }

    def calculate_resource_usage(self) -> dict:
        """Calculates resource usage differences between start and end

        Returns:
            dict: Dictionary of resource usage differences
        """
        # Check if start_resources and end_resources are available
        if not self.start_resources or not self.end_resources:
            return {}

        # Calculate CPU usage average
        cpu_usage_avg = (self.start_resources["cpu_usage"] + self.end_resources["cpu_usage"]) / 2

        # Calculate memory usage differences
        memory_rss_diff = self.end_resources["memory_rss"] - self.start_resources["memory_rss"]
        memory_vms_diff = self.end_resources["memory_vms"] - self.start_resources["memory_vms"]

        # Calculate IO operation differences
        io_read_count_diff = (
            self.end_resources["io_read_count"] - self.start_resources["io_read_count"]
            if self.start_resources.get("io_read_count") is not None and self.end_resources.get("io_read_count") is not None
            else None
        )
        io_write_count_diff = (
            self.end_resources["io_write_count"] - self.start_resources["io_write_count"]
            if self.start_resources.get("io_write_count") is not None and self.end_resources.get("io_write_count") is not None
            else None
        )
        io_read_bytes_diff = (
            self.end_resources["io_read_bytes"] - self.start_resources["io_read_bytes"]
            if self.start_resources.get("io_read_bytes") is not None and self.end_resources.get("io_read_bytes") is not None
            else None
        )
        io_write_bytes_diff = (
            self.end_resources["io_write_bytes"] - self.start_resources["io_write_bytes"]
            if self.start_resources.get("io_write_bytes") is not None and self.end_resources.get("io_write_bytes") is not None
            else None
        )

        # Return dictionary with resource usage differences
        return {
            "cpu_usage_avg": cpu_usage_avg,
            "memory_rss_diff": memory_rss_diff,
            "memory_vms_diff": memory_vms_diff,
            "io_read_count_diff": io_read_count_diff,
            "io_write_count_diff": io_write_count_diff,
            "io_read_bytes_diff": io_read_bytes_diff,
            "io_write_bytes_diff": io_write_bytes_diff
        }

    def report_metrics(self) -> bool:
        """Reports profiling metrics to Cloud Monitoring

        Returns:
            bool: True if successful, False otherwise
        """
        # Check if metric reporting is enabled and client is available
        if not self._metric_client:
            logger.debug("Metric client is not available, skipping metric reporting")
            return False

        # Report duration metric
        success = report_metric(
            metric_name="profiling.duration",
            value=self.end_time - self.start_time,
            labels=self.labels
        )

        # If detailed resources available, report resource metrics
        if self.metrics:
            for metric, value in self.metrics.items():
                if value is not None:
                    report_metric(
                        metric_name=f"profiling.resource.{metric}",
                        value=value,
                        labels=self.labels
                    )

        # Log metrics reporting at debug level
        logger.debug(f"Reported profiling metrics for {self.name}")

        # Return overall success status
        return success

    def is_profiling_enabled(self) -> bool:
        """Checks if profiling is enabled

        Returns:
            bool: True if profiling is enabled
        """
        # Call global is_profiling_enabled function
        return is_profiling_enabled()

    def get_profiling_level(self) -> str:
        """Gets the current profiling level

        Returns:
            str: Profiling level (BASIC, DETAILED, ADVANCED)
        """
        # Get profiling configuration
        profiling_config = get_profiling_config()

        # Return the 'level' value from configuration
        return profiling_config.get("level", DEFAULT_PROFILING_LEVEL)


class ProfilerContext:
    """Context manager for profiling code blocks"""

    def __init__(self, name: str = None, labels: dict = None, level: str = None, report_on_exit: bool = True):
        """Initializes the profiler context manager

        Args:
            name (str): Name of the profile (defaults to 'unnamed_profile')
            labels (dict): Dictionary of labels to add to metrics
            level (str): Profiling level (BASIC, DETAILED, ADVANCED)
            report_on_exit (bool): Whether to report metrics when exiting the context
        """
        # Create new Profiler instance with provided parameters
        self._profiler = Profiler(name=name, labels=labels, level=level)
        # Set report_on_exit flag (default to True)
        self._report_on_exit = report_on_exit

    def __enter__(self):
        """Enters the profiling context and starts the timer

        Returns:
            ProfilerContext: Self reference for context manager
        """
        # Check if profiling is enabled
        if not self._profiler.is_profiling_enabled():
            return self

        # Start the profiler timer
        self._profiler.start_timer()

        # Return self for context variable assignment
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the profiling context and stops the timer

        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        # Stop the profiler timer
        self._profiler.stop_timer()

        # If report_on_exit is True, report metrics
        if self._report_on_exit:
            self._profiler.report_metrics()

        # Return None to propagate any exceptions
        return None

    def get_metrics(self) -> dict:
        """Gets the collected profiling metrics

        Returns:
            dict: Dictionary of profiling metrics
        """
        # Return the metrics dictionary from the profiler
        if self._profiler and self._profiler.active is False:
            return self._profiler.metrics

        # Return empty dict if profiler is not active or metrics not available
        return {}