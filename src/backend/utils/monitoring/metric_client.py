"""
Client for interacting with Google Cloud Monitoring API to create, manage, and query metrics.

This module provides a simplified interface for creating metric descriptors, time series data, 
and retrieving metric data for the self-healing data pipeline's monitoring system.

It offers:
- Creation and management of custom metric descriptors
- Submission of time series data points
- Retrieval and analysis of metrics
- Support for different metric types (gauge, cumulative)
- Automatic retry handling for API resilience
"""

import datetime
import typing
from typing import Dict, List, Optional, Union, Any, Type

from google.cloud.monitoring_v3 import MetricServiceClient
from google.cloud.monitoring_v3.types import (
    MetricDescriptor,
    TimeSeries as TimeSeriesProto,
    TimeInterval,
    Metric,
    Point,
    TypedValue,
    MonitoredResource
)
from google.protobuf.timestamp_pb2 import Timestamp
import google.api_core.exceptions

from ..retry.retry_decorator import retry
from ..auth.gcp_auth import get_default_credentials, get_project_id
from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS
from ...config import get_config
from ...logging_config import get_logger

# Set up logger
logger = get_logger(__name__)

# Default domain for custom metrics
DEFAULT_METRIC_DOMAIN = "custom.googleapis.com/self_healing_pipeline"

# Metric kind constants
METRIC_KIND_GAUGE = "GAUGE"
METRIC_KIND_CUMULATIVE = "CUMULATIVE"

# Value type constants
VALUE_TYPE_INT64 = "INT64"
VALUE_TYPE_DOUBLE = "DOUBLE"
VALUE_TYPE_BOOL = "BOOL"
VALUE_TYPE_STRING = "STRING"

# Default monitored resource type
DEFAULT_RESOURCE_TYPE = "global"


def format_metric_type(metric_name: str, domain: str = DEFAULT_METRIC_DOMAIN) -> str:
    """Formats a metric name with the appropriate domain prefix.
    
    Args:
        metric_name: Base metric name
        domain: Domain prefix for the metric
        
    Returns:
        Fully qualified metric type
    """
    # Check if metric_name already has a domain prefix
    if metric_name.startswith("custom.googleapis.com/") or "/" not in metric_name:
        return f"{domain}/{metric_name}" if "/" not in metric_name else metric_name
    
    # Return the formatted metric type
    return metric_name


def format_resource_labels(labels: Dict[str, Any], project_id: str) -> Dict[str, str]:
    """Formats resource labels for use with Cloud Monitoring.
    
    Args:
        labels: Dictionary of labels to format
        project_id: GCP project ID
        
    Returns:
        Formatted resource labels dictionary
    """
    # Initialize with default project_id label
    resource_labels = {"project_id": project_id}
    
    # Add any additional labels from the input
    if labels:
        resource_labels.update(labels)
    
    # Ensure all label keys and values are strings
    return {str(k): str(v) for k, v in resource_labels.items()}


def create_timestamp(dt: Optional[datetime.datetime] = None) -> Timestamp:
    """Creates a protobuf timestamp from a datetime object.
    
    Args:
        dt: Datetime object to convert, or None for current time
        
    Returns:
        Protobuf timestamp
    """
    if dt is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
    
    timestamp = Timestamp()
    timestamp.seconds = int(dt.timestamp())
    timestamp.nanos = int((dt.timestamp() % 1) * 10**9)
    
    return timestamp


def determine_value_type(value: Any) -> str:
    """Determines the appropriate value type based on the value.
    
    Args:
        value: Value to determine type for
        
    Returns:
        Value type (INT64, DOUBLE, BOOL, STRING)
    """
    if isinstance(value, int):
        return VALUE_TYPE_INT64
    elif isinstance(value, float):
        return VALUE_TYPE_DOUBLE
    elif isinstance(value, bool):
        return VALUE_TYPE_BOOL
    else:
        return VALUE_TYPE_STRING


class MetricClient:
    """Client for interacting with Google Cloud Monitoring API.
    
    This class provides methods to create and manage metric descriptors,
    create time series data, and query metrics data for monitoring
    the self-healing data pipeline.
    """
    
    def __init__(self, project_id: Optional[str] = None, config_override: Optional[Dict[str, Any]] = None):
        """Initializes the MetricClient with project and configuration.
        
        Args:
            project_id: GCP project ID (if None, will be detected)
            config_override: Optional configuration overrides
        """
        # Initialize configuration from application settings
        self._config = get_config().get("monitoring", {})
        
        # Apply any configuration overrides provided
        if config_override:
            self._config.update(config_override)
        
        # Set project ID (use from config if not provided)
        self._project_id = project_id or self._config.get("project_id") or get_project_id()
        if not self._project_id:
            raise ValueError("Project ID must be provided or available in configuration")
        
        # Format project name for API calls
        self._project_name = f"projects/{self._project_id}"
        
        # Set metric domain from config or default
        self._metric_domain = self._config.get("metric_domain", DEFAULT_METRIC_DOMAIN)
        
        # Initialize Google Cloud Monitoring client
        credentials = get_default_credentials()
        self._client = MetricServiceClient(credentials=credentials)
        
        logger.info(f"Initialized MetricClient for project {self._project_id}")
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def create_metric_descriptor(
        self,
        metric_type: str,
        display_name: str,
        description: str,
        metric_kind: str = METRIC_KIND_GAUGE,
        value_type: str = VALUE_TYPE_INT64,
        unit: str = "",
        labels: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Creates a new metric descriptor in Cloud Monitoring.
        
        Args:
            metric_type: Type of metric (will be prefixed with domain if needed)
            display_name: Human-readable name for the metric
            description: Detailed description of the metric
            metric_kind: Kind of metric (GAUGE, CUMULATIVE, etc.)
            value_type: Type of the metric's values (INT64, DOUBLE, etc.)
            unit: Unit of the metric (e.g., "s", "By", "1")
            labels: List of label dictionaries with keys: key, value_type, description
            
        Returns:
            Created metric descriptor
        """
        # Format metric type with domain if not already formatted
        metric_type = format_metric_type(metric_type, self._metric_domain)
        
        # Create label descriptors if provided
        label_descriptors = []
        if labels:
            for label in labels:
                label_descriptors.append(
                    MetricDescriptor.LabelDescriptor(
                        key=label["key"],
                        value_type=label.get("value_type", "STRING"),
                        description=label.get("description", "")
                    )
                )
        
        # Create MetricDescriptor object
        descriptor = MetricDescriptor(
            type=metric_type,
            display_name=display_name,
            description=description,
            metric_kind=metric_kind,
            value_type=value_type,
            unit=unit,
            labels=label_descriptors
        )
        
        # Call API to create descriptor
        try:
            created_descriptor = self._client.create_metric_descriptor(
                name=self._project_name,
                metric_descriptor=descriptor
            )
            logger.info(f"Created metric descriptor: {metric_type}")
            return self._descriptor_to_dict(created_descriptor)
        except google.api_core.exceptions.AlreadyExists:
            logger.warning(f"Metric descriptor {metric_type} already exists")
            # Return existing descriptor
            return self.get_metric_descriptor(metric_type)
        except Exception as e:
            logger.error(f"Error creating metric descriptor {metric_type}: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def get_metric_descriptor(self, metric_type: str) -> Optional[Dict[str, Any]]:
        """Gets a metric descriptor by name.
        
        Args:
            metric_type: Type of metric to retrieve
            
        Returns:
            Metric descriptor or None if not found
        """
        # Format metric type with domain if not already formatted
        metric_type = format_metric_type(metric_type, self._metric_domain)
        
        # Format descriptor name
        descriptor_name = f"{self._project_name}/metricDescriptors/{metric_type}"
        
        try:
            descriptor = self._client.get_metric_descriptor(name=descriptor_name)
            return self._descriptor_to_dict(descriptor)
        except google.api_core.exceptions.NotFound:
            logger.warning(f"Metric descriptor {metric_type} not found")
            return None
        except Exception as e:
            logger.error(f"Error getting metric descriptor {metric_type}: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def list_metric_descriptors(self, filter_str: str = "") -> List[Dict[str, Any]]:
        """Lists metric descriptors matching a filter.
        
        Args:
            filter_str: Filter expression for metrics
            
        Returns:
            List of matching metric descriptors
        """
        try:
            # Prepare request
            request = {"name": self._project_name, "filter": filter_str}
            
            # Call API to list descriptors
            results = []
            page_result = self._client.list_metric_descriptors(request=request)
            
            # Collect all results
            for descriptor in page_result:
                results.append(self._descriptor_to_dict(descriptor))
            
            logger.debug(f"Listed {len(results)} metric descriptors")
            return results
        except Exception as e:
            logger.error(f"Error listing metric descriptors: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def delete_metric_descriptor(self, metric_type: str) -> bool:
        """Deletes a metric descriptor.
        
        Args:
            metric_type: Type of metric to delete
            
        Returns:
            True if successful, False otherwise
        """
        # Format metric type with domain if not already formatted
        metric_type = format_metric_type(metric_type, self._metric_domain)
        
        # Format descriptor name
        descriptor_name = f"{self._project_name}/metricDescriptors/{metric_type}"
        
        try:
            self._client.delete_metric_descriptor(name=descriptor_name)
            logger.info(f"Deleted metric descriptor: {metric_type}")
            return True
        except google.api_core.exceptions.NotFound:
            logger.warning(f"Metric descriptor {metric_type} not found for deletion")
            return False
        except Exception as e:
            logger.error(f"Error deleting metric descriptor {metric_type}: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def create_time_series(
        self,
        metric_type: str,
        value: Any,
        labels: Optional[Dict[str, str]] = None,
        resource_labels: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime.datetime] = None,
        metric_kind: str = METRIC_KIND_GAUGE,
        value_type: Optional[str] = None
    ) -> bool:
        """Creates a new time series data point.
        
        Args:
            metric_type: Type of metric (will be prefixed with domain if needed)
            value: Value for the data point
            labels: Metric labels
            resource_labels: Resource labels
            timestamp: Timestamp for the data point (defaults to now)
            metric_kind: Kind of metric (GAUGE, CUMULATIVE)
            value_type: Type of value (will be determined automatically if None)
            
        Returns:
            True if successful, False otherwise
        """
        # Format metric type with domain if not already formatted
        metric_type = format_metric_type(metric_type, self._metric_domain)
        
        # Determine value type if not provided
        if value_type is None:
            value_type = determine_value_type(value)
        
        # Set default timestamp to now if not provided
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        
        # Create TimeSeries object
        time_series = TimeSeries(
            metric_type=metric_type,
            value=value,
            metric_labels=labels or {},
            resource_labels=resource_labels or {},
            timestamp=timestamp,
            metric_kind=metric_kind,
            value_type=value_type,
            resource_type=DEFAULT_RESOURCE_TYPE
        )
        
        # Convert to proto and create time series
        try:
            time_series_proto = time_series.to_proto()
            self._client.create_time_series(
                name=self._project_name,
                time_series=[time_series_proto]
            )
            logger.debug(f"Created time series data point for {metric_type}")
            return True
        except Exception as e:
            logger.error(f"Error creating time series for {metric_type}: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def create_time_series_batch(self, time_series: List[Union[Dict[str, Any], 'TimeSeries']]) -> bool:
        """Creates multiple time series data points in a batch.
        
        Args:
            time_series: List of TimeSeries objects or dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not time_series:
            logger.warning("Cannot create time series batch: empty list provided")
            return False
        
        # Convert dictionaries to TimeSeries objects if needed
        series_objects = []
        for ts in time_series:
            if isinstance(ts, dict):
                series_objects.append(TimeSeries.from_dict(ts))
            else:
                series_objects.append(ts)
        
        # Convert TimeSeries objects to proto
        series_protos = [ts.to_proto() for ts in series_objects]
        
        # Create time series in batches (max 200 per request)
        try:
            # Process in batches of 200 as per API limits
            batch_size = 200
            for i in range(0, len(series_protos), batch_size):
                batch = series_protos[i:i + batch_size]
                self._client.create_time_series(
                    name=self._project_name,
                    time_series=batch
                )
            
            logger.info(f"Created {len(series_protos)} time series data points in batch")
            return True
        except Exception as e:
            logger.error(f"Error creating time series batch: {str(e)}")
            raise
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def list_time_series(
        self,
        filter_str: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        aggregation: Optional[Dict[str, Any]] = None,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Lists time series data matching a filter.
        
        Args:
            filter_str: Filter expression for time series
            start_time: Start time for the query interval
            end_time: End time for the query interval
            aggregation: Optional aggregation parameters
            page_size: Number of results per page
            
        Returns:
            List of matching time series data
        """
        try:
            # Create interval
            interval = TimeInterval(
                start_time=create_timestamp(start_time),
                end_time=create_timestamp(end_time)
            )
            
            # Prepare request
            request = {
                "name": self._project_name,
                "filter": filter_str,
                "interval": interval,
                "page_size": page_size
            }
            
            # Add aggregation if specified
            if aggregation:
                request["aggregation"] = aggregation
            
            # Call API to list time series
            results = []
            page_result = self._client.list_time_series(request=request)
            
            # Collect all results
            for time_series in page_result:
                results.append(self._time_series_to_dict(time_series))
            
            logger.debug(f"Listed {len(results)} time series")
            return results
        except Exception as e:
            logger.error(f"Error listing time series: {str(e)}")
            raise
    
    def get_metric_data(
        self,
        metric_type: str,
        metric_labels: Optional[Dict[str, str]] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        aggregation: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Gets metric data for a specific metric type and labels.
        
        Args:
            metric_type: Type of metric to retrieve
            metric_labels: Metric labels to filter by
            start_time: Start time for the query interval (defaults to 1 hour ago)
            end_time: End time for the query interval (defaults to now)
            aggregation: Optional aggregation parameters
            
        Returns:
            List of metric data points
        """
        # Format metric type with domain if not already formatted
        metric_type = format_metric_type(metric_type, self._metric_domain)
        
        # Set default time range if not provided
        if end_time is None:
            end_time = datetime.datetime.now(datetime.timezone.utc)
        if start_time is None:
            start_time = end_time - datetime.timedelta(hours=1)
        
        # Build filter string
        filter_parts = [f'metric.type = "{metric_type}"']
        if metric_labels:
            for key, value in metric_labels.items():
                filter_parts.append(f'metric.labels.{key} = "{value}"')
        filter_str = " AND ".join(filter_parts)
        
        # Get time series data
        time_series_list = self.list_time_series(
            filter_str=filter_str,
            start_time=start_time,
            end_time=end_time,
            aggregation=aggregation
        )
        
        # Process results to extract data points
        results = []
        for ts in time_series_list:
            for point in ts.get("points", []):
                # Combine point data with metric metadata
                point_data = {
                    "metric_type": metric_type,
                    "metric_labels": ts.get("metric", {}).get("labels", {}),
                    "resource": ts.get("resource", {}),
                    "timestamp": point.get("interval", {}).get("end_time"),
                    "value": point.get("value", {})
                }
                results.append(point_data)
        
        return results
    
    def get_metric_statistics(
        self,
        metric_type: str,
        metric_labels: Optional[Dict[str, str]] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None
    ) -> Dict[str, Any]:
        """Gets statistical information about a metric.
        
        Args:
            metric_type: Type of metric to analyze
            metric_labels: Metric labels to filter by
            start_time: Start time for the query interval (defaults to 1 hour ago)
            end_time: End time for the query interval (defaults to now)
            
        Returns:
            Dictionary of metric statistics
        """
        # Get metric data points
        data_points = self.get_metric_data(
            metric_type=metric_type,
            metric_labels=metric_labels,
            start_time=start_time,
            end_time=end_time
        )
        
        if not data_points:
            return {
                "count": 0,
                "min": None,
                "max": None,
                "sum": None,
                "avg": None,
                "first": None,
                "last": None
            }
        
        # Extract values
        values = []
        for point in data_points:
            value_data = point.get("value", {})
            
            # Extract value based on type
            if "int64_value" in value_data:
                values.append(value_data["int64_value"])
            elif "double_value" in value_data:
                values.append(value_data["double_value"])
            elif "bool_value" in value_data:
                values.append(1 if value_data["bool_value"] else 0)
            elif "string_value" in value_data:
                # Can't compute stats on string values
                continue
        
        if not values:
            return {
                "count": 0,
                "min": None,
                "max": None,
                "sum": None,
                "avg": None,
                "first": None,
                "last": None
            }
        
        # Calculate statistics
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "sum": sum(values),
            "avg": sum(values) / len(values),
            "first": values[0],
            "last": values[-1]
        }
    
    def create_gauge_metric(
        self,
        metric_type: str,
        value: Any,
        labels: Optional[Dict[str, str]] = None,
        resource_labels: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime.datetime] = None
    ) -> bool:
        """Creates a gauge metric (point-in-time value).
        
        Args:
            metric_type: Type of metric (will be prefixed with domain if needed)
            value: Value for the data point
            labels: Metric labels
            resource_labels: Resource labels
            timestamp: Timestamp for the data point (defaults to now)
            
        Returns:
            True if successful, False otherwise
        """
        value_type = determine_value_type(value)
        return self.create_time_series(
            metric_type=metric_type,
            value=value,
            labels=labels,
            resource_labels=resource_labels,
            timestamp=timestamp,
            metric_kind=METRIC_KIND_GAUGE,
            value_type=value_type
        )
    
    def create_cumulative_metric(
        self,
        metric_type: str,
        value: Any,
        labels: Optional[Dict[str, str]] = None,
        resource_labels: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime.datetime] = None,
        start_time: Optional[datetime.datetime] = None
    ) -> bool:
        """Creates a cumulative metric (monotonically increasing value).
        
        Args:
            metric_type: Type of metric (will be prefixed with domain if needed)
            value: Value for the data point
            labels: Metric labels
            resource_labels: Resource labels
            timestamp: Timestamp for the data point (defaults to now)
            start_time: Start time for the cumulative metric (required)
            
        Returns:
            True if successful, False otherwise
        """
        # Ensure start_time is provided (required for cumulative metrics)
        if start_time is None:
            raise ValueError("start_time is required for cumulative metrics")
        
        value_type = determine_value_type(value)
        
        # Create TimeSeries object with start_time
        time_series = TimeSeries(
            metric_type=metric_type,
            value=value,
            metric_labels=labels or {},
            resource_labels=resource_labels or {},
            timestamp=timestamp,
            metric_kind=METRIC_KIND_CUMULATIVE,
            value_type=value_type,
            resource_type=DEFAULT_RESOURCE_TYPE,
            start_time=start_time
        )
        
        # Convert to proto and create time series
        try:
            time_series_proto = time_series.to_proto()
            self._client.create_time_series(
                name=self._project_name,
                time_series=[time_series_proto]
            )
            logger.debug(f"Created cumulative time series data point for {metric_type}")
            return True
        except Exception as e:
            logger.error(f"Error creating cumulative time series for {metric_type}: {str(e)}")
            raise
    
    def _descriptor_to_dict(self, descriptor) -> Dict[str, Any]:
        """Converts a metric descriptor to a dictionary.
        
        Args:
            descriptor: MetricDescriptor object
            
        Returns:
            Dictionary representation of the descriptor
        """
        labels = []
        for label in descriptor.labels:
            labels.append({
                "key": label.key,
                "value_type": label.value_type,
                "description": label.description
            })
        
        return {
            "name": descriptor.name,
            "type": descriptor.type,
            "display_name": descriptor.display_name,
            "description": descriptor.description,
            "metric_kind": descriptor.metric_kind,
            "value_type": descriptor.value_type,
            "unit": descriptor.unit,
            "labels": labels
        }
    
    def _time_series_to_dict(self, time_series) -> Dict[str, Any]:
        """Converts a time series to a dictionary.
        
        Args:
            time_series: TimeSeries object
            
        Returns:
            Dictionary representation of the time series
        """
        # Extract metric information
        metric_dict = {
            "type": time_series.metric.type,
            "labels": {k: v for k, v in time_series.metric.labels.items()}
        }
        
        # Extract resource information
        resource_dict = {
            "type": time_series.resource.type,
            "labels": {k: v for k, v in time_series.resource.labels.items()}
        }
        
        # Extract points information
        points = []
        for point in time_series.points:
            # Extract value based on type
            value_dict = {}
            if hasattr(point.value, "int64_value") and point.value.int64_value:
                value_dict["int64_value"] = point.value.int64_value
            elif hasattr(point.value, "double_value") and point.value.double_value:
                value_dict["double_value"] = point.value.double_value
            elif hasattr(point.value, "bool_value"):
                value_dict["bool_value"] = point.value.bool_value
            elif hasattr(point.value, "string_value") and point.value.string_value:
                value_dict["string_value"] = point.value.string_value
            
            # Extract interval information
            interval_dict = {}
            if hasattr(point.interval, "end_time") and point.interval.end_time:
                end_dt = datetime.datetime.fromtimestamp(
                    point.interval.end_time.seconds + point.interval.end_time.nanos / 10**9, 
                    datetime.timezone.utc
                )
                interval_dict["end_time"] = end_dt.isoformat()
            
            if hasattr(point.interval, "start_time") and point.interval.start_time:
                start_dt = datetime.datetime.fromtimestamp(
                    point.interval.start_time.seconds + point.interval.start_time.nanos / 10**9, 
                    datetime.timezone.utc
                )
                interval_dict["start_time"] = start_dt.isoformat()
            
            points.append({
                "value": value_dict,
                "interval": interval_dict
            })
        
        return {
            "metric": metric_dict,
            "resource": resource_dict,
            "metric_kind": time_series.metric_kind,
            "value_type": time_series.value_type,
            "points": points
        }


class TimeSeries:
    """Helper class for creating time series data points.
    
    This class encapsulates the creation of time series data for Cloud Monitoring,
    abstracting away the complexities of the protocol buffer formats.
    """
    
    def __init__(
        self,
        metric_type: str,
        value: Any,
        metric_labels: Dict[str, str] = None,
        resource_labels: Dict[str, str] = None,
        timestamp: Optional[datetime.datetime] = None,
        metric_kind: str = METRIC_KIND_GAUGE,
        value_type: Optional[str] = None,
        resource_type: str = DEFAULT_RESOURCE_TYPE,
        start_time: Optional[datetime.datetime] = None
    ):
        """Initializes a new TimeSeries instance.
        
        Args:
            metric_type: Type of metric
            value: Value for the data point
            metric_labels: Metric labels
            resource_labels: Resource labels
            timestamp: Timestamp for the data point (defaults to now)
            metric_kind: Kind of metric (GAUGE, CUMULATIVE)
            value_type: Type of value (will be determined automatically if None)
            resource_type: Type of monitored resource
            start_time: Start time for cumulative metrics
        """
        self.metric_type = metric_type
        self.value = value
        self.metric_labels = metric_labels or {}
        self.resource_labels = resource_labels or {}
        self.timestamp = timestamp or datetime.datetime.now(datetime.timezone.utc)
        self.metric_kind = metric_kind
        self.value_type = value_type or determine_value_type(value)
        self.resource_type = resource_type
        self.start_time = start_time
        
        # Validate required parameters
        if metric_kind == METRIC_KIND_CUMULATIVE and start_time is None:
            raise ValueError("start_time is required for cumulative metrics")
    
    def to_proto(self) -> TimeSeriesProto:
        """Converts the TimeSeries to a protobuf object for the API.
        
        Returns:
            TimeSeries protobuf object
        """
        # Create Metric object with type and labels
        metric = Metric(
            type=self.metric_type,
            labels=self.metric_labels
        )
        
        # Create Resource object with type and labels
        resource = MonitoredResource(
            type=self.resource_type,
            labels=self.resource_labels
        )
        
        # Create Point with value and interval
        point_value = TypedValue()
        
        # Set the appropriate value based on value_type
        if self.value_type == VALUE_TYPE_INT64:
            point_value.int64_value = int(self.value)
        elif self.value_type == VALUE_TYPE_DOUBLE:
            point_value.double_value = float(self.value)
        elif self.value_type == VALUE_TYPE_BOOL:
            point_value.bool_value = bool(self.value)
        else:  # VALUE_TYPE_STRING
            point_value.string_value = str(self.value)
        
        # Create interval
        if self.metric_kind == METRIC_KIND_CUMULATIVE:
            # For cumulative, both start and end time are required
            interval = TimeInterval(
                start_time=create_timestamp(self.start_time),
                end_time=create_timestamp(self.timestamp)
            )
        else:
            # For gauge, only end time is required
            interval = TimeInterval(
                end_time=create_timestamp(self.timestamp)
            )
        
        point = Point(
            value=point_value,
            interval=interval
        )
        
        # Create TimeSeries object
        return TimeSeriesProto(
            metric=metric,
            resource=resource,
            metric_kind=self.metric_kind,
            value_type=self.value_type,
            points=[point]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeSeries':
        """Creates a TimeSeries instance from a dictionary.
        
        Args:
            data: Dictionary with TimeSeries data
            
        Returns:
            TimeSeries instance
        """
        # Parse timestamps if present as strings
        timestamp = data.get('timestamp')
        start_time = data.get('start_time')
        
        if isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp)
        
        if isinstance(start_time, str):
            start_time = datetime.datetime.fromisoformat(start_time)
        
        return cls(
            metric_type=data['metric_type'],
            value=data['value'],
            metric_labels=data.get('metric_labels', {}),
            resource_labels=data.get('resource_labels', {}),
            timestamp=timestamp,
            metric_kind=data.get('metric_kind', METRIC_KIND_GAUGE),
            value_type=data.get('value_type'),
            resource_type=data.get('resource_type', DEFAULT_RESOURCE_TYPE),
            start_time=start_time
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts the TimeSeries to a dictionary representation.
        
        Returns:
            Dictionary representation of the TimeSeries
        """
        result = {
            'metric_type': self.metric_type,
            'value': self.value,
            'metric_labels': self.metric_labels,
            'resource_labels': self.resource_labels,
            'timestamp': self.timestamp.isoformat(),
            'metric_kind': self.metric_kind,
            'value_type': self.value_type,
            'resource_type': self.resource_type
        }
        
        if self.start_time:
            result['start_time'] = self.start_time.isoformat()
        
        return result