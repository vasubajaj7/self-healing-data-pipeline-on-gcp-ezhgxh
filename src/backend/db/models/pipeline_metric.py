"""
Model class for pipeline performance and operational metrics.

This module defines the PipelineMetric class used for tracking, analyzing,
and visualizing pipeline performance metrics to support monitoring,
alerting, and self-healing capabilities.
"""

import datetime
import json
import uuid
import enum
from typing import Dict, Any, Optional, List, Union

from ...constants import (
    METRIC_TYPE_GAUGE,
    METRIC_TYPE_COUNTER,
    METRIC_TYPE_HISTOGRAM,
    METRIC_TYPE_SUMMARY
)
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Initialize logger
logger = get_logger(__name__)

# Define the table name constant
PIPELINE_METRIC_TABLE_NAME = "pipeline_metrics"


def generate_metric_id() -> str:
    """
    Generates a unique identifier for a pipeline metric.
    
    Returns:
        str: Unique metric ID with 'metric_' prefix
    """
    return f"metric_{str(uuid.uuid4())}"


def get_pipeline_metric_table_schema() -> List[SchemaField]:
    """
    Returns the BigQuery table schema for the pipeline metrics table.
    
    Returns:
        list: List of SchemaField objects defining the table schema
    """
    return [
        get_schema_field("metric_id", "STRING", "REQUIRED", "Unique identifier for the metric"),
        get_schema_field("execution_id", "STRING", "REQUIRED", "Pipeline execution ID this metric belongs to"),
        get_schema_field("metric_name", "STRING", "REQUIRED", "Name of the metric"),
        get_schema_field("metric_category", "STRING", "REQUIRED", "Category of the metric (PERFORMANCE, RESOURCE_UTILIZATION, etc.)"),
        get_schema_field("metric_type", "STRING", "REQUIRED", "Type of metric (GAUGE, COUNTER, HISTOGRAM, SUMMARY)"),
        get_schema_field("metric_value", "STRING", "REQUIRED", "Value of the metric (stored as string and parsed based on context)"),
        get_schema_field("timestamp", "TIMESTAMP", "REQUIRED", "Time when the metric was recorded"),
        get_schema_field("labels", "STRING", "NULLABLE", "JSON string of additional labels/dimensions for the metric"),
        get_schema_field("description", "STRING", "NULLABLE", "Description of the metric"),
        get_schema_field("unit", "STRING", "NULLABLE", "Unit of measurement for the metric")
    ]


class MetricCategory(enum.Enum):
    """
    Enumeration of metric categories for pipeline metrics.
    """
    PERFORMANCE = "PERFORMANCE"
    RESOURCE_UTILIZATION = "RESOURCE_UTILIZATION"
    DATA_QUALITY = "DATA_QUALITY"
    THROUGHPUT = "THROUGHPUT"
    RELIABILITY = "RELIABILITY"
    COST = "COST"


class PipelineMetric:
    """
    Model class representing a performance or operational metric for a pipeline execution.
    
    This class provides a structured way to store and manage pipeline metrics
    with support for different metric types, categories, and metadata.
    """
    
    def __init__(
        self,
        execution_id: str,
        metric_name: str,
        metric_value: Any,
        metric_type: str = METRIC_TYPE_GAUGE,
        metric_category: MetricCategory = MetricCategory.PERFORMANCE,
        labels: Dict[str, str] = None,
        description: str = None,
        unit: str = None
    ):
        """
        Initialize a new pipeline metric with provided parameters.
        
        Args:
            execution_id: ID of the pipeline execution this metric belongs to
            metric_name: Name of the metric
            metric_value: Value of the metric (can be number, string, boolean, etc.)
            metric_type: Type of metric (GAUGE, COUNTER, HISTOGRAM, SUMMARY)
            metric_category: Category of the metric
            labels: Additional labels/dimensions for the metric
            description: Description of the metric
            unit: Unit of measurement for the metric
        """
        self.metric_id = generate_metric_id()
        self.execution_id = execution_id
        self.metric_name = metric_name
        self.metric_category = metric_category
        self.metric_type = metric_type
        self.metric_value = metric_value
        self.timestamp = datetime.datetime.now()
        self.labels = labels or {}
        self.description = description
        self.unit = unit
        
        logger.debug(
            f"Created new pipeline metric: {self.metric_name}={self.metric_value} "
            f"[{self.metric_type}, {self.metric_category}, execution_id={self.execution_id}]"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the pipeline metric to a dictionary representation.
        
        Returns:
            dict: Dictionary representation of the pipeline metric
        """
        return {
            'metric_id': self.metric_id,
            'execution_id': self.execution_id,
            'metric_name': self.metric_name,
            'metric_category': self.metric_category.value if isinstance(self.metric_category, MetricCategory) else self.metric_category,
            'metric_type': self.metric_type,
            'metric_value': self.metric_value,
            'timestamp': self.timestamp.isoformat() if isinstance(self.timestamp, datetime.datetime) else self.timestamp,
            'labels': self.labels,
            'description': self.description,
            'unit': self.unit
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineMetric':
        """
        Create a PipelineMetric instance from a dictionary.
        
        Args:
            data: Dictionary containing pipeline metric data
            
        Returns:
            PipelineMetric: New PipelineMetric instance
        """
        # Extract required parameters
        execution_id = data.get('execution_id')
        metric_name = data.get('metric_name')
        metric_value = data.get('metric_value')
        
        # Handle MetricCategory conversion
        if 'metric_category' in data:
            category_str = data.get('metric_category')
            if isinstance(category_str, str):
                try:
                    metric_category = MetricCategory[category_str]
                except KeyError:
                    # Default if string doesn't match enum
                    metric_category = MetricCategory.PERFORMANCE
            else:
                metric_category = category_str or MetricCategory.PERFORMANCE
        else:
            metric_category = MetricCategory.PERFORMANCE
        
        # Create the basic instance
        instance = cls(
            execution_id=execution_id,
            metric_name=metric_name,
            metric_value=metric_value,
            metric_type=data.get('metric_type', METRIC_TYPE_GAUGE),
            metric_category=metric_category,
            labels=data.get('labels', {}),
            description=data.get('description'),
            unit=data.get('unit')
        )
        
        # Set additional properties directly
        if 'metric_id' in data:
            instance.metric_id = data['metric_id']
        
        if 'timestamp' in data:
            timestamp = data['timestamp']
            if isinstance(timestamp, str):
                try:
                    instance.timestamp = datetime.datetime.fromisoformat(timestamp)
                except (ValueError, TypeError):
                    instance.timestamp = datetime.datetime.now()
            else:
                instance.timestamp = timestamp
        
        return instance
    
    @classmethod
    def from_bigquery_row(cls, row: Dict[str, Any]) -> 'PipelineMetric':
        """
        Create a PipelineMetric instance from a BigQuery row.
        
        Args:
            row: BigQuery row data
            
        Returns:
            PipelineMetric: New PipelineMetric instance
        """
        # Convert labels from string if needed
        labels = row.get('labels')
        if labels and isinstance(labels, str):
            try:
                labels = json.loads(labels)
            except json.JSONDecodeError:
                labels = {}
        
        # Create data dictionary
        data = {
            'metric_id': row.get('metric_id'),
            'execution_id': row.get('execution_id'),
            'metric_name': row.get('metric_name'),
            'metric_category': row.get('metric_category'),
            'metric_type': row.get('metric_type'),
            'metric_value': row.get('metric_value'),
            'timestamp': row.get('timestamp'),
            'labels': labels,
            'description': row.get('description'),
            'unit': row.get('unit')
        }
        
        return cls.from_dict(data)
    
    def to_bigquery_row(self) -> Dict[str, Any]:
        """
        Convert the pipeline metric to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        metric_dict = self.to_dict()
        
        # Convert labels to JSON string for BigQuery
        if 'labels' in metric_dict and metric_dict['labels']:
            metric_dict['labels'] = json.dumps(metric_dict['labels'])
        
        return metric_dict
    
    def update_value(self, new_value: Any) -> None:
        """
        Update the value of the pipeline metric.
        
        Args:
            new_value: New value for the metric
        """
        self.metric_value = new_value
        self.timestamp = datetime.datetime.now()
        logger.debug(f"Updated metric {self.metric_name} value to {new_value}")
    
    def update_labels(self, new_labels: Dict[str, str]) -> None:
        """
        Update the labels for the pipeline metric.
        
        Args:
            new_labels: New labels to merge with existing
        """
        self.labels.update(new_labels)
        logger.debug(f"Updated labels for metric {self.metric_name}: {new_labels}")
    
    def get_value_type(self) -> str:
        """
        Get the data type of the metric value.
        
        Returns:
            str: String representation of the value type
        """
        return type(self.metric_value).__name__
    
    def is_numeric(self) -> bool:
        """
        Check if the metric value is numeric (int or float).
        
        Returns:
            bool: True if value is numeric, False otherwise
        """
        return isinstance(self.metric_value, (int, float))
    
    def get_age(self) -> float:
        """
        Get the age of the metric in seconds.
        
        Returns:
            float: Age in seconds
        """
        if not isinstance(self.timestamp, datetime.datetime):
            return 0
        
        delta = datetime.datetime.now() - self.timestamp
        return delta.total_seconds()