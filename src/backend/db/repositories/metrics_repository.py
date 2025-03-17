"""
Repository for managing pipeline metrics in BigQuery.

This module provides the MetricsRepository class for storing, retrieving, and analyzing
performance and operational metrics for the self-healing data pipeline. It supports
various metric types, time-series analysis, and aggregation functions to enable
monitoring, alerting, and self-healing capabilities.
"""

import datetime
import json
from typing import Dict, List, Any, Optional, Union, Tuple

import pandas as pd  # version 2.0.0+

from ...constants import (
    METRIC_TYPE_GAUGE,
    METRIC_TYPE_COUNTER,
    METRIC_TYPE_HISTOGRAM,
    METRIC_TYPE_SUMMARY
)
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.bigquery_client import BigQueryClient
from ..models.pipeline_metric import (
    PipelineMetric, 
    MetricCategory,
    get_pipeline_metric_table_schema,
    PIPELINE_METRIC_TABLE_NAME
)

# Initialize logger
logger = get_logger(__name__)

class MetricsRepository:
    """Repository for managing pipeline metrics in BigQuery"""
    
    def __init__(
        self, 
        bq_client: BigQueryClient,
        dataset_id: str = None,
        project_id: str = None
    ):
        """
        Initializes the MetricsRepository with BigQuery client and configuration.
        
        Args:
            bq_client: BigQuery client instance
            dataset_id: BigQuery dataset ID, defaults to config value if not provided
            project_id: Google Cloud project ID, defaults to config value if not provided
        """
        self._bq_client = bq_client
        
        # Get configuration if not provided
        config = get_config()
        self._dataset_id = dataset_id or config.get_bigquery_dataset()
        self._project_id = project_id or config.get_gcp_project_id()
        
        # Ensure metrics table exists
        self.ensure_table_exists()
        
        logger.info(
            f"Initialized MetricsRepository with dataset '{self._dataset_id}' "
            f"in project '{self._project_id}'"
        )
    
    def ensure_table_exists(self) -> bool:
        """
        Ensures the pipeline metrics table exists in BigQuery, creating it if necessary.
        
        Returns:
            bool: True if table exists or was created successfully
        """
        # Check if table exists
        table_exists = self._bq_client.table_exists(
            self._dataset_id, 
            PIPELINE_METRIC_TABLE_NAME
        )
        
        if not table_exists:
            logger.info(f"Creating metrics table {PIPELINE_METRIC_TABLE_NAME}")
            
            # Get the table schema from the model
            schema = get_pipeline_metric_table_schema()
            
            # Create the table
            created = self._bq_client.create_table(
                self._dataset_id,
                PIPELINE_METRIC_TABLE_NAME,
                schema,
                time_partitioning_field="timestamp",
                clustering_fields=["metric_category", "metric_name"],
                description="Pipeline performance and operational metrics"
            )
            
            if created:
                logger.info(f"Created metrics table {PIPELINE_METRIC_TABLE_NAME}")
                return True
            else:
                logger.error(f"Failed to create metrics table {PIPELINE_METRIC_TABLE_NAME}")
                return False
        
        logger.debug(f"Metrics table {PIPELINE_METRIC_TABLE_NAME} already exists")
        return True
    
    def create_metric(self, metric: PipelineMetric) -> str:
        """
        Creates a new metric record in the database.
        
        Args:
            metric: PipelineMetric object to create
            
        Returns:
            str: ID of the created metric record
        """
        # Validate metric has required fields
        if not metric.execution_id or not metric.metric_name:
            logger.error("Cannot create metric: missing required fields")
            raise ValueError("Metric requires execution_id and metric_name")
        
        # Convert to BigQuery row format
        row = metric.to_bigquery_row()
        
        # Insert into BigQuery
        inserted = self._bq_client.insert_rows(
            self._dataset_id,
            PIPELINE_METRIC_TABLE_NAME,
            [row]
        )
        
        if inserted:
            logger.info(
                f"Created metric {metric.metric_id}: {metric.metric_name} "
                f"for execution {metric.execution_id}"
            )
            return metric.metric_id
        else:
            logger.error(f"Failed to create metric: {metric.metric_name}")
            raise RuntimeError(f"Failed to create metric: {metric.metric_name}")
    
    def batch_create_metrics(self, metrics: List[PipelineMetric]) -> List[str]:
        """
        Creates multiple metric records in a single batch operation.
        
        Args:
            metrics: List of PipelineMetric objects to create
            
        Returns:
            list: List of created metric IDs
        """
        if not metrics:
            logger.warning("No metrics provided for batch creation")
            return []
        
        # Validate all metrics have required fields
        for metric in metrics:
            if not metric.execution_id or not metric.metric_name:
                logger.error("Cannot create metric: missing required fields")
                raise ValueError("All metrics require execution_id and metric_name")
        
        # Convert all metrics to BigQuery row format
        rows = [metric.to_bigquery_row() for metric in metrics]
        
        # Insert batch into BigQuery
        inserted = self._bq_client.insert_rows(
            self._dataset_id,
            PIPELINE_METRIC_TABLE_NAME,
            rows
        )
        
        if inserted:
            logger.info(f"Created {len(metrics)} metrics in batch")
            return [metric.metric_id for metric in metrics]
        else:
            logger.error(f"Failed to create {len(metrics)} metrics in batch")
            raise RuntimeError(f"Failed to create metrics in batch")
    
    def get_metric(self, metric_id: str) -> Optional[PipelineMetric]:
        """
        Retrieves a metric by its ID.
        
        Args:
            metric_id: Unique identifier of the metric
            
        Returns:
            PipelineMetric: PipelineMetric object if found, None otherwise
        """
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE metric_id = '{metric_id}'
        LIMIT 1
        """
        
        results = self._bq_client.query(query)
        
        if not results or len(results) == 0:
            logger.warning(f"Metric not found with ID: {metric_id}")
            return None
        
        # Convert the first result to a PipelineMetric object
        metric = PipelineMetric.from_bigquery_row(results[0])
        logger.debug(f"Retrieved metric: {metric_id}")
        
        return metric
    
    def update_metric(self, metric: PipelineMetric) -> bool:
        """
        Updates an existing metric record in the database.
        
        Args:
            metric: PipelineMetric object with updated values
            
        Returns:
            bool: True if update was successful
        """
        # Validate metric has ID and required fields
        if not metric.metric_id or not metric.execution_id or not metric.metric_name:
            logger.error("Cannot update metric: missing required fields")
            raise ValueError("Metric requires metric_id, execution_id, and metric_name")
        
        # Convert to BigQuery row format
        row = metric.to_bigquery_row()
        
        # Construct update query
        # Note: BigQuery doesn't support traditional updates, so we delete and insert
        # First, check if the metric exists
        existing_metric = self.get_metric(metric.metric_id)
        
        if not existing_metric:
            logger.warning(f"Cannot update: metric not found with ID: {metric.metric_id}")
            return False
        
        # Delete existing record
        delete_query = f"""
        DELETE FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE metric_id = '{metric.metric_id}'
        """
        
        deleted = self._bq_client.execute_query(delete_query)
        
        if not deleted:
            logger.error(f"Failed to delete existing metric: {metric.metric_id}")
            return False
        
        # Insert updated record
        inserted = self._bq_client.insert_rows(
            self._dataset_id,
            PIPELINE_METRIC_TABLE_NAME,
            [row]
        )
        
        if inserted:
            logger.info(f"Updated metric: {metric.metric_id}")
            return True
        else:
            logger.error(f"Failed to update metric: {metric.metric_id}")
            return False
    
    def get_metrics_by_execution_id(
        self, 
        execution_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineMetric]:
        """
        Retrieves metrics for a specific pipeline execution.
        
        Args:
            execution_id: Pipeline execution ID
            limit: Maximum number of metrics to return
            offset: Number of metrics to skip
            
        Returns:
            list: List of PipelineMetric objects for the execution
        """
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE execution_id = '{execution_id}'
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query)
        
        metrics = [PipelineMetric.from_bigquery_row(row) for row in results]
        logger.info(f"Retrieved {len(metrics)} metrics for execution: {execution_id}")
        
        return metrics
    
    def get_metrics_by_category(
        self, 
        category: MetricCategory,
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineMetric]:
        """
        Retrieves metrics filtered by category.
        
        Args:
            category: MetricCategory enum value
            limit: Maximum number of metrics to return
            offset: Number of metrics to skip
            
        Returns:
            list: List of PipelineMetric objects matching the category
        """
        # Convert enum to string if needed
        category_str = category.value if isinstance(category, MetricCategory) else category
        
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE metric_category = '{category_str}'
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query)
        
        metrics = [PipelineMetric.from_bigquery_row(row) for row in results]
        logger.info(f"Retrieved {len(metrics)} metrics for category: {category_str}")
        
        return metrics
    
    def get_metrics_by_name(
        self, 
        metric_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineMetric]:
        """
        Retrieves metrics filtered by metric name.
        
        Args:
            metric_name: Name of the metric
            limit: Maximum number of metrics to return
            offset: Number of metrics to skip
            
        Returns:
            list: List of PipelineMetric objects matching the name
        """
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE metric_name = '{metric_name}'
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query)
        
        metrics = [PipelineMetric.from_bigquery_row(row) for row in results]
        logger.info(f"Retrieved {len(metrics)} metrics for name: {metric_name}")
        
        return metrics
    
    def get_metrics_by_time_range(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineMetric]:
        """
        Retrieves metrics within a specific time range.
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            limit: Maximum number of metrics to return
            offset: Number of metrics to skip
            
        Returns:
            list: List of PipelineMetric objects in the time range
        """
        # Format datetime objects for BigQuery
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE timestamp BETWEEN '{start_str}' AND '{end_str}'
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query)
        
        metrics = [PipelineMetric.from_bigquery_row(row) for row in results]
        logger.info(
            f"Retrieved {len(metrics)} metrics between {start_str} and {end_str}"
        )
        
        return metrics
    
    def get_latest_metric_value(
        self, 
        metric_name: str,
        labels: Dict[str, str] = None
    ) -> Any:
        """
        Retrieves the latest value for a specific metric name.
        
        Args:
            metric_name: Name of the metric
            labels: Optional dictionary of labels to filter by
            
        Returns:
            object: Latest metric value or None if not found
        """
        # Build query with optional label filtering
        query = f"""
        SELECT metric_value, labels, timestamp
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE metric_name = '{metric_name}'
        """
        
        # Add label filtering if provided
        if labels and len(labels) > 0:
            # Convert labels to JSON for comparison
            labels_json = json.dumps(labels)
            query += f" AND JSON_EXTRACT(labels, '$') = '{labels_json}'"
        
        query += """
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        results = self._bq_client.query(query)
        
        if not results or len(results) == 0:
            logger.warning(f"No metrics found for name: {metric_name}")
            return None
        
        # Extract the metric value from the result
        metric_value = results[0]['metric_value']
        
        # Try to parse numeric values
        try:
            # Try to convert to int or float if appropriate
            if metric_value.isdigit():
                return int(metric_value)
            try:
                return float(metric_value)
            except ValueError:
                pass
            
            # Try to parse as JSON
            try:
                return json.loads(metric_value)
            except json.JSONDecodeError:
                pass
            
            # Return as string if no conversion applies
            return metric_value
        except (AttributeError, TypeError):
            # Return as-is if conversion fails
            return metric_value
    
    def get_metric_time_series(
        self,
        metric_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        labels: Dict[str, str] = None,
        aggregation: str = 'avg'
    ) -> pd.DataFrame:
        """
        Retrieves time series data for a specific metric.
        
        Args:
            metric_name: Name of the metric
            start_time: Start of the time range
            end_time: End of the time range
            labels: Optional dictionary of labels to filter by
            aggregation: Aggregation function ('avg', 'sum', 'min', 'max', 'count')
            
        Returns:
            pandas.DataFrame: DataFrame with time series data
        """
        # Validate aggregation parameter
        valid_aggregations = ['avg', 'sum', 'min', 'max', 'count']
        if aggregation not in valid_aggregations:
            logger.warning(f"Invalid aggregation: {aggregation}, using 'avg'")
            aggregation = 'avg'
        
        # Map aggregation to SQL function
        agg_functions = {
            'avg': 'AVG',
            'sum': 'SUM',
            'min': 'MIN',
            'max': 'MAX',
            'count': 'COUNT'
        }
        agg_function = agg_functions[aggregation]
        
        # Format datetime objects for BigQuery
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        
        # Build base query
        query = f"""
        SELECT
            TIMESTAMP_TRUNC(timestamp, HOUR) as time_bucket,
            {agg_function}(CAST(metric_value AS FLOAT64)) as value
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE 
            metric_name = '{metric_name}'
            AND timestamp BETWEEN '{start_str}' AND '{end_str}'
        """
        
        # Add label filtering if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        # Complete the query
        query += """
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        
        results = self._bq_client.query(query)
        
        if not results:
            logger.warning(f"No time series data found for metric: {metric_name}")
            return pd.DataFrame(columns=['timestamp', 'value'])
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Rename columns
        if 'time_bucket' in df.columns:
            df = df.rename(columns={'time_bucket': 'timestamp'})
        
        # Set timestamp as index
        if 'timestamp' in df.columns:
            df = df.set_index('timestamp')
        
        logger.info(
            f"Retrieved time series for {metric_name} with {len(df)} data points"
        )
        
        return df
    
    def get_metric_statistics(
        self,
        metric_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        labels: Dict[str, str] = None
    ) -> Dict[str, float]:
        """
        Calculates statistics for a specific metric.
        
        Args:
            metric_name: Name of the metric
            start_time: Start of the time range
            end_time: End of the time range
            labels: Optional dictionary of labels to filter by
            
        Returns:
            dict: Dictionary with metric statistics
        """
        # Format datetime objects for BigQuery
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        
        # Build query
        query = f"""
        SELECT
            COUNT(*) as count,
            AVG(CAST(metric_value AS FLOAT64)) as avg,
            MIN(CAST(metric_value AS FLOAT64)) as min,
            MAX(CAST(metric_value AS FLOAT64)) as max,
            STDDEV(CAST(metric_value AS FLOAT64)) as stddev
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE 
            metric_name = '{metric_name}'
            AND timestamp BETWEEN '{start_str}' AND '{end_str}'
        """
        
        # Add label filtering if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        results = self._bq_client.query(query)
        
        if not results or len(results) == 0:
            logger.warning(f"No statistics available for metric: {metric_name}")
            return {
                'count': 0,
                'avg': None,
                'min': None,
                'max': None,
                'stddev': None
            }
        
        # Extract statistics
        stats = results[0]
        
        # Return statistics as dictionary
        return {
            'count': stats.get('count', 0),
            'avg': stats.get('avg'),
            'min': stats.get('min'),
            'max': stats.get('max'),
            'stddev': stats.get('stddev')
        }
    
    def get_metric_percentiles(
        self,
        metric_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        percentiles: List[int] = None,
        labels: Dict[str, str] = None
    ) -> Dict[int, float]:
        """
        Calculates percentile values for a specific metric.
        
        Args:
            metric_name: Name of the metric
            start_time: Start of the time range
            end_time: End of the time range
            percentiles: List of percentile values to calculate (0-100)
            labels: Optional dictionary of labels to filter by
            
        Returns:
            dict: Dictionary with percentile values
        """
        # Default percentiles if not provided
        if not percentiles:
            percentiles = [50, 90, 95, 99]
        
        # Validate percentiles are within range
        for p in percentiles:
            if p < 0 or p > 100:
                logger.warning(f"Invalid percentile: {p}, must be between 0 and 100")
                return {}
        
        # Format datetime objects for BigQuery
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        
        # Build query
        query = f"""
        SELECT
            APPROX_QUANTILES(CAST(metric_value AS FLOAT64), 100) as quantiles
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE 
            metric_name = '{metric_name}'
            AND timestamp BETWEEN '{start_str}' AND '{end_str}'
        """
        
        # Add label filtering if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        results = self._bq_client.query(query)
        
        if not results or len(results) == 0 or 'quantiles' not in results[0]:
            logger.warning(f"No percentile data available for metric: {metric_name}")
            return {p: None for p in percentiles}
        
        # Extract quantiles
        quantiles = results[0]['quantiles']
        
        # Calculate requested percentiles
        percentile_values = {}
        for p in percentiles:
            if not quantiles or len(quantiles) < 100:
                percentile_values[p] = None
            else:
                # Convert percentile to index in quantiles array
                idx = min(p, 99)  # Ensure within bounds
                percentile_values[p] = quantiles[idx]
        
        return percentile_values
    
    def compare_metric_periods(
        self,
        metric_name: str,
        period1_start: datetime.datetime,
        period1_end: datetime.datetime,
        period2_start: datetime.datetime,
        period2_end: datetime.datetime,
        labels: Dict[str, str] = None,
        aggregation: str = 'avg'
    ) -> Dict[str, Any]:
        """
        Compares metric values between two time periods.
        
        Args:
            metric_name: Name of the metric
            period1_start: Start of first period
            period1_end: End of first period
            period2_start: Start of second period
            period2_end: End of second period
            labels: Optional dictionary of labels to filter by
            aggregation: Aggregation function ('avg', 'sum', 'min', 'max', 'count')
            
        Returns:
            dict: Dictionary with comparison results
        """
        # Validate aggregation parameter
        valid_aggregations = ['avg', 'sum', 'min', 'max', 'count']
        if aggregation not in valid_aggregations:
            logger.warning(f"Invalid aggregation: {aggregation}, using 'avg'")
            aggregation = 'avg'
        
        # Map aggregation to SQL function
        agg_functions = {
            'avg': 'AVG',
            'sum': 'SUM',
            'min': 'MIN',
            'max': 'MAX',
            'count': 'COUNT'
        }
        agg_function = agg_functions[aggregation]
        
        # Format datetime objects for BigQuery
        period1_start_str = period1_start.isoformat()
        period1_end_str = period1_end.isoformat()
        period2_start_str = period2_start.isoformat()
        period2_end_str = period2_end.isoformat()
        
        # Build query for both periods
        query = f"""
        WITH period1 AS (
            SELECT
                {agg_function}(CAST(metric_value AS FLOAT64)) as value
            FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
            WHERE 
                metric_name = '{metric_name}'
                AND timestamp BETWEEN '{period1_start_str}' AND '{period1_end_str}'
        """
        
        # Add label filtering for period 1 if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        query += f"""
        ),
        period2 AS (
            SELECT
                {agg_function}(CAST(metric_value AS FLOAT64)) as value
            FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
            WHERE 
                metric_name = '{metric_name}'
                AND timestamp BETWEEN '{period2_start_str}' AND '{period2_end_str}'
        """
        
        # Add label filtering for period 2 if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        query += """
        )
        SELECT
            (SELECT value FROM period1) as period1_value,
            (SELECT value FROM period2) as period2_value
        """
        
        results = self._bq_client.query(query)
        
        if not results or len(results) == 0:
            logger.warning(f"No comparison data available for metric: {metric_name}")
            return {
                'period1_value': None,
                'period2_value': None,
                'absolute_change': None,
                'percentage_change': None
            }
        
        # Extract values
        period1_value = results[0].get('period1_value')
        period2_value = results[0].get('period2_value')
        
        # Calculate changes
        absolute_change = None
        percentage_change = None
        
        if period1_value is not None and period2_value is not None:
            absolute_change = period2_value - period1_value
            
            if period1_value != 0:
                percentage_change = (absolute_change / period1_value) * 100
        
        return {
            'period1_value': period1_value,
            'period2_value': period2_value,
            'absolute_change': absolute_change,
            'percentage_change': percentage_change
        }
    
    def get_metric_correlation(
        self,
        metric_name1: str,
        metric_name2: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        labels: Dict[str, str] = None
    ) -> float:
        """
        Calculates correlation between two metrics.
        
        Args:
            metric_name1: Name of first metric
            metric_name2: Name of second metric
            start_time: Start of the time range
            end_time: End of the time range
            labels: Optional dictionary of labels to filter by
            
        Returns:
            float: Correlation coefficient between the metrics
        """
        # Format datetime objects for BigQuery
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        
        # Build query to get time-aligned values for both metrics
        query = f"""
        WITH metric1 AS (
            SELECT
                TIMESTAMP_TRUNC(timestamp, HOUR) as time_bucket,
                AVG(CAST(metric_value AS FLOAT64)) as value1
            FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
            WHERE 
                metric_name = '{metric_name1}'
                AND timestamp BETWEEN '{start_str}' AND '{end_str}'
        """
        
        # Add label filtering for metric1 if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        query += f"""
            GROUP BY time_bucket
        ),
        metric2 AS (
            SELECT
                TIMESTAMP_TRUNC(timestamp, HOUR) as time_bucket,
                AVG(CAST(metric_value AS FLOAT64)) as value2
            FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
            WHERE 
                metric_name = '{metric_name2}'
                AND timestamp BETWEEN '{start_str}' AND '{end_str}'
        """
        
        # Add label filtering for metric2 if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        query += """
            GROUP BY time_bucket
        )
        SELECT
            m1.time_bucket,
            m1.value1,
            m2.value2
        FROM metric1 m1
        JOIN metric2 m2 ON m1.time_bucket = m2.time_bucket
        ORDER BY m1.time_bucket
        """
        
        results = self._bq_client.query(query)
        
        if not results or len(results) < 2:
            logger.warning(
                f"Insufficient data for correlation between {metric_name1} and {metric_name2}"
            )
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Calculate correlation
        correlation = df['value1'].corr(df['value2'])
        
        logger.info(
            f"Correlation between {metric_name1} and {metric_name2}: {correlation:.4f}"
        )
        
        return correlation
    
    def get_metric_anomalies(
        self,
        metric_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        threshold: float = 3.0,
        labels: Dict[str, str] = None
    ) -> pd.DataFrame:
        """
        Detects anomalies in metric values using statistical methods.
        
        Args:
            metric_name: Name of the metric
            start_time: Start of the time range
            end_time: End of the time range
            threshold: Number of standard deviations to consider as anomaly
            labels: Optional dictionary of labels to filter by
            
        Returns:
            pandas.DataFrame: DataFrame with detected anomalies
        """
        # Get time series data
        df = self.get_metric_time_series(
            metric_name,
            start_time,
            end_time,
            labels,
            aggregation='avg'
        )
        
        if df.empty or 'value' not in df.columns:
            logger.warning(f"No data available for anomaly detection on {metric_name}")
            return pd.DataFrame(columns=['timestamp', 'value', 'is_anomaly', 'deviation'])
        
        # Calculate moving average and standard deviation
        window_size = max(5, len(df) // 10)  # Dynamic window size
        df['rolling_mean'] = df['value'].rolling(window=window_size, min_periods=1).mean()
        df['rolling_std'] = df['value'].rolling(window=window_size, min_periods=1).std()
        
        # Mark values outside the threshold as anomalies
        df['deviation'] = (df['value'] - df['rolling_mean']) / df['rolling_std'].replace(0, 1)
        df['is_anomaly'] = df['deviation'].abs() > threshold
        
        # Count anomalies
        anomaly_count = df['is_anomaly'].sum()
        logger.info(
            f"Detected {anomaly_count} anomalies in {metric_name} "
            f"(threshold: {threshold} std dev)"
        )
        
        # Reset index to include timestamp as column
        df = df.reset_index()
        
        return df
    
    def get_metric_trends(
        self,
        metric_name: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        interval: str = 'hourly',
        labels: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Analyzes trends in metric values over time.
        
        Args:
            metric_name: Name of the metric
            start_time: Start of the time range
            end_time: End of the time range
            interval: Time interval for analysis ('hourly', 'daily', 'weekly')
            labels: Optional dictionary of labels to filter by
            
        Returns:
            dict: Dictionary with trend analysis results
        """
        # Get time series data
        df = self.get_metric_time_series(
            metric_name,
            start_time,
            end_time,
            labels,
            aggregation='avg'
        )
        
        if df.empty or 'value' not in df.columns:
            logger.warning(f"No data available for trend analysis on {metric_name}")
            return {
                'trend': 'unknown',
                'slope': None,
                'r_squared': None,
                'change_rate': None,
                'data_points': 0
            }
        
        # Reset index to include timestamp as column
        df = df.reset_index()
        
        # Resample data based on specified interval
        if interval == 'hourly':
            resampling = 'H'
        elif interval == 'daily':
            resampling = 'D'
        elif interval == 'weekly':
            resampling = 'W'
        else:
            logger.warning(f"Invalid interval: {interval}, using 'hourly'")
            resampling = 'H'
        
        if 'timestamp' in df.columns:
            df = df.set_index('timestamp')
            df = df.resample(resampling).mean().dropna()
            df = df.reset_index()
        
        # Need at least 2 points for trend analysis
        if len(df) < 2:
            logger.warning(f"Insufficient data for trend analysis on {metric_name}")
            return {
                'trend': 'unknown',
                'slope': None,
                'r_squared': None,
                'change_rate': None,
                'data_points': len(df)
            }
        
        try:
            # Calculate trend using linear regression
            import numpy as np
            from scipy import stats
            
            # Convert timestamps to numeric (seconds since epoch)
            if 'timestamp' in df.columns:
                x = np.array([(t - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') 
                              for t in df['timestamp']])
            else:
                x = np.arange(len(df))
                
            y = df['value'].values
            
            # Perform linear regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
            
            # Calculate R-squared
            r_squared = r_value ** 2
            
            # Determine trend direction
            if abs(slope) < 0.001 or r_squared < 0.3:
                trend = 'stable'
            elif slope > 0:
                trend = 'increasing'
            else:
                trend = 'decreasing'
            
            # Calculate average change rate (percentage)
            if len(df) >= 2:
                first_value = df['value'].iloc[0]
                last_value = df['value'].iloc[-1]
                
                if first_value != 0:
                    change_rate = ((last_value - first_value) / first_value) * 100
                else:
                    change_rate = None
            else:
                change_rate = None
            
            return {
                'trend': trend,
                'slope': slope,
                'r_squared': r_squared,
                'change_rate': change_rate,
                'data_points': len(df)
            }
            
        except Exception as e:
            logger.error(f"Error in trend analysis: {str(e)}")
            return {
                'trend': 'error',
                'slope': None,
                'r_squared': None,
                'change_rate': None,
                'data_points': len(df),
                'error': str(e)
            }
    
    def search_metrics(
        self,
        search_criteria: Dict[str, Any],
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineMetric]:
        """
        Searches metrics based on multiple criteria.
        
        Args:
            search_criteria: Dictionary of search parameters
            limit: Maximum number of metrics to return
            offset: Number of metrics to skip
            
        Returns:
            list: List of PipelineMetric objects matching criteria
        """
        # Extract search parameters
        execution_id = search_criteria.get('execution_id')
        metric_name = search_criteria.get('metric_name')
        metric_category = search_criteria.get('metric_category')
        metric_type = search_criteria.get('metric_type')
        start_time = search_criteria.get('start_time')
        end_time = search_criteria.get('end_time')
        labels = search_criteria.get('labels')
        
        # Build query with conditions
        query = f"""
        SELECT *
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE 1=1
        """
        
        # Add conditions for each provided parameter
        if execution_id:
            query += f" AND execution_id = '{execution_id}'"
        
        if metric_name:
            query += f" AND metric_name = '{metric_name}'"
        
        if metric_category:
            # Convert enum to string if needed
            category_str = metric_category.value if isinstance(metric_category, MetricCategory) else metric_category
            query += f" AND metric_category = '{category_str}'"
        
        if metric_type:
            query += f" AND metric_type = '{metric_type}'"
        
        if start_time:
            # Format datetime object for BigQuery
            start_str = start_time.isoformat() if isinstance(start_time, datetime.datetime) else start_time
            query += f" AND timestamp >= '{start_str}'"
        
        if end_time:
            # Format datetime object for BigQuery
            end_str = end_time.isoformat() if isinstance(end_time, datetime.datetime) else end_time
            query += f" AND timestamp <= '{end_str}'"
        
        # Add label filtering if provided
        if labels and len(labels) > 0:
            # Add each label as a separate condition
            for key, value in labels.items():
                query += f" AND JSON_EXTRACT(labels, '$.{key}') = '{value}'"
        
        # Add ordering and pagination
        query += f"""
        ORDER BY timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query)
        
        metrics = [PipelineMetric.from_bigquery_row(row) for row in results]
        logger.info(f"Search returned {len(metrics)} metrics")
        
        return metrics
    
    def delete_old_metrics(self, cutoff_date: datetime.datetime) -> int:
        """
        Deletes metrics older than a specified date.
        
        Args:
            cutoff_date: Metrics older than this date will be deleted
            
        Returns:
            int: Number of metrics deleted
        """
        # Format datetime object for BigQuery
        cutoff_str = cutoff_date.isoformat()
        
        # Count metrics to be deleted
        count_query = f"""
        SELECT COUNT(*) as count
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE timestamp < '{cutoff_str}'
        """
        
        count_results = self._bq_client.query(count_query)
        
        if not count_results or len(count_results) == 0:
            return 0
        
        count = count_results[0].get('count', 0)
        
        if count == 0:
            logger.info(f"No metrics to delete before {cutoff_str}")
            return 0
        
        # Delete metrics
        delete_query = f"""
        DELETE FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_METRIC_TABLE_NAME}`
        WHERE timestamp < '{cutoff_str}'
        """
        
        self._bq_client.execute_query(delete_query)
        
        logger.info(f"Deleted {count} metrics older than {cutoff_str}")
        return count