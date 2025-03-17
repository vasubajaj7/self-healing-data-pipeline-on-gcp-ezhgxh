# src/test/performance/backend/test_bigquery_query_perf.py
"""Performance tests for BigQuery query operations in the self-healing data pipeline.
This module measures and validates the performance of query execution, optimization techniques,
and ensures that query operations meet defined performance thresholds.
"""
import pytest  # package_name: pytest, package_version: 7.x.x, purpose: Testing framework for test fixtures and assertions
import time  # package_name: time, package_version: standard library, purpose: Measure execution time for performance tests
import pandas  # package_name: pandas, package_version: 2.0.x, purpose: Data manipulation for test datasets and query results
import numpy  # package_name: numpy, package_version: 1.24.x, purpose: Numerical operations for performance analysis
import statistics  # package_name: statistics, package_version: standard library, purpose: Statistical analysis of performance results

# Internal imports
from src.test.utils.bigquery_test_utils import BigQueryTestHelper, create_test_schema, generate_test_table_data  # Utilities for BigQuery testing with mock data and schemas
from src.test.utils.test_helpers import create_test_dataframe, generate_unique_id  # General test helper functions for data generation and resource management
from src.test.performance.conftest import performance_metrics_collector, performance_test_context, performance_thresholds, test_data_size, test_iterations, bigquery_performance_client  # Performance testing fixtures and utilities
from src.backend.utils.storage.bigquery_client import BigQueryClient  # Client for interacting with BigQuery
from src.backend.optimization.query.query_optimizer import QueryOptimizer, OPTIMIZATION_TECHNIQUES  # Query optimization implementation for testing

TEST_QUERIES = {
    'small': [
        'SELECT * FROM `{project}.{dataset}.{table}` LIMIT 100',
        'SELECT COUNT(*) FROM `{project}.{dataset}.{table}`',
        "SELECT {column1}, {column2} FROM `{project}.{dataset}.{table}` WHERE {column3} = 'test' LIMIT 100"
    ],
    'medium': [
        'SELECT {column1}, {column2}, COUNT(*) as count FROM `{project}.{dataset}.{table}` GROUP BY {column1}, {column2}',
        "SELECT * FROM `{project}.{dataset}.{table}` WHERE {column3} LIKE '%test%' AND {column4} > 100",
        'SELECT t1.{column1}, t2.{column2} FROM `{project}.{dataset}.{table}` t1 JOIN `{project}.{dataset}.{table2}` t2 ON t1.{column1} = t2.{column1} LIMIT 1000'
    ],
    'large': [
        "SELECT * FROM `{project}.{dataset}.{table}` WHERE {column3} LIKE '%test%' AND {column4} > 100 ORDER BY {column1} DESC",
        'SELECT t1.{column1}, t2.{column2}, t3.{column3}, COUNT(*) as count FROM `{project}.{dataset}.{table}` t1 JOIN `{project}.{dataset}.{table2}` t2 ON t1.{column1} = t2.{column1} JOIN `{project}.{dataset}.{table3}` t3 ON t2.{column2} = t3.{column2} GROUP BY t1.{column1}, t2.{column2}, t3.{column3}',
        'WITH subquery AS (SELECT {column1}, {column2}, {column3} FROM `{project}.{dataset}.{table}` WHERE {column4} > 100) SELECT s.{column1}, s.{column2}, t.{column3} FROM subquery s JOIN `{project}.{dataset}.{table2}` t ON s.{column1} = t.{column1} ORDER BY s.{column2} DESC'
    ]
}

TEST_SCHEMA = [
    {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'is_active', 'type': 'BOOLEAN', 'mode': 'REQUIRED'}
]

TEST_SCHEMA_LARGE = [
    {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'subcategory', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'rank', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'is_active', 'type': 'BOOLEAN', 'mode': 'REQUIRED'}
]

def setup_test_tables(bq_helper: BigQueryTestHelper, dataset_id: str, size: str) -> dict:
    """Sets up test tables in BigQuery for performance testing

    Args:
        bq_helper (BigQueryTestHelper): bq_helper
        dataset_id (str): dataset_id
        size (str): size

    Returns:
        dict: Dictionary containing test table references and column names
    """
    table_id = f'perf_test_table_{size}'
    table2_id = f'perf_test_table2_{size}'
    table3_id = f'perf_test_table3_{size}'

    num_rows = 1000
    if size == 'medium':
        num_rows = 10000
    elif size == 'large':
        num_rows = 100000

    schema = TEST_SCHEMA
    if size == 'large':
        schema = TEST_SCHEMA_LARGE

    table = bq_helper.create_temp_table(dataset_id=dataset_id, table_id=table_id, schema=schema, num_rows=num_rows)
    table2 = bq_helper.create_temp_table(dataset_id=dataset_id, table_id=table2_id, schema=schema, num_rows=num_rows)
    table3 = bq_helper.create_temp_table(dataset_id=dataset_id, table_id=table3_id, schema=schema, num_rows=num_rows)

    column1 = 'id'
    column2 = 'name'
    column3 = 'category'
    column4 = 'value'

    return {
        'project': table.project,
        'dataset': table.dataset_id,
        'table': table.table_id,
        'table2': table2.table_id,
        'table3': table3.table_id,
        'column1': column1,
        'column2': column2,
        'column3': column3,
        'column4': column4
    }

def format_test_query(query_template: str, table_info: dict) -> str:
    """Formats a test query template with actual table and column names

    Args:
        query_template (str): query_template
        table_info (dict): table_info

    Returns:
        str: Formatted query ready for execution
    """
    project = table_info['project']
    dataset = table_info['dataset']
    table = table_info['table']
    column1 = table_info['column1']
    column2 = table_info['column2']
    column3 = table_info['column3']
    column4 = table_info['column4']
    table2 = table_info.get('table2', table)
    table3 = table_info.get('table3', table)

    formatted_query = query_template.format(
        project=project,
        dataset=dataset,
        table=table,
        table2=table2,
        table3=table3,
        column1=column1,
        column2=column2,
        column3=column3,
        column4=column4
    )
    return formatted_query

def measure_query_performance(bq_client: BigQueryClient, query: str, iterations: int) -> dict:
    """Measures the performance of a BigQuery query execution

    Args:
        bq_client (BigQueryClient): bq_client
        query (str): query
        iterations (int): iterations

    Returns:
        dict: Performance metrics including execution time, bytes processed, etc.
    """
    execution_times = []
    bytes_processed_list = []
    slot_ms_list = []

    for _ in range(iterations):
        start_time = time.time()
        query_job = bq_client.execute_query(query)
        end_time = time.time()

        execution_time = end_time - start_time
        execution_times.append(execution_time)
        bytes_processed_list.append(query_job.total_bytes_processed)
        slot_ms_list.append(query_job.total_slot_ms)

    min_time = min(execution_times)
    max_time = max(execution_times)
    avg_time = statistics.mean(execution_times)
    median_time = statistics.median(execution_times)
    p95_time = numpy.percentile(execution_times, 95)

    min_bytes = min(bytes_processed_list)
    max_bytes = max(bytes_processed_list)
    avg_bytes = statistics.mean(bytes_processed_list)
    median_bytes = statistics.median(bytes_processed_list)
    p95_bytes = numpy.percentile(bytes_processed_list, 95)

    return {
        'min_time': min_time,
        'max_time': max_time,
        'avg_time': avg_time,
        'median_time': median_time,
        'p95_time': p95_time,
        'min_bytes': min_bytes,
        'max_bytes': max_bytes,
        'avg_bytes': avg_bytes,
        'median_bytes': median_bytes,
        'p95_bytes': p95_bytes,
        'iterations': iterations
    }

def compare_optimized_query_performance(bq_client: BigQueryClient, optimizer: QueryOptimizer, original_query: str, techniques: list, iterations: int) -> dict:
    """Compares performance between original and optimized queries

    Args:
        bq_client (BigQueryClient): bq_client
        optimizer (QueryOptimizer): optimizer
        original_query (str): original_query
        techniques (list): techniques
        iterations (int): iterations

    Returns:
        dict: Comparison metrics between original and optimized queries
    """
    optimized_query_result = optimizer.optimize_query(original_query, techniques, validate=False, use_cache=False)
    optimized_query = optimized_query_result['optimized_query']

    original_performance = measure_query_performance(bq_client, original_query, iterations)
    optimized_performance = measure_query_performance(bq_client, optimized_query, iterations)

    time_improvement = ((original_performance['avg_time'] - optimized_performance['avg_time']) / original_performance['avg_time']) * 100
    bytes_improvement = ((original_performance['avg_bytes'] - optimized_performance['avg_bytes']) / original_performance['avg_bytes']) * 100

    return {
        'original_performance': original_performance,
        'optimized_performance': optimized_performance,
        'time_improvement': time_improvement,
        'bytes_improvement': bytes_improvement,
        'techniques': techniques
    }

class QueryPerformanceTest:
    """Test class for measuring and validating BigQuery query performance"""

    def __init__(self):
        """Initialize the query performance test class"""
        self._test_tables = {}
        self._performance_results = {}

    @classmethod
    def setup_class(cls):
        """Set up test environment before running tests"""
        cls._bq_helper = BigQueryTestHelper()
        cls._test_dataset = cls._bq_helper.create_temp_dataset()
        cls._test_tables = setup_test_tables(cls._bq_helper, cls._test_dataset.dataset_id, 'small')
        cls._test_tables_medium = setup_test_tables(cls._bq_helper, cls._test_dataset.dataset_id, 'medium')
        cls._test_tables_large = setup_test_tables(cls._bq_helper, cls._test_dataset.dataset_id, 'large')

    @classmethod
    def teardown_class(cls):
        """Clean up test environment after tests complete"""
        cls._bq_helper.cleanup()

    def run_query_performance_test(self, bq_client: BigQueryClient, size: str, query_index: int, performance_metrics_collector, performance_test_context) -> dict:
        """Run performance test for a specific query size and type

        Args:
            bq_client (BigQueryClient): bq_client
            size (str): size
            query_index (int): query_index
            performance_metrics_collector (object): performance_metrics_collector
            performance_test_context (object): performance_test_context

        Returns:
            dict: Performance test results
        """
        if size == 'small':
            table_info = self._test_tables
        elif size == 'medium':
            table_info = self._test_tables_medium
        elif size == 'large':
            table_info = self._test_tables_large
        else:
            raise ValueError(f"Invalid size: {size}")

        query_template = TEST_QUERIES[size][query_index]
        query = format_test_query(query_template, table_info)

        with performance_test_context(f"BigQuery Query Performance - {size} - {query_index}") as pt:
            performance_metrics = measure_query_performance(bq_client, query, pt.test_iterations)
            performance_metrics_collector.add_metric(f"BigQuery Query Performance - {size} - {query_index}", performance_metrics)

        return performance_metrics

    def run_optimization_performance_test(self, bq_client: BigQueryClient, size: str, query_index: int, techniques: list, performance_metrics_collector, performance_test_context) -> dict:
        """Run performance test comparing original and optimized queries

        Args:
            bq_client (BigQueryClient): bq_client
            size (str): size
            query_index (int): query_index
            techniques (list): techniques
            performance_metrics_collector (object): performance_metrics_collector
            performance_test_context (object): performance_test_context

        Returns:
            dict: Optimization performance comparison results
        """
        if size == 'small':
            table_info = self._test_tables
        elif size == 'medium':
            table_info = self._test_tables_medium
        elif size == 'large':
            table_info = self._test_tables_large
        else:
            raise ValueError(f"Invalid size: {size}")

        query_template = TEST_QUERIES[size][query_index]
        original_query = format_test_query(query_template, table_info)

        optimizer = QueryOptimizer(bq_client)

        with performance_test_context(f"BigQuery Optimization Performance - {size} - {query_index} - {techniques}") as pt:
            optimization_results = compare_optimized_query_performance(bq_client, optimizer, original_query, techniques, pt.test_iterations)
            performance_metrics_collector.add_metric(f"BigQuery Optimization Performance - {size} - {query_index} - {techniques}", optimization_results)

        return optimization_results

    @pytest.mark.performance
    @pytest.mark.bigquery
    def test_small_query_performance(self, bigquery_performance_client, performance_metrics_collector, performance_test_context, performance_thresholds, test_iterations):
        """Test performance of small BigQuery queries"""
        for i in range(len(TEST_QUERIES['small'])):
            performance_metrics = self.run_query_performance_test(bigquery_performance_client, 'small', i, performance_metrics_collector, performance_test_context)
            assert performance_metrics['avg_time'] < performance_thresholds['small_query_time'], f"Small query {i} avg time exceeds threshold"
            performance_metrics_collector.record_metric_details('small', i, performance_metrics)

    @pytest.mark.performance
    @pytest.mark.bigquery
    def test_medium_query_performance(self, bigquery_performance_client, performance_metrics_collector, performance_test_context, performance_thresholds, test_iterations):
        """Test performance of medium complexity BigQuery queries"""
        for i in range(len(TEST_QUERIES['medium'])):
            performance_metrics = self.run_query_performance_test(bigquery_performance_client, 'medium', i, performance_metrics_collector, performance_test_context)
            assert performance_metrics['avg_time'] < performance_thresholds['medium_query_time'], f"Medium query {i} avg time exceeds threshold"
            performance_metrics_collector.record_metric_details('medium', i, performance_metrics)

    @pytest.mark.performance
    @pytest.mark.bigquery
    @pytest.mark.slow
    def test_large_query_performance(self, bigquery_performance_client, performance_metrics_collector, performance_test_context, performance_thresholds, test_iterations):
        """Test performance of large complex BigQuery queries"""
        for i in range(len(TEST_QUERIES['large'])):
            performance_metrics = self.run_query_performance_test(bigquery_performance_client, 'large', i, performance_metrics_collector, performance_test_context)
            assert performance_metrics['avg_time'] < performance_thresholds['large_query_time'], f"Large query {i} avg time exceeds threshold"
            performance_metrics_collector.record_metric_details('large', i, performance_metrics)

    @pytest.mark.performance
    @pytest.mark.bigquery
    @pytest.mark.optimization
    def test_query_optimization_performance(self, bigquery_performance_client, performance_metrics_collector, performance_test_context, test_iterations):
        """Test performance improvement from query optimization techniques"""
        techniques_to_test = [
            ['PREDICATE_PUSHDOWN'],
            ['JOIN_REORDERING'],
            ['SUBQUERY_FLATTENING'],
            ['COLUMN_PRUNING'],
            ['AGGREGATION_OPTIMIZATION'],
            ['CTE_CONVERSION']
        ]

        for i in range(len(TEST_QUERIES['medium'])):
            for techniques in techniques_to_test:
                optimization_results = self.run_optimization_performance_test(bigquery_performance_client, 'medium', i, techniques, performance_metrics_collector, performance_test_context)
                assert optimization_results['time_improvement'] > 0, f"Query optimization did not improve performance for query {i} with techniques {techniques}"
                performance_metrics_collector.record_metric_details('optimization', i, optimization_results)

    @pytest.mark.performance
    @pytest.mark.bigquery
    @pytest.mark.optimization
    def test_combined_optimization_techniques(self, bigquery_performance_client, performance_metrics_collector, performance_test_context, test_iterations):
        """Test performance improvement from combining multiple optimization techniques"""
        combined_techniques = [
            ['PREDICATE_PUSHDOWN', 'COLUMN_PRUNING'],
            ['JOIN_REORDERING', 'PREDICATE_PUSHDOWN'],
            ['SUBQUERY_FLATTENING', 'COLUMN_PRUNING']
        ]

        for i in range(len(TEST_QUERIES['medium'])):
            for techniques in combined_techniques:
                optimization_results = self.run_optimization_performance_test(bigquery_performance_client, 'medium', i, techniques, performance_metrics_collector, performance_test_context)
                assert optimization_results['time_improvement'] > 0, f"Combined optimization did not improve performance for query {i} with techniques {techniques}"
                performance_metrics_collector.record_metric_details('combined_optimization', i, optimization_results)

    @pytest.mark.bigquery
    @pytest.mark.optimization
    def test_query_optimization_correctness(self, bigquery_performance_client):
        """Test that optimized queries produce the same results as original queries"""
        # TODO: Implement correctness testing by comparing results from original and optimized queries
        pass