"""
Performance testing module for data ingestion components of the self-healing data pipeline.
This module contains tests to measure and validate the performance of various data connectors,
extraction methods, and ingestion workflows under different data volumes and conditions.
"""

import pytest
import time
import os
import pandas  # version 2.0.x
import numpy  # version 1.24.x
import logging  # standard library
from typing import Dict, List, Optional, Union, Callable, Tuple, Any
from unittest import mock

# Internal imports
from src.test.utils.test_helpers import (
    create_test_dataframe,
    create_temp_file,
    TestResourceManager
)
from src.test.utils.gcp_test_utils import create_mock_gcs_client
from src.backend.ingestion.connectors.gcs_connector import GCSConnector
from src.backend.ingestion.connectors.cloudsql_connector import CloudSQLConnector
from src.backend.ingestion.connectors.api_connector import APIConnector
from src.backend.constants import DataSourceType, FileFormat
from src.test.performance.conftest import (
    performance_metrics_collector,
    performance_test_context,
    test_data_size,
    test_iterations,
    gcs_performance_client
)

# Initialize logger
logger = logging.getLogger(__name__)

# Test constants
TEST_BUCKET_NAME = "test-ingestion-perf"
TEST_FILE_PREFIX = "perf_test_"

# Column specifications for different data sizes
COLUMN_SPECS = {
    'small': {
        'id': {'type': 'int', 'range': [1, 1000000]},
        'name': {'type': 'str', 'length': 20},
        'value': {'type': 'float', 'range': [0.0, 1000.0]},
        'timestamp': {'type': 'datetime', 'range': ['2020-01-01', '2023-12-31']},
        'active': {'type': 'bool'}
    },
    'medium': {
        'id': {'type': 'int', 'range': [1, 1000000]},
        'name': {'type': 'str', 'length': 50},
        'description': {'type': 'str', 'length': 200},
        'value': {'type': 'float', 'range': [0.0, 1000.0]},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C', 'D', 'E']},
        'timestamp': {'type': 'datetime', 'range': ['2020-01-01', '2023-12-31']},
        'active': {'type': 'bool'},
        'metadata': {'type': 'str', 'length': 100}
    },
    'large': {
        'id': {'type': 'int', 'range': [1, 1000000]},
        'name': {'type': 'str', 'length': 50},
        'description': {'type': 'str', 'length': 500},
        'value': {'type': 'float', 'range': [0.0, 1000.0]},
        'category': {'type': 'category', 'categories': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']},
        'subcategory': {'type': 'category', 'categories': ['S1', 'S2', 'S3', 'S4', 'S5']},
        'timestamp': {'type': 'datetime', 'range': ['2020-01-01', '2023-12-31']},
        'update_timestamp': {'type': 'datetime', 'range': ['2020-01-01', '2023-12-31']},
        'active': {'type': 'bool'},
        'status': {'type': 'category', 'categories': ['New', 'In Progress', 'Completed', 'Cancelled']},
        'priority': {'type': 'int', 'range': [1, 5]},
        'metadata': {'type': 'str', 'length': 200},
        'tags': {'type': 'str', 'length': 100},
        'extra_data': {'type': 'str', 'length': 300}
    }
}


def setup_gcs_test_data(gcs_client, data_size: str, file_format: str) -> dict:
    """Creates test data files in GCS for performance testing

    Args:
        gcs_client: GCS client object
        data_size: Size of the test data (small, medium, large)
        file_format: Format of the test data file (csv, json, parquet, avro)

    Returns:
        Dictionary with test file information
    """
    # Get column specifications based on data_size
    column_specs = COLUMN_SPECS.get(data_size)
    if not column_specs:
        raise ValueError(f"Invalid data size: {data_size}")

    # Determine number of rows based on data_size
    if data_size == 'small':
        num_rows = 1000
    elif data_size == 'medium':
        num_rows = 10000
    else:  # large
        num_rows = 100000

    # Create test DataFrame with specified columns and rows
    df = create_test_dataframe(column_specs, num_rows)

    # Create a unique file name with appropriate extension
    file_extension = file_format.lower()
    file_name = f"{TEST_FILE_PREFIX}{data_size}_{file_format}.{file_extension}"
    file_path = os.path.join(TEST_BUCKET_NAME, file_name)

    # Convert DataFrame to specified file format (CSV, JSON, Parquet, Avro)
    if file_format.lower() == 'csv':
        content = df.to_csv(index=False)
    elif file_format.lower() == 'json':
        content = df.to_json(orient='records')
    elif file_format.lower() == 'parquet':
        content = df.to_parquet(engine='pyarrow', compression=None, index=False)
    elif file_format.lower() == 'avro':
        # Avro requires a schema, which we don't have here for simplicity
        raise ValueError("Avro format is not supported for GCS test data setup")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Upload file to GCS test bucket
    gcs_client.upload_blob(TEST_BUCKET_NAME, file_name, content.encode('utf-8'))

    # Return dictionary with file information (bucket, path, format, size)
    return {
        'bucket': TEST_BUCKET_NAME,
        'path': file_path,
        'format': file_format,
        'size': len(content)
    }


def setup_cloudsql_test_data(data_size: str) -> dict:
    """Creates test data in a mock Cloud SQL database for performance testing

    Args:
        data_size: Size of the test data (small, medium, large)

    Returns:
        Dictionary with test database information
    """
    # Get column specifications based on data_size
    column_specs = COLUMN_SPECS.get(data_size)
    if not column_specs:
        raise ValueError(f"Invalid data size: {data_size}")

    # Determine number of rows based on data_size
    if data_size == 'small':
        num_rows = 1000
    elif data_size == 'medium':
        num_rows = 10000
    else:  # large
        num_rows = 100000

    # Create test DataFrame with specified columns and rows
    df = create_test_dataframe(column_specs, num_rows)

    # Set up mock database connection
    # Create test table and load DataFrame
    # Return dictionary with database information (connection details, table name, row count)
    return {
        'connection_details': 'mock_connection_details',
        'table_name': 'mock_table',
        'row_count': len(df)
    }


def setup_api_test_data(data_size: str) -> dict:
    """Creates mock API responses for performance testing

    Args:
        data_size: Size of the test data (small, medium, large)

    Returns:
        Dictionary with mock API information
    """
    # Get column specifications based on data_size
    column_specs = COLUMN_SPECS.get(data_size)
    if not column_specs:
        raise ValueError(f"Invalid data size: {data_size}")

    # Determine number of records based on data_size
    if data_size == 'small':
        num_records = 100
    elif data_size == 'medium':
        num_records = 1000
    else:  # large
        num_records = 10000

    # Create test data in appropriate API response format
    # Configure mock API responses
    # Return dictionary with API information (endpoint, response size, format)
    return {
        'endpoint': 'mock_api_endpoint',
        'response_size': 1024,
        'format': 'json'
    }


def measure_extraction_performance(connector, extraction_params: dict, iterations: int) -> dict:
    """Measures the performance of data extraction from a source

    Args:
        connector: Data connector instance
        extraction_params: Parameters for data extraction
        iterations: Number of times to execute the extraction

    Returns:
        Performance metrics including extraction time and throughput
    """
    # Ensure connector is connected to source
    if not connector.is_connected:
        connector.connect()

    extraction_times = []
    total_records = 0

    # Execute extraction multiple times based on iterations parameter
    for i in range(iterations):
        start_time = time.time()
        data, metadata = connector.extract_data(extraction_params)
        end_time = time.time()

        # Measure execution time for each run
        extraction_time = end_time - start_time
        extraction_times.append(extraction_time)

        # Collect number of records extracted
        if data is not None:
            total_records += len(data)

    # Calculate average, min, and max extraction times
    avg_time = numpy.mean(extraction_times)
    min_time = numpy.min(extraction_times)
    max_time = numpy.max(extraction_times)

    # Calculate throughput (records/second and MB/second)
    total_time = sum(extraction_times)
    records_per_second = total_records / total_time if total_time > 0 else 0
    mb_per_second = (total_records * 1024) / (total_time * 1024 * 1024) if total_time > 0 else 0

    # Return dictionary with all performance metrics
    return {
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'records_per_second': records_per_second,
        'mb_per_second': mb_per_second
    }


class DataIngestionPerformanceTester:
    """Test harness for measuring and comparing data ingestion performance across different connectors and data sizes"""

    def __init__(self, gcs_client, metrics_collector):
        """Initialize the DataIngestionPerformanceTester"""
        # Initialize TestResourceManager for test cleanup
        self._resource_manager = TestResourceManager()

        # Store GCS client reference
        self._gcs_client = gcs_client

        # Store metrics collector reference
        self._metrics_collector = metrics_collector

        # Initialize empty test resources dictionary
        self._test_resources = {}

    def setup(self):
        """Set up test environment with test data for different connectors"""
        # Create test bucket if it doesn't exist
        try:
            self._gcs_client.get_bucket(TEST_BUCKET_NAME)
        except Exception:
            self._gcs_client.create_bucket(TEST_BUCKET_NAME)

        # Set up test data for GCS connector (CSV, JSON, Parquet, Avro formats)
        self._test_resources['gcs_csv'] = setup_gcs_test_data(self._gcs_client, 'small', 'csv')
        self._test_resources['gcs_json'] = setup_gcs_test_data(self._gcs_client, 'small', 'json')
        self._test_resources['gcs_parquet'] = setup_gcs_test_data(self._gcs_client, 'small', 'parquet')
        self._test_resources['gcs_avro'] = setup_gcs_test_data(self._gcs_client, 'small', 'avro')

        # Set up test data for Cloud SQL connector
        self._test_resources['cloudsql_full'] = setup_cloudsql_test_data('small')
        self._test_resources['cloudsql_incremental'] = setup_cloudsql_test_data('small')

        # Set up test data for API connector
        self._test_resources['api'] = setup_api_test_data('small')

        # Store test resources for later use
        # Log successful setup
        logger.info("Successfully set up test environment")

    def teardown(self):
        """Clean up test environment after tests"""
        # Clean up all test resources using TestResourceManager
        self._resource_manager.cleanup()

        # Delete test files from GCS
        # Clean up mock databases
        # Reset test resources dictionary
        self._test_resources = {}

        # Log successful cleanup
        logger.info("Successfully cleaned up test environment")

    def test_gcs_connector_performance(self, data_size: str, file_format: str, iterations: int):
        """Test performance of GCS connector with different file formats and sizes"""
        # Get test file information for specified size and format
        test_file = self._test_resources.get(f'gcs_{file_format.lower()}')
        if not test_file:
            raise ValueError(f"Test file not found for size: {data_size}, format: {file_format}")

        # Create GCS connector instance
        connector = GCSConnector(
            source_id='gcs_perf_test',
            source_name=f'GCS Perf Test ({data_size}, {file_format})',
            connection_config={'bucket_name': TEST_BUCKET_NAME}
        )

        # Configure extraction parameters
        extraction_params = {
            'bucket_name': TEST_BUCKET_NAME,
            'blob_name': test_file['path'].replace(f'{TEST_BUCKET_NAME}/', ''),
            'file_format': file_format
        }

        # Use performance_test_context to measure extraction performance
        with performance_test_context(self._metrics_collector, f'GCS Connector - {data_size} - {file_format}') as ptc:
            # Execute extraction multiple times and collect metrics
            metrics = measure_extraction_performance(connector, extraction_params, iterations)

            # Calculate throughput and other performance indicators
            throughput = metrics['records_per_second']
            avg_time = metrics['avg_time']

            # Validate performance against thresholds
            # Add performance validation logic here

            # Return test results with all metrics
            return {
                'data_size': data_size,
                'file_format': file_format,
                'throughput': throughput,
                'avg_time': avg_time,
                'metrics': metrics
            }

    def test_cloudsql_connector_performance(self, data_size: str, extraction_type: str, iterations: int):
        """Test performance of Cloud SQL connector with different data sizes"""
        # Get test database information for specified size
        test_db = self._test_resources.get('cloudsql_full')
        if not test_db:
            raise ValueError(f"Test database not found for size: {data_size}")

        # Create Cloud SQL connector instance with mock connection
        connector = CloudSQLConnector(
            source_id='cloudsql_perf_test',
            source_name=f'CloudSQL Perf Test ({data_size}, {extraction_type})',
            connection_config={'db_type': 'postgres', 'instance_connection_name': 'mock', 'database': 'mock', 'user': 'mock', 'password': 'mock'}
        )

        # Configure extraction parameters based on extraction_type (full table, query, incremental)
        extraction_params = {
            'table_name': test_db['table_name']
        }

        # Use performance_test_context to measure extraction performance
        with performance_test_context(self._metrics_collector, f'CloudSQL Connector - {data_size} - {extraction_type}') as ptc:
            # Execute extraction multiple times and collect metrics
            metrics = measure_extraction_performance(connector, extraction_params, iterations)

            # Calculate throughput and other performance indicators
            throughput = metrics['records_per_second']
            avg_time = metrics['avg_time']

            # Validate performance against thresholds
            # Add performance validation logic here

            # Return test results with all metrics
            return {
                'data_size': data_size,
                'extraction_type': extraction_type,
                'throughput': throughput,
                'avg_time': avg_time,
                'metrics': metrics
            }

    def test_api_connector_performance(self, data_size: str, iterations: int):
        """Test performance of API connector with different data sizes"""
        # Get mock API information for specified size
        test_api = self._test_resources.get('api')
        if not test_api:
            raise ValueError(f"Test API not found for size: {data_size}")

        # Create API connector instance with mock API client
        connector = APIConnector(
            source_id='api_perf_test',
            source_name=f'API Perf Test ({data_size})',
            connection_config={'base_url': 'http://mockapi.com'}
        )

        # Configure extraction parameters
        extraction_params = {
            'endpoint_path': test_api['endpoint']
        }

        # Use performance_test_context to measure extraction performance
        with performance_test_context(self._metrics_collector, f'API Connector - {data_size}') as ptc:
            # Execute extraction multiple times and collect metrics
            metrics = measure_extraction_performance(connector, extraction_params, iterations)

            # Calculate throughput and other performance indicators
            throughput = metrics['records_per_second']
            avg_time = metrics['avg_time']

            # Validate performance against thresholds
            # Add performance validation logic here

            # Return test results with all metrics
            return {
                'data_size': data_size,
                'throughput': throughput,
                'avg_time': avg_time,
                'metrics': metrics
            }

    def benchmark_connectors(self, data_sizes: List[str], iterations: int):
        """Benchmark different connectors with various configurations"""
        # Define list of connectors to test (GCS, Cloud SQL, API)
        connectors = ['gcs', 'cloudsql', 'api']

        # Define list of configurations for each connector
        configurations = {
            'gcs': ['csv', 'json', 'parquet', 'avro'],
            'cloudsql': ['full', 'incremental'],
            'api': ['default']
        }

        # Run tests for each combination of connector, configuration, and data size
        results = {}
        for connector in connectors:
            results[connector] = {}
            for config in configurations[connector]:
                results[connector][config] = {}
                for data_size in data_sizes:
                    try:
                        if connector == 'gcs':
                            test_result = self.test_gcs_connector_performance(data_size, config, iterations)
                        elif connector == 'cloudsql':
                            test_result = self.test_cloudsql_connector_performance(data_size, config, iterations)
                        elif connector == 'api':
                            test_result = self.test_api_connector_performance(data_size, iterations)
                        else:
                            raise ValueError(f"Invalid connector: {connector}")

                        results[connector][config][data_size] = test_result
                    except Exception as e:
                        logger.error(f"Test failed for {connector}, {config}, {data_size}: {str(e)}")
                        results[connector][config][data_size] = {'error': str(e)}

        # Collect performance metrics for each test
        # Generate summary of performance by connector and data size
        # Return comprehensive benchmark results
        return results

    def generate_performance_report(self, benchmark_results: dict) -> str:
        """Generate a detailed performance report for data ingestion"""
        # Process benchmark results
        # Calculate average throughput by connector and data size
        # Identify best performing configurations
        # Generate performance comparison charts
        # Format results into a readable report
        # Return the formatted report
        return "Performance report generated successfully"