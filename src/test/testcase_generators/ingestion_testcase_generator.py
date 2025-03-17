"""
Specialized test case generator for data ingestion testing.

This module provides functionality to create comprehensive test cases for different
data source types (GCS, Cloud SQL, API, etc.) with various configurations, error
scenarios, and expected outcomes to facilitate thorough testing of the ingestion
components of the self-healing data pipeline.
"""

import os
import json
import random
import uuid
import datetime
from typing import Dict, List, Optional, Tuple, Any, Union

import pandas as pd
import numpy as np
from faker import Faker

from src.backend.constants import DataSourceType, FileFormat
from src.test.testcase_generators.schema_data_generator import (
    TestCaseGenerator,
    SchemaDataTestCase,
    generate_schema_data_pair
)
from src.test.utils.test_helpers import (
    create_test_dataframe,
    create_test_json_data,
    generate_unique_id,
    create_temp_file,
    create_temp_directory
)

# Constants
TEST_CASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'ingestion')
DEFAULT_NUM_VARIATIONS = 5
DEFAULT_DATA_SIZE = 100
DEFAULT_FILE_FORMAT = FileFormat.JSON
INGESTION_ERROR_TYPES = [
    "connection_failure", "authentication_error", "permission_denied", 
    "resource_not_found", "timeout", "rate_limit_exceeded", "schema_mismatch", 
    "data_format_error", "network_error", "service_unavailable"
]


def generate_gcs_connection_config(config: Dict) -> Dict:
    """
    Generates a GCS connection configuration for testing.
    
    Args:
        config: Configuration parameters or overrides
        
    Returns:
        GCS connection configuration
    """
    # Extract parameters with defaults
    project_id = config.get('project_id', f"test-project-{uuid.uuid4()}")
    bucket_name = config.get('bucket_name', f"test-bucket-{uuid.uuid4()}")
    
    # Create basic connection config
    connection_config = {
        'project_id': project_id,
        'bucket_name': bucket_name,
        'location': config.get('location', 'us-central1'),
    }
    
    # Add authentication if specified
    if 'auth_method' in config:
        connection_config['auth_method'] = config['auth_method']
        if config['auth_method'] == 'service_account':
            connection_config['service_account_key'] = config.get('service_account_key', 'path/to/key.json')
    
    # Add optional parameters
    for param in ['timeout', 'retry_params', 'user_agent']:
        if param in config:
            connection_config[param] = config[param]
    
    return connection_config


def generate_cloudsql_connection_config(config: Dict) -> Dict:
    """
    Generates a Cloud SQL connection configuration for testing.
    
    Args:
        config: Configuration parameters or overrides
        
    Returns:
        Cloud SQL connection configuration
    """
    # Extract parameters with defaults
    instance = config.get('instance', f"test-instance-{uuid.uuid4()}")
    database = config.get('database', f"test_db_{random.randint(1, 1000)}")
    user = config.get('user', 'test_user')
    password = config.get('password', 'test_password')
    
    # Create basic connection config
    connection_config = {
        'instance_connection_name': instance,
        'database': database,
        'user': user,
        'password': password,
        'database_type': config.get('database_type', 'postgres'),
    }
    
    # Add optional parameters
    for param in ['socket', 'charset', 'timeout', 'connection_args']:
        if param in config:
            connection_config[param] = config[param]
    
    return connection_config


def generate_api_connection_config(config: Dict) -> Dict:
    """
    Generates an API connection configuration for testing.
    
    Args:
        config: Configuration parameters or overrides
        
    Returns:
        API connection configuration
    """
    # Extract parameters with defaults
    base_url = config.get('base_url', 'https://api.example.com')
    auth_type = config.get('auth_type', 'api_key')
    
    # Create basic connection config
    connection_config = {
        'base_url': base_url,
        'auth_type': auth_type,
    }
    
    # Add authentication details based on auth_type
    if auth_type == 'api_key':
        connection_config['api_key'] = config.get('api_key', f"test-api-key-{uuid.uuid4()}")
        connection_config['api_key_header'] = config.get('api_key_header', 'X-API-Key')
    elif auth_type == 'oauth2':
        connection_config['client_id'] = config.get('client_id', f"client-{uuid.uuid4()}")
        connection_config['client_secret'] = config.get('client_secret', f"secret-{uuid.uuid4()}")
        connection_config['token_url'] = config.get('token_url', f"{base_url}/oauth/token")
    elif auth_type == 'basic':
        connection_config['username'] = config.get('username', 'test_user')
        connection_config['password'] = config.get('password', 'test_password')
    
    # Add pagination configuration if specified
    if 'pagination' in config:
        connection_config['pagination'] = config['pagination']
    
    # Add rate limiting configuration if specified
    if 'rate_limit' in config:
        connection_config['rate_limit'] = config['rate_limit']
    
    # Add optional parameters
    for param in ['timeout', 'headers', 'retry_params', 'verify_ssl']:
        if param in config:
            connection_config[param] = config[param]
    
    return connection_config


def generate_extraction_params(source_type: DataSourceType, config: Dict) -> Dict:
    """
    Generates extraction parameters for a specific data source type.
    
    Args:
        source_type: Type of data source
        config: Configuration parameters or overrides
        
    Returns:
        Extraction parameters
    """
    # Common parameters
    params = {
        'format': config.get('format', FileFormat.JSON),
        'batch_size': config.get('batch_size', 1000),
    }
    
    # Source-specific parameters
    if source_type == DataSourceType.GCS:
        # GCS specific parameters
        if 'bucket_name' in config:
            params['bucket_name'] = config['bucket_name']
        
        # Either blob_name, file_list, or prefix/pattern must be specified
        if 'blob_name' in config:
            params['blob_name'] = config['blob_name']
        elif 'file_list' in config:
            params['file_list'] = config['file_list']
        else:
            params['prefix'] = config.get('prefix', '')
            if 'pattern' in config:
                params['pattern'] = config['pattern']
        
    elif source_type == DataSourceType.CLOUD_SQL:
        # Cloud SQL specific parameters
        if 'table_name' in config:
            params['table_name'] = config['table_name']
        elif 'query' in config:
            params['query'] = config['query']
        
        # Incremental extraction parameters
        if 'incremental' in config and config['incremental']:
            params['incremental'] = True
            params['incremental_column'] = config.get('incremental_column', 'updated_at')
            params['last_value'] = config.get('last_value', '2023-01-01T00:00:00')
    
    elif source_type == DataSourceType.API:
        # API specific parameters
        params['endpoint'] = config.get('endpoint', '/api/data')
        params['method'] = config.get('method', 'GET')
        
        # Add query parameters if specified
        if 'query_params' in config:
            params['query_params'] = config['query_params']
        
        # Add body if specified (for POST, PUT, etc.)
        if 'body' in config:
            params['body'] = config['body']
        
        # Add headers if specified
        if 'headers' in config:
            params['headers'] = config['headers']
    
    # Add common optional parameters
    for param in ['max_retries', 'retry_delay', 'timeout']:
        if param in config:
            params[param] = config[param]
    
    return params


def generate_ingestion_error(error_type: str, source_type: DataSourceType, config: Dict) -> Dict:
    """
    Generates an ingestion error scenario for testing.
    
    Args:
        error_type: Type of error to generate
        source_type: Type of data source
        config: Configuration parameters or overrides
        
    Returns:
        Error scenario configuration
    """
    error_scenario = {
        'error_type': error_type,
        'source_type': source_type.value if isinstance(source_type, DataSourceType) else source_type,
    }
    
    # Add error-specific parameters
    if error_type == 'connection_failure':
        error_scenario['error_message'] = config.get('error_message', 'Failed to connect to the data source')
        error_scenario['error_code'] = config.get('error_code', 'CONNECTION_ERROR')
        
    elif error_type == 'authentication_error':
        error_scenario['error_message'] = config.get('error_message', 'Authentication failed')
        error_scenario['error_code'] = config.get('error_code', 'AUTH_ERROR')
        
    elif error_type == 'permission_denied':
        error_scenario['error_message'] = config.get('error_message', 'Permission denied')
        error_scenario['error_code'] = config.get('error_code', 'PERMISSION_ERROR')
        
    elif error_type == 'resource_not_found':
        error_scenario['error_message'] = config.get('error_message', 'Resource not found')
        error_scenario['error_code'] = config.get('error_code', 'NOT_FOUND')
        
    elif error_type == 'timeout':
        error_scenario['error_message'] = config.get('error_message', 'Request timed out')
        error_scenario['error_code'] = config.get('error_code', 'TIMEOUT')
        
    elif error_type == 'rate_limit_exceeded':
        error_scenario['error_message'] = config.get('error_message', 'Rate limit exceeded')
        error_scenario['error_code'] = config.get('error_code', 'RATE_LIMIT')
        
    elif error_type == 'schema_mismatch':
        error_scenario['error_message'] = config.get('error_message', 'Schema mismatch detected')
        error_scenario['error_code'] = config.get('error_code', 'SCHEMA_ERROR')
        error_scenario['expected_schema'] = config.get('expected_schema', {})
        error_scenario['actual_schema'] = config.get('actual_schema', {})
        
    elif error_type == 'data_format_error':
        error_scenario['error_message'] = config.get('error_message', 'Invalid data format')
        error_scenario['error_code'] = config.get('error_code', 'FORMAT_ERROR')
        error_scenario['location'] = config.get('location', 'line 10, column 5')
        
    elif error_type == 'network_error':
        error_scenario['error_message'] = config.get('error_message', 'Network error occurred')
        error_scenario['error_code'] = config.get('error_code', 'NETWORK_ERROR')
        
    elif error_type == 'service_unavailable':
        error_scenario['error_message'] = config.get('error_message', 'Service is currently unavailable')
        error_scenario['error_code'] = config.get('error_code', 'SERVICE_UNAVAILABLE')
    
    # Add recovery suggestion if available
    if 'recovery_suggestion' in config:
        error_scenario['recovery_suggestion'] = config['recovery_suggestion']
    
    # Add self-healing configuration if specified
    if 'self_healing' in config:
        error_scenario['self_healing'] = config['self_healing']
    
    return error_scenario


def save_ingestion_test_case(test_case: Dict, test_case_name: str, output_dir: str) -> Dict:
    """
    Saves an ingestion test case to files.
    
    Args:
        test_case: Test case to save
        test_case_name: Name for the test case
        output_dir: Directory to save the test case
        
    Returns:
        Paths to saved test case files
    """
    # Create output directory if it doesn't exist
    test_case_dir = os.path.join(output_dir, test_case_name)
    os.makedirs(test_case_dir, exist_ok=True)
    
    file_paths = {'test_case_name': test_case_name}
    
    # Save schema to JSON file if present
    if 'schema' in test_case:
        schema_file = os.path.join(test_case_dir, 'schema.json')
        with open(schema_file, 'w') as f:
            json.dump(test_case['schema'], f, indent=2)
        file_paths['schema_path'] = schema_file
    
    # Save data to file in specified format if present
    if 'data' in test_case:
        data = test_case['data']
        if isinstance(data, pd.DataFrame):
            format = test_case.get('format', DEFAULT_FILE_FORMAT)
            if format == FileFormat.JSON:
                data_file = os.path.join(test_case_dir, 'data.json')
                data.to_json(data_file, orient='records', date_format='iso')
            elif format == FileFormat.CSV:
                data_file = os.path.join(test_case_dir, 'data.csv')
                data.to_csv(data_file, index=False)
            elif format == FileFormat.PARQUET:
                data_file = os.path.join(test_case_dir, 'data.parquet')
                data.to_parquet(data_file, index=False)
            elif format == FileFormat.AVRO:
                data_file = os.path.join(test_case_dir, 'data.avro')
                # Default to JSON if avro support not available
                data.to_json(data_file, orient='records', date_format='iso')
            else:
                data_file = os.path.join(test_case_dir, 'data.json')
                data.to_json(data_file, orient='records', date_format='iso')
            
            file_paths['data_path'] = data_file
    
    # Save connection configuration to JSON file
    if 'connection_config' in test_case:
        conn_file = os.path.join(test_case_dir, 'connection_config.json')
        with open(conn_file, 'w') as f:
            json.dump(test_case['connection_config'], f, indent=2)
        file_paths['connection_config_path'] = conn_file
    
    # Save extraction parameters to JSON file
    if 'extraction_params' in test_case:
        params_file = os.path.join(test_case_dir, 'extraction_params.json')
        with open(params_file, 'w') as f:
            json.dump(test_case['extraction_params'], f, indent=2)
        file_paths['extraction_params_path'] = params_file
    
    # Save error scenarios to JSON file if present
    if 'error_scenarios' in test_case:
        error_file = os.path.join(test_case_dir, 'error_scenarios.json')
        with open(error_file, 'w') as f:
            json.dump(test_case['error_scenarios'], f, indent=2)
        file_paths['error_scenarios_path'] = error_file
    
    # Save expected results to JSON file
    if 'expected_results' in test_case:
        results_file = os.path.join(test_case_dir, 'expected_results.json')
        with open(results_file, 'w') as f:
            json.dump(test_case['expected_results'], f, indent=2)
        file_paths['expected_results_path'] = results_file
    
    # Save test case metadata to JSON file
    metadata = {
        'test_case_name': test_case_name,
        'source_type': test_case.get('source_type', None),
        'format': str(test_case.get('format', None)),
        'created_at': datetime.datetime.now().isoformat(),
    }
    if 'metadata' in test_case:
        metadata.update(test_case['metadata'])
    
    metadata_file = os.path.join(test_case_dir, 'metadata.json')
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    file_paths['metadata_path'] = metadata_file
    
    return file_paths


def load_ingestion_test_case(test_case_name: str, input_dir: str) -> Dict:
    """
    Loads a previously saved ingestion test case.
    
    Args:
        test_case_name: Name of the test case
        input_dir: Directory containing the test case
        
    Returns:
        Loaded test case
    """
    # Construct paths to test case files
    test_case_dir = os.path.join(input_dir, test_case_name)
    
    if not os.path.exists(test_case_dir):
        raise FileNotFoundError(f"Test case directory not found: {test_case_dir}")
    
    test_case = {'test_case_name': test_case_name}
    file_paths = {'test_case_name': test_case_name}
    
    # Load schema from JSON file if exists
    schema_file = os.path.join(test_case_dir, 'schema.json')
    if os.path.exists(schema_file):
        with open(schema_file, 'r') as f:
            test_case['schema'] = json.load(f)
        file_paths['schema_path'] = schema_file
    
    # Load data from file in appropriate format if exists
    # Try different formats
    for format_ext in ['.json', '.csv', '.parquet', '.avro']:
        data_file = os.path.join(test_case_dir, f'data{format_ext}')
        if os.path.exists(data_file):
            if format_ext == '.json':
                test_case['data'] = pd.read_json(data_file, orient='records')
                test_case['format'] = FileFormat.JSON
            elif format_ext == '.csv':
                test_case['data'] = pd.read_csv(data_file)
                test_case['format'] = FileFormat.CSV
            elif format_ext == '.parquet':
                test_case['data'] = pd.read_parquet(data_file)
                test_case['format'] = FileFormat.PARQUET
            elif format_ext == '.avro':
                # Default to JSON if avro support not available
                try:
                    import fastavro
                    with open(data_file, 'rb') as f:
                        records = list(fastavro.reader(f))
                    test_case['data'] = pd.DataFrame.from_records(records)
                except ImportError:
                    # Fall back to JSON if fastavro not available
                    test_case['data'] = pd.read_json(data_file, orient='records')
                test_case['format'] = FileFormat.AVRO
            
            file_paths['data_path'] = data_file
            break
    
    # Load connection configuration from JSON file
    conn_file = os.path.join(test_case_dir, 'connection_config.json')
    if os.path.exists(conn_file):
        with open(conn_file, 'r') as f:
            test_case['connection_config'] = json.load(f)
        file_paths['connection_config_path'] = conn_file
    
    # Load extraction parameters from JSON file
    params_file = os.path.join(test_case_dir, 'extraction_params.json')
    if os.path.exists(params_file):
        with open(params_file, 'r') as f:
            test_case['extraction_params'] = json.load(f)
        file_paths['extraction_params_path'] = params_file
    
    # Load error scenarios from JSON file if exists
    error_file = os.path.join(test_case_dir, 'error_scenarios.json')
    if os.path.exists(error_file):
        with open(error_file, 'r') as f:
            test_case['error_scenarios'] = json.load(f)
        file_paths['error_scenarios_path'] = error_file
    
    # Load expected results from JSON file
    results_file = os.path.join(test_case_dir, 'expected_results.json')
    if os.path.exists(results_file):
        with open(results_file, 'r') as f:
            test_case['expected_results'] = json.load(f)
        file_paths['expected_results_path'] = results_file
    
    # Load test case metadata from JSON file
    metadata_file = os.path.join(test_case_dir, 'metadata.json')
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
            test_case['metadata'] = metadata
        file_paths['metadata_path'] = metadata_file
    
    test_case['file_paths'] = file_paths
    
    return test_case


class IngestionTestCaseGenerator(TestCaseGenerator):
    """Generator for creating test cases for data ingestion testing."""
    
    def __init__(self, output_dir: str = TEST_CASE_DIR):
        """
        Initialize the IngestionTestCaseGenerator.
        
        Args:
            output_dir: Directory to save generated test cases
        """
        super().__init__(output_dir)
        
        # Initialize source type generators
        self._source_type_generators = {
            DataSourceType.GCS: self.generate_gcs_test_case,
            DataSourceType.CLOUD_SQL: self.generate_cloudsql_test_case,
            DataSourceType.API: self.generate_api_test_case
        }
        
        # Initialize error type generators mapping
        self._error_type_generators = {error_type: None for error_type in INGESTION_ERROR_TYPES}
        
        # Set output directory
        self._output_dir = output_dir
    
    def generate_gcs_test_case(
        self,
        schema_config: Dict,
        data_config: Dict,
        connection_config: Dict,
        extraction_config: Dict,
        test_case_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for GCS data ingestion.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            connection_config: Configuration for GCS connection
            extraction_config: Configuration for data extraction
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case for GCS ingestion
        """
        # Generate schema and data
        schema, data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate GCS connection configuration
        gcs_connection = generate_gcs_connection_config(connection_config)
        
        # Generate extraction parameters
        extraction_params = generate_extraction_params(DataSourceType.GCS, extraction_config)
        
        # Create expected results
        expected_results = {
            'records_processed': len(data),
            'success': True,
            'processing_time': random.uniform(0.5, 5.0),
            'output_table': extraction_config.get('output_table', 'dataset.table_name'),
            'data_size_bytes': random.randint(1000, 10000000)
        }
        
        # Create test case
        test_case = IngestionTestCase(
            schema=schema,
            data=data,
            connection_config=gcs_connection,
            extraction_params=extraction_params,
            expected_results=expected_results,
            metadata={
                'source_type': DataSourceType.GCS.value,
                'test_case_name': test_case_name,
                'description': f"Test case for GCS data ingestion with {len(data)} records"
            }
        )
        
        # Save test case if requested
        if save_files:
            test_case.save(test_case_name, self._output_dir)
        
        return test_case.to_dict()
    
    def generate_cloudsql_test_case(
        self,
        schema_config: Dict,
        data_config: Dict,
        connection_config: Dict,
        extraction_config: Dict,
        test_case_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for Cloud SQL data ingestion.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            connection_config: Configuration for Cloud SQL connection
            extraction_config: Configuration for data extraction
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case for Cloud SQL ingestion
        """
        # Generate schema and data
        schema, data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate Cloud SQL connection configuration
        sql_connection = generate_cloudsql_connection_config(connection_config)
        
        # Generate extraction parameters
        extraction_params = generate_extraction_params(DataSourceType.CLOUD_SQL, extraction_config)
        
        # Create expected results
        expected_results = {
            'records_processed': len(data),
            'success': True,
            'processing_time': random.uniform(0.5, 10.0),
            'output_table': extraction_config.get('output_table', 'dataset.table_name'),
            'data_size_bytes': random.randint(1000, 10000000)
        }
        
        # Create test case
        test_case = IngestionTestCase(
            schema=schema,
            data=data,
            connection_config=sql_connection,
            extraction_params=extraction_params,
            expected_results=expected_results,
            metadata={
                'source_type': DataSourceType.CLOUD_SQL.value,
                'test_case_name': test_case_name,
                'description': f"Test case for Cloud SQL data ingestion with {len(data)} records"
            }
        )
        
        # Save test case if requested
        if save_files:
            test_case.save(test_case_name, self._output_dir)
        
        return test_case.to_dict()
    
    def generate_api_test_case(
        self,
        schema_config: Dict,
        data_config: Dict,
        connection_config: Dict,
        extraction_config: Dict,
        test_case_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for API data ingestion.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            connection_config: Configuration for API connection
            extraction_config: Configuration for data extraction
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case for API ingestion
        """
        # Generate schema and data
        schema, data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate API connection configuration
        api_connection = generate_api_connection_config(connection_config)
        
        # Generate extraction parameters
        extraction_params = generate_extraction_params(DataSourceType.API, extraction_config)
        
        # Create expected results
        expected_results = {
            'records_processed': len(data),
            'success': True,
            'processing_time': random.uniform(0.2, 8.0),
            'output_table': extraction_config.get('output_table', 'dataset.table_name'),
            'data_size_bytes': random.randint(1000, 5000000),
            'api_requests': random.randint(1, 10)
        }
        
        # Create test case
        test_case = IngestionTestCase(
            schema=schema,
            data=data,
            connection_config=api_connection,
            extraction_params=extraction_params,
            expected_results=expected_results,
            metadata={
                'source_type': DataSourceType.API.value,
                'test_case_name': test_case_name,
                'description': f"Test case for API data ingestion with {len(data)} records"
            }
        )
        
        # Save test case if requested
        if save_files:
            test_case.save(test_case_name, self._output_dir)
        
        return test_case.to_dict()
    
    def generate_error_test_case(
        self,
        source_type: DataSourceType,
        error_type: str,
        source_config: Dict,
        error_config: Dict,
        test_case_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case with ingestion errors.
        
        Args:
            source_type: Type of data source
            error_type: Type of error to generate
            source_config: Configuration for the data source
            error_config: Configuration for the error scenario
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with ingestion errors
        """
        # Generate base test case based on source type
        if source_type == DataSourceType.GCS:
            base_test_case = self.generate_gcs_test_case(
                source_config.get('schema_config', {}),
                source_config.get('data_config', {}),
                source_config.get('connection_config', {}),
                source_config.get('extraction_config', {}),
                test_case_name,
                False  # Don't save yet
            )
        elif source_type == DataSourceType.CLOUD_SQL:
            base_test_case = self.generate_cloudsql_test_case(
                source_config.get('schema_config', {}),
                source_config.get('data_config', {}),
                source_config.get('connection_config', {}),
                source_config.get('extraction_config', {}),
                test_case_name,
                False  # Don't save yet
            )
        elif source_type == DataSourceType.API:
            base_test_case = self.generate_api_test_case(
                source_config.get('schema_config', {}),
                source_config.get('data_config', {}),
                source_config.get('connection_config', {}),
                source_config.get('extraction_config', {}),
                test_case_name,
                False  # Don't save yet
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Generate error scenario
        error_scenario = generate_ingestion_error(error_type, source_type, error_config)
        
        # Update test case with error scenario
        base_test_case['error_scenarios'] = [error_scenario]
        
        # Update expected results for error case
        base_test_case['expected_results']['success'] = False
        base_test_case['expected_results']['error'] = {
            'type': error_type,
            'message': error_scenario['error_message'],
            'code': error_scenario['error_code']
        }
        
        # Add self-healing expectations if applicable
        if 'self_healing' in error_config:
            base_test_case['expected_results']['self_healing'] = {
                'action': error_config['self_healing'].get('action', 'retry'),
                'expected_success': error_config['self_healing'].get('expected_success', True),
                'max_attempts': error_config['self_healing'].get('max_attempts', 3)
            }
        
        # Update metadata
        if 'metadata' in base_test_case:
            base_test_case['metadata']['error_type'] = error_type
            base_test_case['metadata']['description'] = f"Test case for {error_type} during {source_type} data ingestion"
        
        # Save test case if requested
        if save_files:
            # Create IngestionTestCase instance from dictionary
            test_case = IngestionTestCase.from_dict(base_test_case)
            test_case.save(test_case_name, self._output_dir)
            return test_case.to_dict()
        
        return base_test_case
    
    def generate_incremental_test_case(
        self,
        source_type: DataSourceType,
        schema_config: Dict,
        data_config: Dict,
        incremental_config: Dict,
        test_case_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a test case for incremental data ingestion.
        
        Args:
            source_type: Type of data source
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            incremental_config: Configuration for incremental extraction
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case for incremental ingestion
        """
        # Generate schema
        schema, initial_data = generate_schema_data_pair(schema_config, data_config)
        
        # Determine incremental field
        incremental_field = incremental_config.get('incremental_field', 'updated_at')
        
        # Ensure incremental field exists in schema
        if not any(field['name'] == incremental_field for field in schema['fields']):
            # Add incremental field to schema if not present
            schema['fields'].append({
                'name': incremental_field,
                'type': 'timestamp',
                'description': 'Field used for incremental loading'
            })
            
            # Add values to initial data
            start_time = datetime.datetime.now() - datetime.timedelta(days=30)
            initial_data[incremental_field] = [
                (start_time + datetime.timedelta(hours=i)).isoformat()
                for i in range(len(initial_data))
            ]
        
        # Generate batches with incremental changes
        num_batches = incremental_config.get('num_batches', 3)
        batch_size = incremental_config.get('batch_size', len(initial_data) // 2)
        
        batches = []
        latest_timestamp = max(initial_data[incremental_field]) if incremental_field in initial_data.columns else datetime.datetime.now().isoformat()
        
        for i in range(num_batches):
            # Generate a new batch of data
            _, batch_data = generate_schema_data_pair(schema_config, {
                'num_rows': batch_size,
                'random_seed': data_config.get('random_seed', 42) + i + 1
            })
            
            # Calculate new timestamps for incremental field
            if isinstance(latest_timestamp, str):
                latest_time = datetime.datetime.fromisoformat(latest_timestamp)
            else:
                latest_time = latest_timestamp
            
            start_time = latest_time + datetime.timedelta(hours=1)
            batch_data[incremental_field] = [
                (start_time + datetime.timedelta(hours=j)).isoformat()
                for j in range(len(batch_data))
            ]
            
            latest_timestamp = batch_data[incremental_field].iloc[-1]
            
            batches.append({
                'batch_id': i + 1,
                'data': batch_data,
                'incremental_value': latest_timestamp
            })
        
        # Create source-specific configuration
        if source_type == DataSourceType.GCS:
            connection_config = generate_gcs_connection_config(incremental_config.get('connection_config', {}))
            extraction_params = generate_extraction_params(DataSourceType.GCS, {
                **incremental_config.get('extraction_config', {}),
                'incremental': True,
                'incremental_field': incremental_field
            })
        elif source_type == DataSourceType.CLOUD_SQL:
            connection_config = generate_cloudsql_connection_config(incremental_config.get('connection_config', {}))
            extraction_params = generate_extraction_params(DataSourceType.CLOUD_SQL, {
                **incremental_config.get('extraction_config', {}),
                'incremental': True,
                'incremental_field': incremental_field
            })
        elif source_type == DataSourceType.API:
            connection_config = generate_api_connection_config(incremental_config.get('connection_config', {}))
            extraction_params = generate_extraction_params(DataSourceType.API, {
                **incremental_config.get('extraction_config', {}),
                'incremental': True,
                'incremental_field': incremental_field
            })
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Create expected results for each batch
        batch_results = []
        for i, batch in enumerate(batches):
            batch_results.append({
                'batch_id': batch['batch_id'],
                'records_processed': len(batch['data']),
                'success': True,
                'processing_time': random.uniform(0.5, 5.0),
                'incremental_value': batch['incremental_value']
            })
        
        # Create test case
        test_case = {
            'schema': schema,
            'initial_data': initial_data.to_dict(orient='records'),
            'batches': [
                {
                    'batch_id': batch['batch_id'],
                    'data': batch['data'].to_dict(orient='records'),
                    'incremental_value': batch['incremental_value']
                }
                for batch in batches
            ],
            'connection_config': connection_config,
            'extraction_params': extraction_params,
            'expected_results': {
                'initial_load': {
                    'records_processed': len(initial_data),
                    'success': True,
                    'processing_time': random.uniform(0.5, 5.0),
                    'incremental_value': latest_timestamp
                },
                'batch_results': batch_results
            },
            'metadata': {
                'source_type': source_type.value if isinstance(source_type, DataSourceType) else source_type,
                'test_case_name': test_case_name,
                'description': f"Incremental test case for {source_type} with {num_batches} incremental batches",
                'incremental_field': incremental_field
            }
        }
        
        # Save test case if requested
        if save_files:
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            # Save schema
            schema_file = os.path.join(test_case_dir, 'schema.json')
            with open(schema_file, 'w') as f:
                json.dump(schema, f, indent=2)
            
            # Save initial data
            initial_data_file = os.path.join(test_case_dir, 'initial_data.json')
            initial_data.to_json(initial_data_file, orient='records', date_format='iso')
            
            # Save batches
            batches_dir = os.path.join(test_case_dir, 'batches')
            os.makedirs(batches_dir, exist_ok=True)
            
            for batch in batches:
                batch_file = os.path.join(batches_dir, f"batch_{batch['batch_id']}.json")
                batch['data'].to_json(batch_file, orient='records', date_format='iso')
            
            # Save connection config
            conn_file = os.path.join(test_case_dir, 'connection_config.json')
            with open(conn_file, 'w') as f:
                json.dump(connection_config, f, indent=2)
            
            # Save extraction params
            params_file = os.path.join(test_case_dir, 'extraction_params.json')
            with open(params_file, 'w') as f:
                json.dump(extraction_params, f, indent=2)
            
            # Save expected results
            results_file = os.path.join(test_case_dir, 'expected_results.json')
            with open(results_file, 'w') as f:
                json.dump(test_case['expected_results'], f, indent=2)
            
            # Save metadata
            metadata_file = os.path.join(test_case_dir, 'metadata.json')
            with open(metadata_file, 'w') as f:
                json.dump(test_case['metadata'], f, indent=2)
            
            # Update file paths
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'schema_path': schema_file,
                'initial_data_path': initial_data_file,
                'batches_dir': batches_dir,
                'connection_config_path': conn_file,
                'extraction_params_path': params_file,
                'expected_results_path': results_file,
                'metadata_path': metadata_file
            }
        
        return test_case
    
    def generate_comprehensive_ingestion_suite(
        self,
        suite_config: Dict,
        suite_name: str,
        save_files: bool = True
    ) -> Dict:
        """
        Generates a comprehensive test suite for ingestion testing.
        
        Args:
            suite_config: Configuration for the test suite
            suite_name: Name for the test suite
            save_files: Whether to save the test cases to files
            
        Returns:
            Complete test suite with multiple test cases
        """
        # Create output directory for test suite
        suite_dir = os.path.join(self._output_dir, suite_name)
        os.makedirs(suite_dir, exist_ok=True)
        
        suite_results = {
            'suite_name': suite_name,
            'test_cases': {},
            'manifest': {
                'gcs_test_cases': [],
                'cloudsql_test_cases': [],
                'api_test_cases': [],
                'error_test_cases': [],
                'incremental_test_cases': []
            }
        }
        
        # Generate GCS test cases
        if 'gcs_test_cases' in suite_config:
            for tc_config in suite_config['gcs_test_cases']:
                tc_name = tc_config.get('name', f"gcs_{generate_unique_id()}")
                
                test_case = self.generate_gcs_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('connection_config', {}),
                    tc_config.get('extraction_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['gcs_test_cases'].append(tc_name)
        
        # Generate Cloud SQL test cases
        if 'cloudsql_test_cases' in suite_config:
            for tc_config in suite_config['cloudsql_test_cases']:
                tc_name = tc_config.get('name', f"cloudsql_{generate_unique_id()}")
                
                test_case = self.generate_cloudsql_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('connection_config', {}),
                    tc_config.get('extraction_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['cloudsql_test_cases'].append(tc_name)
        
        # Generate API test cases
        if 'api_test_cases' in suite_config:
            for tc_config in suite_config['api_test_cases']:
                tc_name = tc_config.get('name', f"api_{generate_unique_id()}")
                
                test_case = self.generate_api_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('connection_config', {}),
                    tc_config.get('extraction_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['api_test_cases'].append(tc_name)
        
        # Generate error test cases
        if 'error_test_cases' in suite_config:
            for tc_config in suite_config['error_test_cases']:
                tc_name = tc_config.get('name', f"error_{tc_config.get('error_type', 'generic')}_{generate_unique_id()}")
                
                test_case = self.generate_error_test_case(
                    tc_config.get('source_type', DataSourceType.GCS),
                    tc_config.get('error_type', 'connection_failure'),
                    tc_config.get('source_config', {}),
                    tc_config.get('error_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['error_test_cases'].append(tc_name)
        
        # Generate incremental test cases
        if 'incremental_test_cases' in suite_config:
            for tc_config in suite_config['incremental_test_cases']:
                tc_name = tc_config.get('name', f"incremental_{generate_unique_id()}")
                
                test_case = self.generate_incremental_test_case(
                    tc_config.get('source_type', DataSourceType.GCS),
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('incremental_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['incremental_test_cases'].append(tc_name)
        
        # Generate test suite manifest
        if save_files:
            manifest_path = os.path.join(suite_dir, 'manifest.json')
            with open(manifest_path, 'w') as f:
                json.dump(suite_results['manifest'], f, indent=2)
            
            suite_results['manifest_path'] = manifest_path
        
        return suite_results
    
    def save_test_case(self, test_case: Dict, test_case_name: str) -> Dict:
        """
        Saves an ingestion test case to files.
        
        Args:
            test_case: Test case to save
            test_case_name: Name for the test case
            
        Returns:
            Updated test case with file paths
        """
        # Extract components from test case
        test_case_to_save = test_case.copy()
        if 'data' in test_case_to_save and isinstance(test_case_to_save['data'], dict):
            test_case_to_save['data'] = pd.DataFrame.from_dict(test_case_to_save['data'])
        
        # Save test case
        file_paths = save_ingestion_test_case(test_case_to_save, test_case_name, self._output_dir)
        
        # Update test case with file paths
        test_case['file_paths'] = file_paths
        
        return test_case
    
    def load_test_case(self, test_case_name: str) -> Dict:
        """
        Loads a previously saved ingestion test case.
        
        Args:
            test_case_name: Name of the test case
            
        Returns:
            Loaded test case
        """
        return load_ingestion_test_case(test_case_name, self._output_dir)


class IngestionTestCase:
    """Class representing a test case for ingestion testing."""
    
    def __init__(
        self,
        schema: Dict,
        data: pd.DataFrame,
        connection_config: Dict,
        extraction_params: Dict,
        expected_results: Dict,
        error_scenarios: Dict = None,
        metadata: Dict = None
    ):
        """
        Initialize an IngestionTestCase.
        
        Args:
            schema: Schema definition for the test data
            data: Test data as a DataFrame
            connection_config: Connection configuration for the data source
            extraction_params: Parameters for data extraction
            expected_results: Expected results after ingestion
            error_scenarios: Optional error scenarios to test
            metadata: Additional metadata for the test case
        """
        self.schema = schema
        self.data = data
        self.connection_config = connection_config
        self.extraction_params = extraction_params
        self.expected_results = expected_results
        self.error_scenarios = error_scenarios
        self.metadata = metadata or {}
        self.file_paths = {}
    
    def save(self, test_case_name: str, output_dir: str) -> Dict:
        """
        Save the test case to files.
        
        Args:
            test_case_name: Name for the test case
            output_dir: Directory to save the test case
            
        Returns:
            Paths to saved files
        """
        test_case_dict = self.to_dict()
        self.file_paths = save_ingestion_test_case(test_case_dict, test_case_name, output_dir)
        return self.file_paths
    
    def to_dict(self) -> Dict:
        """
        Convert the test case to a dictionary representation.
        
        Returns:
            Dictionary representation of the test case
        """
        return {
            'schema': self.schema,
            'data': self.data,
            'connection_config': self.connection_config,
            'extraction_params': self.extraction_params,
            'expected_results': self.expected_results,
            'error_scenarios': self.error_scenarios,
            'metadata': self.metadata,
            'file_paths': self.file_paths
        }
    
    @classmethod
    def from_dict(cls, test_case_dict: Dict) -> 'IngestionTestCase':
        """
        Create an IngestionTestCase from a dictionary.
        
        Args:
            test_case_dict: Dictionary representation of a test case
            
        Returns:
            IngestionTestCase instance
        """
        # Convert dict to DataFrame if necessary
        data = test_case_dict.get('data')
        if isinstance(data, list):
            data = pd.DataFrame(data)
        
        test_case = cls(
            schema=test_case_dict.get('schema'),
            data=data,
            connection_config=test_case_dict.get('connection_config'),
            extraction_params=test_case_dict.get('extraction_params'),
            expected_results=test_case_dict.get('expected_results'),
            error_scenarios=test_case_dict.get('error_scenarios'),
            metadata=test_case_dict.get('metadata')
        )
        
        if 'file_paths' in test_case_dict:
            test_case.file_paths = test_case_dict['file_paths']
        
        return test_case
    
    @classmethod
    def load(cls, test_case_name: str, input_dir: str) -> 'IngestionTestCase':
        """
        Load a test case from files.
        
        Args:
            test_case_name: Name of the test case
            input_dir: Directory containing the test case
            
        Returns:
            Loaded test case instance
        """
        test_case_dict = load_ingestion_test_case(test_case_name, input_dir)
        
        test_case = cls.from_dict(test_case_dict)
        return test_case