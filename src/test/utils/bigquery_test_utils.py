"""
Provides utility functions and classes for testing BigQuery-related functionality in the self-healing data pipeline.

This module includes mock implementations, test data generators, and helper functions to facilitate testing
BigQuery-dependent components without requiring actual BigQuery resources.
"""

import pytest
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import pandas as pd
import numpy as np
from google.cloud.bigquery import SchemaField
import google.api_core.exceptions

# Internal imports
from src.test.utils.test_helpers import (
    create_temp_file,
    create_test_dataframe,
    TestResourceManager,
    generate_unique_id
)
from src.test.utils.gcp_test_utils import (
    create_mock_bigquery_client,
    create_mock_bigquery_dataset,
    create_mock_bigquery_table,
    TEST_PROJECT_ID,
    TEST_DATASET_ID
)
from src.backend.utils.storage.bigquery_client import BigQueryClient

# Test constants
TEST_PROJECT_ID = "test-project"
TEST_DATASET_ID = "test_dataset"
TEST_TABLE_ID = "test_table"
TEST_LOCATION = "us-central1"


def create_bigquery_test_client(mock_client: MagicMock = None, project_id: str = None, location: str = None) -> BigQueryClient:
    """
    Creates a BigQueryClient instance configured for testing.
    
    Args:
        mock_client: Optional mock client to use internally
        project_id: Project ID to use for the client
        location: Location to use for the client
        
    Returns:
        Configured BigQueryClient for testing
    """
    client = BigQueryClient()
    
    # Replace the internal client with a mock if provided
    if mock_client:
        client._client = mock_client
    else:
        client._client = MagicMock()
    
    # Set project ID and location
    client.project_id = project_id or TEST_PROJECT_ID
    client.location = location or TEST_LOCATION
    
    return client


def create_test_schema(field_definitions: List = None) -> List[SchemaField]:
    """
    Creates a test BigQuery schema definition.
    
    Args:
        field_definitions: List of field definitions, each being a dict with name, type, etc.
        
    Returns:
        List of BigQuery SchemaField objects
    """
    if field_definitions:
        return [
            SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description', ''),
                fields=create_test_schema(field.get('fields', [])) if field.get('fields') else None
            )
            for field in field_definitions
        ]
    
    # Default schema if none provided
    return [
        SchemaField('id', 'INTEGER', 'REQUIRED', 'Unique identifier'),
        SchemaField('name', 'STRING', 'NULLABLE', 'Name field'),
        SchemaField('value', 'FLOAT', 'NULLABLE', 'Numeric value'),
        SchemaField('is_active', 'BOOLEAN', 'NULLABLE', 'Active status'),
        SchemaField('created_at', 'TIMESTAMP', 'NULLABLE', 'Creation timestamp')
    ]


def create_test_query_job(
    result_data: pd.DataFrame = None,
    job_id: str = None,
    state: str = "DONE",
    error_result: Dict = None
) -> MagicMock:
    """
    Creates a mock BigQuery query job for testing.
    
    Args:
        result_data: DataFrame to return as the query result
        job_id: ID for the job
        state: State of the job (e.g., RUNNING, DONE)
        error_result: Optional error result if the job failed
        
    Returns:
        Mock query job
    """
    if result_data is None:
        result_data = pd.DataFrame()
    
    if job_id is None:
        job_id = f"job_{generate_unique_id()}"
    
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    
    # Configure result method
    result_rows = MagicMock()
    result_rows.to_dataframe.return_value = result_data
    job.result.return_value = result_rows
    
    # Set error_result if specified
    if error_result:
        job.error_result = error_result
    
    # Configure to_dataframe method
    job.to_dataframe.return_value = result_data
    
    return job


def create_test_load_job(
    job_id: str = None,
    state: str = "DONE",
    output_rows: int = 100,
    error_result: Dict = None
) -> MagicMock:
    """
    Creates a mock BigQuery load job for testing.
    
    Args:
        job_id: ID for the job
        state: State of the job (e.g., RUNNING, DONE)
        output_rows: Number of output rows
        error_result: Optional error result if the job failed
        
    Returns:
        Mock load job
    """
    if job_id is None:
        job_id = f"job_{generate_unique_id()}"
    
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    job.output_rows = output_rows
    
    # Set error_result if specified
    if error_result:
        job.error_result = error_result
    
    return job


def create_test_extract_job(
    job_id: str = None,
    state: str = "DONE",
    error_result: Dict = None
) -> MagicMock:
    """
    Creates a mock BigQuery extract job for testing.
    
    Args:
        job_id: ID for the job
        state: State of the job (e.g., RUNNING, DONE)
        error_result: Optional error result if the job failed
        
    Returns:
        Mock extract job
    """
    if job_id is None:
        job_id = f"job_{generate_unique_id()}"
    
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    
    # Set error_result if specified
    if error_result:
        job.error_result = error_result
    
    return job


def create_test_copy_job(
    job_id: str = None,
    state: str = "DONE",
    error_result: Dict = None
) -> MagicMock:
    """
    Creates a mock BigQuery copy job for testing.
    
    Args:
        job_id: ID for the job
        state: State of the job (e.g., RUNNING, DONE)
        error_result: Optional error result if the job failed
        
    Returns:
        Mock copy job
    """
    if job_id is None:
        job_id = f"job_{generate_unique_id()}"
    
    job = MagicMock()
    job.job_id = job_id
    job.state = state
    
    # Set error_result if specified
    if error_result:
        job.error_result = error_result
    
    return job


def create_test_row_iterator(
    data: pd.DataFrame = None,
    schema: List = None,
    total_rows: int = None
) -> MagicMock:
    """
    Creates a mock BigQuery RowIterator for testing.
    
    Args:
        data: DataFrame to use as the result data
        schema: Schema for the result
        total_rows: Total number of rows
        
    Returns:
        Mock RowIterator
    """
    if data is None:
        data = pd.DataFrame()
    
    if total_rows is None:
        total_rows = len(data)
    
    row_iterator = MagicMock()
    row_iterator.schema = schema
    row_iterator.total_rows = total_rows
    row_iterator.to_dataframe.return_value = data
    
    # Configure iterator methods
    rows = data.to_dict('records')
    iterator = iter(rows)
    row_iterator.__iter__.return_value = iterator
    row_iterator.__next__.side_effect = lambda: next(iterator)
    
    return row_iterator


def generate_test_table_data(
    schema: List,
    num_rows: int = 100,
    value_generators: Dict = None
) -> pd.DataFrame:
    """
    Generates test data for a BigQuery table based on schema.
    
    Args:
        schema: List of SchemaField objects or dictionaries
        num_rows: Number of rows to generate
        value_generators: Custom value generators by field name
        
    Returns:
        Generated test data as a DataFrame
    """
    if value_generators is None:
        value_generators = {}
    
    # Extract column specifications
    columns_spec = {}
    
    for field in schema:
        if isinstance(field, SchemaField):
            field_name = field.name
            field_type = field.field_type
        else:
            field_name = field.get('name')
            field_type = field.get('type', 'STRING')
        
        # Map BigQuery types to column specifications
        if field_name in value_generators:
            # Use custom generator
            columns_spec[field_name] = value_generators[field_name]
            continue
        
        if field_type == 'STRING':
            columns_spec[field_name] = {'type': 'str', 'length': 10}
        elif field_type == 'INTEGER':
            columns_spec[field_name] = {'type': 'int', 'min': 0, 'max': 1000}
        elif field_type == 'FLOAT' or field_type == 'NUMERIC':
            columns_spec[field_name] = {'type': 'float', 'min': 0, 'max': 1000}
        elif field_type == 'BOOLEAN':
            columns_spec[field_name] = {'type': 'bool'}
        elif field_type == 'TIMESTAMP' or field_type == 'DATETIME':
            columns_spec[field_name] = {'type': 'datetime'}
        elif field_type == 'DATE':
            columns_spec[field_name] = {'type': 'datetime'}  # Will convert to date later
        else:
            # Default to string for unknown types
            columns_spec[field_name] = {'type': 'str', 'length': 10}
    
    # Generate the data
    df = create_test_dataframe(columns_spec, num_rows)
    
    return df


def simulate_bigquery_error(error_type: str, message: str = None, code: int = None) -> Exception:
    """
    Simulates a BigQuery API error for testing error handling.
    
    Args:
        error_type: Type of error to simulate
        message: Error message
        code: Error code
        
    Returns:
        BigQuery API exception
    """
    if message is None:
        message = f"Simulated {error_type} error for testing."
    
    if error_type == "not_found":
        return google.api_core.exceptions.NotFound(message, code)
    elif error_type == "permission_denied":
        return google.api_core.exceptions.PermissionDenied(message, code)
    elif error_type == "invalid_argument":
        return google.api_core.exceptions.InvalidArgument(message, code)
    elif error_type == "failed_precondition":
        return google.api_core.exceptions.FailedPrecondition(message, code)
    elif error_type == "already_exists":
        return google.api_core.exceptions.AlreadyExists(message, code)
    elif error_type == "resource_exhausted":
        return google.api_core.exceptions.ResourceExhausted(message, code)
    elif error_type == "cancelled":
        return google.api_core.exceptions.Cancelled(message, code)
    elif error_type == "deadline_exceeded":
        return google.api_core.exceptions.DeadlineExceeded(message, code)
    else:
        return google.api_core.exceptions.GoogleAPIError(message, code)


def patch_bigquery_client(mock_client: MagicMock = None) -> patch:
    """
    Creates a patch for the BigQueryClient class.
    
    Args:
        mock_client: Optional mock client to use for the patch
        
    Returns:
        Patch context manager
    """
    if mock_client is None:
        mock_client = MagicMock()
    
    return patch('src.backend.utils.storage.bigquery_client.BigQueryClient', return_value=mock_client)


def create_bigquery_table_reference(project_id: str = None, dataset_id: str = None, table_id: str = None) -> str:
    """
    Creates a formatted BigQuery table reference string.
    
    Args:
        project_id: Project ID
        dataset_id: Dataset ID
        table_id: Table ID
        
    Returns:
        Formatted table reference (project.dataset.table)
    """
    project = project_id or TEST_PROJECT_ID
    dataset = dataset_id or TEST_DATASET_ID
    table = table_id or TEST_TABLE_ID
    
    return f"{project}.{dataset}.{table}"


class BigQueryTestHelper:
    """Helper class for BigQuery testing with utility methods."""
    
    def __init__(self):
        """Initialize the BigQuery test helper."""
        self._resource_manager = TestResourceManager()
        self._mock_datasets = {}
        self._mock_tables = {}
        self._mock_query_results = {}
    
    def create_mock_client(self) -> MagicMock:
        """
        Create a mock BigQuery client.
        
        Returns:
            Mock BigQuery client
        """
        return create_mock_bigquery_client(
            datasets=self._mock_datasets,
            tables=self._mock_tables,
            query_results=self._mock_query_results
        )
    
    def add_mock_dataset(self, dataset_id: str, metadata: Dict = None) -> MagicMock:
        """
        Add a mock dataset to the test environment.
        
        Args:
            dataset_id: ID of the dataset
            metadata: Additional metadata for the dataset
            
        Returns:
            Mock dataset
        """
        dataset = create_mock_bigquery_dataset(dataset_id, TEST_PROJECT_ID, metadata=metadata)
        self._mock_datasets[dataset_id] = dataset
        return dataset
    
    def add_mock_table(self, dataset_id: str, table_id: str, schema: List = None, data: pd.DataFrame = None) -> MagicMock:
        """
        Add a mock table to the test environment.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            schema: Schema for the table
            data: Data for the table
            
        Returns:
            Mock table
        """
        # Create the table
        table = create_mock_bigquery_table(table_id, dataset_id, TEST_PROJECT_ID, schema, data)
        
        # Add to tables dict
        key = (dataset_id, table_id)
        self._mock_tables[key] = table
        
        # Add to dataset if it exists
        if dataset_id in self._mock_datasets:
            dataset = self._mock_datasets[dataset_id]
            dataset.tables = getattr(dataset, 'tables', {})
            dataset.tables[table_id] = table
        
        return table
    
    def register_query_result(self, query: str, result: pd.DataFrame):
        """
        Register a predefined result for a query.
        
        Args:
            query: SQL query
            result: Result to return for the query
        """
        self._mock_query_results[query] = result
    
    def create_temp_dataset(self, dataset_id: str = None, metadata: Dict = None) -> MagicMock:
        """
        Create a temporary dataset for testing.
        
        Args:
            dataset_id: ID of the dataset
            metadata: Additional metadata for the dataset
            
        Returns:
            Mock dataset
        """
        if dataset_id is None:
            dataset_id = f"temp_dataset_{generate_unique_id()}"
        
        dataset = self.add_mock_dataset(dataset_id, metadata)
        
        # Register for cleanup
        self._resource_manager.add_resource(dataset_id, lambda d: self._mock_datasets.pop(d, None))
        
        return dataset
    
    def create_temp_table(
        self,
        dataset_id: str = None,
        table_id: str = None,
        schema: List = None,
        data: pd.DataFrame = None,
        num_rows: int = None
    ) -> MagicMock:
        """
        Create a temporary table for testing.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            schema: Schema for the table
            data: Data for the table
            num_rows: Number of rows to generate if data not provided
            
        Returns:
            Mock table
        """
        if dataset_id is None:
            dataset_id = f"temp_dataset_{generate_unique_id()}"
            
        if table_id is None:
            table_id = f"temp_table_{generate_unique_id()}"
        
        # Create dataset if it doesn't exist
        if dataset_id not in self._mock_datasets:
            self.create_temp_dataset(dataset_id)
        
        # Generate data if not provided but schema and num_rows are
        if data is None and schema is not None and num_rows is not None:
            data = generate_test_table_data(schema, num_rows)
        
        table = self.add_mock_table(dataset_id, table_id, schema, data)
        
        # Register for cleanup
        key = (dataset_id, table_id)
        self._resource_manager.add_resource(key, lambda k: self._mock_tables.pop(k, None))
        
        return table
    
    def create_temp_view(
        self,
        dataset_id: str = None,
        view_id: str = None,
        query: str = None,
        data: pd.DataFrame = None
    ) -> MagicMock:
        """
        Create a temporary view for testing.
        
        Args:
            dataset_id: ID of the dataset
            view_id: ID of the view
            query: SQL query for the view
            data: Data to return when querying the view
            
        Returns:
            Mock view
        """
        if dataset_id is None:
            dataset_id = f"temp_dataset_{generate_unique_id()}"
            
        if view_id is None:
            view_id = f"temp_view_{generate_unique_id()}"
        
        # Create dataset if it doesn't exist
        if dataset_id not in self._mock_datasets:
            self.create_temp_dataset(dataset_id)
        
        # Create table/view
        view = self.add_mock_table(dataset_id, view_id, None, data)
        
        # Configure as view
        view.table_type = 'VIEW'
        if query:
            view.view_query = query
            # Register query result if data provided
            if data is not None:
                self.register_query_result(query, data)
        
        # Register for cleanup
        key = (dataset_id, view_id)
        self._resource_manager.add_resource(key, lambda k: self._mock_tables.pop(k, None))
        
        return view
    
    def cleanup(self):
        """Clean up all test resources."""
        self._resource_manager.cleanup()
        self._mock_datasets.clear()
        self._mock_tables.clear()
        self._mock_query_results.clear()
    
    def __enter__(self):
        """Enter context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.cleanup()
        return None


class BigQueryEmulator:
    """In-memory emulator for BigQuery that provides local testing capabilities."""
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the BigQuery emulator.
        
        Args:
            project_id: Project ID
            location: Location
        """
        self.project_id = project_id or TEST_PROJECT_ID
        self.location = location or TEST_LOCATION
        self._datasets = {}
        self._tables = {}
        self._query_results = {}
        self._is_running = False
    
    def start(self):
        """Start the BigQuery emulator."""
        if self._is_running:
            return
        
        # Initialize emulator resources
        self._is_running = True
    
    def stop(self):
        """Stop the BigQuery emulator."""
        if not self._is_running:
            return
        
        # Clean up emulator resources
        self._is_running = False
    
    def reset(self):
        """Reset the emulator state."""
        if not self._is_running:
            self.start()
        
        # Clear all datasets, tables, and query results
        self._datasets.clear()
        self._tables.clear()
        self._query_results.clear()
    
    def create_dataset(self, dataset_id: str, metadata: Dict = None) -> MagicMock:
        """
        Create a dataset in the emulator.
        
        Args:
            dataset_id: ID of the dataset
            metadata: Additional metadata for the dataset
            
        Returns:
            Mock dataset
        """
        if not self._is_running:
            self.start()
        
        dataset = create_mock_bigquery_dataset(dataset_id, self.project_id, metadata=metadata)
        self._datasets[dataset_id] = dataset
        return dataset
    
    def get_dataset(self, dataset_id: str) -> MagicMock:
        """
        Get a dataset from the emulator.
        
        Args:
            dataset_id: ID of the dataset
            
        Returns:
            Mock dataset
        """
        if not self._is_running:
            self.start()
        
        if dataset_id in self._datasets:
            return self._datasets[dataset_id]
        
        raise google.api_core.exceptions.NotFound(f"Dataset {dataset_id} not found")
    
    def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List = None,
        data: pd.DataFrame = None
    ) -> MagicMock:
        """
        Create a table in the emulator.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            schema: Schema for the table
            data: Data for the table
            
        Returns:
            Mock table
        """
        if not self._is_running:
            self.start()
        
        # Get or create the dataset
        try:
            dataset = self.get_dataset(dataset_id)
        except google.api_core.exceptions.NotFound:
            dataset = self.create_dataset(dataset_id)
        
        # Create the table
        table = create_mock_bigquery_table(table_id, dataset_id, self.project_id, schema, data)
        
        # Add to tables dict
        key = (dataset_id, table_id)
        self._tables[key] = table
        
        # Add to dataset
        dataset.tables = getattr(dataset, 'tables', {})
        dataset.tables[table_id] = table
        
        return table
    
    def get_table(self, dataset_id: str, table_id: str) -> MagicMock:
        """
        Get a table from the emulator.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            
        Returns:
            Mock table
        """
        if not self._is_running:
            self.start()
        
        # Get the dataset
        dataset = self.get_dataset(dataset_id)
        
        # Check for the table
        key = (dataset_id, table_id)
        if key in self._tables:
            return self._tables[key]
        
        raise google.api_core.exceptions.NotFound(f"Table {table_id} not found in dataset {dataset_id}")
    
    def execute_query(self, query: str, parameters: Dict = None) -> pd.DataFrame:
        """
        Execute a query in the emulator.
        
        Args:
            query: SQL query
            parameters: Query parameters
            
        Returns:
            Query results
        """
        if not self._is_running:
            self.start()
        
        # Check if we have a predefined result for this query
        if query in self._query_results:
            return self._query_results[query]
        
        # For simple SELECT queries, attempt to execute against in-memory tables
        if query.strip().upper().startswith("SELECT"):
            # This is a simplified implementation - in a real emulator, you would
            # parse the SQL and execute it against the in-memory tables
            # For now, just return an empty DataFrame
            return pd.DataFrame()
        
        # Raise error for other query types not supported by emulator
        raise NotImplementedError(f"Query execution not supported by emulator: {query}")
    
    def register_query_result(self, query: str, result: pd.DataFrame):
        """
        Register a predefined result for a query.
        
        Args:
            query: SQL query
            result: Result to return for the query
        """
        if not self._is_running:
            self.start()
        
        self._query_results[query] = result
    
    def get_client(self) -> BigQueryClient:
        """
        Get a BigQuery client configured to use the emulator.
        
        Returns:
            BigQuery client
        """
        if not self._is_running:
            self.start()
        
        # Create a mock client
        mock_client = create_mock_bigquery_client(
            datasets=self._datasets,
            tables=self._tables,
            query_results=self._query_results
        )
        
        # Wrap in our client class
        return create_bigquery_test_client(mock_client, self.project_id, self.location)
    
    def __enter__(self):
        """Enter context manager."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.stop()
        return None