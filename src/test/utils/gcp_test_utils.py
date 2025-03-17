"""
Provides utility functions and classes for testing Google Cloud Platform (GCP) services in the self-healing data pipeline.

This module includes mock implementations, emulators, and helper functions to facilitate testing GCP-dependent components
without requiring actual GCP resources or credentials.
"""

import pytest
from unittest.mock import MagicMock, patch
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import os
import tempfile
import json
import pandas as pd

# Internal imports
from src.test.utils.test_helpers import (
    create_temp_file,
    create_temp_directory,
    TestResourceManager,
    generate_unique_id
)
from src.backend.utils.storage.gcs_client import GCSClient
from src.backend.utils.storage.bigquery_client import BigQueryClient
from src.backend.utils.ml.vertex_client import VertexAIClient

# External imports
import google.cloud.storage  # version 2.9.0+
import google.cloud.bigquery  # version 3.11.0+
import google.cloud.aiplatform  # version 1.25.0+
import google.api_core.exceptions  # version 2.10.0+

# Default test constants
TEST_PROJECT_ID = "test-project"
TEST_LOCATION = "us-central1"
TEST_DATASET_ID = "test_dataset"
TEST_BUCKET_NAME = "test-bucket"
TEST_MODEL_ID = "test-model"


def create_mock_gcs_client(buckets: Dict = None, blobs: Dict = None) -> MagicMock:
    """
    Creates a mock Google Cloud Storage client for testing.
    
    Args:
        buckets: Dictionary of bucket name to mock bucket object
        blobs: Dictionary of (bucket_name, blob_name) to mock blob object
        
    Returns:
        Configured mock GCS client
    """
    if buckets is None:
        buckets = {}
    if blobs is None:
        blobs = {}
    
    mock_client = MagicMock(spec=google.cloud.storage.Client)
    
    # Configure bucket method
    def mock_bucket(bucket_name):
        if bucket_name in buckets:
            return buckets[bucket_name]
        
        # Create a new mock bucket if it doesn't exist
        mock_bucket = MagicMock()
        mock_bucket.name = bucket_name
        buckets[bucket_name] = mock_bucket
        return mock_bucket
    
    mock_client.bucket = mock_bucket
    
    # Configure get_bucket method
    def mock_get_bucket(bucket_name):
        if bucket_name in buckets:
            return buckets[bucket_name]
        raise google.api_core.exceptions.NotFound(f"Bucket {bucket_name} not found")
    
    mock_client.get_bucket = mock_get_bucket
    
    # Configure list_buckets method
    mock_client.list_buckets.return_value = list(buckets.values())
    
    # Configure each bucket with blob and list_blobs methods
    for bucket_name, bucket in buckets.items():
        # Filter blobs for this bucket
        bucket_blobs = {
            blob_name: blob 
            for (b_name, blob_name), blob in blobs.items() 
            if b_name == bucket_name
        }
        
        def mock_blob(blob_name, bucket=bucket_name, bucket_blobs=bucket_blobs):
            key = (bucket, blob_name)
            if key in blobs:
                return blobs[key]
            
            # Create a new mock blob if it doesn't exist
            mock_blob = MagicMock()
            mock_blob.name = blob_name
            mock_blob.bucket = bucket
            blobs[key] = mock_blob
            return mock_blob
        
        bucket.blob = mock_blob
        
        # Configure list_blobs method
        bucket.list_blobs.return_value = [
            blob for (b_name, _), blob in blobs.items() if b_name == bucket_name
        ]
    
    return mock_client


def create_mock_bigquery_client(datasets: Dict = None, tables: Dict = None, query_results: Dict = None) -> MagicMock:
    """
    Creates a mock BigQuery client for testing.
    
    Args:
        datasets: Dictionary of dataset_id to mock dataset object
        tables: Dictionary of (dataset_id, table_id) to mock table object
        query_results: Dictionary of query string to result
        
    Returns:
        Configured mock BigQuery client
    """
    if datasets is None:
        datasets = {}
    if tables is None:
        tables = {}
    if query_results is None:
        query_results = {}
    
    mock_client = MagicMock(spec=google.cloud.bigquery.Client)
    
    # Configure dataset method
    def mock_dataset(dataset_ref):
        dataset_id = dataset_ref if isinstance(dataset_ref, str) else dataset_ref.dataset_id
        if dataset_id in datasets:
            return datasets[dataset_id]
        
        # Create a new mock dataset if it doesn't exist
        mock_dataset = MagicMock()
        mock_dataset.dataset_id = dataset_id
        mock_dataset.project = TEST_PROJECT_ID
        datasets[dataset_id] = mock_dataset
        return mock_dataset
    
    mock_client.dataset = mock_dataset
    
    # Configure get_dataset method
    def mock_get_dataset(dataset_ref):
        dataset_id = dataset_ref if isinstance(dataset_ref, str) else dataset_ref.dataset_id
        if dataset_id in datasets:
            return datasets[dataset_id]
        raise google.api_core.exceptions.NotFound(f"Dataset {dataset_id} not found")
    
    mock_client.get_dataset = mock_get_dataset
    
    # Configure list_datasets method
    mock_client.list_datasets.return_value = list(datasets.values())
    
    # Configure table methods
    def mock_table(table_ref):
        if isinstance(table_ref, str):
            # Handle string format like 'project.dataset.table'
            parts = table_ref.split('.')
            if len(parts) == 3:
                project, dataset_id, table_id = parts
            elif len(parts) == 2:
                dataset_id, table_id = parts
                project = TEST_PROJECT_ID
            else:
                raise ValueError(f"Invalid table reference: {table_ref}")
        else:
            # Handle TableReference object
            dataset_id = table_ref.dataset_id
            table_id = table_ref.table_id
            project = getattr(table_ref, 'project', TEST_PROJECT_ID)
        
        key = (dataset_id, table_id)
        if key in tables:
            return tables[key]
        
        # Create a new mock table if it doesn't exist
        mock_table = MagicMock()
        mock_table.table_id = table_id
        mock_table.dataset_id = dataset_id
        mock_table.project = project
        tables[key] = mock_table
        return mock_table
    
    mock_client.table = mock_table
    
    # Configure get_table method
    def mock_get_table(table_ref):
        if isinstance(table_ref, str):
            # Handle string format like 'project.dataset.table'
            parts = table_ref.split('.')
            if len(parts) == 3:
                project, dataset_id, table_id = parts
            elif len(parts) == 2:
                dataset_id, table_id = parts
                project = TEST_PROJECT_ID
            else:
                raise ValueError(f"Invalid table reference: {table_ref}")
        else:
            # Handle TableReference object
            dataset_id = table_ref.dataset_id
            table_id = table_ref.table_id
            project = getattr(table_ref, 'project', TEST_PROJECT_ID)
        
        key = (dataset_id, table_id)
        if key in tables:
            return tables[key]
        raise google.api_core.exceptions.NotFound(f"Table {dataset_id}.{table_id} not found")
    
    mock_client.get_table = mock_get_table
    
    # Configure list_tables method
    def mock_list_tables(dataset_ref):
        dataset_id = dataset_ref if isinstance(dataset_ref, str) else dataset_ref.dataset_id
        return [table for (ds_id, _), table in tables.items() if ds_id == dataset_id]
    
    mock_client.list_tables = mock_list_tables
    
    # Configure query method
    def mock_query(query, job_config=None):
        query_job = MagicMock()
        
        if query in query_results:
            result = query_results[query]
            query_job.result.return_value = result
            
            # Configure to_dataframe
            if hasattr(result, 'to_dataframe'):
                query_job.to_dataframe.return_value = result.to_dataframe()
            else:
                query_job.to_dataframe.return_value = result
        else:
            # Default empty result
            query_job.result.return_value = []
            query_job.to_dataframe.return_value = pd.DataFrame()
        
        return query_job
    
    mock_client.query = mock_query
    
    # Configure job creation methods
    mock_client.create_job = MagicMock(return_value=MagicMock())
    mock_client.extract_job = MagicMock(return_value=MagicMock())
    mock_client.load_job = MagicMock(return_value=MagicMock())
    mock_client.copy_job = MagicMock(return_value=MagicMock())
    
    return mock_client


def create_mock_vertex_ai_client(models: Dict = None, endpoints: Dict = None, prediction_results: Dict = None) -> MagicMock:
    """
    Creates a mock Vertex AI client for testing.
    
    Args:
        models: Dictionary of model_id to mock model object
        endpoints: Dictionary of endpoint_id to mock endpoint object
        prediction_results: Dictionary of (endpoint_id, instance) to prediction result
        
    Returns:
        Configured mock Vertex AI client
    """
    if models is None:
        models = {}
    if endpoints is None:
        endpoints = {}
    if prediction_results is None:
        prediction_results = {}
    
    mock_client = MagicMock(spec=google.cloud.aiplatform.Model)
    
    # Configure get_model method
    def mock_get_model(model_id):
        if model_id in models:
            return models[model_id]
        raise google.api_core.exceptions.NotFound(f"Model {model_id} not found")
    
    mock_client.get_model = mock_get_model
    
    # Configure list_models method
    mock_client.list_models.return_value = list(models.values())
    
    # Configure get_endpoint method
    def mock_get_endpoint(endpoint_id):
        if endpoint_id in endpoints:
            return endpoints[endpoint_id]
        raise google.api_core.exceptions.NotFound(f"Endpoint {endpoint_id} not found")
    
    mock_client.get_endpoint = mock_get_endpoint
    
    # Configure list_endpoints method
    mock_client.list_endpoints.return_value = list(endpoints.values())
    
    # Configure predict method
    def mock_predict(endpoint_id, instances, parameters=None):
        key = (endpoint_id, str(instances))
        if key in prediction_results:
            return prediction_results[key]
        
        # Generate a default prediction if not found
        mock_prediction = {
            "predictions": [{"result": f"Predicted value for instance {i}"} for i in range(len(instances))],
            "deployed_model_id": f"model-{endpoint_id}",
            "model_version_id": "1",
            "model_resource_name": f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/models/{TEST_MODEL_ID}"
        }
        
        return mock_prediction
    
    mock_client.predict = mock_predict
    
    # Configure model training and deployment methods
    mock_client.create_model = MagicMock(return_value=MagicMock())
    mock_client.create_endpoint = MagicMock(return_value=MagicMock())
    mock_client.deploy_model = MagicMock(return_value=MagicMock())
    
    return mock_client


def create_mock_gcs_bucket(bucket_name: str, blobs: Dict = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock GCS bucket object for testing.
    
    Args:
        bucket_name: Name of the bucket
        blobs: Dictionary of blob_name to mock blob
        metadata: Dictionary of metadata attributes
        
    Returns:
        Configured mock GCS bucket
    """
    if blobs is None:
        blobs = {}
    if metadata is None:
        metadata = {}
    
    mock_bucket = MagicMock(spec=google.cloud.storage.Bucket)
    mock_bucket.name = bucket_name
    
    # Configure blob method
    def mock_blob(blob_name):
        if blob_name in blobs:
            return blobs[blob_name]
        
        # Create a new mock blob if it doesn't exist
        mock_blob = MagicMock()
        mock_blob.name = blob_name
        mock_blob.bucket = mock_bucket
        blobs[blob_name] = mock_blob
        return mock_blob
    
    mock_bucket.blob = mock_blob
    
    # Configure list_blobs method
    mock_bucket.list_blobs.return_value = list(blobs.values())
    
    # Configure other bucket methods
    mock_bucket.create.return_value = None
    mock_bucket.reload.return_value = None
    mock_bucket.update.return_value = None
    mock_bucket.delete.return_value = None
    
    # Set metadata attributes
    for key, value in metadata.items():
        setattr(mock_bucket, key, value)
    
    return mock_bucket


def create_mock_gcs_blob(blob_name: str, content: bytes = None, content_type: str = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock GCS blob object for testing.
    
    Args:
        blob_name: Name of the blob
        content: Content of the blob
        content_type: Content type of the blob
        metadata: Dictionary of metadata attributes
        
    Returns:
        Configured mock GCS blob
    """
    if content is None:
        content = b""
    if content_type is None:
        content_type = "application/octet-stream"
    if metadata is None:
        metadata = {}
    
    mock_blob = MagicMock(spec=google.cloud.storage.Blob)
    mock_blob.name = blob_name
    mock_blob.content_type = content_type
    
    # Configure download methods
    mock_blob.download_as_bytes.return_value = content
    mock_blob.download_as_string.return_value = content.decode('utf-8') if isinstance(content, bytes) else content
    
    def mock_download_to_filename(filename):
        with open(filename, 'wb') as f:
            f.write(content if isinstance(content, bytes) else content.encode('utf-8'))
    
    mock_blob.download_to_filename.side_effect = mock_download_to_filename
    
    # Configure upload methods
    def mock_upload_from_filename(filename):
        with open(filename, 'rb') as f:
            nonlocal content
            content = f.read()
    
    mock_blob.upload_from_filename.side_effect = mock_upload_from_filename
    
    def mock_upload_from_string(data, content_type=None):
        nonlocal content
        content = data.encode('utf-8') if isinstance(data, str) else data
        if content_type:
            mock_blob.content_type = content_type
    
    mock_blob.upload_from_string.side_effect = mock_upload_from_string
    
    # Configure other blob methods
    mock_blob.reload.return_value = None
    mock_blob.delete.return_value = None
    
    # Set metadata attributes
    mock_blob.metadata = metadata.copy()
    for key, value in metadata.items():
        setattr(mock_blob, key, value)
    
    return mock_blob


def create_mock_bigquery_dataset(dataset_id: str, project_id: str = None, tables: Dict = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock BigQuery dataset object for testing.
    
    Args:
        dataset_id: ID of the dataset
        project_id: ID of the project
        tables: Dictionary of table_id to mock table
        metadata: Dictionary of metadata attributes
        
    Returns:
        Configured mock BigQuery dataset
    """
    if project_id is None:
        project_id = TEST_PROJECT_ID
    if tables is None:
        tables = {}
    if metadata is None:
        metadata = {}
    
    mock_dataset = MagicMock(spec=google.cloud.bigquery.Dataset)
    mock_dataset.dataset_id = dataset_id
    mock_dataset.project = project_id
    
    # Configure table method
    def mock_table(table_id):
        if table_id in tables:
            return tables[table_id]
        
        # Create a new mock table if it doesn't exist
        mock_table = MagicMock()
        mock_table.table_id = table_id
        mock_table.dataset_id = dataset_id
        mock_table.project = project_id
        tables[table_id] = mock_table
        return mock_table
    
    mock_dataset.table = mock_table
    
    # Configure list_tables method
    mock_dataset.list_tables.return_value = list(tables.values())
    
    # Configure other dataset methods
    mock_dataset.create.return_value = None
    mock_dataset.reload.return_value = None
    mock_dataset.update.return_value = None
    mock_dataset.delete.return_value = None
    
    # Set metadata attributes
    for key, value in metadata.items():
        setattr(mock_dataset, key, value)
    
    return mock_dataset


def create_mock_bigquery_table(table_id: str, dataset_id: str, project_id: str = None, schema: List = None, data: pd.DataFrame = None) -> MagicMock:
    """
    Creates a mock BigQuery table object for testing.
    
    Args:
        table_id: ID of the table
        dataset_id: ID of the dataset
        project_id: ID of the project
        schema: Schema of the table
        data: DataFrame containing the table data
        
    Returns:
        Configured mock BigQuery table
    """
    if project_id is None:
        project_id = TEST_PROJECT_ID
    if schema is None and data is not None:
        # Generate schema from DataFrame
        schema = []
        for col_name, dtype in data.dtypes.items():
            field_type = "STRING"
            if pd.api.types.is_integer_dtype(dtype):
                field_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                field_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_dtype(dtype):
                field_type = "TIMESTAMP"
            
            schema.append({"name": col_name, "type": field_type})
    
    mock_table = MagicMock(spec=google.cloud.bigquery.Table)
    mock_table.table_id = table_id
    mock_table.dataset_id = dataset_id
    mock_table.project = project_id
    mock_table.schema = schema
    
    # Configure to_dataframe method
    if data is not None:
        mock_table.to_dataframe.return_value = data
    else:
        mock_table.to_dataframe.return_value = pd.DataFrame()
    
    # Configure other table methods
    mock_table.reload.return_value = None
    mock_table.update.return_value = None
    mock_table.delete.return_value = None
    
    return mock_table


def create_mock_vertex_model(model_id: str, display_name: str = None, model_type: str = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock Vertex AI model object for testing.
    
    Args:
        model_id: ID of the model
        display_name: Display name of the model
        model_type: Type of the model
        metadata: Dictionary of metadata attributes
        
    Returns:
        Configured mock Vertex AI model
    """
    if display_name is None:
        display_name = f"Test Model {model_id}"
    if model_type is None:
        model_type = "classification"
    if metadata is None:
        metadata = {}
    
    mock_model = MagicMock(spec=google.cloud.aiplatform.Model)
    mock_model.name = model_id
    mock_model.display_name = display_name
    mock_model.model_type = model_type
    
    # Set metadata attributes
    for key, value in metadata.items():
        setattr(mock_model, key, value)
    
    # Configure predict method
    def mock_predict(instances, parameters=None):
        # Generate a default prediction
        return {
            "predictions": [{"result": f"Predicted value for instance {i}"} for i in range(len(instances))],
            "deployed_model_id": model_id,
            "model_version_id": "1",
            "model_resource_name": f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/models/{model_id}"
        }
    
    mock_model.predict = mock_predict
    
    # Configure deployment and version methods
    mock_model.deploy.return_value = MagicMock()
    mock_model.delete.return_value = None
    mock_model.update.return_value = None
    
    return mock_model


def create_mock_vertex_endpoint(endpoint_id: str, display_name: str = None, deployed_models: List = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock Vertex AI endpoint object for testing.
    
    Args:
        endpoint_id: ID of the endpoint
        display_name: Display name of the endpoint
        deployed_models: List of deployed models
        metadata: Dictionary of metadata attributes
        
    Returns:
        Configured mock Vertex AI endpoint
    """
    if display_name is None:
        display_name = f"Test Endpoint {endpoint_id}"
    if deployed_models is None:
        deployed_models = []
    if metadata is None:
        metadata = {}
    
    mock_endpoint = MagicMock(spec=google.cloud.aiplatform.Endpoint)
    mock_endpoint.name = endpoint_id
    mock_endpoint.display_name = display_name
    mock_endpoint.deployed_models = deployed_models
    
    # Set metadata attributes
    for key, value in metadata.items():
        setattr(mock_endpoint, key, value)
    
    # Configure predict method
    def mock_predict(instances, parameters=None):
        # Generate a default prediction
        return {
            "predictions": [{"result": f"Predicted value for instance {i}"} for i in range(len(instances))],
            "deployed_model_id": deployed_models[0] if deployed_models else "default-model",
            "model_version_id": "1",
            "model_resource_name": f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/models/{TEST_MODEL_ID}"
        }
    
    mock_endpoint.predict = mock_predict
    
    # Configure deployment management methods
    mock_endpoint.deploy.return_value = MagicMock()
    mock_endpoint.undeploy.return_value = None
    mock_endpoint.delete.return_value = None
    mock_endpoint.update.return_value = None
    
    return mock_endpoint


def create_gcp_test_resource_path(resource_type: str, resource_id: str, project_id: str = None, location: str = None) -> str:
    """
    Creates a GCP resource path for testing.
    
    Args:
        resource_type: Type of the resource
        resource_id: ID of the resource
        project_id: ID of the project
        location: Location of the resource
        
    Returns:
        Fully qualified GCP resource path
    """
    if project_id is None:
        project_id = TEST_PROJECT_ID
    
    if resource_type == "model":
        if location is None:
            location = TEST_LOCATION
        return f"projects/{project_id}/locations/{location}/models/{resource_id}"
    
    elif resource_type == "endpoint":
        if location is None:
            location = TEST_LOCATION
        return f"projects/{project_id}/locations/{location}/endpoints/{resource_id}"
    
    elif resource_type == "dataset":
        return f"projects/{project_id}/datasets/{resource_id}"
    
    elif resource_type == "table":
        parts = resource_id.split(".")
        if len(parts) == 2:
            dataset_id, table_id = parts
            return f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        return resource_id
    
    elif resource_type == "bucket":
        return f"gs://{resource_id}"
    
    elif resource_type == "blob":
        parts = resource_id.split("/", 1)
        if len(parts) == 2:
            bucket_name, blob_name = parts
            return f"gs://{bucket_name}/{blob_name}"
        return resource_id
    
    # Default: return as-is for unknown resource types
    return resource_id


def simulate_gcp_error(error_type: str, message: str = None, code: int = None) -> Exception:
    """
    Simulates a GCP API error for testing error handling.
    
    Args:
        error_type: Type of the error
        message: Error message
        code: Error code
        
    Returns:
        GCP API exception of the specified type
    """
    if message is None:
        message = f"Simulated {error_type} error for testing"
    
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
    elif error_type == "internal":
        return google.api_core.exceptions.InternalServerError(message, code)
    elif error_type == "unavailable":
        return google.api_core.exceptions.ServiceUnavailable(message, code)
    else:
        return google.api_core.exceptions.GoogleAPIError(message, code)


def patch_gcp_client(client_path: str, mock_client: MagicMock) -> patch:
    """
    Creates a patch for a GCP client class.
    
    Args:
        client_path: Path to the client class
        mock_client: Mock client to use for the patch
        
    Returns:
        Patch context manager
    """
    return patch(client_path, return_value=mock_client)


class GCPEmulator:
    """
    Base class for GCP service emulators that provide local testing capabilities.
    """
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the GCP emulator.
        
        Args:
            project_id: GCP project ID
            location: GCP location/region
        """
        self._project_id = project_id or TEST_PROJECT_ID
        self._location = location or TEST_LOCATION
        self._resources = {}
        self._is_running = False
    
    def start(self):
        """Start the emulator."""
        if self._is_running:
            return
        
        # Initialize resources
        self._is_running = True
    
    def stop(self):
        """Stop the emulator."""
        if not self._is_running:
            return
        
        # Clean up resources
        self._is_running = False
    
    def reset(self):
        """Reset the emulator state."""
        if not self._is_running:
            return
        
        # Clear all resources
        self._resources = {}
    
    def get_client(self):
        """
        Get a client for the emulated service.
        
        Returns:
            Client for the emulated service
        """
        if not self._is_running:
            self.start()
        
        # Return a client configured to use the emulator
        raise NotImplementedError("Subclasses must implement get_client method")
    
    def __enter__(self):
        """Enter context manager."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.stop()
        return None


class GCSEmulator(GCPEmulator):
    """
    Emulator for Google Cloud Storage that provides local testing capabilities.
    """
    
    def __init__(self, project_id: str = None):
        """
        Initialize the GCS emulator.
        
        Args:
            project_id: GCP project ID
        """
        super().__init__(project_id)
        self._buckets = {}
        self._blobs = {}
    
    def start(self):
        """Start the GCS emulator."""
        super().start()
        # Initialize any default buckets/blobs if needed
    
    def stop(self):
        """Stop the GCS emulator."""
        # Clean up any temporary files
        super().stop()
    
    def create_bucket(self, bucket_name: str, metadata: Dict = None) -> MagicMock:
        """
        Create a bucket in the emulator.
        
        Args:
            bucket_name: Name of the bucket
            metadata: Additional metadata for the bucket
            
        Returns:
            Mock bucket object
        """
        if not self._is_running:
            self.start()
        
        mock_bucket = create_mock_gcs_bucket(bucket_name, metadata=metadata)
        self._buckets[bucket_name] = mock_bucket
        return mock_bucket
    
    def get_bucket(self, bucket_name: str) -> MagicMock:
        """
        Get a bucket from the emulator.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Mock bucket object
        """
        if not self._is_running:
            self.start()
        
        if bucket_name in self._buckets:
            return self._buckets[bucket_name]
        
        raise google.api_core.exceptions.NotFound(f"Bucket {bucket_name} not found")
    
    def list_buckets(self) -> List[MagicMock]:
        """
        List all buckets in the emulator.
        
        Returns:
            List of mock bucket objects
        """
        if not self._is_running:
            self.start()
        
        return list(self._buckets.values())
    
    def upload_blob(self, bucket_name: str, blob_name: str, content: bytes, content_type: str = None) -> MagicMock:
        """
        Upload a blob to a bucket in the emulator.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            content: Content of the blob
            content_type: Content type of the blob
            
        Returns:
            Mock blob object
        """
        if not self._is_running:
            self.start()
        
        # Get or create the bucket
        try:
            bucket = self.get_bucket(bucket_name)
        except google.api_core.exceptions.NotFound:
            bucket = self.create_bucket(bucket_name)
        
        # Create the blob
        mock_blob = create_mock_gcs_blob(blob_name, content, content_type)
        
        # Add to bucket and blobs dict
        key = (bucket_name, blob_name)
        self._blobs[key] = mock_blob
        
        return mock_blob
    
    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        """
        Download a blob from the emulator.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            Blob content
        """
        if not self._is_running:
            self.start()
        
        # Get the bucket and blob
        bucket = self.get_bucket(bucket_name)
        key = (bucket_name, blob_name)
        
        if key not in self._blobs:
            raise google.api_core.exceptions.NotFound(f"Blob {blob_name} not found in bucket {bucket_name}")
        
        return self._blobs[key].download_as_bytes()
    
    def delete_blob(self, bucket_name: str, blob_name: str):
        """
        Delete a blob from the emulator.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
        """
        if not self._is_running:
            self.start()
        
        # Get the bucket
        bucket = self.get_bucket(bucket_name)
        key = (bucket_name, blob_name)
        
        if key not in self._blobs:
            raise google.api_core.exceptions.NotFound(f"Blob {blob_name} not found in bucket {bucket_name}")
        
        # Remove from blobs dict
        del self._blobs[key]
    
    def get_client(self) -> MagicMock:
        """
        Get a GCS client configured to use the emulator.
        
        Returns:
            Mock GCS client
        """
        if not self._is_running:
            self.start()
        
        # Create a mock client with the emulator's buckets and blobs
        buckets_dict = {name: bucket for name, bucket in self._buckets.items()}
        blobs_dict = {k: v for k, v in self._blobs.items()}
        
        return create_mock_gcs_client(buckets_dict, blobs_dict)


class BigQueryEmulator(GCPEmulator):
    """
    Emulator for BigQuery that provides local testing capabilities.
    """
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the BigQuery emulator.
        
        Args:
            project_id: GCP project ID
            location: GCP location/region
        """
        super().__init__(project_id, location)
        self._datasets = {}
        self._tables = {}
        self._query_results = {}
    
    def start(self):
        """Start the BigQuery emulator."""
        super().start()
        # Initialize any default datasets/tables if needed
    
    def stop(self):
        """Stop the BigQuery emulator."""
        # Clean up any temporary resources
        super().stop()
    
    def create_dataset(self, dataset_id: str, metadata: Dict = None) -> MagicMock:
        """
        Create a dataset in the emulator.
        
        Args:
            dataset_id: ID of the dataset
            metadata: Additional metadata for the dataset
            
        Returns:
            Mock dataset object
        """
        if not self._is_running:
            self.start()
        
        mock_dataset = create_mock_bigquery_dataset(dataset_id, self._project_id, metadata=metadata)
        self._datasets[dataset_id] = mock_dataset
        return mock_dataset
    
    def get_dataset(self, dataset_id: str) -> MagicMock:
        """
        Get a dataset from the emulator.
        
        Args:
            dataset_id: ID of the dataset
            
        Returns:
            Mock dataset object
        """
        if not self._is_running:
            self.start()
        
        if dataset_id in self._datasets:
            return self._datasets[dataset_id]
        
        raise google.api_core.exceptions.NotFound(f"Dataset {dataset_id} not found")
    
    def create_table(self, dataset_id: str, table_id: str, schema: List = None, data: pd.DataFrame = None) -> MagicMock:
        """
        Create a table in the emulator.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            schema: Schema of the table
            data: Data for the table
            
        Returns:
            Mock table object
        """
        if not self._is_running:
            self.start()
        
        # Get or create the dataset
        try:
            dataset = self.get_dataset(dataset_id)
        except google.api_core.exceptions.NotFound:
            dataset = self.create_dataset(dataset_id)
        
        # Create the table
        mock_table = create_mock_bigquery_table(table_id, dataset_id, self._project_id, schema, data)
        
        # Add to dataset and tables dict
        key = (dataset_id, table_id)
        self._tables[key] = mock_table
        
        return mock_table
    
    def get_table(self, dataset_id: str, table_id: str) -> MagicMock:
        """
        Get a table from the emulator.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            
        Returns:
            Mock table object
        """
        if not self._is_running:
            self.start()
        
        # Get the dataset
        dataset = self.get_dataset(dataset_id)
        key = (dataset_id, table_id)
        
        if key in self._tables:
            return self._tables[key]
        
        raise google.api_core.exceptions.NotFound(f"Table {table_id} not found in dataset {dataset_id}")
    
    def execute_query(self, query: str, parameters: Dict = None) -> pd.DataFrame:
        """
        Execute a query in the emulator.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters
            
        Returns:
            Query results as DataFrame
        """
        if not self._is_running:
            self.start()
        
        # Check if we have a predefined result for this query
        if query in self._query_results:
            return self._query_results[query]
        
        # For simple SELECT queries, we could parse and execute against in-memory tables
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
            query: SQL query to register
            result: Result to return for the query
        """
        if not self._is_running:
            self.start()
        
        self._query_results[query] = result
    
    def get_client(self) -> MagicMock:
        """
        Get a BigQuery client configured to use the emulator.
        
        Returns:
            Mock BigQuery client
        """
        if not self._is_running:
            self.start()
        
        # Create a mock client with the emulator's datasets, tables, and query_results
        datasets_dict = {name: dataset for name, dataset in self._datasets.items()}
        tables_dict = {k: v for k, v in self._tables.items()}
        query_results_dict = {k: v for k, v in self._query_results.items()}
        
        return create_mock_bigquery_client(datasets_dict, tables_dict, query_results_dict)


class VertexAIEmulator(GCPEmulator):
    """
    Emulator for Vertex AI that provides local testing capabilities.
    """
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the Vertex AI emulator.
        
        Args:
            project_id: GCP project ID
            location: GCP location/region
        """
        super().__init__(project_id, location)
        self._models = {}
        self._endpoints = {}
        self._prediction_results = {}
    
    def start(self):
        """Start the Vertex AI emulator."""
        super().start()
        # Initialize any default models/endpoints if needed
    
    def stop(self):
        """Stop the Vertex AI emulator."""
        # Clean up any temporary resources
        super().stop()
    
    def create_model(self, model_id: str, display_name: str = None, model_type: str = None, metadata: Dict = None) -> MagicMock:
        """
        Create a model in the emulator.
        
        Args:
            model_id: ID of the model
            display_name: Display name of the model
            model_type: Type of the model
            metadata: Additional metadata for the model
            
        Returns:
            Mock model object
        """
        if not self._is_running:
            self.start()
        
        mock_model = create_mock_vertex_model(model_id, display_name, model_type, metadata)
        self._models[model_id] = mock_model
        return mock_model
    
    def get_model(self, model_id: str) -> MagicMock:
        """
        Get a model from the emulator.
        
        Args:
            model_id: ID of the model
            
        Returns:
            Mock model object
        """
        if not self._is_running:
            self.start()
        
        if model_id in self._models:
            return self._models[model_id]
        
        raise google.api_core.exceptions.NotFound(f"Model {model_id} not found")
    
    def create_endpoint(self, endpoint_id: str, display_name: str = None, deployed_models: List = None, metadata: Dict = None) -> MagicMock:
        """
        Create an endpoint in the emulator.
        
        Args:
            endpoint_id: ID of the endpoint
            display_name: Display name of the endpoint
            deployed_models: List of deployed models
            metadata: Additional metadata for the endpoint
            
        Returns:
            Mock endpoint object
        """
        if not self._is_running:
            self.start()
        
        mock_endpoint = create_mock_vertex_endpoint(endpoint_id, display_name, deployed_models, metadata)
        self._endpoints[endpoint_id] = mock_endpoint
        return mock_endpoint
    
    def get_endpoint(self, endpoint_id: str) -> MagicMock:
        """
        Get an endpoint from the emulator.
        
        Args:
            endpoint_id: ID of the endpoint
            
        Returns:
            Mock endpoint object
        """
        if not self._is_running:
            self.start()
        
        if endpoint_id in self._endpoints:
            return self._endpoints[endpoint_id]
        
        raise google.api_core.exceptions.NotFound(f"Endpoint {endpoint_id} not found")
    
    def predict(self, endpoint_id: str, instances: List, parameters: Dict = None) -> Dict:
        """
        Make a prediction using the emulator.
        
        Args:
            endpoint_id: ID of the endpoint
            instances: Instances to predict
            parameters: Prediction parameters
            
        Returns:
            Prediction results
        """
        if not self._is_running:
            self.start()
        
        # Get the endpoint
        endpoint = self.get_endpoint(endpoint_id)
        
        # Check if we have a predefined result for this endpoint and instances
        key = (endpoint_id, str(instances))
        if key in self._prediction_results:
            return self._prediction_results[key]
        
        # Generate a mock prediction result
        return endpoint.predict(instances, parameters)
    
    def register_prediction_result(self, endpoint_id: str, instances: List, result: Dict):
        """
        Register a predefined prediction result.
        
        Args:
            endpoint_id: ID of the endpoint
            instances: Instances to predict
            result: Result to return for the prediction
        """
        if not self._is_running:
            self.start()
        
        key = (endpoint_id, str(instances))
        self._prediction_results[key] = result
    
    def get_client(self) -> MagicMock:
        """
        Get a Vertex AI client configured to use the emulator.
        
        Returns:
            Mock Vertex AI client
        """
        if not self._is_running:
            self.start()
        
        # Create a mock client with the emulator's models, endpoints, and prediction_results
        models_dict = {name: model for name, model in self._models.items()}
        endpoints_dict = {name: endpoint for name, endpoint in self._endpoints.items()}
        prediction_results_dict = {k: v for k, v in self._prediction_results.items()}
        
        return create_mock_vertex_ai_client(models_dict, endpoints_dict, prediction_results_dict)


class GCPTestContext:
    """
    Context manager for GCP testing that provides mock clients and emulators.
    """
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the GCP test context.
        
        Args:
            project_id: GCP project ID
            location: GCP location/region
        """
        self.project_id = project_id or TEST_PROJECT_ID
        self.location = location or TEST_LOCATION
        self._emulators = {}
        self._mock_clients = {}
        self._patches = []
        self._resource_manager = TestResourceManager()
    
    def __enter__(self):
        """
        Enter the context manager.
        
        Returns:
            Self reference
        """
        # Start all emulators
        for emulator in self._emulators.values():
            emulator.start()
        
        # Apply all patches
        for p in self._patches:
            p.start()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager and clean up resources.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        # Stop all emulators
        for emulator in self._emulators.values():
            emulator.stop()
        
        # Remove all patches
        for p in self._patches:
            p.stop()
        
        # Clean up resources
        self._resource_manager.__exit__(exc_type, exc_val, exc_tb)
        
        return None
    
    def use_gcs_emulator(self) -> GCSEmulator:
        """
        Configure the context to use a GCS emulator.
        
        Returns:
            GCS emulator instance
        """
        if 'gcs' not in self._emulators:
            emulator = GCSEmulator(self.project_id)
            self._emulators['gcs'] = emulator
            
            # Set up patch for GCSClient
            mock_client = emulator.get_client()
            self._patches.append(patch('src.backend.utils.storage.gcs_client.GCSClient', return_value=mock_client))
        
        return self._emulators['gcs']
    
    def use_bigquery_emulator(self) -> BigQueryEmulator:
        """
        Configure the context to use a BigQuery emulator.
        
        Returns:
            BigQuery emulator instance
        """
        if 'bigquery' not in self._emulators:
            emulator = BigQueryEmulator(self.project_id, self.location)
            self._emulators['bigquery'] = emulator
            
            # Set up patch for BigQueryClient
            mock_client = emulator.get_client()
            self._patches.append(patch('src.backend.utils.storage.bigquery_client.BigQueryClient', return_value=mock_client))
        
        return self._emulators['bigquery']
    
    def use_vertex_ai_emulator(self) -> VertexAIEmulator:
        """
        Configure the context to use a Vertex AI emulator.
        
        Returns:
            Vertex AI emulator instance
        """
        if 'vertex_ai' not in self._emulators:
            emulator = VertexAIEmulator(self.project_id, self.location)
            self._emulators['vertex_ai'] = emulator
            
            # Set up patch for VertexAIClient
            mock_client = emulator.get_client()
            self._patches.append(patch('src.backend.utils.ml.vertex_client.VertexAIClient', return_value=mock_client))
        
        return self._emulators['vertex_ai']
    
    def mock_gcs_client(self, buckets: Dict = None, blobs: Dict = None) -> MagicMock:
        """
        Configure the context to use a mock GCS client.
        
        Args:
            buckets: Dictionary of bucket name to mock bucket object
            blobs: Dictionary of (bucket_name, blob_name) to mock blob object
            
        Returns:
            Mock GCS client
        """
        mock_client = create_mock_gcs_client(buckets, blobs)
        self._mock_clients['gcs'] = mock_client
        
        # Set up patch for GCSClient
        self._patches.append(patch('src.backend.utils.storage.gcs_client.GCSClient', return_value=mock_client))
        
        return mock_client
    
    def mock_bigquery_client(self, datasets: Dict = None, tables: Dict = None, query_results: Dict = None) -> MagicMock:
        """
        Configure the context to use a mock BigQuery client.
        
        Args:
            datasets: Dictionary of dataset_id to mock dataset object
            tables: Dictionary of (dataset_id, table_id) to mock table object
            query_results: Dictionary of query string to result
            
        Returns:
            Mock BigQuery client
        """
        mock_client = create_mock_bigquery_client(datasets, tables, query_results)
        self._mock_clients['bigquery'] = mock_client
        
        # Set up patch for BigQueryClient
        self._patches.append(patch('src.backend.utils.storage.bigquery_client.BigQueryClient', return_value=mock_client))
        
        return mock_client
    
    def mock_vertex_ai_client(self, models: Dict = None, endpoints: Dict = None, prediction_results: Dict = None) -> MagicMock:
        """
        Configure the context to use a mock Vertex AI client.
        
        Args:
            models: Dictionary of model_id to mock model object
            endpoints: Dictionary of endpoint_id to mock endpoint object
            prediction_results: Dictionary of (endpoint_id, instance) to prediction result
            
        Returns:
            Mock Vertex AI client
        """
        mock_client = create_mock_vertex_ai_client(models, endpoints, prediction_results)
        self._mock_clients['vertex_ai'] = mock_client
        
        # Set up patch for VertexAIClient
        self._patches.append(patch('src.backend.utils.ml.vertex_client.VertexAIClient', return_value=mock_client))
        
        return mock_client
    
    def create_temp_gcs_bucket(self, bucket_name: str = None, metadata: Dict = None) -> MagicMock:
        """
        Create a temporary GCS bucket for testing.
        
        Args:
            bucket_name: Name of the bucket
            metadata: Additional metadata for the bucket
            
        Returns:
            Mock GCS bucket
        """
        if bucket_name is None:
            bucket_name = generate_unique_id("test-bucket")
        
        if 'gcs' in self._emulators:
            # Use emulator
            bucket = self._emulators['gcs'].create_bucket(bucket_name, metadata)
        elif 'gcs' in self._mock_clients:
            # Use mock client
            client = self._mock_clients['gcs']
            bucket = client.bucket(bucket_name)
            bucket.create()
        else:
            # Create a new mock bucket
            bucket = create_mock_gcs_bucket(bucket_name, metadata=metadata)
        
        # Register for cleanup
        self._resource_manager.add_resource(bucket, lambda b: None)  # No-op cleanup for mock objects
        
        return bucket
    
    def create_temp_bigquery_dataset(self, dataset_id: str = None, metadata: Dict = None) -> MagicMock:
        """
        Create a temporary BigQuery dataset for testing.
        
        Args:
            dataset_id: ID of the dataset
            metadata: Additional metadata for the dataset
            
        Returns:
            Mock BigQuery dataset
        """
        if dataset_id is None:
            dataset_id = generate_unique_id("test_dataset")
        
        if 'bigquery' in self._emulators:
            # Use emulator
            dataset = self._emulators['bigquery'].create_dataset(dataset_id, metadata)
        elif 'bigquery' in self._mock_clients:
            # Use mock client
            client = self._mock_clients['bigquery']
            dataset = client.dataset(dataset_id)
            dataset.create()
        else:
            # Create a new mock dataset
            dataset = create_mock_bigquery_dataset(dataset_id, self.project_id, metadata=metadata)
        
        # Register for cleanup
        self._resource_manager.add_resource(dataset, lambda d: None)  # No-op cleanup for mock objects
        
        return dataset
    
    def create_temp_bigquery_table(self, dataset_id: str = None, table_id: str = None, schema: List = None, data: pd.DataFrame = None) -> MagicMock:
        """
        Create a temporary BigQuery table for testing.
        
        Args:
            dataset_id: ID of the dataset
            table_id: ID of the table
            schema: Schema of the table
            data: Data for the table
            
        Returns:
            Mock BigQuery table
        """
        if dataset_id is None:
            dataset_id = generate_unique_id("test_dataset")
        
        if table_id is None:
            table_id = generate_unique_id("test_table")
        
        if 'bigquery' in self._emulators:
            # Use emulator
            try:
                # Try to get the dataset
                self._emulators['bigquery'].get_dataset(dataset_id)
            except google.api_core.exceptions.NotFound:
                # Create the dataset if it doesn't exist
                self._emulators['bigquery'].create_dataset(dataset_id)
            
            table = self._emulators['bigquery'].create_table(dataset_id, table_id, schema, data)
        elif 'bigquery' in self._mock_clients:
            # Use mock client
            client = self._mock_clients['bigquery']
            try:
                # Try to get the dataset
                client.get_dataset(dataset_id)
            except google.api_core.exceptions.NotFound:
                # Create the dataset if it doesn't exist
                dataset = client.dataset(dataset_id)
                dataset.create()
            
            table = client.table(f"{dataset_id}.{table_id}")
            # Set schema and data if provided
            if schema is not None:
                table.schema = schema
            if data is not None:
                table.to_dataframe.return_value = data
        else:
            # Create a new mock table
            table = create_mock_bigquery_table(table_id, dataset_id, self.project_id, schema, data)
        
        # Register for cleanup
        self._resource_manager.add_resource(table, lambda t: None)  # No-op cleanup for mock objects
        
        return table