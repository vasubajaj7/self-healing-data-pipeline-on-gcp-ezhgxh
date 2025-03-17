"""
Provides mock implementations of various Google Cloud services and other external dependencies for testing the self-healing data pipeline.

This module contains mock classes that simulate the behavior of real services without requiring actual cloud resources,
enabling isolated and deterministic testing.
"""

from unittest.mock import MagicMock, patch
import pytest
from typing import Any, Dict, List, Optional, Union
import io
import pandas as pd

from src.backend.constants import ErrorCategory, ErrorRecoverability, PipelineStatus, FileFormat
from src.test.utils.test_helpers import create_test_dataframe, create_test_json_data, generate_unique_id

# Default values for testing
DEFAULT_PROJECT_ID = "test-project"
DEFAULT_LOCATION = "us-central1"


def create_mock_blob(name: str, bucket_name: str, content: bytes, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock GCS blob object with specified properties.
    
    Args:
        name: Name of the blob
        bucket_name: Name of the containing bucket
        content: Blob content as bytes
        metadata: Optional metadata dictionary
        
    Returns:
        Mock GCS blob object
    """
    mock_blob = MagicMock()
    mock_blob.name = name
    mock_blob.bucket = MagicMock(name=bucket_name)
    mock_blob.download_as_bytes.return_value = content
    mock_blob.download_as_string.return_value = content.decode('utf-8') if content else ""
    if metadata:
        mock_blob.metadata = metadata
    return mock_blob


def create_mock_bucket(name: str, blobs: List = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock GCS bucket object with specified properties.
    
    Args:
        name: Name of the bucket
        blobs: List of blob objects in the bucket
        metadata: Optional metadata dictionary
        
    Returns:
        Mock GCS bucket object
    """
    mock_bucket = MagicMock()
    mock_bucket.name = name
    
    # Configure blob access by name
    def get_blob(blob_name):
        if blobs:
            for blob in blobs:
                if blob.name == blob_name:
                    return blob
        return None
    
    mock_bucket.blob.side_effect = get_blob
    mock_bucket.list_blobs.return_value = blobs or []
    
    if metadata:
        mock_bucket.metadata = metadata
    
    return mock_bucket


def create_mock_bq_table(table_id: str, dataset_id: str, project_id: str, schema: List = None, num_rows: int = None) -> MagicMock:
    """
    Creates a mock BigQuery table object with specified properties.
    
    Args:
        table_id: Table ID
        dataset_id: Dataset ID
        project_id: Project ID
        schema: Table schema
        num_rows: Number of rows in the table
        
    Returns:
        Mock BigQuery table object
    """
    mock_table = MagicMock()
    mock_table.table_id = table_id
    mock_table.dataset_id = dataset_id
    mock_table.project_id = project_id
    mock_table.full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    if schema:
        mock_table.schema = schema
    
    if num_rows is not None:
        mock_table.num_rows = num_rows
    
    return mock_table


def create_mock_bq_job(job_id: str, job_type: str, state: str, result: Dict = None, error: Exception = None) -> MagicMock:
    """
    Creates a mock BigQuery job object with specified properties.
    
    Args:
        job_id: Job ID
        job_type: Job type (query, load, extract, copy)
        state: Job state (PENDING, RUNNING, DONE)
        result: Job result
        error: Optional exception if job failed
        
    Returns:
        Mock BigQuery job object
    """
    mock_job = MagicMock()
    mock_job.job_id = job_id
    mock_job.job_type = job_type
    mock_job.state = state
    
    # Configure result method
    if error:
        mock_job.result.side_effect = error
    else:
        mock_job.result.return_value = result
    
    # Configure done property
    mock_job.done.return_value = (state == "DONE")
    
    return mock_job


def create_mock_vertex_model(model_id: str, display_name: str, version_id: str = None, metadata: Dict = None) -> MagicMock:
    """
    Creates a mock Vertex AI model object with specified properties.
    
    Args:
        model_id: Model ID
        display_name: Display name
        version_id: Model version ID
        metadata: Optional metadata dictionary
        
    Returns:
        Mock Vertex AI model object
    """
    mock_model = MagicMock()
    mock_model.model_id = model_id
    mock_model.display_name = display_name
    mock_model.version_id = version_id
    
    if metadata:
        mock_model.metadata = metadata
    
    # Configure to_dict method
    mock_model.to_dict.return_value = {
        "name": model_id,
        "displayName": display_name,
        "versionId": version_id,
        "metadata": metadata or {}
    }
    
    return mock_model


def create_mock_vertex_endpoint(endpoint_id: str, display_name: str, deployed_models: List = None) -> MagicMock:
    """
    Creates a mock Vertex AI endpoint object with specified properties.
    
    Args:
        endpoint_id: Endpoint ID
        display_name: Display name
        deployed_models: List of deployed models
        
    Returns:
        Mock Vertex AI endpoint object
    """
    mock_endpoint = MagicMock()
    mock_endpoint.endpoint_id = endpoint_id
    mock_endpoint.display_name = display_name
    
    if deployed_models:
        mock_endpoint.deployed_models = deployed_models
    
    # Configure predict method
    mock_endpoint.predict.return_value = {
        "predictions": [{"result": "mock_prediction"}]
    }
    
    # Configure to_dict method
    mock_endpoint.to_dict.return_value = {
        "name": endpoint_id,
        "displayName": display_name,
        "deployedModels": deployed_models or []
    }
    
    return mock_endpoint


def create_mock_pubsub_topic(topic_id: str, project_id: str) -> MagicMock:
    """
    Creates a mock Pub/Sub topic object with specified properties.
    
    Args:
        topic_id: Topic ID
        project_id: Project ID
        
    Returns:
        Mock Pub/Sub topic object
    """
    mock_topic = MagicMock()
    mock_topic.topic_id = topic_id
    mock_topic.project_id = project_id
    mock_topic.full_name = f"projects/{project_id}/topics/{topic_id}"
    
    # Configure publish method
    mock_topic.publish.return_value = f"message-{generate_unique_id()}"
    
    return mock_topic


def create_mock_pubsub_subscription(subscription_id: str, topic_id: str, project_id: str) -> MagicMock:
    """
    Creates a mock Pub/Sub subscription object with specified properties.
    
    Args:
        subscription_id: Subscription ID
        topic_id: Topic ID
        project_id: Project ID
        
    Returns:
        Mock Pub/Sub subscription object
    """
    mock_subscription = MagicMock()
    mock_subscription.subscription_id = subscription_id
    mock_subscription.topic_id = topic_id
    mock_subscription.project_id = project_id
    mock_subscription.full_name = f"projects/{project_id}/subscriptions/{subscription_id}"
    
    # Configure pull method
    mock_subscription.pull.return_value = []
    
    return mock_subscription


class MockGCSClient:
    """Mock implementation of Google Cloud Storage client."""
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the mock GCS client.
        
        Args:
            project_id: GCP project ID
            location: GCP location
        """
        self.project_id = project_id or DEFAULT_PROJECT_ID
        self.location = location or DEFAULT_LOCATION
        self._buckets = {}
        self._blobs = {}
    
    def get_bucket(self, bucket_name: str) -> MagicMock:
        """
        Get a bucket by name.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Mock bucket object
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket doesn't exist
        """
        if bucket_name in self._buckets:
            return self._buckets[bucket_name]
        
        # Simulate NotFound exception
        error = type('NotFound', (Exception,), {})
        raise error(f"Bucket {bucket_name} not found")
    
    def create_bucket(self, bucket_name: str, location: str = None, 
                     storage_class: str = None, labels: Dict = None) -> MagicMock:
        """
        Create a new bucket.
        
        Args:
            bucket_name: Name of the bucket
            location: Bucket location
            storage_class: Storage class
            labels: Bucket labels
            
        Returns:
            Created bucket object
        """
        # Check if bucket already exists
        if bucket_name in self._buckets:
            return self._buckets[bucket_name]
        
        # Create new bucket
        bucket = create_mock_bucket(
            name=bucket_name, 
            metadata={
                "location": location or self.location,
                "storageClass": storage_class or "STANDARD",
                "labels": labels or {}
            }
        )
        
        # Store bucket
        self._buckets[bucket_name] = bucket
        return bucket
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.
        
        Args:
            bucket_name: Name of the bucket
            force: Whether to force deletion with content
            
        Returns:
            True if successful
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket doesn't exist
        """
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # If force is True, delete all blobs in bucket
        if force:
            bucket_blob_keys = [key for key in self._blobs if key.startswith(f"{bucket_name}/")]
            for key in bucket_blob_keys:
                del self._blobs[key]
        
        # Delete bucket
        del self._buckets[bucket_name]
        return True
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            True if bucket exists
        """
        return bucket_name in self._buckets
    
    def list_buckets(self) -> List[str]:
        """
        List all buckets.
        
        Returns:
            List of bucket names
        """
        return list(self._buckets.keys())
    
    def upload_file(self, bucket_name: str, source_file_path: str, destination_blob_name: str,
                   content_type: str = None, metadata: Dict = None) -> MagicMock:
        """
        Upload a file to GCS.
        
        Args:
            bucket_name: Name of the bucket
            source_file_path: Path to source file
            destination_blob_name: Name for the destination blob
            content_type: Content type
            metadata: Optional metadata
            
        Returns:
            Mock blob object
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket doesn't exist
        """
        # Check if bucket exists
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # Create mock blob
        with open(source_file_path, 'rb') as f:
            content = f.read()
        
        blob = create_mock_blob(
            name=destination_blob_name,
            bucket_name=bucket_name,
            content=content,
            metadata=metadata
        )
        
        # Store blob
        self._blobs[f"{bucket_name}/{destination_blob_name}"] = blob
        return blob
    
    def upload_from_string(self, bucket_name: str, data: str, destination_blob_name: str,
                         content_type: str = None, metadata: Dict = None) -> MagicMock:
        """
        Upload string data to GCS.
        
        Args:
            bucket_name: Name of the bucket
            data: String data to upload
            destination_blob_name: Name for the destination blob
            content_type: Content type
            metadata: Optional metadata
            
        Returns:
            Mock blob object
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket doesn't exist
        """
        # Check if bucket exists
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # Create mock blob
        blob = create_mock_blob(
            name=destination_blob_name,
            bucket_name=bucket_name,
            content=data.encode('utf-8') if isinstance(data, str) else data,
            metadata=metadata
        )
        
        # Store blob
        self._blobs[f"{bucket_name}/{destination_blob_name}"] = blob
        return blob
    
    def download_as_string(self, bucket_name: str, blob_name: str) -> str:
        """
        Download blob content as string.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            Blob content as string
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket or blob doesn't exist
        """
        blob_key = f"{bucket_name}/{blob_name}"
        
        # Check if bucket exists
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # Check if blob exists
        if blob_key not in self._blobs:
            error = type('NotFound', (Exception,), {})
            raise error(f"Blob {blob_name} not found in bucket {bucket_name}")
        
        # Return blob content
        return self._blobs[blob_key].download_as_string.return_value
    
    def list_blobs(self, bucket_name: str, prefix: str = None, delimiter: str = None) -> List[MagicMock]:
        """
        List blobs in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix filter
            delimiter: Optional delimiter
            
        Returns:
            List of blob names
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket doesn't exist
        """
        # Check if bucket exists
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # Filter blobs by prefix and delimiter
        blobs = []
        for key, blob in self._blobs.items():
            if key.startswith(f"{bucket_name}/"):
                blob_name = key[len(f"{bucket_name}/"):]
                
                # Apply prefix filter
                if prefix and not blob_name.startswith(prefix):
                    continue
                
                # Apply delimiter filter
                if delimiter:
                    parts = blob_name.split(delimiter)
                    if len(parts) > 1:
                        # Only include the prefix part
                        prefix_part = parts[0] + delimiter
                        if prefix_part not in blobs:
                            blobs.append(prefix_part)
                        continue
                
                blobs.append(blob)
        
        return blobs
    
    def delete_blob(self, bucket_name: str, blob_name: str) -> bool:
        """
        Delete a blob.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            True if successful
            
        Raises:
            google.cloud.exceptions.NotFound: If bucket or blob doesn't exist
        """
        blob_key = f"{bucket_name}/{blob_name}"
        
        # Check if bucket exists
        if bucket_name not in self._buckets:
            error = type('NotFound', (Exception,), {})
            raise error(f"Bucket {bucket_name} not found")
        
        # Check if blob exists
        if blob_key not in self._blobs:
            error = type('NotFound', (Exception,), {})
            raise error(f"Blob {blob_name} not found in bucket {bucket_name}")
        
        # Delete blob
        del self._blobs[blob_key]
        return True
    
    def blob_exists(self, bucket_name: str, blob_name: str) -> bool:
        """
        Check if a blob exists.
        
        Args:
            bucket_name: Name of the bucket
            blob_name: Name of the blob
            
        Returns:
            True if blob exists
        """
        # Check if bucket exists
        if bucket_name not in self._buckets:
            return False
        
        # Check if blob exists
        blob_key = f"{bucket_name}/{blob_name}"
        return blob_key in self._blobs


class MockBigQueryClient:
    """Mock implementation of Google BigQuery client."""
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the mock BigQuery client.
        
        Args:
            project_id: GCP project ID
            location: GCP location
        """
        self.project_id = project_id or DEFAULT_PROJECT_ID
        self.location = location or DEFAULT_LOCATION
        self._datasets = {}
        self._tables = {}
        self._jobs = {}
        self._query_results = {}
    
    def execute_query(self, query: str, parameters: Dict = None, 
                     use_legacy_sql: bool = False, timeout: int = None,
                     destination_table: str = None, write_disposition: str = None) -> MagicMock:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            parameters: Query parameters
            use_legacy_sql: Whether to use legacy SQL
            timeout: Query timeout in seconds
            destination_table: Destination table for results
            write_disposition: Write disposition
            
        Returns:
            Mock query result
        """
        # Check if we have a pre-configured result for this query
        if query in self._query_results:
            return self._query_results[query]
        
        # Otherwise, return empty result
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_result.schema = []
        mock_result.pages = iter([[]])
        
        return mock_result
    
    def execute_query_to_dataframe(self, query: str, parameters: Dict = None,
                                 use_legacy_sql: bool = False, timeout: int = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.
        
        Args:
            query: SQL query string
            parameters: Query parameters
            use_legacy_sql: Whether to use legacy SQL
            timeout: Query timeout in seconds
            
        Returns:
            Query results as DataFrame
        """
        # Get query result
        result = self.execute_query(query, parameters, use_legacy_sql, timeout)
        
        # If result is already a DataFrame, return it
        if isinstance(result, pd.DataFrame):
            return result
        
        # If result is a dict with 'data' that's a DataFrame, return that
        if isinstance(result, dict) and 'data' in result and isinstance(result['data'], pd.DataFrame):
            return result['data']
        
        # Otherwise, return empty DataFrame
        return pd.DataFrame()
    
    def create_dataset(self, dataset_id: str, description: str = None, location: str = None) -> MagicMock:
        """
        Create a new dataset.
        
        Args:
            dataset_id: Dataset ID
            description: Dataset description
            location: Dataset location
            
        Returns:
            Mock dataset object
        """
        # Check if dataset already exists
        if dataset_id in self._datasets:
            return self._datasets[dataset_id]
        
        # Create mock dataset
        mock_dataset = MagicMock()
        mock_dataset.dataset_id = dataset_id
        mock_dataset.project_id = self.project_id
        mock_dataset.description = description
        mock_dataset.location = location or self.location
        
        # Store dataset
        self._datasets[dataset_id] = mock_dataset
        return mock_dataset
    
    def dataset_exists(self, dataset_id: str) -> bool:
        """
        Check if a dataset exists.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            True if dataset exists
        """
        return dataset_id in self._datasets
    
    def create_table(self, dataset_id: str, table_id: str, schema: List = None,
                    clustering_fields: List = None, time_partitioning: Dict = None) -> MagicMock:
        """
        Create a new table.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            schema: Table schema
            clustering_fields: Clustering fields
            time_partitioning: Time partitioning configuration
            
        Returns:
            Mock table object
            
        Raises:
            Exception: If dataset doesn't exist
        """
        # Check if dataset exists
        if dataset_id not in self._datasets:
            # Create the dataset if it doesn't exist
            self.create_dataset(dataset_id)
        
        # Create table key
        table_key = f"{dataset_id}.{table_id}"
        
        # Check if table already exists
        if table_key in self._tables:
            return self._tables[table_key]
        
        # Create mock table
        mock_table = create_mock_bq_table(
            table_id=table_id,
            dataset_id=dataset_id,
            project_id=self.project_id,
            schema=schema
        )
        
        # Add additional properties
        if clustering_fields:
            mock_table.clustering_fields = clustering_fields
        
        if time_partitioning:
            mock_table.time_partitioning = time_partitioning
        
        # Store table
        self._tables[table_key] = mock_table
        return mock_table
    
    def delete_table(self, dataset_id: str, table_id: str, not_found_ok: bool = False) -> bool:
        """
        Delete a table.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            not_found_ok: Whether to ignore if table doesn't exist
            
        Returns:
            True if successful
            
        Raises:
            Exception: If dataset or table doesn't exist and not_found_ok is False
        """
        # Check if dataset exists
        if dataset_id not in self._datasets and not not_found_ok:
            raise Exception(f"Dataset {dataset_id} not found")
        
        # Create table key
        table_key = f"{dataset_id}.{table_id}"
        
        # Check if table exists
        if table_key not in self._tables and not not_found_ok:
            raise Exception(f"Table {table_id} not found in dataset {dataset_id}")
        
        # Delete table if it exists
        if table_key in self._tables:
            del self._tables[table_key]
        
        return True
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            
        Returns:
            True if table exists
        """
        # Create table key
        table_key = f"{dataset_id}.{table_id}"
        
        # Check if dataset and table exist
        return dataset_id in self._datasets and table_key in self._tables
    
    def get_table(self, dataset_id: str, table_id: str) -> MagicMock:
        """
        Get a table reference.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            
        Returns:
            Mock table object
            
        Raises:
            Exception: If dataset or table doesn't exist
        """
        # Check if dataset exists
        if dataset_id not in self._datasets:
            raise Exception(f"Dataset {dataset_id} not found")
        
        # Create table key
        table_key = f"{dataset_id}.{table_id}"
        
        # Check if table exists
        if table_key not in self._tables:
            raise Exception(f"Table {table_id} not found in dataset {dataset_id}")
        
        return self._tables[table_key]
    
    def set_query_result(self, query: str, result: Any) -> None:
        """
        Set a pre-defined result for a query.
        
        Args:
            query: SQL query string
            result: Result to return for the query
        """
        self._query_results[query] = result
    
    def set_table_data(self, dataset_id: str, table_id: str, data: pd.DataFrame) -> None:
        """
        Set data for a table.
        
        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            data: DataFrame containing the table data
            
        Raises:
            Exception: If dataset doesn't exist
        """
        # Ensure dataset and table exist
        if dataset_id not in self._datasets:
            self.create_dataset(dataset_id)
        
        table_key = f"{dataset_id}.{table_id}"
        if table_key not in self._tables:
            self.create_table(dataset_id, table_id)
        
        # Set data property on table
        table = self._tables[table_key]
        table.data = data
        
        # Update num_rows property
        table.num_rows = len(data)


class MockVertexAIClient:
    """Mock implementation of Google Vertex AI client."""
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the mock Vertex AI client.
        
        Args:
            project_id: GCP project ID
            location: GCP location
        """
        self.project_id = project_id or DEFAULT_PROJECT_ID
        self.location = location or DEFAULT_LOCATION
        self._models = {}
        self._endpoints = {}
        self._batch_jobs = {}
        self._prediction_results = {}
    
    def upload_model(self, display_name: str, artifact_uri: str, 
                    serving_container_image_uri: str, metadata: Dict = None) -> str:
        """
        Upload a model to Vertex AI.
        
        Args:
            display_name: Display name
            artifact_uri: Artifact URI
            serving_container_image_uri: Serving container image URI
            metadata: Optional metadata
            
        Returns:
            Model ID
        """
        # Generate model ID
        model_id = generate_unique_id("model")
        
        # Create mock model
        mock_model = create_mock_vertex_model(
            model_id=model_id,
            display_name=display_name,
            metadata={
                "artifact_uri": artifact_uri,
                "serving_container_image_uri": serving_container_image_uri,
                **(metadata or {})
            }
        )
        
        # Store model
        self._models[model_id] = mock_model
        return model_id
    
    def deploy_model(self, model_id: str, endpoint_id: str = None, machine_type: str = None,
                    accelerator_type: str = None, accelerator_count: int = None,
                    min_replica_count: int = None, max_replica_count: int = None,
                    deployment_config: Dict = None) -> str:
        """
        Deploy a model to an endpoint.
        
        Args:
            model_id: Model ID
            endpoint_id: Endpoint ID (optional, will create if not provided)
            machine_type: Machine type
            accelerator_type: Accelerator type
            accelerator_count: Accelerator count
            min_replica_count: Minimum replica count
            max_replica_count: Maximum replica count
            deployment_config: Additional deployment configuration
            
        Returns:
            Deployed model ID
            
        Raises:
            Exception: If model doesn't exist
        """
        # Check if model exists
        if model_id not in self._models:
            raise Exception(f"Model {model_id} not found")
        
        # Get or create endpoint
        if endpoint_id is None:
            endpoint_id = generate_unique_id("endpoint")
            self.create_endpoint(display_name=f"Endpoint for {model_id}")
        elif endpoint_id not in self._endpoints:
            self.create_endpoint(display_name=f"Endpoint {endpoint_id}")
        
        # Generate deployed model ID
        deployed_model_id = generate_unique_id("deployed-model")
        
        # Add model to endpoint's deployed_models list
        endpoint = self._endpoints[endpoint_id]
        if not hasattr(endpoint, 'deployed_models') or endpoint.deployed_models is None:
            endpoint.deployed_models = []
        
        endpoint.deployed_models.append({
            "id": deployed_model_id,
            "model_id": model_id,
            "display_name": self._models[model_id].display_name,
            "machine_type": machine_type,
            "accelerator_type": accelerator_type,
            "accelerator_count": accelerator_count,
            "min_replica_count": min_replica_count,
            "max_replica_count": max_replica_count,
            **(deployment_config or {})
        })
        
        return deployed_model_id
    
    def predict(self, endpoint_id: str, instances: List, parameters: Dict = None, timeout: int = None) -> Dict:
        """
        Make a prediction using a deployed model.
        
        Args:
            endpoint_id: Endpoint ID
            instances: Input instances
            parameters: Prediction parameters
            timeout: Prediction timeout
            
        Returns:
            Prediction results
            
        Raises:
            Exception: If endpoint doesn't exist
        """
        # Check if endpoint exists
        if endpoint_id not in self._endpoints:
            raise Exception(f"Endpoint {endpoint_id} not found")
        
        # Check if we have pre-configured prediction result
        if endpoint_id in self._prediction_results:
            return self._prediction_results[endpoint_id]
        
        # Otherwise, return default prediction
        return {
            "predictions": [{"result": "mock_prediction"} for _ in instances]
        }
    
    def create_endpoint(self, display_name: str, endpoint_config: Dict = None) -> str:
        """
        Create a new endpoint.
        
        Args:
            display_name: Display name
            endpoint_config: Endpoint configuration
            
        Returns:
            Endpoint ID
        """
        # Generate endpoint ID
        endpoint_id = generate_unique_id("endpoint")
        
        # Create mock endpoint
        mock_endpoint = create_mock_vertex_endpoint(
            endpoint_id=endpoint_id,
            display_name=display_name
        )
        
        # Store endpoint
        self._endpoints[endpoint_id] = mock_endpoint
        return endpoint_id
    
    def set_prediction_result(self, endpoint_id: str, result: Any) -> None:
        """
        Set a pre-defined prediction result for an endpoint.
        
        Args:
            endpoint_id: Endpoint ID
            result: Result to return for prediction requests
        """
        self._prediction_results[endpoint_id] = result


class MockPubSubClient:
    """Mock implementation of Google Pub/Sub client."""
    
    def __init__(self, project_id: str = None):
        """
        Initialize the mock Pub/Sub client.
        
        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id or DEFAULT_PROJECT_ID
        self._topics = {}
        self._subscriptions = {}
        self._messages = {}  # topic_id -> [messages]
    
    def create_topic(self, topic_id: str) -> MagicMock:
        """
        Create a new topic.
        
        Args:
            topic_id: Topic ID
            
        Returns:
            Mock topic object
        """
        # Check if topic already exists
        if topic_id in self._topics:
            return self._topics[topic_id]
        
        # Create mock topic
        mock_topic = create_mock_pubsub_topic(
            topic_id=topic_id,
            project_id=self.project_id
        )
        
        # Store topic
        self._topics[topic_id] = mock_topic
        self._messages[topic_id] = []
        
        return mock_topic
    
    def get_topic(self, topic_id: str) -> MagicMock:
        """
        Get a topic by ID.
        
        Args:
            topic_id: Topic ID
            
        Returns:
            Mock topic object
            
        Raises:
            Exception: If topic doesn't exist
        """
        # Check if topic exists
        if topic_id not in self._topics:
            error = type('NotFound', (Exception,), {})
            raise error(f"Topic {topic_id} not found")
        
        return self._topics[topic_id]
    
    def create_subscription(self, subscription_id: str, topic_id: str) -> MagicMock:
        """
        Create a new subscription.
        
        Args:
            subscription_id: Subscription ID
            topic_id: Topic ID
            
        Returns:
            Mock subscription object
            
        Raises:
            Exception: If topic doesn't exist
        """
        # Check if topic exists
        if topic_id not in self._topics:
            error = type('NotFound', (Exception,), {})
            raise error(f"Topic {topic_id} not found")
        
        # Check if subscription already exists
        if subscription_id in self._subscriptions:
            return self._subscriptions[subscription_id]
        
        # Create mock subscription
        mock_subscription = create_mock_pubsub_subscription(
            subscription_id=subscription_id,
            topic_id=topic_id,
            project_id=self.project_id
        )
        
        # Store subscription
        self._subscriptions[subscription_id] = mock_subscription
        
        return mock_subscription
    
    def get_subscription(self, subscription_id: str) -> MagicMock:
        """
        Get a subscription by ID.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Mock subscription object
            
        Raises:
            Exception: If subscription doesn't exist
        """
        # Check if subscription exists
        if subscription_id not in self._subscriptions:
            error = type('NotFound', (Exception,), {})
            raise error(f"Subscription {subscription_id} not found")
        
        return self._subscriptions[subscription_id]
    
    def publish(self, topic_id: str, data: bytes, attributes: Dict = None) -> str:
        """
        Publish a message to a topic.
        
        Args:
            topic_id: Topic ID
            data: Message data
            attributes: Message attributes
            
        Returns:
            Message ID
            
        Raises:
            Exception: If topic doesn't exist
        """
        # Check if topic exists
        if topic_id not in self._topics:
            error = type('NotFound', (Exception,), {})
            raise error(f"Topic {topic_id} not found")
        
        # Generate message ID
        message_id = generate_unique_id("msg")
        
        # Create message
        message = {
            "message_id": message_id,
            "data": data,
            "attributes": attributes or {},
            "publish_time": MagicMock()  # Mock timestamp
        }
        
        # Store message
        self._messages[topic_id].append(message)
        
        return message_id
    
    def pull(self, subscription_id: str, max_messages: int = None) -> List:
        """
        Pull messages from a subscription.
        
        Args:
            subscription_id: Subscription ID
            max_messages: Maximum number of messages to pull
            
        Returns:
            List of messages
            
        Raises:
            Exception: If subscription doesn't exist
        """
        # Check if subscription exists
        if subscription_id not in self._subscriptions:
            error = type('NotFound', (Exception,), {})
            raise error(f"Subscription {subscription_id} not found")
        
        # Get topic ID associated with subscription
        topic_id = self._subscriptions[subscription_id].topic_id
        
        # Get messages for topic
        messages = self._messages.get(topic_id, [])
        
        # Limit to max_messages if specified
        if max_messages is not None and max_messages < len(messages):
            return messages[:max_messages]
        
        return messages
    
    def acknowledge(self, subscription_id: str, ack_ids: List[str]) -> None:
        """
        Acknowledge receipt of messages.
        
        Args:
            subscription_id: Subscription ID
            ack_ids: List of message IDs to acknowledge
            
        Raises:
            Exception: If subscription doesn't exist
        """
        # Check if subscription exists
        if subscription_id not in self._subscriptions:
            error = type('NotFound', (Exception,), {})
            raise error(f"Subscription {subscription_id} not found")
        
        # Get topic ID associated with subscription
        topic_id = self._subscriptions[subscription_id].topic_id
        
        # Remove acknowledged messages from topic
        if topic_id in self._messages:
            self._messages[topic_id] = [
                msg for msg in self._messages[topic_id]
                if msg["message_id"] not in ack_ids
            ]


class MockDataGenerator:
    """Mock data generator for test datasets."""
    
    def __init__(self):
        """Initialize the mock data generator."""
        self._schemas = {}
        self._templates = {}
    
    def register_schema(self, schema_name: str, schema_definition: Dict) -> None:
        """
        Register a schema for data generation.
        
        Args:
            schema_name: Name of the schema
            schema_definition: Schema definition dictionary
        """
        self._schemas[schema_name] = schema_definition
    
    def register_template(self, template_name: str, template_definition: Dict) -> None:
        """
        Register a template for data generation.
        
        Args:
            template_name: Name of the template
            template_definition: Template definition dictionary
        """
        self._templates[template_name] = template_definition
    
    def generate_dataframe(self, schema_name: str, num_rows: int = 100) -> pd.DataFrame:
        """
        Generate a pandas DataFrame with random data.
        
        Args:
            schema_name: Name of the registered schema
            num_rows: Number of rows to generate
            
        Returns:
            Generated DataFrame
            
        Raises:
            ValueError: If schema is not registered
        """
        if schema_name not in self._schemas:
            raise ValueError(f"Schema '{schema_name}' is not registered")
        
        schema_definition = self._schemas[schema_name]
        return create_test_dataframe(schema_definition, num_rows)
    
    def generate_json_data(self, template_name: str, as_string: bool = False) -> Union[str, Dict]:
        """
        Generate JSON data with random values.
        
        Args:
            template_name: Name of the registered template
            as_string: Whether to return a JSON string instead of a dictionary
            
        Returns:
            Generated JSON data
            
        Raises:
            ValueError: If template is not registered
        """
        if template_name not in self._templates:
            raise ValueError(f"Template '{template_name}' is not registered")
        
        template_definition = self._templates[template_name]
        return create_test_json_data(template_definition, as_string)