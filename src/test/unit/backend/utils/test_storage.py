import unittest
from unittest.mock import patch, MagicMock, Mock, call
import pytest
import pandas as pd
import io
from google.cloud.bigquery import Client as BQClient, SchemaField, LoadJobConfig, QueryJobConfig, Table
from google.cloud.storage import Client as GCSClient, Bucket, Blob
from google.cloud.firestore import Client as FirestoreClient, CollectionReference, DocumentReference
from google.api_core.exceptions import NotFound, Forbidden, ServiceUnavailable, DeadlineExceeded, TooManyRequests

from src.backend.utils.storage.bigquery_client import BigQueryClient, BigQueryJobConfig, format_query_parameters, get_table_schema, format_table_reference
from src.backend.utils.storage.gcs_client import GCSClient, map_gcs_exception_to_pipeline_error, get_content_type
from src.backend.utils.storage.firestore_client import FirestoreClient, map_firestore_exception_to_pipeline_error
from src.backend.constants import FileFormat
from src.backend.utils.errors.error_types import PipelineError, ConnectionError, ResourceError, DataError


class TestBigQueryClient(unittest.TestCase):
    """Test cases for the BigQueryClient class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mocks for BigQuery client
        self.mock_bq_client = MagicMock(spec=BQClient)
        self.mock_project_id = "test-project"
        self.mock_dataset = "test_dataset"
        self.mock_location = "us-central1"
        
        # Mock the BigQuery client instantiation
        self.patcher = patch('src.backend.utils.storage.bigquery_client.bigquery.Client')
        self.mock_client_constructor = self.patcher.start()
        self.mock_client_constructor.return_value = self.mock_bq_client
        
        # Create the client to test
        self.client = BigQueryClient(
            project_id=self.mock_project_id,
            location=self.mock_location
        )

    def tearDown(self):
        """Clean up after each test method"""
        self.patcher.stop()

    def test_init(self):
        """Test BigQueryClient initialization"""
        # Check that client was initialized with correct parameters
        self.mock_client_constructor.assert_called_once_with(
            project=self.mock_project_id,
            location=self.mock_location
        )
        
        # Test initialization with default values
        with patch('src.backend.utils.storage.bigquery_client.get_project_id') as mock_get_project_id:
            with patch('src.backend.utils.storage.bigquery_client.get_config') as mock_get_config:
                mock_get_project_id.return_value = "default-project"
                mock_get_config.return_value = {"location": "us-west1"}
                
                client = BigQueryClient()
                
                mock_get_project_id.assert_called_once()
                mock_get_config.assert_called_once()
                self.mock_client_constructor.assert_called_with(
                    project="default-project",
                    location="us-west1"
                )

    def test_execute_query(self):
        """Test execute_query method"""
        # Prepare mock for query job
        mock_query_job = MagicMock()
        mock_results = MagicMock()
        mock_query_job.result.return_value = mock_results
        self.mock_bq_client.query.return_value = mock_query_job
        
        # Test parameters
        test_query = "SELECT * FROM test_table"
        test_timeout = 60
        test_job_config = BigQueryJobConfig(use_query_cache=False)
        
        # Execute the query
        result = self.client.execute_query(
            query=test_query,
            timeout=test_timeout,
            job_config=test_job_config
        )
        
        # Verify query was executed with correct parameters
        self.mock_bq_client.query.assert_called_once()
        call_args = self.mock_bq_client.query.call_args[0]
        self.assertEqual(call_args[0], test_query)
        
        # Verify job_config was passed correctly
        self.assertFalse(call_args[1].use_query_cache)
        
        # Verify result was returned
        self.assertEqual(result, mock_results)
        mock_query_job.result.assert_called_once_with(timeout=test_timeout)

    def test_execute_query_with_parameters(self):
        """Test execute_query with query parameters"""
        # Prepare mock for format_query_parameters
        with patch('src.backend.utils.storage.bigquery_client.format_query_parameters') as mock_format:
            mock_format.return_value = {"test_param": "formatted_value"}
            
            # Prepare mock for query job
            mock_query_job = MagicMock()
            mock_results = MagicMock()
            mock_query_job.result.return_value = mock_results
            self.mock_bq_client.query.return_value = mock_query_job
            
            # Test parameters
            test_query = "SELECT * FROM test_table WHERE col = @test_param"
            test_parameters = {"test_param": "value"}
            
            # Execute the query
            result = self.client.execute_query(
                query=test_query,
                parameters=test_parameters
            )
            
            # Verify format_query_parameters was called with correct parameters
            mock_format.assert_called_once_with(test_parameters)
            
            # Verify query was executed with formatted parameters
            self.mock_bq_client.query.assert_called_once()
            job_config = self.mock_bq_client.query.call_args[0][1]
            self.assertEqual(job_config.query_parameters, {"test_param": "formatted_value"})

    def test_execute_query_to_dataframe(self):
        """Test execute_query_to_dataframe method"""
        # Mock execute_query and to_dataframe
        mock_results = MagicMock()
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_results.to_dataframe.return_value = mock_df
        
        with patch.object(self.client, 'execute_query', return_value=mock_results) as mock_execute:
            # Test parameters
            test_query = "SELECT * FROM test_table"
            test_timeout = 60
            
            # Execute the query
            result_df = self.client.execute_query_to_dataframe(
                query=test_query,
                timeout=test_timeout
            )
            
            # Verify execute_query was called with correct parameters
            mock_execute.assert_called_once_with(
                query=test_query,
                timeout=test_timeout,
                job_config=None,
                parameters=None
            )
            
            # Verify to_dataframe was called
            mock_results.to_dataframe.assert_called_once()
            
            # Verify DataFrame is returned
            pd.testing.assert_frame_equal(result_df, mock_df)

    def test_load_table_from_gcs(self):
        """Test load_table_from_gcs method"""
        # Prepare mock for load job
        mock_load_job = MagicMock()
        mock_table = MagicMock()
        mock_load_job.result.return_value = mock_table
        self.mock_bq_client.load_table_from_uri.return_value = mock_load_job
        
        # Test parameters
        test_uri = "gs://test-bucket/test-file.csv"
        test_dataset = "test_dataset"
        test_table = "test_table"
        test_schema = [SchemaField("col1", "STRING"), SchemaField("col2", "INTEGER")]
        
        # Execute the load
        result = self.client.load_table_from_gcs(
            uri=test_uri,
            dataset=test_dataset,
            table=test_table,
            schema=test_schema,
            source_format=FileFormat.CSV
        )
        
        # Verify load_table_from_uri was called with correct parameters
        self.mock_bq_client.load_table_from_uri.assert_called_once()
        call_args = self.mock_bq_client.load_table_from_uri.call_args
        
        # Check URI
        self.assertEqual(call_args[0][0], test_uri)
        
        # Check destination table reference
        self.assertEqual(call_args[0][1], f"{self.mock_project_id}.{test_dataset}.{test_table}")
        
        # Check job config
        job_config = call_args[1]["job_config"]
        self.assertEqual(job_config.schema, test_schema)
        self.assertEqual(job_config.source_format, "CSV")
        
        # Verify job was waited for
        mock_load_job.result.assert_called_once()
        
        # Verify table was returned
        self.assertEqual(result, mock_table)

    def test_load_table_from_dataframe(self):
        """Test load_table_from_dataframe method"""
        # Prepare test DataFrame
        test_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        
        # Prepare mock for load job
        mock_load_job = MagicMock()
        mock_table = MagicMock()
        mock_load_job.result.return_value = mock_table
        self.mock_bq_client.load_table_from_dataframe.return_value = mock_load_job
        
        # Test parameters
        test_dataset = "test_dataset"
        test_table = "test_table"
        
        # Execute the load
        result = self.client.load_table_from_dataframe(
            dataframe=test_df,
            dataset=test_dataset,
            table=test_table
        )
        
        # Verify load_table_from_dataframe was called with correct parameters
        self.mock_bq_client.load_table_from_dataframe.assert_called_once()
        call_args = self.mock_bq_client.load_table_from_dataframe.call_args
        
        # Check DataFrame
        pd.testing.assert_frame_equal(call_args[0][0], test_df)
        
        # Check destination table reference
        self.assertEqual(call_args[0][1], f"{self.mock_project_id}.{test_dataset}.{test_table}")
        
        # Verify job was waited for
        mock_load_job.result.assert_called_once()
        
        # Verify table was returned
        self.assertEqual(result, mock_table)

    def test_create_table(self):
        """Test create_table method"""
        # Mock Table class
        with patch('src.backend.utils.storage.bigquery_client.bigquery.Table') as mock_table_class:
            mock_table = MagicMock()
            mock_table_class.return_value = mock_table
            
            self.mock_bq_client.create_table.return_value = mock_table
            
            # Test parameters
            test_dataset = "test_dataset"
            test_table = "test_table"
            test_schema = [SchemaField("col1", "STRING"), SchemaField("col2", "INTEGER")]
            
            # Execute the creation
            result = self.client.create_table(
                dataset=test_dataset,
                table=test_table,
                schema=test_schema
            )
            
            # Verify Table was instantiated with correct parameters
            mock_table_class.assert_called_once_with(
                f"{self.mock_project_id}.{test_dataset}.{test_table}",
                schema=test_schema
            )
            
            # Verify create_table was called with Table instance
            self.mock_bq_client.create_table.assert_called_once_with(mock_table)
            
            # Verify table was returned
            self.assertEqual(result, mock_table)

    def test_table_exists(self):
        """Test table_exists method"""
        # Test when table exists
        self.mock_bq_client.get_table.return_value = MagicMock()
        
        test_dataset = "test_dataset"
        test_table = "test_table"
        
        # Check table exists
        result = self.client.table_exists(
            dataset=test_dataset,
            table=test_table
        )
        
        # Verify get_table was called with correct table reference
        self.mock_bq_client.get_table.assert_called_once_with(
            f"{self.mock_project_id}.{test_dataset}.{test_table}"
        )
        
        # Verify result is True
        self.assertTrue(result)
        
        # Test when table does not exist
        self.mock_bq_client.get_table.reset_mock()
        self.mock_bq_client.get_table.side_effect = NotFound("Table not found")
        
        # Check table exists
        result = self.client.table_exists(
            dataset=test_dataset,
            table=test_table
        )
        
        # Verify get_table was called
        self.mock_bq_client.get_table.assert_called_once()
        
        # Verify result is False
        self.assertFalse(result)

    def test_get_table_rows_count(self):
        """Test get_table_rows_count method"""
        # Mock execute_query
        mock_rows = MagicMock()
        mock_rows.__iter__.return_value = [{"row_count": 42}]
        
        with patch.object(self.client, 'execute_query', return_value=mock_rows) as mock_execute:
            # Test parameters
            test_dataset = "test_dataset"
            test_table = "test_table"
            
            # Get row count
            result = self.client.get_table_rows_count(
                dataset=test_dataset,
                table=test_table
            )
            
            # Verify execute_query was called with correct COUNT query
            expected_query = f"SELECT COUNT(*) as row_count FROM `{self.mock_project_id}.{test_dataset}.{test_table}`"
            mock_execute.assert_called_once_with(expected_query)
            
            # Verify correct row count is returned
            self.assertEqual(result, 42)

    def test_format_query_parameters(self):
        """Test format_query_parameters function"""
        # Test parameters with different types
        test_params = {
            "string_param": "value",
            "int_param": 42,
            "float_param": 3.14,
            "bool_param": True,
            "list_param": [1, 2, 3],
            "date_param": pd.Timestamp("2023-01-01").date(),
            "datetime_param": pd.Timestamp("2023-01-01 12:34:56")
        }
        
        # Format parameters
        result = format_query_parameters(test_params)
        
        # Verify parameters are formatted correctly
        self.assertEqual(len(result), len(test_params))
        
        # Check string parameter
        self.assertEqual(result["string_param"].string_value, "value")
        
        # Check int parameter
        self.assertEqual(result["int_param"].numeric_value, 42)
        
        # Check float parameter
        self.assertEqual(result["float_param"].numeric_value, 3.14)
        
        # Check bool parameter
        self.assertEqual(result["bool_param"].bool_value, True)
        
        # Check list parameter
        self.assertEqual(result["list_param"].array_value[0].numeric_value, 1)
        self.assertEqual(result["list_param"].array_value[1].numeric_value, 2)
        self.assertEqual(result["list_param"].array_value[2].numeric_value, 3)
        
        # Check date parameter
        self.assertEqual(result["date_param"].string_value, "2023-01-01")
        
        # Check datetime parameter
        self.assertTrue(result["datetime_param"].string_value.startswith("2023-01-01T12:34:56"))

    def test_get_table_schema(self):
        """Test get_table_schema function"""
        # Test schema definition
        test_schema = [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "address", 
                "type": "RECORD", 
                "mode": "NULLABLE",
                "fields": [
                    {"name": "street", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "city", "type": "STRING", "mode": "NULLABLE"}
                ]
            }
        ]
        
        # Get schema fields
        result = get_table_schema(test_schema)
        
        # Verify SchemaField objects are created correctly
        self.assertEqual(len(result), 3)
        
        # Check first field
        self.assertEqual(result[0].name, "id")
        self.assertEqual(result[0].field_type, "INTEGER")
        self.assertEqual(result[0].mode, "REQUIRED")
        
        # Check second field
        self.assertEqual(result[1].name, "name")
        self.assertEqual(result[1].field_type, "STRING")
        self.assertEqual(result[1].mode, "NULLABLE")
        
        # Check nested field
        self.assertEqual(result[2].name, "address")
        self.assertEqual(result[2].field_type, "RECORD")
        self.assertEqual(len(result[2].fields), 2)
        self.assertEqual(result[2].fields[0].name, "street")
        self.assertEqual(result[2].fields[1].name, "city")

    def test_format_table_reference(self):
        """Test format_table_reference function"""
        # Test parameters
        test_project = "test-project"
        test_dataset = "test_dataset"
        test_table = "test_table"
        
        # Format table reference
        result = format_table_reference(
            project=test_project,
            dataset=test_dataset,
            table=test_table
        )
        
        # Verify reference is formatted correctly
        self.assertEqual(result, "test-project.test_dataset.test_table")

    def test_error_handling(self):
        """Test error handling in BigQueryClient"""
        # Configure mock to raise different exceptions
        self.mock_bq_client.query.side_effect = [
            ServiceUnavailable("Service unavailable"),  # First call - transient error
            DeadlineExceeded("Deadline exceeded"),      # Second call - transient error
            None                                       # Third call - success
        ]
        
        # Mock result for successful query
        mock_query_job = MagicMock()
        mock_results = MagicMock()
        mock_query_job.result.return_value = mock_results
        self.mock_bq_client.query.return_value = mock_query_job
        
        # Execute the query (should retry automatically)
        result = self.client.execute_query("SELECT * FROM test_table")
        
        # Verify query was called multiple times (retries)
        self.assertEqual(self.mock_bq_client.query.call_count, 3)
        
        # Verify final result was returned
        self.assertEqual(result, mock_results)
        
        # Test non-retryable error (permissions)
        self.mock_bq_client.query.reset_mock()
        self.mock_bq_client.query.side_effect = Forbidden("Permission denied")
        
        # Should raise a PipelineError
        with self.assertRaises(PipelineError):
            self.client.execute_query("SELECT * FROM test_table")
        
        # Verify query was called only once (no retry)
        self.mock_bq_client.query.assert_called_once()


class TestGCSClient(unittest.TestCase):
    """Test cases for the GCSClient class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mocks for GCS client
        self.mock_gcs_client = MagicMock(spec=GCSClient)
        self.mock_project_id = "test-project"
        self.mock_location = "us-central1"
        
        # Mock the GCS client instantiation
        self.patcher = patch('src.backend.utils.storage.gcs_client.storage.Client')
        self.mock_client_constructor = self.patcher.start()
        self.mock_client_constructor.return_value = self.mock_gcs_client
        
        # Create the client to test
        self.client = GCSClient(
            project_id=self.mock_project_id
        )

    def tearDown(self):
        """Clean up after each test method"""
        self.patcher.stop()

    def test_init(self):
        """Test GCSClient initialization"""
        # Check that client was initialized with correct parameters
        self.mock_client_constructor.assert_called_once_with(
            project=self.mock_project_id
        )
        
        # Test initialization with default values
        with patch('src.backend.utils.storage.gcs_client.get_project_id') as mock_get_project_id:
            mock_get_project_id.return_value = "default-project"
            
            client = GCSClient()
            
            mock_get_project_id.assert_called_once()
            self.mock_client_constructor.assert_called_with(
                project="default-project"
            )

    def test_get_bucket(self):
        """Test get_bucket method"""
        # Prepare mock bucket
        mock_bucket = MagicMock(spec=Bucket)
        self.mock_gcs_client.get_bucket.return_value = mock_bucket
        
        # Test bucket name
        test_bucket = "test-bucket"
        
        # Get the bucket
        result = self.client.get_bucket(test_bucket)
        
        # Verify get_bucket was called with correct name
        self.mock_gcs_client.get_bucket.assert_called_once_with(test_bucket)
        
        # Verify bucket was returned
        self.assertEqual(result, mock_bucket)

    def test_bucket_exists(self):
        """Test bucket_exists method"""
        # Mock get_bucket method
        mock_bucket = MagicMock(spec=Bucket)
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test when bucket exists
            result = self.client.bucket_exists("test-bucket")
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with("test-bucket")
            
            # Verify result is True
            self.assertTrue(result)
            
            # Test when bucket does not exist
            mock_get_bucket.reset_mock()
            mock_get_bucket.side_effect = NotFound("Bucket not found")
            
            result = self.client.bucket_exists("test-bucket")
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with("test-bucket")
            
            # Verify result is False
            self.assertFalse(result)

    def test_upload_file(self):
        """Test upload_file method"""
        # Mock get_bucket and blob
        mock_bucket = MagicMock(spec=Bucket)
        mock_blob = MagicMock(spec=Blob)
        mock_bucket.blob.return_value = mock_blob
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test parameters
            test_bucket = "test-bucket"
            test_source = "/path/to/local/file.txt"
            test_destination = "path/in/bucket/file.txt"
            
            # Upload the file
            result = self.client.upload_file(
                bucket_name=test_bucket,
                source_file_path=test_source,
                destination_blob_name=test_destination
            )
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with(test_bucket)
            
            # Verify blob was created with correct name
            mock_bucket.blob.assert_called_once_with(test_destination)
            
            # Verify upload_from_filename was called
            mock_blob.upload_from_filename.assert_called_once_with(
                test_source,
                content_type=None
            )
            
            # Verify blob was returned
            self.assertEqual(result, mock_blob)

    def test_upload_from_string(self):
        """Test upload_from_string method"""
        # Mock get_bucket and blob
        mock_bucket = MagicMock(spec=Bucket)
        mock_blob = MagicMock(spec=Blob)
        mock_bucket.blob.return_value = mock_blob
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test parameters
            test_bucket = "test-bucket"
            test_content = "file content"
            test_destination = "path/in/bucket/file.txt"
            test_content_type = "text/plain"
            
            # Upload the content
            result = self.client.upload_from_string(
                bucket_name=test_bucket,
                contents=test_content,
                destination_blob_name=test_destination,
                content_type=test_content_type
            )
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with(test_bucket)
            
            # Verify blob was created with correct name
            mock_bucket.blob.assert_called_once_with(test_destination)
            
            # Verify upload_from_string was called
            mock_blob.upload_from_string.assert_called_once_with(
                test_content,
                content_type=test_content_type
            )
            
            # Verify blob was returned
            self.assertEqual(result, mock_blob)

    def test_download_as_string(self):
        """Test download_as_string method"""
        # Mock get_bucket and blob
        mock_bucket = MagicMock(spec=Bucket)
        mock_blob = MagicMock(spec=Blob)
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_text.return_value = "file content"
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test parameters
            test_bucket = "test-bucket"
            test_blob = "path/in/bucket/file.txt"
            
            # Download the content
            result = self.client.download_as_string(
                bucket_name=test_bucket,
                blob_name=test_blob
            )
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with(test_bucket)
            
            # Verify blob was retrieved with correct name
            mock_bucket.blob.assert_called_once_with(test_blob)
            
            # Verify download_as_text was called
            mock_blob.download_as_text.assert_called_once()
            
            # Verify content was returned
            self.assertEqual(result, "file content")

    def test_list_blobs(self):
        """Test list_blobs method"""
        # Mock get_bucket and list_blobs
        mock_bucket = MagicMock(spec=Bucket)
        mock_blob1 = MagicMock(spec=Blob)
        mock_blob2 = MagicMock(spec=Blob)
        mock_blob1.name = "prefix/file1.txt"
        mock_blob2.name = "prefix/file2.txt"
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test parameters
            test_bucket = "test-bucket"
            test_prefix = "prefix/"
            
            # List blobs
            result = self.client.list_blobs(
                bucket_name=test_bucket,
                prefix=test_prefix
            )
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with(test_bucket)
            
            # Verify list_blobs was called with correct prefix
            mock_bucket.list_blobs.assert_called_once_with(
                prefix=test_prefix,
                delimiter=None
            )
            
            # Verify blob names were returned
            self.assertEqual(result, ["prefix/file1.txt", "prefix/file2.txt"])

    def test_blob_exists(self):
        """Test blob_exists method"""
        # Mock get_bucket and blob
        mock_bucket = MagicMock(spec=Bucket)
        mock_blob = MagicMock(spec=Blob)
        mock_bucket.blob.return_value = mock_blob
        
        with patch.object(self.client, 'get_bucket', return_value=mock_bucket) as mock_get_bucket:
            # Test when blob exists
            mock_blob.exists.return_value = True
            
            result = self.client.blob_exists(
                bucket_name="test-bucket",
                blob_name="test-blob"
            )
            
            # Verify get_bucket was called
            mock_get_bucket.assert_called_once_with("test-bucket")
            
            # Verify blob was retrieved
            mock_bucket.blob.assert_called_once_with("test-blob")
            
            # Verify exists was called
            mock_blob.exists.assert_called_once()
            
            # Verify result is True
            self.assertTrue(result)
            
            # Test when blob does not exist
            mock_get_bucket.reset_mock()
            mock_bucket.blob.reset_mock()
            mock_blob.exists.reset_mock()
            mock_blob.exists.return_value = False
            
            result = self.client.blob_exists(
                bucket_name="test-bucket",
                blob_name="test-blob"
            )
            
            # Verify result is False
            self.assertFalse(result)

    def test_get_content_type(self):
        """Test get_content_type function"""
        # Test with file_format parameter
        self.assertEqual(get_content_type(file_format=FileFormat.CSV), "text/csv")
        self.assertEqual(get_content_type(file_format=FileFormat.JSON), "application/json")
        self.assertEqual(get_content_type(file_format=FileFormat.AVRO), "application/avro")
        self.assertEqual(get_content_type(file_format=FileFormat.PARQUET), "application/parquet")
        
        # Test with file_path parameter
        self.assertEqual(get_content_type(file_path="file.csv"), "text/csv")
        self.assertEqual(get_content_type(file_path="file.json"), "application/json")
        self.assertEqual(get_content_type(file_path="file.avro"), "application/avro")
        self.assertEqual(get_content_type(file_path="file.parquet"), "application/parquet")
        self.assertEqual(get_content_type(file_path="file.txt"), "text/plain")
        
        # Test with unknown extension
        self.assertEqual(get_content_type(file_path="file.unknown"), "application/octet-stream")
        
        # Test with no parameters
        self.assertEqual(get_content_type(), "application/octet-stream")

    def test_map_gcs_exception_to_pipeline_error(self):
        """Test map_gcs_exception_to_pipeline_error function"""
        # Test NotFound exception
        not_found = NotFound("Bucket not found")
        mapped_error = map_gcs_exception_to_pipeline_error(not_found, "test-bucket")
        self.assertIsInstance(mapped_error, ResourceError)
        self.assertEqual(mapped_error.context["resource_name"], "test-bucket")
        
        # Test Forbidden exception
        forbidden = Forbidden("Permission denied")
        mapped_error = map_gcs_exception_to_pipeline_error(forbidden, "test-bucket")
        self.assertIsInstance(mapped_error, PipelineError)
        self.assertEqual(mapped_error.context["resource_name"], "test-bucket")
        
        # Test ServiceUnavailable exception
        unavailable = ServiceUnavailable("Service unavailable")
        mapped_error = map_gcs_exception_to_pipeline_error(unavailable, "test-bucket")
        self.assertIsInstance(mapped_error, ConnectionError)
        self.assertEqual(mapped_error.context["resource_name"], "test-bucket")
        self.assertTrue(mapped_error.retryable)

    def test_error_handling(self):
        """Test error handling in GCSClient"""
        # Configure mock to raise different exceptions
        mock_bucket = MagicMock(spec=Bucket)
        self.mock_gcs_client.get_bucket.side_effect = [
            ServiceUnavailable("Service unavailable"),  # First call - transient error
            DeadlineExceeded("Deadline exceeded"),      # Second call - transient error
            mock_bucket                                # Third call - success
        ]
        
        # Get the bucket (should retry automatically)
        result = self.client.get_bucket("test-bucket")
        
        # Verify get_bucket was called multiple times (retries)
        self.assertEqual(self.mock_gcs_client.get_bucket.call_count, 3)
        
        # Verify final result was returned
        self.assertEqual(result, mock_bucket)
        
        # Test non-retryable error (permissions)
        self.mock_gcs_client.get_bucket.reset_mock()
        self.mock_gcs_client.get_bucket.side_effect = Forbidden("Permission denied")
        
        # Should raise a PipelineError
        with self.assertRaises(PipelineError):
            self.client.get_bucket("test-bucket")
        
        # Verify get_bucket was called only once (no retry)
        self.mock_gcs_client.get_bucket.assert_called_once()


class TestFirestoreClient(unittest.TestCase):
    """Test cases for the FirestoreClient class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mocks for Firestore client
        self.mock_firestore_client = MagicMock(spec=FirestoreClient)
        self.mock_project_id = "test-project"
        
        # Mock the Firestore client instantiation
        self.patcher = patch('src.backend.utils.storage.firestore_client.firestore.Client')
        self.mock_client_constructor = self.patcher.start()
        self.mock_client_constructor.return_value = self.mock_firestore_client
        
        # Create the client to test
        self.client = FirestoreClient(
            project_id=self.mock_project_id
        )

    def tearDown(self):
        """Clean up after each test method"""
        self.patcher.stop()

    def test_init(self):
        """Test FirestoreClient initialization"""
        # Check that client was initialized with correct parameters
        self.mock_client_constructor.assert_called_once_with(
            project=self.mock_project_id
        )
        
        # Test initialization with default values
        with patch('src.backend.utils.storage.firestore_client.get_project_id') as mock_get_project_id:
            mock_get_project_id.return_value = "default-project"
            
            client = FirestoreClient()
            
            mock_get_project_id.assert_called_once()
            self.mock_client_constructor.assert_called_with(
                project="default-project"
            )

    def test_collection(self):
        """Test collection method"""
        # Prepare mock collection
        mock_collection = MagicMock(spec=CollectionReference)
        self.mock_firestore_client.collection.return_value = mock_collection
        
        # Test collection name
        test_collection = "test-collection"
        
        # Get the collection
        result = self.client.collection(test_collection)
        
        # Verify collection was called with correct name
        self.mock_firestore_client.collection.assert_called_once_with(test_collection)
        
        # Verify collection was returned
        self.assertEqual(result, mock_collection)

    def test_document(self):
        """Test document method"""
        # Mock collection method
        mock_collection = MagicMock(spec=CollectionReference)
        mock_document = MagicMock(spec=DocumentReference)
        mock_collection.document.return_value = mock_document
        
        with patch.object(self.client, 'collection', return_value=mock_collection) as mock_collection_method:
            # Test parameters
            test_collection = "test-collection"
            test_document = "test-document"
            
            # Get the document
            result = self.client.document(
                collection_name=test_collection,
                document_id=test_document
            )
            
            # Verify collection was called
            mock_collection_method.assert_called_once_with(test_collection)
            
            # Verify document was called with correct ID
            mock_collection.document.assert_called_once_with(test_document)
            
            # Verify document was returned
            self.assertEqual(result, mock_document)

    def test_create_document(self):
        """Test create_document method"""
        # Mock collection method
        mock_collection = MagicMock(spec=CollectionReference)
        mock_document = MagicMock(spec=DocumentReference)
        mock_collection.document.return_value = mock_document
        
        with patch.object(self.client, 'collection', return_value=mock_collection) as mock_collection_method:
            # Test parameters
            test_collection = "test-collection"
            test_document = "test-document"
            test_data = {"field1": "value1", "field2": 42}
            
            # Create the document
            result = self.client.create_document(
                collection_name=test_collection,
                document_id=test_document,
                data=test_data
            )
            
            # Verify collection was called
            mock_collection_method.assert_called_once_with(test_collection)
            
            # Verify document was created with correct ID
            mock_collection.document.assert_called_once_with(test_document)
            
            # Verify set was called with correct data
            mock_document.set.assert_called_once_with(test_data)
            
            # Verify document was returned
            self.assertEqual(result, mock_document)

    def test_get_document(self):
        """Test get_document method"""
        # Mock document method
        mock_document = MagicMock(spec=DocumentReference)
        mock_snapshot = MagicMock()
        mock_document.get.return_value = mock_snapshot
        
        with patch.object(self.client, 'document', return_value=mock_document) as mock_document_method:
            # Test when document exists
            mock_snapshot.exists = True
            mock_snapshot.to_dict.return_value = {"field1": "value1", "field2": 42}
            
            # Test parameters
            test_collection = "test-collection"
            test_document = "test-document"
            
            # Get the document
            result = self.client.get_document(
                collection_name=test_collection,
                document_id=test_document
            )
            
            # Verify document was retrieved
            mock_document_method.assert_called_once_with(test_collection, test_document)
            
            # Verify get was called
            mock_document.get.assert_called_once()
            
            # Verify to_dict was called
            mock_snapshot.to_dict.assert_called_once()
            
            # Verify data was returned
            self.assertEqual(result, {"field1": "value1", "field2": 42})
            
            # Test when document does not exist
            mock_document_method.reset_mock()
            mock_document.get.reset_mock()
            mock_snapshot.to_dict.reset_mock()
            mock_snapshot.exists = False
            
            result = self.client.get_document(
                collection_name=test_collection,
                document_id=test_document
            )
            
            # Verify document was retrieved
            mock_document_method.assert_called_once()
            
            # Verify get was called
            mock_document.get.assert_called_once()
            
            # Verify to_dict was not called
            mock_snapshot.to_dict.assert_not_called()
            
            # Verify None was returned
            self.assertIsNone(result)

    def test_document_exists(self):
        """Test document_exists method"""
        # Mock document method
        mock_document = MagicMock(spec=DocumentReference)
        mock_snapshot = MagicMock()
        mock_document.get.return_value = mock_snapshot
        
        with patch.object(self.client, 'document', return_value=mock_document) as mock_document_method:
            # Test when document exists
            mock_snapshot.exists = True
            
            result = self.client.document_exists(
                collection_name="test-collection",
                document_id="test-document"
            )
            
            # Verify document was retrieved
            mock_document_method.assert_called_once_with("test-collection", "test-document")
            
            # Verify get was called
            mock_document.get.assert_called_once()
            
            # Verify result is True
            self.assertTrue(result)
            
            # Test when document does not exist
            mock_document_method.reset_mock()
            mock_document.get.reset_mock()
            mock_snapshot.exists = False
            
            result = self.client.document_exists(
                collection_name="test-collection",
                document_id="test-document"
            )
            
            # Verify result is False
            self.assertFalse(result)

    def test_query_documents(self):
        """Test query_documents method"""
        # Mock collection method
        mock_collection = MagicMock(spec=CollectionReference)
        mock_query = MagicMock()
        mock_collection.where.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        
        # Mock document snapshots
        mock_doc1 = MagicMock()
        mock_doc2 = MagicMock()
        mock_doc1.to_dict.return_value = {"id": 1, "name": "Doc 1"}
        mock_doc2.to_dict.return_value = {"id": 2, "name": "Doc 2"}
        mock_query.stream.return_value = [mock_doc1, mock_doc2]
        
        with patch.object(self.client, 'collection', return_value=mock_collection) as mock_collection_method:
            # Test parameters
            test_collection = "test-collection"
            test_filters = [("field", "==", "value")]
            test_order_by = "created_at"
            test_limit = 10
            
            # Query documents
            result = self.client.query_documents(
                collection_name=test_collection,
                filters=test_filters,
                order_by=test_order_by,
                limit=test_limit
            )
            
            # Verify collection was called
            mock_collection_method.assert_called_once_with(test_collection)
            
            # Verify where was called with filter
            mock_collection.where.assert_called_once_with("field", "==", "value")
            
            # Verify order_by was called
            mock_query.order_by.assert_called_once_with(test_order_by)
            
            # Verify limit was called
            mock_query.limit.assert_called_once_with(test_limit)
            
            # Verify stream was called
            mock_query.stream.assert_called_once()
            
            # Verify documents were returned
            self.assertEqual(result, [{"id": 1, "name": "Doc 1"}, {"id": 2, "name": "Doc 2"}])

    def test_batch_operations(self):
        """Test batch operations (create, update, delete)"""
        # Mock batch
        mock_batch = MagicMock()
        self.mock_firestore_client.batch.return_value = mock_batch
        
        # Mock document for batch operations
        mock_document = MagicMock(spec=DocumentReference)
        
        with patch.object(self.client, 'document', return_value=mock_document):
            # Test batch_create
            test_collection = "test-collection"
            test_documents = {
                "doc1": {"field1": "value1"},
                "doc2": {"field2": "value2"}
            }
            
            self.client.batch_create(test_collection, test_documents)
            
            # Verify batch was used
            self.mock_firestore_client.batch.assert_called_once()
            
            # Verify set was called for each document
            self.assertEqual(mock_batch.set.call_count, 2)
            
            # Verify commit was called
            mock_batch.commit.assert_called_once()
            
            # Reset mocks
            self.mock_firestore_client.batch.reset_mock()
            mock_batch.set.reset_mock()
            mock_batch.commit.reset_mock()
            
            # Test batch_update
            self.client.batch_update(test_collection, test_documents)
            
            # Verify batch was used
            self.mock_firestore_client.batch.assert_called_once()
            
            # Verify update was called for each document
            self.assertEqual(mock_batch.update.call_count, 2)
            
            # Verify commit was called
            mock_batch.commit.assert_called_once()
            
            # Reset mocks
            self.mock_firestore_client.batch.reset_mock()
            mock_batch.update.reset_mock()
            mock_batch.commit.reset_mock()
            
            # Test batch_delete
            test_document_ids = ["doc1", "doc2"]
            
            self.client.batch_delete(test_collection, test_document_ids)
            
            # Verify batch was used
            self.mock_firestore_client.batch.assert_called_once()
            
            # Verify delete was called for each document
            self.assertEqual(mock_batch.delete.call_count, 2)
            
            # Verify commit was called
            mock_batch.commit.assert_called_once()

    def test_map_firestore_exception_to_pipeline_error(self):
        """Test map_firestore_exception_to_pipeline_error function"""
        # Test NotFound exception
        not_found = NotFound("Document not found")
        mapped_error = map_firestore_exception_to_pipeline_error(not_found, "test-collection", "test-document")
        self.assertIsInstance(mapped_error, ResourceError)
        self.assertEqual(mapped_error.context["collection_name"], "test-collection")
        self.assertEqual(mapped_error.context["document_id"], "test-document")
        
        # Test Forbidden exception
        forbidden = Forbidden("Permission denied")
        mapped_error = map_firestore_exception_to_pipeline_error(forbidden, "test-collection", "test-document")
        self.assertIsInstance(mapped_error, PipelineError)
        self.assertEqual(mapped_error.context["collection_name"], "test-collection")
        
        # Test ServiceUnavailable exception
        unavailable = ServiceUnavailable("Service unavailable")
        mapped_error = map_firestore_exception_to_pipeline_error(unavailable, "test-collection", "test-document")
        self.assertIsInstance(mapped_error, ConnectionError)
        self.assertEqual(mapped_error.context["collection_name"], "test-collection")
        self.assertTrue(mapped_error.retryable)

    def test_error_handling(self):
        """Test error handling in FirestoreClient"""
        # Configure mock to raise different exceptions
        self.mock_firestore_client.collection.side_effect = [
            ServiceUnavailable("Service unavailable"),  # First call - transient error
            DeadlineExceeded("Deadline exceeded"),      # Second call - transient error
            MagicMock(spec=CollectionReference)        # Third call - success
        ]
        
        # Get the collection (should retry automatically)
        result = self.client.collection("test-collection")
        
        # Verify collection was called multiple times (retries)
        self.assertEqual(self.mock_firestore_client.collection.call_count, 3)
        
        # Verify result is a collection
        self.assertIsInstance(result, MagicMock)
        
        # Test non-retryable error (permissions)
        self.mock_firestore_client.collection.reset_mock()
        self.mock_firestore_client.collection.side_effect = Forbidden("Permission denied")
        
        # Should raise a PipelineError
        with self.assertRaises(PipelineError):
            self.client.collection("test-collection")
        
        # Verify collection was called only once (no retry)
        self.mock_firestore_client.collection.assert_called_once()