"""
Unit tests for the data extractors in the self-healing data pipeline.
This file contains comprehensive tests for all extractor components including
API extractor, batch extractor, file extractor, and incremental extractor,
ensuring they correctly handle data extraction, error scenarios, and
integration with the self-healing mechanisms.
"""
import os
import io
import json
import datetime
import pytest
import pandas  # version: See requirements.txt
from unittest import mock

from src.backend.constants import DataSourceType, FileFormat, DEFAULT_MAX_RETRY_ATTEMPTS  # version: See src/backend/constants.py
from src.backend.ingestion.extractors import ApiExtractor, BatchExtractor, FileExtractor, IncrementalExtractor, detect_file_format, infer_schema  # version: See src/backend/ingestion/extractors/__init__.py
from src.backend.ingestion.connectors.api_connector import ApiConnector, ApiAuthType, ApiPaginationType  # version: See src/backend/ingestion/connectors/api_connector.py
from src.backend.utils.storage.gcs_client import GCSClient  # version: See src/backend/utils/storage/gcs_client.py
from src.backend.utils.storage.firestore_client import get_firestore_client  # version: See src/backend/utils/storage/firestore_client.py
from src.test.fixtures.backend.ingestion_fixtures import mock_gcs_client, mock_api_connector, sample_api_config, sample_gcs_config, test_ingestion_data  # version: See src/test/fixtures/backend/ingestion_fixtures.py
from src.test.utils.test_helpers import create_temp_file, load_test_data  # version: See src/test/utils/test_helpers.py

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '../../../mock_data')


def test_api_extractor_initialization():
    """Test that the API extractor initializes correctly with valid configuration"""
    source_id = "test-api-source"
    source_name = "Test API Source"
    extraction_config = {
        "base_url": "https://api.example.com/v1",
        "auth_type": "API_KEY",
        "auth_config": {"api_key": "test_api_key", "header_name": "X-API-Key"},
        "timeout": 30,
        "verify_ssl": True,
    }

    extractor = ApiExtractor(source_id, source_name, extraction_config)

    assert extractor.source_id == source_id
    assert extractor.source_name == source_name
    assert extractor.extraction_config == extraction_config
    assert extractor.connector is None
    assert extractor.extraction_stats == {
        "total_records": 0,
        "successful_extractions": 0,
        "failed_extractions": 0,
        "last_extraction_time": None,
    }


def test_api_extractor_extract_success(mock_api_connector, sample_api_config):
    """Test that the API extractor successfully extracts data from an API source"""
    extraction_params = {"endpoint_path": "/data", "method": "GET"}
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})
    extractor = ApiExtractor("test-api-source", "Test API Source", sample_api_config)
    extractor.connector = mock_api_connector

    data, metadata = extractor.extract(extraction_params)

    mock_api_connector.extract_data.assert_called_once_with(extraction_params)
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2
    assert extractor.extraction_stats["successful_extractions"] == 1


def test_api_extractor_extract_with_batching(mock_api_connector, sample_api_config):
    """Test that the API extractor correctly uses batch extraction when configured"""
    extraction_params = {"endpoint_path": "/data", "method": "GET", "batch_size": 100}
    sample_api_config["use_batching"] = True
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})
    extractor = ApiExtractor("test-api-source", "Test API Source", sample_api_config)
    extractor.connector = mock_api_connector
    extractor.batch_extractor = mock.MagicMock()
    extractor.batch_extractor.extract_in_batches.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})

    data, metadata = extractor.extract(extraction_params)

    extractor.batch_extractor.extract_in_batches.assert_called_once_with(extraction_params, mock_api_connector)
    mock_api_connector.extract_data.assert_not_called()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2
    assert extractor.extraction_stats["successful_extractions"] == 1


def test_api_extractor_connection_failure(mock_api_connector, sample_api_config):
    """Test that the API extractor handles connection failures appropriately"""
    extraction_params = {"endpoint_path": "/data", "method": "GET"}
    mock_api_connector.connect.side_effect = Exception("Connection failed")
    extractor = ApiExtractor("test-api-source", "Test API Source", sample_api_config)
    extractor.connector = mock_api_connector

    with pytest.raises(Exception, match="Connection failed"):
        extractor.extract(extraction_params)

    assert extractor.extraction_stats["failed_extractions"] == 0


def test_api_extractor_get_schema(mock_api_connector, sample_api_config):
    """Test that the API extractor correctly retrieves schema information"""
    mock_api_connector.get_source_schema.return_value = {"col1": "int", "col2": "str"}
    extractor = ApiExtractor("test-api-source", "Test API Source", sample_api_config)
    extractor.connector = mock_api_connector

    schema = extractor.get_schema("/data")

    mock_api_connector.get_source_schema.assert_called_once_with("/data")
    assert schema == {"col1": "int", "col2": "str"}


def test_api_extractor_close(mock_api_connector, sample_api_config):
    """Test that the API extractor correctly closes and cleans up resources"""
    extractor = ApiExtractor("test-api-source", "Test API Source", sample_api_config)
    extractor.connector = mock_api_connector

    extractor.close()

    mock_api_connector.disconnect.assert_called_once()
    assert extractor.connector is None


def test_batch_extractor_initialization():
    """Test that the batch extractor initializes correctly with valid configuration"""
    source_id = "test-batch-source"
    source_name = "Test Batch Source"
    extraction_config = {
        "batch_size": 500,
        "max_batches": 10,
        "batch_timeout_seconds": 600,
    }

    extractor = BatchExtractor(source_id, source_name, extraction_config)

    assert extractor.source_id == source_id
    assert extractor.source_name == source_name
    assert extractor.extraction_config == extraction_config
    assert extractor.batch_stats == {
        'total_batches_attempted': 0,
        'successful_batches': 0,
        'failed_batches': 0,
        'total_records_processed': 0,
        'total_processing_time': 0.0,
        'start_time': None,
        'end_time': None,
        'batch_history': [],
        'avg_batch_time': 0.0,
        'avg_records_per_batch': 0,
        'records_per_second': 0.0
    }
    assert extractor.default_batch_size == 500
    assert extractor.max_batches == 10
    assert extractor.batch_timeout_seconds == 600


def test_batch_extractor_extract_in_batches(mock_api_connector):
    """Test that the batch extractor correctly processes data in batches"""
    extraction_params = {"batch_size": 100}
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})
    extractor = BatchExtractor("test-batch-source", "Test Batch Source", {})
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})

    data, metadata = extractor.extract_in_batches(extraction_params, mock_api_connector)

    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["total_records"] == 2
    assert extractor.batch_stats["successful_batches"] == 1


def test_batch_extractor_process_batch(mock_api_connector):
    """Test that the batch extractor correctly processes a single batch"""
    extraction_params = {}
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2})
    extractor = BatchExtractor("test-batch-source", "Test Batch Source", {})

    data, metadata = extractor.process_batch(mock_api_connector, extraction_params, 1, 100, 0)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["batch_size"] == 100
    assert metadata["offset"] == 0


def test_batch_extractor_combine_batch_data():
    """Test that the batch extractor correctly combines data from multiple batches"""
    df1 = pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df2 = pandas.DataFrame({"col1": [5, 6], "col2": [7, 8]})
    batch_results = [df1, df2]
    extractor = BatchExtractor("test-batch-source", "Test Batch Source", {})

    combined_data = extractor.combine_batch_data(batch_results)

    assert isinstance(combined_data, pandas.DataFrame)
    assert len(combined_data) == 4
    assert combined_data["col1"].tolist() == [1, 2, 5, 6]


def test_batch_extractor_empty_batches(mock_api_connector):
    """Test that the batch extractor handles empty batches correctly"""
    extraction_params = {"batch_size": 100}
    mock_api_connector.extract_data.side_effect = [(pandas.DataFrame({"col1": [1, 2], "col2": [3, 4]}), {"record_count": 2}), (pandas.DataFrame(), {"record_count": 0})]
    extractor = BatchExtractor("test-batch-source", "Test Batch Source", {})

    data, metadata = extractor.extract_in_batches(extraction_params, mock_api_connector)

    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["total_records"] == 2
    assert extractor.batch_stats["successful_batches"] == 1
    assert extractor.batch_stats["failed_batches"] == 0


def test_file_extractor_initialization():
    """Test that the file extractor initializes correctly with valid configuration"""
    source_id = "test-file-source"
    source_name = "Test File Source"
    extraction_config = {}

    extractor = FileExtractor(source_id, source_name, extraction_config)

    assert extractor.source_id == source_id
    assert extractor.source_name == source_name
    assert extractor.extraction_config == extraction_config
    assert isinstance(extractor.format_handlers, dict)


def test_file_extractor_extract_local_csv(test_ingestion_data):
    """Test that the file extractor correctly extracts data from a local CSV file"""
    csv_data = "col1,col2\n1,2\n3,4"
    file_path = test_ingestion_data.generate_test_file(FileFormat.CSV, {"col1": "1", "col2": "2"})
    extraction_params = {}
    extractor = FileExtractor("test-file-source", "Test File Source", {})

    data, metadata = extractor.extract_file(file_path, extraction_params)

    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 1
    assert metadata["file_path"] == file_path


def test_file_extractor_extract_gcs_json(mock_gcs_client, sample_gcs_config):
    """Test that the file extractor correctly extracts data from a GCS JSON file"""
    json_content = '[{"col1": 1, "col2": 2}, {"col1": 3, "col2": 4}]'
    bucket_name = "test-bucket"
    blob_name = "test.json"
    mock_gcs_client.return_value.download_blob_as_bytes.return_value = json_content.encode()
    extraction_params = {}
    extractor = FileExtractor("test-file-source", "Test File Source", sample_gcs_config)
    extractor._gcs_client = mock_gcs_client.return_value

    data, metadata = extractor.extract_gcs_file(bucket_name, blob_name, extraction_params)

    mock_gcs_client.return_value.download_blob_as_bytes.assert_called_once_with(bucket_name, blob_name)
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["bucket"] == bucket_name
    assert metadata["blob"] == blob_name


def test_file_extractor_extract_parquet(test_ingestion_data):
    """Test that the file extractor correctly extracts data from a Parquet file"""
    data = {'col1': [1, 2], 'col2': [3, 4]}
    df = pandas.DataFrame(data)
    file_path = create_temp_file()
    df.to_parquet(file_path, engine='fastparquet')
    extraction_params = {}
    extractor = FileExtractor("test-file-source", "Test File Source", {})

    data, metadata = extractor.extract_file(file_path, extraction_params)

    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["file_path"] == file_path


def test_file_extractor_format_detection():
    """Test that the file extractor correctly detects file formats"""
    csv_file = create_temp_file(content="col1,col2\n1,2", suffix=".csv")
    json_file = create_temp_file(content='{"col1": 1, "col2": 2}', suffix=".json")
    parquet_file = create_temp_file(suffix=".parquet")
    avro_file = create_temp_file(suffix=".avro")
    txt_file = create_temp_file(content="test data", suffix=".txt")

    assert detect_file_format(csv_file) == FileFormat.CSV
    assert detect_file_format(json_file) == FileFormat.JSON
    # assert detect_file_format(parquet_file) == FileFormat.PARQUET
    # assert detect_file_format(avro_file) == FileFormat.AVRO
    assert detect_file_format(txt_file) == FileFormat.TEXT

    # Test format detection from content when extension is ambiguous
    assert detect_file_format("data", content=b'{"col1": 1, "col2": 2}') == FileFormat.JSON


def test_file_extractor_schema_inference():
    """Test that the file extractor correctly infers schema from data"""
    df_int = pandas.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df_str = pandas.DataFrame({'col1': ['a', 'b'], 'col2': ['c', 'd']})
    df_mixed = pandas.DataFrame({'col1': [1, 'b'], 'col2': [3.0, 4]})

    schema_int = infer_schema(df_int, FileFormat.CSV)
    schema_str = infer_schema(df_str, FileFormat.CSV)
    schema_mixed = infer_schema(df_mixed, FileFormat.CSV)

    assert schema_int['fields'][0]['data_type'] == 'int64'
    assert schema_str['fields'][0]['data_type'] == 'object'
    assert schema_mixed['fields'][0]['data_type'] == 'object'


def test_file_extractor_transform_data():
    """Test that the file extractor correctly transforms extracted data"""
    data = {'col1': [1, 2], 'col2': [3, 4]}
    df = pandas.DataFrame(data)
    transformations = {
        'rename_columns': {'col1': 'new_col1'},
        'convert_types': {'new_col1': 'str'},
        'filters': [{'column': 'col2', 'operator': '>', 'value': 3}]
    }
    extractor = FileExtractor("test-file-source", "Test File Source", {})

    transformed_data = extractor.transform_data(df, transformations)

    assert 'new_col1' in transformed_data.columns
    assert 'col1' not in transformed_data.columns
    assert transformed_data['new_col1'].dtype == 'object'
    assert len(transformed_data) == 1


def test_incremental_extractor_initialization():
    """Test that the incremental extractor initializes correctly with valid configuration"""
    source_id = "test-incremental-source"
    source_name = "Test Incremental Source"
    extraction_config = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "numeric",
        "state_collection": "test_state",
        "lookback_window_hours": 12,
        "watermark_buffer_seconds": 300
    }

    extractor = IncrementalExtractor(source_id, source_name, extraction_config)

    assert extractor.source_id == source_id
    assert extractor.source_name == source_name
    assert extractor.extraction_config == extraction_config
    assert extractor.incremental_stats == {
        'attempts': 0,
        'successes': 0,
        'failures': 0,
        'total_records': 0,
        'total_processing_time': 0,
        'avg_processing_time': 0,
        'last_extraction_time': None,
        'extractions': []
    }
    assert extractor.state_collection == "test_state"
    assert extractor.lookback_window_hours == 12
    assert extractor.watermark_buffer_seconds == 300


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_extract_incremental(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor correctly extracts incremental data"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "numeric"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {
        "last_value": 100,
        "last_updated": "2023-01-01T00:00:00"
    }
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"id": [101, 102], "data": ["a", "b"]}), {"record_count": 2})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    data, metadata = extractor.extract_incremental(extraction_params, mock_api_connector)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_first_run(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor handles first run (no previous state) correctly"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "numeric"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.exists.return_value = False
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"id": [1, 2], "data": ["a", "b"]}), {"record_count": 2})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    data, metadata = extractor.extract_incremental(extraction_params, mock_api_connector)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_timestamp_based(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor correctly handles timestamp-based incremental extraction"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "timestamp",
        "column_type": "timestamp"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {
        "last_value": "2023-01-01T00:00:00",
        "last_updated": "2023-01-01T00:00:00"
    }
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"timestamp": ["2023-01-01T01:00:00", "2023-01-01T02:00:00"], "data": ["a", "b"]}), {"record_count": 2})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    data, metadata = extractor.extract_incremental(extraction_params, mock_api_connector)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_sequence_based(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor correctly handles sequence-based incremental extraction"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "sequence"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {
        "last_value": 100,
        "last_updated": "2023-01-01T00:00:00"
    }
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"id": [101, 102], "data": ["a", "b"]}), {"record_count": 2})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    data, metadata = extractor.extract_incremental(extraction_params, mock_api_connector)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 2
    assert metadata["record_count"] == 2


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_no_new_data(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor correctly handles case with no new data"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "numeric"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {
        "last_value": 100,
        "last_updated": "2023-01-01T00:00:00"
    }
    mock_api_connector.extract_data.return_value = (pandas.DataFrame(), {"record_count": 0})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    data, metadata = extractor.extract_incremental(extraction_params, mock_api_connector)

    mock_api_connector.extract_data.assert_called_once()
    assert isinstance(data, pandas.DataFrame)
    assert len(data) == 0
    assert metadata["record_count"] == 0


@mock.patch('src.backend.utils.storage.firestore_client.get_firestore_client')
def test_incremental_extractor_state_update_failure(mock_get_firestore_client, mock_api_connector):
    """Test that the incremental extractor handles state update failures"""
    extraction_params = {
        "table_name": "test_table",
        "incremental_column": "id",
        "column_type": "numeric"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {
        "last_value": 100,
        "last_updated": "2023-01-01T00:00:00"
    }
    mock_get_firestore_client.return_value.collection.return_value.document.return_value.set.side_effect = Exception("Firestore update failed")
    mock_api_connector.extract_data.return_value = (pandas.DataFrame({"id": [101, 102], "data": ["a", "b"]}), {"record_count": 2})
    extractor = IncrementalExtractor("test-incremental-source", "Test Incremental Source", {})

    with pytest.raises(Exception, match="Firestore update failed"):
        extractor.extract_incremental(extraction_params, mock_api_connector)