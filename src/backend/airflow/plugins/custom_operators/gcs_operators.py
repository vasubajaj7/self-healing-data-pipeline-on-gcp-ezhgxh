"""
Custom Airflow operators for Google Cloud Storage (GCS) operations in the self-healing data pipeline.
These operators extend standard Airflow GCS operators with enhanced error handling,
self-healing capabilities, and optimized data processing for GCS to BigQuery data flows.
"""

import typing
import os
import tempfile
import pandas  # version 2.0.x
from airflow.models import BaseOperator  # version 2.5.x
from airflow.utils.decorators import apply_defaults  # version 2.5.x
from airflow.exceptions import AirflowException  # version 2.5.x
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # version 8.10.0+
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSCreateBucketOperator, GCSDeleteBucketOperator, GCSDeleteObjectsOperator, GCSObjectOperations  # version 8.10.0+
from google.cloud.bigquery import SourceFormat  # version 3.10.0+
from google.cloud.exceptions import NotFound  # version 2.9.0+

from src.backend import constants  # Import enumerations for healing action types and alert severity levels
from src.backend.constants import FileFormat  # Import enumerations for healing action types and alert severity levels
from src.backend.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR  # Import constants for GCS operations and configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for GCS operators
from src.backend.airflow.plugins.hooks.gcs_hooks import EnhancedGCSHook, SelfHealingGCSHook, get_file_format_from_extension  # Use enhanced GCS hooks for storage operations
from src.backend.ingestion.connectors.gcs_connector import GCSConnector  # Utilize GCS connector for data extraction
from src.backend.ingestion.extractors.file_extractor import FileExtractor, detect_file_format  # Process files from GCS with format detection
from src.backend.utils.errors.error_types import PipelineError, DataFormatError, ErrorCategory  # Handle specific error types for GCS operations
from src.backend.self_healing.ai.issue_classifier import IssueClassifier  # Classify GCS issues for self-healing
from src.backend.self_healing.correction.data_corrector import DataCorrector  # Apply corrections to GCS data issues

# Initialize logger
logger = get_logger(__name__)

def validate_file_format(file_format: typing.Union[str, FileFormat]) -> FileFormat:
    """Validates and normalizes file format specification

    Args:
        file_format (Union[str, FileFormat]): file_format

    Returns:
        FileFormat: Normalized FileFormat enum value
    """
    if file_format is None:
        return None
    if isinstance(file_format, FileFormat):
        return file_format
    try:
        return FileFormat[file_format.upper()]
    except KeyError:
        raise ValueError(
            f"Unsupported file format: {file_format}. "
            f"Supported formats are: {', '.join([e.value for e in FileFormat])}"
        )

class GCSBaseOperator(BaseOperator):
    """Base operator for GCS operations with enhanced functionality"""

    template_fields = ['bucket_name']
    
    def __init__(self,
                 bucket_name: str,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the GCS base operator"""
        super().__init__(task_id=task_id, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.hook: EnhancedGCSHook = None
        self.timeout = timeout or DEFAULT_TIMEOUT_SECONDS
        logger.info(f"Initializing GCSBaseOperator with bucket: {bucket_name}, timeout: {self.timeout}")

    def execute(self, context: dict):
        """Execute the GCS operation"""
        self.hook = self.get_hook()
        logger.info(f"Executing GCS operation in bucket: {self.bucket_name}")
        try:
            result = self._execute_operation(context)
            return result
        except Exception as e:
            logger.error(f"GCS operation failed: {str(e)}", exc_info=True)
            raise

    def _execute_operation(self, context: dict):
        """Execute the specific GCS operation (to be implemented by subclasses)"""
        raise NotImplementedError

    def get_hook(self) -> EnhancedGCSHook:
        """Get or create an instance of the EnhancedGCSHook"""
        if self.hook:
            return self.hook
        self.hook = EnhancedGCSHook(gcp_conn_id=self.gcp_conn_id, timeout=self.timeout)
        return self.hook

    def on_kill(self):
        """Clean up when the task is killed"""
        if self.hook:
            self.hook.close()
        logger.info('Operator was killed')

class GCSListOperator(GCSBaseOperator):
    """Operator for listing files in a GCS bucket with pattern matching"""

    template_fields = ['bucket_name', 'prefix', 'delimiter']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 prefix: str = None,
                 delimiter: str = '/',
                 recursive: bool = False,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the GCS list operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, **kwargs)
        self.prefix = prefix
        self.delimiter = delimiter
        self.recursive = recursive
        if not self.bucket_name:
            raise ValueError('The bucket_name parameter must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the GCS list operation"""
        hook = self.get_hook()
        if self.recursive:
            self.delimiter = None
        files = hook.list_files(bucket_name=self.bucket_name, prefix=self.prefix, delimiter=self.delimiter)
        blob_names = [blob.name for blob in files]
        logger.info(f"Found {len(blob_names)} files in bucket: {self.bucket_name}, prefix: {self.prefix}")
        context['task_instance'].xcom_push(key='blob_names', value=blob_names)
        return blob_names

class GCSToDataFrameOperator(GCSBaseOperator):
    """Operator for loading data from GCS files into a pandas DataFrame"""

    template_fields = ['bucket_name', 'blob_name', 'blob_names']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 blob_name: str = None,
                 blob_names: list = None,
                 file_format: typing.Union[str, FileFormat] = None,
                 read_options: dict = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the GCS to DataFrame operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, **kwargs)
        self.blob_name = blob_name
        self.blob_names = blob_names
        if not self.blob_name and not self.blob_names:
            raise ValueError('Either blob_name or blob_names must be specified.')
        self.file_format = validate_file_format(file_format)
        self.read_options = read_options or {}
        if not self.bucket_name:
            raise ValueError('The bucket_name parameter must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the GCS to DataFrame operation"""
        hook = self.get_hook()
        if self.blob_name:
            df = hook.read_file_as_dataframe(bucket_name=self.bucket_name, object_name=self.blob_name,
                                             file_format=self.file_format, read_options=self.read_options)
        elif self.blob_names:
            df = self._read_multiple_files(blob_names=self.blob_names)
        else:
            blob_names = context['task_instance'].xcom_pull(task_ids=self.task_id, key='blob_names')
            df = self._read_multiple_files(blob_names=blob_names)
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame info: {df.info()}")
        context['task_instance'].xcom_push(key='dataframe', value=df)
        return df

    def _read_multiple_files(self, blob_names: list) -> pandas.DataFrame:
        """Read multiple GCS files into a single DataFrame"""
        dfs = []
        for blob_name in blob_names:
            df = self.hook.read_file_as_dataframe(bucket_name=self.bucket_name, object_name=blob_name,
                                                 file_format=self.file_format, read_options=self.read_options)
            dfs.append(df)
        return pandas.concat(dfs, ignore_index=True)

class GCSToLocalOperator(GCSBaseOperator):
    """Operator for downloading files from GCS to local filesystem"""

    template_fields = ['bucket_name', 'blob_name', 'local_path']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 blob_name: str,
                 local_path: str,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the GCS to local operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, **kwargs)
        self.blob_name = blob_name
        self.local_path = local_path
        if not self.bucket_name or not self.blob_name or not self.local_path:
            raise ValueError('bucket_name, blob_name, and local_path must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the GCS to local download operation"""
        hook = self.get_hook()
        local_directory = os.path.dirname(self.local_path)
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)
        hook.download_file(bucket_name=self.bucket_name, object_name=self.blob_name, filename=self.local_path)
        logger.info(f"Successfully downloaded file from gs://{self.bucket_name}/{self.blob_name} to {self.local_path}")
        context['task_instance'].xcom_push(key='local_path', value=self.local_path)
        return self.local_path

class LocalToGCSOperator(GCSBaseOperator):
    """Operator for uploading local files to GCS"""

    template_fields = ['bucket_name', 'local_path', 'blob_name']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 local_path: str,
                 blob_name: str,
                 content_type: str = None,
                 metadata: dict = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the local to GCS operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, **kwargs)
        self.local_path = local_path
        self.blob_name = blob_name
        self.content_type = content_type
        self.metadata = metadata
        if not self.bucket_name or not self.local_path or not self.blob_name:
            raise ValueError('bucket_name, local_path, and blob_name must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the local to GCS upload operation"""
        hook = self.get_hook()
        if not os.path.exists(self.local_path):
            raise FileNotFoundError(f"Local file not found: {self.local_path}")
        hook.upload_file(bucket_name=self.bucket_name, object_name=self.blob_name, filename=self.local_path,
                         mime_type=self.content_type, metadata=self.metadata)
        logger.info(f"Successfully uploaded file from {self.local_path} to gs://{self.bucket_name}/{self.blob_name}")
        context['task_instance'].xcom_push(key='blob_name', value=self.blob_name)
        return self.blob_name

class EnhancedGCSToBigQueryOperator(GCSToBigQueryOperator):
    """Enhanced operator for loading data from GCS to BigQuery with improved error handling and monitoring"""

    template_fields = ['bucket_name', 'source_objects', 'destination_project_dataset_table']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 source_objects: typing.Union[str, list],
                 destination_project_dataset_table: str,
                 schema_fields: list,
                 source_format: typing.Union[str, FileFormat],
                 load_options: dict = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the enhanced GCS to BigQuery operator"""
        super().__init__(
            task_id=task_id,
            bucket=bucket_name,
            source_objects=source_objects,
            destination_project_dataset_table=destination_project_dataset_table,
            schema_fields=schema_fields,
            source_format=source_format,
            gcp_conn_id=gcp_conn_id,
            **kwargs,
        )
        self.bucket_name = bucket_name
        if isinstance(source_objects, str):
            self.source_objects = [source_objects]
        else:
            self.source_objects = source_objects
        self.source_format = validate_file_format(source_format)
        if self.source_format:
            self.source_format = self.source_format.value
        self.load_options = load_options or {}
        self.gcs_hook: EnhancedGCSHook = None
        self.timeout = timeout or DEFAULT_TIMEOUT_SECONDS
        if not self.bucket_name or not self.source_objects or not self.destination_project_dataset_table or not self.schema_fields:
            raise ValueError('bucket_name, source_objects, destination_project_dataset_table, and schema_fields must be specified.')

    def execute(self, context: dict):
        """Execute the GCS to BigQuery load operation with enhanced monitoring"""
        self.gcs_hook = self.get_gcs_hook()
        logger.info(f"Starting GCS to BigQuery load from gs://{self.bucket_name}/{self.source_objects} to {self.destination_project_dataset_table}")
        self._verify_source_objects()
        results = super().execute(context)
        logger.info(f"Successfully loaded data to BigQuery: {results}")
        return results

    def _verify_source_objects(self):
        """Verify that source objects exist in GCS"""
        hook = self.get_gcs_hook()
        for source_object in self.source_objects:
            try:
                hook.file_exists(bucket_name=self.bucket, object_name=source_object)
            except NotFound:
                raise ValueError(f"Source object not found: gs://{self.bucket}/{source_object}")

    def get_gcs_hook(self) -> EnhancedGCSHook:
        """Get or create an instance of the EnhancedGCSHook"""
        if self.gcs_hook:
            return self.gcs_hook
        self.gcs_hook = EnhancedGCSHook(gcp_conn_id=self.gcp_conn_id, timeout=self.timeout)
        return self.gcs_hook

class DataFrameToGCSOperator(GCSBaseOperator):
    """Operator for saving a pandas DataFrame to a GCS file"""

    template_fields = ['bucket_name', 'blob_name']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 blob_name: str,
                 file_format: typing.Union[str, FileFormat],
                 write_options: dict = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the DataFrame to GCS operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, **kwargs)
        self.blob_name = blob_name
        self.file_format = validate_file_format(file_format)
        self.write_options = write_options or {}
        if not self.bucket_name or not self.blob_name:
            raise ValueError('bucket_name and blob_name must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the DataFrame to GCS operation"""
        hook = self.get_hook()
        df = context['task_instance'].xcom_pull(task_ids=self.task_id, key='dataframe')
        hook.write_dataframe_to_file(df=df, bucket_name=self.bucket_name, object_name=self.blob_name,
                                     file_format=self.file_format, write_options=self.write_options)
        logger.info(f"Successfully saved DataFrame to gs://{self.bucket_name}/{self.blob_name}")
        context['task_instance'].xcom_push(key='blob_name', value=self.blob_name)
        return self.blob_name

class SelfHealingGCSBaseOperator(BaseOperator):
    """Base operator for GCS operations with self-healing capabilities"""

    template_fields = ['bucket_name']

    def __init__(self,
                 bucket_name: str,
                 gcp_conn_id: str = 'google_cloud_default',
                 confidence_threshold: float = 0.85,
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the self-healing GCS base operator"""
        super().__init__(task_id=task_id, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.confidence_threshold = confidence_threshold
        self.hook: SelfHealingGCSHook = None
        self.timeout = timeout or DEFAULT_TIMEOUT_SECONDS
        logger.info(f"Initializing SelfHealingGCSBaseOperator with bucket: {bucket_name}, confidence: {confidence_threshold}, timeout: {self.timeout}")

    def execute(self, context: dict):
        """Execute the GCS operation with self-healing capabilities"""
        self.hook = self.get_hook()
        try:
            result = self._execute_operation(context)
            return result
        except Exception as e:
            logger.error(f"GCS operation failed: {str(e)}, attempting self-healing", exc_info=True)
            can_fix, fix_params = self._diagnose_gcs_error(e, operation_type=self.__class__.__name__, operation_params=context)
            if can_fix:
                logger.info(f"Applying self-healing fix: {fix_params}")
                self._log_healing_action(context, fix_params, str(e), self.confidence_threshold)
                # Apply appropriate self-healing strategy based on error type
                # Retry the operation with fixed parameters
                # TODO: Implement retry logic with fixed parameters
                pass
            else:
                logger.warning(f"Self-healing not possible, escalating issue: {str(e)}")
                # TODO: Implement escalation logic
                pass
            raise

    def _execute_operation(self, context: dict):
        """Execute the specific GCS operation (to be implemented by subclasses)"""
        raise NotImplementedError

    def get_hook(self) -> SelfHealingGCSHook:
        """Get or create an instance of the SelfHealingGCSHook"""
        if self.hook:
            return self.hook
        self.hook = SelfHealingGCSHook(gcp_conn_id=self.gcp_conn_id, timeout=self.timeout, confidence_threshold=self.confidence_threshold)
        return self.hook

    def _diagnose_gcs_error(self, error: Exception, operation_type: str, operation_params: dict) -> typing.Tuple[bool, dict]:
        """Diagnose a GCS error and suggest fixes"""
        # Use hook's issue classifier to analyze the error
        # Extract error type, potential fixes, and confidence score
        # If confidence exceeds threshold, return True with fix parameters
        # Otherwise, return False with diagnostic information
        return False, {}

    def _log_healing_action(self, original_params: dict, fixed_params: dict, error_message: str, confidence: float):
        """Log details about the self-healing action taken"""
        # Format log message with error details
        # Log the original parameters
        # Log the fixed parameters
        # Log the confidence score
        # Update task metadata with healing information
        pass

class SelfHealingGCSToDataFrameOperator(SelfHealingGCSBaseOperator):
    """Self-healing operator for loading data from GCS files into a pandas DataFrame with automatic error recovery"""

    template_fields = ['bucket_name', 'blob_name', 'blob_names']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 blob_name: str = None,
                 blob_names: list = None,
                 file_format: typing.Union[str, FileFormat] = None,
                 read_options: dict = None,
                 confidence_threshold: float = 0.85,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the self-healing GCS to DataFrame operator"""
        super().__init__(bucket_name=bucket_name, gcp_conn_id=gcp_conn_id, timeout=timeout, task_id=task_id, confidence_threshold=confidence_threshold, **kwargs)
        self.blob_name = blob_name
        self.blob_names = blob_names
        if not self.blob_name and not self.blob_names:
            raise ValueError('Either blob_name or blob_names must be specified.')
        self.file_format = validate_file_format(file_format)
        self.read_options = read_options or {}
        if not self.bucket_name:
            raise ValueError('The bucket_name parameter must be specified.')

    def _execute_operation(self, context: dict):
        """Execute the self-healing GCS to DataFrame operation"""
        hook = self.get_hook()
        if self.blob_name:
            df = hook.read_file_as_dataframe(bucket_name=self.bucket_name, object_name=self.blob_name,
                                             file_format=self.file_format, read_options=self.read_options,
                                             attempt_healing=True)
        elif self.blob_names:
            df = self._read_multiple_files_with_healing(blob_names=self.blob_names)
        else:
            blob_names = context['task_instance'].xcom_pull(task_ids=self.task_id, key='blob_names')
            df = self._read_multiple_files_with_healing(blob_names=blob_names)
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame info: {df.info()}")
        context['task_instance'].xcom_push(key='dataframe', value=df)
        return df

    def _read_multiple_files_with_healing(self, blob_names: list) -> pandas.DataFrame:
        """Read multiple GCS files into a single DataFrame with self-healing"""
        dfs = []
        failed_blobs = []
        for blob_name in blob_names:
            try:
                df = self.hook.read_file_as_dataframe(bucket_name=self.bucket_name, object_name=blob_name,
                                                     file_format=self.file_format, read_options=self.read_options,
                                                     attempt_healing=True)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read and heal blob {blob_name}: {str(e)}")
                failed_blobs.append(blob_name)
        if not dfs:
            raise AirflowException(f"Failed to read any blobs from {blob_names}")
        if failed_blobs:
            logger.warning(f"Failed to read blobs: {failed_blobs}")
        return pandas.concat(dfs, ignore_index=True)

class SelfHealingGCSToBigQueryOperator(EnhancedGCSToBigQueryOperator):
    """Self-healing operator for loading data from GCS to BigQuery with automatic error recovery"""

    template_fields = ['bucket_name', 'source_objects', 'destination_project_dataset_table']

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 source_objects: typing.Union[str, list],
                 destination_project_dataset_table: str,
                 schema_fields: list,
                 source_format: typing.Union[str, FileFormat],
                 load_options: dict = None,
                 confidence_threshold: float = 0.85,
                 gcp_conn_id: str = 'google_cloud_default',
                 timeout: int = None,
                 task_id: str = None,
                 **kwargs) -> None:
        """Initialize the self-healing GCS to BigQuery operator"""
        super().__init__(
            task_id=task_id,
            bucket_name=bucket_name,
            source_objects=source_objects,
            destination_project_dataset_table=destination_project_dataset_table,
            schema_fields=schema_fields,
            source_format=source_format,
            load_options=load_options,
            gcp_conn_id=gcp_conn_id,
            **kwargs,
        )
        self.confidence_threshold = confidence_threshold
        self.gcs_hook: SelfHealingGCSHook = None

    def execute(self, context: dict):
        """Execute the GCS to BigQuery load operation with self-healing capabilities"""
        self.gcs_hook = self.get_gcs_hook()
        logger.info(f"Starting GCS to BigQuery load from gs://{self.bucket_name}/{self.source_objects} to {self.destination_project_dataset_table}")
        try:
            self._verify_source_objects()
        except Exception as e:
            logger.warning(f"Source object verification failed: {str(e)}, attempting self-healing")
            can_fix, fix_params = self._diagnose_gcs_error(e)
            if can_fix:
                self._apply_load_fix(fix_params)
            else:
                raise
        try:
            results = super().execute(context)
            logger.info(f"Successfully loaded data to BigQuery: {results}")
            return results
        except Exception as e:
            logger.error(f"BigQuery load failed: {str(e)}, attempting self-healing", exc_info=True)
            can_fix, fix_params = self._diagnose_load_error(e)
            if can_fix:
                self._apply_load_fix(fix_params)
                results = super().execute(context)
                logger.info(f"Successfully loaded data to BigQuery after self-healing: {results}")
                return results
            else:
                raise

    def get_gcs_hook(self) -> SelfHealingGCSHook:
        """Get or create an instance of the SelfHealingGCSHook"""
        if self.gcs_hook:
            return self.gcs_hook
        self.gcs_hook = SelfHealingGCSHook(gcp_conn_id=self.gcp_conn_id, timeout=self.timeout, confidence_threshold=self.confidence_threshold)
        return self.gcs_hook

    def _diagnose_load_error(self, error: Exception) -> typing.Tuple[bool, dict]:
        """Diagnose a BigQuery load error and suggest fixes"""
        # Analyze the error message and type
        # For schema mismatch errors, suggest schema adjustments
        # For format errors, suggest format corrections
        # For permission errors, suggest permission fixes
        # Return whether the error can be fixed and fix parameters
        return False, {}

    def _apply_load_fix(self, fix_params: dict):
        """Apply a fix to a failed BigQuery load operation"""
        # Extract fix type from fix_params
        # Apply appropriate fix based on fix type:
        # For schema issues, adjust schema_fields
        # For format issues, adjust source_format or load_options
        # For permission issues, attempt to grant necessary permissions
        # Log the applied fix details
        pass