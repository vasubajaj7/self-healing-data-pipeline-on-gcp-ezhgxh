import os
import datetime

# Airflow imports
import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Internal imports
from src.backend import constants
from src.backend.constants import FileFormat, DEFAULT_CONFIDENCE_THRESHOLD
from src.backend.utils.logging.logger import get_logger
from src.backend.airflow.plugins.hooks.gcs_hooks import SelfHealingGCSHook
from src.backend.airflow.plugins.custom_operators.gcs_operators import GCSListOperator, SelfHealingGCSToDataFrameOperator, SelfHealingGCSToBigQueryOperator
from src.backend.airflow.plugins.custom_operators.quality_operators import GCSDataQualityValidationOperator, QualityBasedBranchOperator, DataQualityReportingOperator

# Initialize logger
logger = get_logger(__name__)

# Define default arguments for the DAG
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": airflow.utils.dates.days_ago(1),
}

# Define global variables from environment variables
BUCKET_NAME = os.environ.get('GCS_SOURCE_BUCKET', 'default-data-bucket')
SOURCE_PREFIX = os.environ.get('GCS_SOURCE_PREFIX', 'data/')
DESTINATION_PROJECT = os.environ.get('BQ_PROJECT_ID', 'default-project')
DESTINATION_DATASET = os.environ.get('BQ_DATASET_ID', 'ingested_data')
QUALITY_RULES_PATH = os.environ.get('QUALITY_RULES_PATH', '/home/airflow/gcs/data/quality_rules/gcs_rules.yaml')
QUALITY_THRESHOLD = float(os.environ.get('QUALITY_THRESHOLD', '0.8'))
CONFIDENCE_THRESHOLD = float(os.environ.get('CONFIDENCE_THRESHOLD', str(DEFAULT_CONFIDENCE_THRESHOLD)))
GCP_CONN_ID = os.environ.get('GCP_CONN_ID', 'google_cloud_default')

# Define functions for dynamic task parameters
def determine_file_format(file_path: str) -> FileFormat:
    """Determines the file format based on file extension"""
    extension = os.path.splitext(file_path)[1].lower()
    if extension == '.csv':
        return FileFormat.CSV
    elif extension == '.json':
        return FileFormat.JSON
    elif extension == '.avro':
        return FileFormat.AVRO
    elif extension == '.parquet':
        return FileFormat.PARQUET
    else:
        raise ValueError(f"Unsupported file format: {extension}")

def determine_table_name(file_path: str) -> str:
    """Determines the BigQuery table name based on file path"""
    file_name = os.path.basename(file_path)
    table_name = os.path.splitext(file_name)[0]
    table_name = table_name.replace('-', '_').replace(' ', '_')
    if not table_name[0].isalpha():
        table_name = 'table_' + table_name
    return table_name

def log_ingestion_success(context: dict) -> None:
    """Logs successful ingestion details"""
    task_instance = context['task_instance']
    file_path = task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0]
    row_count = task_instance.xcom_pull(task_ids='load_gcs_to_bq', key='output')
    logger.info(f"Successfully ingested {file_path} with {row_count} rows")
    return None

def log_ingestion_failure(context: dict) -> None:
    """Logs ingestion failure details"""
    task_instance = context['task_instance']
    file_path = task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0]
    error_message = context['exception']
    logger.error(f"Failed to ingest {file_path}: {error_message}")
    return None

def handle_quality_failure(context: dict) -> None:
    """Handles data quality validation failures"""
    task_instance = context['task_instance']
    validation_results = task_instance.xcom_pull(task_ids='validate_data_quality', key='validation_results')
    logger.error(f"Data quality validation failed: {validation_results}")
    # TODO: Create quality issue ticket if needed
    return None

# Define the DAG
with DAG(
    dag_id="gcs_ingestion_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gcs', 'bigquery', 'ingestion', 'self-healing']
) as dag:
    start = DummyOperator(task_id="start")

    list_gcs_files = GCSListOperator(
        task_id="list_gcs_files",
        bucket_name=BUCKET_NAME,
        prefix=SOURCE_PREFIX,
        gcp_conn_id=GCP_CONN_ID,
    )

    validate_data_quality = GCSDataQualityValidationOperator(
        task_id="validate_data_quality",
        bucket_name=BUCKET_NAME,
        file_path="{{ task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0] }}",
        file_format="{{ ti.xcom_pull(task_ids='determine_file_format') }}",
        rules_path=QUALITY_RULES_PATH,
        quality_threshold=QUALITY_THRESHOLD,
        gcp_conn_id=GCP_CONN_ID,
    )

    determine_file_format_task = PythonOperator(
        task_id='determine_file_format',
        python_callable=determine_file_format,
        op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0] }}"},
    )

    determine_table_name_task = PythonOperator(
        task_id='determine_table_name',
        python_callable=determine_table_name,
        op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0] }}"},
    )

    load_gcs_to_bq = SelfHealingGCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket_name=BUCKET_NAME,
        source_objects=["{{ task_instance.xcom_pull(task_ids='list_gcs_files', key='blob_names')[0] }}"],
        destination_project_dataset_table=f"{DESTINATION_PROJECT}.{DESTINATION_DATASET}.{{{{ ti.xcom_pull(task_ids='determine_table_name') }}}}",
        schema_fields=[
            {"name": "example_column", "type": "STRING", "mode": "NULLABLE", "description": "Example column"}
        ],
        source_format="{{ ti.xcom_pull(task_ids='determine_file_format') }}",
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_CONN_ID,
        confidence_threshold=CONFIDENCE_THRESHOLD,
    )

    quality_check_branch = QualityBasedBranchOperator(
        task_id="quality_check_branch",
        validation_task_id="validate_data_quality",
        quality_threshold=QUALITY_THRESHOLD,
        pass_task_id="load_gcs_to_bq",
        fail_task_id="handle_quality_failure",
        healing_task_id="load_gcs_to_bq",
    )

    handle_quality_failure_task = PythonOperator(
        task_id="handle_quality_failure",
        python_callable=handle_quality_failure,
        provide_context=True,
    )

    report_data_quality = DataQualityReportingOperator(
        task_id="report_data_quality",
        validation_task_id="validate_data_quality",
        report_format="json",
        output_path=None,
        send_notification=False,
    )

    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_ingestion_success,
        provide_context=True,
    )

    log_failure = PythonOperator(
        task_id="log_failure",
        python_callable=log_ingestion_failure,
        trigger_rule='one_failed',
        provide_context=True,
    )

    end = DummyOperator(task_id="end")

    # Define the task dependencies
    start >> list_gcs_files >> determine_file_format_task >> determine_table_name_task
    determine_file_format_task >> validate_data_quality
    validate_data_quality >> quality_check_branch
    quality_check_branch >> load_gcs_to_bq
    quality_check_branch >> handle_quality_failure_task
    load_gcs_to_bq >> log_success
    handle_quality_failure_task >> report_data_quality
    load_gcs_to_bq >> report_data_quality
    report_data_quality >> end
    log_success >> end
    list_gcs_files >> log_failure
    log_failure >> end