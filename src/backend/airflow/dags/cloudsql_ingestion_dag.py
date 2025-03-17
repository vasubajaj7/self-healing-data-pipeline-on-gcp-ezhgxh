# src/backend/airflow/dags/cloudsql_ingestion_dag.py
from datetime import datetime, timedelta
import os

# Airflow imports
import airflow
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Internal imports
from src.backend import constants  # Import constants for data source types and configuration
from src.backend.config import get_config  # Access application configuration settings
from src.backend.utils.logging.logger import get_logger  # Configure logging for the DAG
from src.backend.airflow.plugins.custom_operators.cloudsql_operators import SelfHealingCloudSQLTableExtractOperator, SelfHealingCloudSQLIncrementalExtractOperator, SelfHealingCloudSQLSchemaExtractOperator  # Use custom Cloud SQL operators for data extraction
from src.backend.airflow.plugins.custom_operators.gcs_operators import GCSUploadOperator  # Use custom GCS operators for storing extracted data
from src.backend.airflow.plugins.custom_operators.quality_operators import DataQualityValidationOperator  # Use custom quality operators for data validation
from src.backend.airflow.plugins.custom_sensors.cloudsql_sensors import CloudSQLTableSensor  # Use custom sensors to check Cloud SQL table availability

# Initialize logger
logger = get_logger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Get application configuration
config = get_config()


def get_cloudsql_config() -> dict:
    """Get Cloud SQL ingestion configuration from application config

    Returns:
        dict: Cloud SQL ingestion configuration
    """
    # Get ingestion configuration from config
    ingestion_config = config.get('ingestion', {})

    # Extract Cloud SQL specific configuration
    cloudsql_config = ingestion_config.get('cloudsql', {})

    # Set default values for missing configuration
    cloudsql_config.setdefault('extraction_path', '/tmp/extracted_data')
    cloudsql_config.setdefault('schema_path', '/tmp/schemas')
    cloudsql_config.setdefault('state_path', '/tmp/state')

    # Validate required configuration parameters
    if not cloudsql_config.get('sources'):
        raise ValueError("Cloud SQL configuration missing 'sources' list")

    # Return the Cloud SQL configuration dictionary
    return cloudsql_config


def get_source_config(source_id: str) -> dict:
    """Get configuration for a specific Cloud SQL source

    Args:
        source_id (str): source_id

    Returns:
        dict: Source-specific configuration
    """
    # Get Cloud SQL configuration
    cloudsql_config = get_cloudsql_config()

    # Find the specified source configuration by ID
    source_config = next((source for source in cloudsql_config['sources'] if source['source_id'] == source_id), None)
    if not source_config:
        raise ValueError(f"Source configuration not found for source_id: {source_id}")

    # Set default values for missing source configuration
    source_config.setdefault('file_format', 'csv')
    source_config.setdefault('extraction_method', 'full')

    # Return the source configuration dictionary
    return source_config


def get_table_config(source_id: str, table_name: str) -> dict:
    """Get configuration for a specific table in a Cloud SQL source

    Args:
        source_id (str): source_id
        table_name (str): table_name

    Returns:
        dict: Table-specific configuration
    """
    # Get source configuration
    source_config = get_source_config(source_id)

    # Find the specified table configuration by name
    table_config = next((table for table in source_config['tables'] if table['table_name'] == table_name), None)
    if not table_config:
        raise ValueError(f"Table configuration not found for table_name: {table_name} in source_id: {source_id}")

    # Set default values for missing table configuration
    table_config.setdefault('validation_rules', [])

    # Return the table configuration dictionary
    return table_config


def create_cloudsql_ingestion_dag(source_id: str, schedule: str, dag_args: dict) -> airflow.models.DAG:
    """Factory function to create a Cloud SQL ingestion DAG for a specific source

    Args:
        source_id (str): source_id
        schedule (str): schedule
        dag_args (dict): dag_args

    Returns:
        airflow.models.DAG: Configured Airflow DAG for Cloud SQL ingestion
    """
    # Get source configuration
    source_config = get_source_config(source_id)

    # Create DAG with provided schedule and arguments
    dag_id = f"cloudsql_ingestion_{source_id}"
    dag = DAG(dag_id=dag_id, schedule_interval=schedule, default_args=dag_args, catchup=False)

    with dag:
        # Create start and end dummy tasks
        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        # For each table in the source configuration:
        for table_config in source_config['tables']:
            table_name = table_config['table_name']

            # Create sensor task to check table availability
            table_sensor = CloudSQLTableSensor(
                task_id=f"check_{table_name}_availability",
                conn_id=source_config['conn_id'],
                database=source_config['database'],
                instance_connection_name=source_config['instance_connection_name'],
                db_type=source_config['db_type'],
                table_name=table_name,
            )

            # Create schema extraction task
            schema_path = get_schema_path(source_id, table_name)
            schema_extraction = SelfHealingCloudSQLSchemaExtractOperator(
                task_id=f"extract_{table_name}_schema",
                conn_id=source_config['conn_id'],
                database=source_config['database'],
                instance_connection_name=source_config['instance_connection_name'],
                db_type=source_config['db_type'],
                table_name=table_name,
                output_path=schema_path,
            )

            # Create data extraction task (incremental or full)
            extraction_path = get_extraction_path(source_id, table_name, "{{ ds }}")
            if source_config['extraction_method'] == 'incremental':
                state_path = get_state_path(source_id, table_name)
                data_extraction = SelfHealingCloudSQLIncrementalExtractOperator(
                    task_id=f"extract_{table_name}_data",
                    conn_id=source_config['conn_id'],
                    database=source_config['database'],
                    instance_connection_name=source_config['instance_connection_name'],
                    db_type=source_config['db_type'],
                    table_name=table_name,
                    output_path=extraction_path,
                    incremental_column=table_config['incremental_column'],
                    state_path=state_path,
                )
            else:
                data_extraction = SelfHealingCloudSQLTableExtractOperator(
                    task_id=f"extract_{table_name}_data",
                    conn_id=source_config['conn_id'],
                    database=source_config['database'],
                    instance_connection_name=source_config['instance_connection_name'],
                    db_type=source_config['db_type'],
                    table_name=table_name,
                    output_path=extraction_path,
                )

            # Create data quality validation task
            validation_rules = table_config['validation_rules']
            data_validation = DataQualityValidationOperator(
                task_id=f"validate_{table_name}_data",
                project_id=config.get('gcp.project_id'),
                dataset_id=config.get('bigquery.dataset'),
                table_id=table_name,
                validation_rules=validation_rules,
            )

            # Create GCS upload task for extracted data
            gcs_upload = GCSUploadOperator(
                task_id=f"upload_{table_name}_to_gcs",
                bucket_name=config.get('gcs.bucket'),
                blob_name=extraction_path,
                local_path=extraction_path,
            )

            # Set up task dependencies to create the workflow
            start >> table_sensor >> schema_extraction >> data_extraction >> data_validation >> gcs_upload >> end

    # Return the configured DAG
    return dag


def get_extraction_path(source_id: str, table_name: str, execution_date: str) -> str:
    """Generate the file path for extracted data

    Args:
        source_id (str): source_id
        table_name (str): table_name
        execution_date (str): execution_date

    Returns:
        str: File path for extracted data
    """
    # Get base extraction path from configuration
    cloudsql_config = get_cloudsql_config()
    base_path = cloudsql_config['extraction_path']

    # Format path with source_id, table_name, and execution_date
    formatted_path = f"{base_path}/{source_id}/{table_name}/{execution_date}/{table_name}.csv"

    # Return the formatted path
    return formatted_path


def get_schema_path(source_id: str, table_name: str) -> str:
    """Generate the file path for schema information

    Args:
        source_id (str): source_id
        table_name (str): table_name

    Returns:
        str: File path for schema information
    """
    # Get base schema path from configuration
    cloudsql_config = get_cloudsql_config()
    base_path = cloudsql_config['schema_path']

    # Format path with source_id and table_name
    formatted_path = f"{base_path}/{source_id}/{table_name}/{table_name}_schema.json"

    # Return the formatted path
    return formatted_path


def get_state_path(source_id: str, table_name: str) -> str:
    """Generate the file path for incremental extraction state

    Args:
        source_id (str): source_id
        table_name (str): table_name

    Returns:
        str: File path for state information
    """
    # Get base state path from configuration
    cloudsql_config = get_cloudsql_config()
    base_path = cloudsql_config['state_path']

    # Format path with source_id and table_name
    formatted_path = f"{base_path}/{source_id}/{table_name}/{table_name}_state.txt"

    # Return the formatted path
    return formatted_path


# Create DAGs for each source defined in the configuration
for source in config.get('ingestion.cloudsql.sources', []):
    dag_id = source['source_id']
    schedule = source.get('schedule', '@daily')
    dag_args = default_args.copy()

    try:
        dag = create_cloudsql_ingestion_dag(source_id=dag_id, schedule=schedule, dag_args=dag_args)
    except Exception as e:
        logger.error(f"Failed to create DAG for source {dag_id}: {e}")
        continue

    globals()[dag_id] = dag

dag = DAG(
    dag_id='example_dag',
    default_args={"owner": "Airflow"},
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example']
)

with dag:
    start = DummyOperator(
        task_id='start'
    )