"""
Airflow DAG definition for data quality validation in the self-healing data pipeline.
This DAG orchestrates the validation of datasets against defined quality rules,
generates quality reports, and triggers self-healing processes for quality issues.
It supports both BigQuery and GCS data sources with configurable validation rules and thresholds.
"""

import datetime
import os
import json

# Third-party imports with version specification
import airflow  # apache-airflow:2.5.x
from airflow.models import DAG  # apache-airflow:2.5.x
from airflow.operators.dummy import DummyOperator  # apache-airflow:2.5.x
from airflow.operators.python import PythonOperator  # apache-airflow:2.5.x
from airflow.utils.dates import days_ago  # apache-airflow:2.5.x
from airflow.utils.trigger_rule import TriggerRule  # apache-airflow:2.5.x

# Internal module imports
from src.backend import constants  # Import enumerations for healing action types and alert severity levels
from src.backend.config import config  # Access application configuration settings
from src.backend.utils.logging import logger  # Configure logging for the DAG
from src.backend.airflow.plugins.custom_operators import quality_operators  # Use custom operators for data quality validation
from src.backend.airflow.plugins.custom_sensors import quality_sensors  # Use custom sensors for monitoring quality validation
from src.backend.db.repositories import quality_repository  # Access quality validation data from database

# Initialize logger
logger = logger.get_logger(__name__)

# Global variables from config
QUALITY_CONFIG = config.get('quality', {})
QUALITY_THRESHOLD = QUALITY_CONFIG.get('threshold', 0.8)
CONFIDENCE_THRESHOLD = QUALITY_CONFIG.get('confidence_threshold', constants.DEFAULT_CONFIDENCE_THRESHOLD)
VALIDATION_SCHEDULE = QUALITY_CONFIG.get('schedule', '0 */6 * * *')
RULES_PATH = QUALITY_CONFIG.get('rules_path', os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../configs/quality_rules.yaml'))
NOTIFICATION_CHANNELS = QUALITY_CONFIG.get('notification_channels', ['email', 'teams'])
SELF_HEALING_ENABLED = QUALITY_CONFIG.get('self_healing_enabled', True)

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Export lists for dynamically generated DAGs
bigquery_validation_dags = []
gcs_validation_dags = []

def create_bigquery_validation_dag(
    dag_id: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    schedule: str,
    dag_args: dict,
    validation_config: dict
) -> DAG:
    """Factory function to create a BigQuery data quality validation DAG

    Args:
        dag_id (str): ID of the DAG
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        schedule (str): Schedule interval for the DAG
        dag_args (dict): Default arguments for the DAG
        validation_config (dict): Configuration for the validation operator

    Returns:
        airflow.models.DAG: Configured Airflow DAG for BigQuery validation
    """
    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=dag_args,
        catchup=False,
        tags=['data_quality', 'bigquery']
    ) as dag:
        start = DummyOperator(task_id='start')

        validate_bq_table = quality_operators.DataQualityValidationOperator(
            task_id='validate_bigquery_table',
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            validation_config=validation_config,
            quality_threshold=QUALITY_THRESHOLD,
            rules_path=RULES_PATH,
            fail_on_error=False
        )

        generate_report = quality_operators.DataQualityReportingOperator(
            task_id='generate_quality_report',
            validation_task_id='validate_bigquery_table',
            report_format='json',
            send_notification=True,
            notification_channels=NOTIFICATION_CHANNELS
        )

        branch_task = quality_operators.QualityBasedBranchOperator(
            task_id='quality_branch',
            validation_task_id='validate_bigquery_table',
            quality_threshold=QUALITY_THRESHOLD,
            pass_task_id='validation_success',
            fail_task_id='notify_failure',
            healing_task_id='trigger_self_healing' if SELF_HEALING_ENABLED else None
        )

        trigger_self_healing = PythonOperator(
            task_id='trigger_self_healing',
            python_callable=trigger_self_healing,
            provide_context=True
        )

        validation_success = DummyOperator(task_id='validation_success')

        notify_failure = PythonOperator(
            task_id='notify_failure',
            python_callable=notify_quality_failure,
            provide_context=True,
            trigger_rule=TriggerRule.ONE_FAILED
        )

        end = DummyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

        start >> validate_bq_table >> generate_report >> branch_task
        branch_task >> validation_success >> end
        branch_task >> notify_failure >> end
        if SELF_HEALING_ENABLED:
            branch_task >> trigger_self_healing >> end

        return dag

def create_gcs_validation_dag(
    dag_id: str,
    bucket_name: str,
    file_path: str,
    file_format: str,
    schedule: str,
    dag_args: dict,
    validation_config: dict
) -> DAG:
    """Factory function to create a GCS data quality validation DAG

    Args:
        dag_id (str): ID of the DAG
        bucket_name (str): Google Cloud Storage bucket name
        file_path (str): Path to the file in GCS
        file_format (str): Format of the file (e.g., 'csv', 'json', 'parquet', 'avro')
        schedule (str): Schedule interval for the DAG
        dag_args (dict): Default arguments for the DAG
        validation_config (dict): Configuration for the validation operator

    Returns:
        airflow.models.DAG: Configured Airflow DAG for GCS validation
    """
    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=dag_args,
        catchup=False,
        tags=['data_quality', 'gcs']
    ) as dag:
        start = DummyOperator(task_id='start')

        validate_gcs_file = quality_operators.GCSDataQualityValidationOperator(
            task_id='validate_gcs_file',
            bucket_name=bucket_name,
            file_path=file_path,
            file_format=file_format,
            validation_config=validation_config,
            quality_threshold=QUALITY_THRESHOLD,
            rules_path=RULES_PATH,
            fail_on_error=False
        )

        generate_report = quality_operators.DataQualityReportingOperator(
            task_id='generate_quality_report',
            validation_task_id='validate_gcs_file',
            report_format='json',
            send_notification=True,
            notification_channels=NOTIFICATION_CHANNELS
        )

        branch_task = quality_operators.QualityBasedBranchOperator(
            task_id='quality_branch',
            validation_task_id='validate_gcs_file',
            quality_threshold=QUALITY_THRESHOLD,
            pass_task_id='validation_success',
            fail_task_id='notify_failure',
            healing_task_id='trigger_self_healing' if SELF_HEALING_ENABLED else None
        )

        trigger_self_healing = PythonOperator(
            task_id='trigger_self_healing',
            python_callable=trigger_self_healing,
            provide_context=True
        )

        validation_success = DummyOperator(task_id='validation_success')

        notify_failure = PythonOperator(
            task_id='notify_failure',
            python_callable=notify_quality_failure,
            provide_context=True,
            trigger_rule=TriggerRule.ONE_FAILED
        )

        end = DummyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

        start >> validate_gcs_file >> generate_report >> branch_task
        branch_task >> validation_success >> end
        branch_task >> notify_failure >> end
        if SELF_HEALING_ENABLED:
            branch_task >> trigger_self_healing >> end

        return dag

def trigger_self_healing(context: dict) -> dict:
    """Task function to trigger self-healing for quality issues

    Args:
        context (dict): Airflow context dictionary

    Returns:
        dict: Self-healing result
    """
    logger.info("Triggering self-healing process")
    ti = context['ti']
    validation_results = ti.xcom_pull(task_ids='validate_bigquery_table', key='validation_results')
    if not validation_results:
        logger.warning("No validation results found, cannot trigger self-healing")
        return {"status": "skipped", "message": "No validation results found"}

    # Access the QualityRepository to store validation results
    quality_repo = quality_repository.QualityRepository()
    try:
        # Store validation results in the repository
        quality_repo.store_validation_results(validation_results)

        # Trigger self-healing DAG for the validation ID
        # (Implementation depends on how self-healing DAGs are triggered)
        logger.info(f"Triggering self-healing DAG for validation ID: {validation_results['validation_id']}")
        # Placeholder for triggering self-healing DAG
        # trigger_dag(dag_id='self_healing_dag', conf={'validation_id': validation_results['validation_id']})
        return {"status": "triggered", "validation_id": validation_results['validation_id']}
    except Exception as e:
        logger.error(f"Error triggering self-healing: {e}")
        return {"status": "failed", "message": str(e)}

def notify_quality_failure(context: dict) -> dict:
    """Task function to notify about quality validation failures

    Args:
        context (dict): Airflow context dictionary

    Returns:
        dict: Notification result
    """
    logger.info("Notifying about quality validation failure")
    ti = context['ti']
    validation_results = ti.xcom_pull(task_ids='validate_bigquery_table', key='validation_results')
    if not validation_results:
        logger.warning("No validation results found, cannot send notification")
        return {"status": "skipped", "message": "No validation results found"}

    # Format notification message with validation details
    message = f"Data quality validation failed for table: {validation_results['table_id']}\n"
    message += f"Quality score: {validation_results['quality_score']}\n"
    message += f"Failing rules: {validation_results['failing_rules']}"

    # Send notifications to configured channels
    # (Implementation depends on notification system)
    logger.info(f"Sending notifications to channels: {NOTIFICATION_CHANNELS}")
    # Placeholder for sending notifications
    # send_notifications(message, channels=NOTIFICATION_CHANNELS)
    return {"status": "sent", "channels": NOTIFICATION_CHANNELS}

def get_validation_tables() -> list:
    """Function to get the list of tables to validate from configuration

    Returns:
        list: List of tables to validate
    """
    tables = QUALITY_CONFIG.get('tables')
    if isinstance(tables, list):
        table_configs = tables
    elif isinstance(tables, dict):
        table_configs = [tables]
    elif tables is None:
        table_configs = []
    else:
        logger.warning(f"Invalid tables configuration: {tables}, must be a list or dict")
        table_configs = []

    logger.info(f"Found {len(table_configs)} tables to validate")
    return table_configs

def get_validation_files() -> list:
    """Function to get the list of GCS files to validate from configuration

    Returns:
        list: List of GCS files to validate
    """
    files = QUALITY_CONFIG.get('files')
    if isinstance(files, list):
        file_configs = files
    elif isinstance(files, dict):
        file_configs = [files]
    elif files is None:
        file_configs = []
    else:
        logger.warning(f"Invalid files configuration: {files}, must be a list or dict")
        file_configs = []

    logger.info(f"Found {len(file_configs)} files to validate")
    return file_configs

# Dynamically generate BigQuery validation DAGs
for table_config in get_validation_tables():
    dag_id = f"bq_validation_{table_config['table_id']}"
    bigquery_validation_dags.append(
        create_bigquery_validation_dag(
            dag_id=dag_id,
            project_id=table_config['project_id'],
            dataset_id=table_config['dataset_id'],
            table_id=table_config['table_id'],
            schedule=VALIDATION_SCHEDULE,
            dag_args=default_args,
            validation_config=table_config.get('validation', {})
        )
    )

# Dynamically generate GCS validation DAGs
for file_config in get_validation_files():
    dag_id = f"gcs_validation_{file_config['file_path'].replace('/', '_')}"
    gcs_validation_dags.append(
        create_gcs_validation_dag(
            dag_id=dag_id,
            bucket_name=file_config['bucket_name'],
            file_path=file_config['file_path'],
            file_format=file_config['file_format'],
            schedule=VALIDATION_SCHEDULE,
            dag_args=default_args,
            validation_config=file_config.get('validation', {})
        )
    )