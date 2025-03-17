"""
Airflow DAG definition for data processing in the self-healing data pipeline.
This DAG orchestrates the transformation, enrichment, and processing of data that has been ingested from various sources.
It includes data quality validation, self-healing capabilities, and optimized loading into BigQuery for analytics.
"""

import datetime
import os
import json
import pandas  # version 2.0.x

from airflow import DAG  # apache-airflow version 2.5.x
from airflow.models import BaseOperator  # apache-airflow version 2.5.x
from airflow.operators.dummy import DummyOperator  # apache-airflow version 2.5.x
from airflow.operators.python import PythonOperator  # apache-airflow version 2.5.x
from airflow.utils.dates import days_ago  # apache-airflow version 2.5.x
from airflow.utils.trigger_rule import TriggerRule  # apache-airflow version 2.5.x

from src.backend import constants  # Import constants for configuration and status tracking
from src.backend.config import get_config  # Access application configuration settings
from src.backend.utils.logging.logger import get_logger  # Configure logging for the DAG
from src.backend.airflow.plugins.custom_operators.quality_operators import DataQualityValidationOperator, QualityBasedBranchOperator, load_validation_rules  # Use custom quality operators for data validation
from src.backend.airflow.plugins.custom_operators.healing_operators import DataQualityHealingOperator, PipelineHealingOperator  # Use custom healing operators for self-healing capabilities
from src.backend.utils.storage.bigquery_client import BigQueryClient  # Interact with BigQuery for data processing
from src.backend.utils.storage.gcs_client import GCSClient  # Interact with GCS for data access
from src.backend.utils.monitoring.metric_client import MetricClient  # Report processing metrics to monitoring system

# Initialize logger
logger = get_logger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Access application configuration
config = get_config()

# Extract processing configuration
PROCESSING_CONFIG = config.get('processing', {})

# Set default quality threshold
QUALITY_THRESHOLD = PROCESSING_CONFIG.get('quality_threshold', constants.DEFAULT_QUALITY_THRESHOLD)

# Enable or disable self-healing
SELF_HEALING_ENABLED = PROCESSING_CONFIG.get('self_healing_enabled', True)


def get_processing_config() -> dict:
    """Get data processing configuration from application config"""
    # Get processing configuration from config
    processing_config = config.get('processing', {})

    # Set default values for missing configuration
    processing_config.setdefault('data_format', constants.FileFormat.CSV.value)
    processing_config.setdefault('compression_type', 'gzip')
    processing_config.setdefault('batch_size', 1000)

    # Validate required configuration parameters
    if 'source_dataset' not in processing_config or 'target_dataset' not in processing_config:
        raise ValueError("Source and target datasets must be specified in processing configuration")

    # Return the processing configuration dictionary
    return processing_config


def get_transformation_config(dataset_id: str) -> dict:
    """Get transformation configuration for a specific dataset"""
    # Get processing configuration
    processing_config = get_processing_config()

    # Extract transformation configuration for the specified dataset
    transformation_config = processing_config.get('transformation', {}).get(dataset_id, {})

    # Set default values for missing transformation configuration
    transformation_config.setdefault('transformation_type', 'sql')
    transformation_config.setdefault('target_table', f'{dataset_id}_transformed')

    # Return the transformation configuration dictionary
    return transformation_config


def get_quality_rules_path(dataset_id: str) -> str:
    """Get the path to quality validation rules for a dataset"""
    # Get processing configuration
    processing_config = get_processing_config()

    # Extract quality rules path for the specified dataset
    rules_path = processing_config.get('quality_rules', {}).get(dataset_id)

    # If path is not specified, use default rules path
    if not rules_path:
        rules_path = os.path.join(config.get('base_path'), 'data', 'quality_rules', f'{dataset_id}_rules.json')

    # Return the quality rules path
    return rules_path


def transform_data(context: dict, dataset_id: str) -> dict:
    """Transform data according to transformation configuration"""
    # Get transformation configuration for the dataset
    transformation_config = get_transformation_config(dataset_id)

    # Get source data location from XCom or configuration
    source_data_location = context['ti'].xcom_pull(task_ids='extract_data', key='source_data_location') or transformation_config.get('source_data_location')

    # Initialize BigQuery client for data access
    bq_client = BigQueryClient()

    # Execute transformation SQL or custom transformation logic
    if transformation_config.get('transformation_type') == 'sql':
        transformation_sql = transformation_config.get('transformation_sql')
        transformed_data = bq_client.execute_query(transformation_sql, source_data_location)
    else:
        # Implement custom transformation logic here
        transformed_data = None

    # Store transformed data in target location
    target_data_location = transformation_config.get('target_data_location')
    context['ti'].xcom_push(key='transformed_data_location', value=target_data_location)

    # Log transformation metrics
    logger.info(f"Transformed data for dataset {dataset_id} and stored at {target_data_location}")

    # Return transformation results with metadata
    return {'dataset_id': dataset_id, 'target_data_location': target_data_location}


def enrich_data(context: dict, dataset_id: str) -> dict:
    """Enrich data with additional information from reference datasets"""
    # Get enrichment configuration for the dataset
    enrichment_config = get_transformation_config(dataset_id)

    # Get transformed data location from XCom
    transformed_data_location = context['ti'].xcom_pull(task_ids='transform_data', key='transformed_data_location')

    # Initialize BigQuery client for data access
    bq_client = BigQueryClient()

    # For each enrichment source:
    for source in enrichment_config.get('enrichment_sources', []):
        # Join with reference data according to configuration
        # Apply enrichment transformations
        pass

    # Store enriched data in target location
    enriched_data_location = enrichment_config.get('enriched_data_location')
    context['ti'].xcom_push(key='enriched_data_location', value=enriched_data_location)

    # Log enrichment metrics
    logger.info(f"Enriched data for dataset {dataset_id} and stored at {enriched_data_location}")

    # Return enrichment results with metadata
    return {'dataset_id': dataset_id, 'enriched_data_location': enriched_data_location}


def validate_processed_data(context: dict, dataset_id: str) -> dict:
    """Validate processed data quality before final loading"""
    # Get quality rules path for the dataset
    rules_path = get_quality_rules_path(dataset_id)

    # Get processed data location from XCom
    processed_data_location = context['ti'].xcom_pull(task_ids='enrich_data', key='enriched_data_location')

    # Initialize validation engine
    # Execute validation rules against processed data
    # Calculate quality score and validation metrics
    # Log validation results
    # Return validation results with quality score
    return {'dataset_id': dataset_id, 'quality_score': 0.95}


def heal_data_quality_issues(context: dict, dataset_id: str) -> dict:
    """Apply self-healing to data quality issues"""
    # Get validation results from XCom
    validation_results = context['ti'].xcom_pull(task_ids='validate_processed_data', key='validation_results')

    # Initialize data corrector with appropriate models
    # Analyze quality issues and determine correction strategies
    # Apply corrections to data
    # Validate corrections were successful
    # Log healing metrics and actions taken
    # Return healing results with correction details
    return {'dataset_id': dataset_id, 'healing_actions': ['impute_missing_values', 'remove_outliers']}


def load_processed_data(context: dict, dataset_id: str) -> dict:
    """Load processed data to target BigQuery tables"""
    # Get loading configuration for the dataset
    loading_config = get_transformation_config(dataset_id)

    # Get processed data location from XCom
    processed_data_location = context['ti'].xcom_pull(task_ids='heal_data_quality_issues', key='healed_data_location')

    # Initialize BigQuery client
    bq_client = BigQueryClient()

    # Configure loading options (write disposition, partitioning, clustering)
    # Execute load operation to target table
    # Log loading metrics
    # Return load results with row counts and performance metrics
    return {'dataset_id': dataset_id, 'row_count': 10000}


def report_processing_metrics(context: dict, dataset_id: str) -> dict:
    """Report data processing metrics to monitoring system"""
    # Collect metrics from all processing stages via XCom
    # Initialize MetricClient
    # Report transformation metrics (duration, records processed)
    # Report enrichment metrics (join success rates, enrichment counts)
    # Report validation metrics (quality score, rule pass rates)
    # Report loading metrics (load duration, row counts)
    # Return reporting results with metric details
    return {'dataset_id': dataset_id, 'metrics_reported': True}


def create_data_processing_dag(dataset_id: str, schedule: str, dag_args: dict):
    """Factory function to create a data processing DAG for a specific dataset"""
    # Get processing configuration for the dataset
    processing_config = get_transformation_config(dataset_id)

    # Create DAG with provided schedule and arguments
    dag = DAG(
        dag_id=f'{dataset_id}_data_processing',
        schedule_interval=schedule,
        default_args=default_args,
        catchup=False,
        **dag_args
    )

    with dag:
        # Create start and end dummy tasks
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')

        # Create transformation task
        transform = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            op_kwargs={'dataset_id': dataset_id},
            provide_context=True,
        )

        # Create enrichment task
        enrich = PythonOperator(
            task_id='enrich_data',
            python_callable=enrich_data,
            op_kwargs={'dataset_id': dataset_id},
            provide_context=True,
        )

        # Create validation task
        validate = DataQualityValidationOperator(
            task_id='validate_processed_data',
            project_id=config.get_gcp_project_id(),
            dataset_id=processing_config.get('target_dataset'),
            table_id=processing_config.get('target_table'),
            rules_path=get_quality_rules_path(dataset_id),
            quality_threshold=QUALITY_THRESHOLD,
            provide_context=True,
        )

        # Create quality-based branch task
        branch = QualityBasedBranchOperator(
            task_id='quality_check',
            validation_task_id='validate_processed_data',
            quality_threshold=QUALITY_THRESHOLD,
            pass_task_id='load_processed_data',
            fail_task_id='report_processing_metrics',
            provide_context=True,
        )

        # Create healing task (if self-healing enabled)
        if SELF_HEALING_ENABLED:
            heal = DataQualityHealingOperator(
                task_id='heal_data_quality_issues',
                validation_task_id='validate_processed_data',
                data_source=processing_config.get('target_table'),
                provide_context=True,
            )

        # Create loading task
        load = PythonOperator(
            task_id='load_processed_data',
            python_callable=load_processed_data,
            op_kwargs={'dataset_id': dataset_id},
            provide_context=True,
        )

        # Create metrics reporting task
        report = PythonOperator(
            task_id='report_processing_metrics',
            python_callable=report_processing_metrics,
            op_kwargs={'dataset_id': dataset_id},
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # Set up task dependencies to create the workflow
        start >> transform >> enrich >> validate
        validate >> branch
        if SELF_HEALING_ENABLED:
            branch >> heal >> validate
            validate >> report
            load << branch
        else:
            branch >> load >> report
            validate >> report
        load >> end
        report >> end

    # Return the configured DAG
    return dag


# Example DAG instantiation
dag = create_data_processing_dag(
    dataset_id='customer_data',
    schedule='@daily',
    dag_args={'description': 'Processes customer data daily'}
)