import os
from datetime import timedelta
import json

from airflow import DAG  # apache-airflow version 2.5.x
from airflow.models import BaseOperator  # apache-airflow version 2.5.x
from airflow.operators.dummy import DummyOperator  # apache-airflow version 2.5.x
from airflow.operators.python import PythonOperator  # apache-airflow version 2.5.x
from airflow.utils.dates import days_ago  # apache-airflow version 2.5.x
from airflow.utils.trigger_rule import TriggerRule  # apache-airflow version 2.5.x

from src.backend.constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, DEFAULT_QUALITY_THRESHOLD  # Import constants for API configuration and quality thresholds
from src.backend.config import get_config  # Access application configuration settings
from src.backend.utils.logging.logger import get_logger  # Configure logging for the DAG
from src.backend.airflow.plugins.custom_operators.api_operators import (  # Use custom API operators for data extraction and loading
    ApiRequestOperator,
    ApiDataExtractOperator,
    ApiToDataFrameOperator,
    ApiToBigQueryOperator,
    SelfHealingApiDataExtractOperator,
    SelfHealingApiToDataFrameOperator,
)
from src.backend.airflow.plugins.hooks.api_hooks import ApiPaginationConfig  # Use API hooks for pagination configuration
from src.backend.ingestion.connectors.api_connector import ApiAuthType, ApiPaginationType  # Import API connector enumerations
from src.backend.airflow.plugins.custom_operators.quality_operators import (  # Use custom quality operators for data validation
    DataQualityValidationOperator,
    DataQualityReportingOperator,
    QualityBasedBranchOperator,
    load_validation_rules,
)
from src.backend.airflow.plugins.custom_operators.healing_operators import (  # Use custom healing operators for self-healing capabilities
    DataQualityHealingOperator,
    PipelineHealingOperator,
)

logger = get_logger(__name__)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

config = get_config()


def get_api_connection_params(source_id: str) -> dict:
    """Get API connection parameters from configuration or environment variables

    Args:
        source_id (str): The ID of the API data source

    Returns:
        dict: Connection parameters for the specified API source
    """
    logger.info(f"Getting API connection parameters for source: {source_id}")
    api_sources = config.get('api_sources')
    api_source = next((source for source in api_sources if source['source_id'] == source_id), None)
    if not api_source:
        raise ValueError(f"API source not found: {source_id}")

    connection_params = {
        'base_url': api_source.get('base_url'),
        'auth_type': api_source.get('auth_type'),
        'auth_config': api_source.get('auth_config', {})
    }

    # Override with environment variables if present
    base_url_env_var = f"APP_API_{source_id.upper()}_BASE_URL"
    if base_url_env_var in os.environ:
        connection_params['base_url'] = os.environ[base_url_env_var]

    # Validate required parameters
    if not connection_params['base_url']:
        raise ValueError("Missing required parameter: base_url")

    logger.debug(f"API connection parameters: {connection_params}")
    return connection_params


def get_api_extraction_params(source_id: str) -> dict:
    """Get API extraction parameters from configuration

    Args:
        source_id (str): The ID of the API data source

    Returns:
        dict: Extraction parameters for the specified API source
    """
    logger.info(f"Getting API extraction parameters for source: {source_id}")
    api_sources = config.get('api_sources')
    api_source = next((source for source in api_sources if source['source_id'] == source_id), None)
    if not api_source:
        raise ValueError(f"API source not found: {source_id}")

    extraction_params = {
        'endpoint': api_source.get('endpoint'),
        'method': api_source.get('method', 'GET'),
        'params': api_source.get('params', {}),
        'data_path': api_source.get('data_path'),
        'paginate': api_source.get('paginate', False),
        'pagination_config': api_source.get('pagination_config', {})
    }

    # Set default values for optional parameters
    if 'params' not in extraction_params:
        extraction_params['params'] = {}
    if 'paginate' not in extraction_params:
        extraction_params['paginate'] = False

    # Configure pagination settings if applicable
    if extraction_params['paginate']:
        pagination_settings = extraction_params.get('pagination_config', {})
        if not pagination_settings:
            raise ValueError("Missing pagination configuration")
        extraction_params['pagination_config'] = create_pagination_config(pagination_settings)

    logger.debug(f"API extraction parameters: {extraction_params}")
    return extraction_params


def get_api_auth_config(source_id: str) -> dict:
    """Get API authentication configuration

    Args:
        source_id (str): The ID of the API data source

    Returns:
        dict: Authentication configuration for the specified API source
    """
    logger.info(f"Getting API authentication configuration for source: {source_id}")
    api_sources = config.get('api_sources')
    api_source = next((source for source in api_sources if source['source_id'] == source_id), None)
    if not api_source:
        raise ValueError(f"API source not found: {source_id}")

    auth_type = api_source.get('auth_type')
    auth_config = api_source.get('auth_config', {})

    if auth_type == ApiAuthType.API_KEY.value:
        # Retrieve API key from Secret Manager if needed
        pass  # Add Secret Manager retrieval logic here

    elif auth_type == ApiAuthType.OAUTH2.value:
        # Configure OAuth2 client and retrieve token
        pass  # Add OAuth2 configuration logic here

    elif auth_type == ApiAuthType.BASIC_AUTH.value:
        # Retrieve username and password from Secret Manager if needed
        pass  # Add Secret Manager retrieval logic here

    logger.debug(f"API authentication configuration: {auth_config}")
    return auth_config


def get_bigquery_table_params(source_id: str) -> dict:
    """Get BigQuery table parameters for the API source

    Args:
        source_id (str): The ID of the API data source

    Returns:
        dict: BigQuery table parameters for the specified API source
    """
    logger.info(f"Getting BigQuery table parameters for source: {source_id}")
    api_sources = config.get('api_sources')
    api_source = next((source for source in api_sources if source['source_id'] == source_id), None)
    if not api_source:
        raise ValueError(f"API source not found: {source_id}")

    bq_params = {
        'project_id': config.get('gcp.project_id'),
        'dataset_id': config.get('bigquery.dataset'),
        'table_id': api_source.get('table_id'),
        'schema': api_source.get('schema')
    }

    # Set default values for optional parameters
    if 'project_id' not in bq_params:
        bq_params['project_id'] = config.get('gcp.project_id')
    if 'dataset_id' not in bq_params:
        bq_params['dataset_id'] = config.get('bigquery.dataset')

    logger.debug(f"BigQuery table parameters: {bq_params}")
    return bq_params


def get_quality_validation_rules(source_id: str) -> list:
    """Get data quality validation rules for the API source

    Args:
        source_id (str): The ID of the API data source

    Returns:
        list: List of data quality validation rules for the specified API source
    """
    logger.info(f"Getting data quality validation rules for source: {source_id}")
    api_sources = config.get('api_sources')
    api_source = next((source for source in api_sources if source['source_id'] == source_id), None)
    if not api_source:
        raise ValueError(f"API source not found: {source_id}")

    rules = api_source.get('validation_rules')
    rules_path = api_source.get('rules_path')

    if rules:
        return rules
    elif rules_path:
        return load_validation_rules(rules_path=rules_path)
    else:
        logger.warning(f"No validation rules or rules_path provided for source: {source_id}")
        return []


def create_pagination_config(pagination_settings: dict) -> ApiPaginationConfig:
    """Create pagination configuration for API requests

    Args:
        pagination_settings (dict): Pagination settings dictionary

    Returns:
        ApiPaginationConfig: Configured pagination object
    """
    logger.info("Creating pagination configuration")
    return ApiPaginationConfig.from_dict(pagination_settings)


def create_api_ingestion_dag(source_id: str, schedule: str, dag_args: dict) -> DAG:
    """Factory function to create an API ingestion DAG for a specific source

    Args:
        source_id (str): The ID of the API data source
        schedule (str): The DAG schedule (e.g., '0 0 * * *')
        dag_args (dict): Additional arguments for the DAG

    Returns:
        DAG: Configured Airflow DAG for API ingestion
    """
    logger.info(f"Creating API ingestion DAG for source: {source_id}")

    connection_params = get_api_connection_params(source_id)
    extraction_params = get_api_extraction_params(source_id)
    auth_config = get_api_auth_config(source_id)
    bq_params = get_bigquery_table_params(source_id)
    validation_rules = get_quality_validation_rules(source_id)

    dag_id = f"api_ingestion_{source_id}"
    with DAG(dag_id=dag_id, default_args=default_args, schedule_interval=schedule, **dag_args) as dag:
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')

        extract_api_data = SelfHealingApiDataExtractOperator(
            task_id='extract_api_data',
            conn_id=source_id,
            endpoint=extraction_params['endpoint'],
            params=extraction_params['params'],
            headers=auth_config,
        )

        validate_data_quality = DataQualityValidationOperator(
            task_id='validate_data_quality',
            project_id=bq_params['project_id'],
            dataset_id=bq_params['dataset_id'],
            table_id=bq_params['table_id'],
            validation_rules=validation_rules,
        )

        load_data_to_bigquery = ApiToBigQueryOperator(
            task_id='load_data_to_bigquery',
            conn_id=source_id,
            endpoint=extraction_params['endpoint'],
            params=extraction_params['params'],
            headers=auth_config,
            destination_project_dataset_table=f"{bq_params['project_id']}.{bq_params['dataset_id']}.{bq_params['table_id']}",
            schema_fields=bq_params['schema'],
        )

        start >> extract_api_data >> validate_data_quality >> load_data_to_bigquery >> end

    logger.info(f"API ingestion DAG created: {dag_id}")
    return dag


# Example DAG instantiation
example_dag = create_api_ingestion_dag(
    source_id='my_api',
    schedule='0 0 * * *',
    dag_args={
        'catchup': False,
        'tags': ['api', 'ingestion']
    }
)

dag = example_dag