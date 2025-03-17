# src/backend/api/services/ingestion_service.py
"""
Service layer for data ingestion operations in the self-healing data pipeline.
This service provides methods for managing data sources, pipeline definitions, and
pipeline executions, acting as an intermediary between API controllers and the
underlying data repositories.
"""

import typing
import datetime
import uuid
import json
from typing import List, Dict, Optional, Any, Union

from ..models.data_models import (  # src/backend/api/models/data_models.py
    SourceSystem,
    PipelineDefinition,
    PipelineExecution,
    TaskExecution,
    SourceSystemType,
    ConnectionStatus,
    PipelineStatus,
    TaskStatus
)
from ..models.request_models import (  # src/backend/api/models/request_models.py
    SourceSystemCreateRequest,
    SourceSystemUpdateRequest,
    SourceSystemTestRequest,
    PipelineCreateRequest,
    PipelineUpdateRequest,
    PipelineExecuteRequest,
    PaginationParams,
    DateRangeParams
)
from ..models.error_models import (  # src/backend/api/models/error_models.py
    ResourceNotFoundError,
    ErrorDetail,
    ErrorCategory,
    ErrorSeverity
)
from ...db.repositories.source_repository import SourceRepository  # src/backend/db/repositories/source_repository.py
from ...db.repositories.pipeline_repository import PipelineRepository  # src/backend/db/repositories/pipeline_repository.py
from ...db.repositories.execution_repository import ExecutionRepository  # src/backend/db/repositories/execution_repository.py
from ...ingestion.connectors.base_connector import BaseConnector  # src/backend/ingestion/connectors/base_connector.py
from ...ingestion.connectors import get_connector_for_source_type  # src/backend/ingestion/connectors/__init__.py
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from ...config import config  # src/backend/config.py

# Initialize logger
logger = get_logger(__name__)


class IngestionService:
    """Service class for managing data ingestion operations"""

    def __init__(
        self,
        source_repository: Optional[SourceRepository] = None,
        pipeline_repository: Optional[PipelineRepository] = None,
        execution_repository: Optional[ExecutionRepository] = None,
        composer_client: Optional[Any] = None  # ComposerClient
    ):
        """Initializes the IngestionService with required repositories"""
        self._source_repository = source_repository or SourceRepository()
        self._pipeline_repository = pipeline_repository or PipelineRepository()
        self._execution_repository = execution_repository or ExecutionRepository()
        self._composer_client = composer_client or ComposerClient()
        logger.info("IngestionService initialized")

    def get_source_systems(
        self, pagination: PaginationParams, source_type: Optional[SourceSystemType] = None, status: Optional[ConnectionStatus] = None
    ) -> typing.Tuple[List[SourceSystem], int]:
        """Retrieves a paginated list of data source systems with optional filtering"""
        filter_criteria = {}
        if source_type:
            filter_criteria["source_type"] = source_type
        if status:
            filter_criteria["status"] = status

        sources = self._source_repository.list_sources(
            pagination.page, pagination.page_size, filter_criteria
        )
        total_count = self._source_repository.count_sources(filter_criteria)

        logger.info(
            f"Retrieved {len(sources)} source systems (total: {total_count}) with filter: {filter_criteria}"
        )
        return sources, total_count

    def get_source_system(self, source_id: str) -> Optional[SourceSystem]:
        """Retrieves a specific data source system by ID"""
        source = self._source_repository.get_source_by_id(source_id)
        logger.info(f"Retrieved source system: {source_id}")
        return source

    def create_source_system(self, source_request: SourceSystemCreateRequest) -> SourceSystem:
        """Creates a new data source system"""
        source_system = SourceSystem(**source_request.dict())
        try:
            self._source_repository.validate_source(source_system)
        except ValueError as e:
            raise ValueError(f"Invalid source system: {e}")

        created_source = self._source_repository.create_source(source_system)
        logger.info(f"Created source system: {created_source.source_id}")
        return created_source

    def update_source_system(self, source_id: str, source_update: SourceSystemUpdateRequest) -> Optional[SourceSystem]:
        """Updates an existing data source system"""
        existing_source = self._source_repository.get_source_by_id(source_id)
        if not existing_source:
            return None

        updated_source = existing_source.copy(update=source_update.dict(exclude_unset=True))
        try:
            self._source_repository.validate_source(updated_source)
        except ValueError as e:
            raise ValueError(f"Invalid source system: {e}")

        updated_source = self._source_repository.update_source(updated_source)
        logger.info(f"Updated source system: {updated_source.source_id}")
        return updated_source

    def delete_source_system(self, source_id: str) -> bool:
        """Deletes a data source system"""
        existing_source = self._source_repository.get_source_by_id(source_id)
        if not existing_source:
            return False

        self._source_repository.delete_source(source_id)
        logger.info(f"Deleted source system: {source_id}")
        return True

    def test_source_connection(self, test_request: SourceSystemTestRequest) -> dict:
        """Tests connection to a data source"""
        connector = get_connector_for_source_type(test_request.source_type)
        connection_details = test_request.connection_details
        test_result = connector.test_connection(connection_details)
        logger.info(f"Connection test result: {test_result}")
        return {"connection_successful": test_result[0], "details": test_result[1]}

    def get_source_schema(self, source_id: str, object_name: str) -> Optional[dict]:
        """Retrieves schema information from a data source"""
        source = self._source_repository.get_source_by_id(source_id)
        if not source:
            return None

        connector = get_connector_for_source_type(source.source_type)
        schema = connector.get_schema(source.connection_details, object_name)
        logger.info(f"Retrieved schema for source: {source_id}, object: {object_name}")
        return schema

    def get_pipelines(
        self, pagination: PaginationParams, source_id: Optional[str] = None, is_active: Optional[bool] = None
    ) -> typing.Tuple[List[PipelineDefinition], int]:
        """Retrieves a paginated list of pipeline definitions with optional filtering"""
        search_criteria = {}
        if source_id:
            search_criteria["source_id"] = source_id
        if is_active is not None:
            search_criteria["is_active"] = is_active

        pipelines = self._pipeline_repository.search_pipelines(
            pagination.page, pagination.page_size, search_criteria
        )
        total_count = self._pipeline_repository.count_pipelines(search_criteria)

        logger.info(
            f"Retrieved {len(pipelines)} pipelines (total: {total_count}) with criteria: {search_criteria}"
        )
        return pipelines, total_count

    def get_pipeline(self, pipeline_id: str) -> Optional[PipelineDefinition]:
        """Retrieves a specific pipeline definition by ID"""
        pipeline = self._pipeline_repository.get_pipeline(pipeline_id)
        logger.info(f"Retrieved pipeline: {pipeline_id}")
        return pipeline

    def create_pipeline(self, pipeline_request: PipelineCreateRequest) -> PipelineDefinition:
        """Creates a new pipeline definition"""
        try:
            source = self._source_repository.get_source_by_id(pipeline_request.source_id)
            if not source:
                raise ResourceNotFoundError("SourceSystem", pipeline_request.source_id)

            pipeline_definition = PipelineDefinition(**pipeline_request.dict())
            self._pipeline_repository.validate_pipeline(pipeline_definition)
            created_pipeline = self._pipeline_repository.create_pipeline(pipeline_definition)
            logger.info(f"Created pipeline: {created_pipeline.pipeline_id}")
            return created_pipeline
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
            raise

    def update_pipeline(self, pipeline_id: str, pipeline_update: PipelineUpdateRequest) -> Optional[PipelineDefinition]:
        """Updates an existing pipeline definition"""
        existing_pipeline = self._pipeline_repository.get_pipeline(pipeline_id)
        if not existing_pipeline:
            return None

        updated_pipeline = existing_pipeline.copy(update=pipeline_update.dict(exclude_unset=True))
        try:
            self._pipeline_repository.validate_pipeline(updated_pipeline)
        except ValueError as e:
            raise ValueError(f"Invalid pipeline: {e}")

        updated_pipeline = self._pipeline_repository.update_pipeline(updated_pipeline)
        logger.info(f"Updated pipeline: {updated_pipeline.pipeline_id}")
        return updated_pipeline

    def delete_pipeline(self, pipeline_id: str) -> bool:
        """Deletes a pipeline definition"""
        existing_pipeline = self._pipeline_repository.get_pipeline(pipeline_id)
        if not existing_pipeline:
            return False

        self._pipeline_repository.delete_pipeline(pipeline_id)
        logger.info(f"Deleted pipeline: {pipeline_id}")
        return True

    def execute_pipeline(self, pipeline_id: str, execute_request: PipelineExecuteRequest) -> Optional[PipelineExecution]:
        """Executes a pipeline"""
        pipeline = self._pipeline_repository.get_pipeline(pipeline_id)
        if not pipeline:
            return None

        execution = PipelineExecution(pipeline_id=pipeline_id, execution_params=execute_request.execution_params)
        self._execution_repository.create_pipeline_execution(execution)

        # Trigger DAG execution in Cloud Composer
        dag_config = pipeline.get_dag_config()
        self._composer_client.trigger_dag(dag_config)

        logger.info(f"Executing pipeline: {pipeline_id}, execution: {execution.execution_id}")
        return execution

    def get_pipeline_executions(
        self, pagination: PaginationParams, pipeline_id: str, status: Optional[PipelineStatus] = None, date_range: DateRangeParams = None
    ) -> typing.Tuple[List[PipelineExecution], int]:
        """Retrieves a paginated list of pipeline executions with optional filtering"""
        search_criteria = {"pipeline_id": pipeline_id}
        if status:
            search_criteria["status"] = status
        if date_range and date_range.start_date:
            search_criteria["start_date"] = date_range.start_date
        if date_range and date_range.end_date:
            search_criteria["end_date"] = date_range.end_date

        executions = self._execution_repository.search_executions(
            pagination.page, pagination.page_size, search_criteria
        )
        total_count = self._execution_repository.count_executions(search_criteria)

        logger.info(
            f"Retrieved {len(executions)} executions (total: {total_count}) with criteria: {search_criteria}"
        )
        return executions, total_count

    def get_pipeline_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Retrieves a specific pipeline execution by ID"""
        execution = self._execution_repository.get_pipeline_execution(execution_id)
        logger.info(f"Retrieved pipeline execution: {execution_id}")
        return execution

    def get_task_executions(
        self, pagination: PaginationParams, execution_id: str, status: Optional[TaskStatus] = None
    ) -> typing.Tuple[List[TaskExecution], int]:
        """Retrieves a paginated list of task executions for a pipeline execution"""
        search_criteria = {"execution_id": execution_id}
        if status:
            search_criteria["status"] = status

        task_executions = self._execution_repository.search_task_executions(
            pagination.page, pagination.page_size, search_criteria
        )
        total_count = self._execution_repository.count_task_executions(search_criteria)

        logger.info(
            f"Retrieved {len(task_executions)} task executions (total: {total_count}) with criteria: {search_criteria}"
        )
        return task_executions

    def cancel_pipeline_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Cancels a running pipeline execution"""
        execution = self._execution_repository.get_pipeline_execution(execution_id)
        if not execution:
            return None

        # Cancel DAG run in Cloud Composer
        self._composer_client.cancel_dag_run(execution.dag_run_id)

        # Update execution status to FAILED
        execution.status = PipelineStatus.FAILED
        self._execution_repository.update_pipeline_execution(execution)

        logger.info(f"Cancelled pipeline execution: {execution_id}")
        return execution

    def retry_pipeline_execution(self, execution_id: str, execute_request: Optional[PipelineExecuteRequest] = None) -> Optional[PipelineExecution]:
        """Retries a failed pipeline execution"""
        original_execution = self._execution_repository.get_pipeline_execution(execution_id)
        if not original_execution:
            return None

        pipeline = self._pipeline_repository.get_pipeline(original_execution.pipeline_id)
        if not pipeline:
            return None

        # Create new execution parameters, merging original with execute_request
        execution_params = original_execution.execution_params
        if execute_request and execute_request.execution_params:
            execution_params.update(execute_request.execution_params)

        # Execute the pipeline with the merged parameters
        new_execution = self.execute_pipeline(pipeline.pipeline_id, PipelineExecuteRequest(execution_params=execution_params))

        logger.info(f"Retried pipeline execution: {execution_id}, new execution: {new_execution.execution_id}")
        return new_execution

    def get_supported_source_types(self) -> list:
        """Retrieves a list of supported data source types"""
        source_types = []
        for source_type in SourceSystemType:
            source_types.append({"type": source_type.value, "capabilities": []})
        logger.info(f"Retrieved supported source types: {source_types}")
        return source_types

    def validate_source_connection(self, source_type: SourceSystemType, connection_details: dict) -> typing.Tuple[bool, list]:
        """Validates connection details for a source type"""
        connector = get_connector_for_source_type(source_type)
        is_valid, errors = connector.validate_connection_details(connection_details)
        logger.info(f"Connection details validation result: valid={is_valid}, errors={errors}")
        return is_valid, errors

    def validate_pipeline_configuration(self, configuration: dict, source_type: SourceSystemType) -> typing.Tuple[bool, list]:
        """Validates pipeline configuration"""
        # Validate required configuration fields (schedule, extraction_params)
        if "schedule" not in configuration:
            return False, ["Schedule is required"]
        if "extraction_params" not in configuration:
            return False, ["Extraction parameters are required"]

        # Validate schedule format
        schedule = configuration["schedule"]
        if not isinstance(schedule, str):
            return False, ["Schedule must be a string (cron format)"]

        # Get connector for the source type
        connector = get_connector_for_source_type(source_type)

        # Validate extraction parameters using the connector
        is_valid, errors = connector.validate_extraction_params(configuration["extraction_params"])
        logger.info(f"Pipeline configuration validation result: valid={is_valid}, errors={errors}")
        return is_valid, errors

from google.cloud import composer_v1 as composer
class ComposerClient:
    """
    Client for interacting with Google Cloud Composer (Apache Airflow).
    This class provides methods for triggering and managing DAG executions in Cloud Composer.
    """

    def __init__(self):
        """Initializes the ComposerClient."""
        self.project_id = config.get_gcp_project_id()
        self.location = config.get_gcp_location()
        self.environment = config.get_composer_environment()
        self.client = composer.EnvironmentsClient()
        logger.info("ComposerClient initialized")

    def trigger_dag(self, dag_config: Dict[str, Any]) -> None:
        """Triggers a DAG execution in Cloud Composer.

        Args:
            dag_config: Configuration parameters for the DAG execution.
        """
        try:
            # Construct the environment name
            environment_name = self.client.environment_path(
                self.project_id, self.location, self.environment
            )

            # Prepare the request to trigger the DAG
            request = composer.CreateEnvironmentRequest(
                parent=environment_name,
                environment=dag_config
            )

            # Trigger the DAG execution
            operation = self.client.create_environment(request=request)
            logger.info(f"Triggered DAG execution: {dag_config['dag_id']}")
        except Exception as e:
            logger.error(f"Error triggering DAG execution: {e}")
            raise

    def cancel_dag_run(self, dag_run_id: str) -> None:
        """Cancels a running DAG run in Cloud Composer.

        Args:
            dag_run_id: The ID of the DAG run to cancel.
        """
        try:
            # Construct the environment name
            environment_name = self.client.environment_path(
                self.project_id, self.location, self.environment
            )

            # Prepare the request to cancel the DAG run
            request = composer.DeleteEnvironmentRequest(
                name=environment_name
            )

            # Cancel the DAG run
            operation = self.client.delete_environment(request=request)
            logger.info(f"Cancelled DAG run: {dag_run_id}")
        except Exception as e:
            logger.error(f"Error cancelling DAG run: {e}")
            raise