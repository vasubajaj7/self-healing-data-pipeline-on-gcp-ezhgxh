"""
Controller responsible for handling data ingestion-related API requests in the self-healing data pipeline.
It processes requests for source system management, pipeline definition management, and pipeline execution operations,
delegating business logic to the ingestion service layer.
"""

import typing
import uuid
from typing import List, Dict, Optional, Any, Union

from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, Request, status

from ..models.request_models import (
    SourceSystemCreateRequest, SourceSystemUpdateRequest, SourceSystemTestRequest, PipelineCreateRequest, PipelineUpdateRequest, PipelineExecuteRequest, PaginationParams, DateRangeParams
)
from ..models.response_models import (
    SourceSystemResponse, SourceSystemListResponse, SourceSystemTestResponse, PipelineResponse, PipelineListResponse, PipelineExecutionResponse, PipelineExecutionListResponse, TaskExecutionListResponse
)
from ..models.data_models import (
    SourceSystem, PipelineDefinition, PipelineExecution, TaskExecution, SourceSystemType, ConnectionStatus, PipelineStatus
)
from ..models.error_models import (
    ErrorCategory, ErrorSeverity, ErrorDetail, ResourceNotFoundError
)
from ..utils.response_utils import (
    create_success_response, create_list_response, create_error_response, handle_exception
)
from ..services import ingestion_service
from ...logging_config import get_logger

# Initialize logger
logger = get_logger(__name__)


router = APIRouter()


@router.get("/sources", response_model=SourceSystemListResponse, summary="Retrieve paginated list of data sources")
def get_source_systems(
    request: Request,
    pagination: PaginationParams = Depends(),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    status: Optional[str] = Query(None, description="Filter by connection status")
):
    """Retrieves a paginated list of data source systems with optional filtering"""
    try:
        logger.info(f"Request for source systems with filters: source_type={source_type}, status={status}")
        sources, total_count = ingestion_service.get_source_systems(pagination, source_type, status)
        return create_list_response(
            items=sources,
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
            request=request,
            message="Source systems retrieved successfully"
        )
    except Exception as e:
        return handle_exception(e)


@router.get("/sources/{source_id}", response_model=SourceSystemResponse, summary="Retrieve a specific data source")
def get_source_system(source_id: str):
    """Retrieves a specific data source system by ID"""
    try:
        logger.info(f"Request for source system: {source_id}")
        source = ingestion_service.get_source_system(source_id)
        if source:
            return create_success_response(data=source, message="Source system retrieved successfully")
        else:
            raise ResourceNotFoundError(resource_type="SourceSystem", resource_id=source_id)
    except Exception as e:
        return handle_exception(e)


@router.post("/sources", response_model=SourceSystemResponse, summary="Create a new data source")
def create_source_system(source_request: SourceSystemCreateRequest):
    """Creates a new data source system"""
    try:
        logger.info("Request to create a new source system")
        created_source = ingestion_service.create_source_system(source_request)
        return create_success_response(data=created_source, message="Source system created successfully")
    except Exception as e:
        return handle_exception(e)


@router.put("/sources/{source_id}", response_model=SourceSystemResponse, summary="Update an existing data source")
def update_source_system(source_id: str, source_update: SourceSystemUpdateRequest):
    """Updates an existing data source system"""
    try:
        logger.info(f"Request to update source system: {source_id}")
        updated_source = ingestion_service.update_source_system(source_id, source_update)
        if updated_source:
            return create_success_response(data=updated_source, message="Source system updated successfully")
        else:
            raise ResourceNotFoundError(resource_type="SourceSystem", resource_id=source_id)
    except Exception as e:
        return handle_exception(e)


@router.delete("/sources/{source_id}", summary="Delete a data source")
def delete_source_system(source_id: str):
    """Deletes a data source system"""
    try:
        logger.info(f"Request to delete source system: {source_id}")
        if ingestion_service.delete_source_system(source_id):
            return create_success_response(data={}, message="Source system deleted successfully")
        else:
            raise ResourceNotFoundError(resource_type="SourceSystem", resource_id=source_id)
    except Exception as e:
        return handle_exception(e)


@router.post("/sources/test", response_model=SourceSystemTestResponse, summary="Test connection to a data source")
def test_source_connection(test_request: SourceSystemTestRequest):
    """Tests connection to a data source"""
    try:
        logger.info("Request to test a source connection")
        test_results = ingestion_service.test_source_connection(test_request)
        return create_success_response(data=test_results, message="Connection test completed")
    except Exception as e:
        return handle_exception(e)


@router.get("/sources/{source_id}/schema/{object_name}", response_model=dict, summary="Retrieve schema from a data source")
def get_source_schema(source_id: str, object_name: str):
    """Retrieves schema information from a data source"""
    try:
        logger.info(f"Request for source schema: source_id={source_id}, object_name={object_name}")
        schema_info = ingestion_service.get_source_schema(source_id, object_name)
        if schema_info:
            return create_success_response(data=schema_info, message="Schema information retrieved successfully")
        else:
            raise ResourceNotFoundError(resource_type="SourceSystem", resource_id=source_id)
    except Exception as e:
        return handle_exception(e)


@router.get("/pipelines", response_model=PipelineListResponse, summary="Retrieve paginated list of pipelines")
def get_pipelines(
    request: Request,
    pagination: PaginationParams = Depends(),
    source_id: Optional[str] = Query(None, description="Filter by source ID"),
    is_active: Optional[bool] = Query(None, description="Filter by active status")
):
    """Retrieves a paginated list of pipeline definitions with optional filtering"""
    try:
        logger.info(f"Request for pipelines with filters: source_id={source_id}, is_active={is_active}")
        pipelines, total_count = ingestion_service.get_pipelines(pagination, source_id, is_active)
        return create_list_response(
            items=pipelines,
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
            request=request,
            message="Pipelines retrieved successfully"
        )
    except Exception as e:
        return handle_exception(e)


@router.get("/pipelines/{pipeline_id}", response_model=PipelineResponse, summary="Retrieve a specific pipeline")
def get_pipeline(pipeline_id: str):
    """Retrieves a specific pipeline definition by ID"""
    try:
        logger.info(f"Request for pipeline: {pipeline_id}")
        pipeline = ingestion_service.get_pipeline(pipeline_id)
        if pipeline:
            return create_success_response(data=pipeline, message="Pipeline retrieved successfully")
        else:
            raise ResourceNotFoundError(resource_type="Pipeline", resource_id=pipeline_id)
    except Exception as e:
        return handle_exception(e)


@router.post("/pipelines", response_model=PipelineResponse, summary="Create a new pipeline")
def create_pipeline(pipeline_request: PipelineCreateRequest):
    """Creates a new pipeline definition"""
    try:
        logger.info("Request to create a new pipeline")
        created_pipeline = ingestion_service.create_pipeline(pipeline_request)
        return create_success_response(data=created_pipeline, message="Pipeline created successfully")
    except Exception as e:
        return handle_exception(e)


@router.put("/pipelines/{pipeline_id}", response_model=PipelineResponse, summary="Update an existing pipeline")
def update_pipeline(pipeline_id: str, pipeline_update: PipelineUpdateRequest):
    """Updates an existing pipeline definition"""
    try:
        logger.info(f"Request to update pipeline: {pipeline_id}")
        updated_pipeline = ingestion_service.update_pipeline(pipeline_id, pipeline_update)
        if updated_pipeline:
            return create_success_response(data=updated_pipeline, message="Pipeline updated successfully")
        else:
            raise ResourceNotFoundError(resource_type="Pipeline", resource_id=pipeline_id)
    except Exception as e:
        return handle_exception(e)


@router.delete("/pipelines/{pipeline_id}", summary="Delete a pipeline")
def delete_pipeline(pipeline_id: str):
    """Deletes a pipeline definition"""
    try:
        logger.info(f"Request to delete pipeline: {pipeline_id}")
        if ingestion_service.delete_pipeline(pipeline_id):
            return create_success_response(data={}, message="Pipeline deleted successfully")
        else:
            raise ResourceNotFoundError(resource_type="Pipeline", resource_id=pipeline_id)
    except Exception as e:
        return handle_exception(e)


@router.post("/pipelines/{pipeline_id}/execute", response_model=PipelineExecutionResponse, summary="Execute a pipeline")
def execute_pipeline(pipeline_id: str, execute_request: PipelineExecuteRequest):
    """Executes a pipeline"""
    try:
        logger.info(f"Request to execute pipeline: {pipeline_id}")
        execution = ingestion_service.execute_pipeline(pipeline_id, execute_request)
        if execution:
            return create_success_response(data=execution, message="Pipeline execution started successfully")
        else:
            raise ResourceNotFoundError(resource_type="Pipeline", resource_id=pipeline_id)
    except Exception as e:
        return handle_exception(e)


@router.get("/pipelines/{pipeline_id}/executions", response_model=PipelineExecutionListResponse, summary="Retrieve paginated list of pipeline executions")
def get_pipeline_executions(
    request: Request,
    pagination: PaginationParams = Depends(),
    pipeline_id: str = Query(..., description="Pipeline ID"),
    status: Optional[str] = Query(None, description="Filter by execution status"),
    date_range: DateRangeParams = Depends()
):
    """Retrieves a paginated list of pipeline executions with optional filtering"""
    try:
        logger.info(f"Request for pipeline executions: pipeline_id={pipeline_id}, status={status}, date_range={date_range}")
        executions, total_count = ingestion_service.get_pipeline_executions(pagination, pipeline_id, status, date_range)
        return create_list_response(
            items=executions,
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
            request=request,
            message="Pipeline executions retrieved successfully"
        )
    except Exception as e:
        return handle_exception(e)


@router.get("/executions/{execution_id}", response_model=PipelineExecutionResponse, summary="Retrieve a specific pipeline execution")
def get_pipeline_execution(execution_id: str):
    """Retrieves a specific pipeline execution by ID"""
    try:
        logger.info(f"Request for pipeline execution: {execution_id}")
        execution = ingestion_service.get_pipeline_execution(execution_id)
        if execution:
            return create_success_response(data=execution, message="Pipeline execution retrieved successfully")
        else:
            raise ResourceNotFoundError(resource_type="PipelineExecution", resource_id=execution_id)
    except Exception as e:
        return handle_exception(e)


@router.get("/executions/{execution_id}/tasks", response_model=TaskExecutionListResponse, summary="Retrieve paginated list of task executions")
def get_task_executions(
    request: Request,
    pagination: PaginationParams = Depends(),
    execution_id: str = Query(..., description="Execution ID"),
    status: Optional[str] = Query(None, description="Filter by task status")
):
    """Retrieves a paginated list of task executions for a pipeline execution"""
    try:
        logger.info(f"Request for task executions: execution_id={execution_id}, status={status}")
        task_executions, total_count = ingestion_service.get_task_executions(pagination, execution_id, status)
        return create_list_response(
            items=task_executions,
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_count,
            request=request,
            message="Task executions retrieved successfully"
        )
    except Exception as e:
        return handle_exception(e)


@router.post("/executions/{execution_id}/cancel", response_model=PipelineExecutionResponse, summary="Cancel a pipeline execution")
def cancel_pipeline_execution(execution_id: str):
    """Cancels a running pipeline execution"""
    try:
        logger.info(f"Request to cancel pipeline execution: {execution_id}")
        execution = ingestion_service.cancel_pipeline_execution(execution_id)
        if execution:
            return create_success_response(data=execution, message="Pipeline execution cancelled successfully")
        else:
            raise ResourceNotFoundError(resource_type="PipelineExecution", resource_id=execution_id)
    except Exception as e:
        return handle_exception(e)


@router.post("/executions/{execution_id}/retry", response_model=PipelineExecutionResponse, summary="Retry a pipeline execution")
def retry_pipeline_execution(execution_id: str, execute_request: Optional[PipelineExecuteRequest] = None):
    """Retries a failed pipeline execution"""
    try:
        logger.info(f"Request to retry pipeline execution: {execution_id}")
        execution = ingestion_service.retry_pipeline_execution(execution_id, execute_request)
        if execution:
            return create_success_response(data=execution, message="Pipeline execution retried successfully")
        else:
            raise ResourceNotFoundError(resource_type="PipelineExecution", resource_id=execution_id)
    except Exception as e:
        return handle_exception(e)


@router.get("/sources/types", response_model=dict, summary="Retrieve supported data source types")
def get_supported_source_types():
    """Retrieves a list of supported data source types"""
    try:
        logger.info("Request for supported source types")
        source_types = ingestion_service.get_supported_source_types()
        return create_success_response(data=source_types, message="Supported source types retrieved successfully")
    except Exception as e:
        return handle_exception(e)