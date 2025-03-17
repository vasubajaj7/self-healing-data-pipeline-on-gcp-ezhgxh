"""
Defines API routes for data ingestion operations in the self-healing data pipeline.
This module creates and configures FastAPI router endpoints for managing data sources,
pipeline definitions, and pipeline executions.
"""

from typing import List, Dict, Optional, Any, Union

from fastapi import FastAPI, APIRouter, Depends, Query, Path, Body, status, Request  # fastapi ^0.95.0
from fastapi import Request # fastapi ^0.95.0
from typing import Optional, List # standard library

from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.api.models.request_models import (  # src/backend/api/models/request_models.py
    PaginationParams, DateRangeParams, SourceSystemCreateRequest, SourceSystemUpdateRequest, SourceSystemTestRequest, PipelineCreateRequest, PipelineUpdateRequest, PipelineExecuteRequest
)
from src.backend.api.controllers.ingestion_controller import (  # src/backend/api/controllers/ingestion_controller.py
    get_source_systems, get_source_system, create_source_system, update_source_system, delete_source_system,
    test_source_connection, get_source_schema, get_pipelines, get_pipeline, create_pipeline, update_pipeline,
    delete_pipeline, execute_pipeline, get_pipeline_executions, get_pipeline_execution, get_task_executions,
    cancel_pipeline_execution, retry_pipeline_execution, get_supported_source_types
)

# Initialize logger
logger = get_logger(__name__)

# Create an APIRouter instance with prefix and tags
router = APIRouter(prefix="/api/ingestion", tags=["Ingestion"])

@router.get("/sources", summary="Get all data sources", description="Retrieve a paginated list of data source systems with optional filtering")
def get_source_systems_route(
    request: Request,
    pagination: Depends(PaginationParams),
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    status: Optional[str] = Query(None, description="Filter by connection status")
) -> dict:
    """Route handler for retrieving a paginated list of data source systems"""
    logger.info(f"Request for source systems with filters: source_type={source_type}, status={status}")
    sources, total_count = get_source_systems(pagination, source_type, status)
    return {
        "items": sources,
        "page": pagination.page,
        "page_size": pagination.page_size,
        "total_items": total_count,
        "request": request.url.path,
        "message": "Source systems retrieved successfully"
    }

@router.get("/sources/{source_id}", summary="Get data source", description="Retrieve a specific data source system by ID")
def get_source_system_route(source_id: str = Path(..., description="The ID of the source system to retrieve")) -> dict:
    """Route handler for retrieving a specific data source system by ID"""
    logger.info(f"Request for source system: {source_id}")
    source = get_source_system(source_id)
    return {
        "data": source,
        "message": "Source system retrieved successfully"
    }

@router.post("/sources", status_code=status.HTTP_201_CREATED, summary="Create data source", description="Create a new data source system")
def create_source_system_route(source_request: SourceSystemCreateRequest) -> dict:
    """Route handler for creating a new data source system"""
    logger.info("Request to create a new source system")
    created_source = create_source_system(source_request)
    return {
        "data": created_source,
        "message": "Source system created successfully"
    }

@router.put("/sources/{source_id}", summary="Update data source", description="Update an existing data source system")
def update_source_system_route(source_id: str = Path(..., description="The ID of the source system to update"), source_update: SourceSystemUpdateRequest = Body(..., description="Source system update details")) -> dict:
    """Route handler for updating an existing data source system"""
    logger.info(f"Request to update source system: {source_id}")
    updated_source = update_source_system(source_id, source_update)
    return {
        "data": updated_source,
        "message": "Source system updated successfully"
    }

@router.delete("/sources/{source_id}", summary="Delete data source", description="Delete a data source system")
def delete_source_system_route(source_id: str = Path(..., description="The ID of the source system to delete")) -> dict:
    """Route handler for deleting a data source system"""
    logger.info(f"Request to delete source system: {source_id}")
    delete_source_system(source_id)
    return {
        "message": "Source system deleted successfully"
    }

@router.post("/sources/test-connection", summary="Test source connection", description="Test connection to a data source")
def test_source_connection_route(test_request: SourceSystemTestRequest = Body(..., description="Source connection test details")) -> dict:
    """Route handler for testing a data source connection"""
    logger.info("Request to test a source connection")
    test_results = test_source_connection(test_request)
    return {
        "data": test_results,
        "message": "Connection test completed"
    }

@router.get("/sources/{source_id}/schema/{object_name}", summary="Get source schema", description="Retrieve schema information from a data source")
def get_source_schema_route(source_id: str = Path(..., description="The ID of the source system"), object_name: str = Path(..., description="The name of the object to retrieve schema for")) -> dict:
    """Route handler for retrieving schema information from a data source"""
    logger.info(f"Request for source schema: source_id={source_id}, object_name={object_name}")
    schema_info = get_source_schema(source_id, object_name)
    return {
        "data": schema_info,
        "message": "Schema information retrieved successfully"
    }

@router.get("/sources/types", summary="Get supported source types", description="Retrieve a list of supported data source types")
def get_supported_source_types_route() -> dict:
    """Route handler for retrieving a list of supported data source types"""
    logger.info("Request for supported source types")
    source_types = get_supported_source_types()
    return {
        "data": source_types,
        "message": "Supported source types retrieved successfully"
    }

@router.get("/pipelines", summary="Get all pipelines", description="Retrieve a paginated list of pipeline definitions with optional filtering")
def get_pipelines_route(
    request: Request,
    pagination: Depends(PaginationParams),
    source_id: Optional[str] = Query(None, description="Filter by source ID"),
    is_active: Optional[bool] = Query(None, description="Filter by active status")
) -> dict:
    """Route handler for retrieving a paginated list of pipeline definitions"""
    logger.info(f"Request for pipelines with filters: source_id={source_id}, is_active={is_active}")
    pipelines, total_count = get_pipelines(pagination, source_id, is_active)
    return {
        "items": pipelines,
        "page": pagination.page,
        "page_size": pagination.page_size,
        "total_items": total_count,
        "request": request.url.path,
        "message": "Pipelines retrieved successfully"
    }

@router.get("/pipelines/{pipeline_id}", summary="Get pipeline", description="Retrieve a specific pipeline definition by ID")
def get_pipeline_route(pipeline_id: str = Path(..., description="The ID of the pipeline to retrieve")) -> dict:
    """Route handler for retrieving a specific pipeline definition by ID"""
    logger.info(f"Request for pipeline: {pipeline_id}")
    pipeline = get_pipeline(pipeline_id)
    return {
        "data": pipeline,
        "message": "Pipeline retrieved successfully"
    }

@router.post("/pipelines", status_code=status.HTTP_201_CREATED, summary="Create pipeline", description="Create a new pipeline definition")
def create_pipeline_route(pipeline_request: PipelineCreateRequest = Body(..., description="Pipeline creation details")) -> dict:
    """Route handler for creating a new pipeline definition"""
    logger.info("Request to create a new pipeline")
    created_pipeline = create_pipeline(pipeline_request)
    return {
        "data": created_pipeline,
        "message": "Pipeline created successfully"
    }

@router.put("/pipelines/{pipeline_id}", summary="Update pipeline", description="Update an existing pipeline definition")
def update_pipeline_route(pipeline_id: str = Path(..., description="The ID of the pipeline to update"), pipeline_update: PipelineUpdateRequest = Body(..., description="Pipeline update details")) -> dict:
    """Route handler for updating an existing pipeline definition"""
    logger.info(f"Request to update pipeline: {pipeline_id}")
    updated_pipeline = update_pipeline(pipeline_id, pipeline_update)
    return {
        "data": updated_pipeline,
        "message": "Pipeline updated successfully"
    }

@router.delete("/pipelines/{pipeline_id}", summary="Delete pipeline", description="Delete a pipeline definition")
def delete_pipeline_route(pipeline_id: str = Path(..., description="The ID of the pipeline to delete")) -> dict:
    """Route handler for deleting a pipeline definition"""
    logger.info(f"Request to delete pipeline: {pipeline_id}")
    delete_pipeline(pipeline_id)
    return {
        "message": "Pipeline deleted successfully"
    }

@router.post("/pipelines/{pipeline_id}/execute", summary="Execute pipeline", description="Execute a pipeline")
def execute_pipeline_route(pipeline_id: str = Path(..., description="The ID of the pipeline to execute"), execute_request: PipelineExecuteRequest = Body(..., description="Pipeline execution parameters")) -> dict:
    """Route handler for executing a pipeline"""
    logger.info(f"Request to execute pipeline: {pipeline_id}")
    execution = execute_pipeline(pipeline_id, execute_request)
    return {
        "data": execution,
        "message": "Pipeline execution started successfully"
    }

@router.get("/pipelines/{pipeline_id}/executions", summary="Get pipeline executions", description="Retrieve a paginated list of pipeline executions with optional filtering")
def get_pipeline_executions_route(
    request: Request,
    pagination: Depends(PaginationParams),
    pipeline_id: str = Path(..., description="The ID of the pipeline"),
    status: Optional[str] = Query(None, description="Filter by execution status"),
    date_range: Depends(DateRangeParams)
) -> dict:
    """Route handler for retrieving a paginated list of pipeline executions"""
    logger.info(f"Request for pipeline executions: pipeline_id={pipeline_id}, status={status}, date_range={date_range}")
    executions, total_count = get_pipeline_executions(pagination, pipeline_id, status, date_range)
    return {
        "items": executions,
        "page": pagination.page,
        "page_size": pagination.page_size,
        "total_items": total_count,
        "request": request.url.path,
        "message": "Pipeline executions retrieved successfully"
    }

@router.get("/executions/{execution_id}", summary="Get pipeline execution", description="Retrieve a specific pipeline execution by ID")
def get_pipeline_execution_route(execution_id: str = Path(..., description="The ID of the execution to retrieve")) -> dict:
    """Route handler for retrieving a specific pipeline execution by ID"""
    logger.info(f"Request for pipeline execution: {execution_id}")
    execution = get_pipeline_execution(execution_id)
    return {
        "data": execution,
        "message": "Pipeline execution retrieved successfully"
    }

@router.get("/executions/{execution_id}/tasks", summary="Get task executions", description="Retrieve a paginated list of task executions for a pipeline execution")
def get_task_executions_route(
    request: Request,
    pagination: Depends(PaginationParams),
    execution_id: str = Path(..., description="The ID of the pipeline execution"),
    status: Optional[str] = Query(None, description="Filter by task status")
) -> dict:
    """Route handler for retrieving a paginated list of task executions for a pipeline execution"""
    logger.info(f"Request for task executions: execution_id={execution_id}, status={status}")
    task_executions, total_count = get_task_executions(pagination, execution_id, status)
    return {
        "items": task_executions,
        "page": pagination.page,
        "page_size": pagination.page_size,
        "total_items": total_count,
        "request": request.url.path,
        "message": "Task executions retrieved successfully"
    }

@router.post("/executions/{execution_id}/cancel", summary="Cancel pipeline execution", description="Cancel a running pipeline execution")
def cancel_pipeline_execution_route(execution_id: str = Path(..., description="The ID of the execution to cancel")) -> dict:
    """Route handler for cancelling a running pipeline execution"""
    logger.info(f"Request to cancel pipeline execution: {execution_id}")
    execution = cancel_pipeline_execution(execution_id)
    return {
        "data": execution,
        "message": "Pipeline execution cancelled successfully"
    }

@router.post("/executions/{execution_id}/retry", summary="Retry pipeline execution", description="Retry a failed pipeline execution")
def retry_pipeline_execution_route(execution_id: str = Path(..., description="The ID of the execution to retry"), execute_request: Optional[PipelineExecuteRequest] = Body(None)) -> dict:
    """Route handler for retrying a failed pipeline execution"""
    logger.info(f"Request to retry pipeline execution: {execution_id}")
    execution = retry_pipeline_execution(execution_id, execute_request)
    return {
        "data": execution,
        "message": "Pipeline execution retried successfully"
    }