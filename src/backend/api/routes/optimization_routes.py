"""
Defines FastAPI routes for performance optimization operations in the self-healing data pipeline API.
This module maps HTTP endpoints to controller functions for query optimization, schema optimization, resource optimization, and optimization configuration management.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, Depends, Path, Query, Body, status, Request

# Import internal modules
from ..controllers import optimization_controller
from ..models.request_models import PaginationParams, DateRangeParams, OptimizationConfigUpdateRequest
from ..models.response_models import DataResponse, OptimizationConfigResponse, OptimizationRecommendationResponse, OptimizationRecommendationListResponse, PaginatedResponse
from ..utils.auth_utils import get_current_user, require_permission

# Define the router for optimization endpoints
router = APIRouter(prefix="/optimization", tags=["Performance Optimization"])

@router.post("/query/recommendations", response_model=OptimizationRecommendationResponse)
@require_permission("optimization:read")
async def get_query_optimization_recommendations_route(
    query: str = Body(..., embed=True, description="SQL query to analyze"),
    optimization_techniques: Optional[List[str]] = Query(None, description="Optional specific techniques to apply"),
    current_user: dict = Depends(get_current_user)
) -> OptimizationRecommendationResponse:
    """
    API endpoint to retrieve optimization recommendations for a SQL query without modifying the query
    """
    return optimization_controller.get_query_optimization_recommendations(query, optimization_techniques)

@router.post("/query/optimize", response_model=DataResponse)
@require_permission("optimization:execute")
async def optimize_query_route(
    query: str = Body(..., embed=True, description="SQL query to optimize"),
    optimization_techniques: Optional[List[str]] = Query(None, description="Optional specific techniques to apply"),
    validate_results: Optional[bool] = Query(True, description="Validate that optimized query returns same results"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """
    API endpoint to optimize a SQL query and return the optimized version with performance comparison
    """
    return optimization_controller.optimize_query(query, optimization_techniques, validate_results)

@router.get("/query/history", response_model=PaginatedResponse)
@require_permission("optimization:read")
async def get_query_optimization_history_route(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    query_hash: Optional[str] = Query(None, description="Filter by query hash"),
    current_user: dict = Depends(get_current_user)
) -> PaginatedResponse:
    """
    API endpoint to retrieve optimization history for similar queries
    """
    return optimization_controller.get_query_optimization_history(
        query_hash=query_hash, 
        start_date=date_range.start_date, 
        end_date=date_range.end_date, 
        page=pagination.page, 
        page_size=pagination.page_size
    )

@router.get("/schema/recommendations/{dataset}/{table}", response_model=OptimizationRecommendationResponse)
@require_permission("optimization:read")
async def get_schema_optimization_recommendations_route(
    dataset: str = Path(..., description="Dataset name"),
    table: str = Path(..., description="Table name"),
    current_user: dict = Depends(get_current_user)
) -> OptimizationRecommendationResponse:
    """
    API endpoint to analyze a BigQuery table schema and provide optimization recommendations
    """
    return optimization_controller.get_schema_optimization_recommendations(dataset, table)

@router.post("/schema/optimize/{dataset}/{table}", response_model=DataResponse)
@require_permission("optimization:execute")
async def apply_schema_optimizations_route(
    dataset: str = Path(..., description="Dataset name"),
    table: str = Path(..., description="Table name"),
    optimizations: Dict[str, Any] = Body(..., description="Optimizations to apply"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """
    API endpoint to apply recommended schema optimizations to a BigQuery table
    """
    return optimization_controller.apply_schema_optimizations(dataset, table, optimizations)

@router.get("/schema/candidates", response_model=PaginatedResponse)
@require_permission("optimization:read")
async def get_schema_optimization_candidates_route(
    pagination: PaginationParams = Depends(),
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    min_table_size_gb: Optional[float] = Query(None, description="Minimum table size in GB"),
    min_query_count: Optional[int] = Query(None, description="Minimum query count"),
    current_user: dict = Depends(get_current_user)
) -> PaginatedResponse:
    """
    API endpoint to identify tables that would benefit from schema optimization
    """
    return optimization_controller.get_schema_optimization_candidates(
        dataset=dataset, 
        min_table_size_gb=min_table_size_gb, 
        min_query_count=min_query_count, 
        page=pagination.page, 
        page_size=pagination.page_size
    )

@router.get("/resource/recommendations", response_model=OptimizationRecommendationListResponse)
@require_permission("optimization:read")
async def get_resource_optimization_recommendations_route(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    current_user: dict = Depends(get_current_user)
) -> OptimizationRecommendationListResponse:
    """
    API endpoint to retrieve resource optimization recommendations across the pipeline
    """
    return optimization_controller.get_resource_optimization_recommendations(resource_type)

@router.post("/resource/optimize", response_model=DataResponse)
@require_permission("optimization:execute")
async def apply_resource_optimization_route(
    resource_type: str = Body(..., embed=True, description="Resource type to optimize"),
    strategy: str = Body(..., embed=True, description="Optimization strategy to apply"),
    parameters: Dict[str, Any] = Body(..., description="Optimization parameters"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """
    API endpoint to apply a recommended resource optimization
    """
    return optimization_controller.apply_resource_optimization(resource_type, strategy, parameters)

@router.get("/resource/metrics", response_model=DataResponse)
@require_permission("optimization:read")
async def get_resource_efficiency_metrics_route(
    days: Optional[int] = Query(30, description="Time range in days"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """
    API endpoint to retrieve resource efficiency metrics across the pipeline
    """
    return optimization_controller.get_resource_efficiency_metrics(days, resource_type)

@router.get("/config", response_model=OptimizationConfigResponse)
@require_permission("optimization:read")
async def get_optimization_config_route(
    current_user: dict = Depends(get_current_user)
) -> OptimizationConfigResponse:
    """
    API endpoint to retrieve the current optimization configuration
    """
    return optimization_controller.get_optimization_config()

@router.put("/config", response_model=OptimizationConfigResponse)
@require_permission("optimization:update")
async def update_optimization_config_route(
    config_data: OptimizationConfigUpdateRequest = Body(..., description="New configuration data"),
    current_user: dict = Depends(get_current_user)
) -> OptimizationConfigResponse:
    """
    API endpoint to update the optimization configuration
    """
    return optimization_controller.update_optimization_config(config_data)

@router.get("/dashboard", response_model=DataResponse)
@require_permission("optimization:read")
async def get_optimization_dashboard_data_route(
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """
    API endpoint to retrieve summary data for the optimization dashboard
    """
    return optimization_controller.get_optimization_dashboard_data()