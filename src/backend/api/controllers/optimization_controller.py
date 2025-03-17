"""
Controller module for performance optimization operations in the self-healing data pipeline API.
This module handles requests for query optimization, schema optimization, resource optimization,
and optimization configuration management, serving as an intermediary between API routes
and the optimization service layer.
"""

from typing import List, Dict, Optional, Any, Union
from datetime import datetime
import uuid

# Import optimization service functions
from ..services.optimization_service import (
    get_query_optimization_recommendations,
    optimize_query,
    get_query_optimization_history,
    get_schema_optimization_recommendations,
    apply_schema_optimizations,
    get_schema_optimization_candidates,
    get_resource_optimization_recommendations,
    apply_resource_optimization,
    get_resource_efficiency_metrics,
    get_optimization_config,
    update_optimization_config
)

# Import request and response models
from ..models.request_models import OptimizationConfigUpdateRequest
from ..models.response_models import (
    DataResponse, 
    OptimizationConfigResponse, 
    OptimizationRecommendationResponse,
    OptimizationRecommendationListResponse,
    PaginatedResponse
)

# Import data models
from ..models.data_models import OptimizationConfig, OptimizationRecommendation

# Import error models
from ..models.error_models import ValidationError, ResourceNotFoundError

# Import logger
from ...utils.logging.logger import logger

def get_query_optimization_recommendations(query: str, optimization_techniques: Optional[List[str]] = None) -> OptimizationRecommendationResponse:
    """
    Retrieves optimization recommendations for a SQL query without modifying the query
    
    Args:
        query: The SQL query to analyze
        optimization_techniques: Optional specific optimization techniques to apply
        
    Returns:
        Response with query optimization recommendations
    """
    logger.info(f"Retrieving query optimization recommendations, techniques: {optimization_techniques}")
    
    # Validate that query is not empty
    if not query.strip():
        raise ValidationError("Query cannot be empty", [])
    
    # Call optimization_service.get_query_optimization_recommendations with the query and optimization_techniques
    recommendations = get_query_optimization_recommendations(query, optimization_techniques)
    
    # Create a unique recommendation_id using uuid
    recommendation_id = str(uuid.uuid4())
    
    # Create an OptimizationRecommendation object with the recommendations
    recommendation = OptimizationRecommendation(
        recommendation_id=recommendation_id,
        optimization_type="query",
        target_resource="bigquery_query",
        recommendations=recommendations,
        impact_assessment=recommendations.get("impact", {}),
        created_at=datetime.now()
    )
    
    # Return OptimizationRecommendationResponse with the recommendation data
    return OptimizationRecommendationResponse(
        data=recommendation
    )

def optimize_query(query: str, optimization_techniques: Optional[List[str]] = None, validate_results: bool = True) -> DataResponse:
    """
    Optimizes a SQL query and returns the optimized version with performance comparison
    
    Args:
        query: The SQL query to optimize
        optimization_techniques: Optional specific optimization techniques to apply
        validate_results: Whether to validate that optimized query returns same results
        
    Returns:
        Response with optimized query and performance metrics
    """
    logger.info(f"Optimizing query, techniques: {optimization_techniques}, validate_results: {validate_results}")
    
    # Validate that query is not empty
    if not query.strip():
        raise ValidationError("Query cannot be empty", [])
    
    # Set default value for validate_results if not provided
    validate_results = True if validate_results is None else validate_results
    
    # Call optimization_service.optimize_query with the query, optimization_techniques, and validate_results
    optimization_result = optimize_query(query, optimization_techniques, validate_results)
    
    # Return DataResponse with the optimization results
    return DataResponse(
        data=optimization_result
    )

def get_query_optimization_history(
    query_hash: Optional[str] = None, 
    start_date: Optional[datetime] = None, 
    end_date: Optional[datetime] = None,
    page: int = 1, 
    page_size: int = 20
) -> PaginatedResponse:
    """
    Retrieves optimization history for similar queries
    
    Args:
        query_hash: Optional hash to find similar queries
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering
        page: Page number for pagination
        page_size: Items per page
        
    Returns:
        Paginated response with query optimization history
    """
    logger.info(f"Retrieving query optimization history, query_hash: {query_hash}, page: {page}, page_size: {page_size}")
    
    # Call optimization_service.get_query_optimization_history with query_hash, start_date, end_date, page, and page_size
    history_result = get_query_optimization_history(query_hash, start_date, end_date, page, page_size)
    
    # Extract items and pagination metadata from the result
    items = history_result.get("items", [])
    pagination = history_result.get("pagination", {})
    
    # Return PaginatedResponse with the history items and pagination information
    return PaginatedResponse(
        items=items,
        pagination=pagination
    )

def get_schema_optimization_recommendations(dataset: str, table: str) -> OptimizationRecommendationResponse:
    """
    Analyzes a BigQuery table schema and provides optimization recommendations
    
    Args:
        dataset: The BigQuery dataset name
        table: The BigQuery table name
        
    Returns:
        Response with schema optimization recommendations
    """
    logger.info(f"Retrieving schema optimization recommendations for {dataset}.{table}")
    
    # Validate that dataset and table are not empty
    if not dataset.strip() or not table.strip():
        raise ValidationError("Dataset and table names cannot be empty", [])
    
    # Call optimization_service.get_schema_optimization_recommendations with dataset and table
    recommendations = get_schema_optimization_recommendations(dataset, table)
    
    # Create a unique recommendation_id using uuid
    recommendation_id = str(uuid.uuid4())
    
    # Create an OptimizationRecommendation object with the recommendations
    recommendation = OptimizationRecommendation(
        recommendation_id=recommendation_id,
        optimization_type="schema",
        target_resource=f"{dataset}.{table}",
        recommendations=recommendations,
        impact_assessment=recommendations.get("impact", {}),
        created_at=datetime.now()
    )
    
    # Return OptimizationRecommendationResponse with the recommendation data
    return OptimizationRecommendationResponse(
        data=recommendation
    )

def apply_schema_optimizations(dataset: str, table: str, optimizations: Dict[str, Any]) -> DataResponse:
    """
    Applies recommended schema optimizations to a BigQuery table
    
    Args:
        dataset: The BigQuery dataset name
        table: The BigQuery table name
        optimizations: Dictionary of optimizations to apply
        
    Returns:
        Response with the result of applying schema optimizations
    """
    logger.info(f"Applying schema optimizations to {dataset}.{table}")
    
    # Validate that dataset and table are not empty
    if not dataset.strip() or not table.strip():
        raise ValidationError("Dataset and table names cannot be empty", [])
    
    # Validate that optimizations is not empty
    if not optimizations:
        raise ValidationError("Optimizations cannot be empty", [])
    
    # Call optimization_service.apply_schema_optimizations with dataset, table, and optimizations
    result = apply_schema_optimizations(dataset, table, optimizations)
    
    # Return DataResponse with the optimization results
    return DataResponse(
        data=result
    )

def get_schema_optimization_candidates(
    dataset: Optional[str] = None, 
    min_table_size_gb: Optional[float] = None, 
    min_query_count: Optional[int] = None,
    page: int = 1, 
    page_size: int = 20
) -> PaginatedResponse:
    """
    Identifies tables that would benefit from schema optimization
    
    Args:
        dataset: Optional dataset to filter by
        min_table_size_gb: Minimum table size in GB to consider
        min_query_count: Minimum query count to consider
        page: Page number for pagination
        page_size: Items per page
        
    Returns:
        Paginated response with schema optimization candidates
    """
    logger.info(f"Retrieving schema optimization candidates, dataset: {dataset}, page: {page}, page_size: {page_size}")
    
    # Call optimization_service.get_schema_optimization_candidates with dataset, min_table_size_gb, min_query_count, page, and page_size
    candidates_result = get_schema_optimization_candidates(dataset, min_table_size_gb, min_query_count, page, page_size)
    
    # Extract items and pagination metadata from the result
    items = candidates_result.get("items", [])
    pagination = candidates_result.get("pagination", {})
    
    # Return PaginatedResponse with the candidate items and pagination information
    return PaginatedResponse(
        items=items,
        pagination=pagination
    )

def get_resource_optimization_recommendations(resource_type: Optional[str] = None) -> OptimizationRecommendationListResponse:
    """
    Retrieves resource optimization recommendations across the pipeline
    
    Args:
        resource_type: Optional type of resource to filter by
        
    Returns:
        Response with list of resource optimization recommendations
    """
    logger.info(f"Retrieving resource optimization recommendations, resource_type: {resource_type}")
    
    # Call optimization_service.get_resource_optimization_recommendations with resource_type
    recommendations_data = get_resource_optimization_recommendations(resource_type)
    
    # Create OptimizationRecommendation objects for each recommendation
    recommendations = []
    for rec in recommendations_data:
        recommendation = OptimizationRecommendation(
            recommendation_id=rec.get("id", str(uuid.uuid4())),
            optimization_type="resource",
            target_resource=rec.get("resource_type", "unknown"),
            recommendations=rec.get("recommendations", []),
            impact_assessment=rec.get("impact", {}),
            created_at=rec.get("created_at", datetime.now())
        )
        recommendations.append(recommendation)
    
    # Create a pagination object for the response
    total_items = len(recommendations)
    pagination = {
        "page": 1,
        "page_size": total_items,
        "total_items": total_items,
        "total_pages": 1
    }
    
    # Return OptimizationRecommendationListResponse with the recommendations
    return OptimizationRecommendationListResponse(
        items=recommendations,
        pagination=pagination
    )

def apply_resource_optimization(resource_type: str, strategy: str, parameters: Dict[str, Any]) -> DataResponse:
    """
    Applies a recommended resource optimization
    
    Args:
        resource_type: Type of resource to optimize
        strategy: Optimization strategy to apply
        parameters: Parameters for the optimization
        
    Returns:
        Response with the result of applying resource optimization
    """
    logger.info(f"Applying resource optimization for {resource_type}, strategy: {strategy}")
    
    # Validate that resource_type and strategy are not empty
    if not resource_type.strip() or not strategy.strip():
        raise ValidationError("Resource type and strategy cannot be empty", [])
    
    # Validate that parameters is not empty
    if not parameters:
        raise ValidationError("Parameters cannot be empty", [])
    
    # Call optimization_service.apply_resource_optimization with resource_type, strategy, and parameters
    result = apply_resource_optimization(resource_type, strategy, parameters)
    
    # Return DataResponse with the optimization results
    return DataResponse(
        data=result
    )

def get_resource_efficiency_metrics(days: Optional[int] = 30, resource_type: Optional[str] = None) -> DataResponse:
    """
    Retrieves resource efficiency metrics across the pipeline
    
    Args:
        days: Number of days to include in metrics
        resource_type: Optional type of resource to filter by
        
    Returns:
        Response with resource efficiency metrics
    """
    logger.info(f"Retrieving resource efficiency metrics, days: {days}, resource_type: {resource_type}")
    
    # Call optimization_service.get_resource_efficiency_metrics with days and resource_type
    metrics = get_resource_efficiency_metrics(days, resource_type)
    
    # Return DataResponse with the efficiency metrics
    return DataResponse(
        data=metrics
    )

def get_optimization_config() -> OptimizationConfigResponse:
    """
    Retrieves the current optimization configuration
    
    Returns:
        Response with current optimization configuration
    """
    logger.info("Retrieving optimization configuration")
    
    # Call optimization_service.get_optimization_config
    config_data = get_optimization_config()
    
    # Create an OptimizationConfig object with the configuration data
    config = OptimizationConfig(
        query_optimization_settings=config_data.get("query_optimization_settings", {}),
        schema_optimization_settings=config_data.get("schema_optimization_settings", {}),
        resource_optimization_settings=config_data.get("resource_optimization_settings", {}),
        auto_implementation_enabled=config_data.get("auto_implementation_enabled", False)
    )
    
    # Return OptimizationConfigResponse with the configuration
    return OptimizationConfigResponse(
        data=config
    )

def update_optimization_config(config_data: OptimizationConfigUpdateRequest) -> OptimizationConfigResponse:
    """
    Updates the optimization configuration
    
    Args:
        config_data: New configuration data
        
    Returns:
        Response with updated optimization configuration
    """
    logger.info("Updating optimization configuration")
    
    # Extract configuration data from the request model
    config_dict = {
        "query_optimization_settings": config_data.query_optimization_settings,
        "schema_optimization_settings": config_data.schema_optimization_settings,
        "resource_optimization_settings": config_data.resource_optimization_settings,
        "auto_implementation_enabled": config_data.auto_implementation_enabled
    }
    
    # Call optimization_service.update_optimization_config with the configuration data
    updated_config = update_optimization_config(config_dict)
    
    # Create an OptimizationConfig object with the updated configuration
    config = OptimizationConfig(
        query_optimization_settings=updated_config.get("query_optimization_settings", {}),
        schema_optimization_settings=updated_config.get("schema_optimization_settings", {}),
        resource_optimization_settings=updated_config.get("resource_optimization_settings", {}),
        auto_implementation_enabled=updated_config.get("auto_implementation_enabled", False)
    )
    
    # Return OptimizationConfigResponse with the updated configuration
    return OptimizationConfigResponse(
        data=config
    )

def get_optimization_dashboard_data() -> DataResponse:
    """
    Retrieves summary data for the optimization dashboard
    
    Returns:
        Response with dashboard summary data
    """
    logger.info("Retrieving optimization dashboard data")
    
    # Get optimization configuration from optimization_service.get_optimization_config
    config = get_optimization_config()
    
    # Get resource efficiency metrics from optimization_service.get_resource_efficiency_metrics
    efficiency_metrics = get_resource_efficiency_metrics(7)
    
    # Get recent optimization recommendations from various optimization services
    query_recommendations = get_resource_optimization_recommendations("bigquery_query")
    schema_recommendations = get_resource_optimization_recommendations("bigquery_schema")
    resource_recommendations = get_resource_optimization_recommendations()
    
    # Combine recommendations and sort by recency
    all_recommendations = []
    for rec_list in [query_recommendations.items, schema_recommendations.items, resource_recommendations.items]:
        all_recommendations.extend(rec_list)
    
    # Sort by created_at in descending order
    all_recommendations.sort(key=lambda x: getattr(x, "created_at", datetime.now()), reverse=True)
    recent_recommendations = all_recommendations[:5]  # Take top 5
    
    # Compile dashboard data including optimization stats, recent recommendations, and efficiency metrics
    dashboard_data = {
        "optimization_stats": {
            "queries_optimized_count": efficiency_metrics.get("queries_optimized_count", 0),
            "schemas_optimized_count": efficiency_metrics.get("schemas_optimized_count", 0),
            "resources_optimized_count": efficiency_metrics.get("resources_optimized_count", 0),
            "cost_savings": efficiency_metrics.get("cost_savings", 0)
        },
        "recent_recommendations": recent_recommendations,
        "efficiency_metrics": efficiency_metrics,
        "config_summary": {
            "auto_implementation": config.get("auto_implementation_enabled", False),
            "query_optimization_enabled": len(config.get("query_optimization_settings", {}).get("enabled_techniques", [])) > 0,
            "schema_optimization_enabled": config.get("schema_optimization_settings", {}).get("enabled", False),
            "resource_optimization_enabled": config.get("resource_optimization_settings", {}).get("enabled", False)
        }
    }
    
    # Return DataResponse with the compiled dashboard data
    return DataResponse(
        data=dashboard_data
    )