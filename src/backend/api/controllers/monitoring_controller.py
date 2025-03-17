"""Controller module for monitoring and alerting functionality in the self-healing data pipeline.
This module implements the API endpoints defined in monitoring_routes.py, handling request validation, service layer interaction, and response formatting for metrics, alerts, anomalies, and dashboard data."""

from typing import List, Dict, Optional, Any, Tuple

from fastapi import APIRouter, Depends, HTTPException, status, Query, Request  # fastapi ^0.95.0
from fastapi.responses import JSONResponse

from ..services.monitoring_service import MonitoringService, get_metrics, get_metric_by_id, get_metric_time_series, get_alerts, get_alert_by_id, acknowledge_alert, resolve_alert, get_anomalies, get_anomaly_by_id, get_alert_config, update_alert_config, get_system_metrics, get_dashboard_summary
from ..models.request_models import PaginationParams, DateRangeParams, AlertConfigUpdateRequest, AlertAcknowledgeRequest
from ..models.response_models import PipelineMetricListResponse, PipelineMetricResponse, AlertListResponse, AlertResponse, MetricTimeSeriesResponse, AlertConfigResponse, DataResponse
from ..models.error_models import ResourceNotFoundError, ValidationError
from ..utils.response_utils import create_response_metadata, create_success_response, create_list_response
from ...utils.logging.logger import logger  # Logging functionality


# Create an APIRouter instance
router = APIRouter()


@router.get("/metrics", 
            response_model=PipelineMetricListResponse,
            summary="Retrieve a list of pipeline metrics",
            description="Retrieves a paginated list of pipeline metrics with optional filtering")
async def get_metrics_controller(
    pagination: PaginationParams = Depends(),
    metric_category: Optional[str] = Query(None, description="Filter by metric category"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID")
) -> PipelineMetricListResponse:
    """Controller function for retrieving a paginated list of pipeline metrics with optional filtering"""
    logger.info("Request received: Listing pipeline metrics")
    try:
        # Extract pagination parameters from the request
        page = pagination.page
        page_size = pagination.page_size
        
        # Call monitoring_service.get_metrics with pagination and filter parameters
        metrics, total_count = await get_metrics(page, page_size, metric_category, component, pipeline_id)
        
        # Create response metadata with pagination information
        metadata = create_response_metadata()
        
        # Create and return a PipelineMetricListResponse with the metrics data
        return create_list_response(
            items=metrics,
            page=page,
            page_size=page_size,
            total_items=total_count,
            request=Request,  # Assuming Request is available in this context
            message="Pipeline metrics retrieved successfully",
            request_id=metadata.request_id
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error listing pipeline metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/{metric_id}", 
            response_model=PipelineMetricResponse,
            summary="Retrieve a specific metric by ID",
            description="Retrieves details of a specific pipeline metric using its unique identifier")
async def get_metric_by_id_controller(metric_id: str) -> PipelineMetricResponse:
    """Controller function for retrieving a specific metric by ID"""
    logger.info(f"Request received: Retrieving metric with ID {metric_id}")
    try:
        # Call monitoring_service.get_metric_by_id with the metric_id
        metric = await get_metric_by_id(metric_id)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a PipelineMetricResponse with the metric details
        return create_success_response(
            data=metric,
            message="Metric details retrieved successfully",
            request_id=metadata.request_id
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent metrics
        logger.error(f"Metric not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error retrieving metric {metric_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/{metric_name}/timeseries", 
            response_model=MetricTimeSeriesResponse,
            summary="Retrieve time series data for a metric",
            description="Retrieves time series data for a specific metric over a given date range")
async def get_metric_time_series_controller(
    metric_name: str,
    date_range: DateRangeParams = Depends(),
    aggregation: Optional[str] = Query(None, description="Aggregation function (e.g., sum, avg, max)"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID")
) -> MetricTimeSeriesResponse:
    """Controller function for retrieving time series data for a specific metric"""
    logger.info(f"Request received: Retrieving time series for metric {metric_name}")
    try:
        # Extract date range parameters from the request
        start_date = date_range.start_date
        end_date = date_range.end_date
        
        # Call monitoring_service.get_metric_time_series with the parameters
        time_series_data = await get_metric_time_series(metric_name, start_date, end_date, aggregation, component, pipeline_id)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a MetricTimeSeriesResponse with the time series data
        return MetricTimeSeriesResponse(
            status=200,
            message="Metric time series data retrieved successfully",
            metadata=metadata,
            metric_name=metric_name,
            data_points=time_series_data
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent metrics
        logger.error(f"Metric not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationError as e:
        # Handle ValidationError for invalid parameters
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error retrieving metric time series for {metric_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts", 
            response_model=AlertListResponse,
            summary="Retrieve a list of alerts",
            description="Retrieves a paginated list of alerts with optional filtering")
async def get_alerts_controller(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    status: Optional[str] = Query(None, description="Filter by alert status"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID")
) -> AlertListResponse:
    """Controller function for retrieving a paginated list of alerts with optional filtering"""
    logger.info("Request received: Listing alerts")
    try:
        # Extract pagination and date range parameters from the request
        page = pagination.page
        page_size = pagination.page_size
        start_date = date_range.start_date
        end_date = date_range.end_date
        
        # Call monitoring_service.get_alerts with the parameters
        alerts, total_count = await get_alerts(page, page_size, start_date, end_date, severity, status, component, pipeline_id)
        
        # Create response metadata with pagination information
        metadata = create_response_metadata()
        
        # Create and return an AlertListResponse with the alerts data
        return create_list_response(
            items=alerts,
            page=page,
            page_size=page_size,
            total_items=total_count,
            request=Request,  # Assuming Request is available in this context
            message="Alerts retrieved successfully",
            request_id=metadata.request_id
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error listing alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts/{alert_id}", 
            response_model=AlertResponse,
            summary="Retrieve a specific alert by ID",
            description="Retrieves details of a specific alert using its unique identifier")
async def get_alert_by_id_controller(alert_id: str) -> AlertResponse:
    """Controller function for retrieving a specific alert by ID"""
    logger.info(f"Request received: Retrieving alert with ID {alert_id}")
    try:
        # Call monitoring_service.get_alert_by_id with the alert_id
        alert = await get_alert_by_id(alert_id)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return an AlertResponse with the alert details
        return create_success_response(
            data=alert,
            message="Alert details retrieved successfully",
            request_id=metadata.request_id
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent alerts
        logger.error(f"Alert not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error retrieving alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/acknowledge", 
            response_model=DataResponse,
            summary="Acknowledge an alert",
            description="Acknowledges a specific alert, indicating that it is being investigated")
async def acknowledge_alert_controller(alert_id: str, acknowledge_data: AlertAcknowledgeRequest, user_id: str = Query(..., description="User ID acknowledging the alert")) -> DataResponse:
    """Controller function for acknowledging an alert"""
    logger.info(f"Request received: Acknowledge alert with ID {alert_id}")
    try:
        # Extract acknowledgment details from request
        notes = acknowledge_data.notes
        
        # Call monitoring_service.acknowledge_alert with alert_id, user_id, and details
        success = await acknowledge_alert(alert_id, user_id, notes)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a DataResponse confirming acknowledgment
        return create_success_response(
            data={"acknowledged": success},
            message="Alert acknowledged successfully",
            request_id=metadata.request_id
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent alerts
        logger.error(f"Alert not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationError as e:
        # Handle ValidationError for invalid acknowledgment data
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error acknowledging alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/resolve", 
            response_model=DataResponse,
            summary="Resolve an alert",
            description="Resolves a specific alert, indicating that the underlying issue has been resolved")
async def resolve_alert_controller(alert_id: str, resolution_data: Dict[str, Any], user_id: str = Query(..., description="User ID resolving the alert")) -> DataResponse:
    """Controller function for resolving an alert"""
    logger.info(f"Request received: Resolve alert with ID {alert_id}")
    try:
        # Call monitoring_service.resolve_alert with alert_id, user_id, and details
        success = await resolve_alert(alert_id, resolution_data, user_id)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a DataResponse confirming resolution
        return create_success_response(
            data={"resolved": success},
            message="Alert resolved successfully",
            request_id=metadata.request_id
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent alerts
        logger.error(f"Alert not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationError as e:
        # Handle ValidationError for invalid resolution data
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error resolving alert {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies", 
            response_model=DataResponse,
            summary="Retrieve a list of detected anomalies",
            description="Retrieves a paginated list of detected anomalies with optional filtering")
async def get_anomalies_controller(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
    severity: Optional[str] = Query(None, description="Filter by anomaly severity"),
    component: Optional[str] = Query(None, description="Filter by component"),
    min_confidence: Optional[float] = Query(None, description="Filter by minimum confidence level")
) -> DataResponse:
    """Controller function for retrieving a paginated list of detected anomalies with optional filtering"""
    logger.info("Request received: Listing anomalies")
    try:
        # Extract pagination and date range parameters from the request
        page = pagination.page
        page_size = pagination.page_size
        start_date = date_range.start_date
        end_date = date_range.end_date
        
        # Call monitoring_service.get_anomalies with the parameters
        anomalies, total_count = await get_anomalies(page, page_size, start_date, end_date, metric_name, severity, component, min_confidence)
        
        # Create response metadata with pagination information
        metadata = create_response_metadata()
        
        # Create and return a DataResponse with the anomalies data
        return create_list_response(
            items=anomalies,
            page=page,
            page_size=page_size,
            total_items=total_count,
            request=Request,  # Assuming Request is available in this context
            message="Anomalies retrieved successfully",
            request_id=metadata.request_id
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error listing anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies/{anomaly_id}", 
            response_model=DataResponse,
            summary="Retrieve a specific anomaly by ID",
            description="Retrieves details of a specific anomaly using its unique identifier")
async def get_anomaly_by_id_controller(anomaly_id: str) -> DataResponse:
    """Controller function for retrieving a specific anomaly by ID"""
    logger.info(f"Request received: Retrieving anomaly with ID {anomaly_id}")
    try:
        # Call monitoring_service.get_anomaly_by_id with the anomaly_id
        anomaly = await get_anomaly_by_id(anomaly_id)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a DataResponse with the anomaly details
        return create_success_response(
            data=anomaly,
            message="Anomaly details retrieved successfully",
            request_id=metadata.request_id
        )
    except ResourceNotFoundError as e:
        # Handle ResourceNotFoundError for non-existent anomalies
        logger.error(f"Anomaly not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error retrieving anomaly {anomaly_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config", 
            response_model=AlertConfigResponse,
            summary="Retrieve the current alert configuration",
            description="Retrieves the current alert configuration settings")
async def get_alert_config_controller() -> AlertConfigResponse:
    """Controller function for retrieving the current alert configuration"""
    logger.info("Request received: Retrieving alert configuration")
    try:
        # Call monitoring_service.get_alert_config
        config_data = await get_alert_config()
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return an AlertConfigResponse with the configuration data
        return AlertConfigResponse(
            status=200,
            message="Alert configuration retrieved successfully",
            metadata=metadata,
            data=config_data
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error retrieving alert configuration: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/config", 
            response_model=AlertConfigResponse,
            summary="Update the alert configuration",
            description="Updates the alert configuration settings")
async def update_alert_config_controller(config_data: AlertConfigUpdateRequest) -> AlertConfigResponse:
    """Controller function for updating the alert configuration"""
    logger.info("Request received: Updating alert configuration")
    try:
        # Call monitoring_service.update_alert_config with the config_data
        updated_config = await update_alert_config(config_data)
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return an AlertConfigResponse with the updated configuration
        return AlertConfigResponse(
            status=200,
            message="Alert configuration updated successfully",
            metadata=metadata,
            data=updated_config
        )
    except ValidationError as e:
        # Handle ValidationError for invalid configuration data
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other exceptions and return appropriate error responses
        logger.error(f"Error updating alert configuration: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/metrics", 
            response_model=DataResponse,
            summary="Retrieve current system metrics",
            description="Retrieves current system-level metrics for monitoring")
async def get_system_metrics_controller() -> DataResponse:
    """Controller function for retrieving current system-level metrics for monitoring"""
    logger.info("Request received: Retrieving system metrics")
    try:
        # Call monitoring_service.get_system_metrics
        system_metrics = await get_system_metrics()
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a DataResponse with the system metrics
        return create_success_response(
            data=system_metrics,
            message="System metrics retrieved successfully",
            request_id=metadata.request_id
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error retrieving system metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/summary", 
            response_model=DataResponse,
            summary="Retrieve dashboard summary data",
            description="Retrieves a summary of monitoring data for the dashboard")
async def get_dashboard_summary_controller() -> DataResponse:
    """Controller function for retrieving a summary of monitoring data for the dashboard"""
    logger.info("Request received: Retrieving dashboard summary")
    try:
        # Call monitoring_service.get_dashboard_summary
        dashboard_summary = await get_dashboard_summary()
        
        # Create response metadata
        metadata = create_response_metadata()
        
        # Create and return a DataResponse with the dashboard summary data
        return create_success_response(
            data=dashboard_summary,
            message="Dashboard summary retrieved successfully",
            request_id=metadata.request_id
        )
    except Exception as e:
        # Handle exceptions and return appropriate error responses
        logger.error(f"Error retrieving dashboard summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))