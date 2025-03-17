# src/backend/api/routes/monitoring_routes.py
"""Defines FastAPI routes for monitoring and alerting operations in the self-healing data pipeline API.
This module maps HTTP endpoints to controller functions for retrieving metrics, alerts, anomalies, and dashboard data, as well as managing alert configurations and notifications.
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Body, status, Request  # fastapi ^0.95.0
from fastapi.responses import JSONResponse

from ..controllers import monitoring_controller
from ..models.request_models import PaginationParams, DateRangeParams, AlertConfigUpdateRequest, AlertAcknowledgeRequest
from ..models.response_models import DataResponse, PipelineMetricResponse, PipelineMetricListResponse, AlertResponse, AlertListResponse, MetricTimeSeriesResponse, AlertConfigResponse
from ..utils.auth_utils import get_current_user, require_permission

# Create an APIRouter instance
router = APIRouter(prefix='/monitoring', tags=['Monitoring & Alerting'])


@router.get('/metrics', response_model=PipelineMetricListResponse)
@require_permission("monitoring:read")
async def get_metrics_route(
    pagination: PaginationParams = Depends(),
    metric_category: Optional[str] = Query(None, description="Filter by metric category"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    current_user: dict = Depends(get_current_user)
) -> PipelineMetricListResponse:
    """API endpoint to retrieve a paginated list of pipeline metrics with optional filtering"""
    return await monitoring_controller.get_metrics_controller(pagination, metric_category, component, pipeline_id)


@router.get('/metrics/{metric_id}', response_model=PipelineMetricResponse)
@require_permission("monitoring:read")
async def get_metric_by_id_route(
    metric_id: UUID = Path(..., title="The ID of the metric to get"),
    current_user: dict = Depends(get_current_user)
) -> PipelineMetricResponse:
    """API endpoint to retrieve a specific metric by ID"""
    return await monitoring_controller.get_metric_by_id_controller(str(metric_id))


@router.get('/metrics/timeseries/{metric_name}', response_model=MetricTimeSeriesResponse)
@require_permission("monitoring:read")
async def get_metric_time_series_route(
    metric_name: str = Path(..., title="The name of the metric to get time series data for"),
    date_range: DateRangeParams = Depends(),
    aggregation: Optional[str] = Query(None, description="Aggregation function (e.g., sum, avg, max)"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    current_user: dict = Depends(get_current_user)
) -> MetricTimeSeriesResponse:
    """API endpoint to retrieve time series data for a specific metric"""
    return await monitoring_controller.get_metric_time_series_controller(metric_name, date_range, aggregation, component, pipeline_id)


@router.get('/alerts', response_model=AlertListResponse)
@require_permission("monitoring:read")
async def get_alerts_route(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    severity: Optional[str] = Query(None, description="Filter by alert severity"),
    status: Optional[str] = Query(None, description="Filter by alert status"),
    component: Optional[str] = Query(None, description="Filter by component"),
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    current_user: dict = Depends(get_current_user)
) -> AlertListResponse:
    """API endpoint to retrieve a paginated list of alerts with optional filtering"""
    return await monitoring_controller.get_alerts_controller(pagination, date_range, severity, status, component, pipeline_id)


@router.get('/alerts/{alert_id}', response_model=AlertResponse)
@require_permission("monitoring:read")
async def get_alert_by_id_route(
    alert_id: UUID = Path(..., title="The ID of the alert to get"),
    current_user: dict = Depends(get_current_user)
) -> AlertResponse:
    """API endpoint to retrieve a specific alert by ID"""
    return await monitoring_controller.get_alert_by_id_controller(str(alert_id))


@router.post('/alerts/{alert_id}/acknowledge', response_model=DataResponse)
@require_permission("monitoring:update")
async def acknowledge_alert_route(
    alert_id: UUID = Path(..., title="The ID of the alert to acknowledge"),
    acknowledge_data: AlertAcknowledgeRequest = Body(..., description="Acknowledgment details"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to acknowledge an alert"""
    user_id = current_user.get("username")
    return await monitoring_controller.acknowledge_alert_controller(str(alert_id), acknowledge_data, user_id)


@router.post('/alerts/{alert_id}/resolve', response_model=DataResponse)
@require_permission("monitoring:update")
async def resolve_alert_route(
    alert_id: UUID = Path(..., title="The ID of the alert to resolve"),
    resolution_data: Dict[str, Any] = Body(..., description="Resolution details"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to resolve an alert"""
    user_id = current_user.get("username")
    return await monitoring_controller.resolve_alert_controller(str(alert_id), resolution_data, user_id)


@router.get('/anomalies', response_model=DataResponse)
@require_permission("monitoring:read")
async def get_anomalies_route(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
    severity: Optional[str] = Query(None, description="Filter by anomaly severity"),
    component: Optional[str] = Query(None, description="Filter by component"),
    min_confidence: Optional[float] = Query(None, description="Filter by minimum confidence level"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to retrieve a paginated list of detected anomalies with optional filtering"""
    return await monitoring_controller.get_anomalies_controller(pagination, date_range, metric_name, severity, component, min_confidence)


@router.get('/anomalies/{anomaly_id}', response_model=DataResponse)
@require_permission("monitoring:read")
async def get_anomaly_by_id_route(
    anomaly_id: UUID = Path(..., title="The ID of the anomaly to get"),
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to retrieve a specific anomaly by ID"""
    return await monitoring_controller.get_anomaly_by_id_controller(str(anomaly_id))


@router.get('/config/alerts', response_model=AlertConfigResponse)
@require_permission("monitoring:read")
async def get_alert_config_route(
    current_user: dict = Depends(get_current_user)
) -> AlertConfigResponse:
    """API endpoint to retrieve the current alert configuration"""
    return await monitoring_controller.get_alert_config_controller()


@router.put('/config/alerts', response_model=AlertConfigResponse)
@require_permission("monitoring:admin")
async def update_alert_config_route(
    config_data: AlertConfigUpdateRequest = Body(..., description="Alert configuration data"),
    current_user: dict = Depends(get_current_user)
) -> AlertConfigResponse:
    """API endpoint to update the alert configuration"""
    return await monitoring_controller.update_alert_config_controller(config_data)


@router.get('/system', response_model=DataResponse)
@require_permission("monitoring:read")
async def get_system_metrics_route(
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to retrieve current system-level metrics for monitoring"""
    return await monitoring_controller.get_system_metrics_controller()


@router.get('/dashboard', response_model=DataResponse)
@require_permission("monitoring:read")
async def get_dashboard_summary_route(
    current_user: dict = Depends(get_current_user)
) -> DataResponse:
    """API endpoint to retrieve a summary of monitoring data for the dashboard"""
    return await monitoring_controller.get_dashboard_summary_controller()