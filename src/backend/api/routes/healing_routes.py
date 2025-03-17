"""
Defines FastAPI routes for self-healing operations in the data pipeline API.
This module maps HTTP endpoints to controller functions for managing healing patterns,
actions, executions, configuration, and manual healing triggers.
"""

from typing import Optional, Dict, Any, List
import uuid

from fastapi import APIRouter, Depends, Path, Query, Body, status, Request  # fastapi version: ^0.95.0
from fastapi.responses import JSONResponse
from datetime import datetime

# Import internal modules
from src.backend.api.controllers import healing_controller  # Controller functions for self-healing operations
from src.backend.api.models.request_models import (  # Request models for healing API endpoints
    PaginationParams,
    DateRangeParams,
    HealingPatternCreateRequest,
    HealingPatternUpdateRequest,
    HealingActionCreateRequest,
    HealingActionUpdateRequest,
    HealingConfigUpdateRequest,
    ManualHealingRequest
)
from src.backend.api.models.response_models import (  # Response models for healing API endpoints
    DataResponse,
    IssuePatternResponse,
    IssuePatternListResponse,
    HealingActionResponse,
    HealingActionListResponse,
    HealingExecutionResponse,
    HealingExecutionListResponse,
    HealingConfigResponse,
    ManualHealingResponse
)
from src.backend.api.utils.auth_utils import get_current_user, require_permissions  # Authentication and authorization utilities

# Create a router instance
router = APIRouter(prefix="/healing", tags=["Self-Healing"])


@router.get("/patterns", response_model=IssuePatternListResponse)
@require_permissions("healing:read")
async def get_healing_patterns_route(
    pagination: PaginationParams = Depends(),
    issue_type: Optional[str] = Query(None, description="Filter by issue type"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a paginated list of healing patterns with optional filtering"""
    # Call healing_controller.get_healing_patterns with the parameters
    patterns, pagination_metadata = healing_controller.get_healing_patterns(pagination.dict(), issue_type)
    # Construct the response
    items = [IssuePatternResponse(data=pattern) for pattern in patterns]
    return IssuePatternListResponse(items=items, pagination=pagination_metadata)


@router.get("/patterns/{pattern_id}", response_model=IssuePatternResponse)
@require_permissions("healing:read")
async def get_healing_pattern_by_id_route(
    pattern_id: uuid.UUID = Path(..., description="ID of the healing pattern"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a specific healing pattern by ID"""
    # Call healing_controller.get_healing_pattern_by_id with the pattern_id
    pattern = healing_controller.get_healing_pattern_by_id(str(pattern_id))
    # Return the response from the controller
    return IssuePatternResponse(data=pattern)


@router.post("/patterns", response_model=IssuePatternResponse, status_code=status.HTTP_201_CREATED)
@require_permissions("healing:create")
async def create_healing_pattern_route(
    pattern_data: HealingPatternCreateRequest = Body(..., description="Data for the new healing pattern"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to create a new healing pattern"""
    # Call healing_controller.create_healing_pattern with the pattern_data
    pattern = healing_controller.create_healing_pattern(pattern_data.dict())
    # Return the response from the controller with 201 Created status
    return IssuePatternResponse(data=pattern)


@router.put("/patterns/{pattern_id}", response_model=IssuePatternResponse)
@require_permissions("healing:update")
async def update_healing_pattern_route(
    pattern_id: uuid.UUID = Path(..., description="ID of the healing pattern to update"),
    pattern_data: HealingPatternUpdateRequest = Body(..., description="Data to update for the healing pattern"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to update an existing healing pattern"""
    # Call healing_controller.update_healing_pattern with the pattern_id and pattern_data
    pattern = healing_controller.update_healing_pattern(str(pattern_id), pattern_data.dict(exclude_unset=True))
    # Return the response from the controller
    return IssuePatternResponse(data=pattern)


@router.delete("/patterns/{pattern_id}", response_model=DataResponse)
@require_permissions("healing:delete")
async def delete_healing_pattern_route(
    pattern_id: uuid.UUID = Path(..., description="ID of the healing pattern to delete"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to delete a healing pattern"""
    # Call healing_controller.delete_healing_pattern with the pattern_id
    success = healing_controller.delete_healing_pattern(str(pattern_id))
    # Return the response from the controller
    return DataResponse(data={"deleted": success})


@router.get("/actions", response_model=HealingActionListResponse)
@require_permissions("healing:read")
async def get_healing_actions_route(
    pagination: PaginationParams = Depends(),
    pattern_id: Optional[str] = Query(None, description="Filter by pattern ID"),
    action_type: Optional[str] = Query(None, description="Filter by action type"),
    active_only: Optional[bool] = Query(None, description="Filter for active actions only"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a paginated list of healing actions with optional filtering"""
    # Call healing_controller.get_healing_actions with the parameters
    actions, pagination_metadata = healing_controller.get_healing_actions(pagination.dict(), pattern_id, action_type, active_only)
    # Return the response from the controller
    items = [HealingActionResponse(data=action) for action in actions]
    return HealingActionListResponse(items=items, pagination=pagination_metadata)


@router.get("/actions/{action_id}", response_model=HealingActionResponse)
@require_permissions("healing:read")
async def get_healing_action_by_id_route(
    action_id: uuid.UUID = Path(..., description="ID of the healing action"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a specific healing action by ID"""
    # Call healing_controller.get_healing_action_by_id with the action_id
    action = healing_controller.get_healing_action_by_id(str(action_id))
    # Return the response from the controller
    return HealingActionResponse(data=action)


@router.post("/actions", response_model=HealingActionResponse, status_code=status.HTTP_201_CREATED)
@require_permissions("healing:create")
async def create_healing_action_route(
    action_data: HealingActionCreateRequest = Body(..., description="Data for the new healing action"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to create a new healing action"""
    # Call healing_controller.create_healing_action with the action_data
    action = healing_controller.create_healing_action(action_data.dict())
    # Return the response from the controller with 201 Created status
    return HealingActionResponse(data=action)


@router.put("/actions/{action_id}", response_model=HealingActionResponse)
@require_permissions("healing:update")
async def update_healing_action_route(
    action_id: uuid.UUID = Path(..., description="ID of the healing action to update"),
    action_data: HealingActionUpdateRequest = Body(..., description="Data to update for the healing action"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to update an existing healing action"""
    # Call healing_controller.update_healing_action with the action_id and action_data
    action = healing_controller.update_healing_action(str(action_id), action_data.dict(exclude_unset=True))
    # Return the response from the controller
    return HealingActionResponse(data=action)


@router.delete("/actions/{action_id}", response_model=DataResponse)
@require_permissions("healing:delete")
async def delete_healing_action_route(
    action_id: uuid.UUID = Path(..., description="ID of the healing action to delete"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to delete a healing action"""
    # Call healing_controller.delete_healing_action with the action_id
    success = healing_controller.delete_healing_action(str(action_id))
    # Return the response from the controller
    return DataResponse(data={"deleted": success})


@router.get("/executions", response_model=HealingExecutionListResponse)
@require_permissions("healing:read")
async def get_healing_executions_route(
    pagination: PaginationParams = Depends(),
    date_range: DateRangeParams = Depends(),
    execution_id: Optional[str] = Query(None, description="Filter by execution ID"),
    pattern_id: Optional[str] = Query(None, description="Filter by pattern ID"),
    action_id: Optional[str] = Query(None, description="Filter by action ID"),
    successful_only: Optional[bool] = Query(None, description="Filter for successful executions only"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a paginated list of healing executions with optional filtering"""
    # Call healing_controller.get_healing_executions with the parameters
    executions, pagination_metadata = healing_controller.get_healing_executions(
        pagination.dict(), date_range.dict(), execution_id, pattern_id, action_id, successful_only
    )
    # Return the response from the controller
    items = [HealingExecutionResponse(data=execution) for execution in executions]
    return HealingExecutionListResponse(items=items, pagination=pagination_metadata)


@router.get("/executions/{healing_id}", response_model=HealingExecutionResponse)
@require_permissions("healing:read")
async def get_healing_execution_by_id_route(
    healing_id: uuid.UUID = Path(..., description="ID of the healing execution"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve a specific healing execution by ID"""
    # Call healing_controller.get_healing_execution_by_id with the healing_id
    execution = healing_controller.get_healing_execution_by_id(str(healing_id))
    # Return the response from the controller
    return HealingExecutionResponse(data=execution)


@router.get("/config", response_model=HealingConfigResponse)
@require_permissions("healing:read")
async def get_healing_config_route(
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve the current self-healing configuration"""
    # Call healing_controller.get_healing_config
    config = healing_controller.get_healing_config()
    # Return the response from the controller
    return HealingConfigResponse(data=config)


@router.put("/config", response_model=HealingConfigResponse)
@require_permissions("healing:update")
async def update_healing_config_route(
    config_data: HealingConfigUpdateRequest = Body(..., description="Data to update for the self-healing configuration"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to update the self-healing configuration"""
    # Call healing_controller.update_healing_config with the config_data
    config = healing_controller.update_healing_config(config_data.dict(exclude_unset=True))
    # Return the response from the controller
    return HealingConfigResponse(data=config)


@router.post("/execute", response_model=ManualHealingResponse)
@require_permissions("healing:execute")
async def execute_manual_healing_route(
    healing_request: ManualHealingRequest = Body(..., description="Request details for manual healing"),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to manually trigger a healing action for a specific issue"""
    # Call healing_controller.execute_manual_healing with the healing_request
    result = healing_controller.execute_manual_healing(healing_request.dict())
    # Return the response from the controller
    return ManualHealingResponse(**result)


@router.get("/statistics", response_model=DataResponse)
@require_permissions("healing:read")
async def get_healing_statistics_route(
    date_range: DateRangeParams = Depends(),
    current_user: dict = Depends(get_current_user)
):
    """API endpoint to retrieve statistics about self-healing operations"""
    # Call healing_controller.get_healing_statistics with the date_range
    statistics = healing_controller.get_healing_statistics(date_range.dict())
    # Return the response from the controller
    return DataResponse(data=statistics)