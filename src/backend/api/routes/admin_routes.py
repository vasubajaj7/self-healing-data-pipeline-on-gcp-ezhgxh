from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime

from fastapi import APIRouter, Depends, Path, Query, Body, status, Request  # fastapi: ^0.95.0
from fastapi.security import OAuth2PasswordRequestForm
from typing import Optional, Dict, Any, List  # typing: standard library
import uuid  # uuid: standard library
from datetime import datetime  # datetime: standard library

from ..controllers import admin_controller  # Internal import: Controller functions for administrative operations
from ..models.request_models import PaginationParams, DateRangeParams  # Internal import: Request models for pagination and date filtering
from ..models.response_models import DataResponse, PaginatedResponse, HealthCheckResponse  # Internal import: Response models for API endpoints
from ..utils.auth_utils import get_current_user, require_permission  # Internal import: Authentication and authorization utilities

# Define FastAPI router for administration endpoints
router = APIRouter(prefix="/admin", tags=["Administration"])

@router.post("/login", response_model=DataResponse)
def login_route(form_data: OAuth2PasswordRequestForm = Depends()):
    """API endpoint for user authentication and token generation

    Args:
        form_data (OAuth2PasswordRequestForm, optional): User credentials. Defaults to Depends().

    Returns:
        DataResponse: Authentication result with token
    """
    # Define route with POST method and path '/login'
    # Add username_or_email form parameter for user identification
    # Add password form parameter for user authentication
    # Call admin_controller.authenticate_user with the credentials
    auth_result = admin_controller.authenticate_user(username_or_email=form_data.username, password=form_data.password)
    # Return the response from the controller with authentication token
    return DataResponse(data=auth_result)

@router.get("/users", response_model=PaginatedResponse)
def get_users_route(pagination: PaginationParams = Depends(),
                    search: Optional[str] = Query(None, description="Search by username or email"),
                    role: Optional[str] = Query(None, description="Filter by role"),
                    active_only: Optional[bool] = Query(None, description="Filter active users only"),
                    current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve a paginated list of users with optional filtering"""
    # Define route with GET method and path '/users'
    # Add pagination dependency for page and page_size
    # Add search query parameter for filtering by username or email
    # Add role query parameter for filtering by role
    # Add active_only query parameter for filtering by active status
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_users with the parameters
    users = admin_controller.get_users(page=pagination.page, page_size=pagination.page_size, search=search, role=role, active_only=active_only)
    # Return the response from the controller
    return users

@router.get("/users/{user_id}", response_model=DataResponse)
def get_user_by_id_route(user_id: UUID = Path(..., title="User ID"),
                         current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve a specific user by ID"""
    # Define route with GET method and path '/users/{user_id}'
    # Add user_id path parameter with UUID validation
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_user_by_id with the user_id
    user = admin_controller.get_user_by_id(user_id=str(user_id))
    # Return the response from the controller
    return user

@router.post("/users", response_model=DataResponse, status_code=status.HTTP_201_CREATED)
def create_user_route(user_data: Dict[str, Any] = Body(..., description="User data for creation"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to create a new user"""
    # Define route with POST method and path '/users'
    # Add user_data body parameter for user creation data
    # Add current_user dependency with admin:create permission requirement
    require_permission("admin:create")(current_user)
    # Call admin_controller.create_user with the user_data
    created_user = admin_controller.create_user(user_data=user_data)
    # Return the response from the controller with 201 Created status
    return created_user

@router.put("/users/{user_id}", response_model=DataResponse)
def update_user_route(user_id: UUID = Path(..., title="User ID"),
                      user_data: Dict[str, Any] = Body(..., description="User data for update"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to update an existing user"""
    # Define route with PUT method and path '/users/{user_id}'
    # Add user_id path parameter with UUID validation
    # Add user_data body parameter for user update data
    # Add current_user dependency with admin:update permission requirement
    require_permission("admin:update")(current_user)
    # Call admin_controller.update_user with the user_id and user_data
    updated_user = admin_controller.update_user(user_id=str(user_id), user_data=user_data)
    # Return the response from the controller
    return updated_user

@router.delete("/users/{user_id}", response_model=DataResponse)
def delete_user_route(user_id: UUID = Path(..., title="User ID"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to delete a user"""
    # Define route with DELETE method and path '/users/{user_id}'
    # Add user_id path parameter with UUID validation
    # Add current_user dependency with admin:delete permission requirement
    require_permission("admin:delete")(current_user)
    # Call admin_controller.delete_user with the user_id
    result = admin_controller.delete_user(user_id=str(user_id))
    # Return the response from the controller
    return result

@router.get("/roles", response_model=DataResponse)
def get_roles_route(current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve a list of all roles"""
    # Define route with GET method and path '/roles'
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_roles
    roles = admin_controller.get_roles()
    # Return the response from the controller
    return roles

@router.get("/roles/{role_id}", response_model=DataResponse)
def get_role_by_id_route(role_id: UUID = Path(..., title="Role ID"),
                         current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve a specific role by ID"""
    # Define route with GET method and path '/roles/{role_id}'
    # Add role_id path parameter with UUID validation
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_role_by_id with the role_id
    role = admin_controller.get_role_by_id(role_id=str(role_id))
    # Return the response from the controller
    return role

@router.post("/roles", response_model=DataResponse, status_code=status.HTTP_201_CREATED)
def create_role_route(role_data: Dict[str, Any] = Body(..., description="Role data for creation"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to create a new role"""
    # Define route with POST method and path '/roles'
    # Add role_data body parameter for role creation data
    # Add current_user dependency with admin:create permission requirement
    require_permission("admin:create")(current_user)
    # Call admin_controller.create_role with the role_data
    created_role = admin_controller.create_role(role_data=role_data)
    # Return the response from the controller with 201 Created status
    return created_role

@router.put("/roles/{role_id}", response_model=DataResponse)
def update_role_route(role_id: UUID = Path(..., title="Role ID"),
                      role_data: Dict[str, Any] = Body(..., description="Role data for update"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to update an existing role"""
    # Define route with PUT method and path '/roles/{role_id}'
    # Add role_id path parameter with UUID validation
    # Add role_data body parameter for role update data
    # Add current_user dependency with admin:update permission requirement
    require_permission("admin:update")(current_user)
    # Call admin_controller.update_role with the role_id and role_data
    updated_role = admin_controller.update_role(role_id=str(role_id), role_data=role_data)
    # Return the response from the controller
    return updated_role

@router.delete("/roles/{role_id}", response_model=DataResponse)
def delete_role_route(role_id: UUID = Path(..., title="Role ID"),
                      current_user: dict = Depends(get_current_user)):
    """API endpoint to delete a role"""
    # Define route with DELETE method and path '/roles/{role_id}'
    # Add role_id path parameter with UUID validation
    # Add current_user dependency with admin:delete permission requirement
    require_permission("admin:delete")(current_user)
    # Call admin_controller.delete_role with the role_id
    result = admin_controller.delete_role(role_id=str(role_id))
    # Return the response from the controller
    return result

@router.get("/settings", response_model=DataResponse)
def get_system_settings_route(current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve the current system settings"""
    # Define route with GET method and path '/settings'
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_system_settings
    settings = admin_controller.get_system_settings()
    # Return the response from the controller
    return settings

@router.put("/settings", response_model=DataResponse)
def update_system_settings_route(settings_data: Dict[str, Any] = Body(..., description="Settings data for update"),
                                 current_user: dict = Depends(get_current_user)):
    """API endpoint to update the system settings"""
    # Define route with PUT method and path '/settings'
    # Add settings_data body parameter for settings update data
    # Add current_user dependency with admin:update permission requirement
    require_permission("admin:update")(current_user)
    # Call admin_controller.update_system_settings with the settings_data
    updated_settings = admin_controller.update_system_settings(settings_data=settings_data)
    # Return the response from the controller
    return updated_settings

@router.get("/health", response_model=HealthCheckResponse)
def get_system_health_route(current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve the current system health status"""
    # Define route with GET method and path '/health'
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_system_health
    health = admin_controller.get_system_health()
    # Return the response from the controller
    return health

@router.get("/health/{component_name}", response_model=DataResponse)
def check_component_status_route(component_name: str = Path(..., title="Component Name"),
                                 current_user: dict = Depends(get_current_user)):
    """API endpoint to check the status of a specific system component"""
    # Define route with GET method and path '/health/{component_name}'
    # Add component_name path parameter for the component to check
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.check_component_status with the component_name
    component_status = admin_controller.check_component_status(component_name=component_name)
    # Return the response from the controller
    return component_status

@router.get("/audit-logs", response_model=PaginatedResponse)
def get_audit_logs_route(pagination: PaginationParams = Depends(),
                         date_range: DateRangeParams = Depends(),
                         user_id: Optional[str] = Query(None, description="Filter by user ID"),
                         action_type: Optional[str] = Query(None, description="Filter by action type"),
                         resource_type: Optional[str] = Query(None, description="Filter by resource type"),
                         current_user: dict = Depends(get_current_user)):
    """API endpoint to retrieve system audit logs with filtering options"""
    # Define route with GET method and path '/audit-logs'
    # Add pagination dependency for page and page_size
    # Add date_range dependency for start_date and end_date
    # Add user_id query parameter for filtering by user
    # Add action_type query parameter for filtering by action type
    # Add resource_type query parameter for filtering by resource type
    # Add current_user dependency with admin:read permission requirement
    require_permission("admin:read")(current_user)
    # Call admin_controller.get_audit_logs with the parameters
    audit_logs = admin_controller.get_audit_logs(page=pagination.page, page_size=pagination.page_size, start_date=date_range.start_date, end_date=date_range.end_date, user_id=user_id, action_type=action_type, resource_type=resource_type)
    # Return the response from the controller
    return audit_logs