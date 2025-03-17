import typing
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import uuid

import bcrypt  # version ^3.2.0
import jwt  # version ^2.3.0

from google.cloud import monitoring_v3  # version ^2.9.0

from ..models.error_models import (
    ResourceNotFoundError, ValidationError, AuthorizationError, ConfigurationError, SystemError
)
from ...utils.logging.logger import logger
from ...config import config
from ...utils.storage import firestore_client
from ...utils.storage import bigquery_client
from ...utils.auth import gcp_auth
from ...utils.config import secret_manager
from ...utils.monitoring import metric_client

# Constants for Firestore collections and BigQuery table
USER_COLLECTION = "users"
ROLE_COLLECTION = "roles"
SETTINGS_COLLECTION = "system_settings"
AUDIT_LOG_TABLE = "audit_logs"
TOKEN_EXPIRY_MINUTES = 60


def get_users(page: int, page_size: int, search: Optional[str] = None,
              role: Optional[str] = None, active_only: Optional[bool] = None) -> Dict[str, Any]:
    """Retrieves a paginated list of users with optional filtering

    Args:
        page (int): Page number for pagination
        page_size (int): Number of users per page
        search (Optional[str], optional): Search term for username or email. Defaults to None.
        role (Optional[str], optional): Filter by role ID. Defaults to None.
        active_only (Optional[bool], optional): Filter for active users only. Defaults to None.

    Returns:
        Dict[str, Any]: Paginated list of users with total count
    """
    logger.info(f"Listing users: page={page}, page_size={page_size}, search={search}, role={role}, active_only={active_only}")

    db = firestore_client.get_firestore_client()
    query = db.collection(USER_COLLECTION)

    if search:
        query = query.where("username", ">=", search).where("username", "<=", search + "\uf8ff")

    if role:
        query = query.where("role", "==", role)

    if active_only is not None:
        query = query.where("is_active", "==", active_only)

    skip = (page - 1) * page_size
    limit = page_size

    users = []
    total_count = 0

    try:
        # Get total count before pagination
        count_query = query  # Copy the query before pagination
        total_count = len(list(count_query.stream()))

        # Apply pagination
        query = query.offset(skip).limit(limit)
        results = query.stream()

        for doc in results:
            user_data = doc.to_dict()
            user_data["user_id"] = doc.id
            user_data.pop("password_hash", None)  # Exclude password hash
            users.append(user_data)

    except Exception as e:
        logger.error(f"Error listing users: {e}")
        raise

    return {"users": users, "total_count": total_count, "page": page, "page_size": page_size}


def get_user_by_id(user_id: str) -> Dict[str, Any]:
    """Retrieves a specific user by ID

    Args:
        user_id (str): User ID

    Returns:
        Dict[str, Any]: User details
    """
    logger.info(f"Getting user by ID: {user_id}")

    db = firestore_client.get_firestore_client()
    user_ref = db.collection(USER_COLLECTION).document(user_id)
    user = user_ref.get()

    if not user.exists:
        raise ResourceNotFoundError("User", user_id)

    user_data = user.to_dict()
    user_data["user_id"] = user.id
    user_data.pop("password_hash", None)  # Exclude password hash

    return user_data


def create_user(user_data: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new user in the system

    Args:
        user_data (Dict[str, Any]): User data

    Returns:
        Dict[str, Any]: Created user details
    """
    logger.info(f"Creating new user: {user_data}")

    required_fields = ["username", "email", "password", "role"]
    if not all(field in user_data for field in required_fields):
        raise ValidationError("Missing required fields", [{"field": field, "message": "This field is required"}
                                                        for field in required_fields if field not in user_data])

    db = firestore_client.get_firestore_client()

    # Check if username or email already exists
    username = user_data["username"]
    email = user_data["email"]
    existing_user = next((doc for doc in db.collection(USER_COLLECTION).where("username", "==", username).stream()), None)
    if existing_user:
        raise ValidationError("Username already exists", [{"field": "username", "message": "Username already exists"}])
    existing_email = next((doc for doc in db.collection(USER_COLLECTION).where("email", "==", email).stream()), None)
    if existing_email:
        raise ValidationError("Email already exists", [{"field": "email", "message": "Email already exists"}])

    # Verify that specified role exists
    role_id = user_data["role"]
    role_ref = db.collection(ROLE_COLLECTION).document(role_id)
    role = role_ref.get()
    if not role.exists:
        raise ValidationError("Invalid role", [{"field": "role", "message": "Invalid role"}])

    # Hash the password
    password = user_data.pop("password")
    hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    # Generate user_id
    user_id = str(uuid.uuid4())

    # Create user document
    user_ref = db.collection(USER_COLLECTION).document(user_id)
    user_data["password_hash"] = hashed_password
    user_data["created_at"] = datetime.utcnow()
    user_data["is_active"] = True  # Default to active
    user_ref.set(user_data)

    log_audit_event(user_id="system", action_type="create", resource_type="user", resource_id=user_id, details=user_data)

    user_data["user_id"] = user_id
    user_data.pop("password_hash", None)  # Exclude password hash
    return user_data


def update_user(user_id: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
    """Updates an existing user's information

    Args:
        user_id (str): User ID
        user_data (Dict[str, Any]): User data to update

    Returns:
        Dict[str, Any]: Updated user details
    """
    logger.info(f"Updating user: user_id={user_id}, data={user_data}")

    db = firestore_client.get_firestore_client()
    user_ref = db.collection(USER_COLLECTION).document(user_id)
    user = user_ref.get()

    if not user.exists:
        raise ResourceNotFoundError("User", user_id)

    # Check for username or email conflicts
    if "username" in user_data:
        username = user_data["username"]
        existing_user = next((doc for doc in db.collection(USER_COLLECTION).where("username", "==", username).where(
            "__name__", "!=", user_ref.id).stream()), None)
        if existing_user:
            raise ValidationError("Username already exists", [{"field": "username", "message": "Username already exists"}])

    if "email" in user_data:
        email = user_data["email"]
        existing_email = next((doc for doc in db.collection(USER_COLLECTION).where("email", "==", email).where(
            "__name__", "!=", user_ref.id).stream()), None)
        if existing_email:
            raise ValidationError("Email already exists", [{"field": "email", "message": "Email already exists"}])

    # Verify that specified role exists
    if "role" in user_data:
        role_id = user_data["role"]
        role_ref = db.collection(ROLE_COLLECTION).document(role_id)
        role = role_ref.get()
        if not role.exists:
            raise ValidationError("Invalid role", [{"field": "role", "message": "Invalid role"}])

    # Hash the password if updating
    if "password" in user_data:
        password = user_data.pop("password")
        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        user_data["password_hash"] = hashed_password

    user_data["updated_at"] = datetime.utcnow()
    user_ref.update(user_data)

    log_audit_event(user_id="system", action_type="update", resource_type="user", resource_id=user_id, details=user_data)

    updated_user = user_ref.get().to_dict()
    updated_user["user_id"] = user_id
    updated_user.pop("password_hash", None)  # Exclude password hash
    return updated_user


def delete_user(user_id: str) -> Dict[str, Any]:
    """Deletes a user from the system

    Args:
        user_id (str): User ID

    Returns:
        Dict[str, Any]: Deletion confirmation
    """
    logger.info(f"Deleting user: {user_id}")

    db = firestore_client.get_firestore_client()
    user_ref = db.collection(USER_COLLECTION).document(user_id)
    user = user_ref.get()

    if not user.exists:
        raise ResourceNotFoundError("User", user_id)

    user_ref.delete()

    log_audit_event(user_id="system", action_type="delete", resource_type="user", resource_id=user_id)

    return {"message": f"User {user_id} deleted successfully"}


def get_roles() -> Dict[str, Any]:
    """Retrieves a list of all roles in the system

    Returns:
        Dict[str, Any]: List of roles
    """
    logger.info("Listing roles")

    db = firestore_client.get_firestore_client()
    roles = []
    results = db.collection(ROLE_COLLECTION).stream()
    for doc in results:
        role_data = doc.to_dict()
        role_data["role_id"] = doc.id
        roles.append(role_data)

    return {"roles": roles}


def get_role_by_id(role_id: str) -> Dict[str, Any]:
    """Retrieves a specific role by ID

    Args:
        role_id (str): Role ID

    Returns:
        Dict[str, Any]: Role details
    """
    logger.info(f"Getting role by ID: {role_id}")

    db = firestore_client.get_firestore_client()
    role_ref = db.collection(ROLE_COLLECTION).document(role_id)
    role = role_ref.get()

    if not role.exists:
        raise ResourceNotFoundError("Role", role_id)

    role_data = role.to_dict()
    role_data["role_id"] = role.id

    return role_data


def create_role(role_data: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new role in the system

    Args:
        role_data (Dict[str, Any]): Role data

    Returns:
        Dict[str, Any]: Created role details
    """
    logger.info(f"Creating new role: {role_data}")

    required_fields = ["name", "permissions"]
    if not all(field in role_data for field in required_fields):
        raise ValidationError("Missing required fields", [{"field": field, "message": "This field is required"}
                                                        for field in required_fields if field not in role_data])

    db = firestore_client.get_firestore_client()

    # Check if role name already exists
    name = role_data["name"]
    existing_role = next((doc for doc in db.collection(ROLE_COLLECTION).where("name", "==", name).stream()), None)
    if existing_role:
        raise ValidationError("Role name already exists", [{"field": "name", "message": "Role name already exists"}])

    # Generate role_id
    role_id = str(uuid.uuid4())

    # Create role document
    role_ref = db.collection(ROLE_COLLECTION).document(role_id)
    role_data["created_at"] = datetime.utcnow()
    role_ref.set(role_data)

    log_audit_event(user_id="system", action_type="create", resource_type="role", resource_id=role_id, details=role_data)

    role_data["role_id"] = role_id
    return role_data


def update_role(role_id: str, role_data: Dict[str, Any]) -> Dict[str, Any]:
    """Updates an existing role's information

    Args:
        role_id (str): Role ID
        role_data (Dict[str, Any]): Role data to update

    Returns:
        Dict[str, Any]: Updated role details
    """
    logger.info(f"Updating role: role_id={role_id}, data={role_data}")

    db = firestore_client.get_firestore_client()
    role_ref = db.collection(ROLE_COLLECTION).document(role_id)
    role = role_ref.get()

    if not role.exists:
        raise ResourceNotFoundError("Role", role_id)

    # Check for name conflicts
    if "name" in role_data:
        name = role_data["name"]
        existing_role = next((doc for doc in db.collection(ROLE_COLLECTION).where("name", "==", name).where(
            "__name__", "!=", role_ref.id).stream()), None)
        if existing_role:
            raise ValidationError("Role name already exists", [{"field": "name", "message": "Role name already exists"}])

    role_data["updated_at"] = datetime.utcnow()
    role_ref.update(role_data)

    log_audit_event(user_id="system", action_type="update", resource_type="role", resource_id=role_id, details=role_data)

    updated_role = role_ref.get().to_dict()
    updated_role["role_id"] = role_id
    return updated_role


def delete_role(role_id: str) -> Dict[str, Any]:
    """Deletes a role from the system

    Args:
        role_id (str): Role ID

    Returns:
        Dict[str, Any]: Deletion confirmation
    """
    logger.info(f"Deleting role: {role_id}")

    db = firestore_client.get_firestore_client()
    role_ref = db.collection(ROLE_COLLECTION).document(role_id)
    role = role_ref.get()

    if not role.exists:
        raise ResourceNotFoundError("Role", role_id)

    # Check if any users are assigned this role
    users_with_role = list(db.collection(USER_COLLECTION).where("role", "==", role_id).stream())
    if users_with_role:
        raise ValidationError("Role is in use", [{"field": "role", "message": "Role is currently assigned to users"}])

    role_ref.delete()

    log_audit_event(user_id="system", action_type="delete", resource_type="role", resource_id=role_id)

    return {"message": f"Role {role_id} deleted successfully"}


def get_system_settings() -> Dict[str, Any]:
    """Retrieves the current system settings

    Returns:
        Dict[str, Any]: System settings
    """
    logger.info("Getting system settings")

    db = firestore_client.get_firestore_client()
    settings_ref = db.collection(SETTINGS_COLLECTION).document("settings")
    settings = settings_ref.get()

    if not settings.exists:
        # Create default settings if not found
        default_settings = {
            "self_healing_mode": "SEMI_AUTOMATIC",
            "notification_channel": "TEAMS",
            "log_level": "INFO"
        }
        settings_ref.set(default_settings)
        return default_settings

    return settings.to_dict()


def update_system_settings(settings_data: Dict[str, Any]) -> Dict[str, Any]:
    """Updates the system settings

    Args:
        settings_data (Dict[str, Any]): Settings data to update

    Returns:
        Dict[str, Any]: Updated system settings
    """
    logger.info(f"Updating system settings: {settings_data}")

    db = firestore_client.get_firestore_client()
    settings_ref = db.collection(SETTINGS_COLLECTION).document("settings")
    current_settings = settings_ref.get()

    if not current_settings.exists:
        raise ResourceNotFoundError("System Settings", "settings")

    # Validate settings data structure
    if not isinstance(settings_data, dict):
        raise ValidationError("Invalid settings data", [{"field": "settings", "message": "Settings data must be a dictionary"}])

    # Update settings
    settings_ref.update(settings_data)

    # If sensitive settings changed, update Secret Manager
    if "api_key" in settings_data:
        secret_manager.update_secret("api_key", settings_data["api_key"])

    log_audit_event(user_id="system", action_type="update", resource_type="system_settings", resource_id="settings", details=settings_data)

    updated_settings = settings_ref.get().to_dict()
    return updated_settings


def get_system_health() -> Dict[str, Any]:
    """Retrieves the current system health status

    Returns:
        Dict[str, Any]: System health information
    """
    logger.info("Checking system health")

    health_data = {}

    # Check BigQuery service health
    try:
        bq_status = check_component_status("bigquery")
        health_data["bigquery"] = bq_status
    except Exception as e:
        logger.error(f"Error checking BigQuery health: {e}")
        health_data["bigquery"] = {"status": "ERROR", "message": str(e)}

    # Check Cloud Storage service health
    try:
        gcs_status = check_component_status("cloud_storage")
        health_data["cloud_storage"] = gcs_status
    except Exception as e:
        logger.error(f"Error checking Cloud Storage health: {e}")
        health_data["cloud_storage"] = {"status": "ERROR", "message": str(e)}

    # Check Cloud Composer service health
    try:
        composer_status = check_component_status("cloud_composer")
        health_data["cloud_composer"] = composer_status
    except Exception as e:
        logger.error(f"Error checking Cloud Composer health: {e}")
        health_data["cloud_composer"] = {"status": "ERROR", "message": str(e)}

    # Check Vertex AI service health
    try:
        vertex_status = check_component_status("vertex_ai")
        health_data["vertex_ai"] = vertex_status
    except Exception as e:
        logger.error(f"Error checking Vertex AI health: {e}")
        health_data["vertex_ai"] = {"status": "ERROR", "message": str(e)}

    # Check custom components health
    try:
        custom_status = check_component_status("custom_components")
        health_data["custom_components"] = custom_status
    except Exception as e:
        logger.error(f"Error checking custom components health: {e}")
        health_data["custom_components"] = {"status": "ERROR", "message": str(e)}

    # Collect system metrics (CPU, memory, disk usage)
    # TODO: Implement system metrics collection

    # Determine overall system status based on component health
    overall_status = "OK"
    for component, status in health_data.items():
        if status.get("status") == "ERROR":
            overall_status = "DEGRADED"
            break

    health_data["overall_status"] = overall_status

    return health_data


def check_component_status(component_name: str) -> Dict[str, Any]:
    """Checks the status of a specific system component

    Args:
        component_name (str): Name of the component to check

    Returns:
        Dict[str, Any]: Component status information
    """
    logger.info(f"Checking component status: {component_name}")

    valid_components = ["bigquery", "cloud_storage", "cloud_composer", "vertex_ai", "custom_components"]
    if component_name not in valid_components:
        raise ResourceNotFoundError("Component", component_name)

    # Get metric client for Cloud Monitoring
    metric_client_obj = metric_client.get_metric_client()

    # Query component-specific health metrics
    # TODO: Implement component-specific health metrics queries

    # Determine component status based on metrics
    # TODO: Implement logic to determine status based on metrics

    # For now, return a placeholder status
    status = {"status": "OK", "message": "Component is operational"}
    return status


def get_audit_logs(page: int, page_size: int, start_date: Optional[datetime] = None,
                   end_date: Optional[datetime] = None, user_id: Optional[str] = None,
                   action_type: Optional[str] = None, resource_type: Optional[str] = None) -> Dict[str, Any]:
    """Retrieves system audit logs with filtering options

    Args:
        page (int): Page number for pagination
        page_size (int): Number of logs per page
        start_date (Optional[datetime], optional): Filter logs after this date. Defaults to None.
        end_date (Optional[datetime], optional): Filter logs before this date. Defaults to None.
        user_id (Optional[str], optional): Filter logs by user ID. Defaults to None.
        action_type (Optional[str], optional): Filter logs by action type. Defaults to None.
        resource_type (Optional[str], optional): Filter logs by resource type. Defaults to None.

    Returns:
        Dict[str, Any]: Paginated list of audit logs with total count
    """
    logger.info(f"Listing audit logs: page={page}, page_size={page_size}, start_date={start_date}, end_date={end_date}, "
                f"user_id={user_id}, action_type={action_type}, resource_type={resource_type}")

    bq = bigquery_client.get_bigquery_client()
    dataset_id = config.get_bigquery_dataset()
    table_id = AUDIT_LOG_TABLE
    table_ref = f"`{bq.project}.{dataset_id}.{table_id}`"

    # Build SQL query
    query = f"""
        SELECT *
        FROM {table_ref}
        WHERE TRUE  -- Base condition to allow dynamic filtering
    """

    # Apply filters
    if start_date:
        query += f" AND timestamp >= '{start_date.isoformat()}'"
    if end_date:
        query += f" AND timestamp <= '{end_date.isoformat()}'"
    if user_id:
        query += f" AND user_id = '{user_id}'"
    if action_type:
        query += f" AND action_type = '{action_type}'"
    if resource_type:
        query += f" AND resource_type = '{resource_type}'"

    # Add pagination
    limit = page_size
    offset = (page - 1) * page_size
    query += f" ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"

    # Count query
    count_query = f"""
        SELECT COUNT(*) FROM {table_ref}
        WHERE TRUE  -- Base condition to allow dynamic filtering
    """

    # Apply filters to count query
    if start_date:
        count_query += f" AND timestamp >= '{start_date.isoformat()}'"
    if end_date:
        count_query += f" AND timestamp <= '{end_date.isoformat()}'"
    if user_id:
        count_query += f" AND user_id = '{user_id}'"
    if action_type:
        count_query += f" AND action_type = '{action_type}'"
    if resource_type:
        count_query += f" AND resource_type = '{resource_type}'"

    try:
        # Execute count query
        count_job = bq.query(count_query)
        count_results = list(count_job.result())
        total_count = count_results[0][0] if count_results else 0

        # Execute main query
        query_job = bq.query(query)
        results = list(query_job.result())

        # Format audit log data
        logs = []
        for row in results:
            log = dict(row.items())
            logs.append(log)

    except Exception as e:
        logger.error(f"Error listing audit logs: {e}")
        raise

    return {"logs": logs, "total_count": total_count, "page": page, "page_size": page_size}


def log_audit_event(user_id: str, action_type: str, resource_type: str,
                    resource_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None) -> bool:
    """Records an audit event in the system

    Args:
        user_id (str): User ID performing the action
        action_type (str): Type of action performed
        resource_type (str): Type of resource affected
        resource_id (Optional[str], optional): ID of the resource affected. Defaults to None.
        details (Optional[Dict[str, Any]], optional): Additional details about the event. Defaults to None.

    Returns:
        bool: Success indicator
    """
    try:
        bq = bigquery_client.get_bigquery_client()
        dataset_id = config.get_bigquery_dataset()
        table_id = AUDIT_LOG_TABLE
        table_ref = f"`{bq.project}.{dataset_id}.{table_id}`"

        # Create audit log entry
        row = {
            "timestamp": datetime.utcnow(),
            "user_id": user_id,
            "action_type": action_type,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "details": details
        }

        # Insert audit log entry into BigQuery table
        errors = bq.insert_rows_json(table_ref, [row])
        if errors:
            logger.error(f"Failed to insert audit log entry: {errors}")
            return False

        logger.info(f"Logged audit event: user_id={user_id}, action_type={action_type}, resource_type={resource_type}")
        return True

    except Exception as e:
        logger.error(f"Error logging audit event: {e}")
        return False


def authenticate_user(username_or_email: str, password: str) -> Dict[str, Any]:
    """Authenticates a user with username/email and password

    Args:
        username_or_email (str): Username or email
        password (str): Password

    Returns:
        Dict[str, Any]: Authentication result with token if successful
    """
    logger.info(f"Authentication attempt for user: {username_or_email}")

    db = firestore_client.get_firestore_client()

    # Query user by username or email
    query = db.collection(USER_COLLECTION)
    query = query.where("username", "==", username_or_email).limit(1)
    results = list(query.stream())

    if not results:
        query = db.collection(USER_COLLECTION)
        query = query.where("email", "==", username_or_email).limit(1)
        results = list(query.stream())

    if not results:
        raise AuthorizationError("Invalid credentials")

    user = results[0]
    user_data = user.to_dict()
    hashed_password = user_data.get("password_hash")

    if not hashed_password:
        raise AuthorizationError("Invalid credentials")

    # Verify password hash
    if not bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8")):
        raise AuthorizationError("Invalid credentials")

    # Generate JWT token
    user_id = user.id
    role = user_data.get("role")
    payload = {
        "user_id": user_id,
        "role": role,
        "exp": datetime.utcnow() + typing.cast(datetime.timedelta, typing.cast(datetime.timedelta, datetime.timedelta(minutes=TOKEN_EXPIRY_MINUTES)))
    }
    token = jwt.encode(payload, config.get("jwt.secret"), algorithm="HS256")

    log_audit_event(user_id=user_id, action_type="login", resource_type="user", resource_id=user_id)

    return {"token": token, "user_id": user_id, "role": role}