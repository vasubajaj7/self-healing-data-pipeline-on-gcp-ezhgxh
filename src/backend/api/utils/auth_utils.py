"""
Authentication utilities for the self-healing data pipeline API.
Provides functions for JWT token validation, user authentication, role-based access control,
and integration with GCP authentication mechanisms. Serves as a bridge between the API layer
and the underlying authentication systems.
"""

import jwt  # pyjwt version: ^2.4.0
from datetime import datetime, timedelta
from typing import Dict, Optional
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from starlette import status
from passlib.context import CryptContext  # passlib version: ^1.7.4
from jose import JWTError, jwt as jose_jwt  # python-jose version: ^3.3.0

from ...config import get_config
from ...utils.logging.logger import logger
from .token_manager import get_token_for_service
from .oauth_client import OAuthClient
from .gcp_auth import get_credentials_for_service
from ..models.error_models import AuthenticationError, AuthorizationError, ErrorCategory, ErrorSeverity

# Define OAuth2 scheme for password authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT algorithm and expiration settings
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password

    Args:
        plain_password (str): The plain password to verify
        hashed_password (str): The hashed password to compare against

    Returns:
        bool: True if password matches, False otherwise
    """
    # Use pwd_context to verify the plain password against the hashed password
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generates a password hash for a plain password

    Args:
        password (str): The plain password to hash

    Returns:
        str: Hashed password
    """
    # Use pwd_context to hash the password
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Creates a JWT access token with user data and expiration

    Args:
        data (dict): User data to encode in the token
        expires_delta (datetime.timedelta): Token expiration time

    Returns:
        str: JWT access token
    """
    # Create a copy of the data dictionary to avoid modifying the original
    to_encode = data.copy()

    # Calculate expiration time based on current time plus expires_delta
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        # If expires_delta is None, use default expiration time
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    # Add expiration time to the token data
    to_encode.update({"exp": expire})

    # Get JWT secret key from configuration
    config = get_config()
    secret_key = config.get("auth.jwt_secret")
    if not secret_key:
        raise ValueError("JWT secret key not configured")

    # Encode the data with the secret key using the specified algorithm
    encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)

    # Return the encoded JWT token
    return encoded_jwt


def verify_token(token: str) -> dict:
    """Verifies a JWT token and returns the decoded payload

    Args:
        token (str): The JWT token to verify

    Returns:
        dict: Decoded token payload
    """
    # Get JWT secret key from configuration
    config = get_config()
    secret_key = config.get("auth.jwt_secret")
    if not secret_key:
        raise ValueError("JWT secret key not configured")

    try:
        # Try to decode the token using the secret key and algorithm
        payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        # Verify token expiration
        raise AuthenticationError(message="Token has expired", service_name="JWT", auth_details={"error": "ExpiredSignatureError"})
    except jwt.JWTError as e:
        # Handle JWTError exceptions and raise AuthenticationError
        logger.error(f"JWT verification failed: {e}")
        raise AuthenticationError(message="Invalid token", service_name="JWT", auth_details={"error": str(e)})


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """FastAPI dependency to get the current authenticated user from a token

    Args:
        token (str, optional): The JWT token. Defaults to Depends(oauth2_scheme).

    Returns:
        dict: User information from the token
    """
    try:
        # Call verify_token to validate the token
        payload = verify_token(token)

        # Extract user information from the token payload
        user_info = payload

        # Return the user information
        return user_info
    except AuthenticationError as e:
        # Handle exceptions and raise appropriate HTTP errors
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=e.message,
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_active_user(current_user: dict = Depends(get_current_user)) -> dict:
    """FastAPI dependency to get the current active user

    Args:
        current_user (dict, optional): User information. Defaults to Depends(get_current_user).

    Returns:
        dict: User information if user is active
    """
    # Check if the user is marked as disabled
    if current_user.get("disabled"):
        # If disabled, raise HTTPException with 400 status code
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user"
        )

    # Return the user information if active
    return current_user


def authenticate_user(username: str, password: str) -> dict:
    """Authenticates a user with username and password

    Args:
        username (str): The username
        password (str): The password

    Returns:
        dict: User information if authentication successful, False otherwise
    """
    # Get user from database or user store by username
    config = get_config()
    users = config.get("auth.users")
    if not users or not isinstance(users, dict):
        logger.warning("No users configured for authentication")
        return False

    user = users.get(username)
    if not user:
        # If user not found, return False
        logger.warning(f"User {username} not found")
        return False

    # Verify password using verify_password function
    hashed_password = user.get("hashed_password")
    if not hashed_password:
        logger.error(f"User {username} has no hashed password configured")
        return False

    if not verify_password(password, hashed_password):
        # If password verification fails, return False
        logger.warning(f"Invalid password for user {username}")
        return False

    # Return user information if authentication successful
    logger.info(f"User {username} authenticated successfully")
    return user


def has_permission(user: dict, permission: str) -> bool:
    """Checks if a user has a specific permission

    Args:
        user (dict): User information
        permission (str): The permission to check

    Returns:
        bool: True if user has permission, False otherwise
    """
    # Extract roles from user information
    roles = user.get("roles", [])
    if not roles:
        logger.debug(f"User {user.get('username')} has no roles assigned")
        return False

    # Get role-permission mappings from configuration
    config = get_config()
    role_permissions = config.get("auth.role_permissions")
    if not role_permissions or not isinstance(role_permissions, dict):
        logger.error("No role permissions configured")
        return False

    # Check if any of the user's roles have the required permission
    for role in roles:
        permissions = role_permissions.get(role, [])
        if permission in permissions:
            logger.debug(f"User {user.get('username')} has permission {permission} via role {role}")
            return True

    # Return False if permission not found
    logger.debug(f"User {user.get('username')} does not have permission {permission}")
    return False


def require_permission(permission: str):
    """Decorator to require a specific permission for an endpoint

    Args:
        permission (str): The permission required to access the endpoint

    Returns:
        callable: Decorator function
    """
    # Define decorator function that takes the endpoint function
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get current user and check permission
            user = kwargs.get("current_user")
            if not user:
                raise AuthorizationError(
                    message="User not authenticated",
                    service_name=func.__name__,
                    resource=func.__name__,
                    action=permission,
                )

            if not has_permission(user, permission):
                # If permission check fails, raise AuthorizationError
                raise AuthorizationError(
                    message=f"Insufficient permissions to access this resource. Requires permission: {permission}",
                    service_name=func.__name__,
                    resource=func.__name__,
                    action=permission,
                )

            # If permission check passes, call the original endpoint function
            return func(*args, **kwargs)

        # Return the wrapper function
        return wrapper

    # Return the decorator function
    return decorator


def get_service_token(service_name: str, scopes: list = None) -> str:
    """Gets an authentication token for a specific service

    Args:
        service_name (str): The name of the service to get the token for
        scopes (list): The scopes required for the token

    Returns:
        str: Authentication token for the service
    """
    try:
        # Call get_token_for_service from token_manager
        token = get_token_for_service(service_name, scopes)
        return token
    except Exception as e:
        # Handle exceptions and log errors
        logger.error(f"Error getting service token for {service_name}: {e}")
        raise


def get_oauth_token(client_name: str) -> str:
    """Gets an OAuth token for an external service

    Args:
        client_name (str): The name of the OAuth client configuration

    Returns:
        str: OAuth access token
    """
    try:
        # Get OAuth client configuration from application config
        config = get_config()
        oauth_client_config = config.get(f"oauth.{client_name}")
        if not oauth_client_config:
            raise ValueError(f"OAuth client configuration not found: {client_name}")

        # Create OAuthClient instance with configuration
        oauth_client = OAuthClient(config=oauth_client_config)

        # Call get_access_token to obtain token
        access_token = oauth_client.get_token(client_name)
        return access_token
    except Exception as e:
        # Handle exceptions and log errors
        logger.error(f"Error getting OAuth token for {client_name}: {e}")
        raise AuthenticationError(message=f"Failed to get OAuth token: {e}", service_name=client_name)


from functools import wraps
class UserManager:
    """Manages user authentication, retrieval, and management operations"""

    def __init__(self):
        """Initializes the UserManager with user database"""
        # Initialize user database from configuration or default to empty dict
        config = get_config()
        self._users_db = config.get("auth.users", {})
        # Log initialization of user manager
        logger.info("UserManager initialized")

    def get_user(self, username: str) -> dict:
        """Retrieves a user by username

        Args:
            username (str): The username to retrieve

        Returns:
            dict: User information or None if not found
        """
        # Look up user in the user database by username
        user = self._users_db.get(username)
        # Log user lookup for auditing
        logger.debug(f"User lookup: username={username}, found={user is not None}")
        # Return user information if found, None otherwise
        return user

    def authenticate(self, username: str, password: str) -> dict:
        """Authenticates a user with username and password

        Args:
            username (str): The username
            password (str): The password

        Returns:
            dict: User information if authentication successful, None otherwise
        """
        # Get user by username using get_user method
        user = self.get_user(username)
        if not user:
            # If user not found, return None
            logger.warning(f"Authentication failed: User not found - username={username}")
            return None

        # Verify password using verify_password function
        hashed_password = user.get("hashed_password")
        if not verify_password(password, hashed_password):
            # If password verification fails, return None
            logger.warning(f"Authentication failed: Invalid password - username={username}")
            return None

        # Log authentication attempt
        logger.info(f"Authentication successful: username={username}")
        # Return user information if authentication successful
        return user

    def create_user(self, username: str, password: str, user_data: dict) -> dict:
        """Creates a new user with the provided information

        Args:
            username (str): The username for the new user
            password (str): The password for the new user
            user_data (dict): Additional user data

        Returns:
            dict: Created user information
        """
        # Check if username already exists
        if username in self._users_db:
            # If username exists, raise ValueError
            raise ValueError(f"Username '{username}' already exists")

        # Hash the password using get_password_hash
        hashed_password = get_password_hash(password)

        # Create user entry with username, hashed password, and user data
        user = {"username": username, "hashed_password": hashed_password, **user_data}

        # Store user in the database
        self._users_db[username] = user

        # Log user creation
        logger.info(f"User created: username={username}")
        # Return the created user information (without password)
        return {"username": username, **user_data}

    def update_user(self, username: str, user_data: dict) -> dict:
        """Updates an existing user's information

        Args:
            username (str): The username of the user to update
            user_data (dict): The data to update for the user

        Returns:
            dict: Updated user information
        """
        # Get user by username using get_user method
        user = self.get_user(username)
        if not user:
            # If user not found, raise ValueError
            raise ValueError(f"User '{username}' not found")

        # Update user information with provided data
        user.update(user_data)

        # Log user update
        logger.info(f"User updated: username={username}")
        # Return the updated user information
        return user

    def delete_user(self, username: str) -> bool:
        """Deletes a user by username

        Args:
            username (str): The username of the user to delete

        Returns:
            bool: True if user was deleted, False if not found
        """
        # Check if username exists in database
        if username in self._users_db:
            # If username exists, remove from database and return True
            del self._users_db[username]
            # Log user deletion
            logger.info(f"User deleted: username={username}")
            return True

        # If username not found, return False
        logger.warning(f"Delete user failed: User not found - username={username}")
        return False

    def change_password(self, username: str, current_password: str, new_password: str) -> bool:
        """Changes a user's password

        Args:
            username (str): The username of the user to change password for
            current_password (str): The user's current password
            new_password (str): The user's new password

        Returns:
            bool: True if password was changed, False otherwise
        """
        # Authenticate user with current password
        user = self.authenticate(username, current_password)
        if not user:
            # If authentication fails, return False
            logger.warning(f"Password change failed: Authentication failed - username={username}")
            return False

        # Hash the new password
        hashed_password = get_password_hash(new_password)

        # Update user's password in the database
        user["hashed_password"] = hashed_password

        # Log password change
        logger.info(f"Password changed: username={username}")
        # Return True for successful password change
        return True


class PermissionChecker:
    """Utility class for checking user permissions and roles"""

    def __init__(self):
        """Initializes the PermissionChecker with role-permission mappings"""
        # Load role-permission mappings from configuration
        config = get_config()
        self._role_permissions = config.get("auth.role_permissions", {})
        # Log initialization of permission checker
        logger.info("PermissionChecker initialized")

    def has_permission(self, user: dict, permission: str) -> bool:
        """Checks if a user has a specific permission

        Args:
            user (dict): User information
            permission (str): The permission to check

        Returns:
            bool: True if user has permission, False otherwise
        """
        # Extract roles from user information
        roles = user.get("roles", [])
        if not roles:
            logger.debug(f"User {user.get('username')} has no roles assigned")
            return False

        # Check if any of the user's roles have the required permission
        for role in roles:
            permissions = self._role_permissions.get(role, [])
            if permission in permissions:
                logger.debug(f"User {user.get('username')} has permission {permission} via role {role}")
                return True

        # Log permission check
        logger.debug(f"User {user.get('username')} does not have permission {permission}")
        # Return False if permission not found
        return False

    def has_role(self, user: dict, role: str) -> bool:
        """Checks if a user has a specific role

        Args:
            user (dict): User information
            role (str): The role to check

        Returns:
            bool: True if user has role, False otherwise
        """
        # Extract roles from user information
        roles = user.get("roles", [])
        if not roles:
            logger.debug(f"User {user.get('username')} has no roles assigned")
            return False

        # Check if the specified role is in the user's roles
        if role in roles:
            logger.debug(f"User {user.get('username')} has role {role}")
            return True

        # Log role check
        logger.debug(f"User {user.get('username')} does not have role {role}")
        # Return False if role not found
        return False

    def get_user_permissions(self, user: dict) -> set:
        """Gets all permissions for a user based on their roles

        Args:
            user (dict): User information

        Returns:
            set: Set of permission strings
        """
        # Extract roles from user information
        roles = user.get("roles", [])
        if not roles:
            logger.debug(f"User {user.get('username')} has no roles assigned")
            return set()

        # Initialize empty set for permissions
        permissions = set()

        # For each role, add all associated permissions to the set
        for role in roles:
            role_permissions = self._role_permissions.get(role, [])
            permissions.update(role_permissions)

        # Log permission retrieval
        logger.debug(f"Retrieved permissions for user {user.get('username')}: {permissions}")
        # Return the complete set of permissions
        return permissions

    def require_permission(self, permission: str):
        """Creates a decorator that requires a specific permission

        Args:
            permission (str): The permission required to access the endpoint

        Returns:
            callable: Decorator function
        """
        # Define decorator function that takes the endpoint function
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Get current user and check permission
                user = kwargs.get("current_user")
                if not user:
                    raise AuthorizationError(
                        message="User not authenticated",
                        service_name=func.__name__,
                        resource=func.__name__,
                        action=permission,
                    )

                if not self.has_permission(user, permission):
                    # If permission check fails, raise AuthorizationError
                    raise AuthorizationError(
                        message=f"Insufficient permissions to access this resource. Requires permission: {permission}",
                        service_name=func.__name__,
                        resource=func.__name__,
                        action=permission,
                    )

                # If permission check passes, call the original endpoint function
                return func(*args, **kwargs)

            # Return the wrapper function
            return wrapper

        # Return the decorator function
        return decorator