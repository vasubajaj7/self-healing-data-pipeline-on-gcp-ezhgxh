"""Authentication middleware for the self-healing data pipeline API.

Implements token-based authentication, validation, and role-based access control.
Secures API endpoints by verifying authentication tokens, extracting user identity,
and enforcing authorization policies.
"""

import jwt  # pyjwt ^2.4.0
from fastapi import FastAPI, Request, Response, HTTPException, status, Depends, Security  # fastapi ^0.95.0
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials  # fastapi ^0.95.0
from typing import List, Dict, Any, Optional  # typing standard library
from datetime import datetime  # datetime standard library

from ...config import get_config  # src/backend/config.py
from ...utils.logging.logger import logger  # src/backend/utils/logging/logger.py
from ...utils.auth.token_manager import TokenManager  # src/backend/utils/auth/token_manager.py
from ...utils.auth.gcp_auth import get_token_for_service  # src/backend/utils/auth/gcp_auth.py
from ..models.error_models import AuthenticationError, AuthorizationError  # src/backend/api/models/error_models.py
from ..utils.response_utils import create_error_response  # src/backend/api/utils/response_utils.py

# Initialize TokenManager
token_manager = TokenManager()

# Define OAuth2 scheme
oauth2_scheme = HTTPBearer(auto_error=False)

# JWT Algorithm
ALGORITHM = "RS256"

# Authentication Header Name
AUTH_HEADER_NAME = "Authorization"

# Token Type
TOKEN_TYPE = "Bearer"


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> dict:
    """Extracts and validates the user from the authentication token

    Args:
        credentials (HTTPAuthorizationCredentials, optional): credentials. Defaults to Depends(oauth2_scheme).

    Returns:
        dict: User information extracted from token
    """
    # Check if credentials are provided
    if not credentials:
        raise AuthenticationError(
            message="Missing authentication token",
            service_name="auth_middleware",
            auth_details={}
        )

    # Verify token type is Bearer
    if credentials.scheme != TOKEN_TYPE:
        raise AuthenticationError(
            message="Invalid token type",
            service_name="auth_middleware",
            auth_details={"token_type": credentials.scheme}
        )

    try:
        # Decode and validate the JWT token
        payload = verify_token(credentials.credentials)

        # Extract user information from token payload
        user_info = {
            "user_id": payload.get("sub"),
            "username": payload.get("username"),
            "email": payload.get("email"),
            "roles": payload.get("roles", [])
        }

        # Return user information dictionary
        return user_info

    except AuthenticationError as auth_err:
        # Re-raise AuthenticationError to maintain consistency
        raise auth_err
    except Exception as e:
        # Handle and log exceptions for invalid tokens
        logger.error(f"Invalid authentication token: {e}")
        raise AuthenticationError(
            message="Invalid authentication token",
            service_name="auth_middleware",
            auth_details={"error": str(e)}
        )


def verify_token(token: str) -> dict:
    """Verifies the validity of a JWT token

    Args:
        token (str): JWT token

    Returns:
        dict: Decoded token payload
    """
    try:
        # Get JWT public key from configuration
        public_key = get_config().get("auth.jwt_public_key")
        if not public_key:
            raise AuthenticationError(
                message="JWT public key not configured",
                service_name="auth_middleware",
                auth_details={}
            )

        # Decode the token using the public key and specified algorithm
        payload = jwt.decode(
            token,
            public_key,
            algorithms=[ALGORITHM]
        )

        # Verify token expiration
        if "exp" not in payload:
            raise AuthenticationError(
                message="Missing expiration claim in token",
                service_name="auth_middleware",
                auth_details={"missing_claim": "exp"}
            )

        # Return the decoded token payload
        return payload

    except jwt.ExpiredSignatureError:
        # Handle and log exceptions for invalid or expired tokens
        logger.error("Expired authentication token")
        raise AuthenticationError(
            message="Expired authentication token",
            service_name="auth_middleware",
            auth_details={"error": "token_expired"}
        )
    except jwt.InvalidTokenError as e:
        logger.error(f"Invalid authentication token: {e}")
        raise AuthenticationError(
            message="Invalid authentication token",
            service_name="auth_middleware",
            auth_details={"error": str(e)}
        )
    except AuthenticationError as auth_err:
        # Re-raise AuthenticationError to maintain consistency
        raise auth_err
    except Exception as e:
        logger.error(f"Error verifying token: {e}")
        raise AuthenticationError(
            message="Error verifying token",
            service_name="auth_middleware",
            auth_details={"error": str(e)}
        )


def get_token_from_header(request: Request) -> Optional[str]:
    """Extracts the token from the Authorization header

    Args:
        request (Request): FastAPI request object

    Returns:
        Optional[str]: Extracted token string
    """
    # Get Authorization header from request
    auth_header = request.headers.get(AUTH_HEADER_NAME)

    # Check if header exists and starts with Bearer
    if auth_header and auth_header.startswith(TOKEN_TYPE):
        # Extract token part from the header
        token = auth_header.split(TOKEN_TYPE)[1].strip()
        # Return the token string
        return token

    # Return None if header is missing or invalid
    return None


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme)) -> dict:
    """Dependency function that requires authentication

    Args:
        credentials (HTTPAuthorizationCredentials, optional): credentials. Defaults to Depends(oauth2_scheme).

    Returns:
        dict: User information for authenticated request
    """
    try:
        # Call get_current_user to validate credentials
        user = get_current_user(credentials)
        # Return user information if authentication succeeds
        return user
    except AuthenticationError as auth_err:
        # Raise HTTPException with 401 status if authentication fails
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=auth_err.message,
            headers={"WWW-Authenticate": "Bearer"}
        )


def require_role(required_roles: List[str]):
    """Creates a dependency function that requires specific role

    Args:
        required_roles (List[str]): List of roles required to access the endpoint

    Returns:
        function: Dependency function that checks for required roles
    """
    def check_roles(user: dict = Depends(require_auth)):
        """Inner function that checks if user has required roles

        Args:
            user (dict, optional): User information. Defaults to Depends(require_auth).

        Returns:
            dict: User information if authorized
        """
        # Extract user roles from user information
        user_roles = user.get("roles", [])

        # Check if user has any of the required roles
        if not any(role in user_roles for role in required_roles):
            # Raise HTTPException with 403 status if not authorized
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )

        # Return user information if authorized
        return user

    # Return the inner function
    return check_roles


class AuthMiddleware:
    """Middleware for handling authentication across all API requests"""

    def __init__(self, exclude_paths: List[str], auto_error: bool):
        """Initialize the authentication middleware

        Args:
            exclude_paths (List[str]): List of paths that don't require authentication
            auto_error (bool): Flag to determine error behavior
        """
        # Store exclude_paths for paths that don't require authentication
        self.exclude_paths = exclude_paths
        # Store auto_error flag to determine error behavior
        self.auto_error = auto_error
        # Initialize token manager
        self.token_manager = TokenManager()

    async def async_dispatch(self, request: Request, call_next):
        """Process each request through the authentication middleware

        Args:
            request (Request): FastAPI request object
            call_next (function): Next middleware/endpoint handler

        Returns:
            Response: Response from the endpoint
        """
        # Check if request path is in exclude_paths
        if self.is_path_excluded(request.url.path):
            # If excluded, bypass authentication and call next middleware
            return await call_next(request)

        # Extract token from Authorization header
        token = get_token_from_header(request)

        try:
            # If token is missing and auto_error is True, raise 401 error
            if not token and self.auto_error:
                raise AuthenticationError(
                    message="Missing authentication token",
                    service_name="auth_middleware",
                    auth_details={}
                )

            # If token is present, verify token and extract user information
            if token:
                payload = verify_token(token)
                user_info = {
                    "user_id": payload.get("sub"),
                    "username": payload.get("username"),
                    "email": payload.get("email"),
                    "roles": payload.get("roles", [])
                }
                # Attach user information to request state
                request.state.user = user_info

            # Call next middleware/endpoint handler
            response = await call_next(request)
            # Return the response
            return response

        except AuthenticationError as auth_err:
            # Create error response
            error_response = create_error_response(
                message=auth_err.message,
                status_code=status.HTTP_401_UNAUTHORIZED,
                error_type=auth_err.category.name,
                category=auth_err.category,
                severity=auth_err.severity,
                details=[{"message": str(auth_err)}],
                request_id=request.headers.get("X-Request-ID")
            )
            # Convert to HTTPException
            raise error_to_http_exception(error_response)
        except HTTPException as http_exc:
            # Re-raise HTTPException to maintain consistency
            raise http_exc
        except Exception as e:
            # Handle exceptions and return appropriate error responses
            logger.error(f"Authentication middleware error: {e}")
            error_response = create_error_response(
                message=f"Internal server error: {e}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_type="internal_server_error",
                category=ErrorCategory.SYSTEM,
                severity=ErrorSeverity.CRITICAL,
                details=[{"message": str(e)}],
                request_id=request.headers.get("X-Request-ID")
            )
            # Convert to HTTPException
            raise error_to_http_exception(error_response)

    async def __call__(self, scope, receive, send):
        """Make the class callable as ASGI middleware

        Args:
            scope (object): ASGI scope
            receive (function): ASGI receive function
            send (function): ASGI send function
        """
        # Create ASGI middleware application
        async def app(request: Request, call_next):
            return await self.async_dispatch(request, call_next)

        # Return the application coroutine
        return await app(
            Request(scope, receive=receive),
            lambda request: Response(status_code=404)  # Default response if no route matches
        )(scope, receive, send)

    def is_path_excluded(self, path: str) -> bool:
        """Check if a path should be excluded from authentication

        Args:
            path (str): Request path

        Returns:
            bool: True if path is excluded, False otherwise
        """
        # Check if path exactly matches any exclude_path
        if path in self.exclude_paths:
            return True

        # Check if path matches any pattern in exclude_paths
        for pattern in self.exclude_paths:
            if pattern.endswith("*") and path.startswith(pattern[:-1]):
                return True

        # Return False if not excluded
        return False


class JWTBearer(HTTPBearer):
    """Security dependency for JWT Bearer token authentication"""

    def __init__(self, auto_error: bool = True, required_roles: List[str] = None):
        """Initialize the JWT Bearer security scheme

        Args:
            auto_error (bool, optional): Flag to determine error behavior. Defaults to True.
        """
        # Initialize HTTPBearer with auto_error setting
        super().__init__(auto_error=auto_error)
        # Store required_roles for authorization checks
        self.required_roles = required_roles

    async def __call__(self, request: Request):
        """Validate and process the Bearer token

        Args:
            request (Request): FastAPI request object

        Returns:
            dict: User information from token
        """
        try:
            # Get credentials using parent HTTPBearer class
            credentials: HTTPAuthorizationCredentials = await super().__call__(request)

            # If credentials are None and auto_error is True, raise 401 error
            if credentials is None and self.auto_error:
                raise AuthenticationError(
                    message="Missing authentication token",
                    service_name="auth_middleware",
                    auth_details={}
                )

            # If credentials exist, verify token and extract user information
            if credentials:
                user_info = get_current_user(credentials)

                # If required_roles is specified, check user has required role
                if self.required_roles:
                    user_roles = user_info.get("roles", [])
                    if not any(role in user_roles for role in self.required_roles):
                        raise AuthorizationError(
                            message="Insufficient permissions",
                            service_name="auth_middleware",
                            resource=request.url.path,
                            action="access"
                        )

                # Return user information if authentication and authorization succeed
                return user_info

        except AuthenticationError as auth_err:
            # Raise HTTPException with 401 status if authentication fails
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=auth_err.message,
                headers={"WWW-Authenticate": "Bearer"}
            )
        except AuthorizationError as auth_err:
            # Raise HTTPException with 403 status if authorization fails
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=auth_err.message
            )
        except HTTPException as http_exc:
            # Re-raise HTTPException to maintain consistency
            raise http_exc
        except Exception as e:
            # Handle exceptions and return appropriate error responses
            logger.error(f"Authentication middleware error: {e}")
            error_response = create_error_response(
                message=f"Internal server error: {e}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_type="internal_server_error",
                category=ErrorCategory.SYSTEM,
                severity=ErrorSeverity.CRITICAL,
                details=[{"message": str(e)}],
                request_id=request.headers.get("X-Request-ID")
            )
            # Convert to HTTPException
            raise error_to_http_exception(error_response)