# Core exception handling functionality
from src.backend.utils.errors.exception_handler import ExceptionHandler  # Assuming version from src/backend/utils/errors/exception_handler.py
from src.backend.utils.errors.exception_handler import extract_error_context  # Assuming version from src/backend/utils/errors/exception_handler.py
from src.backend.utils.errors.exception_handler import classify_exception  # Assuming version from src/backend/utils/errors/exception_handler.py
# Error type definitions and classifications
from src.backend.utils.errors.error_types import PipelineError  # Assuming version from src/backend/utils/errors/error_types.py
from src.backend.utils.errors.error_types import ErrorCategory  # Assuming version from src/backend/utils/errors/error_types.py
# API error response models
from src.backend.api.models.error_models import ErrorResponse  # Assuming version from src/backend/api/models/error_models.py
from src.backend.api.models.error_models import ValidationErrorResponse  # Assuming version from src/backend/api/models/error_models.py
from src.backend.api.models.error_models import ErrorCategory  # Assuming version from src/backend/api/models/error_models.py
from src.backend.api.models.error_models import ErrorSeverity  # Assuming version from src/backend/api/models/error_models.py
# API response utilities
from src.backend.api.utils.response_utils import handle_exception  # Assuming version from src/backend/api/utils/response_utils.py
from src.backend.api.utils.response_utils import create_response_metadata  # Assuming version from src/backend/api/utils/response_utils.py
# Logging configuration
from src.backend.utils.logging.logger import get_logger  # Assuming version from src/backend/utils/logging/logger.py
# Application configuration
from src.backend.config import get_config  # Assuming version from src/backend/config.py
# FastAPI framework for building APIs
from fastapi import FastAPI  # fastapi package version: ^0.95.0
# Base middleware class for Starlette/FastAPI
from starlette.middleware.base import BaseHTTPMiddleware  # starlette package version: ^0.26.0
# Request handling for middleware
from starlette.requests import Request  # starlette package version: ^0.26.0
# Response handling for middleware
from starlette.responses import Response, JSONResponse  # starlette package version: ^0.26.0
# Type definitions for middleware components
from starlette.types import ASGIApp, Receive, Scope, Send  # starlette package version: ^0.26.0
# Extract and format stack traces from exceptions
import traceback  # standard library
# Type annotations for better code documentation
from typing import Callable  # standard library
# Generate unique request IDs
import uuid  # standard library
# JSON serialization and deserialization
import json  # standard library

# Initialize logger
logger = get_logger(__name__)

# Initialize exception handler
exception_handler = ExceptionHandler()

# Get debug mode from config
DEBUG_MODE = get_config().api.debug_mode


async def get_request_id(request: Request) -> str:
    """Extracts or generates a request ID from the request headers

    Args:
        request (starlette.requests.Request): The incoming request

    Returns:
        str: Request ID for tracing and correlation
    """
    # Check if X-Request-ID header exists in request
    request_id = request.headers.get("X-Request-ID")
    # If header exists, return its value
    if request_id:
        return request_id
    # Otherwise, generate a new UUID4 string
    else:
        request_id = str(uuid.uuid4())
        return request_id


async def extract_request_context(request: Request) -> dict:
    """Extracts context information from the request for error handling

    Args:
        request (starlette.requests.Request): The incoming request

    Returns:
        dict: Context dictionary with request information
    """
    # Initialize empty context dictionary
    context = {}
    # Extract client IP address
    context["client_ip"] = request.client.host if request.client else None
    # Extract request method and path
    context["request_method"] = request.method
    context["request_path"] = request.url.path
    # Extract query parameters
    context["query_params"] = dict(request.query_params)
    # Extract headers (sanitized)
    context["headers"] = {k: v for k, v in request.headers.items() if k.lower() not in ["authorization", "cookie"]}
    # Add user information if available
    # (This is a placeholder - implement actual user extraction logic)
    context["user"] = {"user_id": "unknown", "username": "unknown"}
    # Return the context dictionary
    return context


class ErrorMiddleware(BaseHTTPMiddleware):
    """Middleware that handles exceptions and converts them to standardized API responses"""

    def __init__(self, app: FastAPI, debug_mode: bool, enable_self_healing: bool):
        """Initialize the error middleware with the FastAPI application

        Args:
            app (fastapi.applications.FastAPI): The FastAPI application instance
            debug_mode (bool): Flag indicating whether debug mode is enabled
            enable_self_healing (bool): Flag indicating whether self-healing is enabled
        """
        # Store the FastAPI app instance
        self.app = app
        # Store debug mode flag
        self.debug_mode = debug_mode
        # Store self-healing enablement flag
        self.enable_self_healing = enable_self_healing
        # Initialize exception handler
        self.exception_handler = ExceptionHandler()

    async def dispatch(self, request: Request, call_next: Callable[[Request], Response]) -> Response:
        """Process the request through the middleware, catching and handling exceptions

        Args:
            request (starlette.requests.Request): The incoming request
            call_next (callable): The next middleware or endpoint in the chain

        Returns:
            starlette.responses.Response: The response from the endpoint or error response
        """
        # Extract or generate request ID
        request_id = await get_request_id(request)
        # Add request ID to request state for downstream use
        request.state.request_id = request_id
        try:
            # Try to execute the request through the next middleware/endpoint
            response = await call_next(request)
            # If successful, return the response
            return response
        except Exception as exc:
            # If exception occurs, catch and handle it
            # Extract context from request
            context = await extract_request_context(request)
            # Process exception with appropriate handler based on type
            error_response = self.handle_exception(exc, context, request_id)
            # Log the error with context
            self.log_error(exc, context, request_id)
            # Return error response with appropriate status code
            return JSONResponse(error_response.dict(), status_code=error_response.status_code)

    def handle_exception(self, exc: Exception, context: dict, request_id: str) -> JSONResponse:
        """Handles an exception by converting it to a standardized error response

        Args:
            exc (Exception): The exception to handle
            context (dict): Contextual information about the error
            request_id (str): The request ID

        Returns:
            starlette.responses.JSONResponse: Standardized error response
        """
        # Log exception details with context
        self.log_error(exc, context, request_id)
        # Determine if self-healing should be attempted
        if self.enable_self_healing:
            # If self-healing enabled, attempt to heal the exception
            healing_result = self.attempt_self_healing(exc, context)
        # Convert exception to standardized error response
        error_response = handle_exception(exc, request_id, include_traceback=DEBUG_MODE)
        # Return JSONResponse with error details and appropriate status code
        return error_response

    def attempt_self_healing(self, exc: Exception, context: dict) -> dict:
        """Attempts to self-heal an exception if possible

        Args:
            exc (Exception): The exception to heal
            context (dict): Contextual information about the error

        Returns:
            dict: Self-healing result with success status and actions taken
        """
        # Check if exception is self-healable
        if not isinstance(exc, PipelineError) or not exc.is_self_healable():
            # If not self-healable, return failure result
            return {"success": False, "actions_taken": []}
        # Use exception_handler to attempt self-healing
        try:
            # TODO: Implement actual self-healing logic using exception_handler
            # For now, just log that self-healing was attempted
            logger.info(f"Attempting self-healing for exception: {exc}")
            healing_result = {"success": True, "actions_taken": ["Placeholder action"]}
        except Exception as e:
            logger.error(f"Error during self-healing attempt: {e}")
            healing_result = {"success": False, "actions_taken": [], "error": str(e)}
        # Log healing attempt result
        logger.info(f"Self-healing attempt result: {healing_result}")
        # Return result dictionary with success status and actions taken
        return healing_result

    def log_error(self, exc: Exception, context: dict, request_id: str) -> None:
        """Logs error details with appropriate level and context

        Args:
            exc (Exception): The exception to log
            context (dict): Contextual information about the error
            request_id (str): The request ID
        """
        # Determine appropriate log level based on exception severity
        if isinstance(exc, PipelineError):
            log_level = exc.severity.name.lower()
        else:
            log_level = "error"  # Default to error for non-PipelineError exceptions
        # Format exception message with context
        message = f"Request ID: {request_id} - Exception: {str(exc)} - Context: {context}"
        # Include request ID in log context
        extra = {"request_id": request_id}
        # Log the exception with determined level
        logger.log(getattr(logger, log_level), message, extra=extra)
        # Log traceback at debug level if in debug mode
        if DEBUG_MODE:
            logger.debug(traceback.format_exc(), extra=extra)