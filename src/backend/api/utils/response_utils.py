"""
Utility functions for standardized API response handling in the self-healing data pipeline.

This module provides helper functions to create consistent response structures,
handle errors, and format responses according to the API standards.
"""

import uuid
import traceback
from typing import Dict, Any, Optional, List, Union

from fastapi import HTTPException, status, Request

from ..models.error_models import (
    ResponseStatus, ResponseMetadata, ErrorCategory, ErrorSeverity,
    ErrorDetail, ErrorResponse, ValidationErrorResponse,
    PipelineError, ValidationError
)
from ..models.response_models import (
    BaseResponse, DataResponse, PaginatedResponse, PaginationMetadata
)
from .pagination_utils import create_pagination_metadata
from ...logging_config import get_logger

# Initialize logger
logger = get_logger(__name__)


def create_response_metadata(
    request_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    additional_info: Optional[Dict[str, Any]] = None
) -> ResponseMetadata:
    """
    Creates standardized response metadata.

    Args:
        request_id: Unique identifier for the request, generated if not provided
        trace_id: Trace identifier for distributed tracing
        additional_info: Additional metadata information

    Returns:
        ResponseMetadata: Response metadata object
    """
    # Generate a new request_id if not provided
    if not request_id:
        request_id = str(uuid.uuid4())
    
    # Create and return metadata
    return ResponseMetadata(
        request_id=request_id,
        trace_id=trace_id,
        additional_info=additional_info
    )


def create_success_response(
    data: Any,
    message: str,
    request_id: Optional[str] = None,
    additional_info: Optional[Dict[str, Any]] = None
) -> DataResponse:
    """
    Creates a standardized success response with data.

    Args:
        data: The data payload to include in the response
        message: Descriptive message about the operation
        request_id: Unique identifier for the request
        additional_info: Additional metadata information

    Returns:
        DataResponse: Success response with data
    """
    # Create response metadata
    metadata = create_response_metadata(
        request_id=request_id,
        additional_info=additional_info
    )
    
    # Create and return success response
    return DataResponse(
        status=ResponseStatus.SUCCESS,
        message=message,
        metadata=metadata,
        data=data
    )


def create_list_response(
    items: List[Any],
    page: int,
    page_size: int,
    total_items: int,
    request: Request,
    message: str,
    request_id: Optional[str] = None,
    sort_by: Optional[str] = None,
    descending: Optional[bool] = None,
    additional_info: Optional[Dict[str, Any]] = None
) -> PaginatedResponse:
    """
    Creates a standardized paginated list response.

    Args:
        items: List of items to include in the response
        page: Current page number
        page_size: Number of items per page
        total_items: Total number of items across all pages
        request: FastAPI request object for pagination links
        message: Descriptive message about the operation
        request_id: Unique identifier for the request
        sort_by: Field used for sorting
        descending: Whether sorting is in descending order
        additional_info: Additional metadata information

    Returns:
        PaginatedResponse: Paginated list response
    """
    # Create response metadata
    metadata = create_response_metadata(
        request_id=request_id,
        additional_info=additional_info
    )
    
    # Create pagination metadata
    pagination = create_pagination_metadata(
        page=page,
        page_size=page_size,
        total_items=total_items,
        request=request,
        sort_by=sort_by,
        descending=descending
    )
    
    # Create and return paginated response
    return PaginatedResponse(
        status=ResponseStatus.SUCCESS,
        message=message,
        metadata=metadata,
        items=items,
        pagination=pagination
    )


def create_error_response(
    message: str,
    status_code: int,
    error_type: str,
    category: ErrorCategory,
    severity: ErrorSeverity,
    details: Optional[List[ErrorDetail]] = None,
    debug_info: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
    additional_info: Optional[Dict[str, Any]] = None
) -> ErrorResponse:
    """
    Creates a standardized error response.

    Args:
        message: Error message
        status_code: HTTP status code
        error_type: Type of error
        category: Error category
        severity: Error severity
        details: Detailed error information
        debug_info: Debug information for troubleshooting
        request_id: Unique identifier for the request
        additional_info: Additional metadata information

    Returns:
        ErrorResponse: Error response object
    """
    # Create response metadata
    metadata = create_response_metadata(
        request_id=request_id,
        additional_info=additional_info
    )
    
    # Create and return error response
    return ErrorResponse(
        status=ResponseStatus.ERROR,
        message=message,
        metadata=metadata,
        status_code=status_code,
        error_type=error_type,
        category=category,
        severity=severity,
        details=details,
        debug_info=debug_info
    )


def create_validation_error_response(
    message: str,
    validation_errors: List[ErrorDetail],
    request_id: Optional[str] = None,
    additional_info: Optional[Dict[str, Any]] = None
) -> ValidationErrorResponse:
    """
    Creates a standardized validation error response.

    Args:
        message: Error message
        validation_errors: List of validation errors
        request_id: Unique identifier for the request
        additional_info: Additional metadata information

    Returns:
        ValidationErrorResponse: Validation error response object
    """
    # Create response metadata
    metadata = create_response_metadata(
        request_id=request_id,
        additional_info=additional_info
    )
    
    # Create and return validation error response
    return ValidationErrorResponse(
        status=ResponseStatus.ERROR,
        message=message,
        metadata=metadata,
        status_code=422,  # Unprocessable Entity
        error_type="validation_error",
        validation_errors=validation_errors
    )


def handle_exception(
    exc: Exception,
    request_id: Optional[str] = None,
    include_traceback: bool = False
) -> Union[ErrorResponse, ValidationErrorResponse]:
    """
    Converts exceptions to standardized error responses.

    Args:
        exc: The exception to handle
        request_id: Unique identifier for the request
        include_traceback: Whether to include traceback in the response

    Returns:
        Union[ErrorResponse, ValidationErrorResponse]: Standardized error response
    """
    # Log exception details
    logger.exception(f"Exception occurred: {str(exc)}")
    
    # Include traceback in debug_info if requested
    debug_info = None
    if include_traceback:
        debug_info = {
            "traceback": traceback.format_exc()
        }
    
    # Handle ValidationError
    if isinstance(exc, ValidationError):
        return exc.to_response(request_id)
    
    # Handle PipelineError
    elif isinstance(exc, PipelineError):
        return exc.to_response(request_id)
    
    # Handle FastAPI HTTPException
    elif isinstance(exc, HTTPException):
        return create_error_response(
            message=str(exc.detail),
            status_code=exc.status_code,
            error_type="http_exception",
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            request_id=request_id,
            debug_info=debug_info
        )
    
    # Handle generic exceptions
    else:
        return create_error_response(
            message=str(exc),
            status_code=500,
            error_type="internal_server_error",
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            request_id=request_id,
            debug_info=debug_info
        )


def error_to_http_exception(error: ErrorResponse) -> HTTPException:
    """
    Converts an ErrorResponse to a FastAPI HTTPException.

    Args:
        error: The error response to convert

    Returns:
        HTTPException: FastAPI HTTPException
    """
    # Extract status_code and message
    status_code = error.status_code
    message = error.message
    
    # Include request_id in headers
    headers = {}
    if error.metadata and error.metadata.request_id:
        headers["X-Request-ID"] = error.metadata.request_id
    
    # Create and return HTTPException
    return HTTPException(
        status_code=status_code,
        detail=message,
        headers=headers
    )


def get_status_code_for_error(category: ErrorCategory) -> int:
    """
    Maps error categories to appropriate HTTP status codes.

    Args:
        category: Error category

    Returns:
        int: HTTP status code
    """
    # Map error categories to HTTP status codes
    status_codes = {
        ErrorCategory.VALIDATION: 422,  # Unprocessable Entity
        ErrorCategory.AUTHENTICATION: 401,  # Unauthorized
        ErrorCategory.AUTHORIZATION: 403,  # Forbidden
        ErrorCategory.RESOURCE_NOT_FOUND: 404,  # Not Found
        ErrorCategory.CONFIGURATION: 400,  # Bad Request
        ErrorCategory.EXTERNAL_SERVICE: 502,  # Bad Gateway
        ErrorCategory.DATA_QUALITY: 422,  # Unprocessable Entity
        ErrorCategory.PIPELINE_EXECUTION: 500,  # Internal Server Error
        ErrorCategory.SYSTEM: 500  # Internal Server Error
    }
    
    # Return the appropriate status code or 500 (Internal Server Error) if not found
    return status_codes.get(category, 500)