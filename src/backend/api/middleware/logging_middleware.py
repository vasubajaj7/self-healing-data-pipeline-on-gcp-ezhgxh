"""
Middleware for logging API requests and responses in the self-healing data pipeline.

This middleware captures request/response details, execution time, and contextual information
for comprehensive API monitoring and troubleshooting. It provides consistent logging format
across all API endpoints and supports request tracing through correlation IDs.
"""

import time
import typing

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send
import fastapi

from ...utils.logging.logger import get_logger, set_context, clear_context, generate_correlation_id
from ...config import get_config

# Initialize logger
logger = get_logger(__name__)

# Constants
REQUEST_ID_HEADER = "X-Request-ID"
EXCLUDED_PATHS = ['/api/health', '/api/docs', '/api/redoc', '/api/openapi.json']
EXCLUDED_HEADERS = ["authorization", "cookie"]


def get_request_id(request: Request) -> str:
    """
    Extracts or generates a request ID from the request headers.

    Args:
        request: The HTTP request object

    Returns:
        A request ID string for tracing and correlation
    """
    # Check if request ID is in headers
    if REQUEST_ID_HEADER.lower() in request.headers:
        return request.headers[REQUEST_ID_HEADER.lower()]
    
    # Generate a new correlation ID if none exists
    return generate_correlation_id()


def extract_request_info(request: Request) -> dict:
    """
    Extracts relevant information from the request for logging.

    Args:
        request: The HTTP request object

    Returns:
        Dictionary containing request information
    """
    # Extract basic request information
    info = {
        "method": request.method,
        "path": request.url.path,
        "client_ip": request.client.host if request.client else "unknown",
    }
    
    # Add query parameters if present
    if request.query_params:
        info["query_params"] = dict(request.query_params)
    
    # Add headers (excluding sensitive ones)
    if request.headers:
        headers = {}
        for name, value in request.headers.items():
            if name.lower() not in EXCLUDED_HEADERS:
                headers[name] = value
        
        if headers:
            info["headers"] = headers
    
    return info


def extract_response_info(response: Response) -> dict:
    """
    Extracts relevant information from the response for logging.

    Args:
        response: The HTTP response object

    Returns:
        Dictionary containing response information
    """
    # Extract basic response information
    info = {
        "status_code": response.status_code,
    }
    
    # Add headers (excluding sensitive ones)
    if response.headers:
        headers = {}
        for name, value in response.headers.items():
            if name.lower() not in EXCLUDED_HEADERS:
                headers[name] = value
        
        if headers:
            info["headers"] = headers
    
    # Add content type if present
    if "content-type" in response.headers:
        info["content_type"] = response.headers["content-type"]
    
    return info


def should_log_path(path: str) -> bool:
    """
    Determines if the request path should be logged.

    Args:
        path: The request path

    Returns:
        True if path should be logged, False otherwise
    """
    return path not in EXCLUDED_PATHS


class LoggingMiddleware:
    """
    Middleware for logging API requests and responses with timing and context information.
    
    This middleware logs detailed information about incoming requests and outgoing responses,
    including execution time and correlation IDs for request tracing. It can optionally
    log request and response bodies for more detailed debugging.
    """
    
    def __init__(
        self,
        log_request_body: bool = False,
        log_response_body: bool = False,
        exclude_paths: typing.List[str] = None,
    ):
        """
        Initialize the logging middleware with configuration options.
        
        Args:
            log_request_body: Whether to log request bodies (default: False)
            log_response_body: Whether to log response bodies (default: False)
            exclude_paths: List of paths to exclude from logging (default: EXCLUDED_PATHS)
        """
        self.log_request_body = log_request_body
        self.log_response_body = log_response_body
        self.exclude_paths = exclude_paths or EXCLUDED_PATHS
        
        logger.info(
            f"Initialized LoggingMiddleware with log_request_body={log_request_body}, "
            f"log_response_body={log_response_body}, exclude_paths={self.exclude_paths}"
        )
    
    async def async_dispatch(
        self, request: Request, call_next: typing.Callable
    ) -> Response:
        """
        Process the request through the middleware, logging request and response details.
        
        Args:
            request: The HTTP request
            call_next: The next middleware or endpoint in the chain
            
        Returns:
            The HTTP response from the endpoint
        """
        # Get or generate request ID
        request_id = get_request_id(request)
        
        # Check if path should be logged
        if not self.is_path_excluded(request.url.path):
            # Extract request information for logging
            request_info = extract_request_info(request)
            
            # Set logging context with request ID and information
            context = {
                "correlation_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "client_ip": request.client.host if request.client else "unknown",
            }
            set_context(context)
            
            # Record start time
            start_time = time.time()
            
            # Log the incoming request
            logger.info(
                f"Request received: {request.method} {request.url.path}",
                extra={"request_info": request_info},
            )
            
            # Log request body if enabled
            await self.log_request_body_if_enabled(request)
        
        try:
            # Process the request
            response = await call_next(request)
            
            # Calculate request processing time and log response if needed
            if not self.is_path_excluded(request.url.path):
                process_time = time.time() - start_time
                
                # Extract response information
                response_info = extract_response_info(response)
                
                # Log the response
                logger.info(
                    f"Response sent: {response.status_code} - {request.method} {request.url.path} "
                    f"- Took {process_time:.4f}s",
                    extra={"response_info": response_info, "process_time": process_time},
                )
                
                # Log response body if enabled
                await self.log_response_body_if_enabled(response)
                
                # Clear logging context
                clear_context()
            
            # Add request ID to response headers for tracing
            response.headers[REQUEST_ID_HEADER] = request_id
            
            return response
            
        except Exception as e:
            # For unhandled exceptions, log error with context
            if not self.is_path_excluded(request.url.path):
                process_time = time.time() - start_time
                logger.error(
                    f"Unhandled exception in {request.method} {request.url.path}: {str(e)}",
                    exc_info=True,
                    extra={"process_time": process_time},
                )
                clear_context()
            
            # Re-raise the exception
            raise
    
    async def __call__(self, scope: dict, receive: typing.Callable, send: typing.Callable):
        """
        Make the class callable as ASGI middleware.
        
        Args:
            scope: The ASGI connection scope
            receive: The ASGI receive function
            send: The ASGI send function
            
        Returns:
            ASGI application coroutine
        """
        # Only process HTTP requests
        if scope["type"] != "http":
            # Pass through to the next middleware
            app = scope.get("app")
            if app:
                await app(scope, receive, send)
            return
        
        # Create request object
        request = Request(scope)
        
        # Create an async function to handle the request
        async def call_next(request):
            app = scope.get("app")
            if not app:
                raise RuntimeError("No app found in middleware scope")
                
            response_sender = ResponseSender(send)
            await app(scope, receive, response_sender.send)
            return await response_sender.response()
        
        # Process the request through our middleware
        response = await self.async_dispatch(request, call_next)
        
        # Send the response
        await response(scope, receive, send)
    
    def is_path_excluded(self, path: str) -> bool:
        """
        Check if a path should be excluded from logging.
        
        Args:
            path: The request path
            
        Returns:
            True if path is excluded, False otherwise
        """
        # Check for exact match
        if path in self.exclude_paths:
            return True
        
        # Check for path prefix match
        for excluded_path in self.exclude_paths:
            if excluded_path.endswith('*') and path.startswith(excluded_path[:-1]):
                return True
        
        return False
    
    async def log_request_body_if_enabled(self, request: Request) -> None:
        """
        Log request body if enabled in configuration.
        
        Args:
            request: The HTTP request
        """
        if not self.log_request_body:
            return
        
        try:
            # Read request body
            body = await request.body()
            
            # Attempt to decode as text
            try:
                body_text = body.decode('utf-8')
                logger.debug(f"Request body: {body_text}")
            except UnicodeDecodeError:
                logger.debug(f"Request body: <binary data of length {len(body)}>")
                
        except Exception as e:
            logger.warning(f"Failed to log request body: {str(e)}")
    
    async def log_response_body_if_enabled(self, response: Response) -> None:
        """
        Log response body if enabled in configuration.
        
        Args:
            response: The HTTP response
        """
        if not self.log_response_body:
            return
        
        try:
            # Get response body if available
            if hasattr(response, 'body'):
                body = response.body
                
                # Attempt to decode as text
                try:
                    body_text = body.decode('utf-8')
                    logger.debug(f"Response body: {body_text}")
                except UnicodeDecodeError:
                    logger.debug(f"Response body: <binary data of length {len(body)}>")
                    
        except Exception as e:
            logger.warning(f"Failed to log response body: {str(e)}")


# Helper class for capturing response in ASGI middleware
class ResponseSender:
    def __init__(self, send):
        self.send = send
        self.response_started = False
        self._response_body = []
    
    async def send(self, message):
        if message["type"] == "http.response.start":
            self.response_started = True
            self.status = message["status"]
            self.headers = message.get("headers", [])
        elif message["type"] == "http.response.body":
            self._response_body.append(message.get("body", b""))
        
        await self.send(message)
    
    async def response(self):
        if not self.response_started:
            raise RuntimeError("Response not started")
        
        body = b"".join(self._response_body)
        return Response(
            content=body,
            status_code=self.status,
            headers=dict([(k.decode('utf-8'), v.decode('utf-8')) for k, v in self.headers])
        )