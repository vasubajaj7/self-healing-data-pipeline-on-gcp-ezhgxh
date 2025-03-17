"""
Cross-Origin Resource Sharing (CORS) middleware for the self-healing data pipeline API.

This module provides CORS middleware functionality to control which origins,
methods, and headers are allowed to access the API. It includes both a custom
CORS middleware implementation and convenience functions for FastAPI integration.

The middleware supports configurable CORS policies to allow controlled access from 
different origins, methods, and headers while maintaining security best practices.
"""

from typing import List, Dict, Optional, Callable, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware as FastAPICORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ../../config import get_config
from ../../utils.logging.logger import logger

# Default CORS settings
DEFAULT_ALLOW_ORIGINS = ["*"]
DEFAULT_ALLOW_METHODS = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
DEFAULT_ALLOW_HEADERS = ["*"]
DEFAULT_ALLOW_CREDENTIALS = False
DEFAULT_MAX_AGE = 600  # 10 minutes


def get_cors_config() -> Dict[str, Any]:
    """
    Retrieves CORS configuration from application settings.
    
    Returns:
        dict: CORS configuration dictionary
    """
    config = get_config()
    
    # Check if CORS is enabled
    cors_enabled = config.get("api.cors.enabled", True)
    if not cors_enabled:
        logger.info("CORS is disabled in configuration")
        return {"enabled": False}
    
    # Get CORS settings from config or use defaults
    return {
        "enabled": True,
        "allow_origins": config.get("api.cors.allow_origins", DEFAULT_ALLOW_ORIGINS),
        "allow_methods": config.get("api.cors.allow_methods", DEFAULT_ALLOW_METHODS),
        "allow_headers": config.get("api.cors.allow_headers", DEFAULT_ALLOW_HEADERS),
        "allow_credentials": config.get("api.cors.allow_credentials", DEFAULT_ALLOW_CREDENTIALS),
        "max_age": config.get("api.cors.max_age", DEFAULT_MAX_AGE)
    }


def setup_cors_middleware(app: FastAPI) -> None:
    """
    Configures and adds CORS middleware to a FastAPI application.
    
    Args:
        app: FastAPI application instance
    """
    # Get CORS configuration
    cors_config = get_cors_config()
    
    # Check if CORS is enabled
    if not cors_config.get("enabled", True):
        logger.info("CORS middleware not applied: disabled in configuration")
        return
    
    # Extract CORS settings
    allow_origins = cors_config.get("allow_origins", DEFAULT_ALLOW_ORIGINS)
    allow_methods = cors_config.get("allow_methods", DEFAULT_ALLOW_METHODS)
    allow_headers = cors_config.get("allow_headers", DEFAULT_ALLOW_HEADERS)
    allow_credentials = cors_config.get("allow_credentials", DEFAULT_ALLOW_CREDENTIALS)
    max_age = cors_config.get("max_age", DEFAULT_MAX_AGE)
    
    logger.info(f"Adding CORS middleware with origins: {allow_origins}, "
                f"methods: {allow_methods}, credentials: {allow_credentials}")
    
    # Add CORS middleware to FastAPI app
    app.add_middleware(
        FastAPICORSMiddleware,
        allow_origins=allow_origins,
        allow_methods=allow_methods,
        allow_headers=allow_headers,
        allow_credentials=allow_credentials,
        max_age=max_age,
    )


class CORSMiddleware(BaseHTTPMiddleware):
    """
    Custom CORS middleware implementation for the self-healing data pipeline API.
    
    This middleware handles Cross-Origin Resource Sharing (CORS) headers to control
    which origins, methods, and headers are allowed to access the API.
    """
    
    def __init__(
        self,
        app=None,
        allow_origins: List[str] = None,
        allow_methods: List[str] = None,
        allow_headers: List[str] = None,
        allow_credentials: bool = None,
        max_age: int = None
    ) -> None:
        """
        Initialize the CORS middleware with configuration settings.
        
        Args:
            app: ASGI application
            allow_origins: List of allowed origins, or ["*"] for all
            allow_methods: List of allowed HTTP methods
            allow_headers: List of allowed HTTP headers, or ["*"] for all
            allow_credentials: Whether to allow credentials (cookies)
            max_age: Maximum age of preflight requests in seconds
        """
        # Store CORS settings
        self.allow_origins = allow_origins or DEFAULT_ALLOW_ORIGINS
        self.allow_methods = allow_methods or DEFAULT_ALLOW_METHODS
        self.allow_headers = allow_headers or DEFAULT_ALLOW_HEADERS
        self.allow_credentials = DEFAULT_ALLOW_CREDENTIALS if allow_credentials is None else allow_credentials
        self.max_age = DEFAULT_MAX_AGE if max_age is None else max_age
        
        logger.debug(f"Initialized CORS middleware: origins={self.allow_origins}, "
                     f"methods={self.allow_methods}, headers={self.allow_headers}, "
                     f"credentials={self.allow_credentials}, max_age={self.max_age}")
        
        # Initialize BaseHTTPMiddleware if app is provided
        if app is not None:
            super().__init__(app)
    
    async def async_dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process each request through the CORS middleware.
        
        Args:
            request: Incoming HTTP request
            call_next: Function to call the next middleware/route handler
            
        Returns:
            Response with CORS headers
        """
        # Get the origin from request headers
        origin = request.headers.get("origin")
        
        # Handle preflight (OPTIONS) requests
        if request.method == "OPTIONS" and origin:
            # Create a new response for the preflight request
            response = Response(status_code=200)
            
            # Add CORS headers to the response
            self.add_cors_headers(response, origin)
            
            # Add preflight-specific headers if they exist in the request
            request_method = request.headers.get("access-control-request-method")
            if request_method:
                response.headers["access-control-allow-methods"] = ", ".join(self.allow_methods)
            
            request_headers = request.headers.get("access-control-request-headers")
            if request_headers:
                if "*" in self.allow_headers:
                    response.headers["access-control-allow-headers"] = request_headers
                else:
                    response.headers["access-control-allow-headers"] = ", ".join(self.allow_headers)
            
            return response
        
        # For regular requests, call the next middleware/route handler
        try:
            response = await call_next(request)
            
            # Add CORS headers to the response if origin header is present
            if origin:
                self.add_cors_headers(response, origin)
            
            return response
        except Exception as e:
            # Ensure CORS headers are added even for error responses
            # Create a response for the error case
            response = Response(content=str(e), status_code=500)
            
            # Add CORS headers if origin header is present
            if origin:
                self.add_cors_headers(response, origin)
            
            # Re-raise the exception
            raise
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        BaseHTTPMiddleware dispatch method that delegates to async_dispatch.
        
        Args:
            request: Incoming HTTP request
            call_next: Function to call the next middleware or route handler
            
        Returns:
            Response with CORS headers
        """
        return await self.async_dispatch(request, call_next)
    
    def is_origin_allowed(self, origin: str) -> bool:
        """
        Check if a given origin is allowed by the CORS policy.
        
        Args:
            origin: Origin to check
            
        Returns:
            True if origin is allowed, False otherwise
        """
        # If "*" is in allowed origins, all origins are allowed
        if "*" in self.allow_origins:
            return True
        
        # Otherwise, check if the specific origin is allowed
        return origin in self.allow_origins
    
    def add_cors_headers(self, response: Response, origin: str) -> None:
        """
        Add CORS headers to a response.
        
        Args:
            response: HTTP response to modify
            origin: Origin from the request
        """
        # Check if the origin is allowed
        if self.is_origin_allowed(origin):
            # Add origin header
            if "*" in self.allow_origins:
                response.headers["access-control-allow-origin"] = "*"
            else:
                response.headers["access-control-allow-origin"] = origin
            
            # Add credentials header if enabled
            if self.allow_credentials:
                response.headers["access-control-allow-credentials"] = "true"
            
            # Add allowed methods
            response.headers["access-control-allow-methods"] = ", ".join(self.allow_methods)
            
            # Add allowed headers
            if "*" in self.allow_headers:
                response.headers["access-control-allow-headers"] = "*"
            else:
                response.headers["access-control-allow-headers"] = ", ".join(self.allow_headers)
            
            # Add max age
            response.headers["access-control-max-age"] = str(self.max_age)