# Standard library imports
import os
import sys
import time
import platform
import psutil  # psutil version: ^5.9.0

# Third-party library imports
from fastapi import FastAPI, Request, HTTPException, status  # fastapi version: ^0.95.0
from fastapi.responses import JSONResponse  # fastapi version: ^0.95.0

# Internal module imports
from ..config import get_config  # src/backend/config.py
from ..constants import API_VERSION, API_PREFIX  # src/backend/constants.py
from ..utils.logging.logger import get_logger, setup_logging  # src/backend/utils/logging/logger.py
from .routes import routers  # src/backend/api/routes/__init__.py
from .middleware import AuthMiddleware, setup_cors_middleware, ErrorMiddleware, LoggingMiddleware  # src/backend/api/middleware/__init__.py
from .models.response_models import HealthCheckResponse  # src/backend/api/models/response_models.py
from .models.error_models import ResponseStatus, ResponseMetadata  # src/backend/api/models/error_models.py


# Initialize logger
logger = get_logger(__name__)

# Global variables
app = None
config = get_config()


def create_app() -> FastAPI:
    """Creates and configures the FastAPI application instance"""
    # Create FastAPI instance with title, description, and version
    fast_app = FastAPI(
        title="Self-Healing Data Pipeline API",
        description="API for managing and monitoring a self-healing data pipeline",
        version=API_VERSION,
    )

    # Configure CORS middleware
    setup_cors_middleware(fast_app)

    # Add authentication middleware
    fast_app.add_middleware(AuthMiddleware, exclude_paths=["/api/health", "/api/version"], auto_error=False)

    # Add error handling middleware
    fast_app.add_middleware(ErrorMiddleware, debug_mode=config.api.debug_mode, enable_self_healing=config.self_healing.enabled)

    # Add logging middleware
    fast_app.add_middleware(LoggingMiddleware, log_request_body=config.api.log_request_body, log_response_body=config.api.log_response_body)

    # Include all routers with appropriate prefixes
    for router in routers:
        fast_app.include_router(router, prefix=API_PREFIX)

    # Add health check endpoint
    @fast_app.get("/health", tags=["System"])
    async def get_health(request: Request) -> HealthCheckResponse:
        """Health check endpoint to verify API is operational"""
        # Collect system information (version, uptime)
        version = API_VERSION
        uptime = time.time() - request.app.start_time

        # Check component statuses (database, storage, etc.)
        # (Placeholder - implement actual component status checks)
        component_statuses = {
            "database": "OK",
            "storage": "OK",
            "scheduler": "OK"
        }

        # Gather system metrics (CPU, memory, disk)
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent

        # Return HealthCheckResponse with collected information
        return HealthCheckResponse(
            status=ResponseStatus.SUCCESS,
            message="API is operational",
            metadata=ResponseMetadata(version=version),
            version=version,
            components=component_statuses,
            system_metrics={
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "disk_usage": disk_usage,
                "uptime_seconds": uptime
            }
        )

    # Add version endpoint
    @fast_app.get("/version", tags=["System"])
    async def get_version() -> JSONResponse:
        """Endpoint to return the API version information"""
        # Get application version from constants
        version = API_VERSION

        # Return JSONResponse with version information
        return JSONResponse({"version": version})

    # Add exception handler for unhandled exceptions
    @fast_app.exception_handler(Exception)
    async def exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """Global exception handler for unhandled exceptions"""
        # Log the exception details
        logger.error(f"Unhandled exception: {exc}", exc_info=True)

        # Create error response with appropriate status code
        error_response = {
            "status": ResponseStatus.ERROR,
            "message": "Internal server error",
            "metadata": {"request_id": request.headers.get("X-Request-ID")},
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error_type": "Internal Server Error"
        }

        # Return JSONResponse with error information
        return JSONResponse(error_response, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Add exception handler for HTTPException instances
    @fast_app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        """Handler for HTTPException instances"""
        # Log the HTTP exception details
        logger.warning(f"HTTP exception: {exc.detail} (status code: {exc.status_code})")

        # Create error response with status code from exception
        error_response = {
            "status": ResponseStatus.ERROR,
            "message": exc.detail,
            "metadata": {"request_id": request.headers.get("X-Request-ID")},
            "status_code": exc.status_code,
            "error_type": "HTTP Exception"
        }

        # Return JSONResponse with error information
        return JSONResponse(error_response, status_code=exc.status_code)

    return fast_app


def configure_app(app: FastAPI) -> FastAPI:
    """Configures an existing FastAPI application with middleware and routes"""
    # Configure CORS middleware on the app
    setup_cors_middleware(app)

    # Add authentication middleware to the app
    app.add_middleware(AuthMiddleware, exclude_paths=["/api/health", "/api/version"], auto_error=False)

    # Add error handling middleware to the app
    app.add_middleware(ErrorMiddleware, debug_mode=config.api.debug_mode, enable_self_healing=config.self_healing.enabled)

    # Add logging middleware to the app
    app.add_middleware(LoggingMiddleware, log_request_body=config.api.log_request_body, log_response_body=config.api.log_response_body)

    # Include all routers with appropriate prefixes
    for router in routers:
        app.include_router(router, prefix=API_PREFIX)

    # Add health check endpoint
    @app.get("/health", tags=["System"])
    async def get_health(request: Request) -> HealthCheckResponse:
        """Health check endpoint to verify API is operational"""
        # Collect system information (version, uptime)
        version = API_VERSION
        uptime = time.time() - request.app.start_time

        # Check component statuses (database, storage, etc.)
        # (Placeholder - implement actual component status checks)
        component_statuses = {
            "database": "OK",
            "storage": "OK",
            "scheduler": "OK"
        }

        # Gather system metrics (CPU, memory, disk)
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent

        # Return HealthCheckResponse with collected information
        return HealthCheckResponse(
            status=ResponseStatus.SUCCESS,
            message="API is operational",
            metadata=ResponseMetadata(version=version),
            version=version,
            components=component_statuses,
            system_metrics={
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "disk_usage": disk_usage,
                "uptime_seconds": uptime
            }
        )

    return app


@app.get("/health", tags=["System"])
async def get_health(request: Request) -> HealthCheckResponse:
    """Health check endpoint to verify API is operational"""
    # Collect system information (version, uptime)
    version = API_VERSION
    uptime = time.time() - request.app.start_time

    # Check component statuses (database, storage, etc.)
    # (Placeholder - implement actual component status checks)
    component_statuses = {
        "database": "OK",
        "storage": "OK",
        "scheduler": "OK"
    }

    # Gather system metrics (CPU, memory, disk)
    cpu_usage = psutil.cpu_percent()
    memory_usage = psutil.virtual_memory().percent
    disk_usage = psutil.disk_usage('/').percent

    # Return HealthCheckResponse with collected information
    return HealthCheckResponse(
        status=ResponseStatus.SUCCESS,
        message="API is operational",
        metadata=ResponseMetadata(version=version),
        version=version,
        components=component_statuses,
        system_metrics={
            "cpu_usage": cpu_usage,
            "memory_usage": memory_usage,
            "disk_usage": disk_usage,
            "uptime_seconds": uptime
        }
    )


@app.get("/version", tags=["System"])
async def get_version() -> JSONResponse:
    """Endpoint to return the API version information"""
    # Get application version from constants
    version = API_VERSION

    # Return JSONResponse with version information
    return JSONResponse({"version": version})


@app.exception_handler(Exception)
async def exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global exception handler for unhandled exceptions"""
    # Log the exception details
    logger.error(f"Unhandled exception: {exc}", exc_info=True)

    # Create error response with appropriate status code
    error_response = {
        "status": ResponseStatus.ERROR,
        "message": "Internal server error",
        "metadata": {"request_id": request.headers.get("X-Request-ID")},
        "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
        "error_type": "Internal Server Error"
    }

    # Return JSONResponse with error information
    return JSONResponse(error_response, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handler for HTTPException instances"""
    # Log the HTTP exception details
    logger.warning(f"HTTP exception: {exc.detail} (status code: {exc.status_code})")

    # Create error response with status code from exception
    error_response = {
        "status": ResponseStatus.ERROR,
        "message": exc.detail,
        "metadata": {"request_id": request.headers.get("X-Request-ID")},
        "status_code": exc.status_code,
        "error_type": "HTTP Exception"
    }

    # Return JSONResponse with error information
    return JSONResponse(error_response, status_code=exc.status_code)


# Initialize the FastAPI app
app = create_app()

# Setup logging
setup_logging()

# Set start time for uptime calculation
app.start_time = time.time()