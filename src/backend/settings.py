"""
Application settings module that initializes and configures the self-healing data pipeline.

This module centralizes the setup of configuration, logging, and other core services
to ensure consistent initialization across the application. It provides a single point
of entry for initializing all required components and services.

The module handles:
- Configuration loading and management
- Logging setup and configuration
- Error reporting integration with Google Cloud
- Distributed tracing with Cloud Trace
- Exception handling and reporting
"""

import os
import sys
import logging
from typing import Optional, Dict, Any

# Google Cloud services for error reporting and tracing
from google.cloud import error_reporting  # version 1.5.0+
from google.cloud import trace  # version 1.5.0+

# Internal imports
from config import get_config
from constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING, 
    ENV_PRODUCTION,
    DEFAULT_CONFIG_PATH
)
from logging_config import setup_logging, get_logger

# Module-level logger
logger = logging.getLogger(__name__)

# Global service clients
app_config = None
error_client = None
trace_client = None

def initialize_app(config_path: str = None) -> None:
    """
    Initializes the application by setting up configuration, logging, and services.
    
    This is the main entry point for application initialization and should be called
    before any other components are used.
    
    Args:
        config_path: Path to the configuration file. If None, uses DEFAULT_CONFIG_PATH.
    """
    global app_config
    
    # Initialize configuration
    config_path = config_path or DEFAULT_CONFIG_PATH
    logger.info(f"Initializing application with config from: {config_path}")
    app_config = get_config(config_path)
    
    # Set up logging based on configuration
    log_level = app_config.get_log_level()
    environment = app_config.get_environment()
    setup_logging(log_level, environment)
    
    logger.info(f"Application environment: {environment}")
    
    # Initialize Google Cloud services in non-development environments
    if environment in [ENV_STAGING, ENV_PRODUCTION]:
        initialize_error_reporting()
        initialize_tracing()
    
    logger.info("Application initialization complete")

def initialize_error_reporting() -> None:
    """
    Initializes Google Cloud Error Reporting client.
    
    Sets up error reporting to automatically capture and report errors
    to Google Cloud Error Reporting service.
    """
    global error_client
    
    try:
        # Get project ID from configuration
        project_id = app_config.get_gcp_project_id()
        if not project_id:
            logger.warning("GCP project ID not found, error reporting not initialized")
            return
        
        # Create error reporting client
        error_client = error_reporting.Client(project=project_id)
        logger.info(f"Error reporting initialized for project: {project_id}")
    except Exception as e:
        logger.error(f"Error initializing error reporting: {e}")

def initialize_tracing() -> None:
    """
    Initializes Google Cloud Trace client for distributed tracing.
    
    Sets up distributed tracing to track request flows across
    different components of the application.
    """
    global trace_client
    
    try:
        # Get project ID from configuration
        project_id = app_config.get_gcp_project_id()
        if not project_id:
            logger.warning("GCP project ID not found, tracing not initialized")
            return
        
        # Create trace client
        trace_client = trace.Client(project=project_id)
        logger.info(f"Distributed tracing initialized for project: {project_id}")
    except Exception as e:
        logger.error(f"Error initializing distributed tracing: {e}")

def get_app_config():
    """
    Returns the application configuration instance.
    
    Ensures the application is initialized before returning the configuration.
    
    Returns:
        Config: Application configuration instance
    """
    global app_config
    
    if app_config is None:
        # Initialize app if not already initialized
        initialize_app()
    
    return app_config

def get_error_client():
    """
    Returns the Google Cloud Error Reporting client.
    
    Initializes error reporting if not already initialized.
    
    Returns:
        google.cloud.error_reporting.Client: Error reporting client
        or None if not initialized
    """
    global error_client
    
    if error_client is None and app_config:
        # Only initialize if app is configured and we're in a cloud environment
        environment = app_config.get_environment()
        if environment in [ENV_STAGING, ENV_PRODUCTION]:
            initialize_error_reporting()
    
    return error_client

def get_trace_client():
    """
    Returns the Google Cloud Trace client.
    
    Initializes tracing if not already initialized.
    
    Returns:
        google.cloud.trace.Client: Trace client
        or None if not initialized
    """
    global trace_client
    
    if trace_client is None and app_config:
        # Only initialize if app is configured and we're in a cloud environment
        environment = app_config.get_environment()
        if environment in [ENV_STAGING, ENV_PRODUCTION]:
            initialize_tracing()
    
    return trace_client

def report_exception(exception: Exception, context: dict = None) -> None:
    """
    Reports an exception to Google Cloud Error Reporting.
    
    This method handles both logging the exception locally and
    reporting it to Google Cloud Error Reporting service.
    
    Args:
        exception: The exception to report
        context: Additional context information for the exception
    """
    # Default to empty dict if no context provided
    context = context or {}
    
    # Log the exception locally
    logger.exception(f"Exception occurred: {str(exception)}", extra=context)
    
    # Report to error reporting service if available
    client = get_error_client()
    if client:
        try:
            client.report_exception(
                http_context=context.get('http_context'),
                user=context.get('user'),
                service=context.get('service', 'self-healing-pipeline')
            )
            logger.info("Exception reported to Error Reporting service")
        except Exception as reporting_error:
            logger.error(f"Failed to report exception to Error Reporting: {reporting_error}")