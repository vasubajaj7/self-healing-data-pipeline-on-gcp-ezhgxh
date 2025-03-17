"""
Initialization module for the Airflow package in the self-healing data pipeline.

This module serves as the entry point for the Airflow integration, providing version
information, configuration access, and importing necessary submodules to make them
available throughout the application.
"""

import os
import importlib

from ..config import get_config
from ..logging_config import configure_logging
from ..constants import DEFAULT_LOG_LEVEL
from ..utils.logging.logger import get_logger

# Version information
VERSION = "1.0.0"

# Set up logger for this module
logger = get_logger(__name__)

# Get the absolute path to the Airflow module directory
AIRFLOW_MODULE_PATH = os.path.dirname(os.path.abspath(__file__))

# Initialize configuration instance
config_instance = get_config()


def init_airflow():
    """
    Initializes the Airflow module by configuring logging and importing submodules.
    
    This function should be called when the Airflow module is first used to ensure
    proper initialization.
    """
    # Configure logging for the Airflow module
    configure_logging(log_level=config_instance.get_log_level())
    
    logger.info("Initializing Airflow module...")
    
    # Import submodules to make them available
    try:
        # Import plugins module to register custom components
        from . import plugins
        logger.debug("Loaded Airflow plugins module")
        
        # Import dags module to make DAG definitions available
        from . import dags
        logger.debug("Loaded Airflow dags module")
        
        # Import config module to load Airflow configurations
        from . import config
        logger.debug("Loaded Airflow config module")
        
    except ImportError as e:
        logger.warning(f"Error importing Airflow submodules: {e}")
    
    logger.info("Airflow module initialized successfully")


def get_version():
    """
    Returns the version of the Airflow module.
    
    Returns:
        str: Version string
    """
    return VERSION


def import_submodules():
    """
    Dynamically imports all submodules of the Airflow package.
    
    This function walks through all subdirectories of the Airflow module and
    imports them as Python modules if they contain an __init__.py file.
    
    Returns:
        dict: Dictionary of imported modules
    """
    modules = {}
    
    for dir_name in os.listdir(AIRFLOW_MODULE_PATH):
        # Skip non-directories and special directories
        dir_path = os.path.join(AIRFLOW_MODULE_PATH, dir_name)
        if not os.path.isdir(dir_path) or dir_name.startswith('__'):
            continue
        
        # Check if directory is a Python package (has __init__.py)
        init_file = os.path.join(dir_path, '__init__.py')
        if not os.path.exists(init_file):
            continue
        
        # Import the module
        module_name = f"{__name__}.{dir_name}"
        try:
            module = importlib.import_module(module_name)
            modules[dir_name] = module
            logger.debug(f"Imported Airflow submodule: {dir_name}")
        except ImportError as e:
            logger.warning(f"Error importing Airflow submodule {module_name}: {e}")
    
    return modules