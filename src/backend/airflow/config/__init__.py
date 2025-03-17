"""
Initialization module for the Airflow configuration package.

This module centralizes configuration loading and provides access to connection definitions,
pool configurations, and Airflow variables for the self-healing data pipeline. These
configuration objects are made available throughout the Airflow environment.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

from ...constants import ENV_DEVELOPMENT, ENV_STAGING, ENV_PRODUCTION
from ...utils.config.config_loader import load_yaml_config
from ...utils.config.environment import get_environment

# Set up logging
logger = logging.getLogger(__name__)

# Config directories and files
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
CONNECTIONS_FILE = os.path.join(CONFIG_DIR, 'connections.json')
POOL_CONFIG_FILE = os.path.join(CONFIG_DIR, 'pool_config.json')
VARIABLES_FILE = os.path.join(CONFIG_DIR, 'variables.json')

def load_connections() -> Dict[str, Dict[str, Any]]:
    """Loads connection configurations from the connections.json file.
    
    Returns:
        Dictionary of connection configurations
    """
    try:
        if not os.path.exists(CONNECTIONS_FILE):
            logger.warning(f"Connections file not found: {CONNECTIONS_FILE}")
            return {}
        
        with open(CONNECTIONS_FILE, 'r') as f:
            connections = json.load(f)
            logger.info(f"Loaded {len(connections)} connection configurations")
            return connections
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing connections file {CONNECTIONS_FILE}: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Error loading connections file {CONNECTIONS_FILE}: {str(e)}")
        return {}

def load_pools() -> Dict[str, Dict[str, Any]]:
    """Loads pool configurations from the pool_config.json file.
    
    Returns:
        Dictionary of pool configurations
    """
    try:
        if not os.path.exists(POOL_CONFIG_FILE):
            logger.warning(f"Pool config file not found: {POOL_CONFIG_FILE}")
            return {}
        
        with open(POOL_CONFIG_FILE, 'r') as f:
            pools = json.load(f)
            logger.info(f"Loaded {len(pools)} pool configurations")
            return pools
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing pool config file {POOL_CONFIG_FILE}: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Error loading pool config file {POOL_CONFIG_FILE}: {str(e)}")
        return {}

def load_variables() -> Dict[str, Any]:
    """Loads Airflow variables from the variables.json file.
    
    Returns:
        Dictionary of Airflow variables
    """
    try:
        if not os.path.exists(VARIABLES_FILE):
            logger.warning(f"Variables file not found: {VARIABLES_FILE}")
            return {}
        
        with open(VARIABLES_FILE, 'r') as f:
            variables = json.load(f)
            logger.info(f"Loaded {len(variables)} Airflow variables")
            return variables
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing variables file {VARIABLES_FILE}: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Error loading variables file {VARIABLES_FILE}: {str(e)}")
        return {}

def get_connection_config(conn_id: str) -> Dict[str, Any]:
    """Retrieves configuration for a specific connection.
    
    Args:
        conn_id: Connection identifier
        
    Returns:
        Connection configuration dictionary
    """
    if conn_id in connections:
        return connections[conn_id]
    
    logger.warning(f"Connection ID not found: {conn_id}")
    return {}

def get_pool_config(pool_name: str) -> Dict[str, Any]:
    """Retrieves configuration for a specific pool.
    
    Args:
        pool_name: Pool name
        
    Returns:
        Pool configuration dictionary
    """
    if pool_name in pools:
        return pools[pool_name]
    
    logger.warning(f"Pool name not found: {pool_name}")
    return {}

def get_variable(var_name: str, default_value: Any = None) -> Any:
    """Retrieves a specific Airflow variable.
    
    Args:
        var_name: Variable name
        default_value: Default value to return if variable not found
        
    Returns:
        Variable value or default if not found
    """
    if var_name in variables:
        return variables[var_name]
    
    logger.debug(f"Variable not found: {var_name}, using default value")
    return default_value

# Load configuration data on module import
connections = load_connections()
pools = load_pools()
variables = load_variables()