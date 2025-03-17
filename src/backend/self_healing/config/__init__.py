"""
Initialization module for the self-healing configuration package.

This module provides a centralized import point for all configuration-related 
functionality in the self-healing system, including healing configuration, 
strategy configuration, and risk management. It simplifies imports for other 
modules by exposing key classes and functions from the configuration submodules.
"""

# Internal imports
from .healing_config import (
    get_healing_config,
    get_healing_mode,
    get_confidence_threshold,
    get_max_retry_attempts,
    get_approval_required,
    is_action_type_enabled,
    get_learning_mode,
    get_rules_by_type,
    get_rule_by_id,
    get_rules_by_set,
    get_action_parameters,
    get_severity_threshold,
    reload_healing_config,
    HealingConfig
)

from .strategy_config import (
    StrategyCategory,
    Strategy,
    StrategyConfig,
    get_strategy_config,
    get_strategies_by_action_type,
    get_strategy_by_name,
    get_strategy_parameters,
    get_default_parameters,
    is_strategy_applicable,
    get_strategy_confidence_threshold,
    get_strategy_risk_level,
    get_strategy_max_attempts,
    reload_strategy_config
)

from .risk_management import (
    RiskLevel,
    ImpactCategory,
    RiskAssessment,
    RiskManager,
    calculate_risk_score,
    assess_impact,
    get_risk_threshold,
    requires_approval,
    load_risk_config
)

from ...utils.logging.logger import get_logger

# Set up module-level logger
logger = get_logger(__name__)

def reload_all_configs() -> bool:
    """Reloads all self-healing configuration modules
    
    Returns:
        bool: True if all configurations were successfully reloaded
    """
    logger.info("Reloading all self-healing configuration modules")
    
    try:
        # Reload healing configuration
        healing_config = reload_healing_config()
        
        # Reload strategy configuration
        strategy_config = reload_strategy_config()
        
        # Reload risk configuration by creating a new instance
        # which automatically loads the configuration
        risk_manager = RiskManager()
        
        logger.info("All self-healing configurations successfully reloaded")
        return True
    except Exception as e:
        logger.error(f"Error reloading self-healing configurations: {str(e)}")
        return False