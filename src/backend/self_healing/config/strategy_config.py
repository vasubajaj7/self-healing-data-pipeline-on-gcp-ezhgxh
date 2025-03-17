"""
Configuration module for healing strategies in the self-healing data pipeline.

This module defines the configuration for healing strategies, including strategy types,
parameters, applicability rules, and execution settings. It serves as a bridge between
the healing configuration and the resolution selection process.
"""

import os
import typing
import yaml
import enum
from typing import Dict, List, Any, Optional, Union

from ...constants import HealingActionType, DEFAULT_CONFIDENCE_THRESHOLD
from ...config import get_config
from ...utils.logging.logger import get_logger
from .healing_config import get_healing_config
from .risk_management import RiskLevel

# Configure module logger
logger = get_logger(__name__)

# Default path for strategy configuration
DEFAULT_STRATEGY_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'configs',
    'healing_strategies.yaml'
)

# Cache for loaded strategy configuration
_strategy_config_cache = None


class StrategyCategory(enum.Enum):
    """Enumeration of strategy categories for different healing approaches."""
    DATA_CORRECTION = "DATA_CORRECTION"
    PIPELINE_ADJUSTMENT = "PIPELINE_ADJUSTMENT"
    RESOURCE_OPTIMIZATION = "RESOURCE_OPTIMIZATION"
    SCHEMA_EVOLUTION = "SCHEMA_EVOLUTION"
    DEPENDENCY_RESOLUTION = "DEPENDENCY_RESOLUTION"


class Strategy:
    """Class representing a healing strategy with its configuration and execution details."""
    
    def __init__(self, name: str, description: str, category: StrategyCategory,
                 action_type: HealingActionType, parameters: Dict[str, Any],
                 applicability_rules: Dict[str, Any], execution_settings: Dict[str, Any],
                 success_rate: float = 0.0):
        """Initialize a strategy with its configuration.
        
        Args:
            name: Strategy name
            description: Strategy description
            category: Strategy category
            action_type: Type of healing action
            parameters: Strategy parameters
            applicability_rules: Rules for when strategy is applicable
            execution_settings: Settings for strategy execution
            success_rate: Historical success rate of the strategy
        """
        self.name = name
        self.description = description
        self.category = category
        self.action_type = action_type
        self.parameters = parameters
        self.applicability_rules = applicability_rules
        self.execution_settings = execution_settings
        self.success_rate = success_rate
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy to dictionary representation.
        
        Returns:
            Dictionary representation of the strategy
        """
        return {
            'name': self.name,
            'description': self.description,
            'category': self.category.value,
            'action_type': self.action_type.value,
            'parameters': self.parameters,
            'applicability_rules': self.applicability_rules,
            'execution_settings': self.execution_settings,
            'success_rate': self.success_rate
        }
    
    @classmethod
    def from_dict(cls, strategy_dict: Dict[str, Any]) -> 'Strategy':
        """Create Strategy from dictionary representation.
        
        Args:
            strategy_dict: Dictionary containing strategy data
            
        Returns:
            Strategy instance
        """
        # Convert string values to enum types
        category = StrategyCategory(strategy_dict.get('category'))
        action_type = HealingActionType(strategy_dict.get('action_type'))
        
        return cls(
            name=strategy_dict.get('name', ''),
            description=strategy_dict.get('description', ''),
            category=category,
            action_type=action_type,
            parameters=strategy_dict.get('parameters', {}),
            applicability_rules=strategy_dict.get('applicability_rules', {}),
            execution_settings=strategy_dict.get('execution_settings', {}),
            success_rate=strategy_dict.get('success_rate', 0.0)
        )
    
    def is_applicable(self, context: Dict[str, Any]) -> bool:
        """Check if strategy is applicable for a given context.
        
        Args:
            context: Context information for evaluation
            
        Returns:
            True if strategy is applicable, False otherwise
        """
        # If no rules defined, assume always applicable
        if not self.applicability_rules:
            return True
        
        # Evaluate the rules against the context
        return evaluate_rules(self.applicability_rules, context)
    
    def get_confidence_threshold(self) -> float:
        """Get the confidence threshold for this strategy.
        
        Returns:
            Confidence threshold value
        """
        threshold = self.execution_settings.get('confidence_threshold')
        if threshold is not None:
            return float(threshold)
        return DEFAULT_CONFIDENCE_THRESHOLD
    
    def get_risk_level(self) -> RiskLevel:
        """Get the risk level for this strategy.
        
        Returns:
            Risk level enum value
        """
        risk_level_str = self.execution_settings.get('risk_level', 'MEDIUM')
        try:
            return RiskLevel[risk_level_str]
        except (KeyError, ValueError):
            return RiskLevel.MEDIUM
    
    def get_max_attempts(self) -> int:
        """Get the maximum execution attempts for this strategy.
        
        Returns:
            Maximum attempts value
        """
        max_attempts = self.execution_settings.get('max_attempts', 3)
        return int(max_attempts)


class StrategyConfig:
    """Class that manages strategy-specific configuration settings."""
    
    def __init__(self, config_path: str = None):
        """Initializes the StrategyConfig instance with the specified configuration path.
        
        Args:
            config_path: Path to the configuration file. If None, uses default path.
        """
        self._config = None
        self._strategy_config = None
        self._config_path = config_path or DEFAULT_STRATEGY_CONFIG_PATH
        self._initialized = False
        self._load_config()
    
    def _load_config(self):
        """Loads strategy configuration from file."""
        # Get core configuration
        self._config = get_config()
        
        # Load strategy-specific configuration
        self._strategy_config = load_strategy_config(self._config_path)
        
        # Validate the configuration
        if not validate_strategy_config(self._strategy_config):
            logger.warning(f"Invalid strategy configuration loaded from {self._config_path}. Using empty config.")
            self._strategy_config = {}
        
        self._initialized = True
        logger.info(f"Strategy configuration loaded from {self._config_path}")
    
    def get_strategies_by_action_type(self, action_type: HealingActionType) -> List[Dict[str, Any]]:
        """Gets strategies applicable for a specific healing action type.
        
        Args:
            action_type: The action type to filter strategies by
            
        Returns:
            List of strategy configurations matching the action type
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
        
        # Get strategies list
        strategies = self._strategy_config.get('strategies', [])
        
        # Filter strategies by action type
        return [s for s in strategies if s.get('action_type') == action_type.value]
    
    def get_strategy_by_name(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """Gets a specific strategy configuration by its name.
        
        Args:
            strategy_name: The name of the strategy to get
            
        Returns:
            Strategy configuration dictionary or None if not found
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
        
        # Get strategies list
        strategies = self._strategy_config.get('strategies', [])
        
        # Find strategy with matching name
        for strategy in strategies:
            if strategy.get('name') == strategy_name:
                return strategy
        
        # Strategy not found
        return None
    
    def get_strategy_parameters(self, strategy_name: str) -> Dict[str, Any]:
        """Gets parameters for a specific strategy.
        
        Args:
            strategy_name: The name of the strategy
            
        Returns:
            Strategy parameters dictionary
        """
        # Get strategy configuration
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            return {}
        
        # Return parameters or empty dict if not found
        return strategy.get('parameters', {})
    
    def get_default_parameters(self, strategy_type: str) -> Dict[str, Any]:
        """Gets default parameters for a strategy type.
        
        Args:
            strategy_type: The type of the strategy
            
        Returns:
            Default parameters dictionary
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
        
        # Get default parameters for the strategy type
        default_params = self._strategy_config.get('default_parameters', {})
        return default_params.get(strategy_type, {})
    
    def is_strategy_applicable(self, strategy_name: str, context: Dict[str, Any]) -> bool:
        """Checks if a strategy is applicable for a given context.
        
        Args:
            strategy_name: The name of the strategy
            context: Context information for evaluation
            
        Returns:
            True if strategy is applicable, False otherwise
        """
        # Get strategy configuration
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            return False
        
        # Get applicability rules
        applicability_rules = strategy.get('applicability_rules', {})
        
        # If no rules defined, assume always applicable
        if not applicability_rules:
            return True
        
        # Evaluate the rules against the context
        return evaluate_rules(applicability_rules, context)
    
    def get_strategy_confidence_threshold(self, strategy_name: str) -> float:
        """Gets the confidence threshold for a specific strategy.
        
        Args:
            strategy_name: The name of the strategy
            
        Returns:
            Confidence threshold value
        """
        # Get strategy configuration
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            return DEFAULT_CONFIDENCE_THRESHOLD
        
        # Get execution settings
        execution_settings = strategy.get('execution_settings', {})
        
        # Get threshold from settings or use default
        threshold = execution_settings.get('confidence_threshold')
        if threshold is not None:
            return float(threshold)
        
        return DEFAULT_CONFIDENCE_THRESHOLD
    
    def get_strategy_risk_level(self, strategy_name: str) -> RiskLevel:
        """Gets the risk level for a specific strategy.
        
        Args:
            strategy_name: The name of the strategy
            
        Returns:
            Risk level enum value
        """
        # Get strategy configuration
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            return RiskLevel.MEDIUM
        
        # Get execution settings
        execution_settings = strategy.get('execution_settings', {})
        
        # Get risk level from settings or use default
        risk_level_str = execution_settings.get('risk_level', 'MEDIUM')
        try:
            return RiskLevel[risk_level_str]
        except (KeyError, ValueError):
            return RiskLevel.MEDIUM
    
    def get_strategy_max_attempts(self, strategy_name: str) -> int:
        """Gets the maximum execution attempts for a specific strategy.
        
        Args:
            strategy_name: The name of the strategy
            
        Returns:
            Maximum attempts value
        """
        # Get strategy configuration
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            return 3  # Default max attempts
        
        # Get execution settings
        execution_settings = strategy.get('execution_settings', {})
        
        # Get max attempts from settings or use default
        max_attempts = execution_settings.get('max_attempts', 3)
        return int(max_attempts)
    
    def reload(self) -> None:
        """Reloads the strategy configuration from file."""
        self._initialized = False
        self._load_config()
        logger.info("Strategy configuration reloaded")


def get_strategy_config(config_path: str = None) -> Dict[str, Any]:
    """Gets the strategy configuration, loading it if not already cached.
    
    Args:
        config_path: Path to the configuration file. If None, uses default path.
        
    Returns:
        Strategy configuration dictionary
    """
    global _strategy_config_cache
    
    if _strategy_config_cache is None:
        file_path = config_path or DEFAULT_STRATEGY_CONFIG_PATH
        config = load_strategy_config(file_path)
        
        if not validate_strategy_config(config):
            logger.warning(f"Invalid strategy configuration loaded from {file_path}. Using empty config.")
            config = {}
        
        _strategy_config_cache = config
        logger.info(f"Strategy configuration loaded from {file_path}")
    
    return _strategy_config_cache


def load_strategy_config(file_path: str) -> Dict[str, Any]:
    """Loads strategy configuration from a YAML file.
    
    Args:
        file_path: Path to the YAML configuration file
        
    Returns:
        Strategy configuration dictionary or empty dict if file not found
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Strategy configuration file not found: {file_path}")
            return {}
        
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
            return config or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML strategy configuration file {file_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error loading strategy configuration file {file_path}: {e}")
        return {}


def validate_strategy_config(config: Dict[str, Any]) -> bool:
    """Validates the structure and content of the strategy configuration.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid, False otherwise
    """
    if not isinstance(config, dict):
        logger.error("Strategy configuration must be a dictionary")
        return False
    
    # Check required top-level sections
    required_sections = ['strategy_types', 'strategy_parameters', 'applicability_rules', 'execution_settings']
    for section in required_sections:
        if section not in config:
            logger.error(f"Strategy configuration missing required section: {section}")
            return False
    
    # Validate strategy_types section
    strategy_types = config.get('strategy_types', {})
    if not isinstance(strategy_types, dict):
        logger.error("strategy_types must be a dictionary")
        return False
    
    # Validate strategy_parameters section
    strategy_parameters = config.get('strategy_parameters', {})
    if not isinstance(strategy_parameters, dict):
        logger.error("strategy_parameters must be a dictionary")
        return False
    
    # Validate applicability_rules section
    applicability_rules = config.get('applicability_rules', {})
    if not isinstance(applicability_rules, dict):
        logger.error("applicability_rules must be a dictionary")
        return False
    
    # Validate execution_settings section
    execution_settings = config.get('execution_settings', {})
    if not isinstance(execution_settings, dict):
        logger.error("execution_settings must be a dictionary")
        return False
    
    # If we get here, configuration is valid
    return True


def get_strategies_by_action_type(action_type: HealingActionType) -> List[Dict[str, Any]]:
    """Gets strategies applicable for a specific healing action type.
    
    Args:
        action_type: The action type to filter strategies by
        
    Returns:
        List of strategy configurations matching the action type
    """
    # Get strategy configuration
    config = get_strategy_config()
    
    # Get strategies list
    strategies = config.get('strategies', [])
    
    # Filter strategies by action type
    return [s for s in strategies if s.get('action_type') == action_type.value]


def get_strategy_by_name(strategy_name: str) -> Optional[Dict[str, Any]]:
    """Gets a specific strategy configuration by its name.
    
    Args:
        strategy_name: The name of the strategy to get
        
    Returns:
        Strategy configuration dictionary or None if not found
    """
    # Get strategy configuration
    config = get_strategy_config()
    
    # Get strategies list
    strategies = config.get('strategies', [])
    
    # Find strategy with matching name
    for strategy in strategies:
        if strategy.get('name') == strategy_name:
            return strategy
    
    # Strategy not found
    return None


def get_strategy_parameters(strategy_name: str) -> Dict[str, Any]:
    """Gets parameters for a specific strategy.
    
    Args:
        strategy_name: The name of the strategy
        
    Returns:
        Strategy parameters dictionary
    """
    # Get strategy configuration
    strategy = get_strategy_by_name(strategy_name)
    if not strategy:
        return {}
    
    # Return parameters or empty dict if not found
    return strategy.get('parameters', {})


def get_default_parameters(strategy_type: str) -> Dict[str, Any]:
    """Gets default parameters for a strategy type.
    
    Args:
        strategy_type: The type of the strategy
        
    Returns:
        Default parameters dictionary
    """
    # Get strategy configuration
    config = get_strategy_config()
    
    # Get default parameters for the strategy type
    default_params = config.get('default_parameters', {})
    return default_params.get(strategy_type, {})


def evaluate_rules(rules: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Evaluates applicability rules against a context.
    
    Args:
        rules: Rules to evaluate
        context: Context to evaluate against
        
    Returns:
        True if rules pass, False otherwise
    """
    # Check if rules is a dict with operator
    if isinstance(rules, dict) and 'operator' in rules:
        operator = rules.get('operator', 'AND')
        conditions = rules.get('conditions', [])
        
        if not conditions:
            return True
        
        if operator == 'AND':
            return all(evaluate_rules(condition, context) for condition in conditions)
        elif operator == 'OR':
            return any(evaluate_rules(condition, context) for condition in conditions)
        elif operator == 'NOT':
            # NOT should have exactly one condition
            if len(conditions) != 1:
                logger.warning("NOT operator should have exactly one condition")
                return False
            return not evaluate_rules(conditions[0], context)
    
    # Check if rules is a simple condition
    if isinstance(rules, dict) and 'field' in rules:
        field = rules.get('field')
        operator = rules.get('operator', '==')
        value = rules.get('value')
        
        # Get field value from context
        field_value = get_field_value(context, field)
        
        # Evaluate the condition
        return evaluate_condition(field_value, operator, value)
    
    # If rules is a list, treat as AND of all conditions
    if isinstance(rules, list):
        return all(evaluate_rules(rule, context) for rule in rules)
    
    # Unknown rule structure
    logger.warning(f"Unknown rule structure: {rules}")
    return False


def get_field_value(context: Dict[str, Any], field_path: str) -> Any:
    """Gets a field value from a nested context using dot notation.
    
    Args:
        context: Context dictionary
        field_path: Field path using dot notation (e.g., data.user.id)
        
    Returns:
        Field value or None if not found
    """
    if not field_path:
        return None
    
    parts = field_path.split('.')
    value = context
    
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    
    return value


def evaluate_condition(field_value: Any, operator: str, expected_value: Any) -> bool:
    """Evaluates a condition using the specified operator.
    
    Args:
        field_value: Actual value from context
        operator: Comparison operator
        expected_value: Expected value to compare against
        
    Returns:
        True if condition passes, False otherwise
    """
    if field_value is None:
        # If field doesn't exist, the condition fails
        return False
    
    # Equality operators
    if operator == '==' or operator == 'eq':
        return field_value == expected_value
    elif operator == '!=' or operator == 'ne':
        return field_value != expected_value
    
    # Comparison operators
    elif operator == '>' or operator == 'gt':
        return field_value > expected_value
    elif operator == '>=' or operator == 'ge':
        return field_value >= expected_value
    elif operator == '<' or operator == 'lt':
        return field_value < expected_value
    elif operator == '<=' or operator == 'le':
        return field_value <= expected_value
    
    # Containment operators
    elif operator == 'in':
        return field_value in expected_value
    elif operator == 'not_in':
        return field_value not in expected_value
    elif operator == 'contains':
        return expected_value in field_value
    elif operator == 'not_contains':
        return expected_value not in field_value
    
    # String operators
    elif operator == 'startswith':
        return str(field_value).startswith(str(expected_value))
    elif operator == 'endswith':
        return str(field_value).endswith(str(expected_value))
    elif operator == 'matches':
        return str(expected_value) in str(field_value)
    
    # Unknown operator
    logger.warning(f"Unknown operator: {operator}")
    return False


def is_strategy_applicable(strategy_name: str, context: Dict[str, Any]) -> bool:
    """Checks if a strategy is applicable for a given context.
    
    Args:
        strategy_name: The name of the strategy
        context: Context information for evaluation
        
    Returns:
        True if strategy is applicable, False otherwise
    """
    # Get strategy configuration
    strategy = get_strategy_by_name(strategy_name)
    if not strategy:
        return False
    
    # Get applicability rules
    applicability_rules = strategy.get('applicability_rules', {})
    
    # If no rules defined, assume always applicable
    if not applicability_rules:
        return True
    
    # Evaluate the rules against the context
    return evaluate_rules(applicability_rules, context)


def get_strategy_confidence_threshold(strategy_name: str) -> float:
    """Gets the confidence threshold for a specific strategy.
    
    Args:
        strategy_name: The name of the strategy
        
    Returns:
        Confidence threshold value
    """
    # Get strategy configuration
    strategy = get_strategy_by_name(strategy_name)
    if not strategy:
        return DEFAULT_CONFIDENCE_THRESHOLD
    
    # Get execution settings
    execution_settings = strategy.get('execution_settings', {})
    
    # Get threshold from settings or use default
    threshold = execution_settings.get('confidence_threshold')
    if threshold is not None:
        return float(threshold)
    
    return DEFAULT_CONFIDENCE_THRESHOLD


def get_strategy_risk_level(strategy_name: str) -> RiskLevel:
    """Gets the risk level for a specific strategy.
    
    Args:
        strategy_name: The name of the strategy
        
    Returns:
        Risk level enum value
    """
    # Get strategy configuration
    strategy = get_strategy_by_name(strategy_name)
    if not strategy:
        return RiskLevel.MEDIUM
    
    # Get execution settings
    execution_settings = strategy.get('execution_settings', {})
    
    # Get risk level from settings or use default
    risk_level_str = execution_settings.get('risk_level', 'MEDIUM')
    try:
        return RiskLevel[risk_level_str]
    except (KeyError, ValueError):
        return RiskLevel.MEDIUM


def get_strategy_max_attempts(strategy_name: str) -> int:
    """Gets the maximum execution attempts for a specific strategy.
    
    Args:
        strategy_name: The name of the strategy
        
    Returns:
        Maximum attempts value
    """
    # Get strategy configuration
    strategy = get_strategy_by_name(strategy_name)
    if not strategy:
        return 3  # Default max attempts
    
    # Get execution settings
    execution_settings = strategy.get('execution_settings', {})
    
    # Get max attempts from settings or use default
    max_attempts = execution_settings.get('max_attempts', 3)
    return int(max_attempts)


def reload_strategy_config(config_path: str = None) -> Dict[str, Any]:
    """Reloads the strategy configuration from file.
    
    Args:
        config_path: Path to the configuration file. If None, uses default path.
        
    Returns:
        Reloaded strategy configuration dictionary
    """
    global _strategy_config_cache
    
    # Clear the cache to force reload
    _strategy_config_cache = None
    
    # Load and return the updated configuration
    config = get_strategy_config(config_path)
    logger.info("Strategy configuration reloaded")
    
    return config