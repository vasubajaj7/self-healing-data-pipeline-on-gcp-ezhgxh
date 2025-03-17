"""
Specialized configuration module for the self-healing AI engine.

This module provides functions to access and manage healing-specific configuration settings
including operational modes, confidence thresholds, action parameters, and rule sets.
It extends the core configuration system with healing-specific functionality.
"""

import os
import typing
import yaml
from typing import Dict, List, Any, Optional, Union

from ...constants import (
    SelfHealingMode,
    HealingActionType,
    DEFAULT_CONFIDENCE_THRESHOLD,
    DEFAULT_MAX_RETRY_ATTEMPTS
)
from ...config import get_config
from ...utils.logging.logger import get_logger

# Configure module logger
logger = get_logger(__name__)

# Default configurations
DEFAULT_HEALING_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'configs',
    'healing_rules.yaml'
)
DEFAULT_APPROVAL_MODE = "high_impact_only"
DEFAULT_LEARNING_MODE = "active"

# Cache for loaded healing configuration
_healing_config_cache = None


def get_healing_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Gets the healing configuration, loading it if not already cached.

    Args:
        config_path: Path to the configuration file. If None, uses default path.

    Returns:
        Dict[str, Any]: Healing configuration dictionary
    """
    global _healing_config_cache
    
    if _healing_config_cache is None:
        file_path = config_path or DEFAULT_HEALING_CONFIG_PATH
        config = load_healing_config(file_path)
        
        if not validate_healing_config(config):
            logger.warning(f"Invalid healing configuration loaded from {file_path}. Using empty config.")
            config = {}
            
        _healing_config_cache = config
        logger.info(f"Healing configuration loaded from {file_path}")
        
    return _healing_config_cache


def load_healing_config(file_path: str) -> Dict[str, Any]:
    """Loads healing configuration from a YAML file.

    Args:
        file_path: Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Healing configuration dictionary or empty dict if file not found.
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Healing configuration file not found: {file_path}")
            return {}
            
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
            return config or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML healing configuration file {file_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error loading healing configuration file {file_path}: {e}")
        return {}


def validate_healing_config(config: Dict[str, Any]) -> bool:
    """Validates the structure and content of the healing configuration.

    Args:
        config: Configuration dictionary to validate.

    Returns:
        bool: True if configuration is valid, False otherwise.
    """
    if not isinstance(config, dict):
        logger.error("Healing configuration must be a dictionary")
        return False
        
    # Check required top-level sections
    required_sections = ['global_settings', 'action_types', 'rules']
    for section in required_sections:
        if section not in config:
            logger.error(f"Healing configuration missing required section: {section}")
            return False
            
    # Validate global_settings section
    global_settings = config.get('global_settings', {})
    if not isinstance(global_settings, dict):
        logger.error("global_settings must be a dictionary")
        return False
        
    # Validate action_types section
    action_types = config.get('action_types', {})
    if not isinstance(action_types, dict):
        logger.error("action_types must be a dictionary")
        return False
        
    # Validate rules section
    rules = config.get('rules', [])
    if not isinstance(rules, list):
        logger.error("rules must be a list")
        return False
        
    # Validate each rule has required fields
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            logger.error(f"Rule {i} must be a dictionary")
            return False
            
        if 'rule_id' not in rule:
            logger.error(f"Rule {i} missing required field: rule_id")
            return False
            
        if 'actions' not in rule or not isinstance(rule['actions'], list):
            logger.error(f"Rule {i} missing required field: actions or actions is not a list")
            return False
    
    # If we get here, configuration is valid
    return True


def get_healing_mode() -> SelfHealingMode:
    """Gets the current self-healing operational mode.

    Returns:
        SelfHealingMode: Self-healing mode enum value
    """
    # Get core configuration
    config = get_config()
    
    # Get self-healing mode from configuration
    mode_str = config.get("self_healing.mode", SelfHealingMode.SEMI_AUTOMATIC.value)
    
    # Convert string mode to enum value
    try:
        return SelfHealingMode(mode_str)
    except ValueError:
        logger.warning(f"Invalid self-healing mode: {mode_str}, using SEMI_AUTOMATIC")
        return SelfHealingMode.SEMI_AUTOMATIC


def get_confidence_threshold(action_type: Optional[HealingActionType] = None) -> float:
    """Gets the confidence threshold for a specific action type.

    Args:
        action_type: The action type to get the threshold for.

    Returns:
        float: Confidence threshold value
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # If action_type provided, try to get specific threshold
    if action_type:
        # Get action types configuration
        action_types = healing_config.get('action_types', {})
        # Get configuration for the specific action type
        action_config = action_types.get(action_type.value, {})
        # Get threshold for the action type, or None if not defined
        threshold = action_config.get('confidence_threshold')
        
        if threshold is not None:
            return float(threshold)
    
    # If action_type not provided or threshold not found, get default
    global_settings = healing_config.get('global_settings', {})
    threshold = global_settings.get('confidence_threshold')
    
    # If not found in global_settings, use constant
    if threshold is None:
        return DEFAULT_CONFIDENCE_THRESHOLD
        
    return float(threshold)


def get_max_retry_attempts(action_type: Optional[HealingActionType] = None) -> int:
    """Gets the maximum retry attempts for a specific action type.

    Args:
        action_type: The action type to get the max retries for.

    Returns:
        int: Maximum retry attempts
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # If action_type provided, try to get specific max retries
    if action_type:
        # Get action types configuration
        action_types = healing_config.get('action_types', {})
        # Get configuration for the specific action type
        action_config = action_types.get(action_type.value, {})
        # Get max retries for the action type, or None if not defined
        max_retries = action_config.get('max_retry_attempts')
        
        if max_retries is not None:
            return int(max_retries)
    
    # If action_type not provided or max retries not found, get default
    global_settings = healing_config.get('global_settings', {})
    max_retries = global_settings.get('max_retry_attempts')
    
    # If not found in global_settings, use constant
    if max_retries is None:
        return DEFAULT_MAX_RETRY_ATTEMPTS
        
    return int(max_retries)


def get_approval_required(action_type: Optional[HealingActionType] = None) -> str:
    """Gets the approval requirement setting for a specific action type.

    Args:
        action_type: The action type to get the approval setting for.

    Returns:
        str: Approval requirement setting (always, high_impact_only, critical_only, never)
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # If action_type provided, try to get specific approval setting
    if action_type:
        # Get action types configuration
        action_types = healing_config.get('action_types', {})
        # Get configuration for the specific action type
        action_config = action_types.get(action_type.value, {})
        # Get approval setting for the action type, or None if not defined
        approval = action_config.get('approval_required')
        
        if approval is not None:
            return approval
    
    # If action_type not provided or approval not found, get default
    global_settings = healing_config.get('global_settings', {})
    approval = global_settings.get('approval_required')
    
    # If not found in global_settings, use default
    if approval is None:
        return DEFAULT_APPROVAL_MODE
        
    return approval


def is_action_type_enabled(action_type: HealingActionType) -> bool:
    """Checks if a specific healing action type is enabled.

    Args:
        action_type: The action type to check.

    Returns:
        bool: True if action type is enabled, False otherwise
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get action types configuration
    action_types = healing_config.get('action_types', {})
    
    # Get configuration for the specific action type
    action_config = action_types.get(action_type.value, {})
    
    # Check if enabled flag is set to True
    return action_config.get('enabled', False)


def get_learning_mode() -> str:
    """Gets the current learning mode for the self-healing system.

    Returns:
        str: Learning mode (active, passive, disabled)
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get global settings
    global_settings = healing_config.get('global_settings', {})
    
    # Get learning mode setting, or default if not found
    return global_settings.get('learning_mode', DEFAULT_LEARNING_MODE)


def get_rules_by_type(action_type: HealingActionType) -> List[Dict[str, Any]]:
    """Gets healing rules filtered by action type.

    Args:
        action_type: The action type to filter rules by.

    Returns:
        List[Dict[str, Any]]: List of rule configurations matching the action type
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get rules list
    rules = healing_config.get('rules', [])
    
    # Filter rules by action type
    filtered_rules = []
    for rule in rules:
        # Get actions list for the rule
        actions = rule.get('actions', [])
        
        # Check if any action matches the action type
        for action in actions:
            if action.get('type') == action_type.value:
                filtered_rules.append(rule)
                break
                
    return filtered_rules


def get_rule_by_id(rule_id: str) -> Optional[Dict[str, Any]]:
    """Gets a specific healing rule by its ID.

    Args:
        rule_id: The ID of the rule to get.

    Returns:
        Dict[str, Any]: Rule configuration dictionary or None if not found
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get rules list
    rules = healing_config.get('rules', [])
    
    # Find rule with matching ID
    for rule in rules:
        if rule.get('rule_id') == rule_id:
            return rule
            
    # Rule not found
    return None


def get_rules_by_set(rule_set_name: str) -> List[Dict[str, Any]]:
    """Gets healing rules belonging to a specific rule set.

    Args:
        rule_set_name: The name of the rule set.

    Returns:
        List[Dict[str, Any]]: List of rule configurations in the specified rule set
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get rule sets configuration
    rule_sets = healing_config.get('rule_sets', {})
    
    # Get rule IDs for the specified rule set
    rule_ids = rule_sets.get(rule_set_name, [])
    
    # Get full rule configurations for each rule ID
    rules = []
    for rule_id in rule_ids:
        rule = get_rule_by_id(rule_id)
        if rule:
            rules.append(rule)
            
    return rules


def get_action_parameters(rule_id: str, strategy_name: str) -> Optional[Dict[str, Any]]:
    """Gets parameters for a specific healing action strategy.

    Args:
        rule_id: The ID of the rule containing the strategy.
        strategy_name: The name of the strategy to get parameters for.

    Returns:
        Dict[str, Any]: Action parameters dictionary or None if not found
    """
    # Get rule configuration
    rule = get_rule_by_id(rule_id)
    if not rule:
        return None
        
    # Get actions list for the rule
    actions = rule.get('actions', [])
    
    # Find action with matching strategy name
    for action in actions:
        if action.get('strategy') == strategy_name:
            return action.get('parameters', {})
            
    # Strategy not found
    return None


def get_severity_threshold(severity: str) -> Optional[Dict[str, Any]]:
    """Gets threshold configuration for a specific severity level.

    Args:
        severity: The severity level to get threshold for.

    Returns:
        Dict[str, Any]: Severity threshold configuration or None if not found
    """
    # Get healing configuration
    healing_config = get_healing_config()
    
    # Get severity thresholds configuration
    severity_thresholds = healing_config.get('severity_thresholds', {})
    
    # Get threshold configuration for the specified severity
    return severity_thresholds.get(severity)


def reload_healing_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Reloads the healing configuration from file.

    Args:
        config_path: Path to the configuration file. If None, uses default path.

    Returns:
        Dict[str, Any]: Reloaded healing configuration dictionary
    """
    global _healing_config_cache
    
    # Clear the cache to force reload
    _healing_config_cache = None
    
    # Load and return the updated configuration
    config = get_healing_config(config_path)
    logger.info("Healing configuration reloaded")
    
    return config


class HealingConfig:
    """Class that manages healing-specific configuration settings."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initializes the HealingConfig instance with the specified configuration path.

        Args:
            config_path: Path to the configuration file. If None, uses default path.
        """
        self._config = None
        self._healing_config = None
        self._config_path = config_path or DEFAULT_HEALING_CONFIG_PATH
        self._initialized = False
        self._load_config()
        
    def _load_config(self):
        """Loads healing configuration from file."""
        # Get core configuration
        self._config = get_config()
        
        # Load healing-specific configuration
        self._healing_config = load_healing_config(self._config_path)
        
        # Validate the configuration
        if not validate_healing_config(self._healing_config):
            logger.warning(f"Invalid healing configuration loaded from {self._config_path}. Using empty config.")
            self._healing_config = {}
        
        self._initialized = True
        logger.info(f"Healing configuration loaded from {self._config_path}")
        
    def get_healing_mode(self) -> SelfHealingMode:
        """Gets the current self-healing operational mode.

        Returns:
            SelfHealingMode: Self-healing mode enum value
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get self-healing mode from configuration
        mode_str = self._config.get("self_healing.mode", SelfHealingMode.SEMI_AUTOMATIC.value)
        
        # Convert string mode to enum value
        try:
            return SelfHealingMode(mode_str)
        except ValueError:
            logger.warning(f"Invalid self-healing mode: {mode_str}, using SEMI_AUTOMATIC")
            return SelfHealingMode.SEMI_AUTOMATIC
            
    def get_confidence_threshold(self, action_type: Optional[HealingActionType] = None) -> float:
        """Gets the confidence threshold for a specific action type.

        Args:
            action_type: The action type to get the threshold for.

        Returns:
            float: Confidence threshold value
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # If action_type provided, try to get specific threshold
        if action_type:
            # Get action types configuration
            action_types = self._healing_config.get('action_types', {})
            # Get configuration for the specific action type
            action_config = action_types.get(action_type.value, {})
            # Get threshold for the action type, or None if not defined
            threshold = action_config.get('confidence_threshold')
            
            if threshold is not None:
                return float(threshold)
        
        # If action_type not provided or threshold not found, get default
        global_settings = self._healing_config.get('global_settings', {})
        threshold = global_settings.get('confidence_threshold')
        
        # If not found in global_settings, use constant
        if threshold is None:
            return DEFAULT_CONFIDENCE_THRESHOLD
            
        return float(threshold)
        
    def get_max_retry_attempts(self, action_type: Optional[HealingActionType] = None) -> int:
        """Gets the maximum retry attempts for a specific action type.

        Args:
            action_type: The action type to get the max retries for.

        Returns:
            int: Maximum retry attempts
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # If action_type provided, try to get specific max retries
        if action_type:
            # Get action types configuration
            action_types = self._healing_config.get('action_types', {})
            # Get configuration for the specific action type
            action_config = action_types.get(action_type.value, {})
            # Get max retries for the action type, or None if not defined
            max_retries = action_config.get('max_retry_attempts')
            
            if max_retries is not None:
                return int(max_retries)
        
        # If action_type not provided or max retries not found, get default
        global_settings = self._healing_config.get('global_settings', {})
        max_retries = global_settings.get('max_retry_attempts')
        
        # If not found in global_settings, use constant
        if max_retries is None:
            return DEFAULT_MAX_RETRY_ATTEMPTS
            
        return int(max_retries)
        
    def get_approval_required(self, action_type: Optional[HealingActionType] = None) -> str:
        """Gets the approval requirement setting for a specific action type.

        Args:
            action_type: The action type to get the approval setting for.

        Returns:
            str: Approval requirement setting (always, high_impact_only, critical_only, never)
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # If action_type provided, try to get specific approval setting
        if action_type:
            # Get action types configuration
            action_types = self._healing_config.get('action_types', {})
            # Get configuration for the specific action type
            action_config = action_types.get(action_type.value, {})
            # Get approval setting for the action type, or None if not defined
            approval = action_config.get('approval_required')
            
            if approval is not None:
                return approval
        
        # If action_type not provided or approval not found, get default
        global_settings = self._healing_config.get('global_settings', {})
        approval = global_settings.get('approval_required')
        
        # If not found in global_settings, use default
        if approval is None:
            return DEFAULT_APPROVAL_MODE
            
        return approval
        
    def is_action_type_enabled(self, action_type: HealingActionType) -> bool:
        """Checks if a specific healing action type is enabled.

        Args:
            action_type: The action type to check.

        Returns:
            bool: True if action type is enabled, False otherwise
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get action types configuration
        action_types = self._healing_config.get('action_types', {})
        
        # Get configuration for the specific action type
        action_config = action_types.get(action_type.value, {})
        
        # Check if enabled flag is set to True
        return action_config.get('enabled', False)
        
    def get_learning_mode(self) -> str:
        """Gets the current learning mode for the self-healing system.

        Returns:
            str: Learning mode (active, passive, disabled)
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get global settings
        global_settings = self._healing_config.get('global_settings', {})
        
        # Get learning mode setting, or default if not found
        return global_settings.get('learning_mode', DEFAULT_LEARNING_MODE)
        
    def get_rules_by_type(self, action_type: HealingActionType) -> List[Dict[str, Any]]:
        """Gets healing rules filtered by action type.

        Args:
            action_type: The action type to filter rules by.

        Returns:
            List[Dict[str, Any]]: List of rule configurations matching the action type
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get rules list
        rules = self._healing_config.get('rules', [])
        
        # Filter rules by action type
        filtered_rules = []
        for rule in rules:
            # Get actions list for the rule
            actions = rule.get('actions', [])
            
            # Check if any action matches the action type
            for action in actions:
                if action.get('type') == action_type.value:
                    filtered_rules.append(rule)
                    break
                    
        return filtered_rules
        
    def get_rule_by_id(self, rule_id: str) -> Optional[Dict[str, Any]]:
        """Gets a specific healing rule by its ID.

        Args:
            rule_id: The ID of the rule to get.

        Returns:
            Dict[str, Any]: Rule configuration dictionary or None if not found
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get rules list
        rules = self._healing_config.get('rules', [])
        
        # Find rule with matching ID
        for rule in rules:
            if rule.get('rule_id') == rule_id:
                return rule
                
        # Rule not found
        return None
        
    def get_rules_by_set(self, rule_set_name: str) -> List[Dict[str, Any]]:
        """Gets healing rules belonging to a specific rule set.

        Args:
            rule_set_name: The name of the rule set.

        Returns:
            List[Dict[str, Any]]: List of rule configurations in the specified rule set
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get rule sets configuration
        rule_sets = self._healing_config.get('rule_sets', {})
        
        # Get rule IDs for the specified rule set
        rule_ids = rule_sets.get(rule_set_name, [])
        
        # Get full rule configurations for each rule ID
        rules = []
        for rule_id in rule_ids:
            rule = self.get_rule_by_id(rule_id)
            if rule:
                rules.append(rule)
                
        return rules
        
    def get_action_parameters(self, rule_id: str, strategy_name: str) -> Optional[Dict[str, Any]]:
        """Gets parameters for a specific healing action strategy.

        Args:
            rule_id: The ID of the rule containing the strategy.
            strategy_name: The name of the strategy to get parameters for.

        Returns:
            Dict[str, Any]: Action parameters dictionary or None if not found
        """
        # Get rule configuration
        rule = self.get_rule_by_id(rule_id)
        if not rule:
            return None
            
        # Get actions list for the rule
        actions = rule.get('actions', [])
        
        # Find action with matching strategy name
        for action in actions:
            if action.get('strategy') == strategy_name:
                return action.get('parameters', {})
                
        # Strategy not found
        return None
        
    def get_severity_threshold(self, severity: str) -> Optional[Dict[str, Any]]:
        """Gets threshold configuration for a specific severity level.

        Args:
            severity: The severity level to get threshold for.

        Returns:
            Dict[str, Any]: Severity threshold configuration or None if not found
        """
        # Ensure configuration is loaded
        if not self._initialized:
            self._load_config()
            
        # Get severity thresholds configuration
        severity_thresholds = self._healing_config.get('severity_thresholds', {})
        
        # Get threshold configuration for the specified severity
        return severity_thresholds.get(severity)
        
    def reload(self) -> None:
        """Reloads the healing configuration from file."""
        self._initialized = False
        self._load_config()
        logger.info("Healing configuration reloaded")