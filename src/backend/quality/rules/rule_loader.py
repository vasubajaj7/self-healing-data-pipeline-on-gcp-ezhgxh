"""
Provides functionality for loading, saving, and managing data quality validation rules from/to files and directories.

This module enables persistent storage of rule definitions and supports various file formats for rule import/export.
It includes functions for rule validation, conversion, and directory operations.
"""

import os
import json
import yaml
import typing
import glob
import uuid

from ...constants import ValidationRuleType, QualityDimension
from ...config import get_config
from ...utils.logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Default rules directory path
DEFAULT_RULES_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../configs/quality_rules')


def load_rules_from_file(file_path: str) -> list:
    """
    Loads rule definitions from a file in JSON or YAML format.

    Args:
        file_path: Path to the rule definition file

    Returns:
        List of rule definition dictionaries

    Raises:
        FileNotFoundError: If the specified file doesn't exist
        ValueError: If the file format is not supported or content is invalid
    """
    logger.debug(f"Loading rules from file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        error_msg = f"Rule file not found: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Determine file format based on extension
    file_ext = os.path.splitext(file_path)[1].lower()
    
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            
            # Parse content based on file format
            if file_ext in ('.json'):
                rules_data = json.loads(content)
            elif file_ext in ('.yaml', '.yml'):
                rules_data = yaml.safe_load(content)
            else:
                error_msg = f"Unsupported file format: {file_ext}. Supported formats are: .json, .yaml, .yml"
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Handle different structure formats
        if isinstance(rules_data, list):
            rules = rules_data
        elif isinstance(rules_data, dict) and 'rules' in rules_data:
            rules = rules_data.get('rules', [])
        else:
            error_msg = "Invalid rule file format. Expected list of rules or dict with 'rules' key."
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        logger.info(f"Successfully loaded {len(rules)} rules from {file_path}")
        return rules
    
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in rule file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in rule file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except Exception as e:
        error_msg = f"Error loading rules from file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise


def save_rules_to_file(file_path: str, rules: list) -> bool:
    """
    Saves rule definitions to a file in JSON or YAML format.

    Args:
        file_path: Path where rules will be saved
        rules: List of rule definitions to save

    Returns:
        True if the save was successful, False otherwise
    """
    logger.debug(f"Saving {len(rules)} rules to file: {file_path}")
    
    # Determine file format based on extension
    file_ext = os.path.splitext(file_path)[1].lower()
    
    # Convert rules to dictionary form if they're objects
    rule_dicts = [convert_rule_to_dict(rule) for rule in rules]
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        with open(file_path, 'w') as file:
            if file_ext in ('.json'):
                json.dump(rule_dicts, file, indent=2)
            elif file_ext in ('.yaml', '.yml'):
                yaml.dump(rule_dicts, file, default_flow_style=False, sort_keys=False)
            else:
                error_msg = f"Unsupported file format: {file_ext}. Supported formats are: .json, .yaml, .yml"
                logger.error(error_msg)
                return False
        
        logger.info(f"Successfully saved {len(rules)} rules to {file_path}")
        return True
    
    except Exception as e:
        error_msg = f"Error saving rules to file {file_path}: {str(e)}"
        logger.error(error_msg)
        return False


def get_rule_files_in_directory(directory_path: str = None, file_pattern: str = None) -> list:
    """
    Gets a list of rule files in the specified directory.

    Args:
        directory_path: Path to the directory containing rule files, or None to use default
        file_pattern: Glob pattern to match rule files, defaults to common rule file extensions

    Returns:
        List of file paths matching the pattern
    """
    # Use default rules directory if none provided
    if directory_path is None:
        directory_path = DEFAULT_RULES_DIRECTORY
    
    logger.debug(f"Searching for rule files in directory: {directory_path}")
    
    # Check if directory exists
    if not os.path.exists(directory_path):
        logger.warning(f"Rules directory not found: {directory_path}")
        return []
    
    try:
        rule_files = []
        
        # Use default patterns if none provided
        if file_pattern is None:
            # Search for common rule file extensions
            for pattern in ["*.json", "*.yaml", "*.yml"]:
                rule_files.extend(glob.glob(os.path.join(directory_path, pattern)))
        else:
            # Use the provided pattern
            rule_files = glob.glob(os.path.join(directory_path, file_pattern))
        
        logger.debug(f"Found {len(rule_files)} rule files")
        return sorted(rule_files)
    
    except Exception as e:
        error_msg = f"Error finding rule files in directory {directory_path}: {str(e)}"
        logger.error(error_msg)
        return []


def load_rules_from_directory(directory_path: str = None, file_pattern: str = None) -> typing.Tuple[list, list]:
    """
    Loads all rule definitions from files in a directory.

    Args:
        directory_path: Path to the directory containing rule files, or None to use default
        file_pattern: Glob pattern to match rule files, defaults to common rule file extensions

    Returns:
        Tuple containing (list of rule definitions, list of errors)
    """
    # Get list of rule files in the directory
    rule_files = get_rule_files_in_directory(directory_path, file_pattern)
    
    logger.info(f"Loading rules from {len(rule_files)} files in directory")
    
    all_rules = []
    errors = []
    
    # Load rules from each file
    for file_path in rule_files:
        try:
            rules = load_rules_from_file(file_path)
            all_rules.extend(rules)
        except Exception as e:
            errors.append({"file": file_path, "error": str(e)})
            logger.error(f"Error loading rules from {file_path}: {str(e)}")
    
    logger.info(f"Loaded a total of {len(all_rules)} rules with {len(errors)} errors")
    return all_rules, errors


def validate_rule_structure(rule_definition: dict) -> typing.Tuple[bool, str]:
    """
    Validates the structure of a rule definition against required fields and types.

    Args:
        rule_definition: Rule definition dictionary to validate

    Returns:
        Tuple containing (validation result, error message or empty string)
    """
    # Check required fields
    required_fields = ['name', 'type', 'dimension', 'parameters']
    
    for field in required_fields:
        if field not in rule_definition:
            return False, f"Missing required field: {field}"
    
    # Validate rule type (should be a valid ValidationRuleType)
    rule_type = rule_definition.get('type')
    valid_rule_types = [t.value for t in ValidationRuleType]
    
    if rule_type not in valid_rule_types:
        return False, f"Invalid rule type: {rule_type}. Must be one of: {', '.join(valid_rule_types)}"
    
    # Validate quality dimension (should be a valid QualityDimension)
    dimension = rule_definition.get('dimension')
    valid_dimensions = [d.value for d in QualityDimension]
    
    if dimension not in valid_dimensions:
        return False, f"Invalid quality dimension: {dimension}. Must be one of: {', '.join(valid_dimensions)}"
    
    # Validate parameters (should be a dictionary)
    parameters = rule_definition.get('parameters')
    if not isinstance(parameters, dict):
        return False, f"Parameters must be a dictionary, got {type(parameters).__name__}"
    
    # Additional type validations
    if not isinstance(rule_definition.get('name'), str):
        return False, "Rule name must be a string"
    
    # All validations passed
    return True, ""


def convert_rule_to_dict(rule: typing.Union[dict, object]) -> dict:
    """
    Converts a rule object or dictionary to a dictionary representation.

    Args:
        rule: Rule object or dictionary to convert

    Returns:
        Dictionary representation of the rule

    Raises:
        TypeError: If the rule cannot be converted to a dictionary
    """
    # If already a dictionary, return it
    if isinstance(rule, dict):
        return rule
    
    # If object has to_dict method, use it
    if hasattr(rule, 'to_dict') and callable(getattr(rule, 'to_dict')):
        return rule.to_dict()
    
    # If object has __dict__ attribute, use it
    if hasattr(rule, '__dict__'):
        return rule.__dict__
    
    # Unable to convert
    raise TypeError(f"Cannot convert object of type {type(rule).__name__} to dictionary")


def export_rules_to_file(file_path: str, rules: list, format: str = None) -> bool:
    """
    Exports rules to a file in the specified format.

    Args:
        file_path: Path where rules will be exported
        rules: List of rule objects or dictionaries to export
        format: Output format (json, yaml) or None to determine from file extension

    Returns:
        True if export was successful, False otherwise
    """
    logger.debug(f"Exporting {len(rules)} rules to file: {file_path}")
    
    # Determine format from parameter or file extension
    if format is None:
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext in ('.json'):
            format = 'json'
        elif file_ext in ('.yaml', '.yml'):
            format = 'yaml'
        else:
            error_msg = f"Cannot determine export format from file extension: {file_ext}"
            logger.error(error_msg)
            return False
    
    # Normalize format
    format = format.lower()
    
    # Convert rules to dictionary representations
    try:
        rule_dicts = [convert_rule_to_dict(rule) for rule in rules]
    except TypeError as e:
        logger.error(f"Error converting rules to dictionaries: {str(e)}")
        return False
    
    # Create directory if it doesn't exist
    try:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directory for {file_path}: {str(e)}")
        return False
    
    # Write to file in specified format
    try:
        with open(file_path, 'w') as file:
            if format == 'json':
                json.dump(rule_dicts, file, indent=2)
            elif format in ('yaml', 'yml'):
                yaml.dump(rule_dicts, file, default_flow_style=False, sort_keys=False)
            else:
                error_msg = f"Unsupported export format: {format}. Supported formats are: json, yaml"
                logger.error(error_msg)
                return False
        
        logger.info(f"Successfully exported {len(rules)} rules to {file_path}")
        return True
    
    except Exception as e:
        error_msg = f"Error exporting rules to file {file_path}: {str(e)}"
        logger.error(error_msg)
        return False


def import_rules_from_file(file_path: str) -> typing.Tuple[list, list]:
    """
    Imports rules from a file and validates their structure.

    Args:
        file_path: Path to the rule definition file

    Returns:
        Tuple containing (list of valid rule definitions, list of errors)
    """
    logger.debug(f"Importing rules from file: {file_path}")
    
    try:
        # Load rules from file
        all_rules = load_rules_from_file(file_path)
        
        valid_rules = []
        errors = []
        
        # Validate each rule
        for i, rule in enumerate(all_rules):
            is_valid, error_message = validate_rule_structure(rule)
            
            if is_valid:
                valid_rules.append(rule)
            else:
                rule_name = rule.get('name', f"Rule {i+1}")
                errors.append({
                    "rule": rule_name,
                    "error": error_message
                })
                logger.warning(f"Invalid rule '{rule_name}': {error_message}")
        
        logger.info(f"Imported {len(valid_rules)} valid rules with {len(errors)} invalid rules")
        return valid_rules, errors
    
    except Exception as e:
        logger.error(f"Error importing rules from {file_path}: {str(e)}")
        return [], [{"file": file_path, "error": str(e)}]