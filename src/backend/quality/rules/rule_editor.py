"""
Provides a user-friendly interface for creating, editing, and managing data quality validation rules.
This module enables users to define, modify, and validate rule definitions through a programmatic interface,
supporting the rule management lifecycle from creation to deployment.
"""

import typing
import os
import json
import uuid
import datetime

from typing import Optional

from src.backend.constants import (
    ValidationRuleType,
    QualityDimension,
)  # src/backend/constants.py
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.quality.rules.rule_engine import (
    RuleEngine,
    Rule,
    validate_rule_structure,
)  # ./rule_engine
from src.backend.quality.rules.rule_loader import (
    load_rules_from_file,
    save_rules_to_file,
    get_rule_files_in_directory,
)  # ./rule_loader
from src.backend.quality.expectations.expectation_manager import (
    map_rule_to_expectation,
)  # ../expectations/expectation_manager


# Initialize logger
logger = get_logger(__name__)

# Define default rules directory
DEFAULT_RULES_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../../../configs/quality_rules"
)


def create_rule_template(
    rule_type: ValidationRuleType, dimension: QualityDimension
) -> dict:
    """
    Creates a template for a new rule based on rule type

    Args:
        rule_type (ValidationRuleType): rule_type
        dimension (QualityDimension): dimension

    Returns:
        dict: Rule template with default structure for the specified type
    """
    # Create base template with common fields (name, description, type, dimension)
    template = {
        "name": f"New {rule_type.value} Rule",
        "description": "Description of the new rule",
        "type": rule_type.value,
        "dimension": dimension.value,
        "parameters": {},
    }
    # Add type-specific parameter templates based on rule_type
    if rule_type == ValidationRuleType.SCHEMA:
        template["parameters"] = {"column_name": "column_name", "data_type": "string"}
    elif rule_type == ValidationRuleType.COMPLETENESS:
        template["parameters"] = {"column_name": "column_name", "threshold": 0.9}
    elif rule_type == ValidationRuleType.ANOMALY:
        template["parameters"] = {"column_name": "column_name", "threshold": 3.0}
    elif rule_type == ValidationRuleType.REFERENTIAL:
        template["parameters"] = {
            "column_name": "column_name",
            "reference_table": "reference_table",
            "reference_column": "reference_column",
        }
    # Set default values appropriate for the rule type
    # Return the template dictionary
    return template


def validate_rule_parameters(rule: dict) -> typing.Tuple[bool, str]:
    """
    Validates the parameters of a rule based on its type

    Args:
        rule (dict): rule

    Returns:
        tuple: (bool, str) - Validation result and error message if any
    """
    # Extract rule type and parameters from rule dictionary
    rule_type = rule.get("type")
    parameters = rule.get("parameters", {})
    # Validate required parameters exist for the specific rule type
    if rule_type == ValidationRuleType.SCHEMA.value:
        if not all(
            key in parameters for key in ["column_name", "data_type"]
        ):  # Corrected parameter names
            return False, "Missing required parameters for SCHEMA rule"
    elif rule_type == ValidationRuleType.COMPLETENESS.value:
        if "column_name" not in parameters:
            return False, "Missing required parameter 'column_name' for COMPLETENESS rule"
    elif rule_type == ValidationRuleType.ANOMALY.value:
        if "column_name" not in parameters:
            return False, "Missing required parameter 'column_name' for ANOMALY rule"
    elif rule_type == ValidationRuleType.REFERENTIAL.value:
        if not all(
            key in parameters for key in ["column_name", "reference_table", "reference_column"]
        ):
            return False, "Missing required parameters for REFERENTIAL rule"
    # Validate parameter types and value ranges
    # Return validation result and error message if any
    return True, ""


def generate_rule_id() -> str:
    """
    Generates a unique identifier for a new rule

    Returns:
        str: Unique rule identifier
    """
    # Generate a UUID
    rule_uuid = uuid.uuid4()
    # Format as a string with 'rule-' prefix
    rule_id = f"rule-{str(rule_uuid)}"
    # Return the formatted ID
    return rule_id


def get_rule_templates() -> dict:
    """
    Returns templates for all supported rule types

    Returns:
        dict: Dictionary of rule templates by rule type
    """
    # Create templates for each ValidationRuleType
    templates = {}
    for rule_type in ValidationRuleType:
        # Include sample parameters for each rule type
        templates[rule_type.value] = create_rule_template(
            rule_type, QualityDimension.ACCURACY
        )  # Using ACCURACY as a default dimension
    # Return dictionary of templates
    return templates


def get_rule_documentation(rule_type: Optional[ValidationRuleType] = None) -> dict:
    """
    Returns documentation for rule types and parameters

    Args:
        rule_type (Optional[ValidationRuleType]): rule_type

    Returns:
        dict: Documentation for rule types or specific rule type
    """
    # If rule_type is provided, return documentation for that specific type
    if rule_type:
        return {
            "description": f"Documentation for {rule_type.value} rules",
            "parameters": {"param1": "Description of param1", "param2": "Description of param2"},
        }
    # Otherwise, return documentation for all rule types
    else:
        return {
            ValidationRuleType.SCHEMA.value: {
                "description": "Documentation for SCHEMA rules",
                "parameters": {"column_name": "Name of the column", "data_type": "Expected data type"},
            },
            ValidationRuleType.COMPLETENESS.value: {
                "description": "Documentation for COMPLETENESS rules",
                "parameters": {"column_name": "Name of the column", "threshold": "Acceptable null percentage"},
            },
        }
    # Include parameter descriptions, examples, and usage notes


class RuleEditor:
    """Class for creating, editing, and managing data quality validation rules"""

    _rule_engine: RuleEngine
    _config: dict
    _rules_directory: str

    def __init__(self, config: Optional[dict] = None, rule_engine: Optional[RuleEngine] = None):
        """Initialize the rule editor with configuration"""
        # Initialize configuration with defaults and override with provided config
        self._config = config or get_config()._config
        # Set rules directory from config or use default
        self._rules_directory = self._config.get("rules_directory", DEFAULT_RULES_DIRECTORY)
        # Initialize or use provided rule engine instance
        self._rule_engine = rule_engine or RuleEngine(self._config)
        # Ensure rules directory exists
        if not os.path.exists(self._rules_directory):
            os.makedirs(self._rules_directory)

    def create_rule(self, rule_definition: dict) -> typing.Tuple[bool, str]:
        """Creates a new validation rule"""
        # Validate rule structure using validate_rule_structure
        is_valid, error_message = validate_rule_structure(rule_definition)
        if not is_valid:
            return False, error_message

        # Generate rule ID if not provided
        if "rule_id" not in rule_definition:
            rule_definition["rule_id"] = generate_rule_id()

        # Register rule with rule engine
        success, rule_id = self._rule_engine.register_rule(rule_definition)
        if not success:
            return False, rule_id

        # Return success status and rule ID or error message
        return True, rule_id

    def update_rule(self, rule_id: str, rule_updates: dict) -> typing.Tuple[bool, str]:
        """Updates an existing validation rule"""
        # Check if rule exists in rule engine
        rule = self._rule_engine.get_rule(rule_id)
        if not rule:
            return False, f"Rule with ID '{rule_id}' not found"

        # Apply updates to rule definition
        try:
            success, message = self._rule_engine.update_rule(rule_id, rule_updates)
            if not success:
                return False, message
        except Exception as e:
            return False, str(e)

        # Return success status and message
        return True, "Rule updated successfully"

    def delete_rule(self, rule_id: str) -> bool:
        """Deletes a validation rule"""
        # Check if rule exists in rule engine
        rule = self._rule_engine.get_rule(rule_id)
        if not rule:
            logger.warning(f"Cannot delete rule with ID '{rule_id}': not found")
            return False

        # Delete rule from rule engine
        success = self._rule_engine.delete_rule(rule_id)
        if not success:
            logger.warning(f"Failed to delete rule with ID '{rule_id}'")
            return False

        # Return success status
        return True

    def get_rule(self, rule_id: str) -> Optional[dict]:
        """Gets a validation rule by ID"""
        # Get rule from rule engine
        rule = self._rule_engine.get_rule(rule_id)
        # If rule exists, convert to dictionary representation
        if rule:
            return rule.to_dict()
        # Return rule dictionary or None if not found
        return None

    def list_rules(
        self,
        rule_type: Optional[ValidationRuleType] = None,
        dimension: Optional[QualityDimension] = None,
        target_dataset: Optional[str] = None,
        target_table: Optional[str] = None,
    ) -> list:
        """Lists all validation rules, optionally filtered by type or dimension"""
        # Get rules from rule engine with filters
        rules = self._rule_engine.list_rules(rule_type, dimension, target_dataset, target_table)
        # Convert each rule to dictionary representation
        rule_dicts = [rule.to_dict() for rule in rules]
        # Return list of rule dictionaries
        return rule_dicts

    def create_rule_from_template(
        self,
        rule_type: ValidationRuleType,
        dimension: QualityDimension,
        name: str,
        description: str,
        parameters: dict,
    ) -> typing.Tuple[bool, str]:
        """Creates a new rule from a template with provided values"""
        # Create rule template using create_rule_template
        template = create_rule_template(rule_type, dimension)
        # Update template with provided values
        template["name"] = name
        template["description"] = description
        template["parameters"] = parameters
        # Create rule using the populated template
        return self.create_rule(template)

    def save_rules_to_file(self, file_path: str, rule_ids: Optional[list] = None) -> bool:
        """Saves rules to a file in the specified format"""
        # Get rules to save (all or specified by rule_ids)
        rules = []
        if rule_ids:
            for rule_id in rule_ids:
                rule = self._rule_engine.get_rule(rule_id)
                if rule:
                    rules.append(rule)
        else:
            rules = list(self._rule_engine._rule_registry.values())

        # Convert rules to dictionary representations
        rule_dicts = [rule.to_dict() for rule in rules]

        # Save rules to file using rule_loader.save_rules_to_file
        return save_rules_to_file(file_path, rule_dicts)

    def load_rules_from_file(self, file_path: str) -> typing.Tuple[int, list]:
        """Loads rules from a file and registers them with the rule engine"""
        # Load rule definitions from file using rule_loader.load_rules_from_file
        try:
            rules, errors = load_rules_from_file(file_path)
        except FileNotFoundError as e:
            return 0, [str(e)]

        success_count = 0
        load_errors = []

        # Validate each rule structure and parameters
        for rule in rules:
            is_valid, error_message = validate_rule_structure(rule)
            if is_valid:
                # Register valid rules with rule engine
                success, rule_id = self._rule_engine.register_rule(rule)
                if success:
                    success_count += 1
                else:
                    load_errors.append(f"Error registering rule: {rule_id}")
            else:
                # Track errors for invalid rules
                load_errors.append(f"Invalid rule structure: {error_message}")

        # Return count of successfully loaded rules and list of errors
        return success_count, load_errors

    def export_rules(self, file_path: str, rule_ids: Optional[list] = None) -> bool:
        """Exports rules to a file in the specified format"""
        # Alias for save_rules_to_file for backward compatibility
        return self.save_rules_to_file(file_path, rule_ids)

    def import_rules(self, file_path: str) -> typing.Tuple[int, list]:
        """Imports rules from a file"""
        # Alias for load_rules_from_file for backward compatibility
        return self.load_rules_from_file(file_path)

    def preview_rule_as_expectation(self, rule_definition: dict) -> dict:
        """Previews how a rule would be translated to a Great Expectations expectation"""
        # Validate rule structure and parameters
        is_valid, error_message = validate_rule_structure(rule_definition)
        if not is_valid:
            raise ValueError(f"Invalid rule structure: {error_message}")

        # Map rule to expectation using map_rule_to_expectation
        expectation_config = map_rule_to_expectation(rule_definition)
        # Return expectation configuration
        return expectation_config

    def get_rule_templates_by_type(self, rule_type: Optional[ValidationRuleType] = None) -> dict:
        """Gets rule templates for all or specified rule types"""
        # If rule_type is provided, return template for that specific type
        if rule_type:
            return {rule_type.value: create_rule_template(rule_type, QualityDimension.ACCURACY)}
        # Otherwise, return templates for all rule types
        else:
            return get_rule_templates()

    def validate_rule(self, rule_definition: dict) -> typing.Tuple[bool, str]:
        """Validates a rule definition without creating it"""
        # Validate rule structure using validate_rule_structure
        is_valid, error_message = validate_rule_structure(rule_definition)
        if not is_valid:
            return False, error_message

        # Validate rule parameters using validate_rule_parameters
        is_valid, error_message = validate_rule_parameters(rule_definition)
        if not is_valid:
            return False, error_message

        # Return validation result and error message if any
        return True, ""


class RuleTemplate:
    """Class representing a template for a validation rule type"""

    rule_type: ValidationRuleType
    name: str
    description: str
    parameters: dict
    required_parameters: list
    parameter_types: dict

    def __init__(
        self,
        rule_type: ValidationRuleType,
        name: str,
        description: str,
        parameters: dict,
        required_parameters: list,
        parameter_types: dict,
    ):
        """Initialize a rule template for a specific rule type"""
        # Set rule_type, name, and description
        self.rule_type = rule_type
        self.name = name
        self.description = description
        # Set parameters with default values
        self.parameters = parameters
        # Set required_parameters list
        self.required_parameters = required_parameters
        # Set parameter_types dictionary
        self.parameter_types = parameter_types

    def to_dict(self) -> dict:
        """Convert template to dictionary representation"""
        # Create dictionary with template properties
        template_dict = {
            "rule_type": self.rule_type.value,
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "required_parameters": self.required_parameters,
            "parameter_types": self.parameter_types,
        }
        # Convert enum values to strings
        # Return the dictionary
        return template_dict

    def create_rule(self, name: str, description: str, parameters: dict, dimension: Optional[QualityDimension] = None) -> dict:
        """Create a rule from this template with provided values"""
        # Create base rule dictionary from template
        rule = {
            "name": name,
            "description": description,
            "type": self.rule_type.value,
            "dimension": dimension.value if dimension else QualityDimension.ACCURACY.value,
            "parameters": self.parameters.copy(),
        }
        # Update with provided name, description, and dimension
        # Validate and merge provided parameters
        for param_name, param_value in parameters.items():
            if param_name in rule["parameters"]:
                rule["parameters"][param_name] = param_value
        # Return complete rule definition
        return rule

    def validate_parameters(self, parameters: dict) -> typing.Tuple[bool, str]:
        """Validate parameters against template requirements"""
        # Check that all required parameters are present
        for required_param in self.required_parameters:
            if required_param not in parameters:
                return False, f"Missing required parameter: {required_param}"
        # Validate parameter types against parameter_types
        for param_name, param_type in self.parameter_types.items():
            if param_name in parameters:
                if not isinstance(parameters[param_name], param_type):
                    return False, f"Invalid type for parameter '{param_name}': expected {param_type.__name__}"
        # Return validation result and error message if any
        return True, ""