"""
Example implementation of a custom validator for the self-healing data pipeline.
This validator demonstrates how to extend the validation framework to implement
specialized data quality checks that aren't covered by the standard validators.
It includes integration with the validation engine and self-healing capabilities.
"""

import typing
import pandas  # version 2.0.x
import numpy  # version 1.24.x
import re  # standard library

from src.backend.constants import (  # src/backend/constants.py
    ValidationRuleType,
    QualityDimension,
    DEFAULT_TIMEOUT_SECONDS
)
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py
from src.backend.quality.engines.validation_engine import ValidationResult, create_validation_result  # ../engines/validation_engine
from src.backend.quality.engines.execution_engine import ExecutionContext  # ../engines/execution_engine
from src.backend.quality.validators.content_validator import ContentValidator  # ../validators/content_validator


# Initialize logger
logger = get_logger(__name__)


def validate_date_format(dataset: typing.Any, column: str, date_format: str) -> dict:
    """Validates that date values in a column match a specified format

    Args:
        dataset (Any): dataset
        column (str): column
        date_format (str): date_format

    Returns:
        dict: Validation result with details about date format violations
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use pd.to_datetime with format parameter and errors='coerce'
        dates = pandas.to_datetime(dataset[column], format=date_format, errors='coerce')
        # Count values that couldn't be parsed with the specified format
        invalid_format_count = dates.isna().sum()
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL to check for date format
        invalid_format_count = 0  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Calculate percentage of invalid date formats
    invalid_format_percentage = (invalid_format_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about format violations
    success = invalid_format_count == 0
    details = {"invalid_format_count": invalid_format_count, "invalid_format_percentage": invalid_format_percentage}
    return {"success": success, "details": details}


def validate_business_rule(dataset: typing.Any, rule_name: str, rule_function: typing.Callable, rule_params: dict) -> dict:
    """Validates that data satisfies a custom business rule defined as a function

    Args:
        dataset (Any): dataset
        rule_name (str): rule_name
        rule_function (callable): rule_function
        rule_params (dict): rule_params

    Returns:
        dict: Validation result with details about business rule violations
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Apply rule_function to DataFrame with rule_params
        violations = dataset[~dataset.apply(lambda row: rule_function(row, **rule_params), axis=1)]
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Convert rule to SQL if possible or extract sample for validation
        violations = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count records that violate the business rule
    violation_count = len(violations) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of violations
    violation_percentage = (violation_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about violations
    success = violation_count == 0
    details = {"violation_count": violation_count, "violation_percentage": violation_percentage}
    return {"success": success, "details": details}


def validate_cross_column_consistency(dataset: typing.Any, column1: str, column2: str, consistency_rule: str) -> dict:
    """Validates consistency between related columns based on specified rules

    Args:
        dataset (Any): dataset
        column1 (str): column1
        column2 (str): column2
        consistency_rule (str): consistency_rule

    Returns:
        dict: Validation result with details about consistency violations
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # Parse consistency_rule to determine relationship (equals, greater_than, etc.)
        if consistency_rule == "equals":
            # Apply appropriate comparison operation between columns
            inconsistent = dataset[dataset[column1] != dataset[column2]]
        elif consistency_rule == "greater_than":
            inconsistent = dataset[dataset[column1] <= dataset[column2]]
        else:
            raise ValueError(f"Unsupported consistency rule: {consistency_rule}")
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL with appropriate comparison operators
        inconsistent = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count records that violate the consistency rule
    inconsistent_count = len(inconsistent) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of violations
    inconsistent_percentage = (inconsistent_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about violations
    success = inconsistent_count == 0
    details = {"inconsistent_count": inconsistent_count, "inconsistent_percentage": inconsistent_percentage}
    return {"success": success, "details": details}


class CustomValidator(ContentValidator):
    """Custom validator implementation that extends ContentValidator with specialized validation capabilities"""

    _config: dict
    _custom_rule_registry: dict

    def __init__(self, config: dict):
        """Initialize the custom validator with configuration

        Args:
            config (dict): config
        """
        # Call parent ContentValidator constructor with config
        super().__init__(config)
        # Initialize custom rule registry dictionary
        self._custom_rule_registry = {}
        # Register custom validation functions
        self.register_custom_rule_type("date_format", validate_date_format)
        self.register_custom_rule_type("business_rule", validate_business_rule)
        self.register_custom_rule_type("cross_column_consistency", validate_cross_column_consistency)
        # Log custom validator initialization
        logger.info("CustomValidator initialized")

    def validate(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate custom rules against a dataset

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Filter rules to include only custom validation rules
        custom_rules = [rule for rule in rules if rule['type'] == "CUSTOM"]

        # If no custom rules, call parent validate method
        if not custom_rules:
            return super().validate(dataset, rules, context)

        # Initialize results list
        results = []

        # For each custom rule, call validate_rule method
        for rule in custom_rules:
            result = self.validate_rule(dataset, rule)
            results.append(result)

        # Combine custom validation results with standard validation results
        all_results = super().validate(dataset, rules, context) + results

        # Update execution context with validation statistics
        context.update_stats("custom_rules_executed", len(custom_rules))

        # Return combined validation results
        return all_results

    def validate_rule(self, dataset: typing.Any, rule: dict) -> ValidationResult:
        """Validate a single custom rule against a dataset

        Args:
            dataset (Any): dataset
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Extract rule subtype and parameters
        rule_subtype = rule['parameters']['subtype']
        params = rule['parameters']

        # Map rule to appropriate validation function
        validation_function = self.map_rule_to_validation_function(rule)

        # Execute validation function with dataset and parameters
        result = validation_function(dataset, **params)

        # Create and return ValidationResult with validation outcome
        return create_validation_result(rule, result['success'], result['details'])

    def register_custom_rule_type(self, rule_type: str, validation_function: typing.Callable) -> None:
        """Register a custom rule type with validation function

        Args:
            rule_type (str): rule_type
            validation_function (callable): validation_function
        """
        # Validate rule_type is a string
        if not isinstance(rule_type, str):
            raise TypeError("rule_type must be a string")

        # Validate validation_function is callable
        if not callable(validation_function):
            raise TypeError("validation_function must be callable")

        # Add rule_type and validation_function to custom rule registry
        self._custom_rule_registry[rule_type] = validation_function

        # Log registration of custom rule type
        logger.info(f"Registered custom rule type: {rule_type}")

    def map_rule_to_validation_function(self, rule: dict) -> typing.Callable:
        """Map a custom rule to the appropriate validation function

        Args:
            rule (dict): rule

        Returns:
            callable: Validation function for the rule
        """
        # Extract rule subtype from rule definition
        rule_subtype = rule['parameters']['subtype']

        # Check if rule subtype exists in custom rule registry
        if rule_subtype in self._custom_rule_registry:
            # If found, return registered validation function
            return self._custom_rule_registry[rule_subtype]
        else:
            # If not found, check parent class for standard validation functions
            try:
                return super().map_rule_to_validation_function(rule)
            except ValueError:
                # If not found in either, raise ValueError for unsupported rule type
                raise ValueError(f"Unsupported rule type: {rule_subtype}")

    def close(self) -> None:
        """Close the validator and release resources"""
        # Call parent close method
        super().close()
        # Clear custom rule registry
        self._custom_rule_registry.clear()
        # Release any other resources
        logger.info("CustomValidator closed")


class BusinessRuleValidator:
    """Specialized validator for complex business rules that may span multiple columns or datasets"""

    _config: dict
    _rule_definitions: dict

    def __init__(self, config: dict):
        """Initialize the business rule validator with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        # Initialize rule definitions dictionary
        self._rule_definitions = {}
        # Load predefined business rules from configuration if available
        self.load_rules_from_config(self._config.get('rules_config_path'))
        # Log business rule validator initialization
        logger.info("BusinessRuleValidator initialized")

    def validate(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate business rules against a dataset

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Filter rules to include only business rule validations
        business_rules = [rule for rule in rules if rule['type'] == "BUSINESS_RULE"]

        # Initialize results list
        results = []

        # For each rule, call validate_rule method
        for rule in business_rules:
            result = self.validate_rule(dataset, rule)
            results.append(result)

        # Update execution context with validation statistics
        context.update_stats("business_rules_executed", len(business_rules))

        # Return list of validation results
        return results

    def validate_rule(self, dataset: typing.Any, rule: dict) -> ValidationResult:
        """Validate a single business rule against a dataset

        Args:
            dataset (Any): dataset
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Extract rule name and parameters
        rule_name = rule['parameters']['rule_name']
        params = rule['parameters']

        # Look up rule definition in rule definitions dictionary
        if rule_name not in self._rule_definitions:
            raise ValueError(f"Business rule not found: {rule_name}")

        rule_definition = self._rule_definitions[rule_name]

        # Execute rule validation function with dataset and parameters
        result = rule_definition['function'](dataset, **params)

        # Create and return ValidationResult with validation outcome
        return create_validation_result(rule, result['success'], result['details'])

    def register_rule(self, rule_name: str, rule_function: typing.Callable, rule_metadata: dict) -> None:
        """Register a business rule definition

        Args:
            rule_name (str): rule_name
            rule_function (callable): rule_function
            rule_metadata (dict): rule_metadata
        """
        # Validate rule_name is a string
        if not isinstance(rule_name, str):
            raise TypeError("rule_name must be a string")

        # Validate rule_function is callable
        if not callable(rule_function):
            raise TypeError("rule_function must be callable")

        # Create rule definition with function and metadata
        rule_definition = {
            'function': rule_function,
            'metadata': rule_metadata
        }

        # Add rule definition to rule definitions dictionary
        self._rule_definitions[rule_name] = rule_definition

        # Log registration of business rule
        logger.info(f"Registered business rule: {rule_name}")

    def load_rules_from_config(self, config_path: str) -> dict:
        """Load business rule definitions from configuration

        Args:
            config_path (str): config_path

        Returns:
            dict: Loaded rule definitions
        """
        # Load rule definitions from specified configuration path
        if not config_path:
            logger.warning("No rules_config_path specified, skipping rule loading")
            return {}

        # Load rule definitions from specified configuration path
        try:
            with open(config_path, 'r') as f:
                rule_definitions = json.load(f)
        except FileNotFoundError:
            logger.warning(f"Rules config file not found: {config_path}")
            return {}
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON format in rules config file: {config_path}")
            return {}

        # For each rule definition, register rule
        for rule_name, rule_def in rule_definitions.items():
            self.register_rule(rule_name, rule_def['function'], rule_def['metadata'])

        # Log number of rules loaded
        logger.info(f"Loaded {len(rule_definitions)} business rules from config")

        # Return dictionary of loaded rule definitions
        return rule_definitions

    def close(self) -> None:
        """Close the validator and release resources"""
        # Clear rule definitions dictionary
        self._rule_definitions.clear()
        # Release any other resources
        logger.info("BusinessRuleValidator closed")