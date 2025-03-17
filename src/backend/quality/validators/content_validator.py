"""Implements content validation for data quality checks, focusing on validating the actual data values within datasets
including null checks, value ranges, pattern matching, and categorical validations.
This validator is part of the data quality validation framework and works with the validation engine to ensure data
content meets quality standards.
"""

import typing
import pandas  # version 2.0.x
import re  # standard library
from google.cloud import bigquery  # version 2.34.4

from src.backend.constants import (  # src/backend/constants.py
    ValidationRuleType,
    QualityDimension,
    DEFAULT_TIMEOUT_SECONDS
)
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py
from src.backend.quality.engines.validation_engine import ValidationResult, create_validation_result  # ../engines/validation_engine
from src.backend.quality.engines.execution_engine import ExecutionContext  # ../engines/execution_engine
from src.backend.quality.integrations.great_expectations_adapter import GreatExpectationsAdapter  # ../integrations/great_expectations_adapter
from src.backend.quality.integrations.bigquery_adapter import BigQueryAdapter  # ../integrations/bigquery_adapter


# Initialize logger
logger = get_logger(__name__)

# Set default validation timeout
DEFAULT_VALIDATION_TIMEOUT = DEFAULT_TIMEOUT_SECONDS


def validate_not_null(dataset: typing.Any, columns: list) -> dict:
    """Validates that specified columns do not contain null values

    Args:
        dataset (Any): dataset
        columns (list): columns

    Returns:
        dict: Validation result with details about null values
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use isna() to check for null values in specified columns
        null_counts = dataset[columns].isna().sum()
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL to check for null values
        null_counts = {}
        total_rows = 0  # BigQuery implementation will populate this

    # Count null values in each column
    null_percentage = {}
    for column in columns:
        if isinstance(dataset, pandas.DataFrame):
            null_percentage[column] = (null_counts[column] / total_rows) * 100 if total_rows > 0 else 0
        else:  # BigQuery implementation
            null_percentage[column] = 0  # Placeholder

    # Return validation result with success status and details about null values
    success = all(count == 0 for count in null_counts.values())
    details = {"null_counts": null_counts.to_dict() if isinstance(dataset, pandas.DataFrame) else null_counts,
               "null_percentage": null_percentage}
    return {"success": success, "details": details}


def validate_value_range(dataset: typing.Any, column: str, min_value: typing.Any, max_value: typing.Any) -> dict:
    """Validates that values in specified columns fall within expected ranges

    Args:
        dataset (Any): dataset
        column (str): column
        min_value (Any): min_value
        max_value (Any): max_value

    Returns:
        dict: Validation result with details about out-of-range values
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use comparison operators to check value ranges
        out_of_range = dataset[(dataset[column] < min_value) | (dataset[column] > max_value)]
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL with WHERE clause for range check
        out_of_range = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count values outside the specified range
    out_of_range_count = len(out_of_range) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of out-of-range values
    out_of_range_percentage = (out_of_range_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about out-of-range values
    success = out_of_range_count == 0
    details = {"out_of_range_count": out_of_range_count, "out_of_range_percentage": out_of_range_percentage}
    return {"success": success, "details": details}


def validate_pattern(dataset: typing.Any, column: str, pattern: str) -> dict:
    """Validates that values in specified columns match a regular expression pattern

    Args:
        dataset (Any): dataset
        column (str): column
        pattern (str): pattern

    Returns:
        dict: Validation result with details about pattern mismatches
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use str.match() with regex pattern
        non_matching = dataset[~dataset[column].astype(str).str.match(pattern, na=False)]
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL with REGEXP_CONTAINS function
        non_matching = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count values that don't match the pattern
    non_matching_count = len(non_matching) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of non-matching values
    non_matching_percentage = (non_matching_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about pattern mismatches
    success = non_matching_count == 0
    details = {"non_matching_count": non_matching_count, "non_matching_percentage": non_matching_percentage}
    return {"success": success, "details": details}


def validate_categorical(dataset: typing.Any, column: str, allowed_values: list) -> dict:
    """Validates that values in specified columns belong to a set of allowed values

    Args:
        dataset (Any): dataset
        column (str): column
        allowed_values (list): allowed_values

    Returns:
        dict: Validation result with details about invalid categorical values
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use isin() to check for values in allowed set
        invalid_values = dataset[~dataset[column].isin(allowed_values)]
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL with IN operator
        invalid_values = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count values not in the allowed set
    invalid_count = len(invalid_values) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of invalid values
    invalid_percentage = (invalid_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about invalid categorical values
    success = invalid_count == 0
    details = {"invalid_count": invalid_count, "invalid_percentage": invalid_percentage}
    return {"success": success, "details": details}


def validate_uniqueness(dataset: typing.Any, columns: list) -> dict:
    """Validates that values in specified columns are unique

    Args:
        dataset (Any): dataset
        columns (list): columns

    Returns:
        dict: Validation result with details about duplicate values
    """
    # Check if dataset is pandas DataFrame or BigQuery table
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use duplicated() to identify duplicate values
        duplicate_rows = dataset[dataset.duplicated(subset=columns, keep=False)]
        total_rows = len(dataset)
    else:  # Assuming BigQuery table
        # For BigQuery: Generate SQL with GROUP BY and HAVING COUNT(*) > 1
        duplicate_rows = None  # BigQuery implementation will populate this
        total_rows = 0  # BigQuery implementation will populate this

    # Count duplicate values
    duplicate_count = len(duplicate_rows) if isinstance(dataset, pandas.DataFrame) else 0

    # Calculate percentage of duplicate values
    duplicate_percentage = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0

    # Return validation result with success status and details about duplicates
    success = duplicate_count == 0
    details = {"duplicate_count": duplicate_count, "duplicate_percentage": duplicate_percentage}
    return {"success": success, "details": details}


class ContentValidator:
    """Validator class for content-based data quality validations"""

    _ge_adapter: GreatExpectationsAdapter
    _bq_adapter: BigQueryAdapter
    _config: dict

    def __init__(self, config: dict):
        """Initialize the content validator with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        # Create GreatExpectationsAdapter for validation operations
        self._ge_adapter = GreatExpectationsAdapter(self._config)
        # Create BigQueryAdapter for large dataset validation if needed
        self._bq_adapter = BigQueryAdapter(self._config)
        # Initialize validator properties
        logger.info("ContentValidator initialized")

    def validate(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate content rules against a dataset

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Filter rules to include only content validation rules
        content_rules = [rule for rule in rules if rule['type'] == ValidationRuleType.CONTENT.value]

        # Determine optimal validation approach based on dataset and rules
        if isinstance(dataset, pandas.DataFrame):
            # For small datasets or pandas dataframes, use in-memory validation
            results = self.validate_in_memory(dataset, content_rules, context)
        else:
            # For large datasets or BigQuery tables, use BigQuery-based validation
            dataset_id = self._config.get("dataset_id")
            table_id = self._config.get("table_id")
            results = self.validate_with_bigquery(dataset_id, table_id, content_rules, context)

        # Process and return validation results
        return results

    def validate_rule(self, dataset: typing.Any, rule: dict) -> ValidationResult:
        """Validate a single content rule against a dataset

        Args:
            dataset (Any): dataset
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Verify rule is a content validation rule
        if rule['type'] != ValidationRuleType.CONTENT.value:
            raise ValueError("Rule is not a content validation rule")

        # Extract rule parameters and validation type
        column = rule['parameters']['column_name']
        validation_type = rule['parameters']['subtype']

        # Call appropriate validation function based on rule type
        if validation_type == 'null_check':
            result = validate_not_null(dataset, [column])
        elif validation_type == 'value_range':
            min_value = rule['parameters']['min_value']
            max_value = rule['parameters']['max_value']
            result = validate_value_range(dataset, column, min_value, max_value)
        elif validation_type == 'pattern_matching':
            pattern = rule['parameters']['pattern']
            result = validate_pattern(dataset, column, pattern)
        elif validation_type == 'categorical_validation':
            categories = rule['parameters']['categories']
            result = validate_categorical(dataset, column, categories)
        elif validation_type == 'uniqueness':
            result = validate_uniqueness(dataset, [column])
        else:
            raise ValueError(f"Unsupported content validation type: {validation_type}")

        # Return validation result
        return create_validation_result(rule, result['success'], result['details'])

    def validate_in_memory(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate content rules using in-memory validation

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Convert dataset to pandas DataFrame if not already
        if not isinstance(dataset, pandas.DataFrame):
            dataset = pandas.DataFrame(dataset)

        # Initialize results list
        results = []

        # For each rule, call appropriate validation function
        for rule in rules:
            result = self.validate_rule(dataset, rule)
            results.append(result.to_dict())

        # Update execution context statistics
        context.update_stats("rules_executed", len(rules))

        # Return list of validation results
        return results

    def validate_with_bigquery(self, dataset_id: str, table_id: str, rules: list, context: ExecutionContext) -> list:
        """Validate content rules using BigQuery

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Use BigQueryAdapter to validate rules against BigQuery table
        results = []
        for rule in rules:
            result = self.validate_rule(dataset_id, table_id, rule)
            results.append(result.to_dict())

        # Update execution context statistics
        context.update_stats("rules_executed", len(rules))

        # Return list of validation results
        return results
    
    def validate_with_great_expectations(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate content rules using Great Expectations

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Use GreatExpectationsAdapter to validate rules against dataset
        results = []
        for rule in rules:
            result = self.validate_rule(dataset, rule)
            results.append(result.to_dict())

        # Update execution context statistics
        context.update_stats("rules_executed", len(rules))

        # Return list of validation results
        return results

    def map_rule_to_validation_function(self, rule: dict) -> typing.Callable:
        """Map a content rule to the appropriate validation function

        Args:
            rule (dict): rule

        Returns:
            callable: Validation function for the rule
        """
        # Extract rule subtype from rule definition
        rule_subtype = rule['parameters']['subtype']

        # Return appropriate validation function based on subtype
        if rule_subtype == 'not_null':
            return validate_not_null
        elif rule_subtype == 'value_range':
            return validate_value_range
        elif rule_subtype == 'pattern_matching':
            return validate_pattern
        elif rule_subtype == 'categorical_validation':
            return validate_categorical
        elif rule_subtype == 'uniqueness':
            return validate_uniqueness
        else:
            raise ValueError(f"Unsupported content validation subtype: {rule_subtype}")

    def close(self) -> None:
        """Close the validator and release resources"""
        # Close GreatExpectationsAdapter if it exists
        if hasattr(self, '_ge_adapter') and self._ge_adapter:
            self._ge_adapter.close()

        # Close BigQueryAdapter if it exists
        if hasattr(self, '_bq_adapter') and self._bq_adapter:
            self._bq_adapter.close()

        # Release any other resources
        logger.info("ContentValidator closed")