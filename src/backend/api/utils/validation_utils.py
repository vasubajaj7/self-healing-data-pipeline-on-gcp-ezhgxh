"""
Utility functions and classes for validating API requests, data formats,
and domain-specific objects in the self-healing data pipeline.

This module provides validation utilities for ensuring data integrity in the pipeline,
including email, URL, and UUID validation, schema validation against JSON schemas,
field type and value range validation, and domain-specific validation functions.
"""

import re
import uuid
import datetime
from typing import Dict, List, Any, Optional, Union, Tuple, Type, Callable

import jsonschema  # jsonschema ^4.17.3
from email_validator import validate_email as email_validate, EmailNotValidError  # email-validator ^1.3.1
from pydantic import ValidationError as PydanticValidationError, BaseModel  # pydantic ^1.9.0
from fastapi import exceptions  # fastapi ^0.95.0

from ..models.error_models import ValidationError, ErrorDetail, ErrorCategory, ErrorSeverity
from .response_utils import create_error_response, create_validation_error_response
from ...logging_config import get_logger
from ...constants import SelfHealingMode, HealingActionType, AlertSeverity, DataSourceType, FileFormat, QualityDimension

# Initialize logger
logger = get_logger(__name__)

# Regular expression patterns
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
URL_REGEX = re.compile(r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?::\d+)?(?:/[^\s]*)?$')

# Basic validation functions
def validate_email(email: str) -> bool:
    """
    Validates if a string is a properly formatted email address.
    
    Args:
        email: The email address to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        email_validate.validate_email(email)
        return True
    except EmailNotValidError:
        logger.debug(f"Invalid email format: {email}")
        return False


def validate_url(url: str) -> bool:
    """
    Validates if a string is a properly formatted URL.
    
    Args:
        url: The URL to validate
        
    Returns:
        True if valid, False otherwise
    """
    if URL_REGEX.match(url):
        return True
    logger.debug(f"Invalid URL format: {url}")
    return False


def validate_uuid(uuid_str: str) -> bool:
    """
    Validates if a string is a properly formatted UUID.
    
    Args:
        uuid_str: The UUID string to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        uuid.UUID(uuid_str)
        return True
    except ValueError:
        logger.debug(f"Invalid UUID format: {uuid_str}")
        return False


def validate_date_format(date_str: str, format_str: str) -> bool:
    """
    Validates if a string matches the specified date format.
    
    Args:
        date_str: The date string to validate
        format_str: The expected date format (e.g., "%Y-%m-%d")
        
    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.datetime.strptime(date_str, format_str)
        return True
    except ValueError:
        logger.debug(f"Invalid date format: {date_str}, expected: {format_str}")
        return False


def validate_json_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validates data against a JSON schema.
    
    Args:
        data: The data to validate
        schema: The JSON schema to validate against
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    try:
        jsonschema.validate(data, schema)
        return True, None
    except jsonschema.exceptions.ValidationError as e:
        error_message = str(e)
        logger.debug(f"JSON schema validation failed: {error_message}")
        return False, error_message


def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> Tuple[bool, List[str]]:
    """
    Validates that all required fields are present in the data.
    
    Args:
        data: The data to validate
        required_fields: List of field names that must be present
        
    Returns:
        Tuple containing validation result (True/False) and list of missing fields
    """
    missing_fields = [field for field in required_fields if field not in data]
    if not missing_fields:
        return True, []
    
    logger.debug(f"Missing required fields: {missing_fields}")
    return False, missing_fields


def validate_field_type(value: Any, expected_type: type, field_name: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Validates that a field is of the expected type.
    
    Args:
        value: The value to validate
        expected_type: The expected type
        field_name: Optional field name for error messages
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    if isinstance(value, expected_type):
        return True, None
    
    field_desc = f"Field '{field_name}'" if field_name else "Value"
    error_message = f"{field_desc} must be of type {expected_type.__name__}, got {type(value).__name__}"
    logger.debug(error_message)
    return False, error_message


def validate_numeric_range(
    value: Union[int, float],
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    field_name: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validates that a numeric value is within the specified range.
    
    Args:
        value: The numeric value to validate
        min_value: Optional minimum allowed value
        max_value: Optional maximum allowed value
        field_name: Optional field name for error messages
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    field_desc = f"Field '{field_name}'" if field_name else "Value"
    
    if min_value is not None and value < min_value:
        error_message = f"{field_desc} must be at least {min_value}, got {value}"
        logger.debug(error_message)
        return False, error_message
    
    if max_value is not None and value > max_value:
        error_message = f"{field_desc} must be at most {max_value}, got {value}"
        logger.debug(error_message)
        return False, error_message
    
    return True, None


def validate_string_length(
    value: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    field_name: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validates that a string's length is within the specified range.
    
    Args:
        value: The string to validate
        min_length: Optional minimum allowed length
        max_length: Optional maximum allowed length
        field_name: Optional field name for error messages
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    field_desc = f"Field '{field_name}'" if field_name else "String"
    
    if min_length is not None and len(value) < min_length:
        error_message = f"{field_desc} must have at least {min_length} characters, got {len(value)}"
        logger.debug(error_message)
        return False, error_message
    
    if max_length is not None and len(value) > max_length:
        error_message = f"{field_desc} must have at most {max_length} characters, got {len(value)}"
        logger.debug(error_message)
        return False, error_message
    
    return True, None


def validate_enum_value(
    value: Any,
    enum_class: type,
    field_name: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validates that a value is a valid member of an enumeration.
    
    Args:
        value: The value to validate
        enum_class: The enumeration class
        field_name: Optional field name for error messages
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    try:
        # Try to convert the value to an enum member
        enum_class(value)
        return True, None
    except (ValueError, TypeError):
        field_desc = f"Field '{field_name}'" if field_name else "Value"
        valid_values = [e.value for e in enum_class]
        error_message = f"{field_desc} must be one of {valid_values}, got {value}"
        logger.debug(error_message)
        return False, error_message


def validate_pattern(
    value: str,
    pattern: Union[str, re.Pattern],
    field_name: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Validates that a string matches the specified regex pattern.
    
    Args:
        value: The string to validate
        pattern: The regex pattern to match against
        field_name: Optional field name for error messages
        
    Returns:
        Tuple containing validation result (True/False) and error message if any
    """
    # Compile pattern if it's a string
    if isinstance(pattern, str):
        pattern = re.compile(pattern)
    
    if pattern.match(value):
        return True, None
    
    field_desc = f"Field '{field_name}'" if field_name else "Value"
    error_message = f"{field_desc} must match pattern {pattern.pattern}"
    logger.debug(error_message)
    return False, error_message


def validate_data(
    data: Dict[str, Any],
    rules: Dict[str, List['ValidationRule']]
) -> Tuple[bool, List[ErrorDetail]]:
    """
    Validates data against a set of validation rules.
    
    Args:
        data: The data to validate
        rules: Dictionary mapping field names to lists of validation rules
        
    Returns:
        Tuple containing validation result (True/False) and list of error details
    """
    error_details = []
    
    for field, field_rules in rules.items():
        if field in data:
            value = data[field]
            for rule in field_rules:
                valid, error = rule.validate(value, field)
                if not valid and error:
                    error_details.append(error)
    
    is_valid = len(error_details) == 0
    if not is_valid:
        logger.debug(f"Validation failed with {len(error_details)} errors")
    
    return is_valid, error_details


def handle_pydantic_validation_error(exc: PydanticValidationError) -> List[ErrorDetail]:
    """
    Converts Pydantic validation errors to standardized error details.
    
    Args:
        exc: The Pydantic validation error
        
    Returns:
        List of standardized error details
    """
    error_details = []
    
    for error in exc.errors():
        # Extract field path and error type
        loc = error.get("loc", [])
        field = ".".join(str(l) for l in loc)
        msg = error.get("msg", "")
        err_type = error.get("type", "")
        
        # Create error detail
        error_detail = create_error_detail(
            field=field,
            message=msg,
            code=err_type
        )
        error_details.append(error_detail)
    
    logger.debug(f"Pydantic validation failed with {len(error_details)} errors")
    return error_details


def handle_request_validation_error(exc: exceptions.RequestValidationError) -> List[ErrorDetail]:
    """
    Converts FastAPI request validation errors to standardized error details.
    
    Args:
        exc: The FastAPI request validation error
        
    Returns:
        List of standardized error details
    """
    error_details = []
    
    for error in exc.errors():
        # Extract field path and error type
        loc = error.get("loc", [])
        # Skip the first item if it's 'body' or 'query'
        if loc and loc[0] in ("body", "query", "path", "header") and len(loc) > 1:
            loc = loc[1:]
        
        field = ".".join(str(l) for l in loc)
        msg = error.get("msg", "")
        err_type = error.get("type", "")
        
        # Create error detail
        error_detail = create_error_detail(
            field=field,
            message=msg,
            code=err_type
        )
        error_details.append(error_detail)
    
    logger.debug(f"Request validation failed with {len(error_details)} errors")
    return error_details


def create_error_detail(
    field: str,
    message: str,
    code: Optional[str] = None,
    context: Optional[Any] = None
) -> ErrorDetail:
    """
    Creates a standardized error detail object.
    
    Args:
        field: The field name with error
        message: Error message
        code: Error code
        context: Additional context
        
    Returns:
        Standardized error detail object
    """
    return ErrorDetail(
        field=field,
        message=message,
        code=code,
        context=context
    )


def validate_model(data: Dict[str, Any], model_class: Type[BaseModel]) -> Tuple[bool, Union[BaseModel, List[ErrorDetail]]]:
    """
    Validates data against a Pydantic model.
    
    Args:
        data: The data to validate
        model_class: The Pydantic model class
        
    Returns:
        Tuple containing validation result (True/False) and either model instance or error details
    """
    try:
        model_instance = model_class(**data)
        return True, model_instance
    except PydanticValidationError as e:
        error_details = handle_pydantic_validation_error(e)
        logger.debug(f"Model validation failed for {model_class.__name__}")
        return False, error_details


# Domain-specific validation functions
def validate_source_connection(
    source_type: DataSourceType,
    connection_details: Dict[str, Any]
) -> Tuple[bool, List[ErrorDetail]]:
    """
    Validates source connection details based on source type.
    
    Args:
        source_type: Type of data source
        connection_details: Connection details to validate
        
    Returns:
        Tuple containing validation result (True/False) and list of error details
    """
    error_details = []
    
    # Define required fields based on source type
    required_fields = []
    if source_type == DataSourceType.GCS:
        required_fields = ["bucket_name", "file_pattern"]
    elif source_type == DataSourceType.CLOUD_SQL:
        required_fields = ["instance_name", "database", "credentials"]
    elif source_type == DataSourceType.API:
        required_fields = ["endpoint_url", "auth_method", "headers"]
    elif source_type == DataSourceType.CUSTOM:
        required_fields = ["connector_class", "parameters"]
    
    # Validate required fields
    valid, missing_fields = validate_required_fields(connection_details, required_fields)
    if not valid:
        for field in missing_fields:
            error_details.append(create_error_detail(
                field=f"connection_details.{field}",
                message=f"Missing required field for {source_type.value} source",
                code="missing_required_field"
            ))
    
    # Validate specific field formats if present
    if source_type == DataSourceType.GCS and "bucket_name" in connection_details:
        bucket_name = connection_details["bucket_name"]
        if not isinstance(bucket_name, str) or len(bucket_name) < 3:
            error_details.append(create_error_detail(
                field="connection_details.bucket_name",
                message="Bucket name must be a string with at least 3 characters",
                code="invalid_bucket_name"
            ))
    
    if source_type == DataSourceType.API and "endpoint_url" in connection_details:
        endpoint_url = connection_details["endpoint_url"]
        if not validate_url(endpoint_url):
            error_details.append(create_error_detail(
                field="connection_details.endpoint_url",
                message="Endpoint URL must be a valid URL",
                code="invalid_url_format"
            ))
    
    is_valid = len(error_details) == 0
    if not is_valid:
        logger.debug(f"Source connection validation failed with {len(error_details)} errors")
    
    return is_valid, error_details


def validate_pipeline_configuration(configuration: Dict[str, Any]) -> Tuple[bool, List[ErrorDetail]]:
    """
    Validates pipeline configuration.
    
    Args:
        configuration: Pipeline configuration to validate
        
    Returns:
        Tuple containing validation result (True/False) and list of error details
    """
    error_details = []
    
    # Validate required fields
    required_fields = ["schedule", "extraction_params"]
    valid, missing_fields = validate_required_fields(configuration, required_fields)
    if not valid:
        for field in missing_fields:
            error_details.append(create_error_detail(
                field=field,
                message=f"Missing required field in pipeline configuration",
                code="missing_required_field"
            ))
    
    # Validate schedule format if present
    if "schedule" in configuration:
        schedule = configuration["schedule"]
        if not isinstance(schedule, str) and not isinstance(schedule, dict):
            error_details.append(create_error_detail(
                field="schedule",
                message="Schedule must be a string (cron format) or a dictionary",
                code="invalid_schedule_format"
            ))
    
    # Validate extraction_params structure if present
    if "extraction_params" in configuration:
        extraction_params = configuration["extraction_params"]
        if not isinstance(extraction_params, dict):
            error_details.append(create_error_detail(
                field="extraction_params",
                message="Extraction parameters must be a dictionary",
                code="invalid_params_format"
            ))
        else:
            # Validate based on source type if specified
            if "source_type" in extraction_params:
                source_type = extraction_params["source_type"]
                if "connection_details" in extraction_params:
                    try:
                        source_type_enum = DataSourceType(source_type)
                        valid, conn_errors = validate_source_connection(
                            source_type_enum,
                            extraction_params["connection_details"]
                        )
                        if not valid:
                            error_details.extend(conn_errors)
                    except ValueError:
                        error_details.append(create_error_detail(
                            field="extraction_params.source_type",
                            message=f"Invalid source type: {source_type}",
                            code="invalid_source_type"
                        ))
    
    is_valid = len(error_details) == 0
    if not is_valid:
        logger.debug(f"Pipeline configuration validation failed with {len(error_details)} errors")
    
    return is_valid, error_details


def validate_quality_rule_definition(
    expectation_type: str,
    rule_definition: Dict[str, Any]
) -> Tuple[bool, List[ErrorDetail]]:
    """
    Validates quality rule definition based on expectation type.
    
    Args:
        expectation_type: Type of Great Expectations expectation
        rule_definition: Rule definition to validate
        
    Returns:
        Tuple containing validation result (True/False) and list of error details
    """
    error_details = []
    
    # Define required fields based on expectation type
    required_fields = []
    if expectation_type == 'expect_column_values_to_not_be_null':
        required_fields = ["column"]
    elif expectation_type == 'expect_column_values_to_be_between':
        required_fields = ["column", "min_value", "max_value"]
    elif expectation_type == 'expect_column_values_to_match_regex':
        required_fields = ["column", "regex"]
    elif expectation_type == 'expect_column_pair_values_to_be_equal':
        required_fields = ["column_A", "column_B"]
    elif expectation_type == 'expect_column_values_to_be_in_set':
        required_fields = ["column", "value_set"]
    elif expectation_type == 'expect_column_values_to_be_unique':
        required_fields = ["column"]
    
    # Validate required fields
    valid, missing_fields = validate_required_fields(rule_definition, required_fields)
    if not valid:
        for field in missing_fields:
            error_details.append(create_error_detail(
                field=field,
                message=f"Missing required field for {expectation_type}",
                code="missing_required_field"
            ))
    
    # Validate specific field types if present
    if expectation_type == 'expect_column_values_to_be_between':
        if "min_value" in rule_definition and "max_value" in rule_definition:
            min_value = rule_definition["min_value"]
            max_value = rule_definition["max_value"]
            
            # Check if min_value and max_value are numeric
            if not isinstance(min_value, (int, float)) and min_value is not None:
                error_details.append(create_error_detail(
                    field="min_value",
                    message="min_value must be numeric or null",
                    code="invalid_type"
                ))
                
            if not isinstance(max_value, (int, float)) and max_value is not None:
                error_details.append(create_error_detail(
                    field="max_value",
                    message="max_value must be numeric or null",
                    code="invalid_type"
                ))
                
            # Check if min_value <= max_value when both are provided
            if (isinstance(min_value, (int, float)) and 
                isinstance(max_value, (int, float)) and 
                min_value > max_value):
                error_details.append(create_error_detail(
                    field="min_value",
                    message="min_value cannot be greater than max_value",
                    code="invalid_range"
                ))
    
    is_valid = len(error_details) == 0
    if not is_valid:
        logger.debug(f"Quality rule validation failed with {len(error_details)} errors")
    
    return is_valid, error_details


def validate_healing_action_definition(
    action_type: HealingActionType,
    action_definition: Dict[str, Any]
) -> Tuple[bool, List[ErrorDetail]]:
    """
    Validates healing action definition based on action type.
    
    Args:
        action_type: Type of healing action
        action_definition: Action definition to validate
        
    Returns:
        Tuple containing validation result (True/False) and list of error details
    """
    error_details = []
    
    # Define required fields based on action type
    required_fields = []
    if action_type == HealingActionType.DATA_CORRECTION:
        required_fields = ["correction_type", "parameters"]
    elif action_type == HealingActionType.PIPELINE_RETRY:
        required_fields = ["max_retries", "backoff_factor"]
    elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
        required_fields = ["parameters_to_adjust", "adjustment_strategy"]
    elif action_type == HealingActionType.RESOURCE_SCALING:
        required_fields = ["resource_type", "scaling_factor"]
    elif action_type == HealingActionType.SCHEMA_EVOLUTION:
        required_fields = ["schema_changes", "compatibility_mode"]
    elif action_type == HealingActionType.DEPENDENCY_RESOLUTION:
        required_fields = ["dependency_type", "resolution_strategy"]
    
    # Validate required fields
    valid, missing_fields = validate_required_fields(action_definition, required_fields)
    if not valid:
        for field in missing_fields:
            error_details.append(create_error_detail(
                field=field,
                message=f"Missing required field for {action_type.value}",
                code="missing_required_field"
            ))
    
    # Validate specific field types and values
    if action_type == HealingActionType.PIPELINE_RETRY:
        if "max_retries" in action_definition:
            max_retries = action_definition["max_retries"]
            if not isinstance(max_retries, int) or max_retries < 1:
                error_details.append(create_error_detail(
                    field="max_retries",
                    message="max_retries must be a positive integer",
                    code="invalid_value"
                ))
        
        if "backoff_factor" in action_definition:
            backoff_factor = action_definition["backoff_factor"]
            if not isinstance(backoff_factor, (int, float)) or backoff_factor <= 0:
                error_details.append(create_error_detail(
                    field="backoff_factor",
                    message="backoff_factor must be a positive number",
                    code="invalid_value"
                ))
    
    elif action_type == HealingActionType.RESOURCE_SCALING:
        if "scaling_factor" in action_definition:
            scaling_factor = action_definition["scaling_factor"]
            if not isinstance(scaling_factor, (int, float)) or scaling_factor <= 0:
                error_details.append(create_error_detail(
                    field="scaling_factor",
                    message="scaling_factor must be a positive number",
                    code="invalid_value"
                ))
    
    is_valid = len(error_details) == 0
    if not is_valid:
        logger.debug(f"Healing action validation failed with {len(error_details)} errors")
    
    return is_valid, error_details


# Validation rule classes
class ValidationRule:
    """Base class for validation rules."""
    
    def validate(self, value: Any, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate a value against the rule.
        
        Args:
            value: The value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        # This is a base class method that should be overridden by subclasses
        return True, None


class TypeRule(ValidationRule):
    """Validation rule for checking value type."""
    
    def __init__(self, expected_type: type):
        """
        Initialize the type validation rule.
        
        Args:
            expected_type: The expected type
        """
        super().__init__()
        self.expected_type = expected_type
    
    def validate(self, value: Any, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate that a value is of the expected type.
        
        Args:
            value: The value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        if isinstance(value, self.expected_type):
            return True, None
        
        error_detail = create_error_detail(
            field=field_name,
            message=f"Must be of type {self.expected_type.__name__}, got {type(value).__name__}",
            code="invalid_type"
        )
        return False, error_detail


class RangeRule(ValidationRule):
    """Validation rule for checking numeric range."""
    
    def __init__(self, min_value: Optional[Union[int, float]] = None, max_value: Optional[Union[int, float]] = None):
        """
        Initialize the range validation rule.
        
        Args:
            min_value: Optional minimum allowed value
            max_value: Optional maximum allowed value
        """
        super().__init__()
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value must be provided")
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, value: Union[int, float], field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate that a numeric value is within the specified range.
        
        Args:
            value: The numeric value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        if self.min_value is not None and value < self.min_value:
            error_detail = create_error_detail(
                field=field_name,
                message=f"Must be at least {self.min_value}, got {value}",
                code="value_too_small"
            )
            return False, error_detail
        
        if self.max_value is not None and value > self.max_value:
            error_detail = create_error_detail(
                field=field_name,
                message=f"Must be at most {self.max_value}, got {value}",
                code="value_too_large"
            )
            return False, error_detail
        
        return True, None


class LengthRule(ValidationRule):
    """Validation rule for checking string length."""
    
    def __init__(self, min_length: Optional[int] = None, max_length: Optional[int] = None):
        """
        Initialize the length validation rule.
        
        Args:
            min_length: Optional minimum allowed length
            max_length: Optional maximum allowed length
        """
        super().__init__()
        if min_length is None and max_length is None:
            raise ValueError("At least one of min_length or max_length must be provided")
        self.min_length = min_length
        self.max_length = max_length
    
    def validate(self, value: str, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate that a string's length is within the specified range.
        
        Args:
            value: The string to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        if self.min_length is not None and len(value) < self.min_length:
            error_detail = create_error_detail(
                field=field_name,
                message=f"Length must be at least {self.min_length}, got {len(value)}",
                code="string_too_short"
            )
            return False, error_detail
        
        if self.max_length is not None and len(value) > self.max_length:
            error_detail = create_error_detail(
                field=field_name,
                message=f"Length must be at most {self.max_length}, got {len(value)}",
                code="string_too_long"
            )
            return False, error_detail
        
        return True, None


class PatternRule(ValidationRule):
    """Validation rule for checking string pattern."""
    
    def __init__(self, pattern: Union[str, re.Pattern]):
        """
        Initialize the pattern validation rule.
        
        Args:
            pattern: The regex pattern to match against
        """
        super().__init__()
        self.pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
    
    def validate(self, value: str, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate that a string matches the specified pattern.
        
        Args:
            value: The string to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        if self.pattern.match(value):
            return True, None
        
        error_detail = create_error_detail(
            field=field_name,
            message=f"Must match pattern {self.pattern.pattern}",
            code="pattern_mismatch"
        )
        return False, error_detail


class EnumRule(ValidationRule):
    """Validation rule for checking enum values."""
    
    def __init__(self, enum_class: type):
        """
        Initialize the enum validation rule.
        
        Args:
            enum_class: The enumeration class
        """
        super().__init__()
        self.enum_class = enum_class
    
    def validate(self, value: Any, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate that a value is a valid member of the enumeration.
        
        Args:
            value: The value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        try:
            self.enum_class(value)
            return True, None
        except (ValueError, TypeError):
            valid_values = [e.value for e in self.enum_class]
            error_detail = create_error_detail(
                field=field_name,
                message=f"Must be one of {valid_values}, got {value}",
                code="invalid_enum_value"
            )
            return False, error_detail


class CustomRule(ValidationRule):
    """Validation rule using a custom validation function."""
    
    def __init__(self, validation_func: Callable, error_message: str):
        """
        Initialize the custom validation rule.
        
        Args:
            validation_func: Function that takes a value and returns True/False
            error_message: Error message to use if validation fails
        """
        super().__init__()
        self.validation_func = validation_func
        self.error_message = error_message
    
    def validate(self, value: Any, field_name: str) -> Tuple[bool, Optional[ErrorDetail]]:
        """
        Validate a value using the custom validation function.
        
        Args:
            value: The value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and error detail if any
        """
        if self.validation_func(value):
            return True, None
        
        error_detail = create_error_detail(
            field=field_name,
            message=self.error_message,
            code="custom_validation_error"
        )
        return False, error_detail


class ValidationRuleSet:
    """Collection of validation rules for a field."""
    
    def __init__(self, rules: Optional[List[ValidationRule]] = None):
        """
        Initialize the validation rule set.
        
        Args:
            rules: Optional list of validation rules
        """
        self.rules = rules or []
    
    def add_rule(self, rule: ValidationRule) -> None:
        """
        Add a validation rule to the set.
        
        Args:
            rule: The validation rule to add
        """
        self.rules.append(rule)
    
    def validate(self, value: Any, field_name: str) -> Tuple[bool, List[ErrorDetail]]:
        """
        Validate a value against all rules in the set.
        
        Args:
            value: The value to validate
            field_name: The field name
            
        Returns:
            Tuple containing validation result (True/False) and list of error details
        """
        error_details = []
        
        for rule in self.rules:
            valid, error = rule.validate(value, field_name)
            if not valid and error:
                error_details.append(error)
        
        is_valid = len(error_details) == 0
        return is_valid, error_details