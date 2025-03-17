"""
Initialization file for the API utilities module in the self-healing data pipeline.
Imports and exposes key utility functions and classes from the auth, pagination, response, and validation utility modules
to provide a unified interface for API-related utilities.
"""

# Authentication utilities
from .auth_utils import (
    oauth2_scheme,
    verify_password,
    get_password_hash,
    create_access_token,
    verify_token,
    get_current_user,
    get_current_active_user,
    authenticate_user,
    has_permission,
    require_permission,
    get_service_token,
    get_oauth_token,
    UserManager,
    PermissionChecker
)

# Pagination utilities
from .pagination_utils import (
    create_pagination_metadata,
    get_pagination_links,
    calculate_pagination,
    apply_pagination,
    get_pagination_params
)

# Response formatting utilities
from .response_utils import (
    create_response_metadata,
    create_success_response,
    create_list_response,
    create_error_response,
    create_validation_error_response,
    handle_exception,
    error_to_http_exception,
    get_status_code_for_error
)

# Validation utilities
from .validation_utils import (
    validate_email,
    validate_url,
    validate_uuid,
    validate_date_format,
    validate_json_schema,
    validate_required_fields,
    validate_field_type,
    validate_numeric_range,
    validate_string_length,
    validate_enum_value,
    validate_pattern,
    validate_data,
    handle_pydantic_validation_error,
    handle_request_validation_error,
    create_error_detail,
    validate_model,
    validate_source_connection,
    validate_pipeline_configuration,
    validate_quality_rule_definition,
    validate_healing_action_definition,
    ValidationRule,
    TypeRule,
    RangeRule,
    LengthRule,
    PatternRule,
    EnumRule,
    CustomRule,
    ValidationRuleSet
)

__all__ = [
    "oauth2_scheme",
    "verify_password",
    "get_password_hash",
    "create_access_token",
    "verify_token",
    "get_current_user",
    "get_current_active_user",
    "authenticate_user",
    "has_permission",
    "require_permission",
    "get_service_token",
    "get_oauth_token",
    "UserManager",
    "PermissionChecker",
    "create_pagination_metadata",
    "get_pagination_links",
    "calculate_pagination",
    "apply_pagination",
    "get_pagination_params",
    "create_response_metadata",
    "create_success_response",
    "create_list_response",
    "create_error_response",
    "create_validation_error_response",
    "handle_exception",
    "error_to_http_exception",
    "get_status_code_for_error",
    "validate_email",
    "validate_url",
    "validate_uuid",
    "validate_date_format",
    "validate_json_schema",
    "validate_required_fields",
    "validate_field_type",
    "validate_numeric_range",
    "validate_string_length",
    "validate_enum_value",
    "validate_pattern",
    "validate_data",
    "handle_pydantic_validation_error",
    "handle_request_validation_error",
    "create_error_detail",
    "validate_model",
    "validate_source_connection",
    "validate_pipeline_configuration",
    "validate_quality_rule_definition",
    "validate_healing_action_definition",
    "ValidationRule",
    "TypeRule",
    "RangeRule",
    "LengthRule",
    "PatternRule",
    "EnumRule",
    "CustomRule",
    "ValidationRuleSet"
]