"""
Initializes the validators package for the data quality validation framework, exposing the validator classes and utility functions for schema, content, relationship, and statistical validation. This module serves as the entry point for all validation capabilities in the self-healing data pipeline.
"""

import typing  # standard library

from .schema_validator import SchemaValidator  # src/backend/quality/validators/schema_validator.py
from .content_validator import ContentValidator  # src/backend/quality/validators/content_validator.py
from .relationship_validator import RelationshipValidator  # src/backend/quality/validators/relationship_validator.py
from .statistical_validator import StatisticalValidator  # src/backend/quality/validators/statistical_validator.py
from ...constants import ValidationRuleType  # src/backend/constants.py
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py

# Configure logging for validators package
logger = get_logger(__name__)

__all__ = ["SchemaValidator", "ContentValidator", "RelationshipValidator", "StatisticalValidator", "get_validator_for_rule_type"]


def get_validator_for_rule_type(rule_type: ValidationRuleType, config: dict) -> object:
    """Factory function that returns the appropriate validator instance for a given rule type

    Args:
        rule_type (ValidationRuleType): The type of validation rule
        config (dict): Configuration dictionary for the validator

    Returns:
        object: Appropriate validator instance for the rule type
    """
    # Check the rule_type parameter against ValidationRuleType enum
    if rule_type == ValidationRuleType.SCHEMA:
        # For ValidationRuleType.SCHEMA, return SchemaValidator instance
        return SchemaValidator(config)
    elif rule_type == ValidationRuleType.CONTENT:
        # For ValidationRuleType.CONTENT, return ContentValidator instance
        return ContentValidator(config)
    elif rule_type == ValidationRuleType.RELATIONSHIP:
        # For ValidationRuleType.RELATIONSHIP, return RelationshipValidator instance
        return RelationshipValidator(config)
    elif rule_type == ValidationRuleType.STATISTICAL:
        # For ValidationRuleType.STATISTICAL, return StatisticalValidator instance
        return StatisticalValidator(config)
    else:
        # If rule_type is not recognized, raise ValueError with appropriate message
        raise ValueError(f"Unsupported rule type: {rule_type}")
    # Pass the config parameter to the validator constructor