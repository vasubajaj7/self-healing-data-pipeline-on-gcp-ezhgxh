"""
Initializes the expectations module for the data quality framework, exposing key classes and functions for managing Great Expectations suites, custom expectations, and validation operations. This module serves as the entry point for the expectations functionality within the self-healing data pipeline.
"""

from .expectation_manager import (  # src/backend/quality/expectations/expectation_manager.py
    ExpectationManager,
    map_rule_to_expectation,
    map_expectation_to_rule,
    create_data_context,
    validate_with_expectation_suite,
    validate_with_expectations
)
from .custom_expectations import (  # src/backend/quality/expectations/custom_expectations.py
    register_custom_expectations,
    ExpectColumnValuesInReferenceTable,
    ExpectColumnValuesTrendIncreasing,
    ExpectColumnValuesTrendDecreasing,
    ExpectColumnValuesSeasonalPattern,
    ExpectColumnValuesAnomalyScore,
    ExpectTableRowCountToBeBetweenDates
)
from .expectation_suite_builder import (  # src/backend/quality/expectations/expectation_suite_builder.py
    ExpectationSuiteBuilder,
    generate_suite_name,
    validate_suite_name,
    load_suite_from_json,
    save_suite_to_json
)

__all__ = [
    "ExpectationManager",
    "ExpectationSuiteBuilder",
    "register_custom_expectations",
    "map_rule_to_expectation",
    "map_expectation_to_rule",
    "create_data_context",
    "validate_with_expectation_suite",
    "validate_with_expectations",
    "generate_suite_name",
    "validate_suite_name",
    "load_suite_from_json",
    "save_suite_to_json",
    "ExpectColumnValuesInReferenceTable",
    "ExpectColumnValuesTrendIncreasing",
    "ExpectColumnValuesTrendDecreasing",
    "ExpectColumnValuesSeasonalPattern",
    "ExpectColumnValuesAnomalyScore",
    "ExpectTableRowCountToBeBetweenDates"
]