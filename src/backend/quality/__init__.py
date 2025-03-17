"""
Initialization file for the quality module that exports key components for data quality validation, including validation engines, rule management, expectations handling, and reporting capabilities. This module serves as the main entry point for the data quality validation framework in the self-healing data pipeline.
"""

from typing import Dict, Any

from .engines import (  # src/backend/quality/engines/__init__.py
    ExecutionEngine,
    ExecutionMode,
    ExecutionContext,
    ValidationEngine,
    ValidationResult,
    ValidationSummary,
    QualityScorer,
    ScoringModel,
)
from .validators import (  # src/backend/quality/validators/__init__.py
    SchemaValidator,
    ContentValidator,
    RelationshipValidator,
    StatisticalValidator,
    get_validator_for_rule_type,
)
from .expectations import (  # src/backend/quality/expectations/__init__.py
    ExpectationManager,
    ExpectationSuiteBuilder,
    register_custom_expectations,
)
from .rules import (  # src/backend/quality/rules/__init__.py
    Rule,
    RuleResult,
    RuleEngine,
    RuleEditor,
)
from .reporters import (  # src/backend/quality/reporters/__init__.py
    QualityIssue,
    IssueDetector,
    QualityReporter,
    ReportGenerator,
)
from .integrations import (  # src/backend/quality/integrations/__init__.py
    BigQueryAdapter,
    GreatExpectationsAdapter,
    MetadataIntegrator,
)
from ..constants import ValidationRuleType  # src/backend/constants.py
from ..utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py

# Configure logging for quality module
logger = get_logger(__name__)

__version__ = "1.0.0"

__all__ = [
    "ValidationEngine",
    "ExecutionEngine",
    "ExecutionMode",
    "ValidationResult",
    "ValidationSummary",
    "QualityScorer",
    "ScoringModel",
    "SchemaValidator",
    "ContentValidator",
    "RelationshipValidator",
    "StatisticalValidator",
    "Rule",
    "RuleResult",
    "RuleEngine",
    "RuleEditor",
    "ExpectationManager",
    "ExpectationSuiteBuilder",
    "QualityIssue",
    "IssueDetector",
    "QualityReporter",
    "ReportGenerator",
    "BigQueryAdapter",
    "GreatExpectationsAdapter",
    "MetadataIntegrator",
    "ValidationRuleType",
    "get_validator_for_rule_type",
    "register_custom_expectations",
    "initialize_quality_framework",
    "create_validation_engine"
]


def initialize_quality_framework(config: Dict) -> "ValidationEngine":
    """Initializes the quality validation framework with the provided configuration

    Args:
        config (dict): config

    Returns:
        ValidationEngine: Initialized validation engine instance
    """
    logger.info("Initializing quality framework")

    # Register custom expectations with Great Expectations
    register_custom_expectations()

    # Create and configure ValidationEngine instance
    validation_engine = create_validation_engine(config)

    logger.info("Quality framework initialized successfully")
    return validation_engine


def create_validation_engine(config: Dict) -> "ValidationEngine":
    """Factory function to create and configure a validation engine instance

    Args:
        config (dict): config

    Returns:
        ValidationEngine: Configured validation engine instance
    """
    # Create new ValidationEngine instance
    validation_engine = ValidationEngine(config)

    logger.info("Validation engine created successfully")
    return validation_engine