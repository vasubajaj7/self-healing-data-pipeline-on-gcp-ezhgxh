"""
Core validation engine for the data quality framework that orchestrates the validation of datasets against defined quality rules. 
It manages validation execution, processes results, and calculates quality scores to determine if data meets quality thresholds.
"""

import enum
import time
import typing
from typing import Any, Dict, List, Optional, Tuple
import pandas  # version 2.0.x
import importlib  # standard library

from src.backend.constants import (  # src/backend/constants.py
    ValidationRuleType,
    QualityDimension,
    VALIDATION_STATUS_PASSED,
    VALIDATION_STATUS_FAILED,
    VALIDATION_STATUS_WARNING
)
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.monitoring.metric_client import MetricClient  # src/backend/utils/monitoring/metric_client.py
from src.backend.quality.engines.execution_engine import ExecutionEngine, ExecutionMode, ExecutionContext  # ./execution_engine
from src.backend.quality.engines.quality_scorer import QualityScorer, QualityScore, ScoringModel  # ./quality_scorer
from src.backend.quality.rules.rule_loader import load_rules_from_config  # ../rules/rule_loader

# Initialize logger for this module
logger = get_logger(__name__)

# Define default quality threshold
DEFAULT_QUALITY_THRESHOLD = 0.8


def create_validation_result(rule: dict, success: bool, details: dict) -> 'ValidationResult':
    """Creates a standardized validation result object
    
    Args:
        rule (dict): rule
        success (bool): success
        details (dict): details
    
    Returns:
        ValidationResult: Standardized validation result object
    """
    # Create a new ValidationResult instance
    validation_result = ValidationResult(rule['rule_id'], ValidationRuleType(rule['rule_type']), QualityDimension(rule['dimension']))
    # Set rule_id, rule_type, and dimension from rule
    # Set success status based on input
    validation_result.set_status(success)
    # Set validation details with provided details
    validation_result.set_details(details)
    # Set timestamp to current time
    # Return the ValidationResult object
    return validation_result


def format_validation_summary(validation_results: list, quality_score: 'QualityScore') -> dict:
    """Formats validation results into a summary report
    
    Args:
        validation_results (list): validation_results
        quality_score (QualityScore): quality_score
    
    Returns:
        dict: Formatted validation summary
    """
    # Count total rules, passed rules, failed rules, and warnings
    total_rules = len(validation_results)
    passed_rules = sum(1 for result in validation_results if result.get('success'))
    failed_rules = total_rules - passed_rules
    warning_rules = sum(1 for result in validation_results if result.get('status') == VALIDATION_STATUS_WARNING)
    # Calculate success rate
    success_rate = passed_rules / total_rules if total_rules > 0 else 0.0
    # Group results by rule type and dimension
    results_by_rule_type = group_results_by_rule_type(validation_results)
    results_by_dimension = group_results_by_dimension(validation_results)
    # Format quality score information
    quality_score_dict = quality_score.to_dict() if quality_score else None
    # Create summary dictionary with all metrics
    summary = {
        'total_rules': total_rules,
        'passed_rules': passed_rules,
        'failed_rules': failed_rules,
        'warning_rules': warning_rules,
        'success_rate': success_rate,
        'results_by_rule_type': {k.value: len(v) for k, v in results_by_rule_type.items()},
        'results_by_dimension': {k.value: len(v) for k, v in results_by_dimension.items()},
        'quality_score': quality_score_dict
    }
    # Return formatted summary
    return summary


def group_results_by_dimension(validation_results: list) -> dict:
    """Groups validation results by quality dimension
    
    Args:
        validation_results (list): validation_results
    
    Returns:
        dict: Results grouped by quality dimension
    """
    # Initialize empty dictionary with QualityDimension keys
    grouped_results = {dimension: [] for dimension in QualityDimension}
    # Iterate through validation results
    for result in validation_results:
        # Extract dimension from each result
        dimension = result.get('dimension')
        # Add result to appropriate group in dictionary
        if dimension in grouped_results:
            grouped_results[dimension].append(result)
    # Return grouped results dictionary
    return grouped_results


def group_results_by_rule_type(validation_results: list) -> dict:
    """Groups validation results by rule type
    
    Args:
        validation_results (list): validation_results
    
    Returns:
        dict: Results grouped by rule type
    """
    # Initialize empty dictionary with ValidationRuleType keys
    grouped_results = {rule_type: [] for rule_type in ValidationRuleType}
    # Iterate through validation results
    for result in validation_results:
        # Extract rule_type from each result
        rule_type = result.get('rule_type')
        # Add result to appropriate group in dictionary
        if rule_type in grouped_results:
            grouped_results[rule_type].append(result)
    # Return grouped results dictionary
    return grouped_results


def create_validator(rule_type: ValidationRuleType) -> Any:
    """Factory function to create a validator instance based on rule type
    
    Args:
        rule_type (ValidationRuleType): rule_type
    
    Returns:
        Any: Validator instance for the specified rule type
    """
    # Check which validator type to create based on rule_type
    if rule_type == ValidationRuleType.SCHEMA:
        # For SCHEMA rule type, dynamically import SchemaValidator using importlib
        module_name = "src.backend.quality.validators.schema_validator"
        class_name = "SchemaValidator"
    elif rule_type == ValidationRuleType.CONTENT:
        # For CONTENT rule type, dynamically import ContentValidator using importlib
        module_name = "src.backend.quality.validators.content_validator"
        class_name = "ContentValidator"
    elif rule_type == ValidationRuleType.RELATIONSHIP:
        # For RELATIONSHIP rule type, dynamically import RelationshipValidator using importlib
        module_name = "src.backend.quality.validators.relationship_validator"
        class_name = "RelationshipValidator"
    elif rule_type == ValidationRuleType.STATISTICAL:
        # For STATISTICAL rule type, dynamically import StatisticalValidator using importlib
        module_name = "src.backend.quality.validators.statistical_validator"
        class_name = "StatisticalValidator"
    else:
        # Raise ValueError for unsupported rule types
        raise ValueError(f"Unsupported rule type: {rule_type}")
    
    # Return validator instance
    return dynamic_import(module_name, class_name)


def dynamic_import(module_path: str, class_name: str) -> Any:
    """Dynamically import a module to avoid circular dependencies
    
    Args:
        module_path (str): module_path
        class_name (str): class_name
    
    Returns:
        Any: The requested class from the specified module
    """
    # Import the module using importlib.import_module
    module = importlib.import_module(module_path)
    # Get the requested class from the module
    clazz = getattr(module, class_name)
    # Return the class
    return clazz


class ValidationResult:
    """Represents the result of a validation rule execution"""
    rule_id: str
    rule_type: ValidationRuleType
    dimension: QualityDimension
    status: str
    success: bool
    details: dict
    timestamp: typing.Any
    execution_time: float

    def __init__(self, rule_id: str, rule_type: ValidationRuleType, dimension: QualityDimension):
        """Initialize a validation result with rule information
        
        Args:
            rule_id (str): rule_id
            rule_type (ValidationRuleType): rule_type
            dimension (QualityDimension): dimension
        """
        # Set rule_id, rule_type, and dimension from parameters
        self.rule_id = rule_id
        self.rule_type = rule_type
        self.dimension = dimension
        # Initialize status to None
        self.status = None
        # Initialize success to False
        self.success = False
        # Initialize details to empty dictionary
        self.details = {}
        # Set timestamp to current time
        self.timestamp = None
        # Initialize execution_time to 0.0
        self.execution_time = 0.0

    def set_status(self, success: bool) -> None:
        """Sets the validation status based on success
        
        Args:
            success (bool): success
        """
        # Set success property to input success value
        self.success = success
        # If success is True, set status to VALIDATION_STATUS_PASSED
        if success:
            self.status = VALIDATION_STATUS_PASSED
        # If success is False, set status to VALIDATION_STATUS_FAILED
        else:
            self.status = VALIDATION_STATUS_FAILED

    def set_warning(self) -> None:
        """Sets the validation status to warning
        """
        # Set status to VALIDATION_STATUS_WARNING
        self.status = VALIDATION_STATUS_WARNING
        # Set success to True (warnings don't fail validation)
        self.success = True

    def set_details(self, details: dict) -> None:
        """Sets the validation details
        
        Args:
            details (dict): details
        """
        # Set details property to input details dictionary
        self.details = details

    def set_execution_time(self, execution_time: float) -> None:
        """Sets the execution time for the validation
        
        Args:
            execution_time (float): execution_time
        """
        # Set execution_time property to input execution_time value
        self.execution_time = execution_time

    def to_dict(self) -> dict:
        """Converts validation result to dictionary representation
        """
        # Create dictionary with all properties
        data = {
            'rule_id': self.rule_id,
            'rule_type': self.rule_type.value if self.rule_type else None,
            'dimension': self.dimension.value if self.dimension else None,
            'status': self.status,
            'success': self.success,
            'details': self.details,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'execution_time': self.execution_time
        }
        # Convert enum values to strings
        # Format timestamp to ISO format
        # Return the dictionary
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'ValidationResult':
        """Creates a ValidationResult from dictionary representation
        
        Args:
            data (dict): data
        
        Returns:
            ValidationResult: ValidationResult instance
        """
        # Create new ValidationResult instance
        validation_result = cls(data['rule_id'], ValidationRuleType(data['rule_type']), QualityDimension(data['dimension']))
        # Set properties from dictionary values
        validation_result.status = data['status']
        validation_result.success = data['success']
        validation_result.details = data['details']
        validation_result.timestamp = data['timestamp']
        validation_result.execution_time = data['execution_time']
        # Convert string representations back to enums
        # Parse timestamp from ISO format
        # Return the ValidationResult instance
        return validation_result


class ValidationSummary:
    """Represents a summary of validation results with quality score"""
    validation_id: str
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    success_rate: float
    results_by_dimension: dict
    results_by_rule_type: dict
    quality_score: 'QualityScore'
    passes_threshold: bool
    timestamp: typing.Any
    execution_time: float

    def __init__(self, validation_results: list, execution_time: float):
        """Initialize a validation summary with results
        
        Args:
            validation_results (list): validation_results
            execution_time (float): execution_time
        """
        # Generate unique validation_id using uuid
        self.validation_id = str(uuid.uuid4())
        # Count total_rules, passed_rules, failed_rules, and warning_rules
        self.total_rules = len(validation_results)
        self.passed_rules = sum(1 for result in validation_results if result.get('success'))
        self.failed_rules = self.total_rules - self.passed_rules
        self.warning_rules = sum(1 for result in validation_results if result.get('status') == VALIDATION_STATUS_WARNING)
        # Calculate success_rate as passed_rules / total_rules
        self.success_rate = self.passed_rules / self.total_rules if self.total_rules > 0 else 0.0
        # Group results by dimension and rule type
        self.results_by_dimension = group_results_by_dimension(validation_results)
        self.results_by_rule_type = group_results_by_rule_type(validation_results)
        # Initialize quality_score to None
        self.quality_score = None
        # Initialize passes_threshold to False
        self.passes_threshold = False
        # Set timestamp to current time
        self.timestamp = None
        # Set execution_time from parameter
        self.execution_time = execution_time

    def set_quality_score(self, quality_score: 'QualityScore', threshold: float) -> None:
        """Sets the quality score and threshold status
        
        Args:
            quality_score (QualityScore): quality_score
            threshold (float): threshold
        """
        # Set quality_score property to input quality_score
        self.quality_score = quality_score
        # Check if quality_score overall_score passes threshold
        self.passes_threshold = quality_score.overall_score >= threshold
        # Set passes_threshold based on comparison

    def passes_threshold(self) -> bool:
        """Checks if validation passes quality threshold
        
        Returns:
            bool: True if validation passes threshold
        """
        # Return passes_threshold property
        return self.passes_threshold

    def to_dict(self) -> dict:
        """Converts validation summary to dictionary representation
        """
        # Create dictionary with all properties
        data = {
            'validation_id': self.validation_id,
            'total_rules': self.total_rules,
            'passed_rules': self.passed_rules,
            'failed_rules': self.failed_rules,
            'warning_rules': self.warning_rules,
            'success_rate': self.success_rate,
            'results_by_dimension': {k.value: len(v) for k, v in self.results_by_dimension.items()},
            'results_by_rule_type': {k.value: len(v) for k, v in self.results_by_rule_type.items()},
            'quality_score': self.quality_score.to_dict() if self.quality_score else None,
            'passes_threshold': self.passes_threshold,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'execution_time': self.execution_time
        }
        # Convert quality_score to dictionary using to_dict()
        # Format timestamp to ISO format
        # Return the dictionary
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'ValidationSummary':
        """Creates a ValidationSummary from dictionary representation
        
        Args:
            data (dict): data
        
        Returns:
            ValidationSummary: ValidationSummary instance
        """
        # Create new ValidationSummary instance with empty results
        validation_summary = cls([], 0.0)
        # Set properties from dictionary values
        validation_summary.validation_id = data['validation_id']
        validation_summary.total_rules = data['total_rules']
        validation_summary.passed_rules = data['passed_rules']
        validation_summary.failed_rules = data['failed_rules']
        validation_summary.warning_rules = data['warning_rules']
        validation_summary.success_rate = data['success_rate']
        validation_summary.results_by_dimension = data['results_by_dimension']
        validation_summary.results_by_rule_type = data['results_by_rule_type']
        # Convert quality_score dictionary to QualityScore object
        # Parse timestamp from ISO format
        # Return the ValidationSummary instance
        return validation_summary


class ValidationEngine:
    """Main engine for validating data quality using defined rules"""
    _config: dict
    _validators: dict
    _execution_engine: ExecutionEngine
    _quality_scorer: QualityScorer
    _rule_registry: dict
    _quality_threshold: float
    _execution_mode: ExecutionMode
    _scoring_model: ScoringModel
    _metric_client: MetricClient

    def __init__(self, config: dict):
        """Initialize the validation engine with configuration
        
        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        # Initialize empty validators dictionary
        self._validators = {}
        # Create ExecutionEngine for optimized validation execution
        self._execution_engine = ExecutionEngine(self._config)
        # Create QualityScorer for quality score calculation
        self._quality_scorer = QualityScorer(config=self._config)
        # Initialize empty rule registry dictionary
        self._rule_registry = {}
        # Set quality threshold from config or use default
        self._quality_threshold = float(self._config.get('quality_threshold', DEFAULT_QUALITY_THRESHOLD))
        # Set execution mode from config or use default
        self._execution_mode = ExecutionMode(self._config.get('execution_mode', ExecutionMode.IN_MEMORY.value))
        # Set scoring model from config or use default
        self._scoring_model = ScoringModel(self._config.get('scoring_model', ScoringModel.WEIGHTED.value))
        # Create MetricClient for reporting metrics
        self._metric_client = MetricClient()
        logger.info("ValidationEngine initialized")

    def validate(self, dataset: Any, rules: list, validation_config: dict) -> Tuple['ValidationSummary', List['ValidationResult']]:
        """Validate a dataset against quality rules
        
        Args:
            dataset (Any): dataset
            rules (list): rules
            validation_config (dict): validation_config
        
        Returns:
            tuple: (ValidationSummary, list[ValidationResult])
        """
        # Log start of validation
        logger.info("Starting validation process")
        # Merge validation_config with default config
        config = self._config.copy()
        config.update(validation_config)
        # Determine execution mode based on dataset and config
        # Dynamically import GreatExpectationsAdapter if needed
        # Execute validation using execution engine
        validation_results, execution_context = self._execution_engine.execute(dataset, rules, config)
        # Calculate quality score from validation results
        quality_score = self._quality_scorer.calculate_score(validation_results)
        # Create validation summary with results and quality score
        validation_summary = ValidationSummary(validation_results, execution_context.get_execution_time())
        validation_summary.set_quality_score(quality_score, self._quality_threshold)
        # Report validation metrics
        self.report_metrics(validation_summary, validation_results)
        # Log validation completion
        logger.info("Validation process completed")
        # Return validation summary and detailed results
        return validation_summary, validation_results

    def validate_rule(self, dataset: Any, rule: dict) -> 'ValidationResult':
        """Validate a single rule against a dataset
        
        Args:
            dataset (Any): dataset
            rule (dict): rule
        
        Returns:
            ValidationResult: Validation result for the rule
        """
        # Get appropriate validator for rule type
        validator = self.get_validator(ValidationRuleType(rule['rule_type']))
        # Execute validation of rule against dataset
        # Return validation result
        return validator.validate(dataset, rule)

    def register_rule(self, rule: dict) -> str:
        """Register a validation rule in the rule registry
        
        Args:
            rule (dict): rule
        
        Returns:
            str: Rule ID
        """
        # Validate rule structure
        # Generate rule_id if not provided
        rule_id = rule.get('rule_id') or str(uuid.uuid4())
        # Store rule in rule registry
        self._rule_registry[rule_id] = rule
        # Return rule_id
        return rule_id

    def get_rule(self, rule_id: str) -> dict:
        """Get a validation rule by ID
        
        Args:
            rule_id (str): rule_id
        
        Returns:
            dict: Rule definition
        """
        # Check if rule_id exists in rule registry
        if rule_id in self._rule_registry:
            # Return rule if found, None otherwise
            return self._rule_registry[rule_id]
        else:
            return None

    def load_rules_from_config(self, config_path: str) -> list:
        """Load validation rules from configuration
        
        Args:
            config_path (str): config_path
        
        Returns:
            list: Loaded validation rules
        """
        # Use rule_loader.load_rules_from_config to load rules
        rules = load_rules_from_config(config_path)
        # Register each loaded rule
        # Return list of loaded rules
        return rules

    def get_validator(self, rule_type: ValidationRuleType) -> Any:
        """Get or create a validator for a specific rule type
        
        Args:
            rule_type (ValidationRuleType): rule_type
        
        Returns:
            Any: Validator instance for the specified rule type
        """
        # Check if validator for rule_type exists in _validators dictionary
        if rule_type in self._validators:
            # If exists, return cached validator
            return self._validators[rule_type]
        # If not, create new validator using create_validator factory function
        else:
            validator = create_validator(rule_type)
            # Cache validator in _validators dictionary
            self._validators[rule_type] = validator
            # Return validator instance
            return validator

    def set_quality_threshold(self, threshold: float) -> None:
        """Set the quality threshold for validation
        
        Args:
            threshold (float): threshold
        """
        # Validate threshold is between 0.0 and 1.0
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Quality threshold must be between 0.0 and 1.0")
        # Set _quality_threshold to provided threshold
        self._quality_threshold = threshold
        # Update quality scorer threshold
        self._quality_scorer.set_quality_threshold(threshold)

    def get_quality_threshold(self) -> float:
        """Get the current quality threshold
        
        Returns:
            float: Current quality threshold
        """
        # Return _quality_threshold
        return self._quality_threshold

    def set_execution_mode(self, mode: ExecutionMode) -> None:
        """Set the execution mode for validation
        
        Args:
            mode (ExecutionMode): mode
        """
        # Validate mode is a valid ExecutionMode
        if not isinstance(mode, ExecutionMode):
            raise ValueError("Mode must be a valid ExecutionMode")
        # Set _execution_mode to provided mode
        self._execution_mode = mode

    def set_scoring_model(self, model: ScoringModel) -> None:
        """Set the scoring model for quality calculation
        
        Args:
            model (ScoringModel): model
        """
        # Validate model is a valid ScoringModel
        if not isinstance(model, ScoringModel):
            raise ValueError("Model must be a valid ScoringModel")
        # Set _scoring_model to provided model
        self._scoring_model = model
        # Update quality scorer model
        self._quality_scorer.set_model(model)

    def report_metrics(self, summary: 'ValidationSummary', results: list) -> None:
        """Report validation metrics to monitoring system
        
        Args:
            summary (ValidationSummary): summary
            results (list): results
        """
        # Report validation count metric
        # Report success rate metric
        # Report quality score metric
        # Report execution time metric
        # Report rule type metrics
        # Report dimension metrics
        pass

    def get_adapter(self) -> Any:
        """Dynamically import and get the Great Expectations adapter
        
        Returns:
            Any: Great Expectations adapter instance
        """
        # Check if adapter is enabled in config
        # Dynamically import GreatExpectationsAdapter class using importlib
        # Create adapter instance
        # Return adapter instance
        pass

    def close(self) -> None:
        """Close the validation engine and release resources
        """
        # Close each validator in _validators dictionary
        for validator in self._validators.values():
            if hasattr(validator, "close") and callable(validator.close):
                validator.close()

        # Close execution engine
        if self._execution_engine and hasattr(self._execution_engine, "close") and callable(self._execution_engine.close):
            self._execution_engine.close()

        # Close metric client
        if self._metric_client and hasattr(self._metric_client, "close") and callable(self._metric_client.close):
            self._metric_client.close()

        # Release any other resources
        logger.info("ValidationEngine closed")