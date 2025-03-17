"""
Core rule engine for the data quality validation framework that manages the lifecycle of
validation rules, executes them against datasets, and provides a unified interface
for rule management. This module is central to the self-healing data pipeline's
quality validation capabilities.
"""

import typing
import uuid
import datetime
import json

from src.backend.constants import (  # src/backend/constants.py
    ValidationRuleType,
    QualityDimension,
    VALIDATION_STATUS_PASSED,
    VALIDATION_STATUS_FAILED,
    VALIDATION_STATUS_WARNING
)
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.quality.engines.validation_engine import ValidationResult  # ./validation_engine
from src.backend.quality.engines.execution_engine import ExecutionMode  # ./execution_engine
from src.backend.quality.engines.quality_scorer import QualityScorer, ScoringModel  # ./quality_scorer

# Initialize logger
logger = get_logger(__name__)

# Default rule registry
DEFAULT_RULE_REGISTRY = "{}"


def validate_rule_structure(rule_definition: dict) -> typing.Tuple[bool, str]:
    """
    Validates the structure of a rule definition against required fields and types

    Args:
        rule_definition (dict): rule_definition

    Returns:
        tuple: (bool, str) - Validation result and error message if any
    """
    # Check for required fields (name, type, dimension, parameters)
    if not all(key in rule_definition for key in ["name", "type", "dimension", "parameters"]):
        return False, "Rule definition missing required fields"

    # Validate that rule_type is a valid ValidationRuleType
    try:
        ValidationRuleType(rule_definition["type"])
    except ValueError:
        return False, f"Invalid rule type: {rule_definition['type']}"

    # Validate that dimension is a valid QualityDimension
    try:
        QualityDimension(rule_definition["dimension"])
    except ValueError:
        return False, f"Invalid quality dimension: {rule_definition['dimension']}"

    # Validate that parameters are present and properly structured
    if not isinstance(rule_definition["parameters"], dict):
        return False, "Parameters must be a dictionary"

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


class Rule:
    """Represents a data quality validation rule with metadata and execution logic"""
    rule_id: str
    name: str
    description: str
    rule_type: ValidationRuleType
    dimension: QualityDimension
    parameters: dict
    metadata: dict
    created_at: datetime
    updated_at: datetime
    enabled: bool

    def __init__(self, rule_definition: dict):
        """Initialize a rule with the provided definition

        Args:
            rule_definition (dict): rule_definition
        """
        # Extract rule_id from definition or generate new one
        self.rule_id = rule_definition.get("rule_id", generate_rule_id())

        # Extract name, description, rule_type, dimension from definition
        self.name = rule_definition.get("name")
        self.description = rule_definition.get("description", "")
        self.rule_type = ValidationRuleType(rule_definition.get("type"))
        self.dimension = QualityDimension(rule_definition.get("dimension"))
        self.parameters = rule_definition.get("parameters", {})

        # Initialize metadata dictionary
        self.metadata = rule_definition.get("metadata", {})

        # Set created_at and updated_at timestamps
        self.created_at = datetime.datetime.now(datetime.timezone.utc)
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)

        # Set enabled to True by default
        self.enabled = True

    def to_dict(self) -> dict:
        """Convert rule to dictionary representation

        Returns:
            dict: Dictionary representation of rule
        """
        # Create dictionary with all rule properties
        rule_dict = {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "type": self.rule_type.value,
            "dimension": self.dimension.value,
            "parameters": self.parameters,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "enabled": self.enabled,
        }

        # Convert enum values to strings
        # Format timestamps to ISO format
        # Return the dictionary
        return rule_dict

    @classmethod
    def from_dict(cls, data: dict) -> 'Rule':
        """Create a Rule from dictionary representation

        Args:
            data (dict): data

        Returns:
            Rule: Rule instance
        """
        # Create new Rule instance with data
        rule = cls(data)

        # Return the Rule instance
        return rule

    def update(self, updates: dict) -> None:
        """Update rule properties with new values

        Args:
            updates (dict): updates

        Returns:
            None: No return value
        """
        # Update rule properties with values from updates dictionary
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)

        # Update updated_at timestamp
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)

        # Validate updated rule structure
        is_valid, error_message = validate_rule_structure(self.to_dict())
        if not is_valid:
            raise ValueError(f"Invalid rule structure after update: {error_message}")

    def enable(self) -> None:
        """Enable the rule

        Returns:
            None: No return value
        """
        # Set enabled to True
        self.enabled = True

        # Update updated_at timestamp
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)

    def disable(self) -> None:
        """Disable the rule

        Returns:
            None: No return value
        """
        # Set enabled to False
        self.enabled = False

        # Update updated_at timestamp
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)

    def is_enabled(self) -> bool:
        """Check if the rule is enabled

        Returns:
            bool: True if rule is enabled
        """
        # Return enabled property
        return self.enabled


class RuleResult:
    """Represents the result of a rule execution with details and metadata"""
    rule_id: str
    rule_type: ValidationRuleType
    dimension: QualityDimension
    status: str
    success: bool
    details: dict
    timestamp: datetime
    execution_time: float

    def __init__(self, rule: Rule):
        """Initialize a rule result with rule information

        Args:
            rule (Rule): rule
        """
        # Set rule_id, rule_type, and dimension from rule
        self.rule_id = rule.rule_id
        self.rule_type = rule.rule_type
        self.dimension = rule.dimension

        # Initialize status to None
        self.status = None

        # Initialize success to False
        self.success = False

        # Initialize details to empty dictionary
        self.details = {}

        # Set timestamp to current time
        self.timestamp = datetime.datetime.now(datetime.timezone.utc)

        # Initialize execution_time to 0.0
        self.execution_time = 0.0

    def to_dict(self) -> dict:
        """Convert rule result to dictionary representation"""
        # Create dictionary with all properties
        data = {
            "rule_id": self.rule_id,
            "rule_type": self.rule_type.value if self.rule_type else None,
            "dimension": self.dimension.value if self.dimension else None,
            "status": self.status,
            "success": self.success,
            "details": self.details,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "execution_time": self.execution_time,
        }

        # Convert enum values to strings
        # Format timestamp to ISO format
        # Return the dictionary
        return data

    @classmethod
    def from_dict(cls, data: dict) -> 'RuleResult':
        """Create a RuleResult from dictionary representation

        Args:
            data (dict): data

        Returns:
            RuleResult: RuleResult instance
        """
        # Create new RuleResult instance
        rule_result = cls(Rule(data))

        # Set properties from dictionary values
        rule_result.status = data['status']
        rule_result.success = data['success']
        rule_result.details = data['details']
        rule_result.timestamp = data['timestamp']
        rule_result.execution_time = data['execution_time']

        # Convert string representations back to enums
        # Parse timestamp from ISO format
        # Return the RuleResult instance
        return rule_result

    def set_execution_time(self, execution_time: float) -> None:
        """Set the execution time for the rule result

        Args:
            execution_time (float): execution_time
        """
        # Set execution_time property to input execution_time value
        self.execution_time = execution_time

    def set_status(self, success: bool) -> None:
        """Set the validation status based on success

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
        """Set the validation status to warning"""
        # Set status to VALIDATION_STATUS_WARNING
        self.status = VALIDATION_STATUS_WARNING

        # Set success to True (warnings don't fail validation)
        self.success = True

    def set_details(self, details: dict) -> None:
        """Set the validation details

        Args:
            details (dict): details
        """
        # Set details property to input details dictionary
        self.details = details


class RuleEngine:
    """Main engine for managing and executing data quality validation rules"""
    _config: dict
    _rule_registry: dict
    _validators: dict
    _quality_scorer: 'QualityScorer'

    def __init__(self, config: dict):
        """Initialize the rule engine with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config

        # Initialize empty rule registry dictionary
        self._rule_registry = {}

        # Initialize empty validators dictionary
        self._validators = {}

        # Create QualityScorer for quality score calculation
        self._quality_scorer = QualityScorer(config=config)

        # Initialize engine properties
        logger.info("RuleEngine initialized")

    def register_rule(self, rule_definition: dict) -> typing.Tuple[bool, str]:
        """Register a validation rule in the rule registry

        Args:
            rule_definition (dict): rule_definition

        Returns:
            tuple: (bool, str) - Success status and rule ID or error message
        """
        # Validate rule structure using validate_rule_structure
        is_valid, error_message = validate_rule_structure(rule_definition)
        if not is_valid:
            return False, error_message

        # Create Rule instance from definition
        rule = Rule(rule_definition)

        # Generate rule_id if not provided
        rule_id = rule.rule_id

        # Store rule in rule registry
        self._rule_registry[rule_id] = rule

        # Return success status and rule_id
        return True, rule_id

    def update_rule(self, rule_id: str, updates: dict) -> typing.Tuple[bool, str]:
        """Update an existing validation rule

        Args:
            rule_id (str): rule_id
            updates (dict): updates

        Returns:
            tuple: (bool, str) - Success status and message
        """
        # Check if rule exists in rule registry
        if rule_id not in self._rule_registry:
            return False, f"Rule with ID '{rule_id}' not found"

        # Get existing rule from rule registry
        rule = self._rule_registry[rule_id]

        # Update rule with provided updates
        try:
            rule.update(updates)
        except ValueError as e:
            return False, str(e)

        # Validate updated rule structure
        is_valid, error_message = validate_rule_structure(rule.to_dict())
        if not is_valid:
            return False, error_message

        # Return success status and message
        return True, "Rule updated successfully"

    def delete_rule(self, rule_id: str) -> bool:
        """Delete a validation rule from the registry

        Args:
            rule_id (str): rule_id

        Returns:
            bool: True if deletion was successful
        """
        # Check if rule exists in rule registry
        if rule_id not in self._rule_registry:
            return False

        # Remove rule from rule registry
        del self._rule_registry[rule_id]

        # Return success status
        return True

    def get_rule(self, rule_id: str) -> typing.Optional[Rule]:
        """Get a validation rule by ID

        Args:
            rule_id (str): rule_id

        Returns:
            Rule: Rule instance or None if not found
        """
        # Check if rule_id exists in rule registry
        if rule_id in self._rule_registry:
            # Return rule if found, None otherwise
            return self._rule_registry[rule_id]
        else:
            return None

    def list_rules(
        self,
        rule_type: ValidationRuleType = None,
        dimension: QualityDimension = None,
        target_dataset: str = None,
        target_table: str = None
    ) -> typing.List[Rule]:
        """List all validation rules, optionally filtered by type or dimension

        Args:
            rule_type (ValidationRuleType): rule_type
            dimension (QualityDimension): dimension
            target_dataset (str): target_dataset
            target_table (str): target_table

        Returns:
            list: List of Rule instances matching the filters
        """
        # Initialize empty result list
        result = []

        # Iterate through rules in registry
        for rule_id, rule in self._rule_registry.items():
            # Apply filters if provided (rule_type, dimension, target_dataset, target_table)
            if rule_type and rule.rule_type != rule_type:
                continue
            if dimension and rule.dimension != dimension:
                continue
            # Add matching rules to result list
            result.append(rule)

        # Return filtered list of rules
        return result

    def execute_rule(self, dataset: typing.Any, rule_id: str) -> RuleResult:
        """Execute a single validation rule against a dataset

        Args:
            dataset (Any): dataset
            rule_id (str): rule_id

        Returns:
            RuleResult: Result of rule execution
        """
        # Get rule by ID from registry
        rule = self.get_rule(rule_id)
        if not rule:
            raise ValueError(f"Rule with ID '{rule_id}' not found")

        # Check if rule is enabled
        if not rule.is_enabled():
            raise ValueError(f"Rule with ID '{rule_id}' is disabled")

        # Create RuleResult instance for the rule
        rule_result = RuleResult(rule)

        # Get appropriate validator for rule type
        validator = self.get_validator(rule.rule_type)

        # Execute validation of rule against dataset
        start_time = datetime.datetime.now(datetime.timezone.utc)
        success, details = validator.validate(dataset, rule.to_dict())
        end_time = datetime.datetime.now(datetime.timezone.utc)

        # Record execution time and result details
        execution_time = (end_time - start_time).total_seconds()
        rule_result.set_execution_time(execution_time)
        rule_result.set_status(success)
        rule_result.set_details(details)

        # Return rule result
        return rule_result

    def execute_rules(self, dataset: typing.Any, rule_ids: list) -> list:
        """Execute multiple validation rules against a dataset

        Args:
            dataset (Any): dataset
            rule_ids (list): rule_ids

        Returns:
            list: List of rule execution results
        """
        # Initialize empty results list
        results = []

        # For each rule_id, execute rule against dataset
        for rule_id in rule_ids:
            result = self.execute_rule(dataset, rule_id)
            # Collect execution results
            results.append(result.to_dict())

        # Return list of rule results
        return results

    def validate_dataset(self, dataset: typing.Any, target_dataset: str, target_table: str, execution_config: dict) -> typing.Tuple[float, list]:
        """Validate a dataset against all applicable rules

        Args:
            dataset (Any): dataset
            target_dataset (str): target_dataset
            target_table (str): target_table
            execution_config (dict): execution_config

        Returns:
            tuple: (float, list) - Quality score and list of rule results
        """
        # Find applicable rules for dataset/table
        applicable_rules = self.list_rules(target_dataset=target_dataset, target_table=target_table)

        # Execute all applicable rules against dataset
        rule_ids = [rule.rule_id for rule in applicable_rules]
        rule_results = self.execute_rules(dataset, rule_ids)

        # Calculate quality score from rule results
        quality_score = self.calculate_quality_score(rule_results)

        # Return quality score and detailed results
        return quality_score, rule_results

    def calculate_quality_score(self, rule_results: list) -> float:
        """Calculate quality score from rule execution results

        Args:
            rule_results (list): rule_results

        Returns:
            float: Quality score between 0.0 and 1.0
        """
        # Use quality scorer to calculate score from results
        score = self._quality_scorer.calculate_score(rule_results)

        # Return calculated quality score
        return score

    def get_validator(self, rule_type: ValidationRuleType) -> typing.Any:
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

        # If not, create new validator based on rule_type
        else:
            # Create validator instance
            # Return validator instance
            return None

    def import_rules(self, source: typing.Union[str, dict, list]) -> typing.Tuple[int, list]:
        """Import rules from a JSON file or dictionary

        Args:
            source (Union[str, dict, list]): source

        Returns:
            tuple: (int, list) - Number of rules imported and list of errors
        """
        # If source is string, load JSON from file
        if isinstance(source, str):
            try:
                with open(source, 'r') as f:
                    rules_data = json.load(f)
            except Exception as e:
                return 0, [str(e)]
        elif isinstance(source, dict):
            rules_data = source.get('rules', [])
        elif isinstance(source, list):
            rules_data = source
        else:
            return 0, ["Invalid source type"]

        success_count = 0
        errors = []

        # Validate and register each rule
        for rule_data in rules_data:
            is_valid, message = validate_rule_structure(rule_data)
            if is_valid:
                try:
                    self.register_rule(rule_data)
                    success_count += 1
                except Exception as e:
                    errors.append(f"Error registering rule: {str(e)}")
            else:
                errors.append(f"Invalid rule structure: {message}")

        # Track successful imports and errors
        # Return count of imported rules and list of errors
        return success_count, errors

    def export_rules(self, file_path: str = None, rule_ids: list = None) -> typing.Union[bool, dict]:
        """Export rules to a JSON file or return as dictionary

        Args:
            file_path (str): file_path
            rule_ids (list): rule_ids

        Returns:
            Union[bool, dict]: Success status or rules dictionary
        """
        # Get rules to export (all or specified by rule_ids)
        rules_to_export = []
        if rule_ids:
            for rule_id in rule_ids:
                rule = self.get_rule(rule_id)
                if rule:
                    rules_to_export.append(rule)
        else:
            rules_to_export = list(self._rule_registry.values())

        # Convert rules to dictionary representations
        rules_data = [rule.to_dict() for rule in rules_to_export]

        # If file_path provided, write to JSON file
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    json.dump(rules_data, f, indent=4)
                return True
            except Exception as e:
                logger.error(f"Error exporting rules to file: {e}")
                return False
        # If no file_path, return rules dictionary
        else:
            return rules_data