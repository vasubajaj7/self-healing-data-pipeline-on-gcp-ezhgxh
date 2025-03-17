"""
Builds and manages Great Expectations suites from validation rules, providing a bridge between the data quality framework and Great Expectations. This module enables the creation, modification, and management of expectation suites based on validation rules defined in the system.
"""

import typing
import os
import json
import uuid

# great-expectations version: 0.15.x
import great_expectations as ge
# great-expectations version: 0.15.x
from great_expectations.core import ExpectationSuite
# great-expectations version: 0.15.x
from great_expectations.data_context import DataContext

# Internal imports
from src.backend.constants import ValidationRuleType, QualityDimension, DEFAULT_TIMEOUT_SECONDS  # src/backend/constants.py
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.quality.expectations.expectation_manager import ExpectationManager, map_rule_to_expectation  # ./expectation_manager
from src.backend.quality.expectations.custom_expectations import register_custom_expectations  # ./custom_expectations
from src.backend.quality.rules.rule_engine import Rule, validate_rule_structure  # ../rules/rule_engine

# Initialize logger
logger = get_logger(__name__)

# Default suite name
DEFAULT_SUITE_NAME = "default_suite"

# Default expectation timeout
DEFAULT_EXPECTATION_TIMEOUT = DEFAULT_TIMEOUT_SECONDS


def generate_suite_name(prefix: str) -> str:
    """Generates a unique name for an expectation suite

    Args:
        prefix (str): prefix

    Returns:
        str: Unique suite name
    """
    # Generate a UUID
    suite_uuid = uuid.uuid4()
    # Format suite name with prefix and UUID
    suite_name = f"{prefix}_{suite_uuid}"
    # Return formatted suite name
    return suite_name


def validate_suite_name(suite_name: str) -> bool:
    """Validates that a suite name follows Great Expectations naming conventions

    Args:
        suite_name (str): suite_name

    Returns:
        bool: True if name is valid
    """
    # Check if suite_name is a valid string
    if not isinstance(suite_name, str):
        return False

    # Validate that suite_name follows Great Expectations naming conventions
    if not suite_name.isidentifier():
        return False

    # Return validation result
    return True


def load_suite_from_json(file_path: str, context: DataContext) -> ExpectationSuite:
    """Loads an expectation suite from a JSON file

    Args:
        file_path (str): file_path
        context (great_expectations.data_context.DataContext): context

    Returns:
        great_expectations.core.ExpectationSuite: Loaded expectation suite
    """
    # Validate file path exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Expectation suite file not found: {file_path}")

    # Read JSON from file
    with open(file_path, 'r') as f:
        suite_json = json.load(f)

    # Create expectation suite from JSON
    suite = ExpectationSuite(**suite_json)

    # Return loaded suite
    return suite


def save_suite_to_json(suite: ExpectationSuite, file_path: str) -> bool:
    """Saves an expectation suite to a JSON file

    Args:
        suite (great_expectations.core.ExpectationSuite): suite
        file_path (str): file_path

    Returns:
        bool: True if save was successful
    """
    try:
        # Convert suite to JSON
        suite_json = suite.to_json_dict()

        # Write JSON to file
        with open(file_path, 'w') as f:
            json.dump(suite_json, f, indent=2)

        # Return success status
        return True
    except Exception as e:
        logger.error(f"Error saving expectation suite to JSON: {e}", exc_info=True)
        return False


class ExpectationSuiteBuilder:
    """Builds and manages Great Expectations suites from validation rules"""

    def __init__(self, config: dict):
        """Initialize the ExpectationSuiteBuilder with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config
        # Set _initialized to False
        self._initialized = False
        # Initialize empty _suite_registry dictionary
        self._suite_registry: typing.Dict[str, ExpectationSuite] = {}
        # Create ExpectationManager instance with config
        self._expectation_manager: ExpectationManager = ExpectationManager(config=self._config)

    def initialize(self) -> bool:
        """Initialize the builder and ensure expectation manager is ready

        Returns:
            bool: True if initialization was successful
        """
        # If already initialized, return True
        if self._initialized:
            return True

        # Initialize expectation manager
        if not self._expectation_manager.initialize():
            logger.error("Failed to initialize ExpectationManager")
            return False

        # Register custom expectations
        register_custom_expectations()

        # Set _initialized to True
        self._initialized = True

        # Return initialization status
        return self._initialized

    def create_suite(self, suite_name: str, overwrite_existing: bool) -> str:
        """Creates a new expectation suite with optional name

        Args:
            suite_name (str): suite_name
            overwrite_existing (bool): overwrite_existing

        Returns:
            str: Name of the created suite
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # If suite_name not provided, generate one
        if not suite_name:
            suite_name = generate_suite_name(DEFAULT_SUITE_NAME)

        # Validate suite name
        if not validate_suite_name(suite_name):
            raise ValueError(f"Invalid suite name: {suite_name}")

        # Create suite using expectation manager
        suite = self._expectation_manager.create_suite(suite_name, overwrite_existing)

        # Register suite in _suite_registry
        self._suite_registry[suite_name] = suite

        # Return suite name
        return suite_name

    def build_suite_from_rules(self, rules: list, suite_name: str, overwrite_existing: bool) -> tuple[str, int]:
        """Builds an expectation suite from a list of validation rules

        Args:
            rules (list): rules
            suite_name (str): suite_name
            overwrite_existing (bool): overwrite_existing

        Returns:
            tuple: (str, int) - Suite name and number of expectations added
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # Create or get suite with provided name
        suite = self.create_suite(suite_name, overwrite_existing)

        # Convert each rule to expectation
        expectations = []
        for rule in rules:
            expectation = map_rule_to_expectation(rule)
            expectations.append(expectation)

        # Add expectations to suite
        for expectation in expectations:
            suite.add_expectation(expectation)

        # Return suite name and count of added expectations
        return suite_name, len(expectations)

    def add_rule_to_suite(self, rule: dict, suite_name: str) -> bool:
        """Adds a single validation rule to an expectation suite

        Args:
            rule (dict): rule
            suite_name (str): suite_name

        Returns:
            bool: True if rule was added successfully
        """
        # Ensure builder is initialized
        if not self.initialize():
            return False

        # Validate rule structure
        is_valid, error_message = validate_rule_structure(rule)
        if not is_valid:
            logger.error(f"Invalid rule structure: {error_message}")
            return False

        try:
            # Convert rule to expectation
            expectation = map_rule_to_expectation(rule)

            # Add expectation to suite
            suite = self.get_suite(suite_name)
            if suite:
                suite.add_expectation(expectation)
                self._suite_registry[suite_name] = suite
                return True
            else:
                logger.error(f"Suite {suite_name} not found")
                return False
        except Exception as e:
            logger.error(f"Error adding rule to suite: {e}", exc_info=True)
            return False

    def add_rules_to_suite(self, rules: list, suite_name: str) -> int:
        """Adds multiple validation rules to an expectation suite

        Args:
            rules (list): rules
            suite_name (str): suite_name

        Returns:
            int: Number of rules added successfully
        """
        # Ensure builder is initialized
        if not self.initialize():
            return 0

        success_count = 0
        for rule in rules:
            if self.add_rule_to_suite(rule, suite_name):
                success_count += 1

        return success_count

    def get_suite(self, suite_name: str) -> ExpectationSuite:
        """Gets an expectation suite by name

        Args:
            suite_name (str): suite_name

        Returns:
            great_expectations.core.ExpectationSuite: Expectation suite or None if not found
        """
        # Ensure builder is initialized
        if not self.initialize():
            return None

        # Get suite from expectation manager
        suite = self._expectation_manager.get_suite(suite_name)

        # Return suite or None if not found
        return suite

    def delete_suite(self, suite_name: str) -> bool:
        """Deletes an expectation suite

        Args:
            suite_name (str): suite_name

        Returns:
            bool: True if deletion was successful
        """
        # Ensure builder is initialized
        if not self.initialize():
            return False

        # Delete suite using expectation manager
        success = self._expectation_manager.delete_suite(suite_name)

        # Remove suite from _suite_registry if present
        if suite_name in self._suite_registry:
            del self._suite_registry[suite_name]

        # Return success status
        return success

    def list_suites(self) -> list:
        """Lists all available expectation suites

        Returns:
            list: List of suite names
        """
        # Ensure builder is initialized
        if not self.initialize():
            return []

        # Get list of suite names from expectation manager
        suite_names = self._expectation_manager.list_suites()

        # Return list of suite names
        return suite_names

    def export_suite(self, suite_name: str, file_path: str) -> bool:
        """Exports an expectation suite to a JSON file

        Args:
            suite_name (str): suite_name
            file_path (str): file_path

        Returns:
            bool: True if export was successful
        """
        # Ensure builder is initialized
        if not self.initialize():
            return False

        # Get suite by name
        suite = self.get_suite(suite_name)
        if not suite:
            logger.error(f"Suite {suite_name} not found")
            return False

        # Export suite to JSON file
        success = self._expectation_manager.export_suite_to_json(suite_name, file_path)

        # Return success status
        return success

    def import_suite(self, file_path: str, suite_name: str) -> str:
        """Imports an expectation suite from a JSON file

        Args:
            file_path (str): file_path
            suite_name (str): suite_name

        Returns:
            str: Name of the imported suite
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # Load suite from JSON file
        suite = self._expectation_manager.import_suite_from_json(file_path, suite_name)
        if not suite:
            raise Exception(f"Failed to import suite from {file_path}")

        # Save suite with provided name
        self._suite_registry[suite_name] = suite

        # Return suite name
        return suite_name

    def clone_suite(self, source_suite_name: str, target_suite_name: str) -> str:
        """Creates a clone of an existing expectation suite

        Args:
            source_suite_name (str): source_suite_name
            target_suite_name (str): target_suite_name

        Returns:
            str: Name of the cloned suite
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # Get source suite by name
        source_suite = self.get_suite(source_suite_name)
        if not source_suite:
            raise ValueError(f"Source suite {source_suite_name} not found")

        # Create new suite with target name
        target_suite = self.create_suite(target_suite_name, overwrite_existing=True)

        # Copy expectations from source to target
        for expectation in source_suite.expectations:
            target_suite.add_expectation(expectation)

        # Save target suite
        self._expectation_manager.save_suite(target_suite)

        # Return target suite name
        return target_suite_name

    def merge_suites(self, source_suite_names: list, target_suite_name: str) -> tuple[str, int]:
        """Merges multiple expectation suites into a new suite

        Args:
            source_suite_names (list): source_suite_names
            target_suite_name (str): target_suite_name

        Returns:
            tuple: (str, int) - Target suite name and number of expectations
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # Create new suite with target name
        target_suite = self.create_suite(target_suite_name, overwrite_existing=True)
        expectation_count = 0

        # For each source suite, get expectations
        for source_suite_name in source_suite_names:
            source_suite = self.get_suite(source_suite_name)
            if not source_suite:
                raise ValueError(f"Source suite {source_suite_name} not found")

            # Add all expectations to target suite
            for expectation in source_suite.expectations:
                target_suite.add_expectation(expectation)
                expectation_count += 1

        # Save target suite
        self._expectation_manager.save_suite(target_suite)

        # Return target suite name and expectation count
        return target_suite_name, expectation_count

    def get_expectation_manager(self) -> ExpectationManager:
        """Gets the underlying expectation manager

        Returns:
            ExpectationManager: Expectation manager instance
        """
        # Ensure builder is initialized
        if not self.initialize():
            raise Exception("ExpectationSuiteBuilder not initialized")

        # Return _expectation_manager
        return self._expectation_manager

    def close(self) -> None:
        """Closes the builder and releases resources"""
        # Close expectation manager
        self._expectation_manager.close()

        # Clear _suite_registry
        self._suite_registry.clear()

        # Set _initialized to False
        self._initialized = False