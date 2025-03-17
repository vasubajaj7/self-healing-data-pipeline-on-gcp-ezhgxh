"""Adapter for integrating Great Expectations with the data quality validation framework.

Provides a bridge between the pipeline's validation engine and Great Expectations library,
enabling advanced data quality validation capabilities while abstracting the complexity
of Great Expectations configuration and execution.
"""

import typing
import os
import time
import tempfile
import pandas  # version 2.0.x
import great_expectations  # version 0.15.x
from great_expectations.core import ExpectationSuite  # version 0.15.x
from great_expectations.data_context import DataContext  # version 0.15.x
from great_expectations.dataset import Dataset  # version 0.15.x
from great_expectations.validator import Validator  # version 0.15.x

from src.backend.constants import (  # src/backend/constants.py
    ValidationRuleType,
    QualityDimension,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_MAX_RETRY_ATTEMPTS
)
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py
from src.backend.quality.engines.validation_engine import ValidationResult, create_validation_result  # ./validation_engine
from src.backend.quality.engines.execution_engine import ExecutionContext, ExecutionMode  # ./execution_engine
from src.backend.quality.expectations.expectation_manager import ExpectationManager, map_rule_to_expectation, map_expectation_to_rule  # ../expectations/expectation_manager
from src.backend.quality.expectations.custom_expectations import register_custom_expectations  # ../expectations/custom_expectations

# Initialize logger
logger = get_logger(__name__)

# Default timeout for Great Expectations operations
DEFAULT_GE_TIMEOUT = DEFAULT_TIMEOUT_SECONDS

# Default context root directory for Great Expectations
DEFAULT_CONTEXT_ROOT = os.path.join(tempfile.gettempdir(), 'great_expectations')


def convert_pandas_to_ge_dataset(dataframe: pandas.DataFrame) -> 'great_expectations.dataset.PandasDataset':
    """Converts a pandas DataFrame to a Great Expectations dataset

    Args:
        dataframe (pandas.DataFrame): dataframe

    Returns:
        great_expectations.dataset.PandasDataset: Great Expectations dataset
    """
    # Import PandasDataset from great_expectations.dataset
    from great_expectations.dataset import PandasDataset

    # Convert pandas DataFrame to Great Expectations PandasDataset
    ge_dataset = PandasDataset(dataframe)

    # Return the converted dataset
    return ge_dataset


def convert_bigquery_to_ge_dataset(project_id: str, dataset_id: str, table_id: str) -> 'great_expectations.dataset.SqlAlchemyDataset':
    """Converts a BigQuery table reference to a Great Expectations dataset

    Args:
        project_id (str): project_id
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        great_expectations.dataset.SqlAlchemyDataset: Great Expectations dataset
    """
    # Import SqlAlchemyDataset from great_expectations.dataset
    from great_expectations.dataset import SqlAlchemyDataset

    # Create BigQuery connection string
    connection_string = f"bigquery://{project_id}/{dataset_id}"

    # Create SqlAlchemyDataset with BigQuery connection
    ge_dataset = SqlAlchemyDataset(connection_string, table_name=table_id)

    # Return the dataset
    return ge_dataset


def create_expectation_from_rule(rule: dict) -> dict:
    """Creates a Great Expectations expectation from a validation rule

    Args:
        rule (dict): rule

    Returns:
        dict: Great Expectations expectation configuration
    """
    # Use map_rule_to_expectation to convert rule to expectation format
    expectation = map_rule_to_expectation(rule)

    # Ensure expectation has required fields (expectation_type, kwargs)
    if 'expectation_type' not in expectation or 'kwargs' not in expectation:
        raise ValueError("Expectation must have 'expectation_type' and 'kwargs' fields")

    # Return the expectation configuration
    return expectation


def create_validation_result_from_expectation_result(expectation_result: dict, rule: dict) -> ValidationResult:
    """Creates a ValidationResult from a Great Expectations validation result

    Args:
        expectation_result (dict): expectation_result
        rule (dict): rule

    Returns:
        ValidationResult: Validation result object
    """
    # Extract success status from expectation_result
    success = expectation_result.get('success', False)

    # Use map_expectation_to_rule to convert expectation details to rule format
    rule_result = map_expectation_to_rule(expectation_result, rule)

    # Create ValidationResult using create_validation_result function
    validation_result = create_validation_result(rule, success, rule_result.get('details', {}))

    # Return the ValidationResult object
    return validation_result


class GreatExpectationsAdapter:
    """Adapter for integrating Great Expectations with the data quality validation framework"""

    _expectation_manager: ExpectationManager
    _config: dict
    _context_root: str
    _dataset_cache: dict
    _initialized: bool

    def __init__(self, config: dict):
        """Initialize the Great Expectations adapter with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        
        # Set _initialized to False
        self._initialized = False

        # Determine context root directory from config or use default
        self._context_root = self._config.get('context_root_dir', DEFAULT_CONTEXT_ROOT)

        # Initialize empty dataset cache dictionary
        self._dataset_cache = {}

        # Initialize _expectation_manager to None (lazy initialization)
        self._expectation_manager = None

        # Log successful initialization
        logger.info("GreatExpectationsAdapter initialized")

    def initialize(self) -> bool:
        """Initialize the Great Expectations adapter and expectation manager

        Returns:
            bool: True if initialization was successful
        """
        # If already initialized, return True
        if self._initialized:
            return True

        # Create ExpectationManager with configuration
        self._expectation_manager = ExpectationManager(self._config)

        # Initialize ExpectationManager
        if not self._expectation_manager.initialize():
            logger.error("Failed to initialize ExpectationManager")
            return False

        # Register custom expectations
        register_custom_expectations()

        # Set _initialized to True
        self._initialized = True

        # Return initialization status
        return True

    def validate(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate a dataset against a set of validation rules

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Convert dataset to appropriate Great Expectations dataset type
        ge_dataset = self.get_dataset(dataset)

        # Create temporary expectation suite for validation
        suite_name = f"temp_suite_{time.time()}"
        expectation_suite = self.create_expectation_suite(suite_name, overwrite_existing=True)

        # Convert rules to expectations and add to suite
        expectations = []
        for rule in rules:
            try:
                expectation = map_rule_to_expectation(rule)
                expectations.append(expectation)
                self.add_expectation_to_suite(suite_name, expectation)
            except Exception as e:
                logger.warning(f"Skipping rule due to mapping error: {e}")

        # Execute validation using Great Expectations
        validation_results = []
        if expectations:
            results = self.validate_with_suite(ge_dataset, suite_name)

            # Convert Great Expectations results to ValidationResult objects
            for expectation_result in results.results:
                for rule in rules:
                    if expectation_result['expectation_config']['meta']['rule_id'] == rule['rule_id']:
                        validation_result = create_validation_result_from_expectation_result(expectation_result, rule)
                        validation_results.append(validation_result.to_dict())
                        break
        
        # Update execution context with statistics
        context.update_stats("expectations_added", len(expectations))

        # Return list of validation results
        return validation_results

    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def validate_rule(self, dataset: typing.Any, rule: dict) -> ValidationResult:
        """Validate a single rule against a dataset

        Args:
            dataset (Any): dataset
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Convert dataset to appropriate Great Expectations dataset type
        ge_dataset = self.get_dataset(dataset)

        # Convert rule to expectation
        expectation = map_rule_to_expectation(rule)

        # Execute validation of expectation against dataset
        result = self.execute_validation(ge_dataset, expectation)

        # Convert expectation result to ValidationResult
        validation_result = create_validation_result_from_expectation_result(result, rule)

        # Return ValidationResult
        return validation_result

    def get_dataset(self, dataset: typing.Any) -> typing.Any:
        """Get or create a Great Expectations dataset for a data source

        Args:
            dataset (Any): dataset

        Returns:
            Any: Great Expectations dataset
        """
        # Check if dataset is already in cache
        if dataset in self._dataset_cache:
            return self._dataset_cache[dataset]

        # Determine dataset type (pandas DataFrame, BigQuery table, etc.)
        if isinstance(dataset, pandas.DataFrame):
            # Convert to Great Expectations PandasDataset
            ge_dataset = convert_pandas_to_ge_dataset(dataset)
        else:
            raise ValueError(f"Unsupported dataset type: {type(dataset)}")

        # Cache dataset for future use
        self._dataset_cache[dataset] = ge_dataset

        # Return dataset
        return ge_dataset

    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def execute_validation(self, ge_dataset: typing.Any, expectation: dict) -> dict:
        """Execute validation using Great Expectations

        Args:
            ge_dataset (Any): ge_dataset
            expectation (dict): expectation

        Returns:
            dict: Validation result from Great Expectations
        """
        # Extract expectation type and parameters
        expectation_type = expectation.get("expectation_type")
        kwargs = expectation.get("kwargs", {})

        # Start execution timer
        start_time = time.time()

        # Call appropriate validation method on ge_dataset
        validate_method = getattr(ge_dataset, expectation_type)
        result = validate_method(**kwargs).to_json_dict()

        # Measure execution time
        execution_time = time.time() - start_time

        # Return validation result with execution time
        result["execution_time"] = execution_time
        return result

    def create_expectation_suite(self, suite_name: str, overwrite_existing: bool = False) -> ExpectationSuite:
        """Create a new expectation suite for validation

        Args:
            suite_name (str): suite_name
            overwrite_existing (bool): overwrite_existing

        Returns:
            great_expectations.core.ExpectationSuite: Created expectation suite
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Use expectation_manager to create suite
        suite = self._expectation_manager.create_suite(suite_name, overwrite_existing)

        # Return created suite
        return suite

    def add_expectation_to_suite(self, suite_name: str, expectation: dict) -> bool:
        """Add an expectation to a suite

        Args:
            suite_name (str): suite_name
            expectation (dict): expectation

        Returns:
            bool: True if expectation was added successfully
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Use expectation_manager to add expectation to suite
        success = self._expectation_manager.add_expectation(suite_name, expectation)

        # Return success status
        return success

    def add_rule_to_suite(self, suite_name: str, rule: dict) -> bool:
        """Add a validation rule to a suite as an expectation

        Args:
            suite_name (str): suite_name
            rule (dict): rule

        Returns:
            bool: True if rule was added successfully
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Convert rule to expectation using create_expectation_from_rule
        # Add expectation to suite using add_expectation_to_suite
        success = self._expectation_manager.add_rule(suite_name, rule)

        # Return success status
        return success

    def validate_with_suite(self, dataset: typing.Any, suite_name: str) -> dict:
        """Validate a dataset against an expectation suite

        Args:
            dataset (Any): dataset
            suite_name (str): suite_name

        Returns:
            dict: Validation results
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Convert dataset to appropriate Great Expectations dataset type
        # Use expectation_manager to validate dataset against suite
        results = self._expectation_manager.validate_dataset(dataset, suite_name)

        # Return validation results
        return results

    def get_expectation_manager(self) -> ExpectationManager:
        """Get the expectation manager instance

        Returns:
            ExpectationManager: Expectation manager instance
        """
        # Ensure adapter is initialized
        if not self._initialized:
            self.initialize()

        # Return _expectation_manager
        return self._expectation_manager

    def close(self) -> None:
        """Close the adapter and release resources"""
        if self._expectation_manager:
            self._expectation_manager.close()
        self._initialized = False
        logger.info("GreatExpectationsAdapter closed")


# Example usage (assuming you have a pandas DataFrame and a validation rule)
if __name__ == '__main__':
    # Create a sample pandas DataFrame
    data = {'col1': [1, 2, 3, 4, 5], 'col2': ['A', 'B', 'C', 'D', None]}
    df = pandas.DataFrame(data)

    # Define a sample validation rule
    sample_rule = {
        'rule_id': 'rule_001',
        'name': 'Column col1 should not have null values',
        'type': 'COMPLETENESS',
        'subtype': 'not_null',
        'dimension': 'COMPLETENESS',
        'parameters': {'column': 'col1'}
    }

    # Initialize the GreatExpectationsAdapter
    adapter = GreatExpectationsAdapter({})

    # Initialize the adapter
    adapter.initialize()

    # Validate the DataFrame against the sample rule
    result = adapter.validate_rule(df, sample_rule)

    # Print the validation result
    print(f"Validation Result: {result}")

    # Close the adapter
    adapter.close()