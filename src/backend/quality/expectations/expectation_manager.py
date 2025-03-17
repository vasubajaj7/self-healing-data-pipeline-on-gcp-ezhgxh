"""
Manages Great Expectations suites, expectations, and their execution for data quality validation.

This module serves as the interface between the data quality framework and Great Expectations,
handling expectation creation, mapping between validation rules and expectations, and execution
of validation checks.
"""

import os
import json
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.data_context import DataContext
from great_expectations.dataset import Dataset

from ...constants import ValidationRuleType, QualityDimension, DEFAULT_TIMEOUT_SECONDS
from ...config import get_config
from ...utils.logging.logger import get_logger
from .custom_expectations import register_custom_expectations

# Initialize logger
logger = get_logger(__name__)

# Default timeout for expectation execution
DEFAULT_EXPECTATION_TIMEOUT = DEFAULT_TIMEOUT_SECONDS

# Mapping from ValidationRuleType to expectation types
RULE_TO_EXPECTATION_MAP = {
    ValidationRuleType.SCHEMA: {
        "column_exists": "expect_column_to_exist",
        "column_type": "expect_column_values_to_be_of_type",
        "column_values_in_set": "expect_column_values_to_be_in_set",
        "column_values_in_range": "expect_column_values_to_be_between",
        "table_columns": "expect_table_columns_to_match_ordered_list",
        "table_row_count": "expect_table_row_count_to_be_between",
    },
    ValidationRuleType.COMPLETENESS: {
        "not_null": "expect_column_values_to_not_be_null",
        "null_percentage": "expect_column_values_to_be_null",
        "not_empty": "expect_column_values_to_not_be_null",
        "row_completeness": "expect_column_values_to_not_be_null",
    },
    ValidationRuleType.ANOMALY: {
        "value_anomaly": "expect_column_values_anomaly_score",
        "statistical_anomaly": "expect_column_values_anomaly_score",
        "outlier_detection": "expect_column_values_to_be_between",
        "pattern_anomaly": "expect_column_values_to_match_regex",
    },
    ValidationRuleType.REFERENTIAL: {
        "reference_table": "expect_column_values_in_reference_table",
        "foreign_key": "expect_column_values_to_be_in_set",
        "primary_key": "expect_column_values_to_be_unique",
        "referential_integrity": "expect_column_values_in_reference_table",
    }
}


def map_rule_to_expectation(rule: Dict) -> Dict:
    """
    Maps a validation rule to a Great Expectations expectation configuration.
    
    Args:
        rule: The validation rule to map
        
    Returns:
        Expectation configuration dictionary
        
    Raises:
        ValueError: If the rule cannot be mapped to an expectation
    """
    try:
        # Extract rule type, subtype, and parameters
        rule_id = rule.get("rule_id")
        rule_type = rule.get("rule_type")
        rule_subtype = rule.get("rule_subtype")
        rule_params = rule.get("parameters", {})
        
        if not rule_type:
            raise ValueError(f"Rule is missing required 'rule_type' field: {rule}")
        
        # Convert string rule_type to enum if needed
        if isinstance(rule_type, str):
            rule_type = ValidationRuleType(rule_type)
        
        # Get appropriate expectation type
        expectation_type = get_expectation_type_for_rule(rule_type, rule_subtype)
        
        # Transform rule parameters to expectation parameters
        kwargs = {}
        
        # Common parameters
        if "column" in rule_params:
            kwargs["column"] = rule_params["column"]
        
        # Type-specific parameter mapping
        if rule_type == ValidationRuleType.SCHEMA:
            if rule_subtype == "column_exists":
                kwargs["column"] = rule_params.get("column_name")
            elif rule_subtype == "column_type":
                kwargs["type_"] = rule_params.get("expected_type")
            elif rule_subtype == "column_values_in_set":
                kwargs["value_set"] = rule_params.get("value_set")
            elif rule_subtype == "column_values_in_range":
                kwargs["min_value"] = rule_params.get("min_value")
                kwargs["max_value"] = rule_params.get("max_value")
            elif rule_subtype == "table_columns":
                kwargs["column_list"] = rule_params.get("expected_columns")
            elif rule_subtype == "table_row_count":
                kwargs["min_value"] = rule_params.get("min_count")
                kwargs["max_value"] = rule_params.get("max_count")
                
        elif rule_type == ValidationRuleType.COMPLETENESS:
            if rule_subtype == "not_null" or rule_subtype == "not_empty":
                kwargs["mostly"] = rule_params.get("threshold", 1.0)
            elif rule_subtype == "null_percentage":
                kwargs["mostly"] = 1.0 - rule_params.get("threshold", 0.0)
                
        elif rule_type == ValidationRuleType.ANOMALY:
            if rule_subtype == "value_anomaly" or rule_subtype == "statistical_anomaly":
                kwargs["threshold"] = rule_params.get("threshold", 3.0)
                kwargs["method"] = rule_params.get("method", "zscore")
            elif rule_subtype == "outlier_detection":
                kwargs["min_value"] = rule_params.get("min_value")
                kwargs["max_value"] = rule_params.get("max_value")
                kwargs["mostly"] = rule_params.get("threshold", 0.95)
                
        elif rule_type == ValidationRuleType.REFERENTIAL:
            if rule_subtype == "reference_table" or rule_subtype == "referential_integrity":
                kwargs["reference_table"] = rule_params.get("reference_table")
                kwargs["reference_column"] = rule_params.get("reference_column")
            elif rule_subtype == "foreign_key":
                kwargs["value_set"] = rule_params.get("reference_values", [])
        
        # Add additional parameters
        if "mostly" in rule_params and "mostly" not in kwargs:
            kwargs["mostly"] = rule_params["mostly"]
            
        # Create expectation configuration
        expectation_config = {
            "expectation_type": expectation_type,
            "kwargs": kwargs,
            "meta": {
                "rule_id": rule_id,
                "rule_type": rule_type.value if hasattr(rule_type, "value") else rule_type,
                "rule_subtype": rule_subtype,
                "quality_dimension": rule.get("quality_dimension")
            }
        }
        
        return expectation_config
        
    except Exception as e:
        logger.error(f"Error mapping rule to expectation: {e}", exc_info=True)
        raise ValueError(f"Failed to map rule to expectation: {str(e)}")


def map_expectation_to_rule(expectation_result: Dict, rule: Dict) -> Dict:
    """
    Maps a Great Expectations validation result to a rule result.
    
    Args:
        expectation_result: The result from a Great Expectations validation
        rule: The original validation rule
        
    Returns:
        Rule validation result in standardized format
    """
    try:
        # Extract success status and result details
        success = expectation_result.get("success", False)
        result_details = expectation_result.get("result", {})
        
        # Get rule metadata
        rule_id = rule.get("rule_id")
        rule_type = rule.get("rule_type")
        rule_subtype = rule.get("rule_subtype")
        quality_dimension = rule.get("quality_dimension")
        
        # Create rule validation result
        rule_result = {
            "rule_id": rule_id,
            "success": success,
            "rule_type": rule_type,
            "rule_subtype": rule_subtype,
            "quality_dimension": quality_dimension,
            "timestamp": pd.Timestamp.now().isoformat(),
            "details": {
                "observed_value": result_details.get("observed_value"),
                "unexpected_count": result_details.get("unexpected_count"),
                "unexpected_percent": result_details.get("unexpected_percent"),
                "unexpected_values": result_details.get("unexpected_values"),
                "additional_details": result_details.get("details", {})
            }
        }
        
        return rule_result
        
    except Exception as e:
        logger.error(f"Error mapping expectation result to rule result: {e}", exc_info=True)
        return {
            "rule_id": rule.get("rule_id"),
            "success": False,
            "rule_type": rule.get("rule_type"),
            "rule_subtype": rule.get("rule_subtype"),
            "quality_dimension": rule.get("quality_dimension"),
            "timestamp": pd.Timestamp.now().isoformat(),
            "details": {
                "error": str(e)
            }
        }


def create_expectation_suite(context: DataContext, suite_name: str, overwrite_existing: bool = False) -> ExpectationSuite:
    """
    Creates a new expectation suite with the given name.
    
    Args:
        context: The Great Expectations data context
        suite_name: Name for the new expectation suite
        overwrite_existing: Whether to overwrite if suite exists
        
    Returns:
        Created expectation suite
        
    Raises:
        ValueError: If suite exists and overwrite_existing is False
    """
    try:
        # Check if suite already exists
        suite_exists = context.expectations_store.has_key(suite_name)
        
        if suite_exists:
            if overwrite_existing:
                # Delete existing suite
                logger.info(f"Deleting existing suite {suite_name} for overwrite")
                context.delete_expectation_suite(suite_name)
            else:
                raise ValueError(f"Expectation suite '{suite_name}' already exists and overwrite_existing is False")
        
        # Create new expectation suite
        logger.info(f"Creating new expectation suite: {suite_name}")
        suite = context.create_expectation_suite(suite_name, overwrite_existing=overwrite_existing)
        
        return suite
        
    except Exception as e:
        logger.error(f"Error creating expectation suite {suite_name}: {e}", exc_info=True)
        raise


def load_expectation_suite(context: DataContext, suite_name: str) -> Optional[ExpectationSuite]:
    """
    Loads an existing expectation suite by name.
    
    Args:
        context: The Great Expectations data context
        suite_name: Name of the expectation suite to load
        
    Returns:
        Loaded expectation suite or None if not found
    """
    try:
        # Check if suite exists
        if not context.expectations_store.has_key(suite_name):
            logger.warning(f"Expectation suite {suite_name} not found")
            return None
        
        # Load and return the suite
        logger.debug(f"Loading expectation suite: {suite_name}")
        return context.get_expectation_suite(suite_name)
        
    except Exception as e:
        logger.error(f"Error loading expectation suite {suite_name}: {e}", exc_info=True)
        return None


def save_expectation_suite(context: DataContext, suite: ExpectationSuite) -> bool:
    """
    Saves an expectation suite to the data context.
    
    Args:
        context: The Great Expectations data context
        suite: The expectation suite to save
        
    Returns:
        True if save was successful
    """
    try:
        logger.debug(f"Saving expectation suite: {suite.expectation_suite_name}")
        context.save_expectation_suite(suite)
        return True
        
    except Exception as e:
        logger.error(f"Error saving expectation suite {suite.expectation_suite_name}: {e}", exc_info=True)
        return False


def validate_with_expectation_suite(
    context: DataContext, 
    dataset: Any, 
    suite_name: str,
    execution_options: Dict = None
) -> Dict:
    """
    Validates a dataset against an expectation suite.
    
    Args:
        context: The Great Expectations data context
        dataset: The dataset to validate (pandas DataFrame, Spark DataFrame, etc.)
        suite_name: Name of the expectation suite to use
        execution_options: Optional execution parameters
        
    Returns:
        Validation results
        
    Raises:
        ValueError: If suite does not exist or validation fails
    """
    try:
        # Set default execution options if not provided
        if execution_options is None:
            execution_options = {}
        
        # Set default timeout if not specified
        if "timeout" not in execution_options:
            execution_options["timeout"] = DEFAULT_EXPECTATION_TIMEOUT
            
        # Load the expectation suite
        suite = load_expectation_suite(context, suite_name)
        if suite is None:
            raise ValueError(f"Expectation suite {suite_name} not found")
        
        # Create a validator for the dataset
        batch = context.get_batch({}, batch_kwargs={
            "dataset": dataset,
            "datasource": "pandas_datasource"
        })
        
        # Execute validation
        logger.info(f"Validating dataset against suite: {suite_name}")
        results = batch.validate(expectation_suite=suite, **execution_options)
        
        return results
        
    except Exception as e:
        logger.error(f"Error validating dataset with suite {suite_name}: {e}", exc_info=True)
        raise


def validate_with_expectations(
    context: DataContext, 
    dataset: Any, 
    expectations: List[Dict],
    execution_options: Dict = None
) -> List[Dict]:
    """
    Validates a dataset against a list of expectations.
    
    Args:
        context: The Great Expectations data context
        dataset: The dataset to validate
        expectations: List of expectation configurations
        execution_options: Optional execution parameters
        
    Returns:
        List of validation results for each expectation
    """
    try:
        # Set default execution options if not provided
        if execution_options is None:
            execution_options = {}
        
        # Set default timeout if not specified
        if "timeout" not in execution_options:
            execution_options["timeout"] = DEFAULT_EXPECTATION_TIMEOUT
            
        # Create a temporary expectation suite
        temp_suite_name = f"temp_suite_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')}"
        temp_suite = context.create_expectation_suite(temp_suite_name, overwrite_existing=True)
        
        # Add expectations to suite
        for expectation_config in expectations:
            temp_suite.add_expectation(ExpectationConfiguration(**expectation_config))
        
        # Create a validator for the dataset
        batch = context.get_batch({}, batch_kwargs={
            "dataset": dataset,
            "datasource": "pandas_datasource"
        })
        
        # Execute validation for each expectation
        validation_results = []
        for expectation_config in expectations:
            expectation_type = expectation_config.get("expectation_type")
            kwargs = expectation_config.get("kwargs", {})
            
            # Get validation method by name
            validate_method = getattr(batch, expectation_type)
            
            # Execute validation method
            result = validate_method(**kwargs)
            validation_results.append(result.to_json_dict())
        
        # Clean up temporary suite
        context.delete_expectation_suite(temp_suite_name)
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating dataset with expectations: {e}", exc_info=True)
        # Try to clean up temporary suite if it exists
        try:
            if 'temp_suite_name' in locals() and context.expectations_store.has_key(temp_suite_name):
                context.delete_expectation_suite(temp_suite_name)
        except:
            pass
        
        raise


def get_expectation_type_for_rule(rule_type: ValidationRuleType, rule_subtype: str) -> str:
    """
    Determines the appropriate expectation type for a validation rule.
    
    Args:
        rule_type: The type of validation rule
        rule_subtype: The subtype of validation rule
        
    Returns:
        Expectation type name
        
    Raises:
        ValueError: If no mapping exists for the rule type and subtype
    """
    try:
        # Get mapping for rule type
        type_mapping = RULE_TO_EXPECTATION_MAP.get(rule_type)
        if type_mapping is None:
            raise ValueError(f"No mapping exists for rule type: {rule_type}")
        
        # Get specific expectation type for rule subtype
        expectation_type = type_mapping.get(rule_subtype)
        if expectation_type is None:
            # Try to find a default mapping for this rule type
            expectation_type = type_mapping.get("default")
            if expectation_type is None:
                raise ValueError(f"No mapping exists for rule subtype: {rule_subtype} under type {rule_type}")
        
        return expectation_type
        
    except Exception as e:
        logger.error(f"Error getting expectation type for rule: {e}", exc_info=True)
        raise ValueError(f"Failed to determine expectation type: {str(e)}")


def create_data_context(context_root_dir: str) -> DataContext:
    """
    Creates or gets a Great Expectations data context.
    
    Args:
        context_root_dir: Root directory for the data context
        
    Returns:
        Great Expectations data context
        
    Raises:
        Exception: If data context cannot be created or loaded
    """
    try:
        # Check if context exists
        if DataContext.does_config_exist(context_root_dir):
            # Load existing context
            logger.debug(f"Loading existing data context from {context_root_dir}")
            return DataContext(context_root_dir)
        else:
            # Create context directory if it doesn't exist
            os.makedirs(context_root_dir, exist_ok=True)
            
            # Create new context
            logger.info(f"Creating new data context at {context_root_dir}")
            return DataContext.create(context_root_dir)
            
    except Exception as e:
        logger.error(f"Error creating/loading data context: {e}", exc_info=True)
        raise


class ExpectationManager:
    """
    Manages Great Expectations suites, expectations, and validation execution.
    
    This class provides a unified interface for working with Great Expectations,
    handling the creation, management, and execution of data quality validations.
    """
    
    def __init__(self, config: Dict = None):
        """
        Initialize the ExpectationManager with configuration.
        
        Args:
            config: Configuration dictionary with options for the manager
        """
        # Initialize configuration
        self._config = config or {}
        self._initialized = False
        self._suites = {}  # Cache for loaded suites
        
        # Get context root directory from config or default
        app_config = get_config()
        default_context_dir = os.path.join(
            app_config.get("quality.base_path", "data/quality"),
            "great_expectations"
        )
        self._context_root_dir = self._config.get("context_root_dir", default_context_dir)
        
        # Initialize context as None (lazy initialization)
        self._context = None
        
        logger.debug(f"ExpectationManager initialized with context root: {self._context_root_dir}")
    
    def initialize(self) -> bool:
        """
        Initialize the Great Expectations context and register custom expectations.
        
        Returns:
            True if initialization was successful
        """
        if self._initialized:
            return True
            
        try:
            # Create data context
            self._context = create_data_context(self._context_root_dir)
            
            # Register custom expectations
            register_custom_expectations()
            
            # Mark as initialized
            self._initialized = True
            logger.info("ExpectationManager successfully initialized")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize ExpectationManager: {e}", exc_info=True)
            return False
    
    def create_suite(self, suite_name: str, overwrite_existing: bool = False) -> ExpectationSuite:
        """
        Creates a new expectation suite.
        
        Args:
            suite_name: Name for the new suite
            overwrite_existing: Whether to overwrite if suite exists
            
        Returns:
            Created expectation suite
            
        Raises:
            ValueError: If suite exists and overwrite_existing is False
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        # Create suite
        suite = create_expectation_suite(self._context, suite_name, overwrite_existing)
        
        # Cache suite
        self._suites[suite_name] = suite
        
        return suite
    
    def get_suite(self, suite_name: str) -> Optional[ExpectationSuite]:
        """
        Gets an expectation suite by name.
        
        Args:
            suite_name: Name of the suite to get
            
        Returns:
            Expectation suite or None if not found
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        # Check if suite is in cache
        if suite_name in self._suites:
            return self._suites[suite_name]
            
        # Load suite
        suite = load_expectation_suite(self._context, suite_name)
        
        # Cache suite if found
        if suite is not None:
            self._suites[suite_name] = suite
            
        return suite
    
    def save_suite(self, suite: ExpectationSuite) -> bool:
        """
        Saves an expectation suite.
        
        Args:
            suite: The expectation suite to save
            
        Returns:
            True if save was successful
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        # Save suite
        result = save_expectation_suite(self._context, suite)
        
        # Update suite in cache if present
        if result and suite.expectation_suite_name in self._suites:
            self._suites[suite.expectation_suite_name] = suite
            
        return result
    
    def delete_suite(self, suite_name: str) -> bool:
        """
        Deletes an expectation suite.
        
        Args:
            suite_name: Name of the suite to delete
            
        Returns:
            True if deletion was successful
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Delete suite
            if self._context.expectations_store.has_key(suite_name):
                self._context.delete_expectation_suite(suite_name)
                logger.info(f"Deleted expectation suite: {suite_name}")
                
                # Remove from cache if present
                if suite_name in self._suites:
                    del self._suites[suite_name]
                    
                return True
            else:
                logger.warning(f"Cannot delete suite {suite_name} - not found")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting suite {suite_name}: {e}", exc_info=True)
            return False
    
    def list_suites(self) -> List[str]:
        """
        Lists all available expectation suites.
        
        Returns:
            List of suite names
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Get list of suite names
            return self._context.list_expectation_suite_names()
            
        except Exception as e:
            logger.error(f"Error listing expectation suites: {e}", exc_info=True)
            return []
    
    def add_expectation(self, suite_name: str, expectation: Dict) -> bool:
        """
        Adds an expectation to a suite.
        
        Args:
            suite_name: Name of the suite to add to
            expectation: Expectation configuration dictionary
            
        Returns:
            True if expectation was added successfully
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Get suite
            suite = self.get_suite(suite_name)
            if suite is None:
                logger.warning(f"Cannot add expectation - suite {suite_name} not found")
                return False
            
            # Create expectation configuration
            expectation_config = ExpectationConfiguration(**expectation)
            
            # Add expectation to suite
            suite.add_expectation(expectation_config)
            
            # Save suite
            return self.save_suite(suite)
            
        except Exception as e:
            logger.error(f"Error adding expectation to suite {suite_name}: {e}", exc_info=True)
            return False
    
    def add_expectations(self, suite_name: str, expectations: List[Dict]) -> int:
        """
        Adds multiple expectations to a suite.
        
        Args:
            suite_name: Name of the suite to add to
            expectations: List of expectation configurations
            
        Returns:
            Number of expectations added successfully
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Get suite
            suite = self.get_suite(suite_name)
            if suite is None:
                logger.warning(f"Cannot add expectations - suite {suite_name} not found")
                return 0
            
            # Track successful additions
            success_count = 0
            
            # Add each expectation
            for expectation in expectations:
                try:
                    expectation_config = ExpectationConfiguration(**expectation)
                    suite.add_expectation(expectation_config)
                    success_count += 1
                except Exception as exp_err:
                    logger.warning(f"Failed to add expectation: {exp_err}")
            
            # Save suite if any expectations were added
            if success_count > 0:
                if self.save_suite(suite):
                    return success_count
                else:
                    return 0
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Error adding expectations to suite {suite_name}: {e}", exc_info=True)
            return 0
    
    def add_rule(self, suite_name: str, rule: Dict) -> bool:
        """
        Adds a validation rule to a suite as an expectation.
        
        Args:
            suite_name: Name of the suite to add to
            rule: Validation rule dictionary
            
        Returns:
            True if rule was added successfully
        """
        try:
            # Map rule to expectation
            expectation = map_rule_to_expectation(rule)
            
            # Add expectation to suite
            return self.add_expectation(suite_name, expectation)
            
        except Exception as e:
            logger.error(f"Error adding rule to suite {suite_name}: {e}", exc_info=True)
            return False
    
    def add_rules(self, suite_name: str, rules: List[Dict]) -> int:
        """
        Adds multiple validation rules to a suite as expectations.
        
        Args:
            suite_name: Name of the suite to add to
            rules: List of validation rule dictionaries
            
        Returns:
            Number of rules added successfully
        """
        try:
            # Map rules to expectations
            expectations = []
            for rule in rules:
                try:
                    expectation = map_rule_to_expectation(rule)
                    expectations.append(expectation)
                except Exception as rule_err:
                    logger.warning(f"Failed to map rule to expectation: {rule_err}")
            
            # Add expectations to suite
            return self.add_expectations(suite_name, expectations)
            
        except Exception as e:
            logger.error(f"Error adding rules to suite {suite_name}: {e}", exc_info=True)
            return 0
    
    def validate_dataset(self, dataset: Any, suite_name: str, execution_options: Dict = None) -> Dict:
        """
        Validates a dataset against an expectation suite.
        
        Args:
            dataset: The dataset to validate
            suite_name: Name of the expectation suite to use
            execution_options: Optional execution parameters
            
        Returns:
            Validation results
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        # Validate dataset
        return validate_with_expectation_suite(
            self._context, 
            dataset, 
            suite_name, 
            execution_options
        )
    
    def validate_with_rules(self, dataset: Any, rules: List[Dict], execution_options: Dict = None) -> List[Dict]:
        """
        Validates a dataset against a list of validation rules.
        
        Args:
            dataset: The dataset to validate
            rules: List of validation rules
            execution_options: Optional execution parameters
            
        Returns:
            List of validation results for each rule
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Map rules to expectations
            expectations = []
            rule_map = {}  # Map expectation index to rule
            
            for i, rule in enumerate(rules):
                try:
                    expectation = map_rule_to_expectation(rule)
                    expectations.append(expectation)
                    rule_map[len(expectations) - 1] = rule
                except Exception as rule_err:
                    logger.warning(f"Skipping rule due to mapping error: {rule_err}")
            
            # Validate dataset against expectations
            expectation_results = validate_with_expectations(
                self._context, 
                dataset, 
                expectations, 
                execution_options
            )
            
            # Map expectation results back to rule results
            rule_results = []
            for i, exp_result in enumerate(expectation_results):
                if i in rule_map:
                    rule = rule_map[i]
                    rule_result = map_expectation_to_rule(exp_result, rule)
                    rule_results.append(rule_result)
            
            return rule_results
            
        except Exception as e:
            logger.error(f"Error validating dataset with rules: {e}", exc_info=True)
            # Return error results for each rule
            return [
                {
                    "rule_id": rule.get("rule_id"),
                    "success": False,
                    "rule_type": rule.get("rule_type"),
                    "rule_subtype": rule.get("rule_subtype"),
                    "quality_dimension": rule.get("quality_dimension"),
                    "timestamp": pd.Timestamp.now().isoformat(),
                    "details": {
                        "error": str(e)
                    }
                }
                for rule in rules
            ]
    
    def export_suite_to_json(self, suite_name: str, file_path: str) -> bool:
        """
        Exports an expectation suite to a JSON file.
        
        Args:
            suite_name: Name of the suite to export
            file_path: Path where the JSON file will be saved
            
        Returns:
            True if export was successful
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Get suite
            suite = self.get_suite(suite_name)
            if suite is None:
                logger.warning(f"Cannot export suite {suite_name} - not found")
                return False
            
            # Convert suite to JSON
            suite_json = suite.to_json_dict()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            
            # Write to file
            with open(file_path, 'w') as f:
                json.dump(suite_json, f, indent=2)
                
            logger.info(f"Exported suite {suite_name} to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting suite {suite_name} to JSON: {e}", exc_info=True)
            return False
    
    def import_suite_from_json(self, file_path: str, suite_name: str = None) -> Optional[ExpectationSuite]:
        """
        Imports an expectation suite from a JSON file.
        
        Args:
            file_path: Path to the JSON file
            suite_name: Optional name for the imported suite (defaults to name in file)
            
        Returns:
            Imported expectation suite or None if import failed
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Read JSON from file
            with open(file_path, 'r') as f:
                suite_json = json.load(f)
            
            # Get suite name from JSON if not provided
            if suite_name is None:
                suite_name = suite_json.get("expectation_suite_name")
                if not suite_name:
                    raise ValueError("Suite name not provided and not found in JSON")
            else:
                # Override name in JSON
                suite_json["expectation_suite_name"] = suite_name
            
            # Create suite from JSON
            suite = ExpectationSuite(**suite_json)
            
            # Save suite
            if self.save_suite(suite):
                logger.info(f"Imported suite {suite_name} from {file_path}")
                return suite
            else:
                logger.warning(f"Failed to save imported suite {suite_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error importing suite from JSON {file_path}: {e}", exc_info=True)
            return None
    
    def create_validator(self, dataset: Any, suite_name: str) -> Optional[Any]:
        """
        Creates a validator for a dataset and expectation suite.
        
        Args:
            dataset: The dataset to validate
            suite_name: Name of the expectation suite
            
        Returns:
            Validator for the dataset
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        try:
            # Get suite
            suite = self.get_suite(suite_name)
            if suite is None:
                logger.warning(f"Cannot create validator - suite {suite_name} not found")
                return None
            
            # Create batch and validator
            batch = self._context.get_batch({}, batch_kwargs={
                "dataset": dataset,
                "datasource": "pandas_datasource"
            })
            
            # Set expectation suite on batch
            batch.expectation_suite = suite
            
            return batch
            
        except Exception as e:
            logger.error(f"Error creating validator: {e}", exc_info=True)
            return None
    
    def get_context(self) -> DataContext:
        """
        Gets the Great Expectations data context.
        
        Returns:
            Great Expectations data context
        """
        # Ensure manager is initialized
        if not self._initialized:
            self.initialize()
            
        return self._context
    
    def close(self) -> None:
        """
        Closes the manager and releases resources.
        """
        try:
            # Save any unsaved suites
            for suite_name, suite in self._suites.items():
                try:
                    save_expectation_suite(self._context, suite)
                except Exception as save_err:
                    logger.warning(f"Error saving suite {suite_name} during close: {save_err}")
            
            # Clear cache
            self._suites.clear()
            
            # Reset initialization flag
            self._initialized = False
            
            logger.debug("ExpectationManager closed")
            
        except Exception as e:
            logger.error(f"Error closing ExpectationManager: {e}", exc_info=True)