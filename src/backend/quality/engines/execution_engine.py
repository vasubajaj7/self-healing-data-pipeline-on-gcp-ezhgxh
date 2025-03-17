"""
Core execution engine for data quality validation that optimizes the execution of
validation rules against datasets. It provides different execution modes for
various dataset sizes and types, manages execution context, and coordinates the
validation process across different validators.
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
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_MAX_RETRY_ATTEMPTS
)
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.monitoring.metric_client import MetricClient  # src/backend/utils/monitoring/metric_client.py

# Initialize logger for this module
logger = get_logger(__name__)

# Default execution timeout in seconds
DEFAULT_EXECUTION_TIMEOUT = DEFAULT_TIMEOUT_SECONDS

# Default dataset size threshold for switching execution modes
DEFAULT_DATASET_SIZE_THRESHOLD = 1000000


def determine_execution_mode(dataset: Any, config: Dict[str, Any]) -> 'ExecutionMode':
    """Determines the optimal execution mode based on dataset characteristics.

    Args:
        dataset (Any): The dataset to be validated.
        config (dict): Configuration settings for the execution engine.

    Returns:
        ExecutionMode: Optimal execution mode for the dataset.
    """
    # Check if dataset is a pandas DataFrame
    if isinstance(dataset, pandas.DataFrame):
        return ExecutionMode.IN_MEMORY

    # Check if dataset is a BigQuery table reference
    # (Implementation depends on how BigQuery tables are represented)
    # Placeholder for BigQuery table check
    # if isinstance(dataset, BigQueryTableReference):
    #    return ExecutionMode.BIGQUERY

    # Estimate dataset size (row count, memory usage)
    size = estimate_dataset_size(dataset)

    # Compare size against thresholds from config
    threshold = config.get("dataset_size_threshold", DEFAULT_DATASET_SIZE_THRESHOLD)

    # Return appropriate ExecutionMode based on dataset type and size
    if size > threshold:
        return ExecutionMode.BIGQUERY
    else:
        return ExecutionMode.IN_MEMORY


def estimate_dataset_size(dataset: Any) -> int:
    """Estimates the size of a dataset to determine execution strategy.

    Args:
        dataset (Any): The dataset to estimate.

    Returns:
        int: Estimated size in number of rows.
    """
    # Check dataset type (pandas DataFrame, BigQuery table, etc.)
    if isinstance(dataset, pandas.DataFrame):
        # For pandas: Use len() or shape attribute
        return len(dataset)
    else:
        # Placeholder for BigQuery table size estimation
        # For BigQuery: Use table metadata or sample query
        logger.warning("Using default size for non-pandas dataset")
        return DEFAULT_DATASET_SIZE_THRESHOLD // 2  # Default size


def create_bigquery_adapter(config: Dict[str, Any]) -> Any:
    """Factory method to create a BigQuery adapter instance.

    Args:
        config (dict): Configuration settings for the BigQuery adapter.

    Returns:
        Any: BigQuery adapter instance.
    """
    # Import BigQueryAdapter dynamically to avoid circular dependency
    try:
        from src.backend.adapters.bigquery_adapter import BigQueryAdapter  # src/backend/adapters/bigquery_adapter.py
    except ImportError as e:
        logger.error(f"Failed to import BigQueryAdapter: {e}")
        raise

    # Create and return an instance with the provided configuration
    return BigQueryAdapter(config)


@enum.enum.unique
class ExecutionMode(enum.Enum):
    """Enumeration of execution modes for validation."""
    IN_MEMORY = "IN_MEMORY"
    BIGQUERY = "BIGQUERY"
    DISTRIBUTED = "DISTRIBUTED"
    SAMPLING = "SAMPLING"

    def __init__(self):
        """Default enum constructor."""
        pass


class ExecutionContext:
    """Context object for tracking validation execution state and metrics."""

    def __init__(self, mode: ExecutionMode, config: Dict[str, Any]):
        """Initialize execution context with mode and configuration.

        Args:
            mode (ExecutionMode): The execution mode for this context.
            config (dict): Configuration settings for the execution.
        """
        self.mode = mode
        self.stats: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.is_complete: bool = False

    def start(self) -> None:
        """Start the execution context timing."""
        self.stats = {}
        self.start_time = time.time()
        self.is_complete = False

    def complete(self) -> None:
        """Mark execution as complete and record end time."""
        self.end_time = time.time()
        self.is_complete = True
        self.update_stats("execution_time", self.get_execution_time())

    def update_stats(self, key: str, value: Any) -> None:
        """Update execution statistics.

        Args:
            key (str): The key for the statistic.
            value (Any): The value of the statistic.
        """
        self.stats[key] = value

    def increment_stat(self, key: str, increment: int) -> None:
        """Increment a numeric statistic.

        Args:
            key (str): The key for the statistic.
            increment (int): The amount to increment the statistic by.
        """
        if key in self.stats:
            self.stats[key] += increment
        else:
            self.stats[key] = increment

    def get_execution_time(self) -> float:
        """Get the total execution time.

        Returns:
            float: Execution time in seconds.
        """
        if self.is_complete:
            return self.end_time - self.start_time
        else:
            return time.time() - self.start_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution context to dictionary representation.

        Returns:
            dict: Dictionary representation of execution context.
        """
        data = {
            "mode": str(self.mode),
            "stats": self.stats,
            "metadata": self.metadata,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "is_complete": self.is_complete,
            "execution_time": self.get_execution_time()
        }
        return data


class ExecutionEngine:
    """Engine for executing validation rules with optimized performance."""

    def __init__(self, config: Dict[str, Any], bq_adapter: Any = None):
        """Initialize the execution engine with configuration.

        Args:
            config (dict): Configuration settings for the execution engine.
            bq_adapter (Any): BigQuery adapter instance (optional).
        """
        self._config = config
        self._validators: Dict[ValidationRuleType, Any] = {}
        self._bq_adapter = bq_adapter
        self._metric_client = MetricClient()
        logger.info("ExecutionEngine initialized")

    def execute(self, dataset: Any, rules: List[Dict], execution_config: Dict) -> Tuple[List, ExecutionContext]:
        """Execute validation rules against a dataset.

        Args:
            dataset (Any): The dataset to be validated.
            rules (list): List of validation rules to execute.
            execution_config (dict): Configuration settings for this execution.

        Returns:
            tuple: (validation_results, execution_context)
        """
        # Determine optimal execution mode for dataset
        mode = determine_execution_mode(dataset, self._config)
        logger.info(f"Determined execution mode: {mode}")

        # Create execution context with determined mode
        context = ExecutionContext(mode, self._config)

        # Start execution context timing
        context.start()

        # Group rules by validation type
        grouped_rules = self.group_rules_by_type(rules)

        validation_results = []
        # Execute validation using appropriate validator and mode
        if mode == ExecutionMode.IN_MEMORY:
            validation_results = self.execute_in_memory(dataset, rules, context)
        elif mode == ExecutionMode.BIGQUERY:
            dataset_id = execution_config.get("dataset_id")
            table_id = execution_config.get("table_id")
            if not dataset_id or not table_id:
                raise ValueError("dataset_id and table_id must be provided for BigQuery execution")
            validation_results = self.execute_with_bigquery(dataset_id, table_id, rules, context)
        elif mode == ExecutionMode.SAMPLING:
            sample_size = execution_config.get("sample_size", 0.1)
            validation_results = self.execute_with_sampling(dataset, rules, context, sample_size)
        else:
            raise ValueError(f"Unsupported execution mode: {mode}")

        # Complete execution context
        context.complete()

        # Report execution metrics
        self.report_metrics(context, validation_results)

        # Return validation results and execution context
        return validation_results, context

    def execute_in_memory(self, dataset: Any, rules: List[Dict], context: ExecutionContext) -> List:
        """Execute validation rules in memory.

        Args:
            dataset (Any): The dataset to be validated (pandas DataFrame).
            rules (list): List of validation rules to execute.
            context (ExecutionContext): The execution context.

        Returns:
            list: Validation results.
        """
        # Ensure dataset is in pandas DataFrame format
        if not isinstance(dataset, pandas.DataFrame):
            raise ValueError("In-memory execution requires a pandas DataFrame")

        # Group rules by validator type
        grouped_rules = self.group_rules_by_type(rules)
        validation_results = []

        # For each validator type, execute rules using appropriate validator
        for rule_type, rules_for_type in grouped_rules.items():
            validator = self.get_validator(rule_type)
            results = validator.validate(dataset, rules_for_type, context)
            validation_results.extend(results)
            context.increment_stat("rules_executed", len(rules_for_type))

        return validation_results

    def execute_with_bigquery(self, dataset_id: str, table_id: str, rules: List[Dict], context: ExecutionContext) -> List:
        """Execute validation rules using BigQuery.

        Args:
            dataset_id (str): The ID of the BigQuery dataset.
            table_id (str): The ID of the BigQuery table.
            rules (list): List of validation rules to execute.
            context (ExecutionContext): The execution context.

        Returns:
            list: Validation results.
        """
        # Ensure BigQuery adapter is available or create one on demand
        bq_adapter = self.ensure_bq_adapter()

        # Group rules by validator type
        grouped_rules = self.group_rules_by_type(rules)
        validation_results = []

        # For each validator type, execute rules using BigQuery adapter
        for rule_type, rules_for_type in grouped_rules.items():
            validator = self.get_validator(rule_type)
            results = validator.validate(dataset_id, table_id, rules_for_type, context, bq_adapter=bq_adapter)
            validation_results.extend(results)
            context.increment_stat("rules_executed", len(rules_for_type))

        return validation_results

    def execute_with_sampling(self, dataset: Any, rules: List[Dict], context: ExecutionContext, sample_size: float) -> List:
        """Execute validation rules using sampling for large datasets.

        Args:
            dataset (Any): The dataset to be validated.
            rules (list): List of validation rules to execute.
            context (ExecutionContext): The execution context.
            sample_size (float): The fraction of the dataset to sample.

        Returns:
            list: Validation results.
        """
        # Determine appropriate sampling method based on dataset type
        # (Implementation depends on dataset type and sampling requirements)
        logger.info(f"Sampling {sample_size*100}% of the dataset")

        # Extract sample from dataset
        # (Implementation depends on dataset type)
        sample = dataset.sample(frac=sample_size)

        # Execute validation on sample using in-memory execution
        results = self.execute_in_memory(sample, rules, context)

        # Adjust confidence levels based on sampling
        # (Implementation depends on statistical methods)
        logger.info("Adjusting confidence levels based on sampling")

        # Return validation results with sampling metadata
        context.update_stats("sample_size", sample_size)
        return results

    def get_validator(self, rule_type: ValidationRuleType) -> Any:
        """Get or create a validator for a specific validation type.

        Args:
            rule_type (ValidationRuleType): The type of validation rule.

        Returns:
            Any: Validator instance for the specified rule type.
        """
        # Check if validator for rule_type exists in _validators dictionary
        if rule_type in self._validators:
            # If exists, return cached validator
            return self._validators[rule_type]
        else:
            # If not, create new validator based on rule_type
            try:
                # Dynamically import the validator class
                module_name = f"src.backend.quality.validators.{rule_type.name.lower()}_validator"
                class_name = f"{rule_type.name.capitalize()}Validator"
                module = importlib.import_module(module_name)
                validator_class = getattr(module, class_name)
            except ImportError as e:
                logger.error(f"Failed to import validator module {rule_type.name}: {e}")
                raise
            except AttributeError as e:
                logger.error(f"Failed to find validator class {rule_type.name} in module: {e}")
                raise

            # Create validator instance
            validator = validator_class(self._config)

            # Cache validator in _validators dictionary
            self._validators[rule_type] = validator
            return validator

    def ensure_bq_adapter(self) -> Any:
        """Ensure BigQuery adapter is available or create one on demand.

        Returns:
            Any: BigQuery adapter instance.
        """
        # Check if _bq_adapter is already initialized
        if self._bq_adapter:
            # If initialized, return existing adapter
            return self._bq_adapter
        else:
            # If not, use create_bigquery_adapter to create a new adapter
            self._bq_adapter = create_bigquery_adapter(self._config)
            return self._bq_adapter

    def group_rules_by_type(self, rules: List[Dict]) -> Dict[ValidationRuleType, List[Dict]]:
        """Group validation rules by their rule type.

        Args:
            rules (list): List of validation rules.

        Returns:
            dict: Rules grouped by ValidationRuleType.
        """
        grouped_rules: Dict[ValidationRuleType, List[Dict]] = {}
        for rule in rules:
            try:
                rule_type = ValidationRuleType(rule["rule_type"])
            except ValueError:
                logger.warning(f"Skipping rule with invalid rule_type: {rule['rule_type']}")
                continue

            if rule_type not in grouped_rules:
                grouped_rules[rule_type] = []
            grouped_rules[rule_type].append(rule)

        return grouped_rules

    def report_metrics(self, context: ExecutionContext, results: List) -> None:
        """Report execution metrics to monitoring system.

        Args:
            context (ExecutionContext): The execution context.
            results (list): List of validation results.
        """
        # Extract metrics from execution context
        execution_time = context.get_execution_time()
        rules_executed = context.stats.get("rules_executed", 0)

        # Calculate success rate from results
        success_count = sum(1 for result in results if result["passed"])
        success_rate = (success_count / len(results)) if results else 1.0

        # Report execution time metric
        self._metric_client.create_gauge_metric(
            metric_type="validation.execution_time",
            value=execution_time,
            labels={"mode": str(context.mode)}
        )

        # Report rules executed metric
        self._metric_client.create_gauge_metric(
            metric_type="validation.rules_executed",
            value=rules_executed,
            labels={"mode": str(context.mode)}
        )

        # Report success rate metric
        self._metric_client.create_gauge_metric(
            metric_type="validation.success_rate",
            value=success_rate,
            labels={"mode": str(context.mode)}
        )

        # Report resource utilization metrics
        # (Implementation depends on resource monitoring system)
        logger.info("Reported validation metrics")

    def close(self) -> None:
        """Close the execution engine and release resources."""
        # Close each validator in _validators dictionary
        for validator in self._validators.values():
            if hasattr(validator, "close") and callable(getattr(validator, "close")):
                validator.close()

        # Close BigQuery adapter if it exists
        if self._bq_adapter and hasattr(self._bq_adapter, "close") and callable(getattr(self._bq_adapter, "close")):
            self._bq_adapter.close()

        # Close MetricClient if it exists
        if self._metric_client and hasattr(self._metric_client, "close") and callable(getattr(self._metric_client, "close")):
            self._metric_client.close()

        logger.info("ExecutionEngine closed")