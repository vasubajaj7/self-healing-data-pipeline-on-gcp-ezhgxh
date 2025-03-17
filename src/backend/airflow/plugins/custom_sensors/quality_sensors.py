"""
Custom Airflow sensors for monitoring data quality validation processes in the self-healing data pipeline.
These sensors enable DAGs to wait for quality validation completion, check quality scores against thresholds,
and detect specific quality issues. The module also includes self-healing variants that can automatically remediate common quality problems.
"""

import typing
import datetime
import json

# Import third-party libraries with version specification
from airflow.sensors.base import BaseSensorOperator  # apache-airflow:2.5.x
from airflow.utils.decorators import apply_defaults  # apache-airflow:2.5.x
from airflow.exceptions import AirflowException  # apache-airflow:2.5.x

# Import internal modules
from src.backend import constants  # Import enumerations for healing action types and alert severity levels
from src.backend.utils.logging import logger  # Configure logging for quality sensors
from src.backend.quality.engines import validation_engine  # Access validation engine for quality checks
from src.backend.quality.engines import quality_scorer  # Access quality scoring functionality
from src.backend.self_healing.ai import issue_classifier  # Classify quality issues for self-healing
from src.backend.self_healing.correction import data_corrector  # Apply corrections to data quality issues
from src.backend.db.repositories import quality_repository  # Access quality validation data from database

# Configure logger
logger = logger.get_logger(__name__)

# Define global constants
DEFAULT_POKE_INTERVAL = 60
DEFAULT_TIMEOUT = 3600


def format_validation_context(context: dict) -> dict:
    """Formats context information for validation sensors

    Args:
        context (dict): Airflow context dictionary

    Returns:
        dict: Formatted context dictionary
    """
    # Extract task instance from context
    task_instance = context['task_instance']

    # Extract DAG run information
    dag_run = context.get('dag_run')

    # Extract execution date
    execution_date = context.get('execution_date')

    # Format context with task, DAG, and execution information
    formatted_context = {
        'task_id': task_instance.task_id,
        'dag_id': dag_run.dag_id if dag_run else None,
        'execution_date': execution_date.isoformat() if execution_date else None
    }

    # Return formatted context dictionary
    return formatted_context


class QualitySensor(BaseSensorOperator):
    """Base sensor class for data quality validation monitoring"""

    @apply_defaults
    def __init__(
        self,
        validation_id: str,
        dataset_id: str,
        table_id: str,
        timeout: int = DEFAULT_TIMEOUT,
        poke_interval: int = DEFAULT_POKE_INTERVAL,
        soft_fail: bool = False,
        mode: str = 'reschedule',
        sensor_config: dict = None,
        **kwargs,
    ):
        """Initialize the quality sensor with configuration

        Args:
            validation_id (str): ID of the validation to monitor
            dataset_id (str): ID of the dataset being validated
            table_id (str): ID of the table being validated
            timeout (int): Maximum time to wait for the sensor condition
            poke_interval (int): Time in seconds between pokes
            soft_fail (bool): Whether to fail the task or continue the DAG
            mode (str): Sensor mode ('poke' or 'reschedule')
            sensor_config (dict): Additional sensor configuration
        """
        # Initialize parent BaseSensorOperator with timeout, poke_interval, etc.
        super().__init__(timeout=timeout, poke_interval=poke_interval, soft_fail=soft_fail, mode=mode, **kwargs)

        # Store validation_id, dataset_id, and table_id
        self.validation_id = validation_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        # Initialize sensor_config with defaults and override with provided config
        self.sensor_config = {
            'default_threshold': 0.8,
            'max_retries': 3
        }
        if sensor_config:
            self.sensor_config.update(sensor_config)

        # Create ValidationEngine instance
        self._validation_engine = validation_engine.ValidationEngine(config=self.sensor_config)

        # Create QualityRepository instance
        self._quality_repository = quality_repository.QualityRepository()

        # Log successful initialization
        logger.info(f"Initialized QualitySensor for validation {validation_id}, dataset {dataset_id}, table {table_id}")

    def poke(self, context: dict) -> bool:
        """Check if the sensor condition is met

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if condition is met, False otherwise
        """
        # Log poke attempt
        logger.info(f"Poking for validation completion: {self.validation_id}")

        # Format validation context from Airflow context
        formatted_context = format_validation_context(context)

        # Call _check_condition method (to be implemented by subclasses)
        condition_met = self._check_condition(context)

        # Return result of condition check
        return condition_met

    def _check_condition(self, context: dict) -> bool:
        """Abstract method to check specific condition

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if condition is met, False otherwise
        """
        # Raise NotImplementedError (to be implemented by subclasses)
        raise NotImplementedError("Subclasses must implement _check_condition method")

    def get_validation_result(self, validation_id: str) -> validation_engine.ValidationResult:
        """Get validation result from repository

        Args:
            validation_id (str): ID of the validation

        Returns:
            validation_engine.ValidationResult: Validation result or None if not found
        """
        # Call quality repository to get validation result
        validation_result = self._quality_repository.get_validation(validation_id)

        # Return validation result or None if not found
        return validation_result

    def get_validation_summary(self, validation_id: str) -> validation_engine.ValidationSummary:
        """Get validation summary from repository

        Args:
            validation_id (str): ID of the validation

        Returns:
            validation_engine.ValidationSummary: Validation summary or None if not found
        """
        # Call quality repository to get validation summary
        validation_summary = self._quality_repository.get_validation(validation_id)

        # Return validation summary or None if not found
        return validation_summary


class QualityValidationCompletionSensor(QualitySensor):
    """Sensor that waits for a quality validation to complete"""

    @apply_defaults
    def __init__(
        self,
        validation_id: str,
        dataset_id: str,
        table_id: str,
        timeout: int = DEFAULT_TIMEOUT,
        poke_interval: int = DEFAULT_POKE_INTERVAL,
        soft_fail: bool = False,
        mode: str = 'reschedule',
        sensor_config: dict = None,
        **kwargs,
    ):
        """Initialize the validation completion sensor

        Args:
            validation_id (str): ID of the validation to monitor
            dataset_id (str): ID of the dataset being validated
            table_id (str): ID of the table being validated
            timeout (int): Maximum time to wait for the sensor condition
            poke_interval (int): Time in seconds between pokes
            soft_fail (bool): Whether to fail the task or continue the DAG
            mode (str): Sensor mode ('poke' or 'reschedule')
            sensor_config (dict): Additional sensor configuration
        """
        # Initialize parent QualitySensor with parameters
        super().__init__(validation_id=validation_id, dataset_id=dataset_id, table_id=table_id, timeout=timeout,
                         poke_interval=poke_interval, soft_fail=soft_fail, mode=mode, sensor_config=sensor_config, **kwargs)

        # Log initialization of validation completion sensor
        logger.info(f"Initialized QualityValidationCompletionSensor for validation {validation_id}")

    def _check_condition(self, context: dict) -> bool:
        """Check if validation has completed

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if validation is complete, False otherwise
        """
        # Get validation summary from repository
        summary = self.get_validation_summary(self.validation_id)

        # If summary is None, validation is not complete, return False
        if summary is None:
            logger.info(f"Validation {self.validation_id} not yet started")
            return False

        # If summary exists, check if execution_time > 0 (indicating completion)
        if summary.execution_time > 0:
            # Log validation completion status
            logger.info(f"Validation {self.validation_id} completed with status: {summary.status}")

            # Return True if validation is complete, False otherwise
            return True
        else:
            logger.info(f"Validation {self.validation_id} in progress")
            return False


class QualityScoreSensor(QualitySensor):
    """Sensor that checks if quality score meets a threshold"""

    @apply_defaults
    def __init__(
        self,
        validation_id: str,
        dataset_id: str,
        table_id: str,
        quality_threshold: float = 0.8,
        timeout: int = DEFAULT_TIMEOUT,
        poke_interval: int = DEFAULT_POKE_INTERVAL,
        soft_fail: bool = False,
        mode: str = 'reschedule',
        sensor_config: dict = None,
        **kwargs,
    ):
        """Initialize the quality score sensor

        Args:
            validation_id (str): ID of the validation to monitor
            dataset_id (str): ID of the dataset being validated
            table_id (str): ID of the table being validated
            quality_threshold (float): Minimum acceptable quality score
            timeout (int): Maximum time to wait for the sensor condition
            poke_interval (int): Time in seconds between pokes
            soft_fail (bool): Whether to fail the task or continue the DAG
            mode (str): Sensor mode ('poke' or 'reschedule')
            sensor_config (dict): Additional sensor configuration
        """
        # Initialize parent QualitySensor with parameters
        super().__init__(validation_id=validation_id, dataset_id=dataset_id, table_id=table_id, timeout=timeout,
                         poke_interval=poke_interval, soft_fail=soft_fail, mode=mode, sensor_config=sensor_config, **kwargs)

        # Set quality threshold (default to 0.8 if not provided)
        self._quality_threshold = quality_threshold

        # Create QualityScorer instance
        self._quality_scorer = quality_scorer.QualityScorer()

        # Log initialization of quality score sensor
        logger.info(f"Initialized QualityScoreSensor for validation {validation_id} with threshold {quality_threshold}")

    def _check_condition(self, context: dict) -> bool:
        """Check if quality score meets threshold

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if quality score meets threshold, False otherwise
        """
        # Get validation summary from repository
        summary = self.get_validation_summary(self.validation_id)

        # If summary is None, validation is not complete, return False
        if summary is None:
            logger.info(f"Validation {self.validation_id} not yet complete")
            return False

        # Extract quality score from summary
        score = summary.quality_score.overall_score

        # Compare quality score with threshold
        meets_threshold = score >= self._quality_threshold

        # Log quality score and threshold comparison
        logger.info(f"Validation {self.validation_id} quality score: {score:.2f}, threshold: {self._quality_threshold:.2f}, meets_threshold: {meets_threshold}")

        # Return True if score >= threshold, False otherwise
        return meets_threshold

    def set_quality_threshold(self, threshold: float) -> None:
        """Set the quality threshold for the sensor

        Args:
            threshold (float): New quality threshold
        """
        # Validate threshold is between 0.0 and 1.0
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Quality threshold must be between 0.0 and 1.0")

        # Set _quality_threshold to provided threshold
        self._quality_threshold = threshold

        # Update quality scorer threshold
        self._quality_scorer.set_quality_threshold(threshold)

        # Log threshold update
        logger.info(f"Updated quality threshold to {threshold:.2f}")


class QualityIssueDetectionSensor(QualitySensor):
    """Sensor that detects specific quality issues"""

    @apply_defaults
    def __init__(
        self,
        validation_id: str,
        dataset_id: str,
        table_id: str,
        issue_types: list = None,
        dimension: str = None,
        min_issues: int = 1,
        timeout: int = DEFAULT_TIMEOUT,
        poke_interval: int = DEFAULT_POKE_INTERVAL,
        soft_fail: bool = False,
        mode: str = 'reschedule',
        sensor_config: dict = None,
        **kwargs,
    ):
        """Initialize the quality issue detection sensor

        Args:
            validation_id (str): ID of the validation to monitor
            dataset_id (str): ID of the dataset being validated
            table_id (str): ID of the table being validated
            issue_types (list): List of issue types to filter for
            dimension (str): Quality dimension to filter for
            min_issues (int): Minimum number of issues to detect
            timeout (int): Maximum time to wait for the sensor condition
            poke_interval (int): Time in seconds between pokes
            soft_fail (bool): Whether to fail the task or continue the DAG
            mode (str): Sensor mode ('poke' or 'reschedule')
            sensor_config (dict): Additional sensor configuration
        """
        # Initialize parent QualitySensor with parameters
        super().__init__(validation_id=validation_id, dataset_id=dataset_id, table_id=table_id, timeout=timeout,
                         poke_interval=poke_interval, soft_fail=soft_fail, mode=mode, sensor_config=sensor_config, **kwargs)

        # Set issue_types to filter for specific issues (or None for any)
        self._issue_types = issue_types

        # Set dimension to filter for specific quality dimension (or None for any)
        self._dimension = dimension

        # Set min_issues threshold (default to 1)
        self._min_issues = min_issues

        # Log initialization of quality issue detection sensor
        logger.info(f"Initialized QualityIssueDetectionSensor for validation {validation_id} with issue_types {issue_types}, dimension {dimension}, min_issues {min_issues}")

    def _check_condition(self, context: dict) -> bool:
        """Check if specific quality issues are detected

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if specified issues are detected, False otherwise
        """
        # Get validation summary from repository
        summary = self.get_validation_summary(self.validation_id)

        # If summary is None, validation is not complete, return False
        if summary is None:
            logger.info(f"Validation {self.validation_id} not yet complete")
            return False

        # Get detailed validation results
        results = self.get_detected_issues()

        # Filter results by issue_types and dimension if specified
        filtered_results = results
        if self._issue_types:
            filtered_results = [r for r in filtered_results if r.get('issue_type') in self._issue_types]
        if self._dimension:
            filtered_results = [r for r in filtered_results if r.get('dimension') == self._dimension]

        # Count matching issues
        issue_count = len(filtered_results)

        # Log issue detection results
        logger.info(f"Validation {self.validation_id} detected {issue_count} issues, min_issues: {self._min_issues}")

        # Return True if count >= min_issues, False otherwise
        return issue_count >= self._min_issues

    def get_detected_issues(self) -> list:
        """Get details of detected issues

        Returns:
            list: List of detected issues matching criteria
        """
        # Get validation results from repository
        validation_result = self.get_validation_result(self.validation_id)
        if validation_result is None:
            logger.info(f"Validation {self.validation_id} not yet complete")
            return []

        # Filter results by issue_types and dimension
        results = validation_result.details.get('results', [])
        filtered_results = results
        if self._issue_types:
            filtered_results = [r for r in filtered_results if r.get('issue_type') in self._issue_types]
        if self._dimension:
            filtered_results = [r for r in filtered_results if r.get('dimension') == self._dimension]

        # Return filtered list of issues
        return filtered_results


class SelfHealingQualitySensor(QualitySensor):
    """Quality sensor with self-healing capabilities"""

    @apply_defaults
    def __init__(
        self,
        validation_id: str,
        dataset_id: str,
        table_id: str,
        confidence_threshold: float = 0.75,
        attempt_healing: bool = True,
        healing_config: dict = None,
        timeout: int = DEFAULT_TIMEOUT,
        poke_interval: int = DEFAULT_POKE_INTERVAL,
        soft_fail: bool = False,
        mode: str = 'reschedule',
        sensor_config: dict = None,
        **kwargs,
    ):
        """Initialize the self-healing quality sensor

        Args:
            validation_id (str): ID of the validation to monitor
            dataset_id (str): ID of the dataset being validated
            table_id (str): ID of the table being validated
            confidence_threshold (float): Minimum confidence for self-healing
            attempt_healing (bool): Whether to attempt self-healing
            healing_config (dict): Configuration for self-healing actions
            timeout (int): Maximum time to wait for the sensor condition
            poke_interval (int): Time in seconds between pokes
            soft_fail (bool): Whether to fail the task or continue the DAG
            mode (str): Sensor mode ('poke' or 'reschedule')
            sensor_config (dict): Additional sensor configuration
        """
        # Initialize parent QualitySensor with parameters
        super().__init__(validation_id=validation_id, dataset_id=dataset_id, table_id=table_id, timeout=timeout,
                         poke_interval=poke_interval, soft_fail=soft_fail, mode=mode, sensor_config=sensor_config, **kwargs)

        # Set confidence_threshold (default to 0.75 if not provided)
        self._confidence_threshold = confidence_threshold

        # Set attempt_healing flag (default to True)
        self._attempt_healing = attempt_healing

        # Initialize healing_config with defaults and override with provided config
        self._healing_config = {
            'max_retries': 3
        }
        if healing_config:
            self._healing_config.update(healing_config)

        # Create IssueClassifier instance
        self._issue_classifier = issue_classifier.IssueClassifier()

        # Create DataCorrector instance
        self._data_corrector = data_corrector.DataCorrector()

        # Log initialization of self-healing quality sensor
        logger.info(f"Initialized SelfHealingQualitySensor for validation {validation_id} with confidence_threshold {confidence_threshold}, attempt_healing {attempt_healing}")

    def poke(self, context: dict) -> bool:
        """Check condition and attempt healing if needed

        Args:
            context (dict): Airflow context dictionary

        Returns:
            bool: True if condition is met or healing succeeded, False otherwise
        """
        # Call parent poke method to check condition
        condition_met = super().poke(context)

        # If condition is met, return True
        if condition_met:
            return True

        # If condition is not met and attempt_healing is True:
        if self._attempt_healing:
            # Get validation results
            validation_result = self.get_validation_result(self.validation_id)

            # Classify quality issues
            issues = validation_result.details.get('results', [])
            if not issues:
                logger.info(f"No issues found for validation {self.validation_id}")
                return False

            # Determine if issues are healable
            healing_result = self._apply_healing(issues, context)

            # If healable with confidence above threshold:
            if healing_result and healing_result.get('success'):
                # Trigger revalidation
                new_validation_id = self._trigger_revalidation(healing_result, context)

                # Check if healing resolved issues
                if new_validation_id:
                    # Return True if healing succeeded
                    logger.info(f"Healing succeeded, revalidation triggered with ID: {new_validation_id}")
                    return True
                else:
                    logger.warning(f"Revalidation failed after healing for validation {self.validation_id}")
                    return False
            else:
                logger.warning(f"No healable issues found or healing failed for validation {self.validation_id}")
                return False

        # Return False if condition not met and healing not attempted or failed
        logger.info(f"Condition not met and healing not attempted or failed for validation {self.validation_id}")
        return False

    def _apply_healing(self, issues: list, context: dict) -> dict:
        """Apply healing actions to quality issues

        Args:
            issues (list): List of issues to heal
            context (dict): Airflow context dictionary

        Returns:
            dict: Healing results with actions taken and success status
        """
        # Classify issues using issue classifier
        # Filter issues that meet confidence threshold
        # Group issues by healing action type
        # Apply appropriate healing actions using data corrector
        # Log healing actions and results
        # Return healing results dictionary
        pass

    def _trigger_revalidation(self, healing_result: dict, context: dict) -> str:
        """Trigger revalidation after healing

        Args:
            healing_result (dict): Healing results
            context (dict): Airflow context dictionary

        Returns:
            str: New validation ID
        """
        # Create revalidation request with healing context
        # Submit revalidation request to validation engine
        # Log revalidation trigger
        # Return new validation ID
        pass

    def set_confidence_threshold(self, threshold: float) -> None:
        """Set the confidence threshold for healing actions

        Args:
            threshold (float): New confidence threshold
        """
        # Validate threshold is between 0.0 and 1.0
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Confidence threshold must be between 0.0 and 1.0")

        # Set _confidence_threshold to provided threshold
        self._confidence_threshold = threshold

        # Update issue classifier confidence threshold
        self._issue_classifier.set_confidence_threshold(threshold)

        # Log threshold update
        logger.info(f"Updated confidence threshold to {threshold:.2f}")

    def set_attempt_healing(self, attempt_healing: bool) -> None:
        """Enable or disable healing attempts

        Args:
            attempt_healing (bool): Whether to attempt healing
        """
        # Set _attempt_healing flag to provided value
        self._attempt_healing = attempt_healing

        # Log healing attempt setting update
        logger.info(f"Set attempt_healing to {attempt_healing}")