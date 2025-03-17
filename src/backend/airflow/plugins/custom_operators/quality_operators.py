"""
Custom Airflow operators for data quality validation in the self-healing data pipeline.
These operators integrate with the data quality framework to validate datasets against defined quality rules,
generate quality reports, and enable quality-based workflow branching.
"""

import typing
import os
import json

# Third-party imports with version specification
import yaml  # pyyaml version 6.0+
import pandas as pd  # pandas version 2.0.x
from airflow.models import BaseOperator  # apache-airflow version 2.5.x
from airflow.utils.decorators import apply_defaults  # apache-airflow version 2.5.x
from airflow.exceptions import AirflowException  # apache-airflow version 2.5.x
from airflow.operators.branch_operator import BaseBranchOperator  # apache-airflow version 2.5.x

# Internal module imports
from src.backend import constants  # Import enumerations for healing action types and alert severity levels
from src.backend.utils.logging import logger  # Configure logging for quality operators
from src.backend.quality.engines import validation_engine  # Use validation engine for data quality validation
from src.backend.quality.engines import quality_scorer  # Use quality scoring for validation results
from src.backend.quality.integrations import great_expectations_adapter  # Use Great Expectations for data validation
from src.backend.quality.rules import rule_loader  # Load validation rules from configuration or files
from src.backend.utils.storage import bigquery_client  # Interact with BigQuery for data validation
from src.backend.utils.storage import gcs_client  # Interact with GCS for data validation
from src.backend.utils.monitoring import metric_client  # Report quality metrics to monitoring system
from src.backend.airflow.plugins.hooks import bigquery_hooks  # Use enhanced BigQuery hooks for data access
from src.backend.airflow.plugins.hooks import gcs_hooks  # Use enhanced GCS hooks for data access

# Initialize logger
logger = logger.get_logger(__name__)

@apply_defaults
class DataQualityValidationOperator(BaseOperator):
    """
    Operator for validating data quality in BigQuery datasets.
    """

    template_fields = ('project_id', 'dataset_id', 'table_id', 'validation_rules', 'rules_path', 'validation_config', 'quality_threshold')

    def __init__(
        self,
        task_id: str,
        project_id: str,
        dataset_id: str,
        table_id: str,
        validation_rules: typing.List = None,
        rules_path: str = None,
        validation_config: typing.Dict = None,
        quality_threshold: float = constants.DEFAULT_QUALITY_THRESHOLD,
        fail_on_error: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize the data quality validation operator.

        Args:
            task_id: The task ID for this operator.
            project_id: The Google Cloud project ID.
            dataset_id: The BigQuery dataset ID.
            table_id: The BigQuery table ID.
            validation_rules: A list of validation rules to apply.
            rules_path: Path to a file containing validation rules.
            validation_config: Configuration for the validation engine.
            quality_threshold: Minimum acceptable data quality score.
            fail_on_error: Whether to fail the task if validation fails.
            kwargs: Other keyword arguments for BaseOperator.
        """
        super().__init__(task_id=task_id, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.validation_rules = validation_rules
        self.rules_path = rules_path
        self.validation_config = validation_config
        self.quality_threshold = quality_threshold
        self.fail_on_error = fail_on_error
        self._validation_engine = None
        logger.info(f"DataQualityValidationOperator initialized for {self.project_id}.{self.dataset_id}.{self.table_id}")

    def execute(self, context: typing.Dict) -> typing.Dict:
        """
        Execute data quality validation on BigQuery table.

        Args:
            context: The Airflow context dictionary.

        Returns:
            Validation results.
        """
        logger.info(f"Starting data quality validation for {self.project_id}.{self.dataset_id}.{self.table_id}")

        # Initialize validation engine if not already initialized
        validation_engine = self.get_validation_engine()

        # Load validation rules if not provided directly
        validation_rules = self.get_validation_rules()

        # Get BigQuery table data using EnhancedBigQueryHook
        bq_hook = bigquery_hooks.EnhancedBigQueryHook(gcp_conn_id=self.gcp_conn_id)
        table_data = bq_hook.get_table_data(dataset_id=self.dataset_id, table_id=self.table_id)

        # Execute validation using validation engine
        summary, results = validation_engine.validate(dataset=table_data, rules=validation_rules, validation_config=self.validation_config)

        # Format validation results for XCom
        formatted_results = format_validation_results(summary, results)

        # Report validation metrics to monitoring system
        self.report_validation_metrics(summary)

        # If fail_on_error is True and validation fails quality threshold, raise AirflowException
        if self.fail_on_error and not summary.get('passes_threshold'):
            error_message = f"Data quality validation failed for {self.project_id}.{self.dataset_id}.{self.table_id}. Quality score: {summary.get('quality_score').get('overall_score') if summary.get('quality_score') else 'N/A'}"
            logger.error(error_message)
            raise AirflowException(error_message)

        # Push formatted results to XCom
        context['ti'].xcom_push(key='validation_results', value=formatted_results)

        logger.info(f"Data quality validation completed for {self.project_id}.{self.dataset_id}.{self.table_id}")
        return formatted_results

    def get_validation_engine(self) -> validation_engine.ValidationEngine:
        """
        Get or create validation engine instance.

        Returns:
            Validation engine instance.
        """
        if self._validation_engine is None:
            self._validation_engine = validation_engine.ValidationEngine(config=self.validation_config)
            self._validation_engine.set_quality_threshold(self.quality_threshold)
        return self._validation_engine

    def get_validation_rules(self) -> typing.List:
        """
        Get validation rules from direct input or load from path.

        Returns:
            List of validation rules.
        """
        if self.validation_rules:
            return self.validation_rules
        elif self.rules_path:
            return rule_loader.load_rules_from_file(self.rules_path)
        else:
            raise ValueError("Either validation_rules or rules_path must be provided")

    def report_validation_metrics(self, summary: validation_engine.ValidationSummary) -> None:
        """
        Report validation metrics to monitoring system.

        Args:
            summary: Validation summary.
        """
        mc = metric_client.MetricClient()
        mc.create_gauge_metric(metric_type="data_quality.success_rate", value=summary.success_rate, labels={"dataset_id": self.dataset_id, "table_id": self.table_id})
        mc.create_gauge_metric(metric_type="data_quality.quality_score", value=summary.quality_score.overall_score, labels={"dataset_id": self.dataset_id, "table_id": self.table_id})
        logger.info(f"Reported validation metrics for {self.project_id}.{self.dataset_id}.{self.table_id}")

@apply_defaults
class GCSDataQualityValidationOperator(BaseOperator):
    """
    Operator for validating data quality in GCS files.
    """

    template_fields = ('bucket_name', 'file_path', 'file_format', 'validation_rules', 'rules_path', 'validation_config', 'quality_threshold')

    def __init__(
        self,
        task_id: str,
        bucket_name: str,
        file_path: str,
        file_format: str,
        validation_rules: typing.List = None,
        rules_path: str = None,
        validation_config: typing.Dict = None,
        quality_threshold: float = constants.DEFAULT_QUALITY_THRESHOLD,
        fail_on_error: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize the GCS data quality validation operator.

        Args:
            task_id: The task ID for this operator.
            bucket_name: The Google Cloud Storage bucket name.
            file_path: The path to the file in GCS.
            file_format: The format of the file (e.g., 'csv', 'json', 'parquet', 'avro').
            validation_rules: A list of validation rules to apply.
            rules_path: Path to a file containing validation rules.
            validation_config: Configuration for the validation engine.
            quality_threshold: Minimum acceptable data quality score.
            fail_on_error: Whether to fail the task if validation fails.
            kwargs: Other keyword arguments for BaseOperator.
        """
        super().__init__(task_id=task_id, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_format = file_format
        self.validation_rules = validation_rules
        self.rules_path = rules_path
        self.validation_config = validation_config
        self.quality_threshold = quality_threshold
        self.fail_on_error = fail_on_error
        self._validation_engine = None
        logger.info(f"GCSDataQualityValidationOperator initialized for gs://{self.bucket_name}/{self.file_path}")

    def execute(self, context: typing.Dict) -> typing.Dict:
        """
        Execute data quality validation on GCS file.

        Args:
            context: The Airflow context dictionary.

        Returns:
            Validation results.
        """
        logger.info(f"Starting data quality validation for gs://{self.bucket_name}/{self.file_path}")

        # Initialize validation engine if not already initialized
        validation_engine = self.get_validation_engine()

        # Load validation rules if not provided directly
        validation_rules = self.get_validation_rules()

        # Get GCS file data using EnhancedGCSHook
        gcs_hook = gcs_hooks.EnhancedGCSHook(gcp_conn_id=self.gcp_conn_id)
        file_content = gcs_hook.download(bucket_name=self.bucket_name, object_name=self.file_path)

        # Convert file data to pandas DataFrame based on file_format
        df = self.load_file_to_dataframe(file_content)

        # Execute validation using validation engine
        summary, results = validation_engine.validate(dataset=df, rules=validation_rules, validation_config=self.validation_config)

        # Format validation results for XCom
        formatted_results = format_validation_results(summary, results)

        # Report validation metrics to monitoring system
        self.report_validation_metrics(summary)

        # If fail_on_error is True and validation fails quality threshold, raise AirflowException
        if self.fail_on_error and not summary.get('passes_threshold'):
            error_message = f"Data quality validation failed for gs://{self.bucket_name}/{self.file_path}. Quality score: {summary.get('quality_score').get('overall_score') if summary.get('quality_score') else 'N/A'}"
            logger.error(error_message)
            raise AirflowException(error_message)

        # Push formatted results to XCom
        context['ti'].xcom_push(key='validation_results', value=formatted_results)

        logger.info(f"Data quality validation completed for gs://{self.bucket_name}/{self.file_path}")
        return formatted_results

    def get_validation_engine(self) -> validation_engine.ValidationEngine:
        """
        Get or create validation engine instance.

        Returns:
            Validation engine instance.
        """
        if self._validation_engine is None:
            self._validation_engine = validation_engine.ValidationEngine(config=self.validation_config)
            self._validation_engine.set_quality_threshold(self.quality_threshold)
        return self._validation_engine

    def get_validation_rules(self) -> typing.List:
        """
        Get validation rules from direct input or load from path.

        Returns:
            List of validation rules.
        """
        if self.validation_rules:
            return self.validation_rules
        elif self.rules_path:
            return rule_loader.load_rules_from_file(self.rules_path)
        else:
            raise ValueError("Either validation_rules or rules_path must be provided")

    def load_file_to_dataframe(self, file_content: str) -> pd.DataFrame:
        """
        Load GCS file to pandas DataFrame based on format.

        Args:
            file_content: The content of the file as a string.

        Returns:
            DataFrame with file data.
        """
        try:
            if self.file_format == 'csv':
                df = pd.read_csv(io.StringIO(file_content))  # io is part of standard library
            elif self.file_format == 'json':
                df = pd.read_json(io.StringIO(file_content))
            elif self.file_format == 'parquet':
                df = pd.read_parquet(io.BytesIO(file_content))
            elif self.file_format == 'avro':
                # Implement custom avro reader
                raise NotImplementedError("Avro format is not yet supported")
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            return df
        except Exception as e:
            logger.error(f"Error loading file to DataFrame: {e}")
            raise

    def report_validation_metrics(self, summary: validation_engine.ValidationSummary) -> None:
        """
        Report validation metrics to monitoring system.

        Args:
            summary: Validation summary.
        """
        mc = metric_client.MetricClient()
        mc.create_gauge_metric(metric_type="data_quality.success_rate", value=summary.success_rate, labels={"bucket_name": self.bucket_name, "file_path": self.file_path})
        mc.create_gauge_metric(metric_type="data_quality.quality_score", value=summary.quality_score.overall_score, labels={"bucket_name": self.bucket_name, "file_path": self.file_path})
        logger.info(f"Reported validation metrics for gs://{self.bucket_name}/{self.file_path}")

@apply_defaults
class DataQualityReportingOperator(BaseOperator):
    """
    Operator for generating data quality reports from validation results.
    """

    template_fields = ('validation_task_id', 'report_format', 'output_path', 'notification_channels')

    def __init__(
        self,
        task_id: str,
        validation_task_id: str,
        report_format: str = 'json',
        output_path: str = None,
        send_notification: bool = False,
        notification_channels: typing.List[str] = None,
        **kwargs,
    ) -> None:
        """
        Initialize the data quality reporting operator.

        Args:
            task_id: The task ID for this operator.
            validation_task_id: The task ID of the validation operator.
            report_format: The format of the report (e.g., 'json', 'html', 'markdown').
            output_path: The path to save the report to.
            send_notification: Whether to send a notification with the report.
            notification_channels: A list of notification channels to send the report to.
            kwargs: Other keyword arguments for BaseOperator.
        """
        super().__init__(task_id=task_id, **kwargs)
        self.validation_task_id = validation_task_id
        self.report_format = report_format
        self.output_path = output_path
        self.send_notification = send_notification
        self.notification_channels = notification_channels
        logger.info(f"DataQualityReportingOperator initialized for validation task {self.validation_task_id}")

    def execute(self, context: typing.Dict) -> typing.Dict:
        """
        Generate data quality report from validation results.

        Args:
            context: The Airflow context dictionary.

        Returns:
            Report generation results.
        """
        logger.info(f"Starting data quality report generation for validation task {self.validation_task_id}")

        # Get validation results from XCom using validation_task_id
        xcom_value = context['ti'].xcom_pull(task_ids=self.validation_task_id, key='validation_results')

        # Parse validation results
        summary, results = parse_validation_results(xcom_value)

        # Generate report in specified format
        report_content = self.generate_report(summary, results)

        # Save report to output_path if provided
        if self.output_path:
            output_file_path = self.save_report(report_content)
        else:
            output_file_path = None

        # If send_notification is True, send report to notification channels
        if self.send_notification:
            notification_results = self.send_notifications(summary, output_file_path)
        else:
            notification_results = None

        report_results = {
            "report_format": self.report_format,
            "output_path": output_file_path,
            "notification_results": notification_results
        }

        logger.info(f"Data quality report generation completed for validation task {self.validation_task_id}")
        return report_results

    def generate_report(self, summary: validation_engine.ValidationSummary, results: typing.List) -> str:
        """
        Generate report in specified format.

        Args:
            summary: Validation summary.
            results: Validation results.

        Returns:
            Generated report content.
        """
        if self.report_format == 'json':
            report_content = json.dumps(format_validation_results(summary, results), indent=2)
        elif self.report_format == 'html':
            # Implement HTML report generation
            raise NotImplementedError("HTML report generation is not yet implemented")
        elif self.report_format == 'markdown':
            # Implement Markdown report generation
            raise NotImplementedError("Markdown report generation is not yet implemented")
        else:
            raise ValueError(f"Unsupported report format: {self.report_format}")

        return report_content

    def save_report(self, report_content: str) -> str:
        """
        Save report to specified output path.

        Args:
            report_content: The content of the report.

        Returns:
            Output file path.
        """
        if self.output_path.startswith('gs://'):
            # Save to GCS
            gcs_hook = gcs_hooks.EnhancedGCSHook(gcp_conn_id=self.gcp_conn_id)
            gcs_hook.upload(bucket_name=self.bucket_name, object_name=self.file_path, data=report_content)
            output_file_path = f"gs://{self.bucket_name}/{self.file_path}"
        else:
            # Save to local file
            with open(self.output_path, 'w') as f:
                f.write(report_content)
            output_file_path = self.output_path

        logger.info(f"Saved report to {output_file_path}")
        return output_file_path

    def send_notifications(self, summary: validation_engine.ValidationSummary, report_path: str) -> typing.Dict:
        """
        Send report notifications to specified channels.

        Args:
            summary: Validation summary.
            report_path: Path to the report.

        Returns:
            Notification results.
        """
        notification_results = {}
        for channel in self.notification_channels:
            if channel == 'email':
                # Implement email notification
                raise NotImplementedError("Email notification is not yet implemented")
            elif channel == 'teams':
                # Implement Microsoft Teams notification
                raise NotImplementedError("Microsoft Teams notification is not yet implemented")
            elif channel == 'slack':
                # Implement Slack notification
                raise NotImplementedError("Slack notification is not yet implemented")
            else:
                logger.warning(f"Unsupported notification channel: {channel}")
                notification_results[channel] = "Unsupported channel"

        return notification_results

@apply_defaults
class QualityBasedBranchOperator(BaseBranchOperator):
    """
    Operator for branching based on data quality validation results.
    """

    template_fields = ('validation_task_id', 'quality_threshold', 'pass_task_id', 'fail_task_id', 'healing_task_id')

    def __init__(
        self,
        task_id: str,
        validation_task_id: str,
        quality_threshold: float = constants.DEFAULT_QUALITY_THRESHOLD,
        pass_task_id: str = None,
        fail_task_id: str = None,
        healing_task_id: str = None,
        **kwargs,
    ) -> None:
        """
        Initialize the quality-based branch operator.

        Args:
            task_id: The task ID for this operator.
            validation_task_id: The task ID of the validation operator.
            quality_threshold: Minimum acceptable data quality score.
            pass_task_id: The task ID to execute if validation passes.
            fail_task_id: The task ID to execute if validation fails.
            healing_task_id: The task ID to execute if healing is possible.
            kwargs: Other keyword arguments for BaseBranchOperator.
        """
        super().__init__(task_id=task_id, **kwargs)
        self.validation_task_id = validation_task_id
        self.quality_threshold = quality_threshold
        self.pass_task_id = pass_task_id
        self.fail_task_id = fail_task_id
        self.healing_task_id = healing_task_id
        logger.info(f"QualityBasedBranchOperator initialized for validation task {self.validation_task_id}")

    def choose_branch(self, context: typing.Dict) -> str:
        """
        Choose the next task based on validation results.

        Args:
            context: The Airflow context dictionary.

        Returns:
            Next task ID to execute.
        """
        logger.info(f"Choosing branch based on validation results from task {self.validation_task_id}")

        # Get validation results from XCom using validation_task_id
        xcom_value = context['ti'].xcom_pull(task_ids=self.validation_task_id, key='validation_results')

        # Parse validation results
        summary, results = parse_validation_results(xcom_value)

        # Check if validation passes quality threshold
        if summary.get('passes_threshold'):
            logger.info(f"Validation passed quality threshold. Branching to task {self.pass_task_id}")
            return self.pass_task_id
        else:
            # Check if healing_task_id is provided and healing is possible
            if self.healing_task_id and self.is_healing_possible(summary, results):
                logger.info(f"Validation failed but healing is possible. Branching to task {self.healing_task_id}")
                return self.healing_task_id
            else:
                logger.info(f"Validation failed and healing is not possible. Branching to task {self.fail_task_id}")
                return self.fail_task_id

    def is_healing_possible(self, summary: validation_engine.ValidationSummary, results: typing.List) -> bool:
        """
        Determine if self-healing is possible for the validation issues.

        Args:
            summary: Validation summary.
            results: Validation results.

        Returns:
            True if healing is possible.
        """
        # Analyze failed validation results
        # Check if failed validations are of types that can be healed
        # Check if failure patterns match known healable patterns
        # Return True if healing is possible, False otherwise
        return False

def load_validation_rules(rules_path: str = None, config: typing.Dict = None) -> typing.List:
    """
    Load validation rules from a file or configuration.

    Args:
        rules_path: Path to the rule file.
        config: Configuration dictionary.

    Returns:
        List of validation rules.
    """
    if rules_path:
        rules = rule_loader.load_rules_from_file(rules_path)
    elif config:
        rules = rule_loader.load_rules_from_config(config)
    else:
        raise ValueError("Either rules_path or config must be provided")

    logger.info(f"Loaded {len(rules)} validation rules")
    return rules

def format_validation_results(summary: validation_engine.ValidationSummary, results: typing.List) -> typing.Dict:
    """
    Format validation results for Airflow XCom.

    Args:
        summary: Validation summary.
        results: Validation results.

    Returns:
        Formatted validation results.
    """
    summary_dict = summary.to_dict() if summary else {}
    results_list = [result.to_dict() for result in results] if results else []
    formatted_results = {
        'summary': summary_dict,
        'results': results_list
    }
    return formatted_results

def parse_validation_results(xcom_value: typing.Dict) -> typing.Tuple[validation_engine.ValidationSummary, typing.List[validation_engine.ValidationResult]]:
    """
    Parse validation results from Airflow XCom.

    Args:
        xcom_value: XCom value.

    Returns:
        Tuple of (ValidationSummary, list[ValidationResult]).
    """
    summary_dict = xcom_value.get('summary', {})
    results_list = xcom_value.get('results', [])

    summary = validation_engine.ValidationSummary.from_dict(summary_dict) if summary_dict else None
    results = [validation_engine.ValidationResult.from_dict(result) for result in results_list] if results_list else []

    return summary, results