"""
Repository class for managing pipeline and task execution data in BigQuery.
Provides methods for creating, retrieving, updating, and querying execution records,
supporting the self-healing data pipeline's execution tracking, monitoring, and analysis capabilities.
"""

import typing
import datetime
import pandas  # version 2.0.x

from ...constants import (  # src/backend/constants.py
    PipelineStatus,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCESS,
    TASK_STATUS_FAILED
)
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from ...utils.storage.bigquery_client import BigQueryClient  # src/backend/utils/storage/bigquery_client.py
from ..models.pipeline_execution import (  # src/backend/db/models/pipeline_execution.py
    PipelineExecution,
    PIPELINE_EXECUTION_TABLE_NAME,
    get_pipeline_execution_table_schema
)
from ..models.task_execution import (  # src/backend/db/models/task_execution.py
    TaskExecution,
    TASK_EXECUTION_TABLE_NAME,
    get_task_execution_table_schema
)
from ...utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py

# Initialize logger
logger = get_logger(__name__)

# Default dataset for BigQuery tables
DEFAULT_DATASET = "pipeline_data"


class ExecutionRepository:
    """
    Repository for managing pipeline and task execution data in BigQuery.
    """

    def __init__(self, bq_client: BigQueryClient, dataset_id: str = None):
        """
        Initialize the execution repository with BigQuery client.

        Args:
            bq_client: BigQuery client for database interactions
            dataset_id: Dataset ID for BigQuery tables, defaults to DEFAULT_DATASET
        """
        self._bq_client = bq_client
        self._dataset_id = dataset_id or DEFAULT_DATASET
        self._ensure_dataset_exists()
        self._ensure_tables_exist()
        logger.info("Execution repository initialized")

    def _ensure_dataset_exists(self) -> None:
        """
        Ensure the BigQuery dataset exists, creating it if necessary.
        """
        if not self._bq_client.dataset_exists(self._dataset_id):
            self._bq_client.create_dataset(self._dataset_id)
            logger.info(f"Created BigQuery dataset: {self._dataset_id}")
        else:
            logger.debug(f"BigQuery dataset already exists: {self._dataset_id}")

    def _ensure_tables_exist(self) -> None:
        """
        Ensure the required BigQuery tables exist, creating them if necessary.
        """
        # Check and create pipeline execution table
        if not self._bq_client.table_exists(self._dataset_id, PIPELINE_EXECUTION_TABLE_NAME):
            schema = get_pipeline_execution_table_schema()
            self._bq_client.create_table(self._dataset_id, PIPELINE_EXECUTION_TABLE_NAME, schema)
            logger.info(f"Created table: {PIPELINE_EXECUTION_TABLE_NAME}")
        else:
            logger.debug(f"Table already exists: {PIPELINE_EXECUTION_TABLE_NAME}")

        # Check and create task execution table
        if not self._bq_client.table_exists(self._dataset_id, TASK_EXECUTION_TABLE_NAME):
            schema = get_task_execution_table_schema()
            self._bq_client.create_table(self._dataset_id, TASK_EXECUTION_TABLE_NAME, schema)
            logger.info(f"Created table: {TASK_EXECUTION_TABLE_NAME}")
        else:
            logger.debug(f"Table already exists: {TASK_EXECUTION_TABLE_NAME}")

    @retry(max_attempts=3)
    def create_pipeline_execution(self, execution: PipelineExecution) -> PipelineExecution:
        """
        Create a new pipeline execution record in BigQuery.

        Args:
            execution: The PipelineExecution object to create

        Returns:
            The created pipeline execution
        """
        row = execution.to_bigquery_row()
        self._bq_client.load_table_from_dataframe(
            self._dataset_id, PIPELINE_EXECUTION_TABLE_NAME, [row]
        )
        logger.info(f"Created pipeline execution record: {execution.execution_id}")
        return execution

    @retry(max_attempts=3)
    def get_pipeline_execution(self, execution_id: str) -> typing.Optional[PipelineExecution]:
        """
        Retrieve a pipeline execution by its ID.

        Args:
            execution_id: The ID of the pipeline execution to retrieve

        Returns:
            The retrieved pipeline execution or None if not found
        """
        query = f"""
            SELECT *
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE execution_id = '{execution_id}'
        """
        result = self._bq_client.execute_query(query)
        if not result:
            return None
        return PipelineExecution.from_bigquery_row(result[0])

    @retry(max_attempts=3)
    def update_pipeline_execution(self, execution: PipelineExecution) -> PipelineExecution:
        """
        Update an existing pipeline execution record in BigQuery.

        Args:
            execution: The PipelineExecution object to update

        Returns:
            The updated pipeline execution
        """
        row = execution.to_bigquery_row()
        merge_query = f"""
            MERGE `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}` AS target
            USING (SELECT '{execution.execution_id}' as execution_id) AS source
            ON target.execution_id = source.execution_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.pipeline_id = '{execution.pipeline_id}',
                    target.dag_run_id = '{execution.dag_run_id}',
                    target.status = '{execution.status}',
                    target.start_time = TIMESTAMP('{execution.start_time.isoformat() if execution.start_time else None}'),
                    target.end_time = TIMESTAMP('{execution.end_time.isoformat() if execution.end_time else None}'),
                    target.execution_params = '{row["execution_params"]}',
                    target.execution_metrics = '{row["execution_metrics"]}',
                    target.error_details = '{row["error_details"]}',
                    target.retry_count = {execution.retry_count},
                    target.retry_history = '{row["retry_history"]}',
                    target.self_healing_attempts = '{row["self_healing_attempts"]}',
                    target.metadata = '{row["metadata"]}',
                    target.records_processed = {execution.records_processed},
                    target.records_failed = {execution.records_failed},
                    target.quality_score = {execution.quality_score}
            WHEN NOT MATCHED THEN
                INSERT (execution_id, pipeline_id, dag_run_id, status, start_time, end_time,
                        execution_params, execution_metrics, error_details, retry_count, retry_history,
                        self_healing_attempts, metadata, records_processed, records_failed, quality_score)
                VALUES ('{execution.execution_id}', '{execution.pipeline_id}', '{execution.dag_run_id}', '{execution.status}',
                        TIMESTAMP('{execution.start_time.isoformat() if execution.start_time else None}'), TIMESTAMP('{execution.end_time.isoformat() if execution.end_time else None}'),
                        '{row["execution_params"]}', '{row["execution_metrics"]}', '{row["error_details"]}', {execution.retry_count}, '{row["retry_history"]}',
                        '{row["self_healing_attempts"]}', '{row["metadata"]}', {execution.records_processed}, {execution.records_failed}, {execution.quality_score})
        """
        self._bq_client.execute_query(merge_query)
        logger.info(f"Updated pipeline execution record: {execution.execution_id}")
        return execution

    @retry(max_attempts=3)
    def list_pipeline_executions(
        self,
        pipeline_id: str = None,
        status: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None,
        limit: int = 100,
        offset: int = 0
    ) -> typing.List[PipelineExecution]:
        """
        List pipeline executions with optional filtering.

        Args:
            pipeline_id: Filter by pipeline ID
            status: Filter by status
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to
            limit: Maximum number of results to return
            offset: Offset for pagination

        Returns:
            List of PipelineExecution objects
        """
        query = f"""
            SELECT *
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if pipeline_id:
            query += f" AND pipeline_id = '{pipeline_id}'"
        if status:
            query += f" AND status = '{status}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"
        query += f" ORDER BY start_time DESC LIMIT {limit} OFFSET {offset}"

        result = self._bq_client.execute_query(query)
        return [PipelineExecution.from_bigquery_row(row) for row in result]

    @retry(max_attempts=3)
    def count_pipeline_executions(
        self,
        pipeline_id: str = None,
        status: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None
    ) -> int:
        """
        Count pipeline executions with optional filtering.

        Args:
            pipeline_id: Filter by pipeline ID
            status: Filter by status
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to

        Returns:
            Count of matching pipeline executions
        """
        query = f"""
            SELECT COUNT(*) as count
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if pipeline_id:
            query += f" AND pipeline_id = '{pipeline_id}'"
        if status:
            query += f" AND status = '{status}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"

        result = self._bq_client.execute_query(query)
        return int(result[0]["count"]) if result else 0

    @retry(max_attempts=3)
    def delete_pipeline_execution(self, execution_id: str) -> bool:
        """
        Delete a pipeline execution and its associated task executions.

        Args:
            execution_id: The ID of the pipeline execution to delete

        Returns:
            True if successful, False otherwise
        """
        # First, delete associated task executions
        self.delete_task_executions(execution_id)

        # Then, delete the pipeline execution
        query = f"""
            DELETE FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE execution_id = '{execution_id}'
        """
        self._bq_client.execute_query(query)
        logger.info(f"Deleted pipeline execution record: {execution_id}")
        return True

    @retry(max_attempts=3)
    def create_task_execution(self, task_execution: TaskExecution) -> TaskExecution:
        """
        Create a new task execution record in BigQuery.

        Args:
            task_execution: The TaskExecution object to create

        Returns:
            The created task execution
        """
        row = task_execution.to_bigquery_row()
        self._bq_client.load_table_from_dataframe(
            self._dataset_id, TASK_EXECUTION_TABLE_NAME, [row]
        )
        logger.info(f"Created task execution record: {task_execution.task_execution_id}")
        return task_execution

    @retry(max_attempts=3)
    def get_task_execution(self, task_execution_id: str) -> typing.Optional[TaskExecution]:
        """
        Retrieve a task execution by its ID.

        Args:
            task_execution_id: The ID of the task execution to retrieve

        Returns:
            The retrieved task execution or None if not found
        """
        query = f"""
            SELECT *
            FROM `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}`
            WHERE task_execution_id = '{task_execution_id}'
        """
        result = self._bq_client.execute_query(query)
        if not result:
            return None
        return TaskExecution.from_bigquery_row(result[0])

    @retry(max_attempts=3)
    def update_task_execution(self, task_execution: TaskExecution) -> TaskExecution:
        """
        Update an existing task execution record in BigQuery.

        Args:
            task_execution: The TaskExecution object to update

        Returns:
            The updated task execution
        """
        row = task_execution.to_bigquery_row()
        merge_query = f"""
            MERGE `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}` AS target
            USING (SELECT '{task_execution.task_execution_id}' as task_execution_id) AS source
            ON target.task_execution_id = source.task_execution_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.execution_id = '{task_execution.execution_id}',
                    target.task_id = '{task_execution.task_id}',
                    target.task_type = '{task_execution.task_type}',
                    target.status = '{task_execution.status}',
                    target.start_time = TIMESTAMP('{task_execution.start_time.isoformat() if task_execution.start_time else None}'),
                    target.end_time = TIMESTAMP('{task_execution.end_time.isoformat() if task_execution.end_time else None}'),
                    target.task_params = '{row["task_params"]}',
                    target.task_metrics = '{row["task_metrics"]}',
                    target.error_details = '{row["error_details"]}',
                    target.retry_count = {task_execution.retry_count},
                    target.retry_history = '{row["retry_history"]}',
                    target.metadata = '{row["metadata"]}',
                    target.records_processed = {task_execution.records_processed},
                    target.records_failed = {task_execution.records_failed},
                    target.duration_seconds = {task_execution.get_duration()}
            WHEN NOT MATCHED THEN
                INSERT (task_execution_id, execution_id, task_id, task_type, status, start_time, end_time,
                        task_params, task_metrics, error_details, retry_count, retry_history, metadata, records_processed, records_failed, duration_seconds)
                VALUES ('{task_execution.task_execution_id}', '{task_execution.execution_id}', '{task_execution.task_id}', '{task_execution.task_type}', '{task_execution.status}',
                        TIMESTAMP('{task_execution.start_time.isoformat() if task_execution.start_time else None}'), TIMESTAMP('{task_execution.end_time.isoformat() if task_execution.end_time else None}'),
                        '{row["task_params"]}', '{row["task_metrics"]}', '{row["error_details"]}', {task_execution.retry_count}, '{row["retry_history"]}', '{row["metadata"]}', {task_execution.records_processed}, {task_execution.records_failed}, {task_execution.get_duration()})
        """
        self._bq_client.execute_query(merge_query)
        logger.info(f"Updated task execution record: {task_execution.task_execution_id}")
        return task_execution

    @retry(max_attempts=3)
    def list_task_executions(
        self,
        execution_id: str = None,
        task_id: str = None,
        status: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None,
        limit: int = 100,
        offset: int = 0
    ) -> typing.List[TaskExecution]:
        """
        List task executions with optional filtering.

        Args:
            execution_id: Filter by execution ID
            task_id: Filter by task ID
            status: Filter by status
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to
            limit: Maximum number of results to return
            offset: Offset for pagination

        Returns:
            List of TaskExecution objects
        """
        query = f"""
            SELECT *
            FROM `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if execution_id:
            query += f" AND execution_id = '{execution_id}'"
        if task_id:
            query += f" AND task_id = '{task_id}'"
        if status:
            query += f" AND status = '{status}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"
        query += f" ORDER BY start_time DESC LIMIT {limit} OFFSET {offset}"

        result = self._bq_client.execute_query(query)
        return [TaskExecution.from_bigquery_row(row) for row in result]

    @retry(max_attempts=3)
    def count_task_executions(
        self,
        execution_id: str = None,
        task_id: str = None,
        status: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None
    ) -> int:
        """
        Count task executions with optional filtering.

        Args:
            execution_id: Filter by execution ID
            task_id: Filter by task ID
            status: Filter by status
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to

        Returns:
            Count of matching task executions
        """
        query = f"""
            SELECT COUNT(*) as count
            FROM `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if execution_id:
            query += f" AND execution_id = '{execution_id}'"
        if task_id:
            query += f" AND task_id = '{task_id}'"
        if status:
            query += f" AND status = '{status}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"

        result = self._bq_client.execute_query(query)
        return int(result[0]["count"]) if result else 0

    @retry(max_attempts=3)
    def delete_task_executions(self, execution_id: str) -> bool:
        """
        Delete task executions for a specific pipeline execution.

        Args:
            execution_id: The ID of the pipeline execution

        Returns:
            True if successful, False otherwise
        """
        query = f"""
            DELETE FROM `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}`
            WHERE execution_id = '{execution_id}'
        """
        self._bq_client.execute_query(query)
        logger.info(f"Deleted task executions for execution: {execution_id}")
        return True

    @retry(max_attempts=3)
    def get_pipeline_execution_metrics(
        self,
        pipeline_id: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None
    ) -> dict:
        """
        Get aggregated metrics for pipeline executions.

        Args:
            pipeline_id: Filter by pipeline ID
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to

        Returns:
            Dictionary of aggregated metrics
        """
        query = f"""
            SELECT
                COUNT(*) as total_executions,
                COUNTIF(status = '{TASK_STATUS_SUCCESS}') as successful_executions,
                COUNTIF(status = '{TASK_STATUS_FAILED}') as failed_executions,
                AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) as avg_duration_seconds,
                (COUNTIF(status = '{TASK_STATUS_SUCCESS}') / COUNT(*)) * 100 as success_rate
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if pipeline_id:
            query += f" AND pipeline_id = '{pipeline_id}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"

        result = self._bq_client.execute_query(query)
        if result:
            return result[0]
        else:
            return {}

    @retry(max_attempts=3)
    def get_task_execution_metrics(
        self,
        execution_id: str = None,
        task_id: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None
    ) -> dict:
        """
        Get aggregated metrics for task executions.

        Args:
            execution_id: Filter by execution ID
            task_id: Filter by task ID
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to

        Returns:
            Dictionary of aggregated metrics
        """
        query = f"""
            SELECT
                COUNT(*) as total_executions,
                COUNTIF(status = '{TASK_STATUS_SUCCESS}') as successful_executions,
                COUNTIF(status = '{TASK_STATUS_FAILED}') as failed_executions,
                AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND)) as avg_duration_seconds,
                AVG(retry_count) as avg_retry_count
            FROM `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}`
            WHERE 1=1
        """
        if execution_id:
            query += f" AND execution_id = '{execution_id}'"
        if task_id:
            query += f" AND task_id = '{task_id}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"

        result = self._bq_client.execute_query(query)
        if result:
            return result[0]
        else:
            return {}

    @retry(max_attempts=3)
    def get_pipeline_execution_history(self, pipeline_id: str, limit: int = 100) -> pandas.DataFrame:  # pandas version 2.0.x
        """
        Get historical execution data for a pipeline.

        Args:
            pipeline_id: ID of the pipeline
            limit: Maximum number of results to return

        Returns:
            DataFrame with historical execution data
        """
        query = f"""
            SELECT
                execution_id,
                status,
                start_time,
                end_time,
                TIMESTAMP_DIFF(end_time, start_time, SECOND) as duration_seconds
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            WHERE pipeline_id = '{pipeline_id}'
            ORDER BY start_time DESC
            LIMIT {limit}
        """
        df = self._bq_client.execute_query_to_dataframe(query)
        return df

    @retry(max_attempts=3)
    def get_failed_task_analysis(
        self,
        pipeline_id: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None,
        limit: int = 100
    ) -> pandas.DataFrame:  # pandas version 2.0.x
        """
        Analyze failed tasks to identify patterns and common issues.

        Args:
            pipeline_id: Filter by pipeline ID
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to
            limit: Maximum number of results to return

        Returns:
            DataFrame with failed task analysis
        """
        query = f"""
            SELECT
                te.task_id,
                te.task_type,
                te.error_details.type as error_type,
                COUNT(*) as failure_frequency,
                AVG(TIMESTAMP_DIFF(te.end_time, te.start_time, SECOND)) as avg_duration_seconds,
                AVG(te.retry_count) as avg_retry_count,
                (COUNTIF(te.status = '{TASK_STATUS_SUCCESS}') / COUNT(*)) * 100 as retry_success_rate
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}` pe
            JOIN `{self._dataset_id}.{TASK_EXECUTION_TABLE_NAME}` te ON pe.execution_id = te.execution_id
            WHERE te.status = '{TASK_STATUS_FAILED}'
        """
        if pipeline_id:
            query += f" AND pe.pipeline_id = '{pipeline_id}'"
        if start_time_from:
            query += f" AND pe.start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND pe.start_time <= TIMESTAMP('{start_time_to.isoformat()}')"
        query += """
            GROUP BY te.task_id, te.task_type, error_type
            ORDER BY failure_frequency DESC
            LIMIT """ + str(limit)
        df = self._bq_client.execute_query_to_dataframe(query)
        return df

    @retry(max_attempts=3)
    def get_self_healing_effectiveness(
        self,
        pipeline_id: str = None,
        start_time_from: datetime = None,
        start_time_to: datetime = None
    ) -> dict:
        """
        Analyze effectiveness of self-healing attempts.

        Args:
            pipeline_id: Filter by pipeline ID
            start_time_from: Filter by start time from
            start_time_to: Filter by start time to

        Returns:
            Dictionary with self-healing effectiveness metrics
        """
        query = f"""
            SELECT
                COUNT(*) as total_attempts,
                COUNTIF(JSON_EXTRACT_SCALAR(attempts, '$[SAFE_OFFSET(0)].successful') = 'true') as successful_attempts,
                AVG(ARRAY_LENGTH(JSON_EXTRACT_ARRAY(attempts, '$')) ) as avg_attempts_per_execution,
                JSON_EXTRACT_SCALAR(attempts, '$[SAFE_OFFSET(0)].issue_type') as issue_type,
                JSON_EXTRACT_SCALAR(attempts, '$[SAFE_OFFSET(0)].action_taken') as action_taken
            FROM `{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
            CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(self_healing_attempts, '$')) as attempts
            WHERE 1=1
        """
        if pipeline_id:
            query += f" AND pipeline_id = '{pipeline_id}'"
        if start_time_from:
            query += f" AND start_time >= TIMESTAMP('{start_time_from.isoformat()}')"
        if start_time_to:
            query += f" AND start_time <= TIMESTAMP('{start_time_to.isoformat()}')"
        query += " GROUP BY issue_type, action_taken"

        result = self._bq_client.execute_query(query)
        if result:
            return result[0]
        else:
            return {}