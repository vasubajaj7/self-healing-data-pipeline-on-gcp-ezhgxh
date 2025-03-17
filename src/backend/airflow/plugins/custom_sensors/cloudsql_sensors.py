"""
Custom Airflow sensors for monitoring Cloud SQL databases. These sensors check for table existence, data availability, and specific values in Cloud SQL tables to trigger downstream tasks in data pipelines, with built-in self-healing capabilities.
"""
import typing

# Third-party imports with version specification
from typing import Dict, Optional
from airflow.sensors.base import BaseSensorOperator  # apache-airflow 2.5.x
from airflow.utils.decorators import apply_defaults  # apache-airflow 2.5.x
from airflow.exceptions import AirflowException  # apache-airflow 2.5.x
from sqlalchemy.exc import SQLAlchemyError  # sqlalchemy 2.0.x

# Internal module imports
from src.backend.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR, DEFAULT_CONFIDENCE_THRESHOLD  # Import constants for Cloud SQL configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for Cloud SQL sensors
from src.backend.utils.retry.retry_decorator import retry  # Apply retry logic to Cloud SQL operations
from src.backend.airflow.plugins.hooks.cloudsql_hooks import EnhancedCloudSQLHook, SelfHealingCloudSQLHook  # Use Cloud SQL hooks for connecting to Cloud SQL databases
from src.backend.utils.errors.error_types import PipelineError, ConnectionError, DataContentError  # Use custom error types for Cloud SQL operations

# Initialize logger
logger = get_logger(__name__)


class CloudSQLSensor(BaseSensorOperator):
    """
    Base sensor class for monitoring Cloud SQL databases
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the Cloud SQL sensor with connection and database details

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.instance_connection_name = instance_connection_name
        self.db_type = db_type
        self.hook: Optional[EnhancedCloudSQLHook] = None

    def poke(self, context: Dict) -> bool:
        """
        Abstract method to be implemented by subclasses to check Cloud SQL condition

        :param context: Airflow context dictionary
        :return: True if condition is met, False otherwise
        """
        raise NotImplementedError("Subclasses must implement the poke method")

    def get_hook(self) -> EnhancedCloudSQLHook:
        """
        Get or create a Cloud SQL hook instance

        :return: Cloud SQL hook instance
        """
        if self.hook is None:
            self.hook = EnhancedCloudSQLHook(
                gcp_conn_id=self.conn_id,
                instance_connection_name=self.instance_connection_name,
                database=self.database,
                db_type=self.db_type,
                hook_params={}
            )
        return self.hook


class CloudSQLTableExistenceSensor(CloudSQLSensor):
    """
    Sensor that checks for the existence of a table in Cloud SQL
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name', 'table_name')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        table_name: str,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the Cloud SQL table existence sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param table_name: Table name to check for existence
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.table_name = table_name

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: Dict) -> bool:
        """
        Check if the table exists in Cloud SQL

        :param context: Airflow context dictionary
        :return: True if table exists, False otherwise
        """
        hook = self.get_hook()
        try:
            tables = hook.list_tables()
            table_exists = self.table_name in tables
            logger.info(f"Checking for table {self.table_name}: {'Exists' if table_exists else 'Does not exist'}")
            return table_exists
        except (PipelineError, SQLAlchemyError) as e:
            logger.error(f"Error while checking for table existence: {e}")
            return False


class CloudSQLTableDataAvailabilitySensor(CloudSQLSensor):
    """
    Sensor that checks for data availability in a Cloud SQL table
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name', 'table_name', 'where_clause')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        table_name: str,
        where_clause: Optional[str] = None,
        min_rows: int = 1,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the Cloud SQL data availability sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param table_name: Table name to check for data availability
        :param where_clause: WHERE clause to filter data
        :param min_rows: Minimum number of rows required for data availability
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.table_name = table_name
        self.where_clause = where_clause
        self.min_rows = min_rows if min_rows is not None else 1

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: Dict) -> bool:
        """
        Check if data is available in the Cloud SQL table

        :param context: Airflow context dictionary
        :return: True if data is available, False otherwise
        """
        hook = self.get_hook()
        try:
            record_count = hook.get_record_count(table_name=self.table_name, where_clause=self.where_clause)
            data_available = record_count >= self.min_rows
            logger.info(f"Checking data availability in table {self.table_name}: {'Available' if data_available else 'Not available'}")
            return data_available
        except (PipelineError, SQLAlchemyError) as e:
            logger.error(f"Error while checking data availability: {e}")
            return False


class CloudSQLTableValueSensor(CloudSQLSensor):
    """
    Sensor that checks for specific values in a Cloud SQL table
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name', 'table_name', 'column_name', 'where_clause', 'value_check')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        table_name: str,
        column_name: str,
        where_clause: Optional[str] = None,
        value_check: str = None,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the Cloud SQL table value sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param table_name: Table name to check for values
        :param column_name: Column name to check for values
        :param where_clause: WHERE clause to filter data
        :param value_check: Expression to evaluate against the query result
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.table_name = table_name
        self.column_name = column_name
        self.where_clause = where_clause
        self.value_check = value_check

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: Dict) -> bool:
        """
        Check if the specified value condition is met in the Cloud SQL table

        :param context: Airflow context dictionary
        :return: True if value condition is met, False otherwise
        """
        hook = self.get_hook()
        try:
            # Build SQL query to select the column with the where clause
            sql = f"SELECT {self.column_name} FROM {self.table_name}"
            if self.where_clause:
                sql += f" WHERE {self.where_clause}"

            # Execute query and fetch the result
            result = hook.execute_query(sql=sql)
            result_value = result.scalar()

            # Evaluate value_check against the query result
            condition_met = self.evaluate_value(result_value)
            logger.info(f"Checking value in table {self.table_name}, column {self.column_name}: {'Condition met' if condition_met else 'Condition not met'}")
            return condition_met
        except (PipelineError, SQLAlchemyError) as e:
            logger.error(f"Error while checking table value: {e}")
            return False

    def evaluate_value(self, result_value: object) -> bool:
        """
        Evaluate the value check against the query result

        :param result_value: The value returned by the query
        :return: True if condition is met, False otherwise
        """
        try:
            if callable(self.value_check):
                # If value_check is callable, call it with result_value
                return self.value_check(result_value)
            elif isinstance(self.value_check, str):
                # If value_check is string, evaluate it as expression with result_value in context
                return eval(self.value_check, {'result_value': result_value})
            else:
                logger.error("value_check must be a callable or a string")
                return False
        except Exception as e:
            logger.error(f"Error while evaluating value check: {e}")
            return False


class SelfHealingCloudSQLSensor(CloudSQLSensor):
    """
    Base Cloud SQL sensor with self-healing capabilities
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the self-healing Cloud SQL sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param confidence_threshold: Minimum confidence threshold for self-healing actions
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.self_healing_hook = SelfHealingCloudSQLHook(
            gcp_conn_id=self.conn_id,
            instance_connection_name=self.instance_connection_name,
            database=self.database,
            db_type=self.db_type,
            healing_config={},  # TODO: Load healing config
            confidence_threshold=confidence_threshold,
            hook_params={}
        )
        self.confidence_threshold = confidence_threshold

    def poke(self, context: Dict) -> bool:
        """
        Abstract method to be implemented by subclasses to check Cloud SQL condition with self-healing

        :param context: Airflow context dictionary
        :return: True if condition is met or was healed, False otherwise
        """
        raise NotImplementedError("Subclasses must implement the poke method")

    def _attempt_self_healing(self, error: Exception) -> bool:
        """
        Attempt to heal Cloud SQL issues

        :param error: The exception that occurred
        :return: True if healing succeeded, False otherwise
        """
        # Analyze error message and type
        # Check for common error patterns (connection issues, table not found, etc.)
        # For each recognized error pattern, generate a healing strategy
        # Try alternative connection parameters or database objects
        # Return True if healing succeeded, False otherwise
        return False


class SelfHealingCloudSQLTableExistenceSensor(SelfHealingCloudSQLSensor):
    """
    Cloud SQL table existence sensor with self-healing capabilities
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name', 'table_name')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        table_name: str,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the self-healing Cloud SQL table existence sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param table_name: Table name to check for existence
        :param confidence_threshold: Minimum confidence threshold for self-healing actions
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, confidence_threshold=confidence_threshold, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.table_name = table_name

    def poke(self, context: Dict) -> bool:
        """
        Check if the table exists in Cloud SQL with self-healing capability

        :param context: Airflow context dictionary
        :return: True if table exists or was healed, False otherwise
        """
        try:
            # Try to check table existence using CloudSQLTableExistenceSensor.poke method
            hook = self.get_hook()
            tables = hook.list_tables()
            table_exists = self.table_name in tables
            logger.info(f"Checking for table {self.table_name}: {'Exists' if table_exists else 'Does not exist'}")
            return table_exists
        except (PipelineError, SQLAlchemyError) as e:
            # If error occurs, attempt self-healing
            logger.warning(f"Table {self.table_name} not found, attempting self-healing")
            if self._attempt_self_healing(e):
                logger.info(f"Self-healing succeeded, retrying table existence check")
                return True  # Retry after self-healing
            else:
                logger.error(f"Self-healing failed, marking task as failed")
                return False

    def _attempt_self_healing(self, error: Exception) -> bool:
        """
        Attempt to heal table existence issues

        :param error: The exception that occurred
        :return: True if healing succeeded, False otherwise
        """
        # Analyze error message and type
        # Check for common error patterns (table not found, schema issues, etc.)
        # For each recognized error pattern, generate a healing strategy
        # Try to find similar table names using string similarity
        # Check if table exists with case-insensitive matching
        # If a suitable alternative is found, update the table_name
        # Return True if healing succeeded, False otherwise
        return False


class SelfHealingCloudSQLTableDataAvailabilitySensor(SelfHealingCloudSQLSensor):
    """
    Cloud SQL data availability sensor with self-healing capabilities
    """

    template_fields = ('conn_id', 'database', 'instance_connection_name', 'table_name', 'where_clause')

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        database: str,
        instance_connection_name: str,
        db_type: str,
        table_name: str,
        where_clause: Optional[str] = None,
        min_rows: int = 1,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        task_id: str,
        poke_interval: float = DEFAULT_TIMEOUT_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        soft_fail: bool = False,
        mode: str = 'poke',
        **kwargs,
    ) -> None:
        """
        Initialize the self-healing Cloud SQL data availability sensor

        :param conn_id: Airflow connection ID for Cloud SQL
        :param database: Database name
        :param instance_connection_name: Cloud SQL instance connection name
        :param db_type: Database type (postgres/mysql)
        :param table_name: Table name to check for data availability
        :param where_clause: WHERE clause to filter data
        :param min_rows: Minimum number of rows required for data availability
        :param confidence_threshold: Minimum confidence threshold for self-healing actions
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: If True, sensor fails softly (marks task as skipped)
        :param mode: Poke mode ('poke' or 'reschedule')
        :param kwargs: Additional keyword arguments
        """
        super().__init__(conn_id=conn_id, database=database, instance_connection_name=instance_connection_name, db_type=db_type, confidence_threshold=confidence_threshold, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.table_name = table_name
        self.where_clause = where_clause
        self.min_rows = min_rows if min_rows is not None else 1

    def poke(self, context: Dict) -> bool:
        """
        Check if data is available in the Cloud SQL table with self-healing capability

        :param context: Airflow context dictionary
        :return: True if data is available or was healed, False otherwise
        """
        try:
            # Try to check data availability using CloudSQLTableDataAvailabilitySensor.poke method
            hook = self.get_hook()
            record_count = hook.get_record_count(table_name=self.table_name, where_clause=self.where_clause)
            data_available = record_count >= self.min_rows
            logger.info(f"Checking data availability in table {self.table_name}: {'Available' if data_available else 'Not available'}")
            return data_available
        except (PipelineError, SQLAlchemyError) as e:
            # If error occurs, attempt self-healing
            logger.warning(f"Data not available in table {self.table_name}, attempting self-healing")
            if self._attempt_self_healing(e):
                logger.info(f"Self-healing succeeded, retrying data availability check")
                return True  # Retry after self-healing
            else:
                logger.error(f"Self-healing failed, marking task as failed")
                return False

    def _attempt_self_healing(self, error: Exception) -> bool:
        """
        Attempt to heal data availability issues

        :param error: The exception that occurred
        :return: True if healing succeeded, False otherwise
        """
        # Analyze error message and type
        # Check for common error patterns (no data found, where clause issues, etc.)
        # For each recognized error pattern, generate a healing strategy
        # Try to modify where_clause to find available data
        # Try to find data in related tables
        # If a suitable alternative is found, update the parameters
        # Return True if healing succeeded, False otherwise
        return False