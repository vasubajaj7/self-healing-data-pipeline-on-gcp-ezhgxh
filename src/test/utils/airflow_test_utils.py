"""
Provides utilities for testing Apache Airflow components in the self-healing data pipeline.
This module contains functions and classes to simplify testing of DAGs, operators, hooks, and other Airflow-specific components with a focus on self-healing capabilities.
"""

import pytest  # version 7.3.1
from unittest import mock  # standard library
import typing  # standard library
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import datetime  # standard library

import pandas  # version 2.0.x
from airflow import DAG  # version 2.5.x
from airflow.models import BaseOperator  # version 2.5.x
from airflow.models import DagRun  # version 2.5.x
from airflow.models import TaskInstance  # version 2.5.x
from airflow.utils.dates import days_ago  # version 2.5.x
from airflow.operators.dummy import DummyOperator  # version 2.5.x

from src.test.utils.test_helpers import compare_nested_structures, MockResponseBuilder, create_temp_file  # Import helper functions for test data creation and comparison
from src.backend.constants import FileFormat  # Import constants used in Airflow components
from src.backend.airflow.plugins.hooks.gcs_hooks import EnhancedGCSHook, SelfHealingGCSHook  # Import GCS hooks for mocking and testing
from src.backend.airflow.plugins.custom_operators.gcs_operators import GCSListOperator, SelfHealingGCSToDataFrameOperator, SelfHealingGCSToBigQueryOperator  # Import custom GCS operators for testing


def create_mock_airflow_context(task_id: str, dag_id: str, task_params: dict = None, xcom_data: dict = None, execution_date: datetime.datetime = None) -> dict:
    """Creates a mock Airflow task context dictionary for testing operators

    Args:
        task_id (str): The task_id for the mock context.
        dag_id (str): The dag_id for the mock context.
        task_params (dict): Optional task parameters to include in the context.
        xcom_data (dict): Optional XCom data to simulate XCom pull.
        execution_date (datetime.datetime): Optional execution date for the context.

    Returns:
        dict: Mock Airflow task context dictionary
    """
    # Create a mock TaskInstance with the specified task_id and dag_id
    mock_task_instance = mock.MagicMock(spec=TaskInstance, task_id=task_id, dag_id=dag_id)

    # Configure the mock TaskInstance to return xcom_data when xcom_pull is called
    mock_task_instance.xcom_pull.side_effect = lambda key=None, task_ids=None: xcom_data.get(key) if xcom_data else None

    # Configure the mock TaskInstance to store values when xcom_push is called
    xcom_push_values = {}
    mock_task_instance.xcom_push.side_effect = lambda key, value: xcom_push_values.update({key: value})
    mock_task_instance.xcom_pull.return_value = xcom_push_values

    # Create a context dictionary with the mock TaskInstance and other required fields
    context = {
        'task_instance': mock_task_instance,
        'dag_run': mock.MagicMock(spec=DagRun),
        'ti': mock_task_instance,  # 'ti' is often used as a shorthand for 'task_instance'
    }

    # Add execution_date to the context if provided, otherwise use current time
    context['execution_date'] = execution_date or datetime.datetime.now()

    # Add task_params to the context if provided
    if task_params:
        context.update(task_params)

    # Return the mock context dictionary
    return context


def create_mock_dag(dag_id: str, default_args: dict = None, schedule_interval: str = None, start_date: datetime.datetime = None) -> DAG:
    """Creates a mock Airflow DAG for testing

    Args:
        dag_id (str): The dag_id for the mock DAG.
        default_args (dict): Optional default arguments for the DAG.
        schedule_interval (str): Optional schedule interval for the DAG.
        start_date (datetime.datetime): Optional start date for the DAG.

    Returns:
        DAG: Mock Airflow DAG
    """
    # Create default_args dictionary if not provided
    if default_args is None:
        default_args = {}

    # Set start_date in default_args if provided
    if start_date:
        default_args['start_date'] = start_date

    # Create and return an airflow.models.DAG instance with the specified parameters
    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )


def create_mock_gcs_hook(self_healing: bool = False, mock_files: dict = None, mock_responses: dict = None, mock_errors: dict = None) -> Union[mock.MagicMock, EnhancedGCSHook, SelfHealingGCSHook]:
    """Creates a mock GCS hook for testing GCS operations

    Args:
        self_healing (bool): Whether to create a self-healing GCS hook.
        mock_files (dict): Dictionary of mock files to return for list_files.
        mock_responses (dict): Dictionary of mock responses to return for read operations.
        mock_errors (dict): Dictionary of mock errors to raise for specific operations.

    Returns:
        Union[mock.MagicMock, EnhancedGCSHook, SelfHealingGCSHook]: Mock GCS hook
    """
    # Create a MagicMock for the appropriate GCS hook class based on self_healing flag
    if self_healing:
        mock_hook = mock.MagicMock(spec=SelfHealingGCSHook)
    else:
        mock_hook = mock.MagicMock(spec=EnhancedGCSHook)

    # Set up mock_files to be returned by list_files if provided
    if mock_files:
        mock_hook.list_files.return_value = [mock.MagicMock(name=name) for name in mock_files]

    # Set up mock responses for read operations if provided
    if mock_responses:
        mock_hook.read_file_as_dataframe.side_effect = lambda bucket_name, object_name, **kwargs: mock_responses.get(object_name)

    # Configure mock errors to be raised by specific operations if provided
    if mock_errors:
        for method, error in mock_errors.items():
            getattr(mock_hook, method).side_effect = error

    # Return the configured mock hook
    return mock_hook


def create_mock_bigquery_hook(mock_tables: dict = None, mock_query_results: dict = None, mock_errors: dict = None) -> mock.MagicMock:
    """Creates a mock BigQuery hook for testing BigQuery operations

    Args:
        mock_tables (dict): Dictionary of mock tables to return for get_table.
        mock_query_results (dict): Dictionary of mock query results to return for run_query.
        mock_errors (dict): Dictionary of mock errors to raise for specific operations.

    Returns:
        mock.MagicMock: Mock BigQuery hook
    """
    # Create a MagicMock for the BigQuery hook
    mock_hook = mock.MagicMock()

    # Set up mock_tables to be returned by get_table if provided
    if mock_tables:
        mock_hook.get_table.side_effect = lambda table_id: mock_tables.get(table_id)

    # Set up mock_query_results to be returned by run_query if provided
    if mock_query_results:
        mock_hook.run_query.side_effect = lambda query, **kwargs: mock_query_results.get(query)

    # Configure mock errors to be raised by specific operations if provided
    if mock_errors:
        for method, error in mock_errors.items():
            getattr(mock_hook, method).side_effect = error

    # Return the configured mock hook
    return mock_hook


def create_test_dataframe_for_gcs(data: dict, file_format: FileFormat, write_options: dict) -> Tuple[pandas.DataFrame, str]:
    """Creates a test pandas DataFrame and saves it to a temporary file for GCS testing

    Args:
        data (dict): Data to create the DataFrame from.
        file_format (FileFormat): File format to save the DataFrame as.
        write_options (dict): Options to use when writing the DataFrame to the file.

    Returns:
        Tuple[pandas.DataFrame, str]: DataFrame and path to temporary file
    """
    # Create a pandas DataFrame from the provided data
    df = pandas.DataFrame(data)

    # Determine file extension based on file_format
    if file_format == FileFormat.CSV:
        file_extension = ".csv"
    elif file_format == FileFormat.JSON:
        file_extension = ".json"
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Create a temporary file with the appropriate extension
    temp_file_path = create_temp_file(suffix=file_extension)

    # Save the DataFrame to the temporary file using the specified format and options
    if file_format == FileFormat.CSV:
        df.to_csv(temp_file_path, **write_options)
    elif file_format == FileFormat.JSON:
        df.to_json(temp_file_path, **write_options)

    # Return the DataFrame and the path to the temporary file
    return df, temp_file_path


def patch_airflow_connection(conn_id: str, conn_params: dict) -> mock._patch:
    """Patches Airflow's connection mechanism to return a mock connection

    Args:
        conn_id (str): The connection ID to patch.
        conn_params (dict): The parameters for the mock connection.

    Returns:
        mock._patch: Patch context manager for use in with statement
    """
    # Create a mock Connection object with the specified parameters
    mock_connection = mock.MagicMock()
    mock_connection.get_extra.return_value = conn_params

    # Create a patch for BaseHook.get_connection to return the mock connection
    return mock.patch('airflow.hooks.base.BaseHook.get_connection', return_value=mock_connection)


def assert_dag_expected_structure(dag: DAG, expected_task_ids: list, expected_dependencies: dict):
    """Asserts that a DAG has the expected structure of tasks and dependencies

    Args:
        dag (DAG): The DAG to validate.
        expected_task_ids (list): List of expected task IDs in the DAG.
        expected_dependencies (dict): Dictionary of expected task dependencies.
    """
    # Get the list of task_ids from the DAG
    task_ids = dag.task_ids

    # Assert that all expected_task_ids are present in the DAG
    assert set(expected_task_ids) == set(task_ids), f"DAG {dag.dag_id} does not have the expected task_ids. Expected: {expected_task_ids}, Actual: {task_ids}"

    # Assert that the DAG doesn't have unexpected tasks if expected_task_ids is provided
    if expected_task_ids:
        unexpected_tasks = set(task_ids) - set(expected_task_ids)
        assert not unexpected_tasks, f"DAG {dag.dag_id} has unexpected tasks: {unexpected_tasks}"

    # For each task in expected_dependencies, verify its upstream and downstream dependencies
    for task_id, dependencies in expected_dependencies.items():
        task = dag.get_task(task_id)
        upstream_task_ids = [t.task_id for t in task.upstream_list]
        downstream_task_ids = [t.task_id for t in task.downstream_list]

        assert set(dependencies.get('upstream', [])) == set(upstream_task_ids), f"Task {task_id} in DAG {dag.dag_id} does not have the expected upstream dependencies. Expected: {dependencies.get('upstream', [])}, Actual: {upstream_task_ids}"
        assert set(dependencies.get('downstream', [])) == set(downstream_task_ids), f"Task {task_id} in DAG {dag.dag_id} does not have the expected downstream dependencies. Expected: {dependencies.get('downstream', [])}, Actual: {downstream_task_ids}"


def assert_operator_executed_successfully(operator: BaseOperator, context: dict, expected_result: Any = None, expected_xcom: dict = None) -> Any:
    """Asserts that an operator executed successfully with expected results

    Args:
        operator (BaseOperator): The operator to execute.
        context (dict): The context to execute the operator with.
        expected_result (Any): The expected result of the operator execution.
        expected_xcom (dict): The expected XCom values to be pushed by the operator.

    Returns:
        Any: Operator execution result
    """
    # Execute the operator with the provided context
    result = operator.execute(context)

    # If expected_result is provided, assert that the execution result matches
    if expected_result is not None:
        assert result == expected_result, f"Operator {operator.task_id} did not return the expected result. Expected: {expected_result}, Actual: {result}"

    # If expected_xcom is provided, assert that the operator pushed the expected values to XCom
    if expected_xcom:
        for key, value in expected_xcom.items():
            assert context['task_instance'].xcom_pull(key=key) == value, f"Operator {operator.task_id} did not push the expected XCom value for key {key}. Expected: {value}, Actual: {context['task_instance'].xcom_pull(key=key)}"

    # Return the execution result
    return result


def assert_task_states(dag_run: DagRun, expected_states: dict):
    """Asserts that tasks in a DAG run have the expected states

    Args:
        dag_run (DagRun): The DAG run to validate.
        expected_states (dict): Dictionary of expected task states.
    """
    # Get the task instances for the DAG run
    task_instances = dag_run.get_task_instances()

    # For each task_id in expected_states, find the corresponding task instance
    for task_id, expected_state in expected_states.items():
        task_instance = next((ti for ti in task_instances if ti.task_id == task_id), None)

        # Assert that the task instance state matches the expected state
        assert task_instance.current_state() == expected_state, f"Task {task_id} in DAG run {dag_run.dag_id} does not have the expected state. Expected: {expected_state}, Actual: {task_instance.current_state()}"


def simulate_gcs_file_event(bucket_name: str, object_name: str, event_type: str) -> dict:
    """Simulates a GCS file event for testing event-triggered DAGs

    Args:
        bucket_name (str): The bucket name for the event.
        object_name (str): The object name for the event.
        event_type (str): The event type for the event.

    Returns:
        dict: GCS event data structure
    """
    # Create a GCS event data structure with the specified parameters
    event_data = {
        "bucket": bucket_name,
        "name": object_name,
        "kind": "storage#object",
        "id": f"{bucket_name}/{object_name}/1234567890",
        "selfLink": f"https://www.googleapis.com/storage/v1/b/{bucket_name}/o/{object_name}",
        "contentType": "text/csv",
        "size": "1024",
        "timeCreated": datetime.datetime.now().isoformat(),
        "updated": datetime.datetime.now().isoformat(),
        "metageneration": "1",
        "generation": "1234567890",
        "resourceState": "exists",
        "eventType": event_type,
    }

    # Return the event data structure for use in tests
    return event_data


class AirflowTestCase:
    """Base test case class for Airflow component testing with helpful utilities"""

    def __init__(self):
        """Initialize the AirflowTestCase"""
        # Initialize empty dictionaries for mock connections and variables
        self.mock_connections = {}
        self.mock_variables = {}

        # Initialize empty list for mock hooks
        self.mock_hooks = []

    def setUp(self):
        """Set up the test environment"""
        # Set up patches for Airflow connections and variables
        self.conn_patch = mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=self.get_mock_connection)
        self.var_patch = mock.patch('airflow.models.Variable.get', side_effect=self.get_mock_variable)
        self.conn_patch.start()
        self.var_patch.start()

        # Configure default test environment
        self.dag = create_mock_dag(dag_id='test_dag', start_date=days_ago(2))

        # Initialize test resources
        self.ti = mock.MagicMock(spec=TaskInstance)

    def tearDown(self):
        """Clean up the test environment"""
        # Stop all patches
        self.conn_patch.stop()
        self.var_patch.stop()

        # Clean up test resources
        self.mock_connections.clear()
        self.mock_variables.clear()
        for hook_patch in self.mock_hooks:
            hook_patch.stop()
        self.mock_hooks.clear()

        # Reset Airflow context
        self.dag = None
        self.ti = None

    def add_mock_connection(self, conn_id: str, conn_params: dict):
        """Add a mock Airflow connection

        Args:
            conn_id (str): The connection ID to add.
            conn_params (dict): The parameters for the mock connection.
        """
        # Create a mock connection with the specified parameters
        self.mock_connections[conn_id] = conn_params

    def add_mock_variable(self, key: str, value: Any):
        """Add a mock Airflow variable

        Args:
            key (str): The variable key to add.
            value (Any): The value for the mock variable.
        """
        # Add the key-value pair to the mock_variables dictionary
        self.mock_variables[key] = value

    def add_mock_hook(self, hook_class_path: str, mock_hook: mock.MagicMock):
        """Add a mock Airflow hook

        Args:
            hook_class_path (str): The path to the hook class to mock.
            mock_hook (mock.MagicMock): The mock hook to use.
        """
        # Create a patch for the specified hook class
        hook_patch = mock.patch(hook_class_path, return_value=mock_hook)

        # Configure the patch to return the mock_hook
        hook_patch.start()

        # Start the patch and add it to the mock_hooks list
        self.mock_hooks.append(hook_patch)

    def get_mock_connection(self, conn_id: str):
        """Get a mock Airflow connection

        Args:
            conn_id (str): The connection ID to retrieve.
        """
        # Return the mock connection with the specified conn_id from the mock_connections dictionary
        return self.mock_connections.get(conn_id)

    def get_mock_variable(self, key: str):
        """Get a mock Airflow variable

        Args:
            key (str): The variable key to retrieve.
        """
        # Return the value for the specified key from the mock_variables dictionary
        return self.mock_variables.get(key)

    def run_dag(self, dag: DAG, execution_date: datetime.datetime, conf: dict):
        """Run a DAG for testing

        Args:
            dag (DAG): The DAG to run.
            execution_date (datetime.datetime): The execution date for the DAG run.
            conf (dict): The configuration for the DAG run.
        """
        # Create a DAG run with the specified parameters
        dag_run = dag.create_dagrun(
            run_id=f"test_run_{datetime.datetime.now().isoformat()}",
            execution_date=execution_date,
            state='running',
            conf=conf,
        )

        # Execute the DAG run
        dag.run(execution_date=execution_date, conf=conf, test_mode=True)

        # Return the DAG run object for assertions
        return dag_run

    def get_task_instance(self, dag_run: DagRun, task_id: str):
        """Get a task instance from a DAG run

        Args:
            dag_run (DagRun): The DAG run to get the task instance from.
            task_id (str): The task ID to get the task instance for.
        """
        # Find the task instance with the specified task_id in the DAG run
        task_instance = next((ti for ti in dag_run.get_task_instances() if ti.task_id == task_id), None)

        # Return the task instance or raise an exception if not found
        if task_instance:
            return task_instance
        else:
            raise ValueError(f"Task instance not found for task_id: {task_id}")


class MockDagBag:
    """Mock DagBag for testing DAG loading and discovery"""

    def __init__(self, dags: dict = None):
        """Initialize the MockDagBag

        Args:
            dags (dict): Dictionary of DAGs to include in the DagBag.
        """
        # Initialize the dags dictionary with provided dags or empty dict
        self.dags = dags or {}

    def get_dag(self, dag_id: str) -> Optional[DAG]:
        """Get a DAG by ID

        Args:
            dag_id (str): The DAG ID to retrieve.

        Returns:
            Optional[DAG]: DAG if found, None otherwise
        """
        # Return the DAG with the specified dag_id from the dags dictionary
        return self.dags.get(dag_id)

    def add_dag(self, dag: DAG):
        """Add a DAG to the DagBag

        Args:
            dag (DAG): The DAG to add.
        """
        # Add the DAG to the dags dictionary using its dag_id as the key
        self.dags[dag.dag_id] = dag


class MockOperator(BaseOperator):
    """Mock operator for testing DAG execution flows"""

    def __init__(self, task_id: str, return_value: Any = None, side_effect: Exception = None, **kwargs):
        """Initialize the MockOperator

        Args:
            task_id (str): The task ID for the operator.
            return_value (Any): The value to return when the operator is executed.
            side_effect (Exception): The exception to raise when the operator is executed.
        """
        # Call parent constructor with task_id and kwargs
        super().__init__(task_id=task_id, **kwargs)

        # Set return_value to be returned when the operator is executed
        self.return_value = return_value

        # Set side_effect to be raised when the operator is executed if provided
        self.side_effect = side_effect

        # Initialize executed flag to False
        self.executed = False

    def execute(self, context: dict) -> Any:
        """Execute the mock operator

        Args:
            context (dict): The context to execute the operator with.

        Returns:
            Any: Configured return value
        """
        # Set executed flag to True
        self.executed = True

        # If side_effect is set, raise the exception
        if self.side_effect:
            raise self.side_effect

        # Return the configured return_value
        return self.return_value


class MockXComPusher(BaseOperator):
    """Mock operator that pushes predefined values to XCom"""

    def __init__(self, task_id: str, xcom_values: dict, **kwargs):
        """Initialize the MockXComPusher

        Args:
            task_id (str): The task ID for the operator.
            xcom_values (dict): The XCom values to push.
        """
        # Call parent constructor with task_id and kwargs
        super().__init__(task_id=task_id, **kwargs)

        # Set xcom_values to be pushed to XCom when the operator is executed
        self.xcom_values = xcom_values

        # Initialize executed flag to False
        self.executed = False

    def execute(self, context: dict):
        """Execute the mock XCom pusher

        Args:
            context (dict): The context to execute the operator with.
        """
        # Set executed flag to True
        self.executed = True

        # Get task instance from context
        task_instance = context['task_instance']

        # For each key-value pair in xcom_values, push to XCom using the task instance
        for key, value in self.xcom_values.items():
            task_instance.xcom_push(key=key, value=value)