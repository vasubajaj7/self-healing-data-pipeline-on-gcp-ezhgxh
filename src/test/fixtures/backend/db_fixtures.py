# src/test/fixtures/backend/db_fixtures.py
"""Provides pytest fixtures and utility functions for database-related testing in the self-healing data pipeline.
This module creates mock database repositories, clients, and test data to enable isolated testing of components that interact with the database layer.
"""
import pytest  # package version 7.x.x
from unittest.mock import MagicMock  # standard library
import pandas  # package version 2.0.x
import datetime  # standard library
import uuid  # standard library
from typing import Dict, List  # standard library

from src.backend.db.models import Alert, PipelineExecution, QualityRule, HealingAction, HealingExecution, IssuePattern, PipelineDefinition, PipelineMetric, QualityValidation, SourceSystem, TaskExecution  # src/backend/db/models/__init__.py
from src.backend.db.repositories import AlertRepository, ExecutionRepository, HealingRepository, MetricsRepository, PipelineRepository, QualityRepository, SourceRepository  # src/backend/db/repositories/__init__.py
from src.backend.utils.storage.bigquery_client import BigQueryClient  # src/backend/utils/storage/bigquery_client.py
from src.backend.constants import AlertSeverity, PipelineStatus, ValidationRuleType, HealingActionType  # src/backend/constants.py
from src.test.utils.test_helpers import TestDataGenerator  # src/test/utils/test_helpers.py

SAMPLE_ALERT_DATA = "[{'alert_id': 'alert-001', 'severity': 'HIGH', 'component': 'data_ingestion', 'message': 'Failed to extract data from source', 'status': 'NEW'}, {'alert_id': 'alert-002', 'severity': 'MEDIUM', 'component': 'data_quality', 'message': 'Data quality validation failed', 'status': 'ACKNOWLEDGED'}]"
SAMPLE_PIPELINE_DATA = "[{'pipeline_id': 'pipeline-001', 'name': 'daily_sales_pipeline', 'description': 'Daily sales data processing', 'source_id': 'source-001', 'status': 'ACTIVE'}, {'pipeline_id': 'pipeline-002', 'name': 'customer_data_pipeline', 'description': 'Customer data processing', 'source_id': 'source-002', 'status': 'ACTIVE'}]"
SAMPLE_EXECUTION_DATA = "[{'execution_id': 'exec-001', 'pipeline_id': 'pipeline-001', 'status': 'COMPLETED', 'start_time': '2023-06-15T08:00:00', 'end_time': '2023-06-15T08:30:00'}, {'execution_id': 'exec-002', 'pipeline_id': 'pipeline-002', 'status': 'FAILED', 'start_time': '2023-06-15T09:00:00', 'end_time': '2023-06-15T09:15:00'}]"

def create_mock_bigquery_client(query_results: Dict = None) -> MagicMock:
    """Creates a mock BigQuery client for testing

    Args:
        query_results: query_results

    Returns:
        MagicMock: A mock BigQueryClient instance
    """
    mock_client = MagicMock(spec=BigQueryClient)
    mock_client.execute_query.return_value = query_results
    mock_client.execute_query_to_dataframe.return_value = pandas.DataFrame()
    mock_client.table_exists.return_value = True
    mock_client.dataset_exists.return_value = True
    mock_client.create_dataset.return_value = None
    mock_client.create_table.return_value = None
    mock_client.insert_rows.return_value = None
    mock_client.update_rows.return_value = None
    mock_client.get_table.return_value = None
    return mock_client

def create_test_alert(alert_id: str, severity: AlertSeverity, component: str, message: str, status: str) -> Alert:
    """Creates a test Alert instance with specified parameters

    Args:
        alert_id: alert_id
        severity: severity
        component: component
        message: message
        status: status

    Returns:
        Alert: An Alert instance
    """
    alert = Alert(
        alert_type="test_alert",
        description=message,
        severity=severity,
        context={"component": component},
        component=component,
        alert_id=alert_id
    )
    alert.status = status
    return alert

def create_test_pipeline_execution(execution_id: str, pipeline_id: str, status: PipelineStatus, start_time: datetime, end_time: datetime) -> PipelineExecution:
    """Creates a test PipelineExecution instance with specified parameters

    Args:
        execution_id: execution_id
        pipeline_id: pipeline_id
        status: status
        start_time: start_time
        end_time: end_time

    Returns:
        PipelineExecution: A PipelineExecution instance
    """
    execution = PipelineExecution(
        pipeline_id=pipeline_id,
        execution_id=execution_id,
        status=status,
    )
    execution.start_time = start_time
    execution.end_time = end_time
    return execution

def create_test_quality_rule(rule_id: str, rule_type: ValidationRuleType, name: str, parameters: dict) -> QualityRule:
    """Creates a test QualityRule instance with specified parameters

    Args:
        rule_id: rule_id
        rule_type: rule_type
        name: name
        parameters: parameters

    Returns:
        QualityRule: A QualityRule instance
    """
    rule = QualityRule(
        name=name,
        rule_type=rule_type,
        subtype="test_subtype",
        dimension=QualityDimension.COMPLETENESS,
        description="Test quality rule",
        parameters=parameters,
        rule_id=rule_id
    )
    return rule

def create_test_healing_action(action_id: str, action_type: HealingActionType, pattern_id: str, parameters: dict) -> HealingAction:
    """Creates a test HealingAction instance with specified parameters

    Args:
        action_id: action_id
        action_type: action_type
        pattern_id: pattern_id
        parameters: parameters

    Returns:
        HealingAction: A HealingAction instance
    """
    action = HealingAction(
        action_id=action_id,
        name="Test Healing Action",
        action_type=action_type,
        description="Test healing action",
        action_parameters=parameters,
        pattern_id=pattern_id
    )
    return action

class TestDatabaseData:
    """Class providing test data for database-related tests"""

    def __init__(self):
        """Initialize test database data"""
        self.alerts = [
            create_test_alert(
                alert_id="alert-001",
                severity=AlertSeverity.HIGH,
                component="data_ingestion",
                message="Failed to extract data from source",
                status="NEW",
            ),
            create_test_alert(
                alert_id="alert-002",
                severity=AlertSeverity.MEDIUM,
                component="data_quality",
                message="Data quality validation failed",
                status="ACKNOWLEDGED",
            ),
        ]
        self.pipeline_executions = [
            create_test_pipeline_execution(
                execution_id="exec-001",
                pipeline_id="pipeline-001",
                status=PipelineStatus.COMPLETED,
                start_time=datetime.datetime(2023, 6, 15, 8, 0, 0),
                end_time=datetime.datetime(2023, 6, 15, 8, 30, 0),
            ),
            create_test_pipeline_execution(
                execution_id="exec-002",
                pipeline_id="pipeline-002",
                status=PipelineStatus.FAILED,
                start_time=datetime.datetime(2023, 6, 15, 9, 0, 0),
                end_time=datetime.datetime(2023, 6, 15, 9, 15, 0),
            ),
        ]
        self.quality_rules = [
            create_test_quality_rule(
                rule_id="rule-001",
                rule_type=ValidationRuleType.SCHEMA_VALIDATION,
                name="Schema Validation Rule",
                parameters={"schema": "test_schema"},
            ),
            create_test_quality_rule(
                rule_id="rule-002",
                rule_type=ValidationRuleType.NOT_NULL_VALIDATION,
                name="Not Null Validation Rule",
                parameters={"column": "test_column"},
            ),
        ]
        self.healing_actions = [
            create_test_healing_action(
                action_id="action-001",
                action_type=HealingActionType.DATA_CORRECTION,
                pattern_id="pattern-001",
                parameters={"correction_logic": "test_logic"},
            ),
            create_test_healing_action(
                action_id="action-002",
                action_type=HealingActionType.PIPELINE_RETRY,
                pattern_id="pattern-002",
                parameters={"retry_count": 3},
            ),
        ]
        self.source_systems = []

    def get_alert_by_id(self, alert_id: str) -> Alert:
        """Get a test alert by ID

        Args:
            alert_id: alert_id

        Returns:
            Alert: The alert with the specified ID or None
        """
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                return alert
        return None

    def get_execution_by_id(self, execution_id: str) -> PipelineExecution:
        """Get a test pipeline execution by ID

        Args:
            execution_id: execution_id

        Returns:
            PipelineExecution: The execution with the specified ID or None
        """
        for execution in self.pipeline_executions:
            if execution.execution_id == execution_id:
                return execution
        return None

    def get_rule_by_id(self, rule_id: str) -> QualityRule:
        """Get a test quality rule by ID

        Args:
            rule_id: rule_id

        Returns:
            QualityRule: The rule with the specified ID or None
        """
        for rule in self.quality_rules:
            if rule.rule_id == rule_id:
                return rule
        return None

    def get_action_by_id(self, action_id: str) -> HealingAction:
        """Get a test healing action by ID

        Args:
            action_id: action_id

        Returns:
            HealingAction: The action with the specified ID or None
        """
        for action in self.healing_actions:
            if action.action_id == action_id:
                return action
        return None

    def generate_test_dataframe(self, rows: int, columns: list, data_types: dict) -> pandas.DataFrame:
        """Generate a test pandas DataFrame with specified characteristics

        Args:
            rows: rows
            columns: columns
            data_types: data_types

        Returns:
            pandas.DataFrame: A test DataFrame
        """
        data = {}
        for column in columns:
            if data_types[column] == 'int':
                data[column] = [1, 2, 3] * rows
            elif data_types[column] == 'str':
                data[column] = ['a', 'b', 'c'] * rows
        return pandas.DataFrame(data)

class MockAlertRepository:
    """Mock implementation of AlertRepository for testing"""

    def __init__(self, initial_alerts: List[Alert] = None, bq_client: MagicMock = None):
        """Initialize mock alert repository

        Args:
            initial_alerts: initial_alerts
            bq_client: bq_client
        """
        self._alerts = initial_alerts or []
        self._bq_client = bq_client

    def create_alert(self, alert: Alert) -> str:
        """Mock implementation of create_alert method

        Args:
            alert: alert

        Returns:
            str: ID of the created alert
        """
        self._alerts.append(alert)
        return alert.alert_id

    def get_alert(self, alert_id: str) -> Alert:
        """Mock implementation of get_alert method

        Args:
            alert_id: alert_id

        Returns:
            Alert: Alert object if found, None otherwise
        """
        for alert in self._alerts:
            if alert.alert_id == alert_id:
                return alert
        return None

    def update_alert(self, alert: Alert) -> bool:
        """Mock implementation of update_alert method

        Args:
            alert: alert

        Returns:
            bool: True if update was successful
        """
        for i, existing_alert in enumerate(self._alerts):
            if existing_alert.alert_id == alert.alert_id:
                self._alerts[i] = alert
                return True
        return False

    def get_active_alerts(self, limit: int, offset: int) -> List[Alert]:
        """Mock implementation of get_active_alerts method

        Args:
            limit: limit
            offset: offset

        Returns:
            list: List of active Alert objects
        """
        active_alerts = [alert for alert in self._alerts if alert.status not in ["RESOLVED", "SUPPRESSED"]]
        return active_alerts[offset:offset + limit]

class MockExecutionRepository:
    """Mock implementation of ExecutionRepository for testing"""

    def __init__(self, initial_executions: List[PipelineExecution] = None, initial_tasks: List[TaskExecution] = None, bq_client: MagicMock = None):
        """Initialize mock execution repository

        Args:
            initial_executions: initial_executions
            initial_tasks: initial_tasks
            bq_client: bq_client
        """
        self._executions = initial_executions or []
        self._tasks = initial_tasks or []
        self._bq_client = bq_client

    def create_execution(self, execution: PipelineExecution) -> str:
        """Mock implementation of create_execution method

        Args:
            execution: execution

        Returns:
            str: ID of the created execution
        """
        self._executions.append(execution)
        return execution.execution_id

    def get_execution(self, execution_id: str) -> PipelineExecution:
        """Mock implementation of get_execution method

        Args:
            execution_id: execution_id

        Returns:
            PipelineExecution: Execution object if found, None otherwise
        """
        for execution in self._executions:
            if execution.execution_id == execution_id:
                return execution
        return None

    def update_execution(self, execution: PipelineExecution) -> bool:
        """Mock implementation of update_execution method

        Args:
            execution: execution

        Returns:
            bool: True if update was successful
        """
        for i, existing_execution in enumerate(self._executions):
            if existing_execution.execution_id == execution.execution_id:
                self._executions[i] = execution
                return True
        return False

    def get_executions_by_pipeline(self, pipeline_id: str, limit: int, offset: int) -> List[PipelineExecution]:
        """Mock implementation of get_executions_by_pipeline method

        Args:
            pipeline_id: pipeline_id
            limit: limit
            offset: offset

        Returns:
            list: List of PipelineExecution objects for the pipeline
        """
        filtered_executions = [execution for execution in self._executions if execution.pipeline_id == pipeline_id]
        return filtered_executions[offset:offset + limit]

@pytest.fixture
def mock_bigquery_client():
    """Pytest fixture providing a mock BigQuery client"""
    return create_mock_bigquery_client()

@pytest.fixture
def mock_alert_repository():
    """Pytest fixture providing a mock AlertRepository"""
    return MockAlertRepository()

@pytest.fixture
def mock_execution_repository():
    """Pytest fixture providing a mock ExecutionRepository"""
    return MockExecutionRepository()

@pytest.fixture
def mock_healing_repository():
    """Pytest fixture providing a mock HealingRepository"""
    return MagicMock()

@pytest.fixture
def mock_quality_repository():
    """Pytest fixture providing a mock QualityRepository"""
    return MagicMock()

@pytest.fixture
def mock_pipeline_repository():
    """Pytest fixture providing a mock PipelineRepository"""
    return MagicMock()

@pytest.fixture
def mock_source_repository():
    """Pytest fixture providing a mock SourceRepository"""
    return MagicMock()

@pytest.fixture
def test_database_data():
    """Pytest fixture providing test database data"""
    return TestDatabaseData()