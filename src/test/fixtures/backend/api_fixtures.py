"""
Provides test fixtures and helper functions for testing the API components of the self-healing data pipeline.
Includes mock objects for API controllers, services, and request/response models to facilitate
isolated testing of API functionality.
"""

import pytest
from unittest.mock import Mock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
import datetime
import uuid

# Internal imports
from src.backend.constants import ValidationStatus, AlertSeverity, DataSourceType, HealingActionType
from src.backend.api.models.data_models import (
    QualityRule, QualityValidation, SourceSystem, PipelineDefinition,
    PipelineExecution, IssuePattern, HealingAction, Alert
)
from src.backend.api.models.response_models import (
    BaseResponse, DataResponse, QualityRuleResponse, QualityRuleListResponse,
    QualityValidationResponse, QualityValidationListResponse, PipelineResponse,
    PipelineListResponse, HealingActionResponse, AlertResponse, PaginationMetadata
)
from src.backend.api.models.request_models import (
    QualityRuleCreateRequest, QualityRuleUpdateRequest, PipelineCreateRequest,
    PipelineUpdateRequest, HealingPatternCreateRequest, HealingActionCreateRequest
)
from src.backend.api.models.error_models import (
    ResourceNotFoundError, ValidationError, ResponseStatus
)
from src.test.utils.test_helpers import create_temp_file

# Sample data for testing
SAMPLE_QUALITY_RULES = [
    {
        "rule_id": "rule_001",
        "rule_type": "SCHEMA",
        "target_dataset": "test_dataset",
        "target_table": "test_table",
        "dimension": "COMPLETENESS",
        "parameters": {"columns": ["id", "name", "value"]},
        "is_active": True,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    },
    {
        "rule_id": "rule_002",
        "rule_type": "CONTENT",
        "target_dataset": "test_dataset",
        "target_table": "test_table",
        "dimension": "ACCURACY",
        "parameters": {"column": "value", "min_value": 0, "max_value": 100},
        "is_active": True,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    }
]

SAMPLE_QUALITY_VALIDATIONS = [
    {
        "validation_id": "val_001",
        "rule_id": "rule_001",
        "execution_id": "exec_001",
        "dataset": "test_dataset",
        "table": "test_table",
        "status": "PASSED",
        "details": {"message": "All required columns present"},
        "validation_time": "2023-01-01T00:00:00Z"
    },
    {
        "validation_id": "val_002",
        "rule_id": "rule_002",
        "execution_id": "exec_001",
        "dataset": "test_dataset",
        "table": "test_table",
        "status": "FAILED",
        "details": {"message": "Values outside acceptable range", "invalid_count": 5},
        "validation_time": "2023-01-01T00:00:00Z"
    }
]

SAMPLE_PIPELINES = [
    {
        "pipeline_id": "pipe_001",
        "name": "Test Pipeline 1",
        "source_id": "source_001",
        "target_dataset": "test_dataset",
        "target_table": "test_table",
        "schedule": "0 0 * * *",
        "is_active": True,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    },
    {
        "pipeline_id": "pipe_002",
        "name": "Test Pipeline 2",
        "source_id": "source_002",
        "target_dataset": "test_dataset2",
        "target_table": "test_table2",
        "schedule": "0 12 * * *",
        "is_active": True,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    }
]

SAMPLE_HEALING_ACTIONS = [
    {
        "action_id": "action_001",
        "pattern_id": "pattern_001",
        "action_type": "DATA_CORRECTION",
        "action_definition": {"correction_type": "imputation", "strategy": "mean"},
        "success_rate": 0.95,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    },
    {
        "action_id": "action_002",
        "pattern_id": "pattern_002",
        "action_type": "PIPELINE_ADJUSTMENT",
        "action_definition": {"parameter": "batch_size", "value": 1000},
        "success_rate": 0.85,
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
    }
]

SAMPLE_ALERTS = [
    {
        "alert_id": "alert_001",
        "alert_type": "DATA_QUALITY",
        "severity": "HIGH",
        "message": "Data quality validation failed",
        "source": "quality_validation",
        "source_id": "val_002",
        "created_at": "2023-01-01T00:00:00Z",
        "acknowledged": False,
        "resolved": False
    },
    {
        "alert_id": "alert_002",
        "alert_type": "PIPELINE_FAILURE",
        "severity": "CRITICAL",
        "message": "Pipeline execution failed",
        "source": "pipeline_execution",
        "source_id": "exec_002",
        "created_at": "2023-01-01T00:00:00Z",
        "acknowledged": True,
        "resolved": False
    }
]

def create_test_app(mock_services: dict) -> FastAPI:
    """
    Creates a test FastAPI application with mock dependencies.
    
    Args:
        mock_services: Dictionary of mock services to inject
        
    Returns:
        Configured FastAPI application for testing
    """
    # This would typically import the app creator from the actual API module
    # For now, we'll create a simple mock app
    app = FastAPI(title="Self-Healing Data Pipeline API Test")
    
    # Store mock services in app state for dependency injection
    app.state.services = mock_services
    
    # In a real implementation, we would register API routes here
    # or use the actual app creator with the mock dependencies
    
    return app

def create_mock_quality_service(quality_rules=None, quality_validations=None, quality_score=0.95):
    """
    Creates a mock quality service for API testing.
    
    Args:
        quality_rules: Optional list of quality rules to return
        quality_validations: Optional list of quality validations to return
        quality_score: Optional quality score to return
        
    Returns:
        Mock quality service
    """
    mock_service = MagicMock()
    
    # Set up default values
    quality_rules = quality_rules or []
    quality_validations = quality_validations or []
    
    # Configure mock methods
    
    # Get quality rules
    mock_service.get_quality_rules.return_value = (quality_rules, len(quality_rules))
    
    # Get quality rule by ID
    def get_quality_rule_by_id(rule_id):
        for rule in quality_rules:
            if rule.rule_id == rule_id:
                return rule
        return None
    mock_service.get_quality_rule_by_id.side_effect = get_quality_rule_by_id
    
    # Create quality rule
    def create_quality_rule(rule_data):
        new_rule = QualityRule(
            rule_id=str(uuid.uuid4()),
            **rule_data.dict()
        )
        return new_rule
    mock_service.create_quality_rule.side_effect = create_quality_rule
    
    # Update quality rule
    def update_quality_rule(rule_id, rule_data):
        rule = get_quality_rule_by_id(rule_id)
        if not rule:
            return None
        
        # In a real implementation, we would update rule fields here
        return rule
    mock_service.update_quality_rule.side_effect = update_quality_rule
    
    # Delete quality rule
    mock_service.delete_quality_rule.return_value = True
    
    # Get quality validations
    mock_service.get_quality_validations.return_value = (quality_validations, len(quality_validations))
    
    # Get validation by ID
    def get_validation_by_id(validation_id):
        for validation in quality_validations:
            if validation.validation_id == validation_id:
                return validation
        return None
    mock_service.get_validation_by_id.side_effect = get_validation_by_id
    
    # Execute validation
    def execute_validation(dataset, table, rule_ids=None):
        new_validation = QualityValidation(
            validation_id=str(uuid.uuid4()),
            rule_id=rule_ids[0] if rule_ids else "rule_001",
            execution_id="exec_001",
            status=ValidationStatus.PASSED,
            validation_time=datetime.datetime.now(),
            success_percent=0.95,
            records_validated=100,
            records_failed=5
        )
        return new_validation
    mock_service.execute_validation.side_effect = execute_validation
    
    # Get quality score
    mock_service.get_quality_score.return_value = quality_score
    
    # Get quality issues
    mock_service.get_quality_issues.return_value = ([], 0)
    
    return mock_service

def create_mock_pipeline_service(pipelines=None, executions=None):
    """
    Creates a mock pipeline service for API testing.
    
    Args:
        pipelines: Optional list of pipelines to return
        executions: Optional list of pipeline executions to return
        
    Returns:
        Mock pipeline service
    """
    mock_service = MagicMock()
    
    # Set up default values
    pipelines = pipelines or []
    executions = executions or []
    
    # Configure mock methods
    
    # Get pipelines
    mock_service.get_pipelines.return_value = (pipelines, len(pipelines))
    
    # Get pipeline by ID
    def get_pipeline_by_id(pipeline_id):
        for pipeline in pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline
        return None
    mock_service.get_pipeline_by_id.side_effect = get_pipeline_by_id
    
    # Create pipeline
    def create_pipeline(pipeline_data):
        new_pipeline = PipelineDefinition(
            pipeline_id=str(uuid.uuid4()),
            **pipeline_data.dict()
        )
        return new_pipeline
    mock_service.create_pipeline.side_effect = create_pipeline
    
    # Update pipeline
    def update_pipeline(pipeline_id, pipeline_data):
        pipeline = get_pipeline_by_id(pipeline_id)
        if not pipeline:
            return None
        
        # In a real implementation, we would update pipeline fields here
        return pipeline
    mock_service.update_pipeline.side_effect = update_pipeline
    
    # Delete pipeline
    mock_service.delete_pipeline.return_value = True
    
    # Get pipeline executions
    mock_service.get_pipeline_executions.return_value = (executions, len(executions))
    
    # Get execution by ID
    def get_execution_by_id(execution_id):
        for execution in executions:
            if execution.execution_id == execution_id:
                return execution
        return None
    mock_service.get_execution_by_id.side_effect = get_execution_by_id
    
    # Execute pipeline
    def execute_pipeline(pipeline_id, params=None):
        new_execution = PipelineExecution(
            execution_id=str(uuid.uuid4()),
            pipeline_id=pipeline_id,
            status="RUNNING",
            start_time=datetime.datetime.now()
        )
        return new_execution
    mock_service.execute_pipeline.side_effect = execute_pipeline
    
    return mock_service

def create_mock_healing_service(patterns=None, actions=None, executions=None):
    """
    Creates a mock healing service for API testing.
    
    Args:
        patterns: Optional list of healing patterns to return
        actions: Optional list of healing actions to return
        executions: Optional list of healing executions to return
        
    Returns:
        Mock healing service
    """
    mock_service = MagicMock()
    
    # Set up default values
    patterns = patterns or []
    actions = actions or []
    executions = executions or []
    
    # Configure mock methods
    
    # Get healing patterns
    mock_service.get_healing_patterns.return_value = (patterns, len(patterns))
    
    # Get healing pattern by ID
    def get_healing_pattern_by_id(pattern_id):
        for pattern in patterns:
            if pattern.pattern_id == pattern_id:
                return pattern
        return None
    mock_service.get_healing_pattern_by_id.side_effect = get_healing_pattern_by_id
    
    # Create healing pattern
    def create_healing_pattern(pattern_data):
        new_pattern = IssuePattern(
            pattern_id=str(uuid.uuid4()),
            **pattern_data.dict()
        )
        return new_pattern
    mock_service.create_healing_pattern.side_effect = create_healing_pattern
    
    # Update healing pattern
    def update_healing_pattern(pattern_id, pattern_data):
        pattern = get_healing_pattern_by_id(pattern_id)
        if not pattern:
            return None
        
        # In a real implementation, we would update pattern fields here
        return pattern
    mock_service.update_healing_pattern.side_effect = update_healing_pattern
    
    # Delete healing pattern
    mock_service.delete_healing_pattern.return_value = True
    
    # Get healing actions
    mock_service.get_healing_actions.return_value = (actions, len(actions))
    
    # Get healing action by ID
    def get_healing_action_by_id(action_id):
        for action in actions:
            if action.action_id == action_id:
                return action
        return None
    mock_service.get_healing_action_by_id.side_effect = get_healing_action_by_id
    
    # Create healing action
    def create_healing_action(action_data):
        new_action = HealingAction(
            action_id=str(uuid.uuid4()),
            **action_data.dict()
        )
        return new_action
    mock_service.create_healing_action.side_effect = create_healing_action
    
    # Update healing action
    def update_healing_action(action_id, action_data):
        action = get_healing_action_by_id(action_id)
        if not action:
            return None
        
        # In a real implementation, we would update action fields here
        return action
    mock_service.update_healing_action.side_effect = update_healing_action
    
    # Delete healing action
    mock_service.delete_healing_action.return_value = True
    
    # Get healing executions
    mock_service.get_healing_executions.return_value = (executions, len(executions))
    
    # Get healing execution by ID
    def get_healing_execution_by_id(execution_id):
        for execution in executions:
            if execution.healing_id == execution_id:
                return execution
        return None
    mock_service.get_healing_execution_by_id.side_effect = get_healing_execution_by_id
    
    # Execute manual healing
    def execute_manual_healing(issue_id, action_id, params=None):
        new_execution = {
            "healing_id": str(uuid.uuid4()),
            "success": True,
            "execution_details": {"message": "Healing action executed successfully"}
        }
        return new_execution
    mock_service.execute_manual_healing.side_effect = execute_manual_healing
    
    return mock_service

def create_mock_monitoring_service(metrics=None, alerts=None, anomalies=None):
    """
    Creates a mock monitoring service for API testing.
    
    Args:
        metrics: Optional list of metrics to return
        alerts: Optional list of alerts to return
        anomalies: Optional list of anomalies to return
        
    Returns:
        Mock monitoring service
    """
    mock_service = MagicMock()
    
    # Set up default values
    metrics = metrics or []
    alerts = alerts or []
    anomalies = anomalies or []
    
    # Configure mock methods
    
    # Get metrics
    mock_service.get_metrics.return_value = (metrics, len(metrics))
    
    # Get metric by ID
    def get_metric_by_id(metric_id):
        for metric in metrics:
            if metric.metric_id == metric_id:
                return metric
        return None
    mock_service.get_metric_by_id.side_effect = get_metric_by_id
    
    # Get metric time series
    def get_metric_time_series(metric_name, start_time, end_time, filters=None):
        return {
            "metric_name": metric_name,
            "data_points": [
                {"timestamp": "2023-01-01T00:00:00Z", "value": 10.0},
                {"timestamp": "2023-01-01T01:00:00Z", "value": 15.0},
                {"timestamp": "2023-01-01T02:00:00Z", "value": 12.5}
            ],
            "statistics": {
                "min": 10.0,
                "max": 15.0,
                "avg": 12.5,
                "count": 3
            }
        }
    mock_service.get_metric_time_series.side_effect = get_metric_time_series
    
    # Get alerts
    mock_service.get_alerts.return_value = (alerts, len(alerts))
    
    # Get alert by ID
    def get_alert_by_id(alert_id):
        for alert in alerts:
            if alert.alert_id == alert_id:
                return alert
        return None
    mock_service.get_alert_by_id.side_effect = get_alert_by_id
    
    # Acknowledge alert
    def acknowledge_alert(alert_id, acknowledge_data=None):
        return {"alert_id": alert_id, "acknowledged": True, "acknowledged_at": datetime.datetime.now().isoformat()}
    mock_service.acknowledge_alert.side_effect = acknowledge_alert
    
    # Resolve alert
    def resolve_alert(alert_id, resolve_data=None):
        return {"alert_id": alert_id, "resolved": True, "resolved_at": datetime.datetime.now().isoformat()}
    mock_service.resolve_alert.side_effect = resolve_alert
    
    # Get anomalies
    mock_service.get_anomalies.return_value = (anomalies, len(anomalies))
    
    # Get anomaly by ID
    def get_anomaly_by_id(anomaly_id):
        for anomaly in anomalies:
            if anomaly.anomaly_id == anomaly_id:
                return anomaly
        return None
    mock_service.get_anomaly_by_id.side_effect = get_anomaly_by_id
    
    # Get system metrics
    def get_system_metrics():
        return {
            "cpu_utilization": 45.2,
            "memory_utilization": 62.8,
            "disk_usage": 38.7,
            "pipeline_success_rate": 95.5,
            "active_pipelines": 12,
            "active_alerts": 3
        }
    mock_service.get_system_metrics.side_effect = get_system_metrics
    
    return mock_service

def create_test_quality_rule(rule_id=None, rule_type="SCHEMA", target_dataset="test_dataset", 
                            target_table="test_table", parameters=None):
    """
    Creates a test quality rule for API testing.
    
    Args:
        rule_id: Optional rule ID
        rule_type: Rule type
        target_dataset: Target dataset
        target_table: Target table
        parameters: Rule parameters
        
    Returns:
        Test quality rule
    """
    if rule_id is None:
        rule_id = f"rule_{uuid.uuid4()}"
    
    if parameters is None:
        parameters = {"columns": ["id", "name", "value"]}
    
    return QualityRule(
        rule_id=rule_id,
        rule_name=f"Test Rule {rule_id[-6:]}",
        target_dataset=target_dataset,
        target_table=target_table,
        rule_type=rule_type,
        expectation_type="expect_column_values_to_not_be_null",
        rule_definition=parameters,
        severity=AlertSeverity.HIGH,
        is_active=True,
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now()
    )

def create_test_quality_validation(validation_id=None, rule_id="rule_001", execution_id="exec_001",
                                  status=ValidationStatus.PASSED, details=None):
    """
    Creates a test quality validation for API testing.
    
    Args:
        validation_id: Optional validation ID
        rule_id: Rule ID
        execution_id: Execution ID
        status: Validation status
        details: Validation details
        
    Returns:
        Test quality validation
    """
    if validation_id is None:
        validation_id = f"val_{uuid.uuid4()}"
    
    if details is None:
        if status == ValidationStatus.PASSED:
            details = {"message": "Validation passed successfully"}
        else:
            details = {"message": "Validation failed", "failed_records": 5}
    
    return QualityValidation(
        validation_id=validation_id,
        rule_id=rule_id,
        execution_id=execution_id,
        status=status,
        validation_time=datetime.datetime.now(),
        validation_results=details,
        success_percent=95.0 if status == ValidationStatus.PASSED else 65.0,
        records_validated=100,
        records_failed=5 if status == ValidationStatus.PASSED else 35
    )

def create_test_pipeline(pipeline_id=None, name=None, source_id="source_001", 
                        target_dataset="test_dataset", target_table="test_table"):
    """
    Creates a test pipeline for API testing.
    
    Args:
        pipeline_id: Optional pipeline ID
        name: Pipeline name
        source_id: Source system ID
        target_dataset: Target dataset
        target_table: Target table
        
    Returns:
        Test pipeline definition
    """
    if pipeline_id is None:
        pipeline_id = f"pipe_{uuid.uuid4()}"
    
    if name is None:
        name = f"Test Pipeline {pipeline_id[-6:]}"
    
    return PipelineDefinition(
        pipeline_id=pipeline_id,
        pipeline_name=name,
        source_id=source_id,
        target_dataset=target_dataset,
        target_table=target_table,
        configuration={
            "schedule": "0 0 * * *",
            "extraction_params": {
                "batch_size": 1000,
                "incremental": True
            }
        },
        is_active=True,
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now()
    )

def create_test_healing_action(action_id=None, pattern_id="pattern_001", action_type=HealingActionType.DATA_CORRECTION,
                              action_definition=None):
    """
    Creates a test healing action for API testing.
    
    Args:
        action_id: Optional action ID
        pattern_id: Pattern ID
        action_type: Action type
        action_definition: Action definition
        
    Returns:
        Test healing action
    """
    if action_id is None:
        action_id = f"action_{uuid.uuid4()}"
    
    if action_definition is None:
        if action_type == HealingActionType.DATA_CORRECTION:
            action_definition = {
                "correction_type": "imputation",
                "strategy": "mean"
            }
        elif action_type == HealingActionType.PIPELINE_RETRY:
            action_definition = {
                "max_retries": 3,
                "backoff_factor": 2.0
            }
        else:
            action_definition = {
                "parameters": {"param1": "value1"}
            }
    
    return HealingAction(
        action_id=action_id,
        pattern_id=pattern_id,
        action_type=action_type,
        action_definition=action_definition,
        is_active=True,
        success_rate=0.95,
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now()
    )

def create_test_alert(alert_id=None, alert_type="DATA_QUALITY", severity=AlertSeverity.HIGH,
                     message="Data quality validation failed", source="quality_validation", source_id="val_002"):
    """
    Creates a test alert for API testing.
    
    Args:
        alert_id: Optional alert ID
        alert_type: Alert type
        severity: Alert severity
        message: Alert message
        source: Alert source
        source_id: Source ID
        
    Returns:
        Test alert
    """
    if alert_id is None:
        alert_id = f"alert_{uuid.uuid4()}"
    
    return Alert(
        alert_id=alert_id,
        alert_type=alert_type,
        severity=severity,
        message=message,
        created_at=datetime.datetime.now(),
        source_id=source_id,
        status="ACTIVE",
        acknowledged_by=None,
        acknowledged_at=None,
        resolved_by=None,
        resolved_at=None
    )

class TestAPIClient:
    """
    Test client for API testing with preconfigured mock services.
    """
    
    def __init__(self, mock_services=None):
        """
        Initialize the test API client with mock services.
        
        Args:
            mock_services: Dictionary of mock services to inject
        """
        self.mock_services = mock_services or {}
        self.app = create_test_app(self.mock_services)
        self.client = TestClient(self.app)
        
        # Configure default headers
        self.client.headers.update({
            "Content-Type": "application/json",
            "X-Test-Client": "True"
        })
    
    def get_quality_rules(self, page=1, page_size=20, filters=None):
        """
        Test the quality rules endpoint.
        
        Args:
            page: Page number
            page_size: Page size
            filters: Optional filters
            
        Returns:
            API response
        """
        params = {
            "page": page,
            "page_size": page_size
        }
        
        if filters:
            params.update(filters)
        
        response = self.client.get("/api/quality/rules", params=params)
        return response.json()
    
    def get_quality_rule(self, rule_id):
        """
        Test the quality rule by ID endpoint.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            API response
        """
        response = self.client.get(f"/api/quality/rules/{rule_id}")
        return response.json()
    
    def create_quality_rule(self, rule_data):
        """
        Test the create quality rule endpoint.
        
        Args:
            rule_data: Rule data
            
        Returns:
            API response
        """
        response = self.client.post("/api/quality/rules", json=rule_data)
        return response.json()
    
    def update_quality_rule(self, rule_id, rule_data):
        """
        Test the update quality rule endpoint.
        
        Args:
            rule_id: Rule ID
            rule_data: Rule data
            
        Returns:
            API response
        """
        response = self.client.put(f"/api/quality/rules/{rule_id}", json=rule_data)
        return response.json()
    
    def delete_quality_rule(self, rule_id):
        """
        Test the delete quality rule endpoint.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            API response
        """
        response = self.client.delete(f"/api/quality/rules/{rule_id}")
        return response.json()
    
    def execute_validation(self, dataset, table, rule_ids=None):
        """
        Test the execute validation endpoint.
        
        Args:
            dataset: Dataset name
            table: Table name
            rule_ids: Optional list of rule IDs
            
        Returns:
            API response
        """
        request_data = {
            "dataset": dataset,
            "table": table,
            "rule_ids": rule_ids or []
        }
        
        response = self.client.post("/api/quality/validate", json=request_data)
        return response.json()

class TestAPIData:
    """
    Class providing test data for API testing.
    """
    
    def __init__(self):
        """
        Initialize test API data.
        """
        # Convert sample data to proper model instances
        self.quality_rules = [
            create_test_quality_rule(
                rule_id=rule["rule_id"],
                rule_type=rule["rule_type"],
                target_dataset=rule["target_dataset"],
                target_table=rule["target_table"],
                parameters=rule["parameters"]
            )
            for rule in SAMPLE_QUALITY_RULES
        ]
        
        self.quality_validations = [
            create_test_quality_validation(
                validation_id=val["validation_id"],
                rule_id=val["rule_id"],
                execution_id=val["execution_id"],
                status=ValidationStatus.PASSED if val["status"] == "PASSED" else ValidationStatus.FAILED,
                details=val["details"]
            )
            for val in SAMPLE_QUALITY_VALIDATIONS
        ]
        
        self.pipelines = [
            create_test_pipeline(
                pipeline_id=pipe["pipeline_id"],
                name=pipe.get("name", f"Test Pipeline {pipe['pipeline_id'][-6:]}"),
                source_id=pipe["source_id"],
                target_dataset=pipe["target_dataset"],
                target_table=pipe["target_table"]
            )
            for pipe in SAMPLE_PIPELINES
        ]
        
        self.pipeline_executions = []
        
        self.healing_patterns = []
        
        self.healing_actions = [
            create_test_healing_action(
                action_id=action["action_id"],
                pattern_id=action["pattern_id"],
                action_type=HealingActionType(action["action_type"]),
                action_definition=action["action_definition"]
            )
            for action in SAMPLE_HEALING_ACTIONS
        ]
        
        self.healing_executions = []
        
        self.alerts = [
            create_test_alert(
                alert_id=alert["alert_id"],
                alert_type=alert["alert_type"],
                severity=AlertSeverity(alert["severity"]),
                message=alert["message"],
                source=alert["source"],
                source_id=alert["source_id"]
            )
            for alert in SAMPLE_ALERTS
        ]
        
        self.metrics = []
    
    def get_quality_rule_by_id(self, rule_id):
        """
        Get a test quality rule by ID.
        
        Args:
            rule_id: Rule ID
            
        Returns:
            The rule with the specified ID or None
        """
        for rule in self.quality_rules:
            if rule.rule_id == rule_id:
                return rule
        return None
    
    def get_validation_by_id(self, validation_id):
        """
        Get a test validation by ID.
        
        Args:
            validation_id: Validation ID
            
        Returns:
            The validation with the specified ID or None
        """
        for validation in self.quality_validations:
            if validation.validation_id == validation_id:
                return validation
        return None
    
    def get_pipeline_by_id(self, pipeline_id):
        """
        Get a test pipeline by ID.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            The pipeline with the specified ID or None
        """
        for pipeline in self.pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline
        return None
    
    def get_healing_action_by_id(self, action_id):
        """
        Get a test healing action by ID.
        
        Args:
            action_id: Action ID
            
        Returns:
            The healing action with the specified ID or None
        """
        for action in self.healing_actions:
            if action.action_id == action_id:
                return action
        return None
    
    def get_alert_by_id(self, alert_id):
        """
        Get a test alert by ID.
        
        Args:
            alert_id: Alert ID
            
        Returns:
            The alert with the specified ID or None
        """
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                return alert
        return None

@pytest.fixture
def mock_quality_service():
    """Pytest fixture providing a mock quality service."""
    test_data = TestAPIData()
    return create_mock_quality_service(
        quality_rules=test_data.quality_rules,
        quality_validations=test_data.quality_validations
    )

@pytest.fixture
def mock_pipeline_service():
    """Pytest fixture providing a mock pipeline service."""
    test_data = TestAPIData()
    return create_mock_pipeline_service(
        pipelines=test_data.pipelines,
        executions=test_data.pipeline_executions
    )

@pytest.fixture
def mock_healing_service():
    """Pytest fixture providing a mock healing service."""
    test_data = TestAPIData()
    return create_mock_healing_service(
        patterns=test_data.healing_patterns,
        actions=test_data.healing_actions,
        executions=test_data.healing_executions
    )

@pytest.fixture
def mock_monitoring_service():
    """Pytest fixture providing a mock monitoring service."""
    test_data = TestAPIData()
    return create_mock_monitoring_service(
        metrics=test_data.metrics,
        alerts=test_data.alerts
    )

@pytest.fixture
def test_api_client(mock_quality_service, mock_pipeline_service, mock_healing_service, mock_monitoring_service):
    """Pytest fixture providing a configured test API client."""
    mock_services = {
        "quality_service": mock_quality_service,
        "pipeline_service": mock_pipeline_service,
        "healing_service": mock_healing_service,
        "monitoring_service": mock_monitoring_service
    }
    return TestAPIClient(mock_services)

@pytest.fixture
def test_api_data():
    """Pytest fixture providing test API data."""
    return TestAPIData()