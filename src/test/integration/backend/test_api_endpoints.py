import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
import datetime
import uuid
import json  # Import the json module

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

from src.backend.api.app import create_app
from src.test.fixtures.backend.api_fixtures import create_test_app, create_mock_quality_service, create_mock_pipeline_service, create_mock_healing_service, create_mock_monitoring_service, TestAPIClient, TestAPIData
from src.test.utils.api_test_utils import assert_successful_response, assert_error_response, assert_pagination_response, APIResponseValidator
from src.backend.constants import ValidationStatus, AlertSeverity, DataSourceType, HealingActionType

API_PREFIX = "/api"

def setup_test_client():
    """Creates a test client with mock services for API testing"""
    mock_services = {
        "quality_service": create_mock_quality_service(),
        "pipeline_service": create_mock_pipeline_service(),
        "healing_service": create_mock_healing_service(),
        "monitoring_service": create_mock_monitoring_service()
    }
    test_app = create_test_app(mock_services)
    test_client = TestAPIClient(test_app)
    return test_client, mock_services

class TestQualityEndpoints:
    """Tests for the quality-related API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None
        self.validator = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()
        self.validator = APIResponseValidator()

    def test_get_quality_rules(self):
        """Test the GET /api/quality/rules endpoint"""
        response = self.client.get_quality_rules()
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["quality_service"].get_quality_rules.assert_called_once()

    def test_get_quality_rule_by_id(self):
        """Test the GET /api/quality/rules/{rule_id} endpoint"""
        rule_id = "rule_001"
        response = self.client.get_quality_rule(rule_id)
        assert_successful_response(response)
        self.validator.validate_quality_rule_response(response["data"])
        self.mock_services["quality_service"].get_quality_rule_by_id.assert_called_once_with(rule_id)

    def test_create_quality_rule(self):
        """Test the POST /api/quality/rules endpoint"""
        rule_data = {"rule_name": "Test Rule", "rule_type": "SCHEMA", "target_dataset": "test_dataset", "target_table": "test_table", "parameters": {}}
        response = self.client.create_quality_rule(rule_data)
        assert_successful_response(response, expected_status_code=201)
        self.validator.validate_quality_rule_response(response["data"])
        self.mock_services["quality_service"].create_quality_rule.assert_called_once()

    def test_update_quality_rule(self):
        """Test the PUT /api/quality/rules/{rule_id} endpoint"""
        rule_id = "rule_001"
        rule_data = {"rule_name": "Updated Rule"}
        response = self.client.update_quality_rule(rule_id, rule_data)
        assert_successful_response(response)
        self.validator.validate_quality_rule_response(response["data"])
        self.mock_services["quality_service"].update_quality_rule.assert_called_once()

    def test_delete_quality_rule(self):
        """Test the DELETE /api/quality/rules/{rule_id} endpoint"""
        rule_id = "rule_001"
        response = self.client.delete_quality_rule(rule_id)
        assert_successful_response(response)
        assert "deleted" in response["data"]
        self.mock_services["quality_service"].delete_quality_rule.assert_called_once_with(rule_id)

    def test_execute_validation(self):
        """Test the POST /api/quality/validate endpoint"""
        request_data = {"dataset": "test_dataset", "table": "test_table"}
        response = self.client.client.post("/api/quality/validate", json=request_data)
        assert_successful_response(response)
        self.mock_services["quality_service"].execute_validation.assert_called_once()

    def test_get_quality_score(self):
        """Test the GET /api/quality/score endpoint"""
        response = self.client.client.get("/api/quality/score?dataset=test_dataset")
        assert_successful_response(response)
        assert "overall_score" in response
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

    def test_get_quality_issues(self):
        """Test the GET /api/quality/issues endpoint"""
        response = self.client.client.get("/api/quality/issues?dataset=test_dataset")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["quality_service"].get_quality_issues.assert_called_once()

    def test_quality_rule_not_found(self):
        """Test error handling for non-existent quality rule"""
        self.mock_services["quality_service"].get_quality_rule_by_id.side_effect = ResourceNotFoundError("QualityRule", "non_existent_id")
        response = self.client.get_quality_rule("non_existent_id")
        assert_error_response(response, expected_status_code=404)

    def test_quality_validation_error(self):
        """Test error handling for validation errors"""
        self.mock_services["quality_service"].create_quality_rule.side_effect = ValidationError("Invalid rule data", [])
        rule_data = {"rule_name": "Invalid Rule", "rule_type": "INVALID", "target_dataset": "test_dataset", "target_table": "test_table", "parameters": {}}
        response = self.client.create_quality_rule(rule_data)
        assert_error_response(response, expected_status_code=422)

class TestHealingEndpoints:
    """Tests for the self-healing related API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None
        self.validator = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()
        self.validator = APIResponseValidator()

    def test_get_healing_patterns(self):
        """Test the GET /api/healing/patterns endpoint"""
        response = self.client.client.get("/api/healing/patterns")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["healing_service"].get_healing_patterns.assert_called_once()

    def test_get_healing_pattern_by_id(self):
        """Test the GET /api/healing/patterns/{pattern_id} endpoint"""
        pattern_id = "pattern_001"
        response = self.client.client.get(f"/api/healing/patterns/{pattern_id}")
        assert_successful_response(response)
        self.mock_services["healing_service"].get_healing_pattern_by_id.assert_called_once_with(pattern_id)

    def test_create_healing_pattern(self):
        """Test the POST /api/healing/patterns endpoint"""
        request_data = {"issue_type": "data_quality", "detection_pattern": {}, "confidence_threshold": 0.8}
        response = self.client.client.post("/api/healing/patterns", json=request_data)
        assert_successful_response(response, expected_status_code=201)
        self.mock_services["healing_service"].create_healing_pattern.assert_called_once()

    def test_get_healing_actions(self):
        """Test the GET /api/healing/actions endpoint"""
        response = self.client.client.get("/api/healing/actions")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["healing_service"].get_healing_actions.assert_called_once()

    def test_get_healing_action_by_id(self):
        """Test the GET /api/healing/actions/{action_id} endpoint"""
        action_id = "action_001"
        response = self.client.client.get(f"/api/healing/actions/{action_id}")
        assert_successful_response(response)
        self.mock_services["healing_service"].get_healing_action_by_id.assert_called_once_with(action_id)

    def test_create_healing_action(self):
        """Test the POST /api/healing/actions endpoint"""
        request_data = {"pattern_id": "pattern_001", "action_type": "DATA_CORRECTION", "action_definition": {}}
        response = self.client.client.post("/api/healing/actions", json=request_data)
        assert_successful_response(response, expected_status_code=201)
        self.mock_services["healing_service"].create_healing_action.assert_called_once()

    def test_get_healing_executions(self):
        """Test the GET /api/healing/executions endpoint"""
        response = self.client.client.get("/api/healing/executions")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["healing_service"].get_healing_executions.assert_called_once()

    def test_execute_manual_healing(self):
        """Test the POST /api/healing/execute endpoint"""
        request_data = {"issue_id": "issue_001", "action_id": "action_001"}
        response = self.client.client.post("/api/healing/execute", json=request_data)
        assert_successful_response(response)
        self.mock_services["healing_service"].execute_manual_healing.assert_called_once()

    def test_get_healing_config(self):
        """Test the GET /api/healing/config endpoint"""
        response = self.client.client.get("/api/healing/config")
        assert_successful_response(response)
        self.mock_services["healing_service"].get_healing_config.assert_called_once()

    def test_update_healing_config(self):
        """Test the PUT /api/healing/config endpoint"""
        request_data = {"healing_mode": "AUTOMATIC", "global_confidence_threshold": 0.9}
        response = self.client.client.put("/api/healing/config", json=request_data)
        assert_successful_response(response)
        self.mock_services["healing_service"].update_healing_config.assert_called_once()

class TestMonitoringEndpoints:
    """Tests for the monitoring and alerting related API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()

    def test_get_alerts(self):
        """Test the GET /api/monitoring/alerts endpoint"""
        response = self.client.client.get("/api/monitoring/alerts")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["monitoring_service"].get_alerts.assert_called_once()

    def test_get_alert_by_id(self):
        """Test the GET /api/monitoring/alerts/{alert_id} endpoint"""
        alert_id = "alert_001"
        response = self.client.client.get(f"/api/monitoring/alerts/{alert_id}")
        assert_successful_response(response)
        self.mock_services["monitoring_service"].get_alert_by_id.assert_called_once_with(alert_id)

    def test_acknowledge_alert(self):
        """Test the POST /api/monitoring/alerts/{alert_id}/acknowledge endpoint"""
        alert_id = "alert_001"
        response = self.client.client.post(f"/api/monitoring/alerts/{alert_id}/acknowledge")
        assert_successful_response(response)
        self.mock_services["monitoring_service"].acknowledge_alert.assert_called_once()

    def test_resolve_alert(self):
        """Test the POST /api/monitoring/alerts/{alert_id}/resolve endpoint"""
        alert_id = "alert_001"
        response = self.client.client.post(f"/api/monitoring/alerts/{alert_id}/resolve")
        assert_successful_response(response)
        self.mock_services["monitoring_service"].resolve_alert.assert_called_once()

    def test_get_metrics(self):
        """Test the GET /api/monitoring/metrics endpoint"""
        response = self.client.client.get("/api/monitoring/metrics")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["monitoring_service"].get_metrics.assert_called_once()

    def test_get_metric_time_series(self):
        """Test the GET /api/monitoring/metrics/{metric_name}/timeseries endpoint"""
        metric_name = "cpu_utilization"
        response = self.client.client.get(f"/api/monitoring/metrics/timeseries/{metric_name}")
        assert_successful_response(response)
        self.mock_services["monitoring_service"].get_metric_time_series.assert_called_once_with(metric_name, None, None, None, None)

    def test_get_system_metrics(self):
        """Test the GET /api/monitoring/system endpoint"""
        response = self.client.client.get("/api/monitoring/system")
        assert_successful_response(response)
        self.mock_services["monitoring_service"].get_system_metrics.assert_called_once()

class TestPipelineEndpoints:
    """Tests for the pipeline management related API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()

    def test_get_pipelines(self):
        """Test the GET /api/ingestion/pipelines endpoint"""
        response = self.client.client.get("/api/ingestion/pipelines")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["pipeline_service"].get_pipelines.assert_called_once()

    def test_get_pipeline_by_id(self):
        """Test the GET /api/ingestion/pipelines/{pipeline_id} endpoint"""
        pipeline_id = "pipe_001"
        response = self.client.client.get(f"/api/ingestion/pipelines/{pipeline_id}")
        assert_successful_response(response)
        self.mock_services["pipeline_service"].get_pipeline_by_id.assert_called_once_with(pipeline_id)

    def test_create_pipeline(self):
        """Test the POST /api/ingestion/pipelines endpoint"""
        request_data = {"pipeline_name": "Test Pipeline", "source_id": "source_001", "target_dataset": "test_dataset", "target_table": "test_table", "configuration": {}}
        response = self.client.client.post("/api/ingestion/pipelines", json=request_data)
        assert_successful_response(response, expected_status_code=201)
        self.mock_services["pipeline_service"].create_pipeline.assert_called_once()

    def test_update_pipeline(self):
        """Test the PUT /api/ingestion/pipelines/{pipeline_id} endpoint"""
        pipeline_id = "pipe_001"
        request_data = {"pipeline_name": "Updated Pipeline"}
        response = self.client.client.put(f"/api/ingestion/pipelines/{pipeline_id}", json=request_data)
        assert_successful_response(response)
        self.mock_services["pipeline_service"].update_pipeline.assert_called_once()

    def test_delete_pipeline(self):
        """Test the DELETE /api/ingestion/pipelines/{pipeline_id} endpoint"""
        pipeline_id = "pipe_001"
        response = self.client.client.delete(f"/api/ingestion/pipelines/{pipeline_id}")
        assert_successful_response(response)
        self.mock_services["pipeline_service"].delete_pipeline.assert_called_once_with(pipeline_id)

    def test_execute_pipeline(self):
        """Test the POST /api/ingestion/pipelines/{pipeline_id}/execute endpoint"""
        pipeline_id = "pipe_001"
        response = self.client.client.post(f"/api/ingestion/pipelines/{pipeline_id}/execute")
        assert_successful_response(response)
        self.mock_services["pipeline_service"].execute_pipeline.assert_called_once()

    def test_get_pipeline_executions(self):
        """Test the GET /api/ingestion/executions endpoint"""
        response = self.client.client.get("/api/ingestion/executions")
        assert_successful_response(response)
        assert_pagination_response(response)
        self.mock_services["pipeline_service"].get_pipeline_executions.assert_called_once()

    def test_get_execution_by_id(self):
        """Test the GET /api/ingestion/executions/{execution_id} endpoint"""
        execution_id = "exec_001"
        response = self.client.client.get(f"/api/ingestion/executions/{execution_id}")
        assert_successful_response(response)
        self.mock_services["pipeline_service"].get_execution_by_id.assert_called_once_with(execution_id)

class TestOptimizationEndpoints:
    """Tests for the performance optimization related API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()

    def test_get_query_recommendations(self):
        """Test the GET /api/optimization/query endpoint"""
        query = "SELECT * FROM test_dataset.test_table"
        response = self.client.client.get(f"/api/optimization/query?query={query}")
        assert_successful_response(response)
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

    def test_get_schema_recommendations(self):
        """Test the GET /api/optimization/schema endpoint"""
        dataset = "test_dataset"
        table = "test_table"
        response = self.client.client.get(f"/api/optimization/schema?dataset={dataset}&table={table}")
        assert_successful_response(response)
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

    def test_apply_optimization(self):
        """Test the POST /api/optimization/apply endpoint"""
        request_data = {"optimization_type": "query", "target_resource": "test_dataset.test_table", "recommendations": []}
        response = self.client.client.post("/api/optimization/apply", json=request_data)
        assert_successful_response(response)
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

    def test_get_resource_utilization(self):
        """Test the GET /api/optimization/resources endpoint"""
        response = self.client.client.get("/api/optimization/resources")
        assert_successful_response(response)
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

    def test_get_cost_analysis(self):
        """Test the GET /api/optimization/cost endpoint"""
        response = self.client.client.get("/api/optimization/cost")
        assert_successful_response(response)
        self.mock_services["quality_service"].get_quality_score.assert_called_once()

class TestHealthEndpoints:
    """Tests for the health and system information API endpoints"""

    def __init__(self):
        """Initialize the test class"""
        self.client = None
        self.mock_services = None

    def setup_method(self):
        """Set up the test environment before each test method"""
        self.client, self.mock_services = setup_test_client()

    def test_health_endpoint(self):
        """Test the GET /health endpoint"""
        response = self.client.client.get("/health")
        assert_successful_response(response)
        assert "status" in response
        assert "message" in response
        assert "version" in response

    def test_version_endpoint(self):
        """Test the GET /version endpoint"""
        response = self.client.client.get("/version")
        assert_successful_response(response)
        assert "version" in response