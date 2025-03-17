import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
import datetime
import uuid
import json
from typing import List, Dict, Optional, Any, Union, Tuple

# Internal imports
from src.backend.api.services import admin_service, quality_service, healing_service, ingestion_service, monitoring_service, optimization_service
from src.backend.api.models import error_models
from src.backend.constants import ValidationStatus, AlertSeverity, HealingActionType

from src.test.fixtures.backend import api_fixtures

@pytest.fixture
def setup_admin_service_mocks():
    """Sets up mocks for admin service dependencies"""
    mock_firestore_client = Mock()
    mock_bigquery_client = Mock()
    mock_gcp_auth_verify_token = Mock()
    mock_gcp_auth_generate_token = Mock()
    mock_metric_client = Mock()

    return {
        "firestore_client": mock_firestore_client,
        "bigquery_client": mock_bigquery_client,
        "gcp_auth_verify_token": mock_gcp_auth_verify_token,
        "gcp_auth_generate_token": mock_gcp_auth_generate_token,
        "metric_client": mock_metric_client
    }

@pytest.fixture
def setup_quality_service_mocks():
    """Sets up mocks for quality service dependencies"""
    mock_bigquery_client = Mock()
    mock_quality_repository = Mock()
    mock_validation_engine = Mock()
    mock_quality_scorer = Mock()

    return {
        "bigquery_client": mock_bigquery_client,
        "quality_repository": mock_quality_repository,
        "validation_engine": mock_validation_engine,
        "quality_scorer": mock_quality_scorer
    }

@pytest.fixture
def setup_healing_service_mocks():
    """Sets up mocks for healing service dependencies"""
    mock_issue_classifier = Mock()
    mock_pattern_recognizer = Mock()
    mock_root_cause_analyzer = Mock()
    mock_data_corrector = Mock()
    mock_pipeline_adjuster = Mock()
    mock_healing_repository = Mock()

    return {
        "issue_classifier": mock_issue_classifier,
        "pattern_recognizer": mock_pattern_recognizer,
        "root_cause_analyzer": mock_root_cause_analyzer,
        "data_corrector": mock_data_corrector,
        "pipeline_adjuster": mock_pipeline_adjuster,
        "healing_repository": mock_healing_repository
    }

@pytest.fixture
def setup_ingestion_service_mocks():
    """Sets up mocks for ingestion service dependencies"""
    mock_source_repository = Mock()
    mock_pipeline_repository = Mock()
    mock_execution_repository = Mock()
    mock_composer_client = Mock()
    mock_get_connector_for_source_type = Mock()

    return {
        "source_repository": mock_source_repository,
        "pipeline_repository": mock_pipeline_repository,
        "execution_repository": mock_execution_repository,
        "composer_client": mock_composer_client,
        "get_connector_for_source_type": mock_get_connector_for_source_type
    }

@pytest.fixture
def setup_monitoring_service_mocks():
    """Sets up mocks for monitoring service dependencies"""
    mock_alert_repository = Mock()
    mock_metric_processor = Mock()
    mock_anomaly_detector = Mock()
    mock_alert_generator = Mock()
    mock_notification_router = Mock()
    mock_bigquery_client = Mock()
    mock_metric_client = Mock()

    return {
        "alert_repository": mock_alert_repository,
        "metric_processor": mock_metric_processor,
        "anomaly_detector": mock_anomaly_detector,
        "alert_generator": mock_alert_generator,
        "notification_router": mock_notification_router,
        "bigquery_client": mock_bigquery_client,
        "metric_client": mock_metric_client
    }

@pytest.fixture
def setup_optimization_service_mocks():
    """Sets up mocks for optimization service dependencies"""
    mock_query_optimizer = Mock()
    mock_schema_analyzer = Mock()
    mock_resource_optimizer = Mock()
    mock_bigquery_client = Mock()
    mock_metric_client = Mock()

    return {
        "query_optimizer": mock_query_optimizer,
        "schema_analyzer": mock_schema_analyzer,
        "resource_optimizer": mock_resource_optimizer,
        "bigquery_client": mock_bigquery_client,
        "metric_client": mock_metric_client
    }

class TestAdminService:
    """Test cases for admin service functions"""

    def test_get_users(self, setup_admin_service_mocks):
        """Test get_users function returns expected results"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.stream.return_value = [Mock(id="user1"), Mock(id="user2")]
        users, total_count = admin_service.get_users(page=1, page_size=10)
        assert len(users) == 0
        assert total_count == 0
        mock_firestore_client.collection.assert_called_with("users")

    def test_get_user_by_id_found(self, setup_admin_service_mocks):
        """Test get_user_by_id function when user exists"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.document.return_value.get.return_value.exists = True
        mock_firestore_client.collection.return_value.document.return_value.get.return_value.to_dict.return_value = {"username": "testuser"}
        user = admin_service.get_user_by_id(user_id="test_id")
        assert user["username"] == "testuser"
        mock_firestore_client.collection.return_value.document.assert_called_with("test_id")

    def test_get_user_by_id_not_found(self, setup_admin_service_mocks):
        """Test get_user_by_id function when user does not exist"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.document.return_value.get.return_value.exists = False
        with pytest.raises(error_models.ResourceNotFoundError):
            admin_service.get_user_by_id(user_id="test_id")
        mock_firestore_client.collection.return_value.document.assert_called_with("test_id")

    def test_create_user(self, setup_admin_service_mocks):
        """Test create_user function creates a user successfully"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.document.return_value.set.return_value = None
        user_data = {"username": "testuser", "email": "test@example.com", "password": "password", "role": "admin"}
        created_user = admin_service.create_user(user_data)
        assert created_user["username"] == "testuser"
        mock_firestore_client.collection.return_value.document.assert_called()

    def test_update_user(self, setup_admin_service_mocks):
        """Test update_user function updates a user successfully"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.document.return_value.get.return_value.exists = True
        mock_firestore_client.collection.return_value.document.return_value.update.return_value = None
        user_data = {"username": "newuser"}
        updated_user = admin_service.update_user(user_id="test_id", user_data=user_data)
        assert updated_user["username"] == "newuser"
        mock_firestore_client.collection.return_value.document.assert_called_with("test_id")

    def test_delete_user(self, setup_admin_service_mocks):
        """Test delete_user function deletes a user successfully"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.document.return_value.get.return_value.exists = True
        mock_firestore_client.collection.return_value.document.return_value.delete.return_value = None
        result = admin_service.delete_user(user_id="test_id")
        assert result["message"] == "User test_id deleted successfully"
        mock_firestore_client.collection.return_value.document.assert_called_with("test_id")

    def test_get_system_health(self, setup_admin_service_mocks):
        """Test get_system_health function returns system health information"""
        mock_metric_client = setup_admin_service_mocks["metric_client"]
        mock_metric_client.check_component_status.return_value = {"status": "OK"}
        health_info = admin_service.get_system_health()
        assert health_info["bigquery"]["status"] == "OK"
        mock_metric_client.check_component_status.assert_called()

    def test_authenticate_user_success(self, setup_admin_service_mocks):
        """Test authenticate_user function with valid credentials"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.where.return_value.limit.return_value.stream.return_value = [Mock(id="user1", to_dict=lambda: {"password_hash": "hashed_password"})]
        setup_admin_service_mocks["gcp_auth_generate_token"].return_value = "test_token"
        result = admin_service.authenticate_user(username_or_email="testuser", password="password")
        assert result["token"] == "test_token"
        mock_firestore_client.collection.return_value.where.assert_called()

    def test_authenticate_user_invalid(self, setup_admin_service_mocks):
        """Test authenticate_user function with invalid credentials"""
        mock_firestore_client = setup_admin_service_mocks["firestore_client"]
        mock_firestore_client.collection.return_value.where.return_value.limit.return_value.stream.return_value = [Mock(id="user1", to_dict=lambda: {"password_hash": "hashed_password"})]
        with pytest.raises(error_models.AuthorizationError):
            admin_service.authenticate_user(username_or_email="testuser", password="wrong_password")
        mock_firestore_client.collection.return_value.where.assert_called()

class TestQualityService:
    """Test cases for quality service functions"""

    def test_get_quality_rules(self, setup_quality_service_mocks):
        """Test get_quality_rules function returns expected results"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.get_all_rules.return_value = [Mock(), Mock()]
        rules, total_count = quality_service.get_quality_rules(page=1, page_size=10)
        assert len(rules) == 2
        assert total_count == 2
        mock_quality_repository.get_all_rules.assert_called()

    def test_get_quality_rule_by_id_found(self, setup_quality_service_mocks):
        """Test get_quality_rule_by_id function when rule exists"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.get_rule_by_id.return_value = Mock(rule_id="test_id")
        rule = quality_service.get_quality_rule_by_id(rule_id="test_id")
        assert rule.rule_id == "test_id"
        mock_quality_repository.get_rule_by_id.assert_called_with("test_id")

    def test_get_quality_rule_by_id_not_found(self, setup_quality_service_mocks):
        """Test get_quality_rule_by_id function when rule does not exist"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.get_rule_by_id.return_value = None
        rule = quality_service.get_quality_rule_by_id(rule_id="test_id")
        assert rule is None
        mock_quality_repository.get_rule_by_id.assert_called_with("test_id")

    def test_create_quality_rule(self, setup_quality_service_mocks):
        """Test create_quality_rule function creates a rule successfully"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.create_rule.return_value = Mock(rule_id="test_id")
        rule_data = Mock()
        rule = quality_service.create_quality_rule(rule_data)
        assert rule.rule_id == "test_id"
        mock_quality_repository.create_rule.assert_called()

    def test_update_quality_rule(self, setup_quality_service_mocks):
        """Test update_quality_rule function updates a rule successfully"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.get_rule_by_id.return_value = Mock(rule_id="test_id")
        mock_quality_repository.update_rule.return_value = Mock(rule_id="test_id")
        rule_data = Mock()
        rule = quality_service.update_quality_rule(rule_id="test_id", rule_update=rule_data)
        assert rule.rule_id == "test_id"
        mock_quality_repository.update_rule.assert_called()

    def test_delete_quality_rule(self, setup_quality_service_mocks):
        """Test delete_quality_rule function deletes a rule successfully"""
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_quality_repository.get_rule_by_id.return_value = Mock(rule_id="test_id")
        mock_quality_repository.delete_rule.return_value = True
        result = quality_service.delete_quality_rule(rule_id="test_id")
        assert result is True
        mock_quality_repository.delete_rule.assert_called_with("test_id")

    def test_execute_validation(self, setup_quality_service_mocks):
        """Test execute_validation function performs validation successfully"""
        mock_validation_engine = setup_quality_service_mocks["validation_engine"]
        mock_validation_engine.validate.return_value = Mock(success=True)
        result = quality_service.execute_validation(dataset="test_dataset")
        assert result.success is True
        mock_validation_engine.validate.assert_called()

    def test_quality_service_class(self, setup_quality_service_mocks):
        """Test QualityService class initialization and methods"""
        mock_bigquery_client = setup_quality_service_mocks["bigquery_client"]
        mock_quality_repository = setup_quality_service_mocks["quality_repository"]
        mock_validation_engine = setup_quality_service_mocks["validation_engine"]
        mock_quality_scorer = setup_quality_service_mocks["quality_scorer"]

        service = quality_service.QualityService(bq_client=mock_bigquery_client, config={})
        service.get_rules(page=1, page_size=10)
        mock_quality_repository.get_all_rules.assert_called()
        service.get_rule_by_id(rule_id="test_id")
        mock_quality_repository.get_rule_by_id.assert_called_with("test_id")
        service.create_rule(rule_data=Mock())
        mock_quality_repository.create_rule.assert_called()
        service.update_rule(rule_id="test_id", rule_update=Mock())
        mock_quality_repository.update_rule.assert_called()
        service.delete_rule(rule_id="test_id")
        mock_quality_repository.delete_rule.assert_called_with("test_id")
        service.execute_validation(dataset="test_dataset")
        mock_validation_engine.validate.assert_called()
        service.close()

class TestHealingService:
    """Test cases for healing service functions"""

    def test_get_healing_patterns(self, setup_healing_service_mocks):
        """Test get_healing_patterns function returns expected results"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.get_issue_patterns.return_value = [Mock(), Mock()]
        patterns, pagination = healing_service.get_healing_patterns(pagination={"page": 1, "page_size": 10}, issue_type="test_type")
        assert len(patterns) == 2
        assert pagination["total"] == 2
        mock_healing_repository.get_issue_patterns.assert_called()

    def test_get_healing_pattern_by_id(self, setup_healing_service_mocks):
        """Test get_healing_pattern_by_id function returns expected pattern"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.get_issue_pattern.return_value = Mock(pattern_id="test_id", to_dict=lambda: {"pattern_id": "test_id"})
        pattern = healing_service.get_healing_pattern_by_id(pattern_id="test_id")
        assert pattern["pattern_id"] == "test_id"
        mock_healing_repository.get_issue_pattern.assert_called_with("test_id")

    def test_create_healing_pattern(self, setup_healing_service_mocks):
        """Test create_healing_pattern function creates a pattern successfully"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.create_issue_pattern.return_value = Mock(pattern_id="test_id", to_dict=lambda: {"pattern_id": "test_id"})
        pattern_data = {"name": "test_pattern", "pattern_type": "test_type", "description": "test_description", "detection_pattern": {}}
        created_pattern = healing_service.create_healing_pattern(pattern_data)
        assert created_pattern["pattern_id"] == "test_id"
        mock_healing_repository.create_issue_pattern.assert_called()

    def test_update_healing_pattern(self, setup_healing_service_mocks):
        """Test update_healing_pattern function updates a pattern successfully"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.update_issue_pattern.return_value = Mock(pattern_id="test_id", to_dict=lambda: {"pattern_id": "test_id"})
        pattern_data = {"name": "new_pattern"}
        updated_pattern = healing_service.update_healing_pattern(pattern_id="test_id", pattern_data=pattern_data)
        assert updated_pattern["pattern_id"] == "test_id"
        mock_healing_repository.update_issue_pattern.assert_called_with("test_id", pattern_data)

    def test_delete_healing_pattern(self, setup_healing_service_mocks):
        """Test delete_healing_pattern function deletes a pattern successfully"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.delete_issue_pattern.return_value = True
        result = healing_service.delete_healing_pattern(pattern_id="test_id")
        assert result is True
        mock_healing_repository.delete_issue_pattern.assert_called_with("test_id")

    def test_get_healing_actions(self, setup_healing_service_mocks):
        """Test get_healing_actions function returns expected results"""
        mock_healing_repository = setup_healing_service_mocks["healing_repository"]
        mock_healing_repository.get_healing_actions.return_value = [Mock(), Mock()]
        actions, pagination = healing_service.get_healing_actions(pagination={"page": 1, "page_size": 10}, pattern_id="test_pattern", action_type="test_type", active_only=True)
        assert len(actions) == 2
        assert pagination["total"] == 2
        mock_healing_repository.get_healing_actions.assert_called()

    def test_execute_manual_healing(self, setup_healing_service_mocks):
        """Test execute_manual_healing function performs healing successfully"""
        result = healing_service.execute_manual_healing(healing_request={"issue_id": "test_issue", "action_id": "test_action"})
        assert result["status"] == "Not implemented"

class TestIngestionService:
    """Test cases for ingestion service functions"""

    def test_get_source_systems(self, setup_ingestion_service_mocks):
        """Test get_source_systems function returns expected results"""
        mock_source_repository = setup_ingestion_service_mocks["source_repository"]
        mock_source_repository.list_sources.return_value = [Mock(), Mock()]
        sources, total_count = ingestion_service.get_source_systems(pagination=Mock())
        assert len(sources) == 2
        assert total_count == 2
        mock_source_repository.list_sources.assert_called()

    def test_get_source_system(self, setup_ingestion_service_mocks):
        """Test get_source_system function returns expected source"""
        mock_source_repository = setup_ingestion_service_mocks["source_repository"]
        mock_source_repository.get_source_by_id.return_value = Mock(source_id="test_id")
        source = ingestion_service.get_source_system(source_id="test_id")
        assert source.source_id == "test_id"
        mock_source_repository.get_source_by_id.assert_called_with("test_id")

    def test_create_source_system(self, setup_ingestion_service_mocks):
        """Test create_source_system function creates a source successfully"""
        mock_source_repository = setup_ingestion_service_mocks["source_repository"]
        mock_source_repository.create_source.return_value = Mock(source_id="test_id")
        source_data = Mock()
        source = ingestion_service.create_source_system(source_data)
        assert source.source_id == "test_id"
        mock_source_repository.create_source.assert_called()

    def test_get_pipelines(self, setup_ingestion_service_mocks):
        """Test get_pipelines function returns expected results"""
        mock_pipeline_repository = setup_ingestion_service_mocks["pipeline_repository"]
        mock_pipeline_repository.search_pipelines.return_value = [Mock(), Mock()]
        pipelines, total_count = ingestion_service.get_pipelines(pagination=Mock())
        assert len(pipelines) == 2
        assert total_count == 2
        mock_pipeline_repository.search_pipelines.assert_called()

    def test_execute_pipeline(self, setup_ingestion_service_mocks):
        """Test execute_pipeline function executes a pipeline successfully"""
        mock_pipeline_repository = setup_ingestion_service_mocks["pipeline_repository"]
        mock_pipeline_repository.get_pipeline.return_value = Mock(pipeline_id="test_id", get_dag_config=lambda: {})
        mock_execution_repository = setup_ingestion_service_mocks["execution_repository"]
        mock_composer_client = setup_ingestion_service_mocks["composer_client"]
        mock_composer_client.trigger_dag.return_value = None
        execution = ingestion_service.execute_pipeline(pipeline_id="test_id", execute_request=Mock())
        assert execution.pipeline_id == "test_id"
        mock_pipeline_repository.get_pipeline.assert_called_with("test_id")
        mock_composer_client.trigger_dag.assert_called()

class TestMonitoringService:
    """Test cases for monitoring service functions"""

    def test_get_alerts(self, setup_monitoring_service_mocks):
        """Test get_alerts function returns expected results"""
        mock_alert_repository = setup_monitoring_service_mocks["alert_repository"]
        mock_alert_repository.search_alerts.return_value = [Mock(), Mock()]
        alerts, total_count = monitoring_service.get_alerts(page=1, page_size=10)
        assert len(alerts) == 2
        assert total_count == 2
        mock_alert_repository.search_alerts.assert_called()

    def test_get_alert_by_id(self, setup_monitoring_service_mocks):
        """Test get_alert_by_id function returns expected alert"""
        mock_alert_repository = setup_monitoring_service_mocks["alert_repository"]
        mock_alert_repository.get_alert.return_value = Mock(alert_id="test_id")
        alert = monitoring_service.get_alert_by_id(alert_id="test_id")
        assert alert.alert_id == "test_id"
        mock_alert_repository.get_alert.assert_called_with("test_id")

    def test_acknowledge_alert(self, setup_monitoring_service_mocks):
        """Test acknowledge_alert function acknowledges an alert successfully"""
        mock_alert_repository = setup_monitoring_service_mocks["alert_repository"]
        mock_alert_repository.acknowledge_alert.return_value = True
        result = monitoring_service.acknowledge_alert(alert_id="test_id", user_id="test_user")
        assert result is True
        mock_alert_repository.acknowledge_alert.assert_called_with(alert_id="test_id", acknowledged_by="test_user", notes=None)

    def test_get_dashboard_summary(self, setup_monitoring_service_mocks):
        """Test get_dashboard_summary function returns dashboard data"""
        mock_metric_processor = setup_monitoring_service_mocks["metric_processor"]
        mock_metric_processor.get_pipeline_health_metrics.return_value = {"status": "OK"}
        summary = monitoring_service.get_dashboard_summary()
        assert summary["pipeline_health"]["status"] == "OK"
        mock_metric_processor.get_pipeline_health_metrics.assert_called()

class TestOptimizationService:
    """Test cases for optimization service functions"""

    def test_get_query_optimization_recommendations(self, setup_optimization_service_mocks):
        """Test get_query_optimization_recommendations function returns recommendations"""
        mock_query_optimizer = setup_optimization_service_mocks["query_optimizer"]
        mock_query_optimizer.get_recommendations.return_value = ["recommendation1", "recommendation2"]
        recommendations = optimization_service.get_query_optimization_recommendations(query="test_query")
        assert len(recommendations) == 2
        mock_query_optimizer.get_recommendations.assert_called_with("test_query")

    def test_optimize_query(self, setup_optimization_service_mocks):
        """Test optimize_query function returns optimized query"""
        mock_query_optimizer = setup_optimization_service_mocks["query_optimizer"]
        mock_query_optimizer.optimize.return_value = "optimized_query"
        optimized_query = optimization_service.optimize_query(query="test_query")
        assert optimized_query == "optimized_query"
        mock_query_optimizer.optimize.assert_called_with("test_query")

    def test_get_schema_optimization_recommendations(self, setup_optimization_service_mocks):
        """Test get_schema_optimization_recommendations function returns recommendations"""
        mock_schema_analyzer = setup_optimization_service_mocks["schema_analyzer"]
        mock_schema_analyzer.get_recommendations.return_value = ["recommendation1", "recommendation2"]
        recommendations = optimization_service.get_schema_optimization_recommendations(dataset="test_dataset", table="test_table")
        assert len(recommendations) == 2
        mock_schema_analyzer.get_recommendations.assert_called_with("test_dataset", "test_table")

    def test_get_optimization_config(self, setup_optimization_service_mocks):
        """Test get_optimization_config function returns configuration"""
        config = optimization_service.get_optimization_config()
        assert config is not None