import unittest
from unittest.mock import MagicMock, patch
import datetime
import pandas as pd

from src.backend.optimization.resource.resource_optimizer import ResourceOptimizer, OptimizationAction, OptimizationStatus
from src.backend.optimization.resource.resource_monitor import ResourceType
from src.backend.optimization.resource.cost_tracker import CostOptimizationType


class TestResourceOptimizer(unittest.TestCase):
    """Test case for the ResourceOptimizer class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mock objects for BigQueryClient, MetricClient, ResourceMonitor, and CostTracker
        self.mock_bq_client = MagicMock()
        self.mock_metric_client = MagicMock()
        self.mock_resource_monitor = MagicMock()
        self.mock_cost_tracker = MagicMock()

        # Initialize ResourceOptimizer with mock dependencies
        self.optimizer = ResourceOptimizer(
            bq_client=self.mock_bq_client,
            metric_client=self.mock_metric_client,
            resource_monitor=self.mock_resource_monitor,
            cost_tracker=self.mock_cost_tracker
        )

        # Set up common test data and configurations
        self.test_project_id = "test-project"
        self.test_location = "us-central1"
        self.test_optimization_id = "test-optimization-123"
        self.test_confidence_threshold = 0.75

        # Configure mocks to return specific values for testing
        self.mock_resource_monitor.get_bigquery_resources.return_value = {}
        self.mock_resource_monitor.get_compute_resources.return_value = {}
        self.mock_resource_monitor.get_storage_resources.return_value = {}

    def tearDown(self):
        """Clean up after each test method"""
        # Reset all mocks
        self.mock_bq_client.reset_mock()
        self.mock_metric_client.reset_mock()
        self.mock_resource_monitor.reset_mock()
        self.mock_cost_tracker.reset_mock()

        # Clean up any test resources
        pass

    def test_init(self):
        """Test initialization of ResourceOptimizer"""
        # Verify ResourceOptimizer is initialized with correct dependencies
        self.assertEqual(self.optimizer._bq_client, self.mock_bq_client)
        self.assertEqual(self.optimizer._metric_client, self.mock_metric_client)
        self.assertEqual(self.optimizer._resource_monitor, self.mock_resource_monitor)
        self.assertEqual(self.optimizer._cost_tracker, self.mock_cost_tracker)

        # Check default values are set correctly
        self.assertEqual(self.optimizer._confidence_threshold, 0.85)

        # Verify project_id and location are set correctly
        self.assertEqual(self.optimizer._project_id, "test-project")
        self.assertEqual(self.optimizer._location, "us-central1")

    def test_get_optimization_recommendations(self):
        """Test getting optimization recommendations"""
        # Set up mock return values for optimize_bigquery_resources, optimize_compute_resources, optimize_storage_resources
        bq_recommendations = [
            {"id": "bq1", "confidence": 0.9, "impact": 100},
            {"id": "bq2", "confidence": 0.6, "impact": 50}
        ]
        compute_recommendations = [
            {"id": "compute1", "confidence": 0.8, "impact": 75}
        ]
        storage_recommendations = [
            {"id": "storage1", "confidence": 0.95, "impact": 120}
        ]

        self.optimizer.optimize_bigquery_resources = MagicMock(return_value=bq_recommendations)
        self.optimizer.optimize_compute_resources = MagicMock(return_value=compute_recommendations)
        self.optimizer.optimize_storage_resources = MagicMock(return_value=storage_recommendations)

        # Call get_optimization_recommendations
        recommendations = self.optimizer.get_optimization_recommendations()

        # Verify correct methods were called
        self.optimizer.optimize_bigquery_resources.assert_called_once()
        self.optimizer.optimize_compute_resources.assert_called_once()
        self.optimizer.optimize_storage_resources.assert_called_once()

        # Verify recommendations are filtered by confidence threshold
        filtered_recommendations = [r for r in recommendations if r["confidence"] >= self.optimizer._confidence_threshold]
        self.assertEqual(len(filtered_recommendations), 3)

        # Verify recommendations are sorted by impact
        sorted_recommendations = sorted(filtered_recommendations, key=lambda x: x["impact"], reverse=True)
        self.assertEqual(sorted_recommendations[0]["impact"], 120)
        self.assertEqual(sorted_recommendations[1]["impact"], 100)
        self.assertEqual(sorted_recommendations[2]["impact"], 75)

    def test_apply_optimization(self):
        """Test applying an optimization action"""
        # Create a test OptimizationAction
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Set up mock for specific optimization method (e.g., apply_bigquery_optimization)
        self.optimizer.apply_bigquery_optimization = MagicMock(return_value={"actual_savings": 90})

        # Call apply_optimization
        result = self.optimizer.apply_optimization(action)

        # Verify correct optimization method was called
        self.optimizer.apply_bigquery_optimization.assert_called_once_with(action)

        # Verify optimization status was updated
        self.assertEqual(action.status, OptimizationStatus.APPLIED)
        self.assertIsNotNone(action.applied_at)

        # Verify result contains actual impact metrics
        self.assertEqual(result["actual_savings"], 90)
        self.assertEqual(action.result_metrics["actual_savings"], 90)

    def test_apply_optimization_dry_run(self):
        """Test applying an optimization in dry run mode"""
        # Create a test OptimizationAction
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call apply_optimization with dry_run=True
        result = self.optimizer.apply_optimization(action, dry_run=True)

        # Verify no actual optimization method was called
        self.optimizer.apply_bigquery_optimization = MagicMock()
        self.optimizer.apply_bigquery_optimization.assert_not_called()

        # Verify optimization status was not updated
        self.assertEqual(action.status, OptimizationStatus.PENDING)
        self.assertIsNone(action.applied_at)

        # Verify result contains impact assessment
        self.assertEqual(result["predicted_impact"], 100)

    def test_schedule_optimization(self):
        """Test scheduling an optimization for future execution"""
        # Create a test OptimizationAction
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Set up future scheduled time
        scheduled_time = datetime.datetime.now() + datetime.timedelta(days=1)

        # Call schedule_optimization
        self.optimizer.schedule_optimization(action, scheduled_time)

        # Verify scheduling information is stored correctly
        self.assertEqual(action.scheduled_at, scheduled_time)

        # Verify optimization status is updated to SCHEDULED
        self.assertEqual(action.status, OptimizationStatus.SCHEDULED)

    def test_get_optimization_history(self):
        """Test retrieving optimization history"""
        # Set up mock return value for BigQueryClient.execute_query_to_dataframe
        history_data = pd.DataFrame([
            {"optimization_id": "1", "resource_type": "BIGQUERY", "action_type": "QUERY_OPTIMIZATION", "status": "APPLIED"},
            {"optimization_id": "2", "resource_type": "COMPUTE", "action_type": "RIGHT_SIZING", "status": "PENDING"}
        ])
        self.mock_bq_client.execute_query_to_dataframe.return_value = history_data

        # Call get_optimization_history with various filters
        history = self.optimizer.get_optimization_history(resource_type=ResourceType.BIGQUERY)

        # Verify correct query was executed
        self.mock_bq_client.execute_query_to_dataframe.assert_called_once()

        # Verify returned history matches expected format
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["optimization_id"], "1")
        self.assertEqual(history[1]["resource_type"], "COMPUTE")

    def test_revert_optimization(self):
        """Test reverting a previously applied optimization"""
        # Set up mock for get_optimization_by_id to return a test optimization
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9,
            status=OptimizationStatus.APPLIED
        )
        self.optimizer.get_optimization_by_id = MagicMock(return_value=action)

        # Call revert_optimization
        self.optimizer.revert_optimization(self.test_optimization_id)

        # Verify reverse operation was applied
        self.optimizer.get_optimization_by_id.assert_called_once_with(self.test_optimization_id)

        # Verify optimization status was updated to REVERTED
        self.assertEqual(action.status, OptimizationStatus.REVERTED)

    def test_optimize_bigquery_resources(self):
        """Test BigQuery-specific optimizations"""
        # Set up mock return values for ResourceMonitor.get_bigquery_resources
        self.mock_resource_monitor.get_bigquery_resources.return_value = {
            "slot_utilization": 95,
            "storage_usage": 80
        }

        # Call optimize_bigquery_resources
        recommendations = self.optimizer.optimize_bigquery_resources()

        # Verify correct analysis was performed
        self.mock_resource_monitor.get_bigquery_resources.assert_called_once()

        # Verify returned optimization actions have correct format and confidence scores
        self.assertTrue(len(recommendations) > 0)
        for action in recommendations:
            self.assertIsInstance(action, dict)
            self.assertIn("confidence", action)

    def test_optimize_compute_resources(self):
        """Test Compute Engine optimizations"""
        # Set up mock return values for ResourceMonitor.get_compute_resources
        self.mock_resource_monitor.get_compute_resources.return_value = {
            "cpu_utilization": 90,
            "memory_utilization": 70
        }

        # Call optimize_compute_resources
        recommendations = self.optimizer.optimize_compute_resources()

        # Verify correct analysis was performed
        self.mock_resource_monitor.get_compute_resources.assert_called_once()

        # Verify returned optimization actions have correct format and confidence scores
        self.assertTrue(len(recommendations) > 0)
        for action in recommendations:
            self.assertIsInstance(action, dict)
            self.assertIn("confidence", action)

    def test_optimize_storage_resources(self):
        """Test Cloud Storage optimizations"""
        # Set up mock return values for ResourceMonitor.get_storage_resources
        self.mock_resource_monitor.get_storage_resources.return_value = {
            "storage_usage": 90,
            "growth_rate": 20
        }

        # Call optimize_storage_resources
        recommendations = self.optimizer.optimize_storage_resources()

        # Verify correct analysis was performed
        self.mock_resource_monitor.get_storage_resources.assert_called_once()

        # Verify returned optimization actions have correct format and confidence scores
        self.assertTrue(len(recommendations) > 0)
        for action in recommendations:
            self.assertIsInstance(action, dict)
            self.assertIn("confidence", action)

    def test_apply_bigquery_optimization(self):
        """Test applying a BigQuery-specific optimization"""
        # Create a test OptimizationAction for BigQuery
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call apply_bigquery_optimization
        self.mock_bq_client.execute_query = MagicMock(return_value=True)
        self.mock_cost_tracker.get_query_cost_savings = MagicMock(return_value=50)
        result = self.optimizer.apply_bigquery_optimization(action)

        # Verify correct BigQuery operations were performed
        self.mock_bq_client.execute_query.assert_called()

        # Verify post-optimization metrics were collected
        self.assertEqual(result["cost_savings"], 50)

    def test_apply_compute_optimization(self):
        """Test applying a Compute Engine optimization"""
        # Create a test OptimizationAction for Compute Engine
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.COMPUTE,
            action_type=CostOptimizationType.RIGHT_SIZING,
            target_resource="test-instance",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call apply_compute_optimization
        self.mock_resource_monitor.resize_instance = MagicMock(return_value=True)
        self.mock_cost_tracker.get_compute_cost_savings = MagicMock(return_value=30)
        result = self.optimizer.apply_compute_optimization(action)

        # Verify correct Compute Engine operations were performed
        self.mock_resource_monitor.resize_instance.assert_called()

        # Verify post-optimization metrics were collected
        self.assertEqual(result["cost_savings"], 30)

    def test_apply_storage_optimization(self):
        """Test applying a Cloud Storage optimization"""
        # Create a test OptimizationAction for Cloud Storage
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.STORAGE,
            action_type=CostOptimizationType.LIFECYCLE_POLICY,
            target_resource="test-bucket",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call apply_storage_optimization
        self.mock_resource_monitor.apply_lifecycle_policy = MagicMock(return_value=True)
        self.mock_cost_tracker.get_storage_cost_savings = MagicMock(return_value=20)
        result = self.optimizer.apply_storage_optimization(action)

        # Verify correct Cloud Storage operations were performed
        self.mock_resource_monitor.apply_lifecycle_policy.assert_called()

        # Verify post-optimization metrics were collected
        self.assertEqual(result["cost_savings"], 20)

    def test_analyze_optimization_effectiveness(self):
        """Test analyzing the effectiveness of past optimizations"""
        # Set up mock return value for get_optimization_history
        history_data = pd.DataFrame([
            {"optimization_id": "1", "resource_type": "BIGQUERY", "action_type": "QUERY_OPTIMIZATION", "status": "APPLIED", "cost_savings": 50},
            {"optimization_id": "2", "resource_type": "COMPUTE", "action_type": "RIGHT_SIZING", "status": "APPLIED", "cost_savings": 30}
        ])
        self.optimizer.get_optimization_history = MagicMock(return_value=history_data)

        # Call analyze_optimization_effectiveness
        effectiveness = self.optimizer.analyze_optimization_effectiveness()

        # Verify effectiveness metrics are calculated correctly
        self.assertEqual(effectiveness["total_savings"], 80)
        self.assertEqual(effectiveness["num_optimizations"], 2)

        # Verify recommendations for optimization strategy are generated
        self.assertIn("recommendations", effectiveness)

    def test_get_optimization_by_id(self):
        """Test retrieving a specific optimization by ID"""
        # Set up mock return value for BigQueryClient.execute_query_to_dataframe
        optimization_data = pd.DataFrame([
            {"optimization_id": self.test_optimization_id, "resource_type": "BIGQUERY", "action_type": "QUERY_OPTIMIZATION", "status": "APPLIED"}
        ])
        self.mock_bq_client.execute_query_to_dataframe.return_value = optimization_data

        # Call get_optimization_by_id
        optimization = self.optimizer.get_optimization_by_id(self.test_optimization_id)

        # Verify correct query was executed
        self.mock_bq_client.execute_query_to_dataframe.assert_called_once()

        # Verify returned optimization matches expected format
        self.assertEqual(optimization.optimization_id, self.test_optimization_id)
        self.assertEqual(optimization.resource_type, ResourceType.BIGQUERY)

    def test_set_confidence_threshold(self):
        """Test setting the confidence threshold for automatic optimizations"""
        # Call set_confidence_threshold with a valid value
        self.optimizer.set_confidence_threshold(0.8)

        # Verify threshold was updated
        self.assertEqual(self.optimizer._confidence_threshold, 0.8)

        # Call set_confidence_threshold with an invalid value
        with self.assertRaises(ValueError):
            self.optimizer.set_confidence_threshold(1.2)

    def test_record_optimization_metrics(self):
        """Test recording metrics about optimization activities"""
        # Create test optimization metrics
        metrics = {
            "optimization_id": self.test_optimization_id,
            "resource_type": ResourceType.BIGQUERY,
            "action_type": CostOptimizationType.QUERY_OPTIMIZATION,
            "potential_impact": 100,
            "actual_impact": 90
        }

        # Call record_optimization_metrics
        self.optimizer.record_optimization_metrics(metrics)

        # Verify metrics were recorded using MetricClient
        self.mock_metric_client.create_gauge_metric.assert_called()

        # Verify detailed metrics were stored in BigQuery
        self.mock_bq_client.insert_row.assert_called()


class TestOptimizationAction(unittest.TestCase):
    """Test case for the OptimizationAction class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Set up common test data for optimization actions
        self.test_optimization_id = "test-optimization-123"
        self.test_resource_type = ResourceType.BIGQUERY
        self.test_action_type = CostOptimizationType.QUERY_OPTIMIZATION
        self.test_target_resource = "test-table"
        self.test_description = "Test optimization"
        self.test_potential_impact = 100
        self.test_confidence = 0.9

    def test_init(self):
        """Test initialization of OptimizationAction"""
        # Create an OptimizationAction with test data
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=self.test_resource_type,
            action_type=self.test_action_type,
            target_resource=self.test_target_resource,
            description=self.test_description,
            potential_impact=self.test_potential_impact,
            confidence=self.test_confidence
        )

        # Verify all properties are set correctly
        self.assertEqual(action.optimization_id, self.test_optimization_id)
        self.assertEqual(action.resource_type, self.test_resource_type)
        self.assertEqual(action.action_type, self.test_action_type)
        self.assertEqual(action.target_resource, self.test_target_resource)
        self.assertEqual(action.description, self.test_description)
        self.assertEqual(action.potential_impact, self.test_potential_impact)
        self.assertEqual(action.confidence, self.test_confidence)

        # Verify default values are set correctly
        self.assertEqual(action.status, OptimizationStatus.PENDING)
        self.assertIsNone(action.applied_at)
        self.assertIsNone(action.result_metrics)

    def test_to_dict(self):
        """Test conversion of OptimizationAction to dictionary"""
        # Create an OptimizationAction with test data
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=self.test_resource_type,
            action_type=self.test_action_type,
            target_resource=self.test_target_resource,
            description=self.test_description,
            potential_impact=self.test_potential_impact,
            confidence=self.test_confidence
        )

        # Call to_dict method
        action_dict = action.to_dict()

        # Verify dictionary contains all expected keys
        expected_keys = ["optimization_id", "resource_type", "action_type", "target_resource", "description",
                         "potential_impact", "confidence", "status", "applied_at", "result_metrics", "scheduled_at"]
        for key in expected_keys:
            self.assertIn(key, action_dict)

        # Verify values are correctly converted (enums to strings, datetimes to ISO format)
        self.assertEqual(action_dict["resource_type"], "BIGQUERY")
        self.assertEqual(action_dict["action_type"], "QUERY_OPTIMIZATION")
        self.assertEqual(action_dict["status"], "PENDING")

    def test_from_dict(self):
        """Test creation of OptimizationAction from dictionary"""
        # Create a test dictionary with optimization data
        action_dict = {
            "optimization_id": self.test_optimization_id,
            "resource_type": "BIGQUERY",
            "action_type": "QUERY_OPTIMIZATION",
            "target_resource": self.test_target_resource,
            "description": self.test_description,
            "potential_impact": self.test_potential_impact,
            "confidence": self.test_confidence,
            "status": "APPLIED",
            "applied_at": datetime.datetime.now().isoformat(),
            "result_metrics": {"cost_savings": 50},
            "scheduled_at": datetime.datetime.now().isoformat()
        }

        # Call OptimizationAction.from_dict
        action = OptimizationAction.from_dict(action_dict)

        # Verify created instance has correct properties
        self.assertEqual(action.optimization_id, self.test_optimization_id)
        self.assertEqual(action.resource_type, ResourceType.BIGQUERY)
        self.assertEqual(action.action_type, CostOptimizationType.QUERY_OPTIMIZATION)
        self.assertEqual(action.target_resource, self.test_target_resource)
        self.assertEqual(action.description, self.test_description)
        self.assertEqual(action.potential_impact, self.test_potential_impact)
        self.assertEqual(action.confidence, self.test_confidence)
        self.assertEqual(action.status, OptimizationStatus.APPLIED)
        self.assertIsNotNone(action.applied_at)
        self.assertEqual(action.result_metrics["cost_savings"], 50)
        self.assertIsNotNone(action.scheduled_at)

        # Verify values are correctly converted (strings to enums, ISO format to datetimes)
        self.assertIsInstance(action.resource_type, ResourceType)
        self.assertIsInstance(action.action_type, CostOptimizationType)
        self.assertIsInstance(action.status, OptimizationStatus)
        self.assertIsInstance(action.applied_at, datetime.datetime)
        self.assertIsInstance(action.scheduled_at, datetime.datetime)

    def test_update_status(self):
        """Test updating the status of an optimization action"""
        # Create an OptimizationAction with initial status
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call update_status with new status
        action.update_status(OptimizationStatus.APPLIED)

        # Verify status is updated
        self.assertEqual(action.status, OptimizationStatus.APPLIED)

        # Verify applied_at is set when status is APPLIED
        self.assertIsNotNone(action.applied_at)

    def test_record_results(self):
        """Test recording actual results of an optimization"""
        # Create an OptimizationAction
        action = OptimizationAction(
            optimization_id=self.test_optimization_id,
            resource_type=ResourceType.BIGQUERY,
            action_type=CostOptimizationType.QUERY_OPTIMIZATION,
            target_resource="test-table",
            description="Test optimization",
            potential_impact=100,
            confidence=0.9
        )

        # Call record_results with test metrics
        metrics = {"cost_savings": 50, "execution_time": 10}
        action.record_results(metrics)

        # Verify result_metrics are stored
        self.assertEqual(action.result_metrics, metrics)

        # Verify actual vs. predicted impact comparison
        self.assertEqual(action.actual_vs_predicted_impact, -50)