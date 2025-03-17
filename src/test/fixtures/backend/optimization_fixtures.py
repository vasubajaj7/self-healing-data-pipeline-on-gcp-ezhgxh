# src/test/fixtures/backend/optimization_fixtures.py
"""Provides test fixtures and helper functions for testing the performance optimization
components of the self-healing data pipeline. Includes mock objects, test data generators,
and utility functions to simplify optimization testing for query, schema, and resource
optimization."""

import pytest  # package_version: 7.3.1
import unittest.mock as mock  # package_version: standard library
import pandas  # package_version: 2.0.x
from google.cloud import bigquery  # package_version: 3.11.0
import datetime  # package_version: standard library
import uuid  # package_version: standard library

# Internal imports
from src.backend.constants import OptimizationType, DEFAULT_CONFIDENCE_THRESHOLD  # src/backend/constants.py
from src.backend.optimization.query.query_optimizer import QueryOptimizer, OptimizationResult, OPTIMIZATION_TECHNIQUES  # src/backend/optimization/query/query_optimizer.py
from src.backend.optimization.schema.schema_analyzer import SchemaAnalyzer, SchemaOptimizationRecommendation  # src/backend/optimization/schema/schema_analyzer.py
from src.backend.optimization.resource.resource_optimizer import ResourceOptimizer, OptimizationAction, OptimizationStatus  # src/backend/optimization/resource/resource_optimizer.py
from src.test.utils.test_helpers import create_temp_file  # src/test/utils/test_helpers.py

SAMPLE_SQL_QUERIES = [
    {'name': 'simple_query', 'sql': 'SELECT * FROM `project.dataset.table` WHERE value > 100'},
    {'name': 'join_query', 'sql': 'SELECT a.id, a.name, b.value FROM `project.dataset.table_a` a JOIN `project.dataset.table_b` b ON a.id = b.id WHERE a.active = TRUE'},
    {'name': 'complex_query', 'sql': 'SELECT dept, COUNT(*) as count, AVG(salary) as avg_salary FROM `project.dataset.employees` WHERE hire_date > "2020-01-01" GROUP BY dept HAVING count > 10 ORDER BY avg_salary DESC'}
]

SAMPLE_SCHEMA_DEFINITIONS = [
    {'table': 'users', 'columns': [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'STRING'}, {'name': 'email', 'type': 'STRING'}, {'name': 'created_at', 'type': 'TIMESTAMP'}]},
    {'table': 'orders', 'columns': [{'name': 'order_id', 'type': 'INTEGER'}, {'name': 'user_id', 'type': 'INTEGER'}, {'name': 'amount', 'type': 'FLOAT'}, {'name': 'status', 'type': 'STRING'}, {'name': 'created_at', 'type': 'TIMESTAMP'}]}
]

SAMPLE_RESOURCE_METRICS = {
    'bigquery': {'slots_used': 50, 'total_slots': 100, 'bytes_processed': 1073741824, 'query_count': 1000},
    'compute': {'cpu_utilization': 0.65, 'memory_utilization': 0.72, 'disk_utilization': 0.45},
    'storage': {'total_bytes': 10737418240, 'hot_bytes': 5368709120, 'cold_bytes': 5368709120}
}


def create_test_query_optimization_result(original_query: str, optimized_query: str, applied_techniques: list, performance_comparison: dict, is_equivalent: bool) -> OptimizationResult:
    """Creates a test query optimization result with specified parameters"""
    optimization_result = OptimizationResult(
        original_query=original_query,
        optimized_query=optimized_query,
        applied_techniques=applied_techniques,
        performance_comparison=performance_comparison,
        is_equivalent=is_equivalent,
        validation_details={}  # Provide a default value
    )
    return optimization_result


def create_test_schema_recommendation(optimization_type: str, column_name: str, current_type: str, recommended_type: str, rationale: str, statistics: dict, estimated_impact: dict) -> SchemaOptimizationRecommendation:
    """Creates a test schema optimization recommendation with specified parameters"""
    schema_recommendation = SchemaOptimizationRecommendation(
        optimization_type=optimization_type,
        column_name=column_name,
        current_type=current_type,
        recommended_type=recommended_type,
        rationale=rationale,
        statistics=statistics,
        estimated_impact=estimated_impact
    )
    return schema_recommendation


def create_test_resource_optimization_action(optimization_type: str, parameters: dict, impact_assessment: dict, confidence_score: float, status: OptimizationStatus = OptimizationStatus.RECOMMENDED) -> OptimizationAction:
    """Creates a test resource optimization action with specified parameters"""
    resource_action = OptimizationAction(
        optimization_type=optimization_type,
        parameters=parameters,
        impact_assessment=impact_assessment,
        confidence_score=confidence_score,
        status=status
    )
    return resource_action


def create_mock_query_optimizer(optimization_results: dict) -> mock.Mock:
    """Creates a mock QueryOptimizer for testing"""
    mock_optimizer = mock.Mock(spec=QueryOptimizer)
    mock_optimizer.optimize_query.return_value = optimization_results
    mock_optimizer.get_optimized_query.return_value = optimization_results.get('optimized_query', 'SELECT 1')
    mock_optimizer.validate_query_equivalence.return_value = {'is_equivalent': True}
    mock_optimizer.get_optimization_recommendations.return_value = []
    return mock_optimizer


def create_mock_schema_analyzer(schema_recommendations: list) -> mock.Mock:
    """Creates a mock SchemaAnalyzer for testing"""
    mock_analyzer = mock.Mock(spec=SchemaAnalyzer)
    mock_analyzer.analyze_table_schema.return_value = {'recommendations': schema_recommendations}
    mock_analyzer.get_schema_recommendations.return_value = schema_recommendations
    mock_analyzer.apply_schema_optimizations.return_value = True
    return mock_analyzer


def create_mock_resource_optimizer(optimization_actions: list) -> mock.Mock:
    """Creates a mock ResourceOptimizer for testing"""
    mock_optimizer = mock.Mock(spec=ResourceOptimizer)
    mock_optimizer.get_optimization_recommendations.return_value = optimization_actions
    mock_optimizer.apply_optimization.return_value = {'success': True}
    mock_optimizer.schedule_optimization.return_value = True
    return mock_optimizer


def generate_test_query(query_type: str, parameters: dict) -> str:
    """Generates a test SQL query with specified characteristics"""
    if query_type == 'simple':
        return f"SELECT * FROM `project.dataset.table` WHERE id > {parameters.get('min_id', 10)}"
    elif query_type == 'join':
        return f"SELECT a.name, b.value FROM `project.dataset.table_a` a JOIN `project.dataset.table_b` b ON a.id = b.id WHERE a.category = '{parameters.get('category', 'X')}'"
    else:
        return "SELECT 1"


def generate_test_table_schema(table_type: str, column_count: int, options: dict) -> list:
    """Generates a test BigQuery table schema with specified characteristics"""
    schema = []
    for i in range(column_count):
        schema.append(bigquery.SchemaField(f"column_{i}", "STRING"))
    return schema


def generate_test_resource_metrics(resource_type: str, utilization_profile: dict) -> dict:
    """Generates test resource utilization metrics with specified characteristics"""
    if resource_type == 'bigquery':
        return {'slots_used': utilization_profile.get('slots_used', 50), 'bytes_processed': utilization_profile.get('bytes_processed', 1000000)}
    elif resource_type == 'compute':
        return {'cpu_utilization': utilization_profile.get('cpu_utilization', 0.7), 'memory_utilization': utilization_profile.get('memory_utilization', 0.8)}
    else:
        return {}


class TestQueryOptimizationData:
    """Class providing test data for query optimization tests"""

    def __init__(self):
        """Initialize test query optimization data"""
        self.sample_queries = SAMPLE_SQL_QUERIES
        self.optimization_results = {
            query['name']: {'optimized_query': f"Optimized {query['sql']}", 'applied_techniques': ['test_technique'], 'performance_comparison': {}, 'is_equivalent': True}
            for query in self.sample_queries
        }
        self.query_plans = {
            query['name']: {'cost': 100, 'steps': []}
            for query in self.sample_queries
        }
        self.performance_metrics = {
            query['name']: {'bytes_processed': 1000, 'execution_time': 10}
            for query in self.sample_queries
        }

    def get_query_by_name(self, query_name: str) -> str:
        """Get a test query by name"""
        for query in self.sample_queries:
            if query['name'] == query_name:
                return query['sql']
        return None

    def get_optimization_result(self, query_name: str) -> OptimizationResult:
        """Get a test optimization result for a query"""
        if query_name in self.optimization_results:
            return self.optimization_results[query_name]
        return None

    def get_query_with_antipattern(self, antipattern_type: str) -> str:
        """Get a test query with a specific anti-pattern"""
        return f"SELECT * FROM `project.dataset.table` WHERE {antipattern_type} > 10"

    def get_query_plan(self, query_name: str) -> dict:
        """Get a test query execution plan"""
        if query_name in self.query_plans:
            return self.query_plans[query_name]
        return None


class TestSchemaOptimizationData:
    """Class providing test data for schema optimization tests"""

    def __init__(self):
        """Initialize test schema optimization data"""
        self.sample_schemas = SAMPLE_SCHEMA_DEFINITIONS
        self.optimization_recommendations = {
            schema['table']: [{'column_name': col['name'], 'current_type': col['type'], 'recommended_type': 'OPTIMIZED'} for col in schema['columns']]
            for schema in self.sample_schemas
        }
        self.column_statistics = {
            schema['table']: {col['name']: {'min': 0, 'max': 100} for col in schema['columns']}
            for schema in self.sample_schemas
        }
        self.query_patterns = {
            schema['table']: {'frequent_columns': [col['name'] for col in schema['columns']]}
            for schema in self.sample_schemas
        }

    def get_schema_by_table(self, table_name: str) -> dict:
        """Get a test schema by table name"""
        for schema in self.sample_schemas:
            if schema['table'] == table_name:
                return schema
        return None

    def get_recommendations_for_table(self, table_name: str) -> list:
        """Get test optimization recommendations for a table"""
        if table_name in self.optimization_recommendations:
            return self.optimization_recommendations[table_name]
        return []

    def get_column_statistics(self, table_name: str, column_name: str) -> dict:
        """Get test column statistics for a table"""
        if table_name in self.column_statistics and column_name in self.column_statistics[table_name]:
            return self.column_statistics[table_name][column_name]
        return {}

    def generate_schema_with_issues(self, issue_types: list) -> dict:
        """Generate a test schema with specific optimization issues"""
        base_schema = {'table': 'test_table', 'columns': [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'STRING'}]}
        if 'inefficient_types' in issue_types:
            base_schema['columns'][0]['type'] = 'BIGNUMERIC'
        if 'missing_partitioning' in issue_types:
            pass  # Omit partitioning
        return base_schema


class TestResourceOptimizationData:
    """Class providing test data for resource optimization tests"""

    def __init__(self):
        """Initialize test resource optimization data"""
        self.resource_metrics = SAMPLE_RESOURCE_METRICS
        self.optimization_actions = {
            'bigquery': [{'optimization_type': 'slot_adjustment', 'parameters': {'new_slots': 150}, 'impact_assessment': {}, 'confidence_score': 0.9}],
            'compute': [{'optimization_type': 'instance_resize', 'parameters': {'new_size': 'n1-standard-4'}, 'impact_assessment': {}, 'confidence_score': 0.8}]
        }
        self.cost_data = {
            'bigquery': {'cost_last_30_days': 500},
            'compute': {'cost_last_30_days': 200}
        }
        self.utilization_history = {
            'bigquery': {'daily_slots': [50, 60, 70, 80]},
            'compute': {'daily_cpu': [0.6, 0.7, 0.8, 0.9]}
        }

    def get_metrics_by_resource_type(self, resource_type: str) -> dict:
        """Get test metrics for a specific resource type"""
        if resource_type in self.resource_metrics:
            return self.resource_metrics[resource_type]
        return {}

    def get_optimization_actions(self, resource_type: str) -> list:
        """Get test optimization actions for a resource type"""
        if resource_type in self.optimization_actions:
            return self.optimization_actions[resource_type]
        return []

    def get_cost_data(self, resource_type: str, days: int) -> dict:
        """Get test cost data for a resource type"""
        if resource_type in self.cost_data:
            return self.cost_data[resource_type]
        return {}

    def generate_utilization_profile(self, resource_type: str, profile_type: str, days: int) -> dict:
        """Generate a test utilization profile with specific characteristics"""
        return {}


class MockQueryOptimizer:
    """Mock implementation of QueryOptimizer for testing"""

    def __init__(self, optimization_results: dict = None, validation_results: dict = None, config: dict = None):
        """Initialize mock query optimizer"""
        self._optimization_results = optimization_results or {}
        self._validation_results = validation_results or {}
        self._config = config or {}

    def optimize_query(self, query: str, techniques: list, validate: bool, use_cache: bool) -> dict:
        """Mock implementation of optimize_query method"""
        return self._optimization_results.get(query, {'optimized_query': query, 'applied_techniques': [], 'performance_comparison': {}, 'is_equivalent': True})

    def get_optimized_query(self, query: str, techniques: list, validate: bool) -> str:
        """Mock implementation of get_optimized_query method"""
        result = self.optimize_query(query, techniques, validate, use_cache=False)
        return result.get('optimized_query', query)

    def validate_query_equivalence(self, original_query: str, optimized_query: str, validation_options: dict) -> dict:
        """Mock implementation of validate_query_equivalence method"""
        key = (original_query, optimized_query)
        return self._validation_results.get(key, {'is_equivalent': True})

    def get_optimization_recommendations(self, query: str) -> list:
        """Mock implementation of get_optimization_recommendations method"""
        return []


@pytest.fixture
def mock_query_optimizer(optimization_results=None):
    """Pytest fixture providing a mock query optimizer"""
    return create_mock_query_optimizer(optimization_results or {})


@pytest.fixture
def mock_schema_analyzer(schema_recommendations=None):
    """Pytest fixture providing a mock schema analyzer"""
    return create_mock_schema_analyzer(schema_recommendations or [])


@pytest.fixture
def mock_resource_optimizer(optimization_actions=None):
    """Pytest fixture providing a mock resource optimizer"""
    return create_mock_resource_optimizer(optimization_actions or [])


@pytest.fixture
def test_query_optimization_data():
    """Pytest fixture providing test query optimization data"""
    return TestQueryOptimizationData()


@pytest.fixture
def test_schema_optimization_data():
    """Pytest fixture providing test schema optimization data"""
    return TestSchemaOptimizationData()


@pytest.fixture
def test_resource_optimization_data():
    """Pytest fixture providing test resource optimization data"""
    return TestResourceOptimizationData()


@pytest.fixture
def sample_sql_queries():
    """Pytest fixture providing sample SQL queries"""
    return SAMPLE_SQL_QUERIES


@pytest.fixture
def sample_schema_definitions():
    """Pytest fixture providing sample schema definitions"""
    return SAMPLE_SCHEMA_DEFINITIONS


@pytest.fixture
def sample_resource_metrics():
    """Pytest fixture providing sample resource metrics"""
    return SAMPLE_RESOURCE_METRICS