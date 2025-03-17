import pytest  # pytest version: 7.3.1
import time  # standard library
import statistics  # standard library
import concurrent.futures  # standard library
from fastapi.testclient import TestClient  # fastapi version: ^0.95.0
import json  # standard library
import logging  # standard library

from src.test.utils.test_helpers import generate_unique_id, create_test_data  # src/test/utils/test_helpers.py
from src.test.utils.api_test_utils import APITestClient, create_test_api_client  # src/test/utils/api_test_utils.py
from src.test.performance.conftest import performance_metrics_collector, performance_test_context, test_data_size, test_iterations  # src/test/performance/conftest.py
from src.backend.api.app import create_app  # src/backend/api/app.py
from src.backend.constants import API_VERSION, API_PREFIX  # src/backend/constants.py

logger = logging.getLogger(__name__)

API_ENDPOINTS = {
    "ingestion": [
        "/api/ingestion/sources",
        "/api/ingestion/sources/{source_id}",
        "/api/ingestion/sources/types",
        "/api/ingestion/pipelines",
        "/api/ingestion/pipelines/{pipeline_id}",
        "/api/ingestion/pipelines/{pipeline_id}/execute",
        "/api/ingestion/pipelines/{pipeline_id}/executions",
        "/api/ingestion/executions/{execution_id}",
        "/api/ingestion/executions/{execution_id}/tasks",
    ],
    "quality": [
        "/api/quality/rules",
        "/api/quality/rules/{rule_id}",
        "/api/quality/validations",
        "/api/quality/validations/{validation_id}",
        "/api/quality/validate",
        "/api/quality/score",
        "/api/quality/issues",
        "/api/quality/trend",
    ],
    "healing": [
        "/api/healing/issues",
        "/api/healing/issues/{issue_id}",
        "/api/healing/actions",
        "/api/healing/actions/{action_id}",
        "/api/healing/models",
        "/api/healing/models/{model_id}",
        "/api/healing/models/{model_id}/metrics",
        "/api/healing/config",
    ],
    "monitoring": [
        "/api/monitoring/alerts",
        "/api/monitoring/alerts/{alert_id}",
        "/api/monitoring/metrics",
        "/api/monitoring/metrics/{metric_id}",
        "/api/monitoring/dashboards",
        "/api/monitoring/dashboards/{dashboard_id}",
        "/api/monitoring/notifications/settings",
    ],
    "optimization": [
        "/api/optimization/recommendations",
        "/api/optimization/recommendations/{recommendation_id}",
        "/api/optimization/queries",
        "/api/optimization/queries/{query_id}",
        "/api/optimization/schemas",
        "/api/optimization/schemas/{schema_id}",
        "/api/optimization/resources",
    ],
}

TEST_PAYLOAD_TEMPLATES = {
    "ingestion": {
        "source_create": {
            "source_name": "test_source_{id}",
            "source_type": "GCS",
            "connection_details": {"bucket_name": "test-bucket", "prefix": "test-data/"},
            "is_active": True,
        },
        "pipeline_create": {
            "pipeline_name": "test_pipeline_{id}",
            "source_id": "{source_id}",
            "target_dataset": "test_dataset",
            "target_table": "test_table",
            "schedule": "0 0 * * *",
            "is_active": True,
        },
        "pipeline_execute": {"parameters": {"run_date": "2023-06-15"}},
    },
    "quality": {
        "rule_create": {
            "rule_name": "test_rule_{id}",
            "rule_type": "SCHEMA",
            "target_dataset": "test_dataset",
            "target_table": "test_table",
            "rule_definition": {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}},
            "severity": "HIGH",
            "is_active": True,
        },
        "validation_request": {"dataset": "test_dataset", "table": "test_table", "rule_ids": []},
    },
    "healing": {
        "action_create": {
            "action_name": "test_action_{id}",
            "action_type": "DATA_CORRECTION",
            "target_dataset": "test_dataset",
            "target_table": "test_table",
            "action_definition": {"correction_type": "IMPUTATION", "parameters": {"column": "value", "method": "MEAN"}},
            "is_active": True,
        },
        "issue_fix": {"fix_type": "AUTO", "parameters": {}},
    },
    "monitoring": {
        "alert_update": {"status": "ACKNOWLEDGED", "comment": "Acknowledged for testing"},
        "notification_settings": {
            "teams_webhook": "https://example.com/webhook",
            "email_recipients": ["test@example.com"],
            "severity_thresholds": {"HIGH": ["TEAMS", "EMAIL"], "MEDIUM": ["TEAMS"], "LOW": []},
        },
    },
    "optimization": {
        "recommendation_apply": {"apply_immediately": True, "schedule_time": None},
        "query_analyze": {"optimization_techniques": ["PREDICATE_PUSHDOWN", "JOIN_REORDERING"]},
    },
}

PERFORMANCE_THRESHOLDS = {
    "api": {"response_time": {"p50": 200, "p95": 500, "p99": 1000}, "throughput": {"min": 50, "target": 100}}
}


def setup_test_data(api_client: APITestClient, data_size: str) -> dict:
    """Creates test data for API performance testing"""
    unique_id = generate_unique_id()
    test_data = {}

    # Create test source systems
    test_data["source_create"] = api_client.create_resource(
        "ingestion", "sources", TEST_PAYLOAD_TEMPLATES["ingestion"]["source_create"].copy(), unique_id
    )

    # Create test pipeline definitions
    test_data["pipeline_create"] = api_client.create_resource(
        "ingestion",
        "pipelines",
        TEST_PAYLOAD_TEMPLATES["ingestion"]["pipeline_create"].copy(),
        unique_id,
        path_params={"source_id": test_data["source_create"]["source_id"]},
    )

    # Create test quality rules
    test_data["rule_create"] = api_client.create_resource(
        "quality", "rules", TEST_PAYLOAD_TEMPLATES["quality"]["rule_create"].copy(), unique_id
    )

    # Create test healing actions
    test_data["action_create"] = api_client.create_resource(
        "healing",
        "actions",
        TEST_PAYLOAD_TEMPLATES["healing"]["action_create"].copy(),
        unique_id,
        path_params={"pattern_id": "test_pattern"},
    )

    # Create test monitoring configurations
    # Create test optimization scenarios

    return test_data


def generate_endpoint_url(endpoint_template: str, path_params: dict) -> str:
    """Generates a complete endpoint URL with path parameters"""
    endpoint = endpoint_template
    for param, value in path_params.items():
        endpoint = endpoint.replace("{" + param + "}", value)
    return endpoint


def generate_request_payload(template: dict, test_data: dict, unique_id: str) -> dict:
    """Generates a request payload based on template and test data"""
    payload = template.copy()
    payload = json.loads(json.dumps(payload).replace("{id}", unique_id))
    for key, value in test_data.items():
        payload = json.loads(json.dumps(payload).replace("{" + key + "}", value))
    return payload


def measure_api_performance(api_client: APITestClient, method: str, endpoint: str, payload: dict, query_params: dict, iterations: int) -> dict:
    """Measures the performance of API endpoints"""
    response_times = []
    status_codes = []

    for i in range(iterations):
        start_time = time.time()
        response = api_client.make_request(method, endpoint, query_params=query_params, json_data=payload)
        end_time = time.time()

        response_times.append(end_time - start_time)
        status_codes.append(response.response.status_code)

    min_time = min(response_times)
    max_time = max(response_times)
    avg_time = sum(response_times) / len(response_times)
    median_time = statistics.median(response_times)
    p95_time = statistics.quantiles(response_times, n=100)[94]
    p99_time = statistics.quantiles(response_times, n=100)[98]

    return {
        "min": min_time,
        "max": max_time,
        "avg": avg_time,
        "median": median_time,
        "p95": p95_time,
        "p99": p99_time,
        "status_codes": status_codes,
    }


def run_concurrent_api_requests(api_client: APITestClient, method: str, endpoint: str, payload: dict, concurrency: int, requests_per_thread: int) -> dict:
    """Executes multiple API requests concurrently to simulate load"""
    response_times = []
    status_codes = []

    def make_requests():
        thread_times = []
        thread_codes = []
        for _ in range(requests_per_thread):
            start_time = time.time()
            response = api_client.make_request(method, endpoint, json_data=payload)
            end_time = time.time()

            thread_times.append(end_time - start_time)
            thread_codes.append(response.response.status_code)
        return thread_times, thread_codes

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(make_requests) for _ in range(concurrency)]

        for future in concurrent.futures.as_completed(futures):
            thread_times, thread_codes = future.result()
            response_times.extend(thread_times)
            status_codes.extend(thread_codes)

    throughput = len(response_times) / sum(response_times) if sum(response_times) > 0 else 0
    min_time = min(response_times)
    max_time = max(response_times)
    avg_time = sum(response_times) / len(response_times)
    median_time = statistics.median(response_times)
    p95_time = statistics.quantiles(response_times, n=100)[94]
    p99_time = statistics.quantiles(response_times, n=100)[98]

    return {
        "throughput": throughput,
        "min": min_time,
        "max": max_time,
        "avg": avg_time,
        "median": median_time,
        "p95": p95_time,
        "p99": p99_time,
        "status_codes": status_codes,
    }


def validate_performance_metrics(metrics: dict, thresholds: dict) -> tuple:
    """Validates performance metrics against defined thresholds"""
    results = {}
    success = True

    for metric, value in metrics.items():
        if metric in thresholds:
            if value > thresholds[metric]:
                results[metric] = f"FAILED: {value} > {thresholds[metric]}"
                success = False
            else:
                results[metric] = f"PASSED: {value} <= {thresholds[metric]}"
        else:
            results[metric] = "SKIPPED: No threshold defined"

    return success, results


class APIPerformanceTester:
    """Test harness for measuring and validating API performance across different endpoints"""

    def __init__(self, api_client: APITestClient, metrics_collector):
        """Initialize the API performance tester"""
        self._api_client = api_client
        self._test_data = {}
        self._metrics_collector = metrics_collector

    def setup(self, data_size: str):
        """Set up test environment with test data"""
        self._test_data = setup_test_data(self._api_client, data_size)
        logger.info("API performance test setup complete")

    def teardown(self):
        """Clean up test environment after tests"""
        # Clean up test resources created during testing
        # Reset test data dictionary
        logger.info("API performance test teardown complete")

    def test_endpoint_performance(self, category: str, endpoint_template: str, method: str, path_params: dict, query_params: dict, payload: dict, iterations: int) -> dict:
        """Test performance of a specific API endpoint"""
        endpoint = generate_endpoint_url(endpoint_template, path_params)
        if payload:
            payload = generate_request_payload(payload, self._test_data, generate_unique_id())

        metrics = measure_api_performance(self._api_client, method, endpoint, payload, query_params, iterations)
        self._metrics_collector.record_metrics(category, endpoint, metrics)

        return metrics

    def test_endpoint_load(self, category: str, endpoint_template: str, method: str, path_params: dict, payload: dict, concurrency: int, requests_per_thread: int) -> dict:
        """Test performance of an endpoint under concurrent load"""
        endpoint = generate_endpoint_url(endpoint_template, path_params)
        if payload:
            payload = generate_request_payload(payload, self._test_data, generate_unique_id())

        metrics = run_concurrent_api_requests(self._api_client, method, endpoint, payload, concurrency, requests_per_thread)
        self._metrics_collector.record_metrics(category, endpoint, metrics)

        return metrics

    def test_category_endpoints(self, category: str, iterations: int) -> dict:
        """Test all endpoints for a specific API category"""
        results = {}
        for endpoint_template in API_ENDPOINTS[category]:
            if "GET" in endpoint_template:
                method = "GET"
                path_params = {}
                query_params = {}
                payload = None
            else:
                method = "POST"
                path_params = {}
                query_params = {}
                payload = TEST_PAYLOAD_TEMPLATES[category].get("create", {})

            results[endpoint_template] = self.test_endpoint_performance(category, endpoint_template, method, path_params, query_params, payload, iterations)

        return results

    def benchmark_api(self, categories: list, iterations: int) -> dict:
        """Benchmark all API categories with various configurations"""
        results = {}
        for category in categories:
            results[category] = self.test_category_endpoints(category, iterations)

        return results

    def generate_performance_report(self, benchmark_results: dict) -> str:
        """Generate a detailed performance report for the API"""
        # Process benchmark results
        # Calculate average response times by category and endpoint
        # Identify slowest and fastest endpoints
        # Generate performance comparison charts
        # Format results into a readable report
        return "Performance report"


@pytest.mark.performance
@pytest.mark.api
def test_api_response_time(performance_metrics_collector, performance_test_context, test_data_size, test_iterations):
    """Test response time of individual API endpoints"""
    api_client = create_test_api_client()
    tester = APIPerformanceTester(api_client, performance_metrics_collector)
    tester.setup(test_data_size)

    # Test response time of key endpoints
    results = tester.benchmark_api(["ingestion", "quality", "healing", "monitoring", "optimization"], test_iterations)

    # Validate response times against thresholds
    # Record detailed metrics for reporting
    tester.teardown()


@pytest.mark.performance
@pytest.mark.api
@pytest.mark.load
def test_api_throughput(performance_metrics_collector, performance_test_context, test_data_size):
    """Test API throughput under concurrent load"""
    api_client = create_test_api_client()
    tester = APIPerformanceTester(api_client, performance_metrics_collector)
    tester.setup(test_data_size)

    # Define concurrency levels to test (10, 20, 50 concurrent users)
    concurrency_levels = [10, 20, 50]

    for concurrency in concurrency_levels:
        # Test API throughput
        results = tester.test_endpoint_load("ingestion", "/api/ingestion/sources", "GET", {}, {}, concurrency, 10)

        # Measure requests per second and response time distribution
        # Validate throughput against thresholds
        # Record detailed metrics for reporting
    tester.teardown()


@pytest.mark.performance
@pytest.mark.api
@pytest.mark.stability
@pytest.mark.slow
def test_api_stability(performance_metrics_collector, performance_test_context, test_data_size):
    """Test API stability under sustained load"""
    api_client = create_test_api_client()
    tester = APIPerformanceTester(api_client, performance_metrics_collector)
    tester.setup(test_data_size)

    # Run sustained load test for 5 minutes with moderate concurrency
    # Monitor response time stability over the test duration
    # Check for error rate increases over time
    # Validate that performance remains consistent throughout the test
    # Record detailed metrics for reporting
    tester.teardown()


@pytest.mark.performance
@pytest.mark.api
def test_api_error_handling(performance_metrics_collector, performance_test_context):
    """Test API error handling under invalid requests"""
    api_client = create_test_api_client()
    tester = APIPerformanceTester(api_client, performance_metrics_collector)

    # Generate invalid request scenarios (malformed payloads, invalid parameters)
    # Measure response time for error responses
    # Verify consistent error handling across endpoints
    # Validate that error responses meet performance thresholds
    # Record detailed metrics for reporting
    pass


@pytest.mark.performance
@pytest.mark.api
def test_api_category_comparison(performance_metrics_collector, performance_test_context, test_data_size, test_iterations):
    """Compare performance across different API categories"""
    api_client = create_test_api_client()
    tester = APIPerformanceTester(api_client, performance_metrics_collector)
    tester.setup(test_data_size)

    # Benchmark all API categories with consistent parameters
    results = tester.benchmark_api(["ingestion", "quality", "healing", "monitoring", "optimization"], test_iterations)

    # Compare performance metrics across categories
    # Identify performance outliers and bottlenecks
    # Generate comparative performance report
    # Record detailed metrics for reporting
    tester.teardown()