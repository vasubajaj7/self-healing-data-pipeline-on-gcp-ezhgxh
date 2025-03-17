"""
Provides higher-level mock service implementations for testing the self-healing data pipeline.

This module builds upon the basic mocks in mocks.py to create more complex service simulations
that mimic real-world behavior of Google Cloud services and other external dependencies.
"""

from unittest.mock import MagicMock, patch
import pytest
from typing import Any, Dict, List, Optional, Union, Callable
import io
import pandas as pd

# GCP related imports
from google.cloud.storage import Client as StorageClient  # version 2.7.0+
from google.cloud.bigquery import Client as BigQueryClient  # version 3.11.0+
from google.cloud.aiplatform import Endpoint as VertexAIEndpoint  # version 1.25.0
from google.cloud.pubsub import PublisherClient  # version 2.13.0+
from google.cloud.composer import EnvironmentsClient  # version 1.4.0+
from google.cloud.monitoring import MetricServiceClient  # version 2.14.1+
import requests  # version 2.31.0

# Internal imports
from src.backend.constants import (
    ErrorCategory, ErrorRecoverability, PipelineStatus, 
    FileFormat, DataSourceType
)
from src.test.utils.test_helpers import (
    create_test_dataframe, create_test_json_data, 
    generate_unique_id, MockResponseBuilder
)
from src.test.utils.mocks import (
    MockGCSClient, MockBigQueryClient, MockVertexAIClient, MockPubSubClient,
    create_mock_blob, create_mock_bucket, create_mock_bq_table
)

# Default testing values
DEFAULT_PROJECT_ID = "test-project"
DEFAULT_LOCATION = "us-central1"
DEFAULT_DATASET_ID = "test_dataset"
DEFAULT_BUCKET_NAME = "test-bucket"

def create_mock_gcs_service(bucket_configs: Dict, project_id: str = DEFAULT_PROJECT_ID) -> MockGCSClient:
    """
    Creates a mock GCS service with pre-configured buckets and files.
    
    Args:
        bucket_configs: Dictionary mapping bucket names to lists of file configurations
            Each file configuration should include 'name', 'content', and optional 'metadata'
        project_id: GCP project ID
        
    Returns:
        Configured mock GCS client
    """
    gcs_client = MockGCSClient(project_id=project_id)
    
    for bucket_name, file_configs in bucket_configs.items():
        # Create bucket
        bucket = gcs_client.create_bucket(bucket_name)
        
        # Add files to bucket
        for file_config in file_configs:
            content = file_config.get('content', b'')
            if isinstance(content, str):
                content = content.encode('utf-8')
                
            blob_name = file_config.get('name')
            metadata = file_config.get('metadata')
            
            gcs_client.upload_from_string(
                bucket_name=bucket_name,
                data=content,
                destination_blob_name=blob_name,
                metadata=metadata
            )
    
    return gcs_client

def create_mock_bigquery_service(dataset_configs: Dict, query_results: Dict = None, 
                               project_id: str = DEFAULT_PROJECT_ID) -> MockBigQueryClient:
    """
    Creates a mock BigQuery service with pre-configured datasets, tables, and query results.
    
    Args:
        dataset_configs: Dictionary mapping dataset IDs to lists of table configurations
            Each table configuration should include 'table_id', 'schema', and 'data'
        query_results: Dictionary mapping query strings to result objects
        project_id: GCP project ID
        
    Returns:
        Configured mock BigQuery client
    """
    bq_client = MockBigQueryClient(project_id=project_id)
    
    # Configure datasets and tables
    for dataset_id, table_configs in dataset_configs.items():
        # Create dataset
        bq_client.create_dataset(dataset_id)
        
        # Create tables in dataset
        for table_config in table_configs:
            table_id = table_config.get('table_id')
            schema = table_config.get('schema')
            data = table_config.get('data')
            clustering_fields = table_config.get('clustering_fields')
            time_partitioning = table_config.get('time_partitioning')
            
            # Create table
            bq_client.create_table(
                dataset_id=dataset_id,
                table_id=table_id,
                schema=schema,
                clustering_fields=clustering_fields,
                time_partitioning=time_partitioning
            )
            
            # Set table data if provided
            if data is not None:
                bq_client.set_table_data(dataset_id, table_id, data)
    
    # Configure query results
    if query_results:
        for query, result in query_results.items():
            bq_client.set_query_result(query, result)
    
    return bq_client

def create_mock_vertex_ai_service(model_configs: Dict, endpoint_configs: Dict = None,
                                prediction_results: Dict = None, 
                                project_id: str = DEFAULT_PROJECT_ID) -> MockVertexAIClient:
    """
    Creates a mock Vertex AI service with pre-configured models and endpoints.
    
    Args:
        model_configs: Dictionary of model configurations
        endpoint_configs: Dictionary of endpoint configurations mapping endpoints to models
        prediction_results: Dictionary mapping endpoint IDs to prediction results
        project_id: GCP project ID
        
    Returns:
        Configured mock Vertex AI client
    """
    vertex_client = MockVertexAIClient(project_id=project_id)
    
    # Create models
    for model_name, model_config in model_configs.items():
        display_name = model_config.get('display_name', model_name)
        artifact_uri = model_config.get('artifact_uri', f"gs://model-artifacts/{model_name}")
        container_image = model_config.get('container_image', "gcr.io/model-serving/tf-serving:latest")
        metadata = model_config.get('metadata', {})
        
        # Upload model
        model_id = vertex_client.upload_model(
            display_name=display_name,
            artifact_uri=artifact_uri,
            serving_container_image_uri=container_image,
            metadata=metadata
        )
        
        # Store model_id in config for reference when creating endpoints
        model_configs[model_name]['model_id'] = model_id
    
    # Create endpoints and deploy models
    if endpoint_configs:
        for endpoint_name, endpoint_config in endpoint_configs.items():
            # Create endpoint
            display_name = endpoint_config.get('display_name', endpoint_name)
            endpoint_id = vertex_client.create_endpoint(display_name=display_name)
            
            # Store endpoint_id in config
            endpoint_configs[endpoint_name]['endpoint_id'] = endpoint_id
            
            # Deploy models to endpoint
            deployed_models = endpoint_config.get('deployed_models', [])
            for deployment in deployed_models:
                model_name = deployment.get('model_name')
                if model_name in model_configs:
                    model_id = model_configs[model_name]['model_id']
                    
                    # Deploy model to endpoint
                    vertex_client.deploy_model(
                        model_id=model_id,
                        endpoint_id=endpoint_id,
                        machine_type=deployment.get('machine_type'),
                        min_replica_count=deployment.get('min_replicas'),
                        max_replica_count=deployment.get('max_replicas')
                    )
    
    # Configure prediction results
    if prediction_results:
        for endpoint_name, result in prediction_results.items():
            if endpoint_name in endpoint_configs:
                endpoint_id = endpoint_configs[endpoint_name]['endpoint_id']
                vertex_client.set_prediction_result(endpoint_id, result)
    
    return vertex_client

def create_mock_pubsub_service(topic_configs: Dict, project_id: str = DEFAULT_PROJECT_ID) -> MockPubSubClient:
    """
    Creates a mock Pub/Sub service with pre-configured topics and subscriptions.
    
    Args:
        topic_configs: Dictionary mapping topic IDs to configurations
            Each topic configuration should include 'subscriptions' and optional 'messages'
        project_id: GCP project ID
        
    Returns:
        Configured mock Pub/Sub client
    """
    pubsub_client = MockPubSubClient(project_id=project_id)
    
    # Create topics and subscriptions
    for topic_id, topic_config in topic_configs.items():
        # Create topic
        pubsub_client.create_topic(topic_id)
        
        # Create subscriptions
        subscriptions = topic_config.get('subscriptions', [])
        for subscription_id in subscriptions:
            pubsub_client.create_subscription(subscription_id, topic_id)
        
        # Publish pre-configured messages
        messages = topic_config.get('messages', [])
        for message in messages:
            data = message.get('data', '')
            if isinstance(data, str):
                data = data.encode('utf-8')
                
            attributes = message.get('attributes', {})
            pubsub_client.publish(topic_id, data, attributes)
    
    return pubsub_client

def create_mock_composer_service(environment_configs: Dict, dag_configs: Dict = None,
                               project_id: str = DEFAULT_PROJECT_ID) -> MagicMock:
    """
    Creates a mock Cloud Composer service with pre-configured environments and DAGs.
    
    Args:
        environment_configs: Dictionary of Composer environment configurations
        dag_configs: Dictionary of DAG configurations
        project_id: GCP project ID
        
    Returns:
        Configured mock Composer client
    """
    composer_client = MagicMock()
    
    # Configure environments
    environments = []
    for env_name, env_config in environment_configs.items():
        env = MagicMock()
        env.name = f"projects/{project_id}/locations/{env_config.get('location', DEFAULT_LOCATION)}/environments/{env_name}"
        env.config = env_config
        environments.append(env)
    
    # Configure list_environments method
    composer_client.list_environments.return_value = environments
    
    # Configure get_environment method
    def get_environment(name):
        for env in environments:
            if env.name == name:
                return env
        raise Exception(f"Environment {name} not found")
    
    composer_client.get_environment.side_effect = get_environment
    
    # Configure DAG-related methods if DAG configs provided
    if dag_configs:
        # Configure list_dags method
        def list_dags(parent):
            env_name = parent.split('/environments/')[1]
            if env_name in environment_configs:
                dags = []
                for dag_id, dag_config in dag_configs.items():
                    if dag_config.get('environment') == env_name:
                        dag = MagicMock()
                        dag.dag_id = dag_id
                        dag.schedule = dag_config.get('schedule')
                        dag.is_paused = dag_config.get('is_paused', False)
                        dags.append(dag)
                return dags
            return []
        
        composer_client.list_dags.side_effect = list_dags
        
        # Configure get_dag method
        def get_dag(name):
            parts = name.split('/')
            dag_id = parts[-1]
            env_name = parts[-3]
            
            if dag_id in dag_configs and dag_configs[dag_id].get('environment') == env_name:
                dag = MagicMock()
                dag.dag_id = dag_id
                dag.schedule = dag_configs[dag_id].get('schedule')
                dag.is_paused = dag_configs[dag_id].get('is_paused', False)
                return dag
            
            raise Exception(f"DAG {dag_id} not found in environment {env_name}")
        
        composer_client.get_dag.side_effect = get_dag
        
        # Configure list_dag_runs method
        def list_dag_runs(parent):
            parts = parent.split('/')
            dag_id = parts[-1]
            env_name = parts[-3]
            
            if dag_id in dag_configs and dag_configs[dag_id].get('environment') == env_name:
                dag_runs = []
                runs_config = dag_configs[dag_id].get('runs', [])
                
                for run_config in runs_config:
                    run = MagicMock()
                    run.dag_run_id = run_config.get('run_id', generate_unique_id())
                    run.state = run_config.get('state', 'RUNNING')
                    run.start_date = run_config.get('start_date')
                    run.end_date = run_config.get('end_date')
                    dag_runs.append(run)
                
                return dag_runs
            
            return []
        
        composer_client.list_dag_runs.side_effect = list_dag_runs
    
    return composer_client

def create_mock_monitoring_service(metric_configs: Dict, alert_configs: Dict = None,
                                 project_id: str = DEFAULT_PROJECT_ID) -> MagicMock:
    """
    Creates a mock Cloud Monitoring service with pre-configured metrics and alerts.
    
    Args:
        metric_configs: Dictionary of metric configurations
        alert_configs: Dictionary of alert policy configurations
        project_id: GCP project ID
        
    Returns:
        Configured mock Monitoring client
    """
    monitoring_client = MagicMock()
    
    # Configure list_time_series method
    def list_time_series(name, filter_str, interval, view=None):
        series = []
        
        # Parse filter to determine which metrics to return
        metric_type = None
        resource_type = None
        
        if 'metric.type' in filter_str:
            for part in filter_str.split(' AND '):
                if 'metric.type' in part:
                    metric_type = part.split('"')[1]
                elif 'resource.type' in part:
                    resource_type = part.split('"')[1]
        
        # Find matching metric configs
        for metric_name, metric_config in metric_configs.items():
            if (not metric_type or metric_config.get('type') == metric_type) and \
               (not resource_type or metric_config.get('resource_type') == resource_type):
                # Create time series
                ts = MagicMock()
                ts.metric.type = metric_config.get('type')
                ts.resource.type = metric_config.get('resource_type')
                ts.points = []
                
                # Add data points
                for point_config in metric_config.get('points', []):
                    point = MagicMock()
                    point.value.double_value = point_config.get('value')
                    point.interval.end_time = point_config.get('time')
                    ts.points.append(point)
                
                series.append(ts)
        
        return series
    
    monitoring_client.list_time_series.side_effect = list_time_series
    
    # Configure list_alert_policies method
    if alert_configs:
        def list_alert_policies(name):
            policies = []
            
            for policy_name, policy_config in alert_configs.items():
                policy = MagicMock()
                policy.name = f"projects/{project_id}/alertPolicies/{policy_name}"
                policy.display_name = policy_config.get('display_name', policy_name)
                policy.enabled = policy_config.get('enabled', True)
                policy.severity = policy_config.get('severity', 'WARNING')
                
                # Configure conditions
                conditions = []
                for condition_config in policy_config.get('conditions', []):
                    condition = MagicMock()
                    condition.display_name = condition_config.get('display_name')
                    condition.condition_threshold.filter = condition_config.get('filter')
                    condition.condition_threshold.comparison = condition_config.get('comparison')
                    condition.condition_threshold.threshold_value = condition_config.get('threshold')
                    conditions.append(condition)
                
                policy.conditions = conditions
                policies.append(policy)
            
            return policies
        
        monitoring_client.list_alert_policies.side_effect = list_alert_policies
    
    return monitoring_client

def create_mock_api_service(endpoint_configs: Dict, auth_config: Dict = None) -> MagicMock:
    """
    Creates a mock API service with pre-configured endpoints and responses.
    
    Args:
        endpoint_configs: Dictionary mapping endpoint URLs to response configurations
            Each response configuration should include 'status_code', 'content', and 'headers'
        auth_config: Authentication configuration
        
    Returns:
        Configured mock requests session
    """
    session = MagicMock()
    
    # Configure request methods (get, post, put, delete)
    def make_request_method(method_name):
        def request_method(url, *args, **kwargs):
            # Check if we have a configuration for this URL
            if url in endpoint_configs:
                config = endpoint_configs[url]
                
                # Get method-specific config if available, otherwise use common config
                method_config = config.get(method_name, config)
                
                # Configure response
                response = MagicMock()
                response.status_code = method_config.get('status_code', 200)
                response.headers = method_config.get('headers', {})
                
                # Configure content
                content = method_config.get('content', '')
                if isinstance(content, dict):
                    response.json.return_value = content
                    response.text = str(content)
                else:
                    response.text = str(content)
                    
                    # Configure json method to raise ValueError for non-JSON responses
                    def raise_value_error():
                        raise ValueError("Response content is not JSON")
                    
                    if method_config.get('content_type') != 'application/json':
                        response.json.side_effect = raise_value_error
                    else:
                        response.json.return_value = {}
                
                return response
            
            # If URL not configured, return generic 404 response
            response = MagicMock()
            response.status_code = 404
            response.text = "Not Found"
            response.json.side_effect = ValueError("Response content is not JSON")
            return response
        
        return request_method
    
    # Assign request methods
    session.get = make_request_method('get')
    session.post = make_request_method('post')
    session.put = make_request_method('put')
    session.delete = make_request_method('delete')
    
    # Configure authentication if provided
    if auth_config:
        auth_type = auth_config.get('type')
        
        if auth_type == 'basic':
            session.auth = (auth_config.get('username'), auth_config.get('password'))
        elif auth_type == 'token':
            headers = {'Authorization': f"Bearer {auth_config.get('token')}"}
            session.headers.update(headers)
        elif auth_type == 'api_key':
            param_name = auth_config.get('param_name', 'api_key')
            param_value = auth_config.get('param_value')
            session.params = {param_name: param_value}
    
    return session

def create_mock_database_service(table_configs: Dict, query_results: Dict = None,
                               database_type: str = 'postgresql') -> MagicMock:
    """
    Creates a mock database service with pre-configured tables and query results.
    
    Args:
        table_configs: Dictionary mapping table names to schema and data configurations
        query_results: Dictionary mapping query strings to result objects
        database_type: Type of database (postgresql, mysql, etc.)
        
    Returns:
        Configured mock database connection
    """
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    
    # Configure execute method
    def execute(query, params=None):
        # Check if we have a pre-configured result for this query
        if query_results and query in query_results:
            result = query_results[query]
            cursor.rowcount = len(result) if isinstance(result, list) else 1
            cursor.description = []
            
            # If result is list of dicts, extract column names for description
            if result and isinstance(result, list) and isinstance(result[0], dict):
                cursor.description = [(name, None, None, None, None, None, None) for name in result[0].keys()]
            
            # Configure fetchall and fetchone methods
            cursor.fetchall.return_value = result
            cursor.fetchone.return_value = result[0] if isinstance(result, list) and result else result
            
            return cursor
        
        # Handle table schema queries
        for table_name, table_config in table_configs.items():
            if f"FROM {table_name}" in query or f"from {table_name}" in query:
                schema = table_config.get('schema', [])
                data = table_config.get('data', [])
                
                cursor.rowcount = len(data)
                cursor.description = [(col['name'], None, None, None, None, None, None) for col in schema]
                
                cursor.fetchall.return_value = data
                cursor.fetchone.return_value = data[0] if data else None
                
                return cursor
        
        # Default empty result
        cursor.rowcount = 0
        cursor.description = []
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = None
        
        return cursor
    
    cursor.execute.side_effect = execute
    
    # Configure additional database-specific methods
    if database_type == 'postgresql':
        # PostgreSQL-specific configurations
        pass
    elif database_type == 'mysql':
        # MySQL-specific configurations
        pass
    
    return connection

def create_mock_pipeline_service(pipeline_configs: Dict, execution_configs: Dict = None) -> MagicMock:
    """
    Creates a mock pipeline service with pre-configured pipeline definitions and executions.
    
    Args:
        pipeline_configs: Dictionary of pipeline configurations
        execution_configs: Dictionary of execution configurations
        
    Returns:
        Configured mock pipeline service
    """
    pipeline_service = MagicMock()
    
    # Configure get_pipeline method
    def get_pipeline(pipeline_id):
        if pipeline_id in pipeline_configs:
            pipeline = MagicMock()
            pipeline.pipeline_id = pipeline_id
            pipeline.name = pipeline_configs[pipeline_id].get('name', pipeline_id)
            pipeline.description = pipeline_configs[pipeline_id].get('description', '')
            pipeline.source = pipeline_configs[pipeline_id].get('source', {})
            pipeline.destination = pipeline_configs[pipeline_id].get('destination', {})
            pipeline.transformations = pipeline_configs[pipeline_id].get('transformations', [])
            pipeline.schedule = pipeline_configs[pipeline_id].get('schedule', None)
            return pipeline
        
        raise ValueError(f"Pipeline {pipeline_id} not found")
    
    pipeline_service.get_pipeline.side_effect = get_pipeline
    
    # Configure list_pipelines method
    def list_pipelines():
        pipelines = []
        for pipeline_id, config in pipeline_configs.items():
            pipeline = MagicMock()
            pipeline.pipeline_id = pipeline_id
            pipeline.name = config.get('name', pipeline_id)
            pipeline.description = config.get('description', '')
            pipelines.append(pipeline)
        return pipelines
    
    pipeline_service.list_pipelines.return_value = list_pipelines()
    
    # Configure execution methods if execution configs provided
    if execution_configs:
        # Configure get_execution method
        def get_execution(execution_id):
            for pipeline_id, executions in execution_configs.items():
                for execution in executions:
                    if execution.get('execution_id') == execution_id:
                        exec_obj = MagicMock()
                        exec_obj.execution_id = execution_id
                        exec_obj.pipeline_id = pipeline_id
                        exec_obj.status = execution.get('status', PipelineStatus.RUNNING)
                        exec_obj.start_time = execution.get('start_time')
                        exec_obj.end_time = execution.get('end_time')
                        exec_obj.parameters = execution.get('parameters', {})
                        exec_obj.metrics = execution.get('metrics', {})
                        
                        # Configure tasks
                        tasks = []
                        for task_config in execution.get('tasks', []):
                            task = MagicMock()
                            task.task_id = task_config.get('task_id')
                            task.name = task_config.get('name')
                            task.status = task_config.get('status')
                            task.start_time = task_config.get('start_time')
                            task.end_time = task_config.get('end_time')
                            task.error = task_config.get('error')
                            tasks.append(task)
                        
                        exec_obj.tasks = tasks
                        return exec_obj
            
            raise ValueError(f"Execution {execution_id} not found")
        
        pipeline_service.get_execution.side_effect = get_execution
        
        # Configure list_executions method
        def list_executions(pipeline_id=None, status=None, limit=None):
            executions = []
            
            for pid, pipeline_executions in execution_configs.items():
                if pipeline_id and pid != pipeline_id:
                    continue
                
                for execution in pipeline_executions:
                    if status and execution.get('status') != status:
                        continue
                    
                    exec_obj = MagicMock()
                    exec_obj.execution_id = execution.get('execution_id')
                    exec_obj.pipeline_id = pid
                    exec_obj.status = execution.get('status', PipelineStatus.RUNNING)
                    exec_obj.start_time = execution.get('start_time')
                    exec_obj.end_time = execution.get('end_time')
                    
                    executions.append(exec_obj)
                    
                    if limit and len(executions) >= limit:
                        break
            
            return executions
        
        pipeline_service.list_executions.side_effect = list_executions
        
        # Configure execute_pipeline method
        def execute_pipeline(pipeline_id, parameters=None):
            if pipeline_id in pipeline_configs:
                execution_id = generate_unique_id()
                
                # Create new execution config
                execution = {
                    'execution_id': execution_id,
                    'status': PipelineStatus.RUNNING,
                    'start_time': 'now',  # Would be an actual timestamp in real implementation
                    'parameters': parameters or {},
                    'tasks': []
                }
                
                # Add execution to configs
                if pipeline_id not in execution_configs:
                    execution_configs[pipeline_id] = []
                
                execution_configs[pipeline_id].append(execution)
                
                return execution_id
            
            raise ValueError(f"Pipeline {pipeline_id} not found")
        
        pipeline_service.execute_pipeline.side_effect = execute_pipeline
    
    return pipeline_service


class MockServiceFactory:
    """Factory class for creating and managing mock services for testing."""
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Initialize the MockServiceFactory.
        
        Args:
            project_id: GCP project ID
            location: GCP location
        """
        self.project_id = project_id or DEFAULT_PROJECT_ID
        self.location = location or DEFAULT_LOCATION
        self._services = {}
        
    def create_gcs_service(self, bucket_configs: Dict) -> MockGCSClient:
        """
        Create a mock GCS service.
        
        Args:
            bucket_configs: Dictionary mapping bucket names to lists of file configurations
            
        Returns:
            Configured mock GCS client
        """
        service = create_mock_gcs_service(bucket_configs, self.project_id)
        self._services['gcs'] = service
        return service
    
    def create_bigquery_service(self, dataset_configs: Dict, query_results: Dict = None) -> MockBigQueryClient:
        """
        Create a mock BigQuery service.
        
        Args:
            dataset_configs: Dictionary mapping dataset IDs to lists of table configurations
            query_results: Dictionary mapping query strings to result objects
            
        Returns:
            Configured mock BigQuery client
        """
        service = create_mock_bigquery_service(dataset_configs, query_results, self.project_id)
        self._services['bigquery'] = service
        return service
    
    def create_vertex_ai_service(self, model_configs: Dict, endpoint_configs: Dict = None,
                               prediction_results: Dict = None) -> MockVertexAIClient:
        """
        Create a mock Vertex AI service.
        
        Args:
            model_configs: Dictionary of model configurations
            endpoint_configs: Dictionary of endpoint configurations mapping endpoints to models
            prediction_results: Dictionary mapping endpoint IDs to prediction results
            
        Returns:
            Configured mock Vertex AI client
        """
        service = create_mock_vertex_ai_service(
            model_configs, 
            endpoint_configs, 
            prediction_results, 
            self.project_id
        )
        self._services['vertex_ai'] = service
        return service
    
    def create_pubsub_service(self, topic_configs: Dict) -> MockPubSubClient:
        """
        Create a mock Pub/Sub service.
        
        Args:
            topic_configs: Dictionary mapping topic IDs to configurations
            
        Returns:
            Configured mock Pub/Sub client
        """
        service = create_mock_pubsub_service(topic_configs, self.project_id)
        self._services['pubsub'] = service
        return service
    
    def create_composer_service(self, environment_configs: Dict, 
                               dag_configs: Dict = None) -> MagicMock:
        """
        Create a mock Cloud Composer service.
        
        Args:
            environment_configs: Dictionary of Composer environment configurations
            dag_configs: Dictionary of DAG configurations
            
        Returns:
            Configured mock Composer client
        """
        service = create_mock_composer_service(environment_configs, dag_configs, self.project_id)
        self._services['composer'] = service
        return service
    
    def create_monitoring_service(self, metric_configs: Dict, 
                                alert_configs: Dict = None) -> MagicMock:
        """
        Create a mock Cloud Monitoring service.
        
        Args:
            metric_configs: Dictionary of metric configurations
            alert_configs: Dictionary of alert policy configurations
            
        Returns:
            Configured mock Monitoring client
        """
        service = create_mock_monitoring_service(metric_configs, alert_configs, self.project_id)
        self._services['monitoring'] = service
        return service
    
    def create_api_service(self, endpoint_configs: Dict, auth_config: Dict = None) -> MagicMock:
        """
        Create a mock API service.
        
        Args:
            endpoint_configs: Dictionary mapping endpoint URLs to response configurations
            auth_config: Authentication configuration
            
        Returns:
            Configured mock requests session
        """
        service = create_mock_api_service(endpoint_configs, auth_config)
        self._services['api'] = service
        return service
    
    def create_database_service(self, table_configs: Dict, query_results: Dict = None,
                               database_type: str = 'postgresql') -> MagicMock:
        """
        Create a mock database service.
        
        Args:
            table_configs: Dictionary mapping table names to schema and data configurations
            query_results: Dictionary mapping query strings to result objects
            database_type: Type of database (postgresql, mysql, etc.)
            
        Returns:
            Configured mock database connection
        """
        service = create_mock_database_service(table_configs, query_results, database_type)
        self._services['database'] = service
        return service
    
    def create_pipeline_service(self, pipeline_configs: Dict, 
                              execution_configs: Dict = None) -> MagicMock:
        """
        Create a mock pipeline service.
        
        Args:
            pipeline_configs: Dictionary of pipeline configurations
            execution_configs: Dictionary of execution configurations
            
        Returns:
            Configured mock pipeline service
        """
        service = create_mock_pipeline_service(pipeline_configs, execution_configs)
        self._services['pipeline'] = service
        return service
    
    def get_service(self, service_name: str) -> Any:
        """
        Get a previously created service.
        
        Args:
            service_name: Name of the service to retrieve
            
        Returns:
            The requested service
            
        Raises:
            KeyError: If service not found
        """
        if service_name in self._services:
            return self._services[service_name]
        raise KeyError(f"Service '{service_name}' not found. Available services: {list(self._services.keys())}")
    
    def reset(self) -> None:
        """
        Reset all services.
        """
        self._services = {}


class MockServiceEnvironment:
    """Context manager for setting up and tearing down a complete mock service environment."""
    
    def __init__(self, service_configs: Dict, project_id: str = None, location: str = None):
        """
        Initialize the MockServiceEnvironment.
        
        Args:
            service_configs: Dictionary mapping service names to configurations
            project_id: GCP project ID
            location: GCP location
        """
        self.service_factory = MockServiceFactory(project_id, location)
        self.service_configs = service_configs
        self.patches = []
        
    def __enter__(self) -> 'MockServiceEnvironment':
        """
        Set up the mock service environment.
        
        Returns:
            Self reference for use in with statement
        """
        # Create configured services
        for service_name, config in self.service_configs.items():
            if service_name == 'gcs':
                self.service_factory.create_gcs_service(**config)
            elif service_name == 'bigquery':
                self.service_factory.create_bigquery_service(**config)
            elif service_name == 'vertex_ai':
                self.service_factory.create_vertex_ai_service(**config)
            elif service_name == 'pubsub':
                self.service_factory.create_pubsub_service(**config)
            elif service_name == 'composer':
                self.service_factory.create_composer_service(**config)
            elif service_name == 'monitoring':
                self.service_factory.create_monitoring_service(**config)
            elif service_name == 'api':
                self.service_factory.create_api_service(**config)
            elif service_name == 'database':
                self.service_factory.create_database_service(**config)
            elif service_name == 'pipeline':
                self.service_factory.create_pipeline_service(**config)
        
        # Set up patches to replace real services with mocks
        if 'gcs' in self.service_configs:
            patch_gcs = patch('google.cloud.storage.Client', 
                            return_value=self.service_factory.get_service('gcs'))
            self.patches.append(patch_gcs.start())
            
        if 'bigquery' in self.service_configs:
            patch_bq = patch('google.cloud.bigquery.Client', 
                           return_value=self.service_factory.get_service('bigquery'))
            self.patches.append(patch_bq.start())
            
        if 'vertex_ai' in self.service_configs:
            patch_ai = patch('google.cloud.aiplatform.Endpoint', 
                          return_value=self.service_factory.get_service('vertex_ai'))
            self.patches.append(patch_ai.start())
            
        if 'pubsub' in self.service_configs:
            patch_pubsub = patch('google.cloud.pubsub.PublisherClient', 
                               return_value=self.service_factory.get_service('pubsub'))
            self.patches.append(patch_pubsub.start())
            
        if 'composer' in self.service_configs:
            patch_composer = patch('google.cloud.composer.EnvironmentsClient', 
                                 return_value=self.service_factory.get_service('composer'))
            self.patches.append(patch_composer.start())
            
        if 'monitoring' in self.service_configs:
            patch_monitoring = patch('google.cloud.monitoring.MetricServiceClient', 
                                   return_value=self.service_factory.get_service('monitoring'))
            self.patches.append(patch_monitoring.start())
            
        if 'api' in self.service_configs:
            patch_requests = patch('requests.Session', 
                                 return_value=self.service_factory.get_service('api'))
            self.patches.append(patch_requests.start())
        
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Tear down the mock service environment.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        # Stop all patches
        for patch in self.patches:
            patch.stop()
        
        # Reset the service factory
        self.service_factory.reset()
        
        # Return None to propagate any exceptions
        return None
    
    def get_service(self, service_name: str) -> Any:
        """
        Get a service from the environment.
        
        Args:
            service_name: Name of the service to retrieve
            
        Returns:
            The requested service
        """
        return self.service_factory.get_service(service_name)
    
    def add_service(self, service_name: str, service_config: Dict) -> Any:
        """
        Add a new service to the environment.
        
        Args:
            service_name: Name of the service to add
            service_config: Configuration for the service
            
        Returns:
            The created service
        """
        method_name = f"create_{service_name}_service"
        if hasattr(self.service_factory, method_name):
            method = getattr(self.service_factory, method_name)
            return method(**service_config)
        
        raise ValueError(f"Unknown service type: {service_name}")


class MockPipelineTestEnvironment:
    """Specialized environment for testing data pipelines with pre-configured services."""
    
    def __init__(self, pipeline_configs: Dict, test_data: Dict = None, project_id: str = None):
        """
        Initialize the MockPipelineTestEnvironment.
        
        Args:
            pipeline_configs: Dictionary of pipeline configurations
            test_data: Dictionary of test data for various services
            project_id: GCP project ID
        """
        self.pipeline_configs = pipeline_configs
        self.test_data = test_data or {}
        
        # Determine required services based on pipeline configs
        service_configs = self._determine_service_configs()
        
        # Create the service environment
        self.service_env = MockServiceEnvironment(service_configs, project_id)
    
    def _determine_service_configs(self) -> Dict:
        """
        Determine required service configurations based on pipeline configs.
        
        Returns:
            Dictionary of service configurations
        """
        service_configs = {}
        
        # Analyze pipeline configs to determine required services
        for pipeline_id, config in self.pipeline_configs.items():
            source_type = config.get('source', {}).get('type')
            destination_type = config.get('destination', {}).get('type')
            
            # Configure GCS if needed
            if source_type == DataSourceType.GCS or destination_type == DataSourceType.GCS:
                service_configs['gcs'] = {
                    'bucket_configs': self.test_data.get('gcs_buckets', {})
                }
            
            # Configure BigQuery if needed
            if source_type == DataSourceType.BIGQUERY or destination_type == DataSourceType.BIGQUERY:
                service_configs['bigquery'] = {
                    'dataset_configs': self.test_data.get('bigquery_datasets', {}),
                    'query_results': self.test_data.get('bigquery_queries', {})
                }
            
            # Configure Cloud SQL if needed
            if source_type == DataSourceType.CLOUD_SQL:
                service_configs['database'] = {
                    'table_configs': self.test_data.get('database_tables', {}),
                    'query_results': self.test_data.get('database_queries', {})
                }
            
            # Configure API if needed
            if source_type == DataSourceType.API:
                service_configs['api'] = {
                    'endpoint_configs': self.test_data.get('api_endpoints', {}),
                    'auth_config': self.test_data.get('api_auth', {})
                }
        
        # Add self-healing related services
        service_configs['vertex_ai'] = {
            'model_configs': self.test_data.get('ai_models', {}),
            'endpoint_configs': self.test_data.get('ai_endpoints', {}),
            'prediction_results': self.test_data.get('ai_predictions', {})
        }
        
        # Add monitoring
        service_configs['monitoring'] = {
            'metric_configs': self.test_data.get('metrics', {}),
            'alert_configs': self.test_data.get('alerts', {})
        }
        
        # Add pipeline service
        service_configs['pipeline'] = {
            'pipeline_configs': self.pipeline_configs,
            'execution_configs': self.test_data.get('executions', {})
        }
        
        # Add composer service if orchestration is needed
        service_configs['composer'] = {
            'environment_configs': self.test_data.get('composer_environments', {}),
            'dag_configs': self.test_data.get('composer_dags', {})
        }
        
        return service_configs
    
    def __enter__(self) -> 'MockPipelineTestEnvironment':
        """
        Set up the pipeline test environment.
        
        Returns:
            Self reference for use in with statement
        """
        # Enter the service environment context
        self.service_env.__enter__()
        
        # Set up test data in appropriate services
        self._setup_test_data()
        
        # Configure pipeline-specific mocks
        self._setup_pipeline_mocks()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Tear down the pipeline test environment.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        # Exit the service environment context
        self.service_env.__exit__(exc_type, exc_val, exc_tb)
        
        # Return None to propagate any exceptions
        return None
    
    def _setup_test_data(self) -> None:
        """Set up test data in the appropriate services."""
        # Additional setup beyond basic service configuration
        pass
    
    def _setup_pipeline_mocks(self) -> None:
        """Configure pipeline-specific mocks."""
        # Additional pipeline-specific mock configurations
        pass
    
    def execute_pipeline(self, pipeline_id: str, parameters: Dict = None) -> Dict:
        """
        Execute a pipeline in the test environment.
        
        Args:
            pipeline_id: ID of the pipeline to execute
            parameters: Parameters for the pipeline execution
            
        Returns:
            Pipeline execution results
            
        Raises:
            ValueError: If pipeline doesn't exist
        """
        # Get the pipeline service
        pipeline_service = self.service_env.get_service('pipeline')
        
        # Execute the pipeline
        execution_id = pipeline_service.execute_pipeline(pipeline_id, parameters)
        
        # Mock the execution process
        self._simulate_pipeline_execution(pipeline_id, execution_id, parameters)
        
        # Get final execution state
        execution = pipeline_service.get_execution(execution_id)
        
        # Return execution details
        return {
            'execution_id': execution_id,
            'pipeline_id': pipeline_id,
            'status': execution.status,
            'start_time': execution.start_time,
            'end_time': execution.end_time,
            'tasks': execution.tasks,
            'metrics': execution.metrics
        }
    
    def _simulate_pipeline_execution(self, pipeline_id: str, execution_id: str, parameters: Dict) -> None:
        """
        Simulate the execution of a pipeline.
        
        Args:
            pipeline_id: ID of the pipeline being executed
            execution_id: ID of the execution
            parameters: Parameters for the execution
        """
        # Get services
        pipeline_service = self.service_env.get_service('pipeline')
        
        # Get execution configs
        for executions in self.test_data.get('executions', {}).values():
            for execution in executions:
                if execution.get('execution_id') == execution_id:
                    # Set status to SUCCESS unless configured differently
                    execution['status'] = execution.get('status', PipelineStatus.SUCCESS)
                    execution['end_time'] = 'completed'  # Would be actual timestamp in real implementation
                    
                    # Update task statuses based on overall status
                    for task in execution.get('tasks', []):
                        if execution['status'] == PipelineStatus.SUCCESS:
                            task['status'] = task.get('status', 'SUCCESS')
                            task['end_time'] = task.get('end_time', 'completed')
                        elif execution['status'] == PipelineStatus.FAILED:
                            # Set at least one task to FAILED
                            if 'error' in task:
                                task['status'] = 'FAILED'
                                task['end_time'] = task.get('end_time', 'completed')
                    
                    return
        
        # If no pre-configured execution found, create a simple successful one
        simple_execution = {
            'execution_id': execution_id,
            'status': PipelineStatus.SUCCESS,
            'start_time': 'started',
            'end_time': 'completed',
            'parameters': parameters or {},
            'tasks': [
                {
                    'task_id': 'extract',
                    'name': 'Extract Data',
                    'status': 'SUCCESS',
                    'start_time': 'started',
                    'end_time': 'completed'
                },
                {
                    'task_id': 'transform',
                    'name': 'Transform Data',
                    'status': 'SUCCESS',
                    'start_time': 'started',
                    'end_time': 'completed'
                },
                {
                    'task_id': 'load',
                    'name': 'Load Data',
                    'status': 'SUCCESS',
                    'start_time': 'started',
                    'end_time': 'completed'
                }
            ]
        }
        
        # Add execution to configs
        if pipeline_id not in self.test_data.get('executions', {}):
            if 'executions' not in self.test_data:
                self.test_data['executions'] = {}
            self.test_data['executions'][pipeline_id] = []
        
        self.test_data['executions'][pipeline_id].append(simple_execution)
    
    def verify_data_quality(self, pipeline_id: str, execution_id: str) -> Dict:
        """
        Verify data quality results for a pipeline.
        
        Args:
            pipeline_id: ID of the pipeline
            execution_id: ID of the execution
            
        Returns:
            Quality validation results
        """
        # Mock quality validation results
        quality_results = {
            'execution_id': execution_id,
            'pipeline_id': pipeline_id,
            'validation_time': 'now',  # Would be actual timestamp in real implementation
            'total_validations': 5,
            'passed_validations': 5,
            'failed_validations': 0,
            'quality_score': 1.0,
            'validation_results': [
                {
                    'rule_id': 'schema_validation',
                    'rule_name': 'Schema Validation',
                    'rule_type': 'schema',
                    'dimension': 'completeness',
                    'success': True,
                    'details': {}
                },
                {
                    'rule_id': 'null_check',
                    'rule_name': 'Null Check',
                    'rule_type': 'null_check',
                    'dimension': 'completeness',
                    'success': True,
                    'details': {}
                },
                {
                    'rule_id': 'data_type',
                    'rule_name': 'Data Type Validation',
                    'rule_type': 'data_type',
                    'dimension': 'validity',
                    'success': True,
                    'details': {}
                },
                {
                    'rule_id': 'range_check',
                    'rule_name': 'Range Check',
                    'rule_type': 'range',
                    'dimension': 'accuracy',
                    'success': True,
                    'details': {}
                },
                {
                    'rule_id': 'uniqueness',
                    'rule_name': 'Uniqueness Check',
                    'rule_type': 'uniqueness',
                    'dimension': 'uniqueness',
                    'success': True,
                    'details': {}
                }
            ]
        }
        
        # Check if we have pre-configured quality results
        if 'quality_results' in self.test_data:
            for result in self.test_data['quality_results']:
                if result.get('execution_id') == execution_id:
                    return result
        
        return quality_results
    
    def verify_data_transformation(self, pipeline_id: str, execution_id: str, target_table: str) -> pd.DataFrame:
        """
        Verify data transformation results for a pipeline.
        
        Args:
            pipeline_id: ID of the pipeline
            execution_id: ID of the execution
            target_table: Target table to verify
            
        Returns:
            Transformed data as DataFrame
        """
        # Parse table reference
        parts = target_table.split('.')
        if len(parts) == 2:
            dataset_id, table_id = parts
        elif len(parts) == 3:
            _, dataset_id, table_id = parts
        else:
            raise ValueError(f"Invalid table reference: {target_table}")
        
        # Get BigQuery service
        bigquery = self.service_env.get_service('bigquery')
        
        # Get table data
        if bigquery.table_exists(dataset_id, table_id):
            table = bigquery.get_table(dataset_id, table_id)
            if hasattr(table, 'data') and isinstance(table.data, pd.DataFrame):
                return table.data
        
        # Return empty DataFrame if table not found or has no data
        return pd.DataFrame()
    
    def inject_error(self, error_type: str, component: str, error_details: Dict = None) -> None:
        """
        Inject an error into the pipeline execution.
        
        Args:
            error_type: Type of error to inject
            component: Component to inject error into
            error_details: Details of the error
        """
        error_details = error_details or {}
        
        # Determine which service to modify based on component
        if component == 'gcs':
            service = self.service_env.get_service('gcs')
            bucket_name = error_details.get('bucket_name')
            blob_name = error_details.get('blob_name')
            
            if error_type == 'not_found':
                # Configure to raise NotFound for specific blob
                def raise_not_found(*args, **kwargs):
                    error = type('NotFound', (Exception,), {})
                    raise error(f"Blob {blob_name} not found in bucket {bucket_name}")
                
                # Find the appropriate method to patch
                if hasattr(service, 'download_as_string'):
                    original_method = service.download_as_string
                    service.download_as_string = lambda b, n: raise_not_found() if b == bucket_name and n == blob_name else original_method(b, n)
        
        elif component == 'bigquery':
            service = self.service_env.get_service('bigquery')
            dataset_id = error_details.get('dataset_id')
            table_id = error_details.get('table_id')
            query = error_details.get('query')
            
            if error_type == 'query_error':
                # Configure to raise error for specific query
                def raise_query_error(*args, **kwargs):
                    raise ValueError(error_details.get('message', 'Query error'))
                
                # Patch execute_query method for specific query
                if query:
                    original_method = service.execute_query
                    service.execute_query = lambda q, *args, **kwargs: raise_query_error() if q == query else original_method(q, *args, **kwargs)
        
        elif component == 'vertex_ai':
            service = self.service_env.get_service('vertex_ai')
            endpoint_id = error_details.get('endpoint_id')
            
            if error_type == 'prediction_error':
                # Configure to raise error for specific prediction
                def raise_prediction_error(*args, **kwargs):
                    raise ValueError(error_details.get('message', 'Prediction error'))
                
                # Patch predict method for specific endpoint
                if endpoint_id:
                    original_method = service.predict
                    service.predict = lambda e, *args, **kwargs: raise_prediction_error() if e == endpoint_id else original_method(e, *args, **kwargs)
    
    def verify_self_healing(self, pipeline_id: str, execution_id: str) -> Dict:
        """
        Verify self-healing actions for a pipeline execution.
        
        Args:
            pipeline_id: ID of the pipeline
            execution_id: ID of the execution
            
        Returns:
            Self-healing actions and results
        """
        # Mock self-healing results
        healing_results = {
            'execution_id': execution_id,
            'pipeline_id': pipeline_id,
            'healing_time': 'now',  # Would be actual timestamp in real implementation
            'issues_detected': 1,
            'issues_resolved': 1,
            'success_rate': 1.0,
            'healing_actions': [
                {
                    'action_id': generate_unique_id('action'),
                    'action_type': 'data_correction',
                    'issue_type': 'missing_values',
                    'confidence_score': 0.95,
                    'successful': True,
                    'parameters': {
                        'column': 'example_column',
                        'method': 'imputation',
                        'value': 'default_value'
                    },
                    'result': {
                        'records_affected': 5,
                        'execution_time': 0.8,
                        'timestamp': 'now'  # Would be actual timestamp in real implementation
                    }
                }
            ]
        }
        
        # Check if we have pre-configured healing results
        if 'healing_results' in self.test_data:
            for result in self.test_data['healing_results']:
                if result.get('execution_id') == execution_id:
                    return result
        
        return healing_results