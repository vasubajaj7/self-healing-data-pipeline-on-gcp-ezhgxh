import random
import json
import uuid
import time
import datetime
from locust import User, TaskSet, task, tag, constant, between

from src.test.utils.api_test_utils import (
    create_test_auth_headers,
    load_api_test_data,
    create_pagination_params
)

from src.test.utils.test_data_generators import (
    generate_random_string,
    DataGenerator
)

# Global constants
API_BASE_URL = '/api/ingestion'
SOURCE_TYPES = ['GCS', 'CLOUD_SQL', 'API', 'CUSTOM']
TEST_USER_CREDENTIALS = {
    'username': 'test_user',
    'password': 'test_password',
    'roles': ['admin']
}


class SourceSystemTasks(TaskSet):
    """TaskSet for testing data source system API endpoints"""
    
    def __init__(self, parent):
        """Initialize the SourceSystemTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.source_ids = []
        # Get auth headers from parent if available
        self.client.headers = getattr(parent, 'auth_headers', {})
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data
        self.test_data = {
            'source_systems': load_api_test_data('source_systems.json')
        }
        
        # Get available source_ids from parent if available
        if hasattr(self.parent, 'source_ids'):
            self.source_ids = self.parent.source_ids

    @task(5)
    @tag('source', 'read')
    def get_source_systems(self):
        """Task to test GET /source-systems endpoint with pagination"""
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/source-systems", params=params, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'metadata' in data:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get source systems: {response.status_code}")
    
    @task(3)
    @tag('source', 'read')
    def get_source_system_by_id(self):
        """Task to test GET /source-systems/{source_id} endpoint"""
        # Skip if no source IDs are available
        if not self.source_ids:
            return
        
        # Select random source ID
        source_id = random.choice(self.source_ids)
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/source-systems/{source_id}", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'source_id' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get source system: {response.status_code}")
    
    @task(2)
    @tag('source', 'create')
    def create_source_system(self):
        """Task to test POST /source-systems endpoint"""
        # Generate random source system data
        source_type = random.choice(SOURCE_TYPES)
        source_data = {
            'source_name': f"Test Source {generate_random_string(5)}",
            'source_type': source_type,
            'connection_details': {
                'bucket': f"test-bucket-{generate_random_string(5)}" if source_type == 'GCS' else None,
                'host': f"test-host-{generate_random_string(5)}" if source_type == 'CLOUD_SQL' else None,
                'url': f"https://api-{generate_random_string(5)}.example.com" if source_type == 'API' else None,
                'custom_config': {
                    'key': generate_random_string(10)
                } if source_type == 'CUSTOM' else None
            }
        }
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/source-systems", json=source_data, catch_response=True) as response:
            if response.status_code == 201:
                data = response.json()
                # Validate response and store source_id
                if 'data' in data and 'source_id' in data['data']:
                    self.source_ids.append(data['data']['source_id'])
                    # Also update parent's source_ids if available
                    if hasattr(self.parent, 'source_ids'):
                        self.parent.source_ids.append(data['data']['source_id'])
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to create source system: {response.status_code}")
    
    @task(2)
    @tag('source', 'update')
    def update_source_system(self):
        """Task to test PUT /source-systems/{source_id} endpoint"""
        # Skip if no source IDs are available
        if not self.source_ids:
            return
        
        # Select random source ID
        source_id = random.choice(self.source_ids)
        
        # Generate updated source data
        update_data = {
            'source_name': f"Updated Source {generate_random_string(5)}",
            'connection_details': {
                'updated_config': generate_random_string(10)
            }
        }
        
        # Make the request
        with self.client.put(f"{API_BASE_URL}/source-systems/{source_id}", json=update_data, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'source_id' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to update source system: {response.status_code}")
    
    @task(1)
    @tag('source', 'delete')
    def delete_source_system(self):
        """Task to test DELETE /source-systems/{source_id} endpoint"""
        # Skip if no source IDs are available
        if not self.source_ids:
            return
        
        # Select random source ID
        source_id = random.choice(self.source_ids)
        
        # Make the request
        with self.client.delete(f"{API_BASE_URL}/source-systems/{source_id}", catch_response=True) as response:
            if response.status_code == 204:
                # Remove source_id from list
                self.source_ids.remove(source_id)
                # Also update parent's source_ids if available
                if hasattr(self.parent, 'source_ids') and source_id in self.parent.source_ids:
                    self.parent.source_ids.remove(source_id)
                response.success()
            else:
                response.failure(f"Failed to delete source system: {response.status_code}")
    
    @task(2)
    @tag('source', 'test')
    def test_source_connection(self):
        """Task to test POST /source-systems/test-connection endpoint"""
        # Generate random source connection data
        source_type = random.choice(SOURCE_TYPES)
        connection_data = {
            'source_type': source_type,
            'connection_details': {
                'bucket': f"test-bucket-{generate_random_string(5)}" if source_type == 'GCS' else None,
                'host': f"test-host-{generate_random_string(5)}" if source_type == 'CLOUD_SQL' else None,
                'url': f"https://api-{generate_random_string(5)}.example.com" if source_type == 'API' else None,
                'custom_config': {
                    'key': generate_random_string(10)
                } if source_type == 'CUSTOM' else None
            }
        }
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/source-systems/test-connection", json=connection_data, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'status' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to test source connection: {response.status_code}")
    
    @task(2)
    @tag('source', 'schema', 'read')
    def get_source_schema(self):
        """Task to test GET /source-systems/{source_id}/schema/{object_name} endpoint"""
        # Skip if no source IDs are available
        if not self.source_ids:
            return
        
        # Select random source ID
        source_id = random.choice(self.source_ids)
        
        # Generate random object name
        object_name = f"test_object_{generate_random_string(5)}"
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/source-systems/{source_id}/schema/{object_name}", catch_response=True) as response:
            # This might return 404 for non-existent objects, which is fine for testing
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure(f"Failed to get source schema: {response.status_code}")
    
    @task(1)
    @tag('source', 'types', 'read')
    def get_supported_source_types(self):
        """Task to test GET /source-systems/types endpoint"""
        # Make the request
        with self.client.get(f"{API_BASE_URL}/source-systems/types", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and isinstance(data['data'], list):
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get supported source types: {response.status_code}")


class PipelineTasks(TaskSet):
    """TaskSet for testing pipeline definition API endpoints"""
    
    def __init__(self, parent):
        """Initialize the PipelineTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.pipeline_ids = []
        self.source_ids = []
        # Get auth headers from parent if available
        self.client.headers = getattr(parent, 'auth_headers', {})
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data
        self.test_data = {
            'pipelines': load_api_test_data('pipelines.json')
        }
        
        # Get available source_ids from parent if available
        if hasattr(self.parent, 'source_ids'):
            self.source_ids = self.parent.source_ids
            
        # Get available pipeline_ids from parent if available
        if hasattr(self.parent, 'pipeline_ids'):
            self.pipeline_ids = self.parent.pipeline_ids
    
    @task(5)
    @tag('pipeline', 'read')
    def get_pipelines(self):
        """Task to test GET /pipelines endpoint with pagination"""
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/pipelines", params=params, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'metadata' in data:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get pipelines: {response.status_code}")
    
    @task(3)
    @tag('pipeline', 'read')
    def get_pipeline_by_id(self):
        """Task to test GET /pipelines/{pipeline_id} endpoint"""
        # Skip if no pipeline IDs are available
        if not self.pipeline_ids:
            return
        
        # Select random pipeline ID
        pipeline_id = random.choice(self.pipeline_ids)
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/pipelines/{pipeline_id}", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'pipeline_id' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get pipeline: {response.status_code}")
    
    @task(2)
    @tag('pipeline', 'create')
    def create_pipeline(self):
        """Task to test POST /pipelines endpoint"""
        # Skip if no source IDs are available
        if not self.source_ids:
            return
        
        # Select random source ID
        source_id = random.choice(self.source_ids)
        
        # Generate random pipeline data
        pipeline_data = {
            'pipeline_name': f"Test Pipeline {generate_random_string(5)}",
            'source_id': source_id,
            'target_dataset': f"test_dataset_{generate_random_string(5)}",
            'target_table': f"test_table_{generate_random_string(5)}",
            'schedule': "0 */6 * * *",  # Every 6 hours
            'enabled': True,
            'configuration': {
                'parameters': {
                    'param1': generate_random_string(5),
                    'param2': random.randint(1, 100)
                }
            }
        }
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/pipelines", json=pipeline_data, catch_response=True) as response:
            if response.status_code == 201:
                data = response.json()
                # Validate response and store pipeline_id
                if 'data' in data and 'pipeline_id' in data['data']:
                    self.pipeline_ids.append(data['data']['pipeline_id'])
                    # Also update parent's pipeline_ids if available
                    if hasattr(self.parent, 'pipeline_ids'):
                        self.parent.pipeline_ids.append(data['data']['pipeline_id'])
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to create pipeline: {response.status_code}")
    
    @task(2)
    @tag('pipeline', 'update')
    def update_pipeline(self):
        """Task to test PUT /pipelines/{pipeline_id} endpoint"""
        # Skip if no pipeline IDs are available
        if not self.pipeline_ids:
            return
        
        # Select random pipeline ID
        pipeline_id = random.choice(self.pipeline_ids)
        
        # Generate updated pipeline data
        update_data = {
            'pipeline_name': f"Updated Pipeline {generate_random_string(5)}",
            'schedule': "0 */12 * * *",  # Every 12 hours
            'configuration': {
                'parameters': {
                    'param1': generate_random_string(5),
                    'param3': True
                }
            }
        }
        
        # Make the request
        with self.client.put(f"{API_BASE_URL}/pipelines/{pipeline_id}", json=update_data, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'pipeline_id' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to update pipeline: {response.status_code}")
    
    @task(1)
    @tag('pipeline', 'delete')
    def delete_pipeline(self):
        """Task to test DELETE /pipelines/{pipeline_id} endpoint"""
        # Skip if no pipeline IDs are available
        if not self.pipeline_ids:
            return
        
        # Select random pipeline ID
        pipeline_id = random.choice(self.pipeline_ids)
        
        # Make the request
        with self.client.delete(f"{API_BASE_URL}/pipelines/{pipeline_id}", catch_response=True) as response:
            if response.status_code == 204:
                # Remove pipeline_id from list
                self.pipeline_ids.remove(pipeline_id)
                # Also update parent's pipeline_ids if available
                if hasattr(self.parent, 'pipeline_ids') and pipeline_id in self.parent.pipeline_ids:
                    self.parent.pipeline_ids.remove(pipeline_id)
                response.success()
            else:
                response.failure(f"Failed to delete pipeline: {response.status_code}")


class PipelineExecutionTasks(TaskSet):
    """TaskSet for testing pipeline execution API endpoints"""
    
    def __init__(self, parent):
        """Initialize the PipelineExecutionTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.pipeline_ids = []
        self.execution_ids = []
        # Get auth headers from parent if available
        self.client.headers = getattr(parent, 'auth_headers', {})
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data
        self.test_data = {
            'executions': load_api_test_data('pipeline_executions.json')
        }
        
        # Get available pipeline_ids from parent if available
        if hasattr(self.parent, 'pipeline_ids'):
            self.pipeline_ids = self.parent.pipeline_ids
            
        # Get available execution_ids from parent if available
        if hasattr(self.parent, 'execution_ids'):
            self.execution_ids = self.parent.execution_ids
    
    @task(3)
    @tag('execution', 'create')
    def execute_pipeline(self):
        """Task to test POST /pipelines/{pipeline_id}/execute endpoint"""
        # Skip if no pipeline IDs are available
        if not self.pipeline_ids:
            return
        
        # Select random pipeline ID
        pipeline_id = random.choice(self.pipeline_ids)
        
        # Generate random execution parameters
        execution_params = {
            'parameters': {
                'param1': generate_random_string(5),
                'param2': random.randint(1, 100),
                'run_id': str(uuid.uuid4())
            }
        }
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/pipelines/{pipeline_id}/execute", json=execution_params, catch_response=True) as response:
            if response.status_code == 202:
                data = response.json()
                # Validate response and store execution_id
                if 'data' in data and 'execution_id' in data['data']:
                    self.execution_ids.append(data['data']['execution_id'])
                    # Also update parent's execution_ids if available
                    if hasattr(self.parent, 'execution_ids'):
                        self.parent.execution_ids.append(data['data']['execution_id'])
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to execute pipeline: {response.status_code}")
    
    @task(5)
    @tag('execution', 'read')
    def get_pipeline_executions(self):
        """Task to test GET /pipelines/{pipeline_id}/executions endpoint with pagination"""
        # Skip if no pipeline IDs are available
        if not self.pipeline_ids:
            return
        
        # Select random pipeline ID
        pipeline_id = random.choice(self.pipeline_ids)
        
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Generate random date range for filtering (last 7 days)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=random.randint(1, 7))
        
        params['start_date'] = start_date.isoformat()
        params['end_date'] = end_date.isoformat()
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/pipelines/{pipeline_id}/executions", params=params, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'metadata' in data:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get pipeline executions: {response.status_code}")
    
    @task(4)
    @tag('execution', 'read')
    def get_pipeline_execution_by_id(self):
        """Task to test GET /executions/{execution_id} endpoint"""
        # Skip if no execution IDs are available
        if not self.execution_ids:
            return
        
        # Select random execution ID
        execution_id = random.choice(self.execution_ids)
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/executions/{execution_id}", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'execution_id' in data['data']:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get pipeline execution: {response.status_code}")
    
    @task(3)
    @tag('execution', 'tasks', 'read')
    def get_task_executions(self):
        """Task to test GET /executions/{execution_id}/tasks endpoint with pagination"""
        # Skip if no execution IDs are available
        if not self.execution_ids:
            return
        
        # Select random execution ID
        execution_id = random.choice(self.execution_ids)
        
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 3),
            page_size=random.randint(10, 30)
        )
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/executions/{execution_id}/tasks", params=params, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                # Validate response has expected structure
                if 'data' in data and 'metadata' in data:
                    response.success()
                else:
                    response.failure("Response missing expected structure")
            else:
                response.failure(f"Failed to get task executions: {response.status_code}")
    
    @task(1)
    @tag('execution', 'cancel')
    def cancel_pipeline_execution(self):
        """Task to test POST /executions/{execution_id}/cancel endpoint"""
        # Skip if no execution IDs are available
        if not self.execution_ids:
            return
        
        # Select random execution ID
        execution_id = random.choice(self.execution_ids)
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/executions/{execution_id}/cancel", catch_response=True) as response:
            if response.status_code in (200, 202, 409):  # Accept conflict if already finished
                response.success()
            else:
                response.failure(f"Failed to cancel pipeline execution: {response.status_code}")
    
    @task(1)
    @tag('execution', 'retry')
    def retry_pipeline_execution(self):
        """Task to test POST /executions/{execution_id}/retry endpoint"""
        # Skip if no execution IDs are available
        if not self.execution_ids:
            return
        
        # Select random execution ID
        execution_id = random.choice(self.execution_ids)
        
        # Generate random execution parameters for retry
        retry_params = {
            'parameters': {
                'param1': generate_random_string(5),
                'param2': random.randint(1, 100),
                'retry_id': str(uuid.uuid4())
            }
        }
        
        # Make the request
        with self.client.post(f"{API_BASE_URL}/executions/{execution_id}/retry", json=retry_params, catch_response=True) as response:
            if response.status_code in (202, 409):  # Accept conflict if cannot retry
                if response.status_code == 202:
                    data = response.json()
                    # Store new execution_id if available
                    if 'data' in data and 'execution_id' in data['data']:
                        self.execution_ids.append(data['data']['execution_id'])
                        # Also update parent's execution_ids if available
                        if hasattr(self.parent, 'execution_ids'):
                            self.parent.execution_ids.append(data['data']['execution_id'])
                response.success()
            else:
                response.failure(f"Failed to retry pipeline execution: {response.status_code}")


class IngestionUser(User):
    """Locust user class that simulates a user interacting with ingestion API endpoints"""
    
    def __init__(self, environment):
        """Initialize the IngestionUser"""
        super().__init__(environment)
        self.source_ids = []
        self.pipeline_ids = []
        self.execution_ids = []
        self.auth_headers = {}
        
        # Set up task sets
        self.tasks = [SourceSystemTasks, PipelineTasks, PipelineExecutionTasks]
        
        # Define wait time between tasks (1-5 seconds)
        self.wait_time = between(1, 5)
    
    def on_start(self):
        """Setup method called when a simulated user starts"""
        # Authenticate with the API
        self.auth_headers = create_test_auth_headers(TEST_USER_CREDENTIALS)
        
        # Initialize common data structures for task sets
        self._pre_create_test_data()
    
    def on_stop(self):
        """Cleanup method called when a simulated user stops"""
        # Clean up any resources created during the test
        self._cleanup_test_data()
    
    def _pre_create_test_data(self):
        """Pre-create some test data for task sets to use"""
        # Create a few test source systems
        for _ in range(2):
            source_type = random.choice(SOURCE_TYPES)
            source_data = {
                'source_name': f"Test Source {generate_random_string(5)}",
                'source_type': source_type,
                'connection_details': {
                    'test_key': generate_random_string(10)
                }
            }
            
            response = self.client.post(
                f"{API_BASE_URL}/source-systems",
                json=source_data,
                headers=self.auth_headers
            )
            
            if response.status_code == 201:
                data = response.json()
                if 'data' in data and 'source_id' in data['data']:
                    self.source_ids.append(data['data']['source_id'])
        
        # Create a test pipeline for each source
        for source_id in self.source_ids:
            pipeline_data = {
                'pipeline_name': f"Test Pipeline {generate_random_string(5)}",
                'source_id': source_id,
                'target_dataset': f"test_dataset_{generate_random_string(5)}",
                'target_table': f"test_table_{generate_random_string(5)}",
                'schedule': "0 */6 * * *",  # Every 6 hours
                'enabled': True,
                'configuration': {
                    'parameters': {
                        'test_param': generate_random_string(5)
                    }
                }
            }
            
            response = self.client.post(
                f"{API_BASE_URL}/pipelines",
                json=pipeline_data,
                headers=self.auth_headers
            )
            
            if response.status_code == 201:
                data = response.json()
                if 'data' in data and 'pipeline_id' in data['data']:
                    self.pipeline_ids.append(data['data']['pipeline_id'])
    
    def _cleanup_test_data(self):
        """Clean up test data created by this user"""
        # Clean up pipeline executions
        for execution_id in self.execution_ids:
            try:
                self.client.delete(
                    f"{API_BASE_URL}/executions/{execution_id}",
                    headers=self.auth_headers
                )
            except:
                pass
        
        # Clean up pipelines
        for pipeline_id in self.pipeline_ids:
            try:
                self.client.delete(
                    f"{API_BASE_URL}/pipelines/{pipeline_id}",
                    headers=self.auth_headers
                )
            except:
                pass
        
        # Clean up source systems
        for source_id in self.source_ids:
            try:
                self.client.delete(
                    f"{API_BASE_URL}/source-systems/{source_id}",
                    headers=self.auth_headers
                )
            except:
                pass