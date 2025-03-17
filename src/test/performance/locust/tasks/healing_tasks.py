import random
import json
import uuid
import time
import datetime
from locust import HttpUser, TaskSet, task, tag, constant, between

from src.test.utils.api_test_utils import (
    create_test_auth_headers,
    load_api_test_data, 
    create_pagination_params,
    create_date_range_params
)
from src.test.utils.test_helpers import (
    generate_unique_id,
    create_test_healing_action
)

# API base URL
API_BASE_URL = '/api/healing'

# Test user credentials
TEST_USER_CREDENTIALS = {
    'username': 'test_user',
    'password': 'test_password',
    'roles': ['healing:read', 'healing:create', 'healing:update', 'healing:delete', 'healing:execute']
}

class HealingPatternTasks(TaskSet):
    """TaskSet for testing healing pattern API endpoints"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.test_data = {}
        self.pattern_ids = []
        self.auth_headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Load test data
        self.test_data = load_api_test_data('healing_patterns.json')
        
    @task(5)
    @tag('healing', 'patterns', 'read')
    def get_patterns(self):
        """Task to test GET /patterns endpoint with pagination"""
        # Create pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 3),
            page_size=random.choice([10, 20, 50])
        )
        
        # Randomly add issue_type filter
        if random.random() < 0.3:
            params['issue_type'] = random.choice(['data_quality', 'performance', 'system_error'])
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/patterns",
            params=params,
            headers=self.auth_headers,
            name="GET /healing/patterns",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Store pattern_ids for other tasks
                    if 'data' in data and 'items' in data['data']:
                        self.pattern_ids = [item['pattern_id'] for item in data['data']['items']]
                    response.success()
                else:
                    response.failure(f"Failed to get patterns: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(3)
    @tag('healing', 'patterns', 'read')
    def get_pattern_by_id(self):
        """Task to test GET /patterns/{pattern_id} endpoint"""
        if not self.pattern_ids:
            return
        
        # Select a random pattern_id
        pattern_id = random.choice(self.pattern_ids)
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/patterns/{pattern_id}",
            headers=self.auth_headers,
            name="GET /healing/patterns/{pattern_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to get pattern: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'patterns', 'create')
    def create_pattern(self):
        """Task to test POST /patterns endpoint"""
        # Generate random pattern data
        pattern_data = {
            'issue_type': random.choice(['data_quality', 'performance', 'system_error']),
            'detection_pattern': {
                'error_message': f"Test error pattern {generate_unique_id()}",
                'metadata': {
                    'source': random.choice(['quality_validation', 'pipeline_execution', 'system_metrics']),
                    'severity': random.choice(['high', 'medium', 'low'])
                }
            },
            'confidence_threshold': round(random.uniform(0.7, 0.95), 2),
            'description': f"Test pattern created by load test at {time.time()}"
        }
        
        # Make request
        with self.client.post(
            f"{API_BASE_URL}/patterns",
            json=pattern_data,
            headers=self.auth_headers,
            name="POST /healing/patterns",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 201:
                    data = response.json()
                    # Store pattern_id for future requests
                    if 'data' in data and 'pattern_id' in data['data']:
                        self.pattern_ids.append(data['data']['pattern_id'])
                    response.success()
                else:
                    response.failure(f"Failed to create pattern: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'patterns', 'update')
    def update_pattern(self):
        """Task to test PUT /patterns/{pattern_id} endpoint"""
        if not self.pattern_ids:
            return
        
        # Select a random pattern_id
        pattern_id = random.choice(self.pattern_ids)
        
        # Generate updated pattern data
        updated_data = {
            'confidence_threshold': round(random.uniform(0.7, 0.95), 2),
            'description': f"Updated by load test at {time.time()}"
        }
        
        # Make request
        with self.client.put(
            f"{API_BASE_URL}/patterns/{pattern_id}",
            json=updated_data,
            headers=self.auth_headers,
            name="PUT /healing/patterns/{pattern_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to update pattern: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'patterns', 'delete')
    def delete_pattern(self):
        """Task to test DELETE /patterns/{pattern_id} endpoint"""
        if not self.pattern_ids:
            return
        
        # Select a random pattern_id
        pattern_id = random.choice(self.pattern_ids)
        
        # Make request
        with self.client.delete(
            f"{API_BASE_URL}/patterns/{pattern_id}",
            headers=self.auth_headers,
            name="DELETE /healing/patterns/{pattern_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 204:
                    # Remove from list if successful
                    if pattern_id in self.pattern_ids:
                        self.pattern_ids.remove(pattern_id)
                    response.success()
                else:
                    response.failure(f"Failed to delete pattern: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")


class HealingActionTasks(TaskSet):
    """TaskSet for testing healing action API endpoints"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.test_data = {}
        self.action_ids = []
        self.pattern_ids = []
        self.auth_headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Load test data
        self.test_data = load_api_test_data('healing_actions.json')
        
        # Get pattern_ids from parent if available
        if hasattr(self.parent, 'pattern_ids') and self.parent.pattern_ids:
            self.pattern_ids = self.parent.pattern_ids
    
    @task(5)
    @tag('healing', 'actions', 'read')
    def get_actions(self):
        """Task to test GET /actions endpoint with pagination and filtering"""
        # Create pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 3),
            page_size=random.choice([10, 20, 50])
        )
        
        # Randomly add filters
        if self.pattern_ids and random.random() < 0.3:
            params['pattern_id'] = random.choice(self.pattern_ids)
        
        if random.random() < 0.3:
            params['action_type'] = random.choice(['data_correction', 'pipeline_retry', 'resource_adjustment', 'schema_fix'])
        
        if random.random() < 0.2:
            params['active_only'] = 'true'
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/actions",
            params=params,
            headers=self.auth_headers,
            name="GET /healing/actions",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Store action_ids for other tasks
                    if 'data' in data and 'items' in data['data']:
                        self.action_ids = [item['action_id'] for item in data['data']['items']]
                    response.success()
                else:
                    response.failure(f"Failed to get actions: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(3)
    @tag('healing', 'actions', 'read')
    def get_action_by_id(self):
        """Task to test GET /actions/{action_id} endpoint"""
        if not self.action_ids:
            return
        
        # Select a random action_id
        action_id = random.choice(self.action_ids)
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/actions/{action_id}",
            headers=self.auth_headers,
            name="GET /healing/actions/{action_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to get action: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'actions', 'create')
    def create_action(self):
        """Task to test POST /actions endpoint"""
        if not self.pattern_ids:
            # Create a pattern_id if none available
            pattern_id = f"pattern-{generate_unique_id()}"
        else:
            pattern_id = random.choice(self.pattern_ids)
        
        # Generate random action data
        action_data = {
            'pattern_id': pattern_id,
            'action_type': random.choice(['data_correction', 'pipeline_retry', 'resource_adjustment', 'schema_fix']),
            'parameters': {
                'param1': f"value-{generate_unique_id()}",
                'param2': random.randint(1, 100)
            },
            'description': f"Test action created by load test at {time.time()}"
        }
        
        # Make request
        with self.client.post(
            f"{API_BASE_URL}/actions",
            json=action_data,
            headers=self.auth_headers,
            name="POST /healing/actions",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 201:
                    data = response.json()
                    # Store action_id for future requests
                    if 'data' in data and 'action_id' in data['data']:
                        self.action_ids.append(data['data']['action_id'])
                    response.success()
                else:
                    response.failure(f"Failed to create action: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'actions', 'update')
    def update_action(self):
        """Task to test PUT /actions/{action_id} endpoint"""
        if not self.action_ids:
            return
        
        # Select a random action_id
        action_id = random.choice(self.action_ids)
        
        # Generate updated action data
        updated_data = {
            'parameters': {
                'param1': f"updated-value-{generate_unique_id()}",
                'param2': random.randint(1, 100)
            },
            'description': f"Updated by load test at {time.time()}"
        }
        
        # Make request
        with self.client.put(
            f"{API_BASE_URL}/actions/{action_id}",
            json=updated_data,
            headers=self.auth_headers,
            name="PUT /healing/actions/{action_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to update action: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'actions', 'delete')
    def delete_action(self):
        """Task to test DELETE /actions/{action_id} endpoint"""
        if not self.action_ids:
            return
        
        # Select a random action_id
        action_id = random.choice(self.action_ids)
        
        # Make request
        with self.client.delete(
            f"{API_BASE_URL}/actions/{action_id}",
            headers=self.auth_headers,
            name="DELETE /healing/actions/{action_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 204:
                    # Remove from list if successful
                    if action_id in self.action_ids:
                        self.action_ids.remove(action_id)
                    response.success()
                else:
                    response.failure(f"Failed to delete action: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")


class HealingExecutionTasks(TaskSet):
    """TaskSet for testing healing execution API endpoints"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.test_data = {}
        self.execution_ids = []
        self.pattern_ids = []
        self.action_ids = []
        self.auth_headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Load test data
        self.test_data = load_api_test_data('healing_executions.json')
        
        # Get pattern_ids and action_ids from parent if available
        if hasattr(self.parent, 'pattern_ids') and self.parent.pattern_ids:
            self.pattern_ids = self.parent.pattern_ids
        
        if hasattr(self.parent, 'action_ids') and self.parent.action_ids:
            self.action_ids = self.parent.action_ids
    
    @task(5)
    @tag('healing', 'executions', 'read')
    def get_executions(self):
        """Task to test GET /executions endpoint with pagination and filtering"""
        # Create pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 3),
            page_size=random.choice([10, 20, 50])
        )
        
        # Add date range filter (last 7 days)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=7)
        date_params = create_date_range_params(start_date, end_date)
        params.update(date_params)
        
        # Randomly add other filters
        if self.execution_ids and random.random() < 0.2:
            params['execution_id'] = random.choice(self.execution_ids)
        
        if self.pattern_ids and random.random() < 0.2:
            params['pattern_id'] = random.choice(self.pattern_ids)
        
        if self.action_ids and random.random() < 0.2:
            params['action_id'] = random.choice(self.action_ids)
        
        if random.random() < 0.3:
            params['successful_only'] = 'true' if random.random() < 0.7 else 'false'
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/executions",
            params=params,
            headers=self.auth_headers,
            name="GET /healing/executions",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Store execution_ids for other tasks
                    if 'data' in data and 'items' in data['data']:
                        self.execution_ids = [item['healing_id'] for item in data['data']['items']]
                    response.success()
                else:
                    response.failure(f"Failed to get executions: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(3)
    @tag('healing', 'executions', 'read')
    def get_execution_by_id(self):
        """Task to test GET /executions/{healing_id} endpoint"""
        if not self.execution_ids:
            return
        
        # Select a random healing_id
        healing_id = random.choice(self.execution_ids)
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/executions/{healing_id}",
            headers=self.auth_headers,
            name="GET /healing/executions/{healing_id}",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to get execution: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'execute')
    def execute_manual_healing(self):
        """Task to test POST /execute endpoint for manual healing"""
        if not self.action_ids:
            return
        
        # Select a random action_id
        action_id = random.choice(self.action_ids)
        
        # Generate healing request data
        healing_data = {
            'action_id': action_id,
            'issue_details': {
                'issue_type': random.choice(['data_quality', 'performance', 'system_error']),
                'description': f"Test issue from load test at {time.time()}",
                'metadata': {
                    'source': random.choice(['manual', 'automated']),
                    'severity': random.choice(['high', 'medium', 'low'])
                }
            }
        }
        
        # Make request
        with self.client.post(
            f"{API_BASE_URL}/execute",
            json=healing_data,
            headers=self.auth_headers,
            name="POST /healing/execute",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 202:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to execute healing: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")


class HealingConfigTasks(TaskSet):
    """TaskSet for testing healing configuration API endpoints"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.test_data = {}
        self.auth_headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Load test data
        self.test_data = load_api_test_data('healing_config.json')
    
    @task(5)
    @tag('healing', 'config', 'read')
    def get_config(self):
        """Task to test GET /config endpoint"""
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/config",
            headers=self.auth_headers,
            name="GET /healing/config",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Store config data for update tasks
                    if 'data' in data:
                        self.test_data['config'] = data['data']
                    response.success()
                else:
                    response.failure(f"Failed to get config: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(1)
    @tag('healing', 'config', 'update')
    def update_config(self):
        """Task to test PUT /config endpoint"""
        # Generate updated config data
        config_data = {
            'autonomous_mode': random.choice(['fully_automatic', 'semi_automatic', 'approval_required', 'manual']),
            'confidence_threshold': round(random.uniform(0.7, 0.95), 2),
            'max_retry_attempts': random.randint(1, 5),
            'learning_mode': random.choice(['active', 'passive', 'disabled'])
        }
        
        # Make request
        with self.client.put(
            f"{API_BASE_URL}/config",
            json=config_data,
            headers=self.auth_headers,
            name="PUT /healing/config",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to update config: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")
    
    @task(3)
    @tag('healing', 'statistics', 'read')
    def get_statistics(self):
        """Task to test GET /statistics endpoint"""
        # Generate date range (last 30 days)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=30)
        params = create_date_range_params(start_date, end_date)
        
        # Make request
        with self.client.get(
            f"{API_BASE_URL}/statistics",
            params=params,
            headers=self.auth_headers,
            name="GET /healing/statistics",
            catch_response=True
        ) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    response.success()
                else:
                    response.failure(f"Failed to get statistics: {response.status_code}")
            except Exception as e:
                response.failure(f"Error processing response: {str(e)}")


class HealingUser(HttpUser):
    """Locust user class that simulates a user interacting with healing API endpoints"""
    
    def __init__(self, environment):
        super().__init__(environment)
        self.pattern_ids = []
        self.action_ids = []
        self.execution_ids = []
        self.auth_headers = {}
        
        # Set task sets for this user
        self.tasks = [
            HealingPatternTasks,
            HealingActionTasks,
            HealingExecutionTasks,
            HealingConfigTasks
        ]
        
        # Set wait time between tasks
        self.wait_time = between(1, 5)
    
    def on_start(self):
        """Setup method called when a simulated user starts"""
        # Authenticate and set up headers
        self.auth_headers = create_test_auth_headers(TEST_USER_CREDENTIALS)
    
    def on_stop(self):
        """Cleanup method called when a simulated user stops"""
        # Clean up any resources created during the test
        pass