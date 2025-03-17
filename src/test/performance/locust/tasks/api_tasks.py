"""
Defines Locust tasks for performance testing the general API endpoints of the self-healing data pipeline.

This module simulates user behavior for interacting with various API endpoints to measure
system performance under load conditions.
"""

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
from src.test.utils.test_data_generators import (
    generate_random_string,
    DataGenerator
)

# Global constants
API_BASE_URL = '/api'
TEST_USER_CREDENTIALS = {'username': 'test_user', 'password': 'test_password', 'roles': ['admin']}


class HealthCheckTasks(TaskSet):
    """TaskSet for testing health check and status API endpoints"""
    
    def __init__(self, parent):
        """Initialize the HealthCheckTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        # Set up authentication headers using the parent's auth headers
        self.client.headers.update(self.parent.auth_headers)
        
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for health checks
        self.test_data = {
            "health": {},
            "status": {},
            "version": {}
        }
    
    @task(10)
    @tag('health', 'read')
    def get_health(self):
        """Task to test GET /health endpoint"""
        with self.client.get(f"{API_BASE_URL}/health", 
                             name="Health Check", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'status' in data and data['status'] == 'healthy':
                        response.success()
                    else:
                        response.failure(f"Invalid health response: {data}")
                else:
                    response.failure(f"Health check failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Health check exception: {str(e)}")
    
    @task(5)
    @tag('status', 'read')
    def get_status(self):
        """Task to test GET /status endpoint"""
        with self.client.get(f"{API_BASE_URL}/status", 
                             name="System Status", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'status' in data and 'components' in data:
                        response.success()
                    else:
                        response.failure(f"Invalid status response: {data}")
                else:
                    response.failure(f"Status check failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Status check exception: {str(e)}")
    
    @task(2)
    @tag('version', 'read')
    def get_version(self):
        """Task to test GET /version endpoint"""
        with self.client.get(f"{API_BASE_URL}/version", 
                             name="Version Info", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'version' in data:
                        response.success()
                    else:
                        response.failure(f"Invalid version response: {data}")
                else:
                    response.failure(f"Version check failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Version check exception: {str(e)}")


class AuthenticationTasks(TaskSet):
    """TaskSet for testing authentication API endpoints"""
    
    def __init__(self, parent):
        """Initialize the AuthenticationTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.access_token = None
        
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for authentication
        self.test_data = {
            "login": {
                "username": f"user_{generate_random_string(8)}",
                "password": f"pass_{generate_random_string(10)}"
            }
        }
    
    @task(3)
    @tag('auth', 'login')
    def login(self):
        """Task to test POST /auth/login endpoint"""
        # Generate random user credentials
        credentials = {
            "username": f"user_{random.randint(1000, 9999)}",
            "password": f"password_{random.randint(1000, 9999)}"
        }
        
        with self.client.post(f"{API_BASE_URL}/auth/login", 
                              json=credentials,
                              name="User Login", 
                              catch_response=True) as response:
            try:
                if response.status_code in [200, 201]:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'access_token' in data and 'refresh_token' in data:
                        self.access_token = data['access_token']
                        response.success()
                    else:
                        response.failure(f"Invalid login response: {data}")
                else:
                    # For performance testing, we might still consider this a success
                    # if the status code indicates credential failure but the API is working
                    if response.status_code == 401:
                        response.success() 
                    else:
                        response.failure(f"Login failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Login exception: {str(e)}")
    
    @task(2)
    @tag('auth', 'refresh')
    def refresh_token(self):
        """Task to test POST /auth/refresh endpoint"""
        # Skip if no access_token available
        if not self.access_token:
            return
        
        refresh_payload = {
            "refresh_token": "dummy_refresh_token"  # In a real test, we'd use the actual token
        }
        
        with self.client.post(f"{API_BASE_URL}/auth/refresh", 
                              json=refresh_payload,
                              name="Refresh Token", 
                              catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'access_token' in data:
                        self.access_token = data['access_token']
                        response.success()
                    else:
                        response.failure(f"Invalid refresh response: {data}")
                else:
                    # For testing purposes, we might still consider certain failures as "successful tests"
                    if response.status_code in [401, 403]:
                        response.success()
                    else:
                        response.failure(f"Token refresh failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Token refresh exception: {str(e)}")
    
    @task(1)
    @tag('auth', 'logout')
    def logout(self):
        """Task to test POST /auth/logout endpoint"""
        # Skip if no access_token available
        if not self.access_token:
            return
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        with self.client.post(f"{API_BASE_URL}/auth/logout", 
                              headers=headers,
                              name="User Logout", 
                              catch_response=True) as response:
            try:
                if response.status_code == 200:
                    self.access_token = None
                    response.success()
                else:
                    # For testing purposes, we might still consider certain failures as "successful tests"
                    if response.status_code in [401, 403]:
                        self.access_token = None
                        response.success()
                    else:
                        response.failure(f"Logout failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Logout exception: {str(e)}")


class UserManagementTasks(TaskSet):
    """TaskSet for testing user management API endpoints"""
    
    def __init__(self, parent):
        """Initialize the UserManagementTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.user_ids = []
        # Set up authentication headers using the parent's auth headers
        self.client.headers.update(self.parent.auth_headers)
        
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for user management
        try:
            # Load sample user data
            self.test_data = load_api_test_data("users.json")
        except Exception:
            # Fallback to generated data if file not found
            self.test_data = {
                "users": [
                    {
                        "username": f"user_{generate_random_string(6)}",
                        "email": f"user_{generate_random_string(6)}@example.com",
                        "role": random.choice(["admin", "user", "viewer"]),
                        "active": random.choice([True, False])
                    }
                    for _ in range(5)
                ]
            }
    
    @task(5)
    @tag('users', 'read')
    def get_users(self):
        """Task to test GET /users endpoint with pagination"""
        # Generate random pagination parameters
        page = random.randint(1, 5)
        page_size = random.choice([10, 20, 50])
        params = create_pagination_params(page, page_size)
        
        with self.client.get(f"{API_BASE_URL}/users", 
                             params=params,
                             name="Get Users", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data and 'metadata' in data:
                        # Store user_ids from response for other tasks
                        if 'data' in data and isinstance(data['data'], list):
                            user_ids = [user.get('id') for user in data['data'] if 'id' in user]
                            if user_ids:
                                self.user_ids = user_ids
                        response.success()
                    else:
                        response.failure(f"Invalid users response format: {data}")
                else:
                    response.failure(f"Get users failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Get users exception: {str(e)}")
    
    @task(3)
    @tag('users', 'read')
    def get_user_by_id(self):
        """Task to test GET /users/{user_id} endpoint"""
        # Select a random user_id from available users
        if not self.user_ids:
            return
            
        user_id = random.choice(self.user_ids)
        
        with self.client.get(f"{API_BASE_URL}/users/{user_id}", 
                             name="Get User by ID", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data and 'id' in data['data']:
                        response.success()
                    else:
                        response.failure(f"Invalid user response format: {data}")
                else:
                    # 404 can be expected if a user has been deleted
                    if response.status_code == 404:
                        # Remove this ID from our list
                        if user_id in self.user_ids:
                            self.user_ids.remove(user_id)
                        response.success()
                    else:
                        response.failure(f"Get user failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Get user exception: {str(e)}")
    
    @task(2)
    @tag('users', 'create')
    def create_user(self):
        """Task to test POST /users endpoint"""
        # Generate random user data
        user_data = {
            "username": f"user_{generate_random_string(8)}",
            "email": f"email_{generate_random_string(8)}@example.com",
            "password": f"pass_{generate_random_string(10)}",
            "first_name": f"First_{generate_random_string(6)}",
            "last_name": f"Last_{generate_random_string(6)}",
            "role": random.choice(["admin", "user", "viewer"]),
            "active": True
        }
        
        with self.client.post(f"{API_BASE_URL}/users", 
                              json=user_data,
                              name="Create User", 
                              catch_response=True) as response:
            try:
                if response.status_code in [200, 201]:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data and 'id' in data['data']:
                        # Store created user_id for future requests
                        user_id = data['data']['id']
                        if user_id not in self.user_ids:
                            self.user_ids.append(user_id)
                        response.success()
                    else:
                        response.failure(f"Invalid create user response: {data}")
                else:
                    response.failure(f"Create user failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Create user exception: {str(e)}")
    
    @task(2)
    @tag('users', 'update')
    def update_user(self):
        """Task to test PUT /users/{user_id} endpoint"""
        # Select a random user_id from available users
        if not self.user_ids:
            return
            
        user_id = random.choice(self.user_ids)
        
        # Generate updated user data
        update_data = {
            "email": f"updated_{generate_random_string(8)}@example.com",
            "first_name": f"UpdatedFirst_{generate_random_string(6)}",
            "last_name": f"UpdatedLast_{generate_random_string(6)}",
            "role": random.choice(["admin", "user", "viewer"]),
            "active": random.choice([True, False])
        }
        
        with self.client.put(f"{API_BASE_URL}/users/{user_id}", 
                             json=update_data,
                             name="Update User", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data and 'id' in data['data']:
                        response.success()
                    else:
                        response.failure(f"Invalid update user response: {data}")
                else:
                    # 404 can be expected if a user has been deleted
                    if response.status_code == 404:
                        # Remove this ID from our list
                        if user_id in self.user_ids:
                            self.user_ids.remove(user_id)
                        response.success()
                    else:
                        response.failure(f"Update user failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Update user exception: {str(e)}")
    
    @task(1)
    @tag('users', 'delete')
    def delete_user(self):
        """Task to test DELETE /users/{user_id} endpoint"""
        # Select a random user_id from available users
        if not self.user_ids:
            return
            
        user_id = random.choice(self.user_ids)
        
        with self.client.delete(f"{API_BASE_URL}/users/{user_id}", 
                                name="Delete User", 
                                catch_response=True) as response:
            try:
                if response.status_code in [200, 204]:
                    # Remove deleted user_id from list
                    if user_id in self.user_ids:
                        self.user_ids.remove(user_id)
                    response.success()
                else:
                    # 404 can be expected if a user has already been deleted
                    if response.status_code == 404:
                        # Remove this ID from our list
                        if user_id in self.user_ids:
                            self.user_ids.remove(user_id)
                        response.success()
                    else:
                        response.failure(f"Delete user failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Delete user exception: {str(e)}")


class ConfigurationTasks(TaskSet):
    """TaskSet for testing configuration API endpoints"""
    
    def __init__(self, parent):
        """Initialize the ConfigurationTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        # Set up authentication headers using the parent's auth headers
        self.client.headers.update(self.parent.auth_headers)
        
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for configuration
        try:
            # Load sample configuration data
            self.test_data = load_api_test_data("config.json")
        except Exception:
            # Fallback to generated data if file not found
            self.test_data = {
                "system": {
                    "scheduler_enabled": True,
                    "default_timeout": 300,
                    "max_retries": 3,
                    "logging_level": "INFO",
                    "notification_channels": ["email", "teams"]
                },
                "features": {
                    "self_healing": True,
                    "anomaly_detection": True,
                    "auto_optimization": False,
                    "advanced_monitoring": True
                }
            }
    
    @task(5)
    @tag('config', 'system', 'read')
    def get_system_config(self):
        """Task to test GET /config/system endpoint"""
        with self.client.get(f"{API_BASE_URL}/config/system", 
                             name="Get System Config", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data:
                        # Store config data for update tasks
                        if 'data' in data and isinstance(data['data'], dict):
                            self.test_data["system"] = data['data']
                        response.success()
                    else:
                        response.failure(f"Invalid system config response: {data}")
                else:
                    response.failure(f"Get system config failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Get system config exception: {str(e)}")
    
    @task(1)
    @tag('config', 'system', 'update')
    def update_system_config(self):
        """Task to test PUT /config/system endpoint"""
        # Generate updated system config data
        if not self.test_data.get("system"):
            return
            
        updated_config = self.test_data["system"].copy()
        updated_config["logging_level"] = random.choice(["DEBUG", "INFO", "WARNING", "ERROR"])
        updated_config["max_retries"] = random.randint(1, 5)
        
        with self.client.put(f"{API_BASE_URL}/config/system", 
                             json=updated_config,
                             name="Update System Config", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data:
                        # Update our stored config
                        self.test_data["system"] = data['data']
                        response.success()
                    else:
                        response.failure(f"Invalid update system config response: {data}")
                else:
                    response.failure(f"Update system config failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Update system config exception: {str(e)}")
    
    @task(3)
    @tag('config', 'features', 'read')
    def get_feature_flags(self):
        """Task to test GET /config/features endpoint"""
        with self.client.get(f"{API_BASE_URL}/config/features", 
                             name="Get Feature Flags", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data:
                        # Store feature flags for update tasks
                        if 'data' in data and isinstance(data['data'], dict):
                            self.test_data["features"] = data['data']
                        response.success()
                    else:
                        response.failure(f"Invalid feature flags response: {data}")
                else:
                    response.failure(f"Get feature flags failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Get feature flags exception: {str(e)}")
    
    @task(1)
    @tag('config', 'features', 'update')
    def update_feature_flags(self):
        """Task to test PUT /config/features endpoint"""
        # Generate updated feature flags data
        if not self.test_data.get("features"):
            return
            
        updated_features = self.test_data["features"].copy()
        # Toggle a random feature flag
        if updated_features:
            feature_key = random.choice(list(updated_features.keys()))
            updated_features[feature_key] = not updated_features[feature_key]
        
        with self.client.put(f"{API_BASE_URL}/config/features", 
                             json=updated_features,
                             name="Update Feature Flags", 
                             catch_response=True) as response:
            try:
                if response.status_code == 200:
                    data = response.json()
                    # Check that the response contains the expected fields
                    if 'data' in data:
                        # Update our stored feature flags
                        self.test_data["features"] = data['data']
                        response.success()
                    else:
                        response.failure(f"Invalid update feature flags response: {data}")
                else:
                    response.failure(f"Update feature flags failed with status code: {response.status_code}")
            except Exception as e:
                response.failure(f"Update feature flags exception: {str(e)}")


class ApiUser(HttpUser):
    """Locust user class that simulates a user interacting with general API endpoints"""
    
    def __init__(self, environment):
        """Initialize the ApiUser"""
        super().__init__(environment)
        self.auth_headers = {}
        self.user_ids = []
        
        # Set up authentication headers using test credentials
        self.auth_headers = create_test_auth_headers(TEST_USER_CREDENTIALS)
        
        # Set task sets for this user (HealthCheckTasks, AuthenticationTasks, UserManagementTasks, ConfigurationTasks)
        self.tasks = {
            HealthCheckTasks: 2,
            AuthenticationTasks: 1,
            UserManagementTasks: 2,
            ConfigurationTasks: 1
        }
        
        # Set wait time between tasks (between 1 and 3 seconds)
        self.wait_time = between(1, 3)
    
    def on_start(self):
        """Setup method called when a simulated user starts"""
        # Authenticate with the API
        self.auth_headers = create_test_auth_headers(TEST_USER_CREDENTIALS)
        
        # Initialize shared data structures for task sets
        self.user_ids = []
    
    def on_stop(self):
        """Cleanup method called when a simulated user stops"""
        # Clean up any resources created during the test
        
        # Log user-specific metrics
        self.environment.events.request.fire(
            request_type="SUMMARY",
            name="User Session Ended",
            response_time=0,
            response_length=0,
            exception=None,
            context={
                "user_id": id(self),
                "tasks_executed": getattr(self, "_task_count", 0)
            }
        )