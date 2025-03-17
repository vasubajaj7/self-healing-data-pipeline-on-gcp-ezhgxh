"""
Defines Locust tasks for performance testing the data quality validation components of the self-healing data pipeline.
This module simulates user behavior for interacting with quality-related API endpoints to measure system
performance under load.
"""

import random
import json
import uuid
import time
import datetime
from locust import User, TaskSet, task, tag, constant, between

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

# Base URL for quality API endpoints
API_BASE_URL = '/api/quality'

# Test data for quality-related endpoints
TEST_DATASETS = ['customer_data', 'sales_metrics', 'product_catalog', 'inventory']
TEST_TABLES = {
    'customer_data': ['customers', 'addresses'],
    'sales_metrics': ['daily_sales', 'monthly_sales'],
    'product_catalog': ['products', 'categories'],
    'inventory': ['stock_levels', 'warehouses']
}


class QualityRuleTasks(TaskSet):
    """TaskSet for testing quality rule API endpoints"""
    
    def __init__(self, parent):
        """Initialize the QualityRuleTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.rule_ids = []
        self.headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for quality rules
        self.test_data = load_api_test_data('quality_rules.json')
        
        # Start with some pre-existing rule IDs if available
        if hasattr(self.parent, 'rule_ids') and self.parent.rule_ids:
            self.rule_ids = self.parent.rule_ids
    
    @task(5)
    @tag('quality', 'rules', 'read')
    def get_rules(self):
        """Task to test GET /rules endpoint with pagination and filtering"""
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Add random filter parameters
        if random.random() > 0.5:
            params['target_dataset'] = random.choice(TEST_DATASETS)
        
        if random.random() > 0.5:
            dataset = params.get('target_dataset', random.choice(TEST_DATASETS))
            params['target_table'] = random.choice(TEST_TABLES[dataset])
        
        if random.random() > 0.5:
            params['rule_type'] = random.choice(['schema', 'null_check', 'uniqueness', 'referential', 'custom'])
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/rules", params=params, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Store rule IDs for future requests
                    rules = data.get('data', [])
                    for rule in rules:
                        if rule.get('rule_id') and rule.get('rule_id') not in self.rule_ids:
                            self.rule_ids.append(rule.get('rule_id'))
                    
                    # Share rule IDs with parent user for other task sets
                    self.parent.rule_ids = self.rule_ids
                    response.success()
                else:
                    response.failure(f"Failed to get rules: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get rules: HTTP {response.status_code}")
    
    @task(3)
    @tag('quality', 'rules', 'read')
    def get_rule_by_id(self):
        """Task to test GET /rules/{rule_id} endpoint"""
        if not self.rule_ids:
            return
        
        rule_id = random.choice(self.rule_ids)
        
        with self.client.get(f"{API_BASE_URL}/rules/{rule_id}", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to get rule {rule_id}: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get rule {rule_id}: HTTP {response.status_code}")
    
    @task(2)
    @tag('quality', 'rules', 'create')
    def create_rule(self):
        """Task to test POST /rules endpoint"""
        # Generate random rule data
        rule_data = {
            'rule_name': f"Test Rule {generate_random_string(6)}",
            'rule_type': random.choice(['schema', 'null_check', 'uniqueness', 'referential', 'custom']),
            'target_dataset': random.choice(TEST_DATASETS),
            'dimension': random.choice(['completeness', 'accuracy', 'consistency', 'validity', 'timeliness']),
            'severity': random.choice(['critical', 'high', 'medium', 'low']),
            'enabled': random.choice([True, False])
        }
        
        # Add target table based on selected dataset
        rule_data['target_table'] = random.choice(TEST_TABLES[rule_data['target_dataset']])
        
        # Add rule-specific details
        if rule_data['rule_type'] == 'schema':
            rule_data['rule_config'] = {
                'schema_checks': ['field_types', 'required_fields']
            }
        elif rule_data['rule_type'] == 'null_check':
            rule_data['rule_config'] = {
                'columns': [f"column_{random.randint(1,5)}" for _ in range(random.randint(1,3))],
                'nulls_allowed': False
            }
        elif rule_data['rule_type'] == 'uniqueness':
            rule_data['rule_config'] = {
                'columns': [f"column_{random.randint(1,5)}" for _ in range(random.randint(1,3))],
                'scope': 'table'
            }
        
        with self.client.post(f"{API_BASE_URL}/rules", json=rule_data, headers=self.headers, catch_response=True) as response:
            if response.status_code == 201:
                data = response.json()
                if data.get('status') == 'success':
                    # Store the new rule ID
                    rule_id = data.get('data', {}).get('rule_id')
                    if rule_id and rule_id not in self.rule_ids:
                        self.rule_ids.append(rule_id)
                        self.parent.rule_ids = self.rule_ids
                    response.success()
                else:
                    response.failure(f"Failed to create rule: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to create rule: HTTP {response.status_code}")
    
    @task(2)
    @tag('quality', 'rules', 'update')
    def update_rule(self):
        """Task to test PUT /rules/{rule_id} endpoint"""
        if not self.rule_ids:
            return
        
        rule_id = random.choice(self.rule_ids)
        
        # Create update data
        update_data = {
            'rule_name': f"Updated Rule {generate_random_string(6)}",
            'severity': random.choice(['critical', 'high', 'medium', 'low']),
            'enabled': random.choice([True, False])
        }
        
        with self.client.put(f"{API_BASE_URL}/rules/{rule_id}", json=update_data, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to update rule {rule_id}: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to update rule {rule_id}: HTTP {response.status_code}")
    
    @task(1)
    @tag('quality', 'rules', 'delete')
    def delete_rule(self):
        """Task to test DELETE /rules/{rule_id} endpoint"""
        if not self.rule_ids:
            return
        
        rule_id = random.choice(self.rule_ids)
        
        with self.client.delete(f"{API_BASE_URL}/rules/{rule_id}", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Remove the rule ID from the list
                    if rule_id in self.rule_ids:
                        self.rule_ids.remove(rule_id)
                        self.parent.rule_ids = self.rule_ids
                    response.success()
                else:
                    response.failure(f"Failed to delete rule {rule_id}: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to delete rule {rule_id}: HTTP {response.status_code}")


class ValidationTasks(TaskSet):
    """TaskSet for testing validation API endpoints"""
    
    def __init__(self, parent):
        """Initialize the ValidationTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.validation_ids = []
        self.headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for validations
        self.test_data = load_api_test_data('quality_validations.json')
        
        # Start with some pre-existing validation IDs if available
        if hasattr(self.parent, 'validation_ids') and self.parent.validation_ids:
            self.validation_ids = self.parent.validation_ids
    
    @task(5)
    @tag('quality', 'validations', 'read')
    def get_validations(self):
        """Task to test GET /validations endpoint with pagination and filtering"""
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Add random date range parameters
        if random.random() > 0.5:
            now = datetime.datetime.now()
            start_date = now - datetime.timedelta(days=random.randint(1, 30))
            end_date = now
            
            date_params = create_date_range_params(start_date, end_date)
            params.update(date_params)
        
        # Add other filter parameters
        if random.random() > 0.5 and hasattr(self.parent, 'rule_ids') and self.parent.rule_ids:
            params['rule_id'] = random.choice(self.parent.rule_ids)
        
        if random.random() > 0.5:
            params['status'] = random.choice(['passed', 'failed', 'in_progress'])
        
        # Make the request
        with self.client.get(f"{API_BASE_URL}/validations", params=params, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    # Store validation IDs for future requests
                    validations = data.get('data', [])
                    for validation in validations:
                        if validation.get('validation_id') and validation.get('validation_id') not in self.validation_ids:
                            self.validation_ids.append(validation.get('validation_id'))
                    
                    # Share validation IDs with parent user
                    self.parent.validation_ids = self.validation_ids
                    response.success()
                else:
                    response.failure(f"Failed to get validations: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get validations: HTTP {response.status_code}")
    
    @task(3)
    @tag('quality', 'validations', 'read')
    def get_validation_by_id(self):
        """Task to test GET /validations/{validation_id} endpoint"""
        if not self.validation_ids:
            return
        
        validation_id = random.choice(self.validation_ids)
        
        with self.client.get(f"{API_BASE_URL}/validations/{validation_id}", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to get validation {validation_id}: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get validation {validation_id}: HTTP {response.status_code}")
    
    @task(3)
    @tag('quality', 'validations', 'execute')
    def execute_validation(self):
        """Task to test POST /validate endpoint"""
        # Select a random dataset and table for validation
        dataset = random.choice(TEST_DATASETS)
        table = random.choice(TEST_TABLES[dataset])
        
        # Generate a random execution ID
        execution_id = f"test-exec-{uuid.uuid4()}"
        
        # Create validation request
        validation_request = {
            'target_dataset': dataset,
            'target_table': table,
            'execution_id': execution_id
        }
        
        # Optionally include specific rule IDs
        if random.random() > 0.7 and hasattr(self.parent, 'rule_ids') and self.parent.rule_ids:
            # Select a random subset of rule IDs
            num_rules = min(random.randint(1, 3), len(self.parent.rule_ids))
            validation_request['rule_ids'] = random.sample(self.parent.rule_ids, num_rules)
        
        with self.client.post(f"{API_BASE_URL}/validate", json=validation_request, headers=self.headers, catch_response=True) as response:
            if response.status_code == 202:
                data = response.json()
                if data.get('status') == 'success':
                    # Store the validation ID
                    validation_id = data.get('data', {}).get('validation_id')
                    if validation_id and validation_id not in self.validation_ids:
                        self.validation_ids.append(validation_id)
                        self.parent.validation_ids = self.validation_ids
                    response.success()
                else:
                    response.failure(f"Failed to execute validation: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to execute validation: HTTP {response.status_code}")


class QualityScoreTasks(TaskSet):
    """TaskSet for testing quality score API endpoints"""
    
    def __init__(self, parent):
        """Initialize the QualityScoreTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for quality scores
        self.test_data = {}
    
    @task(5)
    @tag('quality', 'score', 'read')
    def get_quality_score(self):
        """Task to test GET /score endpoint"""
        # Select a random dataset and table
        dataset = random.choice(TEST_DATASETS)
        table = random.choice(TEST_TABLES[dataset])
        
        # Create parameters
        params = {
            'dataset': dataset,
            'table': table
        }
        
        # Optionally add an as_of_date parameter
        if random.random() > 0.5:
            now = datetime.datetime.now()
            days_ago = random.randint(0, 30)
            as_of_date = now - datetime.timedelta(days=days_ago)
            params['as_of_date'] = as_of_date.strftime('%Y-%m-%d')
        
        with self.client.get(f"{API_BASE_URL}/score", params=params, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to get quality score: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get quality score: HTTP {response.status_code}")
    
    @task(3)
    @tag('quality', 'trend', 'read')
    def get_validation_trend(self):
        """Task to test GET /trend endpoint"""
        # Select a random dataset and table
        dataset = random.choice(TEST_DATASETS)
        table = random.choice(TEST_TABLES[dataset])
        
        # Create parameters
        params = {
            'dataset': dataset,
            'table': table,
            'interval': random.choice(['daily', 'weekly', 'monthly']),
            'num_intervals': random.randint(5, 20)
        }
        
        with self.client.get(f"{API_BASE_URL}/trend", params=params, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to get validation trend: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get validation trend: HTTP {response.status_code}")


class QualityIssueTasks(TaskSet):
    """TaskSet for testing quality issues API endpoints"""
    
    def __init__(self, parent):
        """Initialize the QualityIssueTasks TaskSet"""
        super().__init__(parent)
        self.test_data = {}
        self.headers = self.parent.auth_headers
    
    def on_start(self):
        """Setup method called when a simulated user starts executing this TaskSet"""
        # Initialize test data for quality issues
        self.test_data = {}
    
    @task(5)
    @tag('quality', 'issues', 'read')
    def get_quality_issues(self):
        """Task to test GET /issues endpoint with pagination and filtering"""
        # Generate random pagination parameters
        params = create_pagination_params(
            page=random.randint(1, 5),
            page_size=random.randint(10, 50)
        )
        
        # Add random dataset and table filters
        if random.random() > 0.5:
            dataset = random.choice(TEST_DATASETS)
            params['dataset'] = dataset
            
            if random.random() > 0.5:
                params['table'] = random.choice(TEST_TABLES[dataset])
        
        # Add severity filter
        if random.random() > 0.5:
            params['severity'] = random.choice(['critical', 'high', 'medium', 'low'])
        
        # Add resolution status filter
        if random.random() > 0.5:
            params['is_resolved'] = random.choice([True, False])
        
        # Add date range parameters
        if random.random() > 0.5:
            now = datetime.datetime.now()
            start_date = now - datetime.timedelta(days=random.randint(1, 30))
            end_date = now
            
            date_params = create_date_range_params(start_date, end_date)
            params.update(date_params)
        
        with self.client.get(f"{API_BASE_URL}/issues", params=params, headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    response.success()
                else:
                    response.failure(f"Failed to get quality issues: {data.get('error', {}).get('message', 'Unknown error')}")
            else:
                response.failure(f"Failed to get quality issues: HTTP {response.status_code}")


class QualityUser(User):
    """Locust user class that simulates a user interacting with quality API endpoints"""
    
    def __init__(self, environment):
        """Initialize the QualityUser"""
        super().__init__(environment)
        self.auth_headers = {}
        self.rule_ids = []
        self.validation_ids = []
        
        # Set task sets
        self.tasks = {
            QualityRuleTasks: 3,
            ValidationTasks: 3,
            QualityScoreTasks: 2,
            QualityIssueTasks: 2
        }
        
        # Set wait time between tasks
        self.wait_time = between(1, 5)
    
    def on_start(self):
        """Setup method called when a simulated user starts"""
        # Set up authentication headers
        self.auth_headers = create_test_auth_headers()
    
    def on_stop(self):
        """Cleanup method called when a simulated user stops"""
        # Clean up any resources created during the test
        pass