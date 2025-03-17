-- Create the test database if it doesn't exist
CREATE DATABASE IF NOT EXISTS test_pipeline;

-- Create a separate schema for test data
CREATE SCHEMA IF NOT EXISTS test_data;

-- Table: source_systems
CREATE TABLE IF NOT EXISTS source_systems (
    source_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    source_type VARCHAR(50) NOT NULL,
    connection_details JSONB NOT NULL,
    schema_definition JSONB,
    schema_version VARCHAR(20),
    extraction_settings JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    metadata JSONB,
    last_extraction_time TIMESTAMP
);

-- Indexes for source_systems
CREATE INDEX IF NOT EXISTS idx_source_systems_name ON source_systems(name);
CREATE INDEX IF NOT EXISTS idx_source_systems_source_type ON source_systems(source_type);
CREATE INDEX IF NOT EXISTS idx_source_systems_is_active ON source_systems(is_active);

-- Table: pipeline_definitions
CREATE TABLE IF NOT EXISTS pipeline_definitions (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    pipeline_type VARCHAR(50) NOT NULL,
    source_id VARCHAR(50) NOT NULL,
    target_dataset VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    transformation_config JSONB,
    quality_config JSONB,
    self_healing_config JSONB,
    scheduling_config JSONB,
    execution_config JSONB,
    performance_config JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    metadata JSONB,
    dag_id VARCHAR(100),
    quality_rule_ids JSONB,
    FOREIGN KEY (source_id) REFERENCES source_systems(source_id) ON DELETE CASCADE
);

-- Indexes for pipeline_definitions
CREATE INDEX IF NOT EXISTS idx_pipeline_definitions_name ON pipeline_definitions(name);
CREATE INDEX IF NOT EXISTS idx_pipeline_definitions_source_id ON pipeline_definitions(source_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_definitions_is_active ON pipeline_definitions(is_active);
CREATE INDEX IF NOT EXISTS idx_pipeline_definitions_dag_id ON pipeline_definitions(dag_id);

-- Table: quality_rules
CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    subtype VARCHAR(50),
    dimension VARCHAR(50),
    description TEXT,
    parameters JSONB NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version VARCHAR(20) DEFAULT '1.0',
    enabled BOOLEAN DEFAULT TRUE
);

-- Indexes for quality_rules
CREATE INDEX IF NOT EXISTS idx_quality_rules_name ON quality_rules(name);
CREATE INDEX IF NOT EXISTS idx_quality_rules_type ON quality_rules(rule_type);
CREATE INDEX IF NOT EXISTS idx_quality_rules_dimension ON quality_rules(dimension);
CREATE INDEX IF NOT EXISTS idx_quality_rules_enabled ON quality_rules(enabled);

-- Table: pipeline_executions
CREATE TABLE IF NOT EXISTS pipeline_executions (
    execution_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    dag_run_id VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    error_details JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES pipeline_definitions(pipeline_id) ON DELETE CASCADE
);

-- Indexes for pipeline_executions
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_pipeline_id ON pipeline_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON pipeline_executions(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_start_time ON pipeline_executions(start_time);

-- Table: task_executions
CREATE TABLE IF NOT EXISTS task_executions (
    task_execution_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    task_type VARCHAR(50) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    error_details JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE CASCADE
);

-- Indexes for task_executions
CREATE INDEX IF NOT EXISTS idx_task_executions_execution_id ON task_executions(execution_id);
CREATE INDEX IF NOT EXISTS idx_task_executions_task_id ON task_executions(task_id);
CREATE INDEX IF NOT EXISTS idx_task_executions_status ON task_executions(status);

-- Table: quality_validations
CREATE TABLE IF NOT EXISTS quality_validations (
    validation_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    rule_id VARCHAR(50) NOT NULL,
    validation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN NOT NULL,
    failure_details JSONB,
    metrics JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (rule_id) REFERENCES quality_rules(rule_id) ON DELETE CASCADE
);

-- Indexes for quality_validations
CREATE INDEX IF NOT EXISTS idx_quality_validations_execution_id ON quality_validations(execution_id);
CREATE INDEX IF NOT EXISTS idx_quality_validations_rule_id ON quality_validations(rule_id);
CREATE INDEX IF NOT EXISTS idx_quality_validations_passed ON quality_validations(passed);
CREATE INDEX IF NOT EXISTS idx_quality_validations_validation_time ON quality_validations(validation_time);

-- Table: issue_patterns
CREATE TABLE IF NOT EXISTS issue_patterns (
    pattern_id VARCHAR(50) PRIMARY KEY,
    issue_type VARCHAR(50) NOT NULL,
    detection_pattern JSONB NOT NULL,
    confidence_threshold FLOAT DEFAULT 0.8,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Indexes for issue_patterns
CREATE INDEX IF NOT EXISTS idx_issue_patterns_issue_type ON issue_patterns(issue_type);
CREATE INDEX IF NOT EXISTS idx_issue_patterns_is_active ON issue_patterns(is_active);

-- Table: healing_actions
CREATE TABLE IF NOT EXISTS healing_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    description TEXT,
    action_parameters JSONB NOT NULL,
    pattern_id VARCHAR(50) NOT NULL,
    execution_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    success_rate FLOAT DEFAULT 0.0,
    is_active BOOLEAN DEFAULT TRUE,
    last_executed TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pattern_id) REFERENCES issue_patterns(pattern_id) ON DELETE CASCADE
);

-- Indexes for healing_actions
CREATE INDEX IF NOT EXISTS idx_healing_actions_pattern_id ON healing_actions(pattern_id);
CREATE INDEX IF NOT EXISTS idx_healing_actions_action_type ON healing_actions(action_type);
CREATE INDEX IF NOT EXISTS idx_healing_actions_is_active ON healing_actions(is_active);
CREATE INDEX IF NOT EXISTS idx_healing_actions_success_rate ON healing_actions(success_rate);

-- Table: healing_executions
CREATE TABLE IF NOT EXISTS healing_executions (
    healing_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    validation_id VARCHAR(50),
    pattern_id VARCHAR(50) NOT NULL,
    action_id VARCHAR(50) NOT NULL,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    successful BOOLEAN NOT NULL,
    confidence_score FLOAT,
    execution_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (validation_id) REFERENCES quality_validations(validation_id) ON DELETE SET NULL,
    FOREIGN KEY (pattern_id) REFERENCES issue_patterns(pattern_id) ON DELETE CASCADE,
    FOREIGN KEY (action_id) REFERENCES healing_actions(action_id) ON DELETE CASCADE
);

-- Indexes for healing_executions
CREATE INDEX IF NOT EXISTS idx_healing_executions_execution_id ON healing_executions(execution_id);
CREATE INDEX IF NOT EXISTS idx_healing_executions_pattern_id ON healing_executions(pattern_id);
CREATE INDEX IF NOT EXISTS idx_healing_executions_action_id ON healing_executions(action_id);
CREATE INDEX IF NOT EXISTS idx_healing_executions_successful ON healing_executions(successful);

-- Table: pipeline_metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    metric_category VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value FLOAT NOT NULL,
    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE CASCADE
);

-- Indexes for pipeline_metrics
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_execution_id ON pipeline_metrics(execution_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_category ON pipeline_metrics(metric_category);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name ON pipeline_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_collection_time ON pipeline_metrics(collection_time);

-- Table: alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    metadata JSONB,
    FOREIGN KEY (execution_id) REFERENCES pipeline_executions(execution_id) ON DELETE SET NULL
);

-- Indexes for alerts
CREATE INDEX IF NOT EXISTS idx_alerts_execution_id ON alerts(execution_id);
CREATE INDEX IF NOT EXISTS idx_alerts_alert_type ON alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged ON alerts(acknowledged);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(resolved);

-- Sample data for source_systems
INSERT INTO source_systems (source_id, name, description, source_type, connection_details, schema_definition, schema_version, extraction_settings, is_active, created_by, metadata)
VALUES
    ('src_gcs_test_data', 'Test GCS Data Source', 'GCS bucket containing test data files', 'GCS', 
     '{"bucket_name": "test-data-bucket", "path_prefix": "test-data/"}',
     '{"fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED"}, {"name": "name", "type": "STRING", "mode": "REQUIRED"}, {"name": "value", "type": "FLOAT", "mode": "NULLABLE"}, {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}]}',
     '1.0',
     '{"file_format": "CSV", "delimiter": ",", "has_header": true}',
     TRUE, 'test_user', '{"environment": "test", "data_owner": "test_team"}'),
     
    ('src_cloudsql_test_data', 'Test Cloud SQL Source', 'Cloud SQL database with test data', 'CLOUD_SQL', 
     '{"instance": "test-instance", "database": "test_db", "table": "test_table"}',
     '{"fields": [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "product_name", "type": "STRING", "mode": "REQUIRED"}, {"name": "price", "type": "FLOAT", "mode": "REQUIRED"}, {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"}, {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}]}',
     '1.0',
     '{"incremental_field": "updated_at", "batch_size": 1000}',
     TRUE, 'test_user', '{"environment": "test", "data_owner": "test_team"}'),
     
    ('src_api_test_data', 'Test API Source', 'External API providing test data', 'API', 
     '{"base_url": "http://mock-api:1080/api/v1", "endpoint": "/data", "method": "GET", "headers": {"Content-Type": "application/json", "Authorization": "Bearer test_token"}}',
     '{"fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED"}, {"name": "customer_name", "type": "STRING", "mode": "REQUIRED"}, {"name": "email", "type": "STRING", "mode": "NULLABLE"}, {"name": "subscription_type", "type": "STRING", "mode": "REQUIRED"}, {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"}]}',
     '1.0',
     '{"pagination": {"enabled": true, "page_param": "page", "size_param": "size", "size_value": 100}, "rate_limit": {"requests_per_minute": 60}}',
     TRUE, 'test_user', '{"environment": "test", "data_owner": "test_team"}');

-- Sample data for quality_rules
INSERT INTO quality_rules (rule_id, name, rule_type, subtype, dimension, description, parameters, metadata, version, enabled)
VALUES
    ('rule_not_null_check', 'Not Null Check', 'SCHEMA_VALIDATION', 'NOT_NULL', 'COMPLETENESS', 
     'Validates that required fields are not null', 
     '{"column": "id", "action_on_failure": "FAIL"}',
     '{"severity": "HIGH", "owner": "data_quality_team"}',
     '1.0', TRUE),
     
    ('rule_value_range_check', 'Value Range Check', 'CONTENT_VALIDATION', 'RANGE', 'ACCURACY', 
     'Validates that numeric values fall within expected range', 
     '{"column": "price", "min_value": 0, "max_value": 10000, "action_on_failure": "FLAG"}',
     '{"severity": "MEDIUM", "owner": "data_quality_team"}',
     '1.0', TRUE),
     
    ('rule_format_check', 'Format Check', 'CONTENT_VALIDATION', 'FORMAT', 'CONSISTENCY', 
     'Validates that string values match expected format', 
     '{"column": "email", "format_regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", "action_on_failure": "FLAG"}',
     '{"severity": "MEDIUM", "owner": "data_quality_team"}',
     '1.0', TRUE),
     
    ('rule_uniqueness_check', 'Uniqueness Check', 'CONTENT_VALIDATION', 'UNIQUENESS', 'UNIQUENESS', 
     'Validates that values in a column are unique', 
     '{"column": "id", "action_on_failure": "FAIL"}',
     '{"severity": "HIGH", "owner": "data_quality_team"}',
     '1.0', TRUE),
     
    ('rule_freshness_check', 'Freshness Check', 'CONTENT_VALIDATION', 'FRESHNESS', 'TIMELINESS', 
     'Validates that data is not older than expected', 
     '{"column": "updated_at", "max_age_days": 7, "action_on_failure": "FLAG"}',
     '{"severity": "MEDIUM", "owner": "data_quality_team"}',
     '1.0', TRUE);

-- Sample data for pipeline_definitions
INSERT INTO pipeline_definitions (pipeline_id, name, description, pipeline_type, source_id, target_dataset, target_table, 
                                 transformation_config, quality_config, self_healing_config, scheduling_config, 
                                 execution_config, performance_config, is_active, created_by, metadata, dag_id, quality_rule_ids)
VALUES
    ('pipe_gcs_to_bq', 'GCS to BigQuery Pipeline', 'Loads data from GCS to BigQuery with quality validation', 'BATCH', 
     'src_gcs_test_data', 'test_dataset', 'test_data',
     '{"transformations": [{"column": "value", "operation": "ROUND", "parameters": {"precision": 2}}]}',
     '{"validation_enabled": true, "threshold": 0.95, "action_on_failure": "CONTINUE_WITH_ERRORS"}',
     '{"enabled": true, "confidence_threshold": 0.8, "max_attempts": 3}',
     '{"schedule_interval": "0 */3 * * *", "start_date": "2023-01-01T00:00:00Z", "end_date": null, "catchup": false}',
     '{"timeout_seconds": 3600, "retries": 2, "retry_delay_seconds": 300}',
     '{"batch_size": 10000, "parallel_tasks": 4}',
     TRUE, 'test_user', '{"environment": "test", "owner": "data_engineering_team"}',
     'gcs_to_bq_pipeline', '["rule_not_null_check", "rule_uniqueness_check"]'),
     
    ('pipe_cloudsql_to_bq', 'CloudSQL to BigQuery Pipeline', 'Extracts data from Cloud SQL and loads to BigQuery', 'BATCH', 
     'src_cloudsql_test_data', 'test_dataset', 'products',
     '{}',
     '{"validation_enabled": true, "threshold": 0.98, "action_on_failure": "FAIL"}',
     '{"enabled": true, "confidence_threshold": 0.85, "max_attempts": 2}',
     '{"schedule_interval": "0 0 * * *", "start_date": "2023-01-01T00:00:00Z", "end_date": null, "catchup": false}',
     '{"timeout_seconds": 7200, "retries": 3, "retry_delay_seconds": 600}',
     '{"batch_size": 5000, "parallel_tasks": 2}',
     TRUE, 'test_user', '{"environment": "test", "owner": "data_engineering_team"}',
     'cloudsql_to_bq_pipeline', '["rule_not_null_check", "rule_value_range_check", "rule_freshness_check"]'),
     
    ('pipe_api_to_bq', 'API to BigQuery Pipeline', 'Fetches data from external API and loads to BigQuery', 'BATCH', 
     'src_api_test_data', 'test_dataset', 'customers',
     '{}',
     '{"validation_enabled": true, "threshold": 0.9, "action_on_failure": "CONTINUE_WITH_ERRORS"}',
     '{"enabled": true, "confidence_threshold": 0.75, "max_attempts": 3}',
     '{"schedule_interval": "0 */6 * * *", "start_date": "2023-01-01T00:00:00Z", "end_date": null, "catchup": false}',
     '{"timeout_seconds": 1800, "retries": 3, "retry_delay_seconds": 300}',
     '{"batch_size": 1000, "parallel_tasks": 1}',
     TRUE, 'test_user', '{"environment": "test", "owner": "data_engineering_team"}',
     'api_to_bq_pipeline', '["rule_not_null_check", "rule_format_check", "rule_uniqueness_check"]');

-- Sample data for issue_patterns
INSERT INTO issue_patterns (pattern_id, issue_type, detection_pattern, confidence_threshold, description, is_active, metadata)
VALUES
    ('pattern_missing_values', 'DATA_QUALITY', 
     '{"rule_type": "SCHEMA_VALIDATION", "subtype": "NOT_NULL", "error_message_pattern": ".*null value.*"}',
     0.85, 'Pattern for detecting missing values in required fields', TRUE, 
     '{"priority": "HIGH", "owner": "data_quality_team"}'),
     
    ('pattern_out_of_range', 'DATA_QUALITY', 
     '{"rule_type": "CONTENT_VALIDATION", "subtype": "RANGE", "error_message_pattern": ".*out of range.*"}',
     0.80, 'Pattern for detecting values outside expected range', TRUE, 
     '{"priority": "MEDIUM", "owner": "data_quality_team"}'),
     
    ('pattern_format_error', 'DATA_QUALITY', 
     '{"rule_type": "CONTENT_VALIDATION", "subtype": "FORMAT", "error_message_pattern": ".*invalid format.*"}',
     0.75, 'Pattern for detecting format errors in string fields', TRUE, 
     '{"priority": "MEDIUM", "owner": "data_quality_team"}'),
     
    ('pattern_job_timeout', 'PIPELINE_EXECUTION', 
     '{"error_message_pattern": ".*timeout.*", "task_type": "EXTRACT"}',
     0.90, 'Pattern for detecting job timeout issues', TRUE, 
     '{"priority": "HIGH", "owner": "data_engineering_team"}'),
     
    ('pattern_api_rate_limit', 'PIPELINE_EXECUTION', 
     '{"error_message_pattern": ".*rate limit.*|.*429.*", "source_type": "API"}',
     0.95, 'Pattern for detecting API rate limit issues', TRUE, 
     '{"priority": "HIGH", "owner": "data_engineering_team"}');

-- Sample data for healing_actions
INSERT INTO healing_actions (action_id, name, action_type, description, action_parameters, pattern_id, 
                           execution_count, success_count, success_rate, is_active)
VALUES
    ('action_impute_missing', 'Impute Missing Values', 'DATA_CORRECTION', 
     'Imputes missing values with defaults or statistical measures', 
     '{"strategy": "DEFAULT", "default_values": {"string": "", "numeric": 0, "boolean": false, "timestamp": "1970-01-01T00:00:00Z"}}',
     'pattern_missing_values', 10, 8, 0.8, TRUE),
     
    ('action_clamp_values', 'Clamp Out-of-Range Values', 'DATA_CORRECTION', 
     'Clamps values to be within the specified range', 
     '{"strategy": "CLAMP", "min_value": 0, "max_value": 10000}',
     'pattern_out_of_range', 5, 5, 1.0, TRUE),
     
    ('action_format_correction', 'Format Correction', 'DATA_CORRECTION', 
     'Corrects common format issues in string fields', 
     '{"strategy": "FORMAT_FIX", "format_type": "EMAIL", "transformations": [{"pattern": "\\s+", "replacement": ""}, {"pattern": "@+", "replacement": "@"}]}',
     'pattern_format_error', 7, 5, 0.71, TRUE),
     
    ('action_increase_timeout', 'Increase Job Timeout', 'PIPELINE_ADJUSTMENT', 
     'Increases the timeout for jobs that are timing out', 
     '{"strategy": "INCREASE_TIMEOUT", "multiplier": 1.5, "max_timeout_seconds": 7200}',
     'pattern_job_timeout', 3, 3, 1.0, TRUE),
     
    ('action_rate_limit_backoff', 'API Rate Limit Backoff', 'PIPELINE_ADJUSTMENT', 
     'Implements exponential backoff for API rate limit issues', 
     '{"strategy": "EXPONENTIAL_BACKOFF", "initial_delay_seconds": 60, "max_delay_seconds": 600, "backoff_factor": 2}',
     'pattern_api_rate_limit', 8, 7, 0.88, TRUE);

-- Sample data for pipeline_executions
INSERT INTO pipeline_executions (execution_id, pipeline_id, dag_run_id, start_time, end_time, status, records_processed, metadata, error_details)
VALUES
    ('exec_gcs_20230615_001', 'pipe_gcs_to_bq', 'gcs_to_bq_pipeline_20230615T000000', 
     '2023-06-15T00:00:00Z', '2023-06-15T00:15:30Z', 'SUCCESS', 5000, 
     '{"source_files": ["test-data-20230615.csv"], "environment": "test"}', NULL),
     
    ('exec_cloudsql_20230615_001', 'pipe_cloudsql_to_bq', 'cloudsql_to_bq_pipeline_20230615T000000', 
     '2023-06-15T00:00:00Z', '2023-06-15T00:10:15Z', 'SUCCESS', 2500, 
     '{"incremental_key_value": "2023-06-14T00:00:00Z", "environment": "test"}', NULL),
     
    ('exec_api_20230615_001', 'pipe_api_to_bq', 'api_to_bq_pipeline_20230615T000000', 
     '2023-06-15T00:00:00Z', NULL, 'FAILED', 0, 
     '{"api_endpoint": "/api/v1/data", "environment": "test"}',
     '{"error_message": "API rate limit exceeded", "error_code": "429", "task_id": "extract_api_data"}'),
     
    ('exec_gcs_20230615_002', 'pipe_gcs_to_bq', 'gcs_to_bq_pipeline_20230615T030000', 
     '2023-06-15T03:00:00Z', '2023-06-15T03:20:45Z', 'SUCCESS_WITH_WARNINGS', 5200, 
     '{"source_files": ["test-data-20230615-2.csv"], "warnings": ["5 records with missing values"], "environment": "test"}',
     NULL);

-- Sample data for quality_validations
INSERT INTO quality_validations (validation_id, execution_id, rule_id, validation_time, passed, metrics, metadata)
VALUES
    ('val_gcs_20230615_001_rule1', 'exec_gcs_20230615_001', 'rule_not_null_check', '2023-06-15T00:05:30Z', 
     TRUE, '{"total_records": 5000, "passed_records": 5000, "failed_records": 0}', 
     '{"environment": "test"}'),
     
    ('val_gcs_20230615_001_rule2', 'exec_gcs_20230615_001', 'rule_uniqueness_check', '2023-06-15T00:06:15Z', 
     TRUE, '{"total_records": 5000, "passed_records": 5000, "failed_records": 0}', 
     '{"environment": "test"}'),
     
    ('val_cloudsql_20230615_001_rule1', 'exec_cloudsql_20230615_001', 'rule_not_null_check', '2023-06-15T00:03:45Z', 
     TRUE, '{"total_records": 2500, "passed_records": 2500, "failed_records": 0}', 
     '{"environment": "test"}'),
     
    ('val_cloudsql_20230615_001_rule2', 'exec_cloudsql_20230615_001', 'rule_value_range_check', '2023-06-15T00:04:30Z', 
     TRUE, '{"total_records": 2500, "passed_records": 2450, "failed_records": 50}', 
     '{"environment": "test"}');

-- Validation that failed
INSERT INTO quality_validations (validation_id, execution_id, rule_id, validation_time, passed, failure_details, metrics, metadata)
VALUES
    ('val_gcs_20230615_002_rule1', 'exec_gcs_20230615_002', 'rule_not_null_check', '2023-06-15T03:05:30Z', 
     FALSE, 
     '{"error_message": "Found 5 records with null values in required fields", "failed_records_sample": [{"id": "1001", "name": null, "value": 10.5, "timestamp": "2023-06-15T00:00:00Z"}, {"id": "1002", "name": null, "value": 15.2, "timestamp": "2023-06-15T00:00:00Z"}]}',
     '{"total_records": 5200, "passed_records": 5195, "failed_records": 5}', 
     '{"environment": "test"}');

-- Sample data for healing_executions
INSERT INTO healing_executions (healing_id, execution_id, validation_id, pattern_id, action_id, execution_time, 
                              successful, confidence_score, execution_details)
VALUES
    ('heal_gcs_20230615_002_001', 'exec_gcs_20230615_002', 'val_gcs_20230615_002_rule1', 
     'pattern_missing_values', 'action_impute_missing', '2023-06-15T03:06:15Z', 
     TRUE, 0.92, '{"records_fixed": 5, "fix_details": "Imputed missing name values with empty string"}'),
     
    ('heal_api_20230615_001_001', 'exec_api_20230615_001', NULL, 
     'pattern_api_rate_limit', 'action_rate_limit_backoff', '2023-06-15T00:01:30Z', 
     TRUE, 0.98, '{"backoff_applied": "120 seconds", "retry_attempt": 1}');

-- Sample data for pipeline_metrics
INSERT INTO pipeline_metrics (metric_id, execution_id, metric_category, metric_name, metric_value, collection_time, metadata)
VALUES
    ('metric_gcs_20230615_001_duration', 'exec_gcs_20230615_001', 'PERFORMANCE', 'execution_duration_seconds', 
     930.0, '2023-06-15T00:15:35Z', '{"environment": "test"}'),
     
    ('metric_gcs_20230615_001_throughput', 'exec_gcs_20230615_001', 'PERFORMANCE', 'records_per_second', 
     5.38, '2023-06-15T00:15:35Z', '{"environment": "test"}'),
     
    ('metric_gcs_20230615_001_quality', 'exec_gcs_20230615_001', 'QUALITY', 'quality_score', 
     1.0, '2023-06-15T00:15:35Z', '{"environment": "test"}'),
     
    ('metric_cloudsql_20230615_001_duration', 'exec_cloudsql_20230615_001', 'PERFORMANCE', 'execution_duration_seconds', 
     615.0, '2023-06-15T00:10:20Z', '{"environment": "test"}'),
     
    ('metric_cloudsql_20230615_001_quality', 'exec_cloudsql_20230615_001', 'QUALITY', 'quality_score', 
     0.98, '2023-06-15T00:10:20Z', '{"environment": "test"}'),
     
    ('metric_gcs_20230615_002_duration', 'exec_gcs_20230615_002', 'PERFORMANCE', 'execution_duration_seconds', 
     1245.0, '2023-06-15T03:20:50Z', '{"environment": "test"}'),
     
    ('metric_gcs_20230615_002_quality', 'exec_gcs_20230615_002', 'QUALITY', 'quality_score', 
     0.95, '2023-06-15T03:20:50Z', '{"environment": "test"}'),
     
    ('metric_gcs_20230615_002_healing', 'exec_gcs_20230615_002', 'SELF_HEALING', 'healing_success_rate', 
     1.0, '2023-06-15T03:20:50Z', '{"environment": "test"}');

-- Sample data for alerts
INSERT INTO alerts (alert_id, execution_id, alert_type, severity, message, details, created_at, 
                  acknowledged, acknowledged_by, acknowledged_at, resolved, resolved_at, metadata)
VALUES
    ('alert_api_20230615_001', 'exec_api_20230615_001', 'PIPELINE_FAILURE', 'HIGH', 
     'API pipeline failed due to rate limit exceeded', 
     '{"error_message": "API rate limit exceeded", "error_code": "429", "task_id": "extract_api_data", "healing_attempted": true, "healing_successful": true}',
     '2023-06-15T00:01:15Z', TRUE, 'test_user', '2023-06-15T00:05:30Z', TRUE, '2023-06-15T00:10:45Z',
     '{"environment": "test"}'),
     
    ('alert_gcs_20230615_002', 'exec_gcs_20230615_002', 'DATA_QUALITY', 'MEDIUM', 
     'Data quality validation failed: Found 5 records with null values', 
     '{"validation_id": "val_gcs_20230615_002_rule1", "rule_id": "rule_not_null_check", "healing_attempted": true, "healing_successful": true}',
     '2023-06-15T03:05:45Z', TRUE, 'test_user', '2023-06-15T03:10:15Z', TRUE, '2023-06-15T03:15:30Z',
     '{"environment": "test"}'),
     
    ('alert_system_20230615_001', NULL, 'SYSTEM', 'LOW', 
     'BigQuery slot utilization above 80%', 
     '{"current_utilization": 85.2, "threshold": 80.0, "duration_minutes": 15}',
     '2023-06-15T10:30:00Z', FALSE, NULL, NULL, FALSE, NULL,
     '{"environment": "test"}');

-- Create a separate database for Airflow
CREATE DATABASE IF NOT EXISTS test_airflow;

-- Create additional indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_created_at ON pipeline_executions(created_at);
CREATE INDEX IF NOT EXISTS idx_task_executions_start_time ON task_executions(start_time);
CREATE INDEX IF NOT EXISTS idx_quality_validations_created_at ON quality_validations(created_at);
CREATE INDEX IF NOT EXISTS idx_healing_executions_created_at ON healing_executions(created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_metric_name_value ON pipeline_metrics(metric_name, metric_value);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at_severity ON alerts(created_at, severity);

-- Grant necessary permissions to the test database user
GRANT ALL PRIVILEGES ON DATABASE test_pipeline TO postgres;
GRANT ALL PRIVILEGES ON DATABASE test_airflow TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;