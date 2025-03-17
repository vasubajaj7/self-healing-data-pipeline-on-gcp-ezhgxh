"""
Provides functionality to seed initial data into the database for the self-healing data pipeline.

This module creates sample data for source systems, pipeline definitions, quality rules, 
issue patterns, and healing actions to enable the system to function properly from the start.
"""

import uuid
import datetime
import json
import random

from ...constants import (
    DataSourceType,
    QualityDimension,
    HealingActionType,
    AlertSeverity,
    SelfHealingMode
)
from ..schema.bigquery_schema import BIGQUERY_TABLES
from ..schema.firestore_schema import COLLECTION_NAMES
from ...utils.logging.logger import get_logger
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.storage.firestore_client import FirestoreClient

# Configure logging
logger = get_logger(__name__)


def generate_source_systems_data():
    """
    Generates sample data for source systems.

    Returns:
        List of source system records
    """
    now = datetime.datetime.now()
    
    source_systems = [
        {
            "source_id": str(uuid.uuid4()),
            "source_name": "Sales Data Lake",
            "source_type": DataSourceType.GCS.value,
            "connection_details": json.dumps({
                "bucket_name": "sales-data-lake",
                "region": "us-central1",
                "prefix": "daily-sales/",
                "file_format": "CSV"
            }),
            "schema_version": "1.0",
            "extraction_method": "BATCH",
            "last_successful_extraction": now - datetime.timedelta(hours=8),
            "created_at": now - datetime.timedelta(days=30),
            "updated_at": now - datetime.timedelta(days=2),
            "is_active": True
        },
        {
            "source_id": str(uuid.uuid4()),
            "source_name": "Customer Database",
            "source_type": DataSourceType.CLOUD_SQL.value,
            "connection_details": json.dumps({
                "instance": "customer-db-instance",
                "database": "customer_data",
                "region": "us-central1",
                "connection_type": "PRIVATE"
            }),
            "schema_version": "2.1",
            "extraction_method": "INCREMENTAL",
            "last_successful_extraction": now - datetime.timedelta(hours=4),
            "created_at": now - datetime.timedelta(days=45),
            "updated_at": now - datetime.timedelta(days=5),
            "is_active": True
        },
        {
            "source_id": str(uuid.uuid4()),
            "source_name": "Product Catalog API",
            "source_type": DataSourceType.API.value,
            "connection_details": json.dumps({
                "endpoint": "https://api.example.com/products",
                "auth_type": "OAUTH2",
                "method": "GET",
                "rate_limit": 100
            }),
            "schema_version": "1.3",
            "extraction_method": "POLLING",
            "last_successful_extraction": now - datetime.timedelta(hours=2),
            "created_at": now - datetime.timedelta(days=15),
            "updated_at": now - datetime.timedelta(days=1),
            "is_active": True
        }
    ]
    
    return source_systems


def generate_pipeline_definitions_data(source_systems):
    """
    Generates sample data for pipeline definitions.

    Args:
        source_systems: List of source system records

    Returns:
        List of pipeline definition records
    """
    now = datetime.datetime.now()
    
    # Create mapping from source type to source_id for easy reference
    source_map = {}
    for source in source_systems:
        source_map[source["source_type"]] = source["source_id"]
    
    pipeline_definitions = [
        {
            "pipeline_id": str(uuid.uuid4()),
            "pipeline_name": "Daily Sales ETL",
            "source_id": source_map.get(DataSourceType.GCS.value),
            "target_dataset": "sales_analytics",
            "target_table": "daily_sales",
            "dag_id": "daily_sales_etl",
            "schedule_interval": "0 2 * * *",  # Daily at 2 AM
            "quality_threshold": 0.95,
            "self_healing_mode": SelfHealingMode.SEMI_AUTOMATIC.value,
            "self_healing_confidence_threshold": 0.85,
            "description": "Daily ETL process for sales data from GCS to BigQuery",
            "created_at": now - datetime.timedelta(days=30),
            "updated_at": now - datetime.timedelta(days=5),
            "is_active": True,
            "owner": "data_engineering_team"
        },
        {
            "pipeline_id": str(uuid.uuid4()),
            "pipeline_name": "Customer Data Synchronization",
            "source_id": source_map.get(DataSourceType.CLOUD_SQL.value),
            "target_dataset": "customer_analytics",
            "target_table": "customer_profile",
            "dag_id": "customer_data_sync",
            "schedule_interval": "0 */4 * * *",  # Every 4 hours
            "quality_threshold": 0.98,
            "self_healing_mode": SelfHealingMode.SEMI_AUTOMATIC.value,
            "self_healing_confidence_threshold": 0.90,
            "description": "Synchronization of customer data from Cloud SQL to BigQuery",
            "created_at": now - datetime.timedelta(days=45),
            "updated_at": now - datetime.timedelta(days=2),
            "is_active": True,
            "owner": "customer_data_team"
        },
        {
            "pipeline_id": str(uuid.uuid4()),
            "pipeline_name": "Product Catalog Updates",
            "source_id": source_map.get(DataSourceType.API.value),
            "target_dataset": "product_analytics",
            "target_table": "product_catalog",
            "dag_id": "product_catalog_updates",
            "schedule_interval": "0 */6 * * *",  # Every 6 hours
            "quality_threshold": 0.92,
            "self_healing_mode": SelfHealingMode.AUTOMATIC.value,
            "self_healing_confidence_threshold": 0.80,
            "description": "Regular updates from Product API to BigQuery catalog",
            "created_at": now - datetime.timedelta(days=15),
            "updated_at": now - datetime.timedelta(days=1),
            "is_active": True,
            "owner": "product_team"
        }
    ]
    
    return pipeline_definitions


def generate_quality_rules_data():
    """
    Generates sample data for quality validation rules.

    Returns:
        List of quality rule records
    """
    now = datetime.datetime.now()
    
    quality_rules = [
        # Schema validation rules
        {
            "rule_id": str(uuid.uuid4()),
            "rule_name": "Sales Data Schema Validation",
            "rule_type": "SCHEMA",
            "target_dataset": "sales_analytics",
            "target_table": "daily_sales",
            "quality_dimension": QualityDimension.VALIDITY.value,
            "expectation_type": "expect_table_schema_to_match",
            "expectation_kwargs": json.dumps({
                "schema": {
                    "date": "DATE",
                    "product_id": "STRING",
                    "store_id": "STRING",
                    "quantity": "INTEGER",
                    "unit_price": "FLOAT",
                    "total_price": "FLOAT"
                }
            }),
            "severity": AlertSeverity.HIGH.value,
            "description": "Validates the schema of the daily sales data",
            "created_at": now - datetime.timedelta(days=30),
            "updated_at": now - datetime.timedelta(days=5),
            "is_active": True
        },
        # Null check rules
        {
            "rule_id": str(uuid.uuid4()),
            "rule_name": "Customer ID Not Null",
            "rule_type": "NULL_CHECK",
            "target_dataset": "customer_analytics",
            "target_table": "customer_profile",
            "quality_dimension": QualityDimension.COMPLETENESS.value,
            "expectation_type": "expect_column_values_to_not_be_null",
            "expectation_kwargs": json.dumps({
                "column": "customer_id"
            }),
            "severity": AlertSeverity.CRITICAL.value,
            "description": "Ensures customer ID is never null",
            "created_at": now - datetime.timedelta(days=45),
            "updated_at": now - datetime.timedelta(days=10),
            "is_active": True
        },
        # Value range rules
        {
            "rule_id": str(uuid.uuid4()),
            "rule_name": "Product Price Range Check",
            "rule_type": "VALUE_RANGE",
            "target_dataset": "product_analytics",
            "target_table": "product_catalog",
            "quality_dimension": QualityDimension.ACCURACY.value,
            "expectation_type": "expect_column_values_to_be_between",
            "expectation_kwargs": json.dumps({
                "column": "price",
                "min_value": 0.01,
                "max_value": 9999.99
            }),
            "severity": AlertSeverity.HIGH.value,
            "description": "Validates that product prices are within a reasonable range",
            "created_at": now - datetime.timedelta(days=15),
            "updated_at": now - datetime.timedelta(days=3),
            "is_active": True
        },
        # Referential integrity rules
        {
            "rule_id": str(uuid.uuid4()),
            "rule_name": "Product ID Reference Check",
            "rule_type": "REFERENCE",
            "target_dataset": "sales_analytics",
            "target_table": "daily_sales",
            "quality_dimension": QualityDimension.CONSISTENCY.value,
            "expectation_type": "expect_column_values_to_exist_in_set",
            "expectation_kwargs": json.dumps({
                "column": "product_id",
                "value_set_query": "SELECT product_id FROM product_analytics.product_catalog"
            }),
            "severity": AlertSeverity.MEDIUM.value,
            "description": "Ensures product IDs in sales data exist in the product catalog",
            "created_at": now - datetime.timedelta(days=20),
            "updated_at": now - datetime.timedelta(days=2),
            "is_active": True
        },
        # Statistical validation rules
        {
            "rule_id": str(uuid.uuid4()),
            "rule_name": "Daily Sales Anomaly Detection",
            "rule_type": "STATISTICAL",
            "target_dataset": "sales_analytics",
            "target_table": "daily_sales",
            "quality_dimension": QualityDimension.ACCURACY.value,
            "expectation_type": "expect_column_mean_to_be_between",
            "expectation_kwargs": json.dumps({
                "column": "total_price",
                "min_value": {
                    "type": "dynamic",
                    "query": "SELECT AVG(total_price) * 0.5 FROM sales_analytics.daily_sales WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
                },
                "max_value": {
                    "type": "dynamic",
                    "query": "SELECT AVG(total_price) * 2.0 FROM sales_analytics.daily_sales WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
                }
            }),
            "severity": AlertSeverity.MEDIUM.value,
            "description": "Detects anomalies in daily sales totals",
            "created_at": now - datetime.timedelta(days=25),
            "updated_at": now - datetime.timedelta(days=1),
            "is_active": True
        }
    ]
    
    return quality_rules


def generate_issue_patterns_data():
    """
    Generates sample data for common issue patterns.

    Returns:
        List of issue pattern records
    """
    now = datetime.datetime.now()
    
    issue_patterns = [
        # Missing values pattern
        {
            "pattern_id": str(uuid.uuid4()),
            "name": "Missing Values Pattern",
            "issue_type": "DATA_QUALITY",
            "pattern_type": "MISSING_VALUES",
            "description": "Detects missing or null values in required fields",
            "detection_pattern": json.dumps({
                "rule_type": "NULL_CHECK",
                "error_message_pattern": ".*null.*required.*",
                "failure_threshold": 5
            }),
            "confidence_threshold": 0.85,
            "occurrence_count": 27,
            "success_count": 24,
            "success_rate": 0.89,
            "is_active": True,
            "last_seen": now - datetime.timedelta(days=1),
            "created_at": now - datetime.timedelta(days=60),
            "updated_at": now - datetime.timedelta(days=1)
        },
        # Schema drift pattern
        {
            "pattern_id": str(uuid.uuid4()),
            "name": "Schema Drift Pattern",
            "issue_type": "DATA_QUALITY",
            "pattern_type": "SCHEMA_DRIFT",
            "description": "Detects changes in source data schema",
            "detection_pattern": json.dumps({
                "rule_type": "SCHEMA",
                "error_message_pattern": ".*schema.*mismatch.*|.*unexpected.*column.*",
                "failure_threshold": 1
            }),
            "confidence_threshold": 0.90,
            "occurrence_count": 12,
            "success_count": 10,
            "success_rate": 0.83,
            "is_active": True,
            "last_seen": now - datetime.timedelta(days=5),
            "created_at": now - datetime.timedelta(days=90),
            "updated_at": now - datetime.timedelta(days=5)
        },
        # Data type mismatch pattern
        {
            "pattern_id": str(uuid.uuid4()),
            "name": "Data Type Mismatch Pattern",
            "issue_type": "DATA_QUALITY",
            "pattern_type": "TYPE_MISMATCH",
            "description": "Detects incorrect data types in source data",
            "detection_pattern": json.dumps({
                "rule_type": "VALUE_TYPE",
                "error_message_pattern": ".*invalid.*type.*|.*cannot.*convert.*",
                "failure_threshold": 3
            }),
            "confidence_threshold": 0.92,
            "occurrence_count": 18,
            "success_count": 17,
            "success_rate": 0.94,
            "is_active": True,
            "last_seen": now - datetime.timedelta(days=3),
            "created_at": now - datetime.timedelta(days=75),
            "updated_at": now - datetime.timedelta(days=3)
        },
        # Performance degradation pattern
        {
            "pattern_id": str(uuid.uuid4()),
            "name": "Query Performance Degradation",
            "issue_type": "PERFORMANCE",
            "pattern_type": "PERFORMANCE_DEGRADATION",
            "description": "Detects significant slowdown in query execution",
            "detection_pattern": json.dumps({
                "metric": "query_execution_time",
                "baseline_period_days": 7,
                "threshold_increase_percentage": 50,
                "min_execution_time_seconds": 60
            }),
            "confidence_threshold": 0.80,
            "occurrence_count": 8,
            "success_count": 6,
            "success_rate": 0.75,
            "is_active": True,
            "last_seen": now - datetime.timedelta(days=8),
            "created_at": now - datetime.timedelta(days=120),
            "updated_at": now - datetime.timedelta(days=8)
        },
        # Resource constraint pattern
        {
            "pattern_id": str(uuid.uuid4()),
            "name": "BigQuery Slot Exhaustion",
            "issue_type": "RESOURCE",
            "pattern_type": "RESOURCE_EXHAUSTION",
            "description": "Detects BigQuery slot capacity limits being reached",
            "detection_pattern": json.dumps({
                "metric": "bigquery_slots_usage_percentage",
                "threshold_percentage": 95,
                "duration_minutes": 10,
                "error_message_pattern": ".*quota.*exceeded.*|.*resource.*exhausted.*"
            }),
            "confidence_threshold": 0.95,
            "occurrence_count": 5,
            "success_count": 5,
            "success_rate": 1.0,
            "is_active": True,
            "last_seen": now - datetime.timedelta(days=15),
            "created_at": now - datetime.timedelta(days=150),
            "updated_at": now - datetime.timedelta(days=15)
        }
    ]
    
    return issue_patterns


def generate_healing_actions_data(issue_patterns):
    """
    Generates sample data for healing actions.

    Args:
        issue_patterns: List of issue pattern records

    Returns:
        List of healing action records
    """
    now = datetime.datetime.now()
    
    # Create mapping from pattern type to pattern_id for easy reference
    pattern_map = {}
    for pattern in issue_patterns:
        pattern_map[pattern["pattern_type"]] = pattern["pattern_id"]
    
    healing_actions = [
        # Missing value imputation action
        {
            "action_id": str(uuid.uuid4()),
            "name": "Missing Value Imputation",
            "action_type": HealingActionType.DATA_CORRECTION.value,
            "pattern_id": pattern_map.get("MISSING_VALUES"),
            "description": "Imputes missing values based on historical patterns",
            "action_parameters": json.dumps({
                "imputation_method": "MEAN",
                "fallback_method": "MEDIAN",
                "lookback_period_days": 30,
                "apply_to_columns": ["quantity", "unit_price", "total_price"]
            }),
            "execution_count": 24,
            "success_count": 22,
            "success_rate": 0.92,
            "is_active": True,
            "last_executed": now - datetime.timedelta(days=1),
            "created_at": now - datetime.timedelta(days=60),
            "updated_at": now - datetime.timedelta(days=1)
        },
        # Schema evolution action
        {
            "action_id": str(uuid.uuid4()),
            "name": "Schema Evolution Handler",
            "action_type": HealingActionType.SCHEMA_EVOLUTION.value,
            "pattern_id": pattern_map.get("SCHEMA_DRIFT"),
            "description": "Adapts to schema changes by updating target schema",
            "action_parameters": json.dumps({
                "allow_new_columns": True,
                "allow_column_type_widening": True,
                "disallow_breaking_changes": True,
                "update_validation_rules": True
            }),
            "execution_count": 10,
            "success_count": 8,
            "success_rate": 0.80,
            "is_active": True,
            "last_executed": now - datetime.timedelta(days=5),
            "created_at": now - datetime.timedelta(days=90),
            "updated_at": now - datetime.timedelta(days=5)
        },
        # Data type conversion action
        {
            "action_id": str(uuid.uuid4()),
            "name": "Automatic Type Conversion",
            "action_type": HealingActionType.DATA_CORRECTION.value,
            "pattern_id": pattern_map.get("TYPE_MISMATCH"),
            "description": "Automatically converts data to the expected types",
            "action_parameters": json.dumps({
                "string_to_number_handling": "TRY_CONVERT",
                "date_format_detection": True,
                "handle_special_values": True,
                "error_tolerance_percentage": 1.0
            }),
            "execution_count": 17,
            "success_count": 15,
            "success_rate": 0.88,
            "is_active": True,
            "last_executed": now - datetime.timedelta(days=3),
            "created_at": now - datetime.timedelta(days=75),
            "updated_at": now - datetime.timedelta(days=3)
        },
        # Query optimization action
        {
            "action_id": str(uuid.uuid4()),
            "name": "BigQuery Query Optimizer",
            "action_type": HealingActionType.PARAMETER_ADJUSTMENT.value,
            "pattern_id": pattern_map.get("PERFORMANCE_DEGRADATION"),
            "description": "Optimizes slow-running queries by rewriting or adding hints",
            "action_parameters": json.dumps({
                "optimization_techniques": ["JOIN_REORDERING", "PREDICATE_PUSHDOWN", "MATERIALIZATION"],
                "apply_clustering_recommendations": True,
                "apply_partitioning_recommendations": True,
                "max_query_cost_increase_percentage": 10
            }),
            "execution_count": 6,
            "success_count": 5,
            "success_rate": 0.83,
            "is_active": True,
            "last_executed": now - datetime.timedelta(days=8),
            "created_at": now - datetime.timedelta(days=120),
            "updated_at": now - datetime.timedelta(days=8)
        },
        # Resource scaling action
        {
            "action_id": str(uuid.uuid4()),
            "name": "BigQuery Slot Allocator",
            "action_type": HealingActionType.RESOURCE_SCALING.value,
            "pattern_id": pattern_map.get("RESOURCE_EXHAUSTION"),
            "description": "Dynamically allocates additional BigQuery slots",
            "action_parameters": json.dumps({
                "max_additional_slots": 500,
                "allocation_duration_minutes": 30,
                "cost_limit_usd": 50.0,
                "gradual_release": True
            }),
            "execution_count": 5,
            "success_count": 5,
            "success_rate": 1.0,
            "is_active": True,
            "last_executed": now - datetime.timedelta(days=15),
            "created_at": now - datetime.timedelta(days=150),
            "updated_at": now - datetime.timedelta(days=15)
        }
    ]
    
    return healing_actions


def seed_bigquery_data(bq_client, dataset_id):
    """
    Seeds sample data into BigQuery tables.

    Args:
        bq_client: BigQuery client instance
        dataset_id: Target dataset ID

    Returns:
        Success status of the seeding operation
    """
    try:
        logger.info(f"Starting to seed BigQuery data for dataset {dataset_id}")
        
        # Generate sample data
        source_systems = generate_source_systems_data()
        pipeline_definitions = generate_pipeline_definitions_data(source_systems)
        quality_rules = generate_quality_rules_data()
        issue_patterns = generate_issue_patterns_data()
        healing_actions = generate_healing_actions_data(issue_patterns)

        # Insert data into BigQuery tables
        table_name = BIGQUERY_TABLES["SOURCE_SYSTEMS"] if isinstance(BIGQUERY_TABLES, dict) else BIGQUERY_TABLES.SOURCE_SYSTEMS
        if bq_client.insert_rows(dataset_id, table_name, source_systems):
            logger.info(f"Successfully seeded {len(source_systems)} source system records")
        else:
            logger.error("Failed to seed source system records")
            return False

        table_name = BIGQUERY_TABLES["PIPELINE_DEFINITIONS"] if isinstance(BIGQUERY_TABLES, dict) else BIGQUERY_TABLES.PIPELINE_DEFINITIONS
        if bq_client.insert_rows(dataset_id, table_name, pipeline_definitions):
            logger.info(f"Successfully seeded {len(pipeline_definitions)} pipeline definition records")
        else:
            logger.error("Failed to seed pipeline definition records")
            return False

        table_name = BIGQUERY_TABLES["QUALITY_RULES"] if isinstance(BIGQUERY_TABLES, dict) else BIGQUERY_TABLES.QUALITY_RULES
        if bq_client.insert_rows(dataset_id, table_name, quality_rules):
            logger.info(f"Successfully seeded {len(quality_rules)} quality rule records")
        else:
            logger.error("Failed to seed quality rule records")
            return False

        table_name = BIGQUERY_TABLES["ISSUE_PATTERNS"] if isinstance(BIGQUERY_TABLES, dict) else BIGQUERY_TABLES.ISSUE_PATTERNS
        if bq_client.insert_rows(dataset_id, table_name, issue_patterns):
            logger.info(f"Successfully seeded {len(issue_patterns)} issue pattern records")
        else:
            logger.error("Failed to seed issue pattern records")
            return False

        table_name = BIGQUERY_TABLES["HEALING_ACTIONS"] if isinstance(BIGQUERY_TABLES, dict) else BIGQUERY_TABLES.HEALING_ACTIONS
        if bq_client.insert_rows(dataset_id, table_name, healing_actions):
            logger.info(f"Successfully seeded {len(healing_actions)} healing action records")
        else:
            logger.error("Failed to seed healing action records")
            return False

        logger.info("Successfully completed BigQuery data seeding")
        return True
        
    except Exception as e:
        logger.error(f"Error seeding BigQuery data: {str(e)}")
        return False


def seed_firestore_data(fs_client):
    """
    Seeds sample data into Firestore collections.

    Args:
        fs_client: Firestore client instance

    Returns:
        Success status of the seeding operation
    """
    try:
        logger.info("Starting to seed Firestore data")
        
        # Generate sample data
        source_systems = generate_source_systems_data()
        pipeline_definitions = generate_pipeline_definitions_data(source_systems)
        issue_patterns = generate_issue_patterns_data()
        healing_actions = generate_healing_actions_data(issue_patterns)

        # Insert source systems data
        collection_name = COLLECTION_NAMES["SOURCE_SYSTEMS"] if isinstance(COLLECTION_NAMES, dict) else COLLECTION_NAMES.SOURCE_SYSTEMS
        for source in source_systems:
            source_id = source["source_id"]
            fs_client.set_document(collection_name, source_id, source)
        logger.info(f"Successfully seeded {len(source_systems)} source system documents")

        # Insert pipeline definitions data
        collection_name = COLLECTION_NAMES["PIPELINE_DEFINITIONS"] if isinstance(COLLECTION_NAMES, dict) else COLLECTION_NAMES.PIPELINE_DEFINITIONS
        for pipeline in pipeline_definitions:
            pipeline_id = pipeline["pipeline_id"]
            fs_client.set_document(collection_name, pipeline_id, pipeline)
        logger.info(f"Successfully seeded {len(pipeline_definitions)} pipeline definition documents")

        # Insert issue patterns data
        collection_name = COLLECTION_NAMES["ISSUE_PATTERNS"] if isinstance(COLLECTION_NAMES, dict) else COLLECTION_NAMES.ISSUE_PATTERNS
        for pattern in issue_patterns:
            pattern_id = pattern["pattern_id"]
            fs_client.set_document(collection_name, pattern_id, pattern)
        logger.info(f"Successfully seeded {len(issue_patterns)} issue pattern documents")

        # Insert healing actions data
        collection_name = COLLECTION_NAMES["HEALING_ACTIONS"] if isinstance(COLLECTION_NAMES, dict) else COLLECTION_NAMES.HEALING_ACTIONS
        for action in healing_actions:
            action_id = action["action_id"]
            fs_client.set_document(collection_name, action_id, action)
        logger.info(f"Successfully seeded {len(healing_actions)} healing action documents")

        # Insert configuration data
        collection_name = COLLECTION_NAMES["CONFIGURATION"] if isinstance(COLLECTION_NAMES, dict) else COLLECTION_NAMES.CONFIGURATION
        default_config = {
            "config_id": str(uuid.uuid4()),
            "config_type": "SYSTEM_DEFAULTS",
            "name": "System Default Configuration",
            "description": "Default system configuration values",
            "config_values": {
                "self_healing_mode": SelfHealingMode.SEMI_AUTOMATIC.value,
                "confidence_threshold": 0.85,
                "max_retry_attempts": 3,
                "notification_channels": ["EMAIL", "TEAMS"],
                "default_quality_threshold": 0.9
            },
            "is_active": True,
            "version": "1.0",
            "created_at": datetime.datetime.now(),
            "updated_at": datetime.datetime.now()
        }
        fs_client.set_document(collection_name, default_config["config_id"], default_config)
        logger.info("Successfully seeded configuration data")

        logger.info("Successfully completed Firestore data seeding")
        return True

    except Exception as e:
        logger.error(f"Error seeding Firestore data: {str(e)}")
        return False


def run_seeding(config, bq_client, fs_client):
    """
    Main function to run the data seeding process.

    Args:
        config: Configuration dictionary
        bq_client: BigQuery client instance
        fs_client: Firestore client instance

    Returns:
        Overall success status of the seeding process
    """
    logger.info("Starting data seeding process")
    
    # Get BigQuery dataset ID from config
    dataset_id = config.get("bigquery", {}).get("dataset", "self_healing_pipeline")
    
    # Seed BigQuery data
    bq_success = seed_bigquery_data(bq_client, dataset_id)
    
    # Seed Firestore data
    fs_success = seed_firestore_data(fs_client)
    
    # Overall success
    success = bq_success and fs_success
    
    if success:
        logger.info("Data seeding process completed successfully")
    else:
        logger.error("Data seeding process completed with errors")
    
    return success