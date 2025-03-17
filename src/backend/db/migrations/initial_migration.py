"""
Implements the initial database migration for the self-healing data pipeline, creating BigQuery tables
and Firestore collections with appropriate schemas. This script establishes the foundational data
structures required for pipeline execution tracking, data quality validation, and self-healing capabilities.
"""
import argparse  # standard library
import os  # standard library
import sys  # standard library
from datetime import datetime  # standard library

# Third-party libraries
from google.cloud import bigquery  # google-cloud-bigquery 3.11.0+
from google.cloud import firestore  # google-cloud-firestore 2.11.1

# Internal modules
from src.backend.constants import (  # src/backend/constants.py
    DataSourceType,
    PipelineStatus,
    ValidationRuleType,
    QualityDimension,
    HealingActionType,
    AlertSeverity,
)
from src.backend.db.schema.bigquery_schema import (  # src/backend/db/schema/bigquery_schema.py
    get_bigquery_schema,
    get_all_bigquery_schemas,
    BIGQUERY_TABLES,
)
from src.backend.db.schema.firestore_schema import (  # src/backend/db/schema/firestore_schema.py
    CollectionSchemas,
    COLLECTION_NAMES,
)
from src.backend.db.migrations.seed_data import run_seeding  # src/backend/db/migrations/seed_data.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.config.config_loader import get_config  # src/backend/utils/config/config_loader.py

# Initialize logger
logger = get_logger(__name__)

# Migration version
MIGRATION_VERSION = "1.0.0"


def create_bigquery_dataset(
    client: bigquery.Client, dataset_id: str, location: str
) -> bigquery.Dataset:
    """Creates a BigQuery dataset if it doesn't exist.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance.
        dataset_id (str): The ID of the dataset to create.
        location (str): The location for the dataset.

    Returns:
        google.cloud.bigquery.Dataset: The created or existing dataset.
    """
    # Construct the fully qualified dataset reference
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)

    # Check if dataset already exists
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"BigQuery dataset {dataset_id} already exists")
    except Exception:
        # If dataset doesn't exist, create it with specified location
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Created BigQuery dataset {dataset_id} in {location}")

    # Return the dataset reference
    return dataset


def create_bigquery_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list,
    clustering_fields: dict,
    partitioning_config: dict,
) -> bigquery.Table:
    """Creates a BigQuery table with the specified schema.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table to create.
        schema (list): The schema for the table.
        clustering_fields (dict): The clustering configuration for the table.
        partitioning_config (dict): The partitioning configuration for the table.

    Returns:
        google.cloud.bigquery.Table: The created or existing table.
    """
    # Construct the fully qualified table reference
    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(client.project, dataset_id), table_id
    )

    # Create a Table object with the specified schema
    table = bigquery.Table(table_ref, schema=schema)

    # Apply clustering configuration if provided
    if clustering_fields:
        table.clustering_fields = clustering_fields

    # Apply partitioning configuration if provided
    if partitioning_config:
        table.time_partitioning = partitioning_config

    # Check if table already exists
    try:
        client.get_table(table_ref)
        logger.info(f"BigQuery table {table_id} already exists in dataset {dataset_id}")
    except Exception:
        # If table doesn't exist, create it with specified schema and configuration
        client.create_table(table)
        logger.info(f"Created BigQuery table {table_id} in dataset {dataset_id}")
    else:
        # If table exists, update schema if needed
        existing_table = client.get_table(table_ref)
        if existing_table.schema != schema:
            existing_table.schema = schema
            client.update_table(existing_table, ["schema"])
            logger.info(f"Updated schema for BigQuery table {table_id} in dataset {dataset_id}")

    # Return the table reference
    return table


def create_all_bigquery_tables(client: bigquery.Client, dataset_id: str) -> bool:
    """Creates all required BigQuery tables for the pipeline.

    Args:
        client (google.cloud.bigquery.Client): BigQuery client instance.
        dataset_id (str): The ID of the dataset to create tables in.

    Returns:
        bool: Success status of the table creation operations.
    """
    try:
        # Get all BigQuery table schemas using get_all_bigquery_schemas
        all_schemas = get_all_bigquery_schemas()

        # For each table in BIGQUERY_TABLES:
        for table_name in BIGQUERY_TABLES:
            # Get schema for the table
            schema = get_bigquery_schema(table_name)

            # Determine partitioning configuration based on table (time-based for execution tables)
            partitioning_config = None
            if table_name in ["pipeline_executions", "task_executions", "quality_validations", "healing_executions", "pipeline_metrics", "alerts"]:
                partitioning_config = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="start_time" if table_name in ["pipeline_executions", "task_executions", "quality_validations", "healing_executions"] else "collection_time" if table_name == "pipeline_metrics" else "created_at",
                    expiration_ms=None,  # Keep partitions indefinitely
                )

            # Determine clustering fields based on table (e.g., pipeline_id for executions)
            clustering_fields = None
            if table_name == "pipeline_executions":
                clustering_fields = ["pipeline_id", "status"]
            elif table_name == "task_executions":
                clustering_fields = ["execution_id", "task_id"]
            elif table_name == "quality_validations":
                clustering_fields = ["execution_id", "passed"]
            elif table_name == "healing_executions":
                clustering_fields = ["pattern_id", "successful"]
            elif table_name == "pipeline_metrics":
                clustering_fields = ["metric_category", "pipeline_id"]
            elif table_name == "alerts":
                clustering_fields = ["severity", "acknowledged"]

            # Create table using create_bigquery_table
            create_bigquery_table(
                client=client,
                dataset_id=dataset_id,
                table_id=table_name,
                schema=schema,
                clustering_fields=clustering_fields,
                partitioning_config=partitioning_config,
            )

        logger.info("Successfully created all BigQuery tables")
        return True

    except Exception as e:
        logger.error(f"Error creating BigQuery tables: {str(e)}")
        return False


def create_firestore_collections(client: firestore.Client) -> bool:
    """Creates Firestore collections with appropriate schemas.

    Args:
        client (google.cloud.firestore.Client): Firestore client instance.

    Returns:
        bool: Success status of the collection creation operations.
    """
    try:
        # For each collection in COLLECTION_NAMES:
        for collection_name in COLLECTION_NAMES:
            # Get schema for the collection using CollectionSchemas
            schema = getattr(CollectionSchemas, f"{collection_name}_schema")()

            # Create a dummy document to initialize the collection if it doesn't exist
            doc_ref = client.collection(collection_name).document("init")
            doc_ref.set({"initialized": True})

            # Delete the dummy document to keep collection clean
            doc_ref.delete()

            logger.info(f"Created Firestore collection {collection_name}")

        logger.info("Successfully created all Firestore collections")
        return True

    except Exception as e:
        logger.error(f"Error creating Firestore collections: {str(e)}")
        return False


def run_migration(config: dict, seed_data: bool) -> bool:
    """Main function to run the database migration process.

    Args:
        config (dict): Configuration dictionary.
        seed_data (bool): Whether to seed the database with initial data.

    Returns:
        bool: Success status of the migration process.
    """
    try:
        # Initialize BigQuery client with project from config
        bq_client = bigquery.Client(project=config.get("gcp", {}).get("project_id"))

        # Initialize Firestore client with project from config
        fs_client = firestore.Client(project=config.get("gcp", {}).get("project_id"))

        # Get dataset ID and location from config
        dataset_id = config.get("bigquery", {}).get("dataset")
        location = config.get("gcp", {}).get("location")

        # Create BigQuery dataset using create_bigquery_dataset
        create_bigquery_dataset(client=bq_client, dataset_id=dataset_id, location=location)

        # Create all BigQuery tables using create_all_bigquery_tables
        create_all_bigquery_tables(client=bq_client, dataset_id=dataset_id)

        # Create Firestore collections using create_firestore_collections
        create_firestore_collections(client=fs_client)

        # If seed_data is True, run data seeding process using run_seeding
        if seed_data:
            run_seeding(config, bq_client, fs_client)

        logger.info("Database migration completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database migration failed: {str(e)}")
        return False


def main() -> int:
    """Entry point for the migration script when run directly.

    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    # Set up argument parser for command-line options
    parser = argparse.ArgumentParser(description="Run database migration for self-healing pipeline.")

    # Add arguments for project_id, dataset_id, location, seed_data, config_file
    parser.add_argument("--project_id", help="Google Cloud project ID")
    parser.add_argument("--dataset_id", help="BigQuery dataset ID")
    parser.add_argument("--location", help="GCP location (region)")
    parser.add_argument("--seed_data", action="store_true", help="Seed the database with initial data")
    parser.add_argument("--config_file", help="Path to the configuration file")

    # Parse command-line arguments
    args = parser.parse_args()

    # Load configuration from config file
    config = get_config(args.config_file)._config

    # Override config with command-line arguments if provided
    if args.project_id:
        config["gcp"]["project_id"] = args.project_id
    if args.dataset_id:
        config["bigquery"]["dataset"] = args.dataset_id
    if args.location:
        config["gcp"]["location"] = args.location

    # Run the migration process with run_migration
    success = run_migration(config, args.seed_data)

    # Return appropriate exit code based on migration success
    return 0 if success else 1


# Execute main function if script is run directly
if __name__ == "__main__":
    sys.exit(main())