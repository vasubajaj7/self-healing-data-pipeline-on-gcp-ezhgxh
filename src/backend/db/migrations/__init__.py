"""
Initialization module for the database migrations package that provides functionality for database schema creation, migration, and data seeding. 
This module serves as the entry point for database initialization and migration operations in the self-healing data pipeline.
"""
# Import necessary modules for database initialization and migration
from .initial_migration import run_migration, MIGRATION_VERSION  # Import initial migration functionality
from .schema import SCHEMA_VERSION, SchemaRegistry  # Import schema definitions and registry
from .seed_data import run_seeding  # Import data seeding functionality
from ...utils.logging.logger import get_logger  # Configure logging for the migrations module
from ...utils.config.config_loader import get_config  # Load configuration settings
from ...utils.storage.bigquery_client import BigQueryClient  # Use BigQuery client for database operations
from ...utils.storage.firestore_client import FirestoreClient  # Use Firestore client for database operations

# Initialize logger for this module
logger = get_logger(__name__)

# Initialize schema registry
schema_registry = SchemaRegistry()

# Define the public interface for this module
__all__ = ["initialize_database", "run_migration", "MIGRATION_VERSION", "SCHEMA_VERSION", "schema_registry"]


def initialize_database(config: dict, seed_data: bool) -> bool:
    """
    Initializes the database by running migrations and optionally seeding data.

    Args:
        config (dict): Configuration dictionary containing database settings.
        seed_data (bool): Flag indicating whether to seed the database with initial data.

    Returns:
        bool: True if the database was initialized successfully, False otherwise.
    """
    logger.info("Starting database initialization...")

    try:
        # Initialize BigQueryClient with configuration
        bq_client = BigQueryClient(config=config)

        # Initialize FirestoreClient with configuration
        fs_client = FirestoreClient(config=config)

        # Run database migration using run_migration function from initial_migration module
        migration_success = run_migration(config, seed_data)
        if not migration_success:
            logger.error("Database migration failed.")
            return False

        # If seed_data is True, run data seeding process using run_seeding function from seed_data module
        if seed_data:
            seeding_success = run_seeding(config, bq_client, fs_client)
            if not seeding_success:
                logger.error("Data seeding failed.")
                return False

        logger.info("Database initialization completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

def get_migration_status(bq_client: BigQueryClient, fs_client: FirestoreClient) -> dict:
    """
    Retrieves the current migration status from the database.

    Args:
        bq_client (BigQueryClient): BigQuery client instance.
        fs_client (FirestoreClient): Firestore client instance.

    Returns:
        dict: Migration status information including version and timestamp.
    """
    migration_metadata_collection = "migration_metadata"
    try:
        # Check if migration metadata collection exists in Firestore
        collection_ref = fs_client.client.collection(migration_metadata_collection)
        if collection_ref is None:
            logger.info(f"Migration metadata collection '{migration_metadata_collection}' does not exist.")
            return {"version": None, "timestamp": None, "status": "NOT_INITIALIZED"}

        # Retrieve the latest migration record
        query = collection_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(1)
        results = query.get()

        # Extract migration version, timestamp, and status
        for doc in results:
            migration_data = doc.to_dict()
            version = migration_data.get("version")
            timestamp = migration_data.get("timestamp")
            status = migration_data.get("status")
            logger.info(f"Retrieved migration status: Version={version}, Timestamp={timestamp}, Status={status}")
            return {"version": version, "timestamp": timestamp, "status": status}

        # If no migration record found, return default status with version None
        logger.info("No migration record found in Firestore.")
        return {"version": None, "timestamp": None, "status": "NOT_FOUND"}

    except Exception as e:
        logger.error(f"Error retrieving migration status: {e}")
        return {"version": None, "timestamp": None, "status": "ERROR", "error": str(e)}

def update_migration_status(fs_client: FirestoreClient, version: str, success: bool) -> bool:
    """
    Updates the migration status in the database.

    Args:
        fs_client (FirestoreClient): Firestore client instance.
        version (str): Migration version.
        success (bool): Success status of the migration.

    Returns:
        bool: Success status of the update operation.
    """
    migration_metadata_collection = "migration_metadata"
    try:
        # Create migration status record with version, timestamp, and success status
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        status = "SUCCESS" if success else "FAILED"
        migration_status = {
            "version": version,
            "timestamp": timestamp,
            "status": status
        }

        # Store record in migration_metadata collection in Firestore
        doc_ref = fs_client.client.collection(migration_metadata_collection).document()
        doc_ref.set(migration_status)

        logger.info(f"Updated migration status to version {version}, status {status}")
        return True

    except Exception as e:
        logger.error(f"Error updating migration status: {e}")
        return False