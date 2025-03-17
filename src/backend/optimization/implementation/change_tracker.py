"""
Tracks and manages changes made during optimization implementations in the self-healing data pipeline.
This component records details of schema changes, query optimizations, and resource adjustments,
enabling auditing, rollback capabilities, and effectiveness analysis.
"""

import datetime  # standard library
import typing  # standard library
import json  # standard library
import uuid  # standard library

from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.storage.firestore_client import FirestoreClient  # src/backend/utils/storage/firestore_client.py
from src.backend.utils.storage.bigquery_client import BigQueryClient  # src/backend/utils/storage/bigquery_client.py
from src.backend.utils.errors.exception_handler import handle_exceptions  # src/backend/utils/errors/exception_handler.py
from src.backend.utils.monitoring.metric_client import MetricClient  # src/backend/utils/monitoring/metric_client.py

# Initialize logger for this module
logger = get_logger(__name__)

# Define constants for change types, statuses, and collection names
CHANGE_TYPES = {
    'QUERY': 'query_optimization',
    'SCHEMA': 'schema_optimization',
    'RESOURCE': 'resource_optimization',
    'ROLLBACK': 'optimization_rollback'
}
CHANGE_STATUS = {
    'PENDING': 'pending',
    'IN_PROGRESS': 'in_progress',
    'COMPLETED': 'completed',
    'FAILED': 'failed',
    'ROLLED_BACK': 'rolled_back'
}
CHANGE_COLLECTION = "optimization_changes"
EFFECTIVENESS_COLLECTION = "optimization_effectiveness"
CHANGE_METRIC_PREFIX = "optimization.changes"


def generate_change_id(change_type: str, target_id: str) -> str:
    """Generates a unique identifier for a change record

    Args:
        change_type (str): Type of the change
        target_id (str): ID of the target being changed

    Returns:
        str: Unique change ID
    """
    # Generate a UUID for the change
    change_uuid = uuid.uuid4()

    # Combine with change type prefix for readability
    change_id = f"{change_type.upper()}-{target_id}-{change_uuid}"

    # Return the formatted change ID
    return change_id


def format_change_record(change_id: str, change_type: str, target_id: str, before_state: dict, after_state: dict, status: str, metadata: dict) -> dict:
    """Formats a change record for storage

    Args:
        change_id (str): Unique identifier for the change
        change_type (str): Type of the change
        target_id (str): ID of the target being changed
        before_state (dict): State of the target before the change
        after_state (dict): State of the target after the change
        status (str): Status of the change
        metadata (dict): Additional metadata about the change

    Returns:
        dict: Formatted change record
    """
    # Create change record dictionary with all parameters
    change_record = {
        "change_id": change_id,
        "change_type": change_type,
        "target_id": target_id,
        "before_state": before_state,
        "after_state": after_state,
        "status": status,
        "metadata": metadata,
    }

    # Add timestamp for change creation
    change_record["created_at"] = datetime.datetime.utcnow().isoformat()

    # Add user information if available
    # TODO: Implement user context retrieval
    # user = get_current_user()
    # if user:
    #     change_record["user_id"] = user.id
    #     change_record["user_name"] = user.name

    # Ensure all required fields are present
    # TODO: Add validation for required fields

    # Return the formatted record
    return change_record


class ChangeTracker:
    """Tracks and manages changes made during optimization implementations"""

    def __init__(self, firestore_client: FirestoreClient, bq_client: BigQueryClient, metric_client: MetricClient):
        """Initializes the ChangeTracker with necessary dependencies

        Args:
            firestore_client (FirestoreClient): Client for interacting with Firestore
            bq_client (BigQueryClient): Client for interacting with BigQuery
            metric_client (MetricClient): Client for recording metrics
        """
        # Store provided clients as instance variables
        self._firestore_client = firestore_client
        self._bq_client = bq_client
        self._metric_client = metric_client

        # Load configuration settings
        self._config = get_config()

        # Ensure required Firestore collections exist
        self.ensure_collections_exist()

        # Initialize logger for change tracking activities
        logger.info("ChangeTracker initialized")

    @handle_exceptions(log_exception=True, report_exception=True)
    def track_change(self, change_type: str, target_id: str, before_state: dict, after_state: dict, status: str, metadata: dict) -> str:
        """Records a change made during optimization implementation

        Args:
            change_type (str): Type of the change (e.g., 'QUERY', 'SCHEMA', 'RESOURCE')
            target_id (str): ID of the target being changed (e.g., query ID, table ID)
            before_state (dict): State of the target before the change
            after_state (dict): State of the target after the change
            status (str): Status of the change (e.g., 'PENDING', 'IN_PROGRESS', 'COMPLETED')
            metadata (dict): Additional metadata about the change

        Returns:
            str: ID of the recorded change
        """
        # Validate change_type against CHANGE_TYPES
        if change_type not in CHANGE_TYPES:
            raise ValueError(f"Invalid change_type: {change_type}. Must be one of {CHANGE_TYPES.keys()}")

        # Validate status against CHANGE_STATUS
        if status not in CHANGE_STATUS:
            raise ValueError(f"Invalid status: {status}. Must be one of {CHANGE_STATUS.keys()}")

        # Generate unique change ID
        change_id = generate_change_id(change_type, target_id)

        # Format change record with all details
        change_record = format_change_record(
            change_id=change_id,
            change_type=change_type,
            target_id=target_id,
            before_state=before_state,
            after_state=after_state,
            status=status,
            metadata=metadata
        )

        # Store change record in Firestore
        self._firestore_client.create_document(CHANGE_COLLECTION, change_id, change_record)

        # Record change tracking metrics
        self.record_metrics(f"{CHANGE_METRIC_PREFIX}.tracked", 1, {"change_type": change_type, "status": status})

        # Log change tracking activity
        logger.info(f"Tracked change: {change_id} for target {target_id} with status {status}")

        # Return the change ID
        return change_id

    @handle_exceptions(log_exception=True, report_exception=True)
    def update_change_status(self, change_id: str, status: str, additional_metadata: dict = None) -> bool:
        """Updates the status of a previously recorded change

        Args:
            change_id (str): ID of the change to update
            status (str): New status of the change
            additional_metadata (dict, optional): Additional metadata to add to the change record. Defaults to None.

        Returns:
            bool: True if update was successful
        """
        # Validate status against CHANGE_STATUS
        if status not in CHANGE_STATUS:
            raise ValueError(f"Invalid status: {status}. Must be one of {CHANGE_STATUS.keys()}")

        # Retrieve existing change record
        change_record = self.get_change(change_id)
        if not change_record:
            raise ValueError(f"Change record not found: {change_id}")

        # Update status and add status update timestamp
        change_record["status"] = status
        change_record["updated_at"] = datetime.datetime.utcnow().isoformat()

        # Merge additional metadata if provided
        if additional_metadata:
            change_record["metadata"] = change_record.get("metadata", {})
            change_record["metadata"].update(additional_metadata)

        # Update change record in Firestore
        self._firestore_client.update_document(CHANGE_COLLECTION, change_id, change_record)

        # Record status update metrics
        self.record_metrics(f"{CHANGE_METRIC_PREFIX}.status_updated", 1, {"status": status})

        # Log status update activity
        logger.info(f"Updated status of change {change_id} to {status}")

        # Return success indicator
        return True

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_change(self, change_id: str) -> dict:
        """Retrieves a specific change record by ID

        Args:
            change_id (str): ID of the change to retrieve

        Returns:
            dict: Change record or None if not found
        """
        # Retrieve change record from Firestore
        change_record = self._firestore_client.get_document(CHANGE_COLLECTION, change_id)

        # Log retrieval activity
        if change_record:
            logger.debug(f"Retrieved change record: {change_id}")
        else:
            logger.debug(f"Change record not found: {change_id}")

        # Return the change record or None if not found
        return change_record

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_changes_by_target(self, target_id: str, change_type: str = None, limit: int = 10) -> list:
        """Retrieves all changes for a specific target (query, table, resource)

        Args:
            target_id (str): ID of the target
            change_type (str, optional): Type of change to filter by. Defaults to None.
            limit (int, optional): Maximum number of results to return. Defaults to 10.

        Returns:
            list: List of change records
        """
        # Build Firestore query filters
        filters = [("target_id", "==", target_id)]

        # Add change_type filter if provided
        if change_type:
            filters.append(("change_type", "==", change_type))

        # Execute query with limit if specified
        change_records = self._firestore_client.query_documents(CHANGE_COLLECTION, filters=filters, limit=limit)

        # Log retrieval activity with count
        logger.debug(f"Retrieved {len(change_records)} changes for target {target_id}")

        # Return list of change records
        return change_records

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_changes_by_type(self, change_type: str, status: str = None, start_date: datetime.datetime = None, end_date: datetime.datetime = None, limit: int = 10) -> list:
        """Retrieves changes filtered by type and optional status

        Args:
            change_type (str): Type of change to filter by
            status (str, optional): Status of change to filter by. Defaults to None.
            start_date (datetime, optional): Start date for filtering. Defaults to None.
            end_date (datetime, optional): End date for filtering. Defaults to None.
            limit (int, optional): Maximum number of results to return. Defaults to 10.

        Returns:
            list: List of change records
        """
        # Build Firestore query filters for change_type
        filters = [("change_type", "==", change_type)]

        # Add status filter if provided
        if status:
            filters.append(("status", "==", status))

        # Add date range filters if provided
        if start_date:
            filters.append(("created_at", ">=", start_date.isoformat()))
        if end_date:
            filters.append(("created_at", "<=", end_date.isoformat()))

        # Execute query with limit if specified
        change_records = self._firestore_client.query_documents(CHANGE_COLLECTION, filters=filters, limit=limit)

        # Log retrieval activity with count
        logger.debug(f"Retrieved {len(change_records)} changes of type {change_type}")

        # Return list of change records
        return change_records

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_change_history(self, start_date: datetime.datetime, end_date: datetime.datetime, change_type: str = None, status: str = None, limit: int = 10) -> list:
        """Retrieves change history for a specific period

        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            change_type (str, optional): Type of change to filter by. Defaults to None.
            status (str, optional): Status of change to filter by. Defaults to None.
            limit (int, optional): Maximum number of results to return. Defaults to 10.

        Returns:
            list: List of change records
        """
        # Build Firestore query filters for date range
        filters = [("created_at", ">=", start_date.isoformat()), ("created_at", "<=", end_date.isoformat())]

        # Add change_type filter if provided
        if change_type:
            filters.append(("change_type", "==", change_type))

        # Add status filter if provided
        if status:
            filters.append(("status", "==", status))

        # Execute query with limit if specified
        change_records = self._firestore_client.query_documents(CHANGE_COLLECTION, filters=filters, limit=limit)

        # Log retrieval activity with count
        logger.debug(f"Retrieved {len(change_records)} changes between {start_date} and {end_date}")

        # Return list of change records
        return change_records

    @handle_exceptions(log_exception=True, report_exception=True)
    def store_rollback_details(self, original_change_id: str, rollback_details: dict, status: str) -> str:
        """Stores details of a rollback operation

        Args:
            original_change_id (str): ID of the original change being rolled back
            rollback_details (dict): Details about the rollback operation
            status (str): Status of the rollback operation

        Returns:
            str: ID of the rollback change record
        """
        # Retrieve original change record
        original_change = self.get_change(original_change_id)
        if not original_change:
            raise ValueError(f"Original change record not found: {original_change_id}")

        # Create rollback change record linking to original
        rollback_id = generate_change_id("ROLLBACK", original_change_id)
        rollback_record = format_change_record(
            change_id=rollback_id,
            change_type="ROLLBACK",
            target_id=original_change["target_id"],
            before_state=original_change["after_state"],
            after_state=original_change["before_state"],
            status=status,
            metadata={"original_change_id": original_change_id, "rollback_details": rollback_details}
        )

        # Store rollback record in Firestore
        self._firestore_client.create_document(CHANGE_COLLECTION, rollback_id, rollback_record)

        # Update original change status to ROLLED_BACK
        self.update_change_status(original_change_id, "ROLLED_BACK", {"rollback_id": rollback_id})

        # Log rollback activity
        logger.info(f"Stored rollback details for change {original_change_id} with rollback ID {rollback_id}")

        # Return the rollback change ID
        return rollback_id

    @handle_exceptions(log_exception=True, report_exception=True)
    def analyze_change_effectiveness(self, change_id: str, effectiveness_metrics: dict) -> str:
        """Analyzes the effectiveness of implemented changes

        Args:
            change_id (str): ID of the change to analyze
            effectiveness_metrics (dict): Metrics describing the effectiveness of the change

        Returns:
            str: ID of the effectiveness record
        """
        # Retrieve change record
        change_record = self.get_change(change_id)
        if not change_record:
            raise ValueError(f"Change record not found: {change_id}")

        # Create effectiveness record with metrics
        effectiveness_id = generate_change_id("EFFECTIVENESS", change_id)
        effectiveness_record = {
            "effectiveness_id": effectiveness_id,
            "change_id": change_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "metrics": effectiveness_metrics,
        }

        # Store effectiveness record in Firestore
        self._firestore_client.create_document(EFFECTIVENESS_COLLECTION, effectiveness_id, effectiveness_record)

        # Update change record with effectiveness reference
        self.update_change_status(change_id, "COMPLETED", {"effectiveness_id": effectiveness_id})

        # Log effectiveness analysis activity
        logger.info(f"Analyzed effectiveness of change {change_id} with effectiveness ID {effectiveness_id}")

        # Return the effectiveness record ID
        return effectiveness_id

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_effectiveness_metrics(self, change_id: str) -> dict:
        """Retrieves effectiveness metrics for a specific change

        Args:
            change_id (str): ID of the change to retrieve effectiveness metrics for

        Returns:
            dict: Effectiveness metrics or None if not found
        """
        # Query effectiveness collection for records linked to change_id
        filters = [("change_id", "==", change_id)]
        effectiveness_records = self._firestore_client.query_documents(EFFECTIVENESS_COLLECTION, filters=filters, limit=1, order_by=[("timestamp", "DESCENDING")])

        # Return the most recent effectiveness record or None if not found
        if effectiveness_records:
            return effectiveness_records[0]["metrics"]
        else:
            logger.debug(f"No effectiveness metrics found for change {change_id}")
            return None

    @handle_exceptions(log_exception=True, report_exception=True)
    def get_effectiveness_summary(self, change_type: str, start_date: datetime.datetime, end_date: datetime.datetime) -> dict:
        """Generates a summary of effectiveness metrics by change type

        Args:
            change_type (str): Type of change to summarize
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering

        Returns:
            dict: Summary of effectiveness metrics
        """
        # Query effectiveness records for the specified period and type
        filters = [("change_type", "==", change_type), ("timestamp", ">=", start_date.isoformat()), ("timestamp", "<=", end_date.isoformat())]
        effectiveness_records = self._firestore_client.query_documents(EFFECTIVENESS_COLLECTION, filters=filters)

        # Calculate aggregate metrics (average improvement, success rate, etc.)
        total_improvement = 0
        successful_count = 0
        total_count = len(effectiveness_records)

        for record in effectiveness_records:
            metrics = record.get("metrics", {})
            improvement = metrics.get("improvement", 0)  # Example metric
            success = metrics.get("success", False)  # Example metric

            total_improvement += improvement
            if success:
                successful_count += 1

        # Generate summary statistics by category
        if total_count > 0:
            average_improvement = total_improvement / total_count
            success_rate = successful_count / total_count
        else:
            average_improvement = 0
            success_rate = 0

        # Return the effectiveness summary
        return {
            "change_type": change_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "average_improvement": average_improvement,
            "success_rate": success_rate,
            "total_count": total_count,
        }

    @handle_exceptions(log_exception=True, report_exception=True)
    def delete_change(self, change_id: str) -> bool:
        """Deletes a change record (for testing or data cleanup)

        Args:
            change_id (str): ID of the change to delete

        Returns:
            bool: True if deletion was successful
        """
        # Delete change record from Firestore
        self._firestore_client.delete_document(CHANGE_COLLECTION, change_id)

        # Delete any associated effectiveness records
        # TODO: Implement deletion of effectiveness records

        # Log deletion activity
        logger.info(f"Deleted change record: {change_id}")

        # Return success indicator
        return True

    @handle_exceptions(log_exception=True, report_exception=True)
    def ensure_collections_exist(self) -> bool:
        """Ensures required Firestore collections exist

        Returns:
            bool: True if collections exist or were created
        """
        # Check if change collection exists
        change_collection_exists = self._firestore_client.collection_exists(CHANGE_COLLECTION)

        # Check if effectiveness collection exists
        effectiveness_collection_exists = self._firestore_client.collection_exists(EFFECTIVENESS_COLLECTION)

        # Create collections if they don't exist
        if not change_collection_exists:
            self._firestore_client.create_collection(CHANGE_COLLECTION)
            logger.info(f"Created collection: {CHANGE_COLLECTION}")
        if not effectiveness_collection_exists:
            self._firestore_client.create_collection(EFFECTIVENESS_COLLECTION)
            logger.info(f"Created collection: {EFFECTIVENESS_COLLECTION}")

        # Return success indicator
        return True

    def record_metrics(self, metric_name: str, value: float, labels: dict = None) -> None:
        """Records metrics about change tracking operations

        Args:
            metric_name (str): Name of the metric to record
            value (float): Value of the metric
            labels (dict, optional): Labels to attach to the metric. Defaults to None.
        """
        # Format metric name with prefix
        metric_name = f"{CHANGE_METRIC_PREFIX}.{metric_name}"

        # Record metric using metric client
        self._metric_client.create_gauge_metric(metric_name, value, labels)

        # Log metric recording for debugging
        logger.debug(f"Recorded metric: {metric_name} with value {value} and labels {labels}")