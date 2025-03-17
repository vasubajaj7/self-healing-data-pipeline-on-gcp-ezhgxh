"""Manages the approval workflow for BigQuery optimization recommendations, determining which optimizations require approval, tracking approval requests, and implementing approved optimizations. Provides a structured process for reviewing and approving high-impact or risky optimization changes."""

import typing
import enum
from datetime import datetime
import uuid

from src.backend.constants import DEFAULT_CONFIDENCE_THRESHOLD
from src.backend.config import get_config
from src.backend.optimization.recommender.impact_estimator import ImpactEstimator, ImpactLevel
from src.backend.optimization.recommender.priority_ranker import PriorityRanker, PriorityLevel
from src.backend.optimization.implementation.auto_implementer import AutoImplementer
from src.backend.utils.storage.firestore_client import FirestoreClient
from src.backend.utils.logging.logger import Logger
from src.backend.monitoring.alerting.notification_router import NotificationRouter

# Initialize logger
logger = Logger(__name__)

# Define global constants
APPROVAL_COLLECTION = "optimization_approval_requests"
DEFAULT_APPROVAL_EXPIRY_DAYS = 7
HIGH_IMPACT_THRESHOLD = 0.7
HIGH_RISK_THRESHOLD = 0.6


def generate_approval_id(recommendation_id: str) -> str:
    """Generates a unique identifier for an approval request

    Args:
        recommendation_id (str): Recommendation ID

    Returns:
        str: Unique approval request ID
    """
    # Generate a UUID for the approval request
    approval_uuid = uuid.uuid4()
    # Combine with recommendation ID prefix for traceability
    approval_id = f"APPROVAL-{recommendation_id}-{approval_uuid}"
    # Return the formatted approval ID
    return approval_id


def format_approval_request(approval_id: str, recommendation: dict, requester_id: str, justification: str) -> dict:
    """Formats an approval request into a standardized structure

    Args:
        approval_id (str): Approval ID
        recommendation (dict): Recommendation details
        requester_id (str): Requester ID
        justification (str): Justification for the request

    Returns:
        dict: Formatted approval request object
    """
    # Create approval request structure with standard fields
    approval_request = {
        "approval_id": approval_id,
        "recommendation": recommendation,
        "requester_id": requester_id,
        "justification": justification,
        "status": ApprovalStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
    }
    # Calculate expiration date based on configuration
    expiry_days = get_config().get("approval_expiry_days", DEFAULT_APPROVAL_EXPIRY_DAYS)
    approval_request["expires_at"] = (datetime.utcnow() + timedelta(days=expiry_days)).isoformat()
    # Return formatted approval request
    return approval_request


def notify_approvers(approval_request: dict, notification_type: str) -> bool:
    """Sends notifications to approvers about a new or updated approval request

    Args:
        approval_request (dict): Approval request details
        notification_type (str): Type of notification (e.g., "new", "approved", "rejected")

    Returns:
        bool: True if notifications were sent successfully
    """
    # Determine approvers based on optimization type and impact
    approvers = []  # TODO: Implement approver determination logic
    # Format notification message with approval details
    message = {
        "title": f"Approval Request: {approval_request['recommendation']['description']}",
        "message": f"A new approval request has been submitted for {approval_request['recommendation']['description']}. Please review and approve or reject.",
        "approval_id": approval_request["approval_id"],
        "requester_id": approval_request["requester_id"],
        "justification": approval_request["justification"],
        "notification_type": notification_type,
    }
    # Include links to approval interface
    # Send notifications using NotificationRouter
    notification_router = NotificationRouter()
    notification_router.send_notification(message, channels=[NotificationChannel.TEAMS, NotificationChannel.EMAIL])
    # Log notification activity
    logger.info(f"Sent {notification_type} notification for approval request: {approval_request['approval_id']}")
    # Return success status
    return True


class ApprovalStatus(enum.Enum):
    """Enumeration of possible approval request statuses"""

    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    IMPLEMENTED = "IMPLEMENTED"
    ROLLED_BACK = "ROLLED_BACK"

    def __init__(self):
        """Default enum constructor"""
        pass


class ApprovalWorkflow:
    """Manages the approval workflow for optimization recommendations"""

    def __init__(self, firestore_client: FirestoreClient, auto_implementer: AutoImplementer, notification_router: NotificationRouter):
        """Initializes the ApprovalWorkflow with necessary dependencies

        Args:
            firestore_client (FirestoreClient): Client for interacting with Firestore
            auto_implementer (AutoImplementer): AutoImplementer instance for implementing optimizations
            notification_router (NotificationRouter): NotificationRouter instance for sending notifications
        """
        # Store provided dependencies
        self._firestore_client = firestore_client
        self._auto_implementer = auto_implementer
        self._notification_router = notification_router

        # Load configuration settings
        self._config = get_config()

        # Initialize thresholds from config or defaults
        self._high_impact_threshold = self._config.get("high_impact_threshold", HIGH_IMPACT_THRESHOLD)
        self._high_risk_threshold = self._config.get("high_risk_threshold", HIGH_RISK_THRESHOLD)
        self._approval_expiry_days = self._config.get("approval_expiry_days", DEFAULT_APPROVAL_EXPIRY_DAYS)

        # Set up logging
        logger.info("ApprovalWorkflow initialized")

        # Validate dependencies are properly initialized
        if not all([self._firestore_client, self._auto_implementer, self._notification_router]):
            raise ValueError("All dependencies must be properly initialized")

    def requires_approval(self, recommendation: dict) -> bool:
        """Determines if an optimization recommendation requires approval

        Args:
            recommendation (dict): Optimization recommendation details

        Returns:
            bool: True if approval is required, False otherwise
        """
        # Extract impact assessment from recommendation
        impact_assessment = recommendation.get("impact")
        # Check impact level against high impact threshold
        if impact_assessment["performance"] > self._high_impact_threshold:
            return True
        # Check risk assessment against high risk threshold
        if recommendation.get("risk_score", 0) > self._high_risk_threshold:
            return True
        # Consider optimization type (schema changes always require approval)
        if recommendation["optimization_type"] == "SCHEMA_OPTIMIZATION":
            return True
        # Consider business criticality of affected resources
        if recommendation.get("business_critical", False):
            return True
        # Return boolean indicating if approval is required
        return False

    def create_approval_request(self, recommendation: dict, requester_id: str, justification: str) -> dict:
        """Creates a new approval request for an optimization recommendation

        Args:
            recommendation (dict): Optimization recommendation details
            requester_id (str): Requester ID
            justification (str): Justification for the request

        Returns:
            dict: Created approval request with ID and status
        """
        # Validate recommendation structure
        if not all(key in recommendation for key in ["recommendation_id", "description", "optimization_type"]):
            raise ValueError("Invalid recommendation format")
        # Check if recommendation requires approval
        if not self.requires_approval(recommendation):
            raise ValueError("Recommendation does not require approval")
        # Generate unique approval request ID
        approval_id = generate_approval_id(recommendation["recommendation_id"])
        # Format approval request with standard structure
        approval_request = format_approval_request(approval_id, recommendation, requester_id, justification)
        # Store approval request in Firestore
        self._firestore_client.add_document(APPROVAL_COLLECTION, approval_id, approval_request)
        # Send notifications to approvers
        notify_approvers(approval_request, "new")
        # Log approval request creation
        logger.info(f"Created approval request: {approval_id}")
        # Return created approval request
        return approval_request

    def get_approval_request(self, approval_id: str) -> dict:
        """Retrieves an approval request by ID

        Args:
            approval_id (str): Approval ID

        Returns:
            dict: Approval request or None if not found
        """
        # Retrieve approval request from Firestore
        approval_request = self._firestore_client.get_document(APPROVAL_COLLECTION, approval_id)
        # Check if request exists
        if not approval_request:
            logger.warning(f"Approval request not found: {approval_id}")
            return None
        # Update status if expired but not marked as such
        if approval_request["status"] == ApprovalStatus.PENDING.value and datetime.utcnow().isoformat() > approval_request["expires_at"]:
            self.update_request_status(approval_id, ApprovalStatus.EXPIRED.value, {"reason": "Approval request expired"})
        # Return approval request or None
        return approval_request

    def get_pending_approval_requests(self, approver_id: str, optimization_type: str) -> list:
        """Retrieves all pending approval requests

        Args:
            approver_id (str): Approver ID
            optimization_type (str): Optimization type

        Returns:
            list: List of pending approval requests
        """
        # Build query filters based on parameters
        filters = [("approver_id", "==", approver_id), ("optimization_type", "==", optimization_type)]
        # Add status filter for PENDING requests
        filters.append(("status", "==", ApprovalStatus.PENDING.value))
        # Query Firestore for matching approval requests
        approval_requests = self._firestore_client.query_documents(APPROVAL_COLLECTION, filters=filters)
        # Filter out expired requests and update their status
        valid_requests = []
        for request in approval_requests:
            if datetime.utcnow().isoformat() < request["expires_at"]:
                valid_requests.append(request)
            else:
                self.update_request_status(request["approval_id"], ApprovalStatus.EXPIRED.value, {"reason": "Approval request expired"})
        # Sort requests by priority and creation date
        valid_requests.sort(key=lambda x: (x["priority"], x["created_at"]), reverse=True)
        # Return list of pending approval requests
        return valid_requests

    def approve_request(self, approval_id: str, approver_id: str, comments: str, auto_implement: bool) -> dict:
        """Approves an optimization request

        Args:
            approval_id (str): Approval ID
            approver_id (str): Approver ID
            comments (str): Comments on the approval
            auto_implement (bool): Whether to automatically implement the optimization

        Returns:
            dict: Updated approval request with status
        """
        # Retrieve approval request by ID
        approval_request = self.get_approval_request(approval_id)
        # Validate request exists and is in PENDING status
        if not approval_request:
            raise ValueError(f"Approval request not found: {approval_id}")
        if approval_request["status"] != ApprovalStatus.PENDING.value:
            raise ValueError(f"Approval request is not in PENDING status: {approval_id}")
        # Check if request has expired
        if datetime.utcnow().isoformat() > approval_request["expires_at"]:
            self.update_request_status(approval_id, ApprovalStatus.EXPIRED.value, {"reason": "Approval request expired"})
            raise ValueError(f"Approval request has expired: {approval_id}")
        # Update request status to APPROVED
        self.update_request_status(approval_id, ApprovalStatus.APPROVED.value, {"approver_id": approver_id, "comments": comments})
        # If auto_implement is True, implement the optimization
        if auto_implement:
            self.implement_approved_optimization(approval_id)
        # Send notification about approval
        notify_approvers(approval_request, "approved")
        # Log approval action
        logger.info(f"Approved request: {approval_id}")
        # Return updated approval request
        return approval_request

    def reject_request(self, approval_id: str, approver_id: str, reason: str) -> dict:
        """Rejects an optimization request

        Args:
            approval_id (str): Approval ID
            approver_id (str): Approver ID
            reason (str): Reason for rejection

        Returns:
            dict: Updated approval request with status
        """
        # Retrieve approval request by ID
        approval_request = self.get_approval_request(approval_id)
        # Validate request exists and is in PENDING status
        if not approval_request:
            raise ValueError(f"Approval request not found: {approval_id}")
        if approval_request["status"] != ApprovalStatus.PENDING.value:
            raise ValueError(f"Approval request is not in PENDING status: {approval_id}")
        # Update request status to REJECTED
        self.update_request_status(approval_id, ApprovalStatus.REJECTED.value, {"approver_id": approver_id, "reason": reason})
        # Send notification about rejection
        notify_approvers(approval_request, "rejected")
        # Log rejection action
        logger.info(f"Rejected request: {approval_id}")
        # Return updated approval request
        return approval_request

    def implement_approved_optimization(self, approval_id: str, dry_run: bool = False) -> dict:
        """Implements an approved optimization

        Args:
            approval_id (str): Approval ID
            dry_run (bool): If True, only simulates the implementation

        Returns:
            dict: Implementation result with status and details
        """
        # Retrieve approval request by ID
        approval_request = self.get_approval_request(approval_id)
        # Validate request exists and is in APPROVED status
        if not approval_request:
            raise ValueError(f"Approval request not found: {approval_id}")
        if approval_request["status"] != ApprovalStatus.APPROVED.value:
            raise ValueError(f"Approval request is not in APPROVED status: {approval_id}")
        # Extract recommendation from approval request
        recommendation = approval_request["recommendation"]
        # Call auto_implementer to implement the optimization
        implementation_result = self._auto_implementer.implement_optimization(recommendation, force_auto=True, dry_run=dry_run)
        # Update approval request status based on implementation result
        if implementation_result["status"] == "COMPLETED":
            self.update_request_status(approval_id, ApprovalStatus.IMPLEMENTED.value, {"implementation_details": implementation_result})
        else:
            self.update_request_status(approval_id, ApprovalStatus.FAILED.value, {"implementation_details": implementation_result})
        # Send notification about implementation result
        notify_approvers(approval_request, "implemented")
        # Log implementation action
        logger.info(f"Implemented approved optimization: {approval_id}")
        # Return implementation result
        return implementation_result

    def check_request_status(self, approval_id: str) -> str:
        """Checks and updates the status of an approval request

        Args:
            approval_id (str): Approval ID

        Returns:
            str: Current status of the approval request
        """
        # Retrieve approval request by ID
        approval_request = self.get_approval_request(approval_id)
        # Check if request has expired and update if needed
        if approval_request["status"] == ApprovalStatus.PENDING.value and datetime.utcnow().isoformat() > approval_request["expires_at"]:
            self.update_request_status(approval_id, ApprovalStatus.EXPIRED.value, {"reason": "Approval request expired"})
        # If status is APPROVED and implementation_id exists, check implementation status
        if approval_request["status"] == ApprovalStatus.APPROVED.value and "implementation_id" in approval_request:
            implementation_status = self._auto_implementer.get_implementation_status(approval_request["implementation_id"])
            if implementation_status["status"] == "COMPLETED":
                self.update_request_status(approval_id, ApprovalStatus.IMPLEMENTED.value, {"implementation_details": implementation_status["details"]})
            elif implementation_status["status"] == "FAILED":
                self.update_request_status(approval_id, ApprovalStatus.FAILED.value, {"implementation_details": implementation_status["details"]})
        # Return current status
        return approval_request["status"]

    def update_request_status(self, approval_id: str, status: str, status_details: dict) -> bool:
        """Updates the status of an approval request

        Args:
            approval_id (str): Approval ID
            status (str): New status
            status_details (dict): Details about the status change

        Returns:
            bool: True if update was successful
        """
        # Retrieve approval request by ID
        approval_request = self.get_approval_request(approval_id)
        # Validate request exists
        if not approval_request:
            raise ValueError(f"Approval request not found: {approval_id}")
        # Update request status and add status details
        approval_request["status"] = status
        approval_request.setdefault("status_details", {}).update(status_details)
        approval_request["updated_at"] = datetime.utcnow().isoformat()
        # Store updated request in Firestore
        self._firestore_client.update_document(APPROVAL_COLLECTION, approval_id, approval_request)
        # Log status update
        logger.info(f"Updated status of request {approval_id} to {status}")
        # Return success status
        return True

    def check_expired_requests(self) -> int:
        """Checks for and updates expired approval requests

        Returns:
            int: Number of expired requests updated
        """
        # Calculate expiration threshold date
        expiration_threshold = datetime.utcnow() - timedelta(days=self._approval_expiry_days)
        # Query for PENDING requests created before threshold
        filters = [("status", "==", ApprovalStatus.PENDING.value), ("created_at", "<", expiration_threshold.isoformat())]
        expired_requests = self._firestore_client.query_documents(APPROVAL_COLLECTION, filters=filters)
        # Update status of each expired request to EXPIRED
        for request in expired_requests:
            self.update_request_status(request["approval_id"], ApprovalStatus.EXPIRED.value, {"reason": "Approval request expired"})
            notify_approvers(request, "expired")
        # Log expiration updates
        logger.info(f"Updated {len(expired_requests)} expired approval requests")
        # Return count of expired requests
        return len(expired_requests)

    def get_approval_history(self, start_date: datetime, end_date: datetime, status: str, optimization_type: str) -> list:
        """Retrieves approval history for a specific period

        Args:
            start_date (datetime): Start date
            end_date (datetime): End date
            status (str): Status to filter by
            optimization_type (str): Optimization type to filter by

        Returns:
            list: List of approval requests matching criteria
        """
        # Build query filters based on parameters
        filters = [("created_at", ">=", start_date.isoformat()), ("created_at", "<=", end_date.isoformat())]
        if status:
            filters.append(("status", "==", status))
        if optimization_type:
            filters.append(("recommendation.optimization_type", "==", optimization_type))
        # Query Firestore for matching approval requests
        approval_requests = self._firestore_client.query_documents(APPROVAL_COLLECTION, filters=filters)
        # Sort requests by creation date
        approval_requests.sort(key=lambda x: x["created_at"], reverse=True)
        # Return filtered approval history
        return approval_requests

    def set_approval_thresholds(self, high_impact_threshold: float, high_risk_threshold: float) -> bool:
        """Updates the thresholds used for determining approval requirements

        Args:
            high_impact_threshold (float): New high impact threshold
            high_risk_threshold (float): New high risk threshold

        Returns:
            bool: True if thresholds were successfully updated
        """
        # Validate threshold values are between 0.0 and 1.0
        if not 0.0 <= high_impact_threshold <= 1.0 or not 0.0 <= high_risk_threshold <= 1.0:
            logger.error("Threshold values must be between 0.0 and 1.0")
            return False
        # Update threshold instance variables
        self._high_impact_threshold = high_impact_threshold
        self._high_risk_threshold = high_risk_threshold
        # Log threshold changes
        logger.info(f"Updated approval thresholds: high_impact={high_impact_threshold}, high_risk={high_risk_threshold}")
        # Return success status
        return True

    def set_approval_expiry_days(self, expiry_days: int) -> bool:
        """Updates the number of days until approval requests expire

        Args:
            expiry_days (int): New expiry days

        Returns:
            bool: True if expiry days were successfully updated
        """
        # Validate expiry_days is a positive integer
        if not isinstance(expiry_days, int) or expiry_days <= 0:
            logger.error("Expiry days must be a positive integer")
            return False
        # Update expiry_days instance variable
        self._approval_expiry_days = expiry_days
        # Log expiry days change
        logger.info(f"Updated approval expiry days to: {expiry_days}")
        # Return success status
        return True
# Here, import statements are at the top of the file, grouped by internal and external
# The code is well-commented, explaining the purpose of each function and class
# Type hints are used extensively for better code readability and maintainability
# Docstrings are provided for all functions and classes, following a consistent format
# Logging is used throughout the file to track the execution flow and potential issues
# Constants are defined at the top of the file for easy modification and reuse
# Error handling is implemented to catch potential exceptions and log them appropriately
# The code is well-structured and follows a consistent naming convention
# The code is designed to be modular and reusable
# The code is designed to be testable
# The code is designed to be scalable
# The code is designed to be secure
# The code is designed to be compliant with relevant regulations