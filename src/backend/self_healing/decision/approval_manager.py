"""
Implements the approval workflow management system for the self-healing AI engine.

This module handles the creation, tracking, and resolution of approval requests for healing
actions that require human intervention based on confidence scores, impact analysis, and
operational policies.
"""

import enum
import datetime
import typing
import uuid
from typing import Dict, List, Any, Optional, Union, Tuple

from ...constants import (
    HealingActionType,
    AlertSeverity,
    SelfHealingMode
)
from ...config import get_config
from ...utils.logging.logger import get_logger
from ..config.healing_config import get_approval_required
from ..config.risk_management import RiskManager, ImpactLevel
from ...utils.storage.firestore_client import FirestoreClient

# Configure logger
logger = get_logger(__name__)

# Collection name for approval requests in Firestore
APPROVAL_COLLECTION = "healing_approval_requests"

# Default values
DEFAULT_APPROVAL_EXPIRATION_HOURS = 24
DEFAULT_HIGH_IMPACT_THRESHOLD = 0.7
DEFAULT_CRITICAL_IMPACT_THRESHOLD = 0.9


class ApprovalStatus(enum.Enum):
    """Enumeration of possible approval statuses for healing actions."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class ApprovalRequest:
    """Data class representing an approval request for a healing action."""
    
    def __init__(
        self,
        action_id: str,
        action_type: HealingActionType,
        issue_id: str,
        issue_description: str,
        action_details: Dict[str, Any],
        confidence_score: float,
        impact_score: float,
        impact_level: ImpactLevel,
        requester: str,
        context: Dict[str, Any] = None,
        expiration_hours: int = DEFAULT_APPROVAL_EXPIRATION_HOURS,
        request_id: str = None
    ):
        """
        Initialize an approval request with required information.
        
        Args:
            action_id: Unique identifier for the healing action
            action_type: Type of healing action
            issue_id: Identifier for the issue being addressed
            issue_description: Human-readable description of the issue
            action_details: Details of the proposed healing action
            confidence_score: AI confidence score in the action (0.0-1.0)
            impact_score: Impact score of the action (0.0-1.0)
            impact_level: Categorization of impact severity
            requester: Identifier of the entity/service requesting approval
            context: Additional contextual information
            expiration_hours: Hours until the request expires
            request_id: Unique identifier for the request (generated if None)
        """
        self.request_id = request_id or str(uuid.uuid4())
        self.action_id = action_id
        self.action_type = action_type
        self.issue_id = issue_id
        self.issue_description = issue_description
        self.action_details = action_details or {}
        self.confidence_score = confidence_score
        self.impact_score = impact_score
        self.impact_level = impact_level
        self.status = ApprovalStatus.PENDING
        self.requester = requester
        self.approver = None
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()
        self.expires_at = self.created_at + datetime.timedelta(hours=expiration_hours)
        self.rejection_reason = None
        self.context = context or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert approval request to dictionary representation."""
        return {
            'request_id': self.request_id,
            'action_id': self.action_id,
            'action_type': self.action_type.value,
            'issue_id': self.issue_id,
            'issue_description': self.issue_description,
            'action_details': self.action_details,
            'confidence_score': self.confidence_score,
            'impact_score': self.impact_score,
            'impact_level': self.impact_level.value,
            'status': self.status.value,
            'requester': self.requester,
            'approver': self.approver,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'expires_at': self.expires_at.isoformat(),
            'rejection_reason': self.rejection_reason,
            'context': self.context
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ApprovalRequest':
        """Create ApprovalRequest from dictionary representation."""
        # Create an empty instance to fill in
        instance = cls(
            action_id=data.get('action_id', ''),
            action_type=HealingActionType(data.get('action_type')),
            issue_id=data.get('issue_id', ''),
            issue_description=data.get('issue_description', ''),
            action_details=data.get('action_details', {}),
            confidence_score=data.get('confidence_score', 0.0),
            impact_score=data.get('impact_score', 0.0),
            impact_level=ImpactLevel(data.get('impact_level')),
            requester=data.get('requester', ''),
            request_id=data.get('request_id')
        )
        
        # Set additional attributes
        instance.status = ApprovalStatus(data.get('status'))
        instance.approver = data.get('approver')
        instance.created_at = datetime.datetime.fromisoformat(data.get('created_at'))
        instance.updated_at = datetime.datetime.fromisoformat(data.get('updated_at'))
        instance.expires_at = datetime.datetime.fromisoformat(data.get('expires_at'))
        instance.rejection_reason = data.get('rejection_reason')
        instance.context = data.get('context', {})
        
        return instance
    
    def is_expired(self) -> bool:
        """Check if the approval request has expired."""
        return datetime.datetime.now() > self.expires_at
    
    def approve(self, approver: str) -> bool:
        """
        Approve the request.
        
        Args:
            approver: Identifier of the entity/person approving the request
            
        Returns:
            True if approval was successful, False otherwise
        """
        if self.status != ApprovalStatus.PENDING:
            logger.warning(f"Cannot approve request {self.request_id} with status {self.status}")
            return False
        
        if self.is_expired():
            logger.warning(f"Cannot approve expired request {self.request_id}")
            return False
        
        self.status = ApprovalStatus.APPROVED
        self.approver = approver
        self.updated_at = datetime.datetime.now()
        return True
    
    def reject(self, approver: str, reason: str) -> bool:
        """
        Reject the request with a reason.
        
        Args:
            approver: Identifier of the entity/person rejecting the request
            reason: Reason for rejection
            
        Returns:
            True if rejection was successful, False otherwise
        """
        if self.status != ApprovalStatus.PENDING:
            logger.warning(f"Cannot reject request {self.request_id} with status {self.status}")
            return False
        
        if self.is_expired():
            logger.warning(f"Cannot reject expired request {self.request_id}")
            return False
        
        self.status = ApprovalStatus.REJECTED
        self.approver = approver
        self.rejection_reason = reason
        self.updated_at = datetime.datetime.now()
        return True
    
    def expire(self) -> bool:
        """
        Mark the request as expired.
        
        Returns:
            True if expiration was successful, False otherwise
        """
        if self.status != ApprovalStatus.PENDING:
            logger.warning(f"Cannot expire request {self.request_id} with status {self.status}")
            return False
        
        self.status = ApprovalStatus.EXPIRED
        self.updated_at = datetime.datetime.now()
        return True


class ApprovalManager:
    """Manager class for handling approval workflows for healing actions."""
    
    def __init__(self, firestore_client: FirestoreClient, risk_manager: RiskManager, config: Dict[str, Any] = None):
        """
        Initialize the approval manager with configuration.
        
        Args:
            firestore_client: Client for Firestore database operations
            risk_manager: Manager for assessing risks and impacts
            config: Optional configuration overrides
        """
        self._firestore_client = firestore_client
        self._risk_manager = risk_manager
        self._config = get_config()
        
        # Apply any configuration overrides
        if config:
            for key, value in config.items():
                setattr(self, f"_{key}", value)
        
        # Get expiration hours from config
        self._expiration_hours = self._config.get(
            "self_healing.approval_expiration_hours", 
            DEFAULT_APPROVAL_EXPIRATION_HOURS
        )
        
        # Ensure approval collection exists
        self._firestore_client.ensure_collection(APPROVAL_COLLECTION)
        
        logger.info("ApprovalManager initialized")
    
    def requires_manual_approval(
        self,
        action_type: HealingActionType,
        confidence_score: float,
        impact_score: float,
        impact_level: ImpactLevel,
        context: Dict[str, Any] = None
    ) -> bool:
        """
        Determines if a healing action requires manual approval.
        
        Args:
            action_type: Type of healing action
            confidence_score: AI confidence score in the action (0.0-1.0)
            impact_score: Impact score of the action (0.0-1.0)
            impact_level: Categorization of impact severity
            context: Additional contextual information
            
        Returns:
            True if manual approval is required, False otherwise
        """
        # Default context if not provided
        context = context or {}
        
        # Get current self-healing mode
        healing_mode = self._config.get_self_healing_mode()
        
        # If the self-healing mode is DISABLED, always require approval
        if healing_mode == SelfHealingMode.DISABLED:
            logger.debug(f"Manual approval required: Self-healing is DISABLED")
            return True
        
        # If the self-healing mode is RECOMMENDATION_ONLY, always require approval
        if healing_mode == SelfHealingMode.RECOMMENDATION_ONLY:
            logger.debug(f"Manual approval required: Self-healing is in RECOMMENDATION_ONLY mode")
            return True
        
        # Get approval requirement setting for this action type
        approval_setting = get_approval_required(action_type)
        
        # Always require approval if configured to do so
        if approval_setting == "always":
            logger.debug(f"Manual approval required: {action_type.value} is configured to always require approval")
            return True
        
        # Never require approval if configured to do so
        if approval_setting == "never":
            logger.debug(f"No manual approval required: {action_type.value} is configured to never require approval")
            return False
        
        # Check impact-based approval requirements
        if approval_setting == "high_impact_only":
            # Require approval for high and critical impact
            if impact_level in [ImpactLevel.HIGH, ImpactLevel.CRITICAL]:
                logger.debug(f"Manual approval required: Impact level {impact_level.value} requires approval")
                return True
        elif approval_setting == "critical_only":
            # Require approval only for critical impact
            if impact_level == ImpactLevel.CRITICAL:
                logger.debug(f"Manual approval required: Critical impact level requires approval")
                return True
        
        # Consider confidence score
        if confidence_score < self._config.get_self_healing_confidence_threshold():
            logger.debug(f"Manual approval required: Confidence score {confidence_score} below threshold")
            return True
        
        # Consider business hours if in context
        if context.get("is_business_hours", False) and context.get("business_hours_require_approval", False):
            logger.debug(f"Manual approval required: Action during business hours")
            return True
        
        logger.debug(f"No manual approval required for {action_type.value} with impact {impact_level.value}")
        return False
    
    def create_approval_request(
        self,
        action_id: str,
        action_type: HealingActionType,
        issue_id: str,
        issue_description: str,
        action_details: Dict[str, Any],
        confidence_score: float,
        impact_score: float,
        impact_level: ImpactLevel,
        requester: str,
        context: Dict[str, Any] = None
    ) -> str:
        """
        Creates a new approval request for a healing action.
        
        Args:
            action_id: Unique identifier for the healing action
            action_type: Type of healing action
            issue_id: Identifier for the issue being addressed
            issue_description: Human-readable description of the issue
            action_details: Details of the proposed healing action
            confidence_score: AI confidence score in the action (0.0-1.0)
            impact_score: Impact score of the action (0.0-1.0)
            impact_level: Categorization of impact severity
            requester: Identifier of the entity/service requesting approval
            context: Additional contextual information
            
        Returns:
            ID of the created approval request
        """
        # Create approval request object
        request = ApprovalRequest(
            action_id=action_id,
            action_type=action_type,
            issue_id=issue_id,
            issue_description=issue_description,
            action_details=action_details,
            confidence_score=confidence_score,
            impact_score=impact_score,
            impact_level=impact_level,
            requester=requester,
            context=context or {},
            expiration_hours=self._expiration_hours
        )
        
        # Convert to dictionary for storage
        request_dict = request.to_dict()
        
        # Store in Firestore
        self._firestore_client.add_document(
            collection=APPROVAL_COLLECTION,
            document_id=request.request_id,
            data=request_dict
        )
        
        logger.info(f"Created approval request {request.request_id} for action {action_id} of type {action_type.value}")
        return request.request_id
    
    def get_approval_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """
        Retrieves an approval request by its ID.
        
        Args:
            request_id: ID of the approval request to retrieve
            
        Returns:
            ApprovalRequest object if found, None otherwise
        """
        # Retrieve document from Firestore
        doc = self._firestore_client.get_document(
            collection=APPROVAL_COLLECTION,
            document_id=request_id
        )
        
        if not doc:
            logger.warning(f"Approval request {request_id} not found")
            return None
        
        # Convert to ApprovalRequest object
        request = ApprovalRequest.from_dict(doc)
        
        # Check if request has expired but not marked as expired
        if request.status == ApprovalStatus.PENDING and request.is_expired():
            # Update status to expired
            request.expire()
            # Update in Firestore
            self._firestore_client.update_document(
                collection=APPROVAL_COLLECTION,
                document_id=request_id,
                data=request.to_dict()
            )
            logger.info(f"Updated expired approval request {request_id}")
        
        return request
    
    def get_pending_approval_requests(
        self,
        action_type: HealingActionType = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[ApprovalRequest]:
        """
        Retrieves all pending approval requests.
        
        Args:
            action_type: Optional filter by action type
            limit: Maximum number of requests to retrieve
            offset: Offset for pagination
            
        Returns:
            List of pending ApprovalRequest objects
        """
        # Build query filters
        filters = [("status", "==", ApprovalStatus.PENDING.value)]
        
        # Add action type filter if provided
        if action_type:
            filters.append(("action_type", "==", action_type.value))
        
        # Query Firestore with pagination
        docs = self._firestore_client.query_documents(
            collection=APPROVAL_COLLECTION,
            filters=filters,
            order_by=[("created_at", "desc")],
            limit=limit,
            offset=offset
        )
        
        # Convert to ApprovalRequest objects
        requests = [ApprovalRequest.from_dict(doc) for doc in docs]
        
        # Filter out any requests that have expired
        pending_requests = []
        expired_requests = []
        
        for request in requests:
            if request.is_expired():
                # Mark as expired and collect for batch update
                request.expire()
                expired_requests.append(request)
            else:
                pending_requests.append(request)
        
        # Update expired requests in database if any found
        if expired_requests:
            batch_updates = {
                request.request_id: request.to_dict() for request in expired_requests
            }
            self._firestore_client.batch_update(
                collection=APPROVAL_COLLECTION,
                updates=batch_updates
            )
            logger.info(f"Updated {len(expired_requests)} expired approval requests")
        
        return pending_requests
    
    def approve_request(self, request_id: str, approver: str) -> bool:
        """
        Approves a pending approval request.
        
        Args:
            request_id: ID of the approval request to approve
            approver: Identifier of the entity/person approving the request
            
        Returns:
            True if approval was successful, False otherwise
        """
        # Get request by ID
        request = self.get_approval_request(request_id)
        
        if not request:
            logger.warning(f"Cannot approve non-existent request {request_id}")
            return False
        
        if request.status != ApprovalStatus.PENDING:
            logger.warning(f"Cannot approve request {request_id} with status {request.status}")
            return False
        
        # Approve the request
        if not request.approve(approver):
            return False
        
        # Update in Firestore
        self._firestore_client.update_document(
            collection=APPROVAL_COLLECTION,
            document_id=request_id,
            data=request.to_dict()
        )
        
        logger.info(f"Approval request {request_id} approved by {approver}")
        return True
    
    def reject_request(self, request_id: str, approver: str, reason: str) -> bool:
        """
        Rejects a pending approval request.
        
        Args:
            request_id: ID of the approval request to reject
            approver: Identifier of the entity/person rejecting the request
            reason: Reason for rejection
            
        Returns:
            True if rejection was successful, False otherwise
        """
        # Get request by ID
        request = self.get_approval_request(request_id)
        
        if not request:
            logger.warning(f"Cannot reject non-existent request {request_id}")
            return False
        
        if request.status != ApprovalStatus.PENDING:
            logger.warning(f"Cannot reject request {request_id} with status {request.status}")
            return False
        
        # Reject the request
        if not request.reject(approver, reason):
            return False
        
        # Update in Firestore
        self._firestore_client.update_document(
            collection=APPROVAL_COLLECTION,
            document_id=request_id,
            data=request.to_dict()
        )
        
        logger.info(f"Approval request {request_id} rejected by {approver} with reason: {reason}")
        return True
    
    def check_request_status(self, request_id: str) -> Optional[ApprovalStatus]:
        """
        Checks the current status of an approval request.
        
        Args:
            request_id: ID of the approval request to check
            
        Returns:
            Current status of the request or None if not found
        """
        request = self.get_approval_request(request_id)
        
        if not request:
            return None
        
        return request.status
    
    def cleanup_expired_requests(self) -> int:
        """
        Updates the status of all expired approval requests.
        
        Returns:
            Number of expired requests updated
        """
        # Calculate current time
        now = datetime.datetime.now()
        
        # Query for pending requests with expires_at < now
        filters = [
            ("status", "==", ApprovalStatus.PENDING.value),
            ("expires_at", "<", now.isoformat())
        ]
        
        # Get expired requests
        docs = self._firestore_client.query_documents(
            collection=APPROVAL_COLLECTION,
            filters=filters
        )
        
        if not docs:
            return 0
        
        # Update each expired request
        batch_updates = {}
        count = 0
        
        for doc in docs:
            request = ApprovalRequest.from_dict(doc)
            if request.expire():
                batch_updates[request.request_id] = request.to_dict()
                count += 1
        
        # Perform batch update
        if batch_updates:
            self._firestore_client.batch_update(
                collection=APPROVAL_COLLECTION,
                updates=batch_updates
            )
        
        logger.info(f"Updated {count} expired approval requests")
        return count
    
    def get_approval_requests_by_action(
        self,
        action_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[ApprovalRequest]:
        """
        Retrieves approval requests for a specific action.
        
        Args:
            action_id: ID of the action to get requests for
            limit: Maximum number of requests to retrieve
            offset: Offset for pagination
            
        Returns:
            List of ApprovalRequest objects for the action
        """
        # Query Firestore for requests with matching action_id
        docs = self._firestore_client.query_documents(
            collection=APPROVAL_COLLECTION,
            filters=[("action_id", "==", action_id)],
            order_by=[("created_at", "desc")],
            limit=limit,
            offset=offset
        )
        
        # Convert to ApprovalRequest objects
        return [ApprovalRequest.from_dict(doc) for doc in docs]
    
    def get_approval_requests_by_issue(
        self,
        issue_id: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[ApprovalRequest]:
        """
        Retrieves approval requests for a specific issue.
        
        Args:
            issue_id: ID of the issue to get requests for
            limit: Maximum number of requests to retrieve
            offset: Offset for pagination
            
        Returns:
            List of ApprovalRequest objects for the issue
        """
        # Query Firestore for requests with matching issue_id
        docs = self._firestore_client.query_documents(
            collection=APPROVAL_COLLECTION,
            filters=[("issue_id", "==", issue_id)],
            order_by=[("created_at", "desc")],
            limit=limit,
            offset=offset
        )
        
        # Convert to ApprovalRequest objects
        return [ApprovalRequest.from_dict(doc) for doc in docs]
    
    def get_approval_statistics(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Gets statistics about approval requests.
        
        Args:
            days_back: Number of days to look back for statistics
            
        Returns:
            Dictionary with approval statistics
        """
        # Calculate start date
        start_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
        
        # Query for requests created after start date
        docs = self._firestore_client.query_documents(
            collection=APPROVAL_COLLECTION,
            filters=[("created_at", ">=", start_date.isoformat())],
            order_by=[("created_at", "desc")]
        )
        
        # Initialize counters
        stats = {
            "total": len(docs),
            "pending": 0,
            "approved": 0,
            "rejected": 0,
            "expired": 0,
            "avg_response_time_hours": 0,
            "approval_rate": 0,
            "by_action_type": {},
            "by_impact_level": {}
        }
        
        # Process each request
        response_times = []
        action_type_counts = {}
        impact_level_counts = {}
        
        for doc in docs:
            status = doc.get("status")
            
            # Count by status
            if status == ApprovalStatus.PENDING.value:
                stats["pending"] += 1
            elif status == ApprovalStatus.APPROVED.value:
                stats["approved"] += 1
                # Calculate response time for approved requests
                created_at = datetime.datetime.fromisoformat(doc.get("created_at"))
                updated_at = datetime.datetime.fromisoformat(doc.get("updated_at"))
                response_time = (updated_at - created_at).total_seconds() / 3600  # hours
                response_times.append(response_time)
            elif status == ApprovalStatus.REJECTED.value:
                stats["rejected"] += 1
                # Also get response time for rejected requests
                created_at = datetime.datetime.fromisoformat(doc.get("created_at"))
                updated_at = datetime.datetime.fromisoformat(doc.get("updated_at"))
                response_time = (updated_at - created_at).total_seconds() / 3600  # hours
                response_times.append(response_time)
            elif status == ApprovalStatus.EXPIRED.value:
                stats["expired"] += 1
            
            # Count by action type
            action_type = doc.get("action_type")
            if action_type:
                action_type_counts[action_type] = action_type_counts.get(action_type, 0) + 1
            
            # Count by impact level
            impact_level = doc.get("impact_level")
            if impact_level:
                impact_level_counts[impact_level] = impact_level_counts.get(impact_level, 0) + 1
        
        # Calculate average response time
        if response_times:
            stats["avg_response_time_hours"] = sum(response_times) / len(response_times)
        
        # Calculate approval rate
        decided = stats["approved"] + stats["rejected"]
        if decided > 0:
            stats["approval_rate"] = stats["approved"] / decided
        
        # Add counts by action type and impact level
        stats["by_action_type"] = action_type_counts
        stats["by_impact_level"] = impact_level_counts
        
        return stats
    
    def reload_config(self) -> bool:
        """
        Reloads configuration settings.
        
        Returns:
            Success status
        """
        try:
            # Reload application config
            self._config = get_config()
            
            # Update expiration hours
            self._expiration_hours = self._config.get(
                "self_healing.approval_expiration_hours", 
                DEFAULT_APPROVAL_EXPIRATION_HOURS
            )
            
            logger.info("ApprovalManager configuration reloaded")
            return True
        except Exception as e:
            logger.error(f"Error reloading ApprovalManager configuration: {e}")
            return False