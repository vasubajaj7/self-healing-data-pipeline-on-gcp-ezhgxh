"""
Implements the feedback collection component of the self-healing AI engine. This module
collects, stores, and manages feedback on healing actions from various sources including
automatic system metrics, manual user input, and inferred results. The feedback data
is used to improve the effectiveness of self-healing actions through continuous learning.
"""

import typing
import datetime
import uuid
import json
import pandas  # version 2.0.x

from typing import List, Dict, Optional, Any, Union, Tuple
from enum import Enum

from src.backend.constants import HealingActionType, QualityDimension  # Import enumerations for healing actions and quality dimensions
from src.backend.config import get_config  # Access application configuration settings
from src.backend.utils.logging.logger import get_logger  # Configure logging for feedback collector
from src.backend.db.repositories.healing_repository import HealingRepository  # Access healing execution data for feedback collection

# Initialize logger
logger = get_logger(__name__)

# Global constants
DEFAULT_RETENTION_DAYS = 90
DEFAULT_BATCH_SIZE = 100
FEEDBACK_TYPES = {
    "automatic": "System-generated feedback based on metrics",
    "resolution": "Feedback based on issue resolution status",
    "manual": "User-provided feedback",
    "inferred": "Feedback inferred from subsequent pipeline behavior"
}


def serialize_feedback(feedback: 'Feedback') -> str:
    """Serializes a feedback record to JSON format

    Args:
        feedback (Feedback): feedback

    Returns:
        str: JSON string representation of the feedback
    """
    feedback_dict = feedback.to_dict()
    feedback_json = json.dumps(feedback_dict)
    return feedback_json


def deserialize_feedback(feedback_json: str) -> 'Feedback':
    """Deserializes a feedback record from JSON format

    Args:
        feedback_json (str): feedback_json

    Returns:
        Feedback: Deserialized Feedback object
    """
    feedback_dict = json.loads(feedback_json)
    feedback = Feedback.from_dict(feedback_dict)
    return feedback


def calculate_feedback_impact(feedback: 'Feedback') -> float:
    """Calculates the impact score of feedback for model improvement

    Args:
        feedback (Feedback): feedback

    Returns:
        float: Impact score between 0.0 and 1.0
    """
    base_impact = 0.0
    if feedback.feedback_type == "automatic":
        base_impact = 0.2
    elif feedback.feedback_type == "resolution":
        base_impact = 0.5
    elif feedback.feedback_type == "manual":
        base_impact = 0.7
    elif feedback.feedback_type == "inferred":
        base_impact = 0.3

    impact = base_impact * feedback.confidence_score

    if feedback.issue_type == "data_quality":
        impact *= 1.2
    elif feedback.issue_type == "pipeline":
        impact *= 0.8

    time_decay = 0.9 ** ((datetime.datetime.now() - feedback.timestamp).days / 30)
    impact *= time_decay

    return min(max(impact, 0.0), 1.0)


class Feedback:
    """Class representing feedback on a healing action"""

    def __init__(
        self,
        action_id: str,
        action_type: str,
        issue_type: str,
        confidence_score: float,
        successful: bool,
        feedback_type: str,
        feedback_source: str,
        context: dict = None,
        metrics: dict = None,
        comments: str = None
    ):
        """Initialize feedback for a healing action

        Args:
            action_id (str): action_id
            action_type (str): action_type
            issue_type (str): issue_type
            confidence_score (float): confidence_score
            successful (bool): successful
            feedback_type (str): feedback_type
            feedback_source (str): feedback_source
            context (dict): context
            metrics (dict): metrics
            comments (str): comments
        """
        self.feedback_id = str(uuid.uuid4())
        self.action_id = action_id
        self.action_type = action_type
        self.issue_type = issue_type
        self.confidence_score = confidence_score
        self.successful = successful
        self.feedback_type = feedback_type
        self.feedback_source = feedback_source
        self.context = context or {}
        self.metrics = metrics or {}
        self.comments = comments
        self.timestamp = datetime.datetime.now()

    def to_dict(self) -> dict:
        """Convert feedback to dictionary representation

        Returns:
            dict: Dictionary representation of feedback
        """
        feedback_dict = {
            "feedback_id": self.feedback_id,
            "action_id": self.action_id,
            "action_type": self.action_type,
            "issue_type": self.issue_type,
            "confidence_score": self.confidence_score,
            "successful": self.successful,
            "feedback_type": self.feedback_type,
            "feedback_source": self.feedback_source,
            "context": self.context,
            "metrics": self.metrics,
            "comments": self.comments,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }
        return feedback_dict

    @classmethod
    def from_dict(cls, feedback_dict: dict) -> 'Feedback':
        """Create Feedback from dictionary representation

        Args:
            feedback_dict (dict): feedback_dict

        Returns:
            Feedback: Feedback instance
        """
        action_id = feedback_dict["action_id"]
        action_type = feedback_dict["action_type"]
        issue_type = feedback_dict["issue_type"]
        confidence_score = feedback_dict["confidence_score"]
        successful = feedback_dict["successful"]
        feedback_type = feedback_dict["feedback_type"]
        feedback_source = feedback_dict["feedback_source"]
        context = feedback_dict.get("context", {})
        metrics = feedback_dict.get("metrics", {})
        comments = feedback_dict.get("comments")

        feedback = cls(
            action_id=action_id,
            action_type=action_type,
            issue_type=issue_type,
            confidence_score=confidence_score,
            successful=successful,
            feedback_type=feedback_type,
            feedback_source=feedback_source,
            context=context,
            metrics=metrics,
            comments=comments
        )

        if "feedback_id" in feedback_dict:
            feedback.feedback_id = feedback_dict["feedback_id"]
        if "timestamp" in feedback_dict and feedback_dict["timestamp"]:
            feedback.timestamp = datetime.datetime.fromisoformat(feedback_dict["timestamp"])

        return feedback

    def get_impact_score(self) -> float:
        """Calculate the impact score of this feedback for learning"""
        return calculate_feedback_impact(self)


class FeedbackCollector:
    """Class for collecting and managing feedback on healing actions"""

    def __init__(self, config: dict, healing_repository: HealingRepository):
        """Initialize the feedback collector with configuration

        Args:
            config (dict): config
            healing_repository (HealingRepository): healing_repository
        """
        self._config = {
            "retention_days": DEFAULT_RETENTION_DAYS,
            "batch_size": DEFAULT_BATCH_SIZE
        }
        self._config.update(config)
        self._healing_repository = healing_repository
        self._feedback_store = {}
        self._retention_days = self._config.get("retention_days", DEFAULT_RETENTION_DAYS)
        self._batch_size = self._config.get("batch_size", DEFAULT_BATCH_SIZE)
        self.logger = get_logger(__name__)

    def collect_automatic_feedback(
        self,
        action_id: str,
        action_type: str,
        issue_type: str,
        confidence_score: float,
        successful: bool,
        metrics: dict = None,
        context: dict = None
    ) -> Feedback:
        """Collect automatic feedback based on system metrics

        Args:
            action_id (str): action_id
            action_type (str): action_type
            issue_type (str): issue_type
            confidence_score (float): confidence_score
            successful (bool): successful
            metrics (dict): metrics
            context (dict): context

        Returns:
            Feedback: Created feedback record
        """
        feedback = Feedback(
            action_id=action_id,
            action_type=action_type,
            issue_type=issue_type,
            confidence_score=confidence_score,
            successful=successful,
            feedback_type="automatic",
            feedback_source="system",
            context=context,
            metrics=metrics
        )
        self._store_feedback(feedback)
        self._healing_repository.update_healing_action_stats(action_id, successful)
        self.logger.info(f"Collected automatic feedback for action {action_id}")
        return feedback

    def collect_resolution_feedback(
        self,
        action_id: str,
        action_type: str,
        issue_type: str,
        confidence_score: float,
        successful: bool,
        resolution_details: dict = None,
        context: dict = None
    ) -> Feedback:
        """Collect feedback based on issue resolution status

        Args:
            action_id (str): action_id
            action_type (str): action_type
            issue_type (str): issue_type
            confidence_score (float): confidence_score
            successful (bool): successful
            resolution_details (dict): resolution_details
            context (dict): context

        Returns:
            Feedback: Created feedback record
        """
        feedback = Feedback(
            action_id=action_id,
            action_type=action_type,
            issue_type=issue_type,
            confidence_score=confidence_score,
            successful=successful,
            feedback_type="resolution",
            feedback_source="pipeline",
            context=context,
            metrics=resolution_details
        )
        self._store_feedback(feedback)
        self._healing_repository.update_healing_action_stats(action_id, successful)
        self.logger.info(f"Collected resolution feedback for action {action_id}")
        return feedback

    def collect_manual_feedback(
        self,
        action_id: str,
        action_type: str,
        issue_type: str,
        confidence_score: float,
        successful: bool,
        user_id: str,
        comments: str = None,
        context: dict = None
    ) -> Feedback:
        """Collect manual feedback from users

        Args:
            action_id (str): action_id
            action_type (str): action_type
            issue_type (str): issue_type
            confidence_score (float): confidence_score
            successful (bool): successful
            user_id (str): user_id
            comments (str): comments
            context (dict): context

        Returns:
            Feedback: Created feedback record
        """
        feedback = Feedback(
            action_id=action_id,
            action_type=action_type,
            issue_type=issue_type,
            confidence_score=confidence_score,
            successful=successful,
            feedback_type="manual",
            feedback_source=user_id,
            context=context,
            comments=comments
        )
        self._store_feedback(feedback)
        self._healing_repository.update_healing_action_stats(action_id, successful)
        self.logger.info(f"Collected manual feedback for action {action_id} from user {user_id}")
        return feedback

    def collect_inferred_feedback(
        self,
        action_id: str,
        action_type: str,
        issue_type: str,
        confidence_score: float,
        successful: bool,
        inference_data: dict = None,
        context: dict = None
    ) -> Feedback:
        """Collect feedback inferred from subsequent pipeline behavior

        Args:
            action_id (str): action_id
            action_type (str): action_type
            issue_type (str): issue_type
            confidence_score (float): confidence_score
            successful (bool): successful
            inference_data (dict): inference_data
            context (dict): context

        Returns:
            Feedback: Created feedback record
        """
        feedback = Feedback(
            action_id=action_id,
            action_type=action_type,
            issue_type=issue_type,
            confidence_score=confidence_score,
            successful=successful,
            feedback_type="inferred",
            feedback_source="pipeline_analysis",
            context=context,
            metrics=inference_data
        )
        self._store_feedback(feedback)
        self._healing_repository.update_healing_action_stats(action_id, successful)
        self.logger.info(f"Collected inferred feedback for action {action_id}")
        return feedback

    def get_feedback(self, filters: dict = None) -> List[Feedback]:
        """Get all feedback records with optional filtering

        Args:
            filters (dict): filters

        Returns:
            list: List of Feedback objects matching filters
        """
        result = []
        for feedback_id, feedback in self._feedback_store.items():
            if filters is None or all(
                getattr(feedback, key) == value for key, value in filters.items()
            ):
                result.append(feedback)
        return result

    def get_feedback_for_action(self, action_id: str) -> List[Feedback]:
        """Get all feedback for a specific healing action

        Args:
            action_id (str): action_id

        Returns:
            list: List of Feedback objects for the action
        """
        return self.get_feedback(filters={"action_id": action_id})

    def get_feedback_by_type(self, feedback_type: str) -> List[Feedback]:
        """Get feedback filtered by feedback type

        Args:
            feedback_type (str): feedback_type

        Returns:
            list: List of Feedback objects of the specified type
        """
        return self.get_feedback(filters={"feedback_type": feedback_type})

    def get_feedback_by_source(self, feedback_source: str) -> List[Feedback]:
        """Get feedback filtered by feedback source

        Args:
            feedback_source (str): feedback_source

        Returns:
            list: List of Feedback objects from the specified source
        """
        return self.get_feedback(filters={"feedback_source": feedback_source})

    def get_feedback_statistics(self) -> dict:
        """Get statistical summary of collected feedback

        Returns:
            dict: Dictionary of feedback statistics
        """
        total_feedback = len(self._feedback_store)
        feedback_by_type = {}
        feedback_by_source = {}
        success_counts = {}

        for feedback_id, feedback in self._feedback_store.items():
            feedback_type = feedback.feedback_type
            feedback_source = feedback.feedback_source

            feedback_by_type[feedback_type] = feedback_by_type.get(feedback_type, 0) + 1
            feedback_by_source[feedback_source] = feedback_by_source.get(feedback_source, 0) + 1

            if feedback_type not in success_counts:
                success_counts[feedback_type] = {"successful": 0, "total": 0}
            success_counts[feedback_type]["total"] += 1
            if feedback.successful:
                success_counts[feedback_type]["successful"] += 1

        success_rates = {}
        for feedback_type, counts in success_counts.items():
            success_rates[feedback_type] = (
                counts["successful"] / counts["total"] if counts["total"] > 0 else 0
            )

        confidence_scores = [
            feedback.confidence_score for feedback in self._feedback_store.values()
            if feedback.confidence_score is not None
        ]
        avg_confidence = (
            sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        )

        return {
            "total_feedback": total_feedback,
            "feedback_by_type": feedback_by_type,
            "feedback_by_source": feedback_by_source,
            "success_rates": success_rates,
            "avg_confidence": avg_confidence,
        }

    def analyze_feedback(self, analysis_params: dict = None) -> dict:
        """Analyze feedback to identify patterns and trends

        Args:
            analysis_params (dict): analysis_params

        Returns:
            dict: Analysis results
        """
        df = pd.DataFrame([feedback.to_dict() for feedback in self._feedback_store.values()])
        if df.empty:
            return {}

        success_rate_trends = df.groupby("feedback_type")["successful"].mean().to_dict()
        confidence_correlation = df[["confidence_score", "successful"]].corr().iloc[0, 1]
        feedback_by_issue = df["issue_type"].value_counts().to_dict()
        feedback_by_action = df["action_type"].value_counts().to_dict()

        return {
            "success_rate_trends": success_rate_trends,
            "confidence_correlation": confidence_correlation,
            "feedback_by_issue": feedback_by_issue,
            "feedback_by_action": feedback_by_action,
        }

    def export_feedback(self, format: str = "json") -> object:
        """Export feedback data to various formats

        Args:
            format (str): format

        Returns:
            object: Exported feedback in the specified format
        """
        feedback_list = [feedback.to_dict() for feedback in self._feedback_store.values()]

        if format == "json":
            return json.dumps(feedback_list)
        elif format == "csv":
            df = pd.DataFrame(feedback_list)
            return df.to_csv()
        elif format == "dataframe":
            return pd.DataFrame(feedback_list)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def clear_old_feedback(self) -> int:
        """Remove feedback older than retention period

        Returns:
            int: Number of records removed
        """
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=self._retention_days)
        removed_count = 0
        for feedback_id, feedback in list(self._feedback_store.items()):
            if feedback.timestamp < cutoff_date:
                del self._feedback_store[feedback_id]
                removed_count += 1
        self.logger.info(f"Removed {removed_count} old feedback records")
        return removed_count

    def set_retention_days(self, days: int) -> None:
        """Set the feedback retention period in days

        Args:
            days (int): days
        """
        if not isinstance(days, int) or days <= 0:
            raise ValueError("Retention days must be a positive integer")
        self._retention_days = days

    def set_batch_size(self, size: int) -> None:
        """Set the batch size for feedback processing

        Args:
            size (int): size
        """
        if not isinstance(size, int) or size <= 0:
            raise ValueError("Batch size must be a positive integer")
        self._batch_size = size

    def _store_feedback(self, feedback: Feedback) -> bool:
        """Store a feedback record in the internal store

        Args:
            feedback (Feedback): feedback

        Returns:
            bool: True if stored successfully
        """
        self._feedback_store[feedback.feedback_id] = feedback
        if len(self._feedback_store) > self._batch_size:
            self._persist_feedback_batch(list(self._feedback_store.values())[:self._batch_size])
        return True

    def _persist_feedback_batch(self, feedback_batch: list) -> bool:
        """Persist a batch of feedback records to storage

        Args:
            feedback_batch (list): feedback_batch

        Returns:
            bool: True if persisted successfully
        """
        # In a real implementation, this would persist the feedback to a database or other storage
        self.logger.info(f"Persisting {len(feedback_batch)} feedback records to storage")
        return True