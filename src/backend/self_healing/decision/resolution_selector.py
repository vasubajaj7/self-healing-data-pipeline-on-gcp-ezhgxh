"""
Implements the resolution selection component of the self-healing AI engine.
This module evaluates potential healing actions for detected issues,
selects the optimal resolution strategy based on confidence scores and impact analysis,
and manages the execution of healing actions with appropriate approval workflows.
"""

import typing
from typing import Dict, Any, Optional
import uuid
import datetime

from typing import List

from ...constants import (
    HealingActionType,
    SelfHealingMode,
    DEFAULT_CONFIDENCE_THRESHOLD,
    DEFAULT_MAX_RETRY_ATTEMPTS,
    HEALING_STATUS_PENDING,
    HEALING_STATUS_IN_PROGRESS,
    HEALING_STATUS_SUCCESS,
    HEALING_STATUS_FAILED,
    HEALING_STATUS_APPROVAL_REQUIRED
)
from ...config import get_config
from ...utils.logging.logger import get_logger
from ..config.healing_config import get_healing_mode, get_confidence_threshold, get_max_retry_attempts, get_rules_by_type
from .confidence_scorer import ConfidenceScorer, ConfidenceScore
from .impact_analyzer import ImpactAnalyzer, ImpactAnalysis, ImpactLevel
from .approval_manager import ApprovalManager, ApprovalStatus
from ...db.repositories.healing_repository import HealingRepository

# Initialize logger
logger = get_logger(__name__)

# Define default minimum confidence threshold
DEFAULT_MIN_CONFIDENCE_THRESHOLD = 0.7
# Define default maximum resolution attempts
DEFAULT_MAX_RESOLUTION_ATTEMPTS = 3


class ResolutionOption:
    """
    Data class representing a potential resolution option for an issue
    """

    def __init__(
        self,
        action_id: str,
        action_type: HealingActionType,
        action_details: Dict[str, Any],
        confidence_score: ConfidenceScore,
        impact_analysis: ImpactAnalysis,
        requires_approval: bool,
        metadata: Dict[str, Any] = None,
    ):
        """
        Initialize a resolution option with evaluation metrics

        Args:
            action_id (str): The ID of the healing action
            action_type (HealingActionType): The type of the healing action
            action_details (dict): Details of the action to be performed
            confidence_score (ConfidenceScore): The confidence score for this resolution
            impact_analysis (ImpactAnalysis): The impact analysis for this resolution
            requires_approval (bool): Whether this resolution requires manual approval
            metadata (dict): Additional metadata for this resolution
        """
        self.action_id = action_id
        self.action_type = action_type
        self.action_details = action_details
        self.confidence_score = confidence_score
        self.impact_analysis = impact_analysis
        self.priority_score = confidence_score.overall_score - impact_analysis.overall_impact
        self.requires_approval = requires_approval
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert resolution option to dictionary representation

        Returns:
            dict: Dictionary representation of resolution option
        """
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.value if self.action_type else None,
            "action_details": self.action_details,
            "confidence_score": self.confidence_score.to_dict() if self.confidence_score else None,
            "impact_analysis": self.impact_analysis.to_dict() if self.impact_analysis else None,
            "priority_score": self.priority_score,
            "requires_approval": self.requires_approval,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResolutionOption":
        """
        Create ResolutionOption from dictionary representation

        Args:
            data (dict): Dictionary representation of a resolution option

        Returns:
            ResolutionOption: ResolutionOption instance
        """
        action_id = data.get("action_id")
        action_type = HealingActionType(data.get("action_type")) if data.get("action_type") else None
        action_details = data.get("action_details")
        confidence_score = ConfidenceScore.from_dict(data.get("confidence_score")) if data.get("confidence_score") else None
        impact_analysis = ImpactAnalysis.from_dict(data.get("impact_analysis")) if data.get("impact_analysis") else None
        requires_approval = data.get("requires_approval")
        metadata = data.get("metadata")

        return cls(
            action_id=action_id,
            action_type=action_type,
            action_details=action_details,
            confidence_score=confidence_score,
            impact_analysis=impact_analysis,
            requires_approval=requires_approval,
            metadata=metadata,
        )

    def meets_thresholds(self, confidence_threshold: float, impact_threshold: float) -> bool:
        """
        Check if resolution option meets confidence and impact thresholds

        Args:
            confidence_threshold (float): Minimum confidence threshold
            impact_threshold (float): Maximum impact threshold

        Returns:
            bool: True if option meets both thresholds
        """
        if not self.confidence_score or not self.impact_analysis:
            return False

        return self.confidence_score.meets_threshold(confidence_threshold) and not self.impact_analysis.exceeds_threshold(impact_threshold)


class Resolution:
    """
    Data class representing a selected resolution with execution details
    """

    def __init__(
        self,
        issue_id: str,
        issue_description: str,
        action_id: str,
        action_type: HealingActionType,
        action_details: Dict[str, Any],
        confidence_score: ConfidenceScore,
        impact_analysis: ImpactAnalysis,
        requires_approval: bool,
        metadata: Dict[str, Any] = None,
    ):
        """
        Initialize a resolution with selected action and execution details

        Args:
            issue_id (str): The ID of the issue being resolved
            issue_description (str): A description of the issue
            action_id (str): The ID of the healing action
            action_type (HealingActionType): The type of the healing action
            action_details (dict): Details of the action to be performed
            confidence_score (ConfidenceScore): The confidence score for this resolution
            impact_analysis (ImpactAnalysis): The impact analysis for this resolution
            requires_approval (bool): Whether this resolution requires manual approval
            metadata (dict): Additional metadata for this resolution
        """
        self.resolution_id = str(uuid.uuid4())
        self.issue_id = issue_id
        self.issue_description = issue_description
        self.action_id = action_id
        self.action_type = action_type
        self.action_details = action_details
        self.status = HEALING_STATUS_PENDING
        self.confidence_score = confidence_score
        self.impact_analysis = impact_analysis
        self.requires_approval = requires_approval
        self.approval_id = None
        self.approval_status = None
        self.created_at = datetime.datetime.utcnow()
        self.updated_at = datetime.datetime.utcnow()
        self.executed_at = None
        self.attempt_count = 0
        self.execution_result = {}
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert resolution to dictionary representation

        Returns:
            dict: Dictionary representation of resolution
        """
        return {
            "resolution_id": self.resolution_id,
            "issue_id": self.issue_id,
            "issue_description": self.issue_description,
            "action_id": self.action_id,
            "action_type": self.action_type.value if self.action_type else None,
            "action_details": self.action_details,
            "status": self.status,
            "confidence_score": self.confidence_score.to_dict() if self.confidence_score else None,
            "impact_analysis": self.impact_analysis.to_dict() if self.impact_analysis else None,
            "requires_approval": self.requires_approval,
            "approval_id": self.approval_id,
            "approval_status": self.approval_status.value if self.approval_status else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "attempt_count": self.attempt_count,
            "execution_result": self.execution_result,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Resolution":
        """
        Create Resolution from dictionary representation

        Args:
            data (dict): Dictionary representation of a resolution

        Returns:
            Resolution: Resolution instance
        """
        resolution = cls(
            issue_id=data.get("issue_id"),
            issue_description=data.get("issue_description"),
            action_id=data.get("action_id"),
            action_type=HealingActionType(data.get("action_type")) if data.get("action_type") else None,
            action_details=data.get("action_details"),
            confidence_score=ConfidenceScore.from_dict(data.get("confidence_score")) if data.get("confidence_score") else None,
            impact_analysis=ImpactAnalysis.from_dict(data.get("impact_analysis")) if data.get("impact_analysis") else None,
            requires_approval=data.get("requires_approval"),
            metadata=data.get("metadata"),
        )
        resolution.resolution_id = data.get("resolution_id")
        resolution.status = data.get("status")
        resolution.approval_id = data.get("approval_id")
        resolution.approval_status = ApprovalStatus(data.get("approval_status")) if data.get("approval_status") else None
        resolution.created_at = datetime.datetime.fromisoformat(data.get("created_at")) if data.get("created_at") else None
        resolution.updated_at = datetime.datetime.fromisoformat(data.get("updated_at")) if data.get("updated_at") else None
        resolution.executed_at = datetime.datetime.fromisoformat(data.get("executed_at")) if data.get("executed_at") else None
        resolution.attempt_count = data.get("attempt_count")
        resolution.execution_result = data.get("execution_result")
        return resolution

    def update_status(self, new_status: str, result_details: dict = None):
        """
        Update the status of the resolution

        Args:
            new_status (str): The new status to set
            result_details (dict): Details about the execution result
        """
        self.status = new_status
        self.updated_at = datetime.datetime.utcnow()
        if new_status == HEALING_STATUS_IN_PROGRESS:
            self.attempt_count += 1
        if new_status in [HEALING_STATUS_SUCCESS, HEALING_STATUS_FAILED]:
            self.executed_at = datetime.datetime.utcnow()
        if result_details:
            self.execution_result = result_details

    def requires_approval(self) -> bool:
        """
        Check if resolution requires manual approval

        Returns:
            bool: True if approval is required
        """
        return self.requires_approval

    def is_automatic(self) -> bool:
        """
        Check if resolution can be executed automatically

        Returns:
            bool: True if automatic execution is allowed
        """
        return not self.requires_approval

    def is_recommendation_only(self) -> bool:
        """
        Check if resolution is recommendation-only (no execution)

        Returns:
            bool: True if resolution is recommendation-only
        """
        return self.metadata.get("recommendation_only", False)

    def set_approval_details(self, approval_id: str, approval_status: ApprovalStatus):
        """
        Set approval details for the resolution

        Args:
            approval_id (str): The ID of the approval request
            approval_status (ApprovalStatus): The status of the approval
        """
        self.approval_id = approval_id
        self.approval_status = approval_status
        self.updated_at = datetime.datetime.utcnow()


class ResolutionSelector:
    """
    Main class for selecting and managing resolutions for pipeline and data quality issues
    """

    def __init__(
        self,
        confidence_scorer: ConfidenceScorer,
        impact_analyzer: ImpactAnalyzer,
        approval_manager: ApprovalManager,
        repository: HealingRepository,
        config: Dict[str, Any] = None,
    ):
        """
        Initialize the resolution selector with required components

        Args:
            confidence_scorer (ConfidenceScorer): The confidence scorer to use
            impact_analyzer (ImpactAnalyzer): The impact analyzer to use
            approval_manager (ApprovalManager): The approval manager to use
            repository (HealingRepository): The repository for healing actions
            config (dict): Configuration overrides
        """
        self._confidence_scorer = confidence_scorer
        self._impact_analyzer = impact_analyzer
        self._approval_manager = approval_manager
        self._repository = repository
        self._config = get_config()

        self._confidence_threshold = self._config.get("self_healing.confidence_threshold", DEFAULT_CONFIDENCE_THRESHOLD)
        self._impact_threshold = self._config.get("self_healing.impact_threshold", 0.5)
        self._max_attempts = self._config.get("self_healing.max_attempts", DEFAULT_MAX_RESOLUTION_ATTEMPTS)

        self.logger = get_logger(__name__)

    def select_resolution(
        self,
        issue_id: str,
        issue_description: str,
        issue_details: Dict[str, Any],
        action_type: HealingActionType,
        context: Dict[str, Any],
        pattern: Dict[str, Any] = None,
    ) -> Resolution:
        """
        Select the optimal resolution for an issue based on confidence and impact

        Args:
            issue_id (str): The ID of the issue to resolve
            issue_description (str): A description of the issue
            issue_details (dict): Details about the issue
            action_type (HealingActionType): The type of healing action to perform
            context (dict): Contextual information about the pipeline and environment
            pattern (dict): The pattern that matches the issue

        Returns:
            Resolution: Selected resolution or None if no suitable resolution found
        """
        healing_mode = get_healing_mode()
        if healing_mode == SelfHealingMode.DISABLED:
            self.logger.info(f"Self-healing is disabled, no resolution selected for issue {issue_id}")
            return None

        options = self.generate_resolution_options(issue_id, issue_details, action_type, context, pattern)
        if not options:
            self.logger.info(f"No resolution options available for issue {issue_id}")
            return None

        # Filter options that meet confidence and impact thresholds
        filtered_options = [
            option
            for option in options
            if option.meets_thresholds(self._confidence_threshold, self._impact_threshold)
        ]
        if not filtered_options:
            self.logger.info(f"No resolution options meet thresholds for issue {issue_id}")
            return None

        # Sort options by priority score (highest first)
        sorted_options = sorted(filtered_options, key=lambda x: x.priority_score, reverse=True)

        # Select the highest priority option
        selected_option = sorted_options[0]

        # Create Resolution object from selected option
        resolution = Resolution(
            issue_id=issue_id,
            issue_description=issue_description,
            action_id=selected_option.action_id,
            action_type=selected_option.action_type,
            action_details=selected_option.action_details,
            confidence_score=selected_option.confidence_score,
            impact_analysis=selected_option.impact_analysis,
            requires_approval=selected_option.requires_approval,
            metadata=selected_option.metadata,
        )

        # If requires_approval, create approval request
        if resolution.requires_approval:
            # TODO: Implement approval workflow
            self.logger.info(f"Approval required for resolution {resolution.resolution_id} for issue {issue_id}")
            pass

        # If mode is RECOMMENDATION_ONLY, mark as recommendation only
        if healing_mode == SelfHealingMode.RECOMMENDATION_ONLY:
            resolution.metadata["recommendation_only"] = True
            self.logger.info(f"Resolution {resolution.resolution_id} marked as recommendation only")

        self.logger.info(f"Selected resolution {resolution.resolution_id} for issue {issue_id} with action {selected_option.action_id}")
        return resolution

    def generate_resolution_options(
        self,
        issue_id: str,
        issue_details: Dict[str, Any],
        action_type: HealingActionType,
        context: Dict[str, Any],
        pattern: Dict[str, Any] = None,
    ) -> List[ResolutionOption]:
        """
        Generate potential resolution options for an issue

        Args:
            issue_id (str): The ID of the issue to resolve
            issue_details (dict): Details about the issue
            action_type (HealingActionType): The type of healing action to perform
            context (dict): Contextual information about the pipeline and environment
            pattern (dict): The pattern that matches the issue

        Returns:
            list: List of ResolutionOption objects
        """
        options = []
        healing_actions = self._repository.get_healing_actions_by_type(action_type)

        for action in healing_actions:
            confidence_score = self._confidence_scorer.calculate_confidence(action_type, action.action_parameters, issue_details, context, pattern)
            impact_analysis = self._impact_analyzer.analyze_impact(action_type, action.action_parameters, context)
            requires_approval = self._approval_manager.requires_manual_approval(action_type, confidence_score.overall_score, impact_analysis.overall_impact, context)

            option = ResolutionOption(
                action_id=action.action_id,
                action_type=action_type,
                action_details=action.action_parameters,
                confidence_score=confidence_score,
                impact_analysis=impact_analysis,
                requires_approval=requires_approval,
                metadata={"action_name": action.name, "action_description": action.description},
            )
            options.append(option)

        return options

    def execute_resolution(self, resolution: Resolution) -> bool:
        """
        Execute a selected resolution

        Args:
            resolution (Resolution): The resolution to execute

        Returns:
            bool: True if execution was successful
        """
        # TODO: Implement execution logic for different action types
        self.logger.info(f"Executing resolution {resolution.resolution_id} for issue {resolution.issue_id}")
        return True

    def get_resolution(self, resolution_id: str) -> Optional[Resolution]:
        """
        Get a resolution by its ID

        Args:
            resolution_id (str): The ID of the resolution

        Returns:
            Resolution: Resolution object if found, None otherwise
        """
        # TODO: Implement retrieval from database
        self.logger.info(f"Getting resolution with ID: {resolution_id}")
        return None

    def get_resolutions_for_issue(self, issue_id: str) -> List[Resolution]:
        """
        Get all resolutions for a specific issue

        Args:
            issue_id (str): The ID of the issue

        Returns:
            list: List of Resolution objects for the issue
        """
        # TODO: Implement retrieval from database
        self.logger.info(f"Getting resolutions for issue with ID: {issue_id}")
        return []

    def set_confidence_threshold(self, threshold: float):
        """
        Set the confidence threshold for resolution selection

        Args:
            threshold (float): The new confidence threshold
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Confidence threshold must be between 0.0 and 1.0")
        self._confidence_threshold = threshold
        self.logger.info(f"Confidence threshold set to {threshold}")

    def set_impact_threshold(self, threshold: float):
        """
        Set the impact threshold for resolution selection

        Args:
            threshold (float): The new impact threshold
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Impact threshold must be between 0.0 and 1.0")
        self._impact_threshold = threshold
        self.logger.info(f"Impact threshold set to {threshold}")

    def set_max_attempts(self, max_attempts: int):
        """
        Set the maximum number of execution attempts

        Args:
            max_attempts (int): The new maximum number of attempts
        """
        if not isinstance(max_attempts, int) or max_attempts <= 0:
            raise ValueError("Max attempts must be a positive integer")
        self._max_attempts = max_attempts
        self.logger.info(f"Max attempts set to {max_attempts}")

    def reload_config(self) -> bool:
        """
        Reload configuration settings
        """
        try:
            self._config = get_config()
            self._confidence_threshold = self._config.get("self_healing.confidence_threshold", DEFAULT_CONFIDENCE_THRESHOLD)
            self._impact_threshold = self._config.get("self_healing.impact_threshold", 0.5)
            self._max_attempts = self._config.get("self_healing.max_attempts", DEFAULT_MAX_RESOLUTION_ATTEMPTS)
            self.logger.info("ResolutionSelector configuration reloaded")
            return True
        except Exception as e:
            self.logger.error(f"Error reloading ResolutionSelector configuration: {e}")
            return False