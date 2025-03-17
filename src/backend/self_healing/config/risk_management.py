"""
Risk management module for the self-healing data pipeline.

This module provides functionality to assess the risk of automated healing actions,
calculate risk scores, and determine when human approval is required. It serves as
a critical component in the decision-making process for autonomous healing actions.
"""

import os
import enum
import yaml
from typing import Dict, List, Any, Optional, Tuple, Union

from ...constants import HealingActionType, AlertSeverity, SelfHealingMode
from ...config import get_config
from ...utils.logging.logger import get_logger

# Configure logger
logger = get_logger(__name__)

# Default configuration path
DEFAULT_RISK_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'configs',
    'risk_management.yaml'
)

# Default risk management values
DEFAULT_RISK_THRESHOLD = 0.7
DEFAULT_CONFIDENCE_ADJUSTMENT = 0.2
DEFAULT_SEVERITY_MULTIPLIERS = {
    AlertSeverity.CRITICAL: 2.0,
    AlertSeverity.HIGH: 1.5,
    AlertSeverity.MEDIUM: 1.0,
    AlertSeverity.LOW: 0.5,
    AlertSeverity.INFO: 0.1
}


class RiskLevel(enum.Enum):
    """Enumeration of risk levels for healing actions."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class ImpactCategory(enum.Enum):
    """Enumeration of impact categories for risk assessment."""
    DATA = "DATA"
    PIPELINE = "PIPELINE"
    BUSINESS = "BUSINESS"
    RESOURCE = "RESOURCE"


class RiskAssessment:
    """
    Data class representing the results of a risk assessment.
    """
    
    def __init__(self, risk_score: float, impact_scores: Dict[str, float], 
                approval_required: bool, details: Dict[str, Any] = None):
        """
        Initialize a risk assessment with calculated scores.
        
        Args:
            risk_score: Overall risk score (0.0 to 1.0)
            impact_scores: Dictionary of impact scores by category
            approval_required: Whether human approval is required
            details: Additional details about the assessment
        """
        self.risk_score = risk_score
        self.impact_scores = impact_scores
        self.approval_required = approval_required
        self.details = details or {}
        
        # Determine risk level based on risk score
        if risk_score < 0.3:
            self.risk_level = RiskLevel.LOW
        elif risk_score < 0.6:
            self.risk_level = RiskLevel.MEDIUM
        elif risk_score < 0.8:
            self.risk_level = RiskLevel.HIGH
        else:
            self.risk_level = RiskLevel.CRITICAL
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert risk assessment to dictionary representation.
        
        Returns:
            Dictionary representation of risk assessment
        """
        return {
            'risk_score': self.risk_score,
            'risk_level': self.risk_level.name,
            'impact_scores': self.impact_scores,
            'approval_required': self.approval_required,
            'details': self.details
        }
    
    @classmethod
    def from_dict(cls, risk_dict: Dict[str, Any]) -> 'RiskAssessment':
        """
        Create RiskAssessment from dictionary representation.
        
        Args:
            risk_dict: Dictionary containing risk assessment data
            
        Returns:
            RiskAssessment instance
        """
        return cls(
            risk_score=risk_dict.get('risk_score', 0.0),
            impact_scores=risk_dict.get('impact_scores', {}),
            approval_required=risk_dict.get('approval_required', False),
            details=risk_dict.get('details', {})
        )
    
    def get_highest_impact_category(self) -> Optional[ImpactCategory]:
        """
        Get the category with highest impact.
        
        Returns:
            Highest impact category
        """
        if not self.impact_scores:
            return None
        
        # Find category with highest score
        highest_category = max(self.impact_scores.items(), key=lambda x: x[1])[0]
        
        # Convert string to ImpactCategory enum
        try:
            return ImpactCategory(highest_category)
        except ValueError:
            return None
    
    def get_explanation(self) -> str:
        """
        Get human-readable explanation of risk assessment.
        
        Returns:
            Explanation of risk assessment
        """
        explanation = [
            f"Risk Assessment: {self.risk_score:.2f} ({self.risk_level.name})",
            f"Approval Required: {'Yes' if self.approval_required else 'No'}"
        ]
        
        # Add impact explanations
        explanation.append("Impact Analysis:")
        for category, score in self.impact_scores.items():
            explanation.append(f"  - {category}: {score:.2f}")
        
        # Add any additional details
        if self.details:
            explanation.append("Additional Details:")
            for key, value in self.details.items():
                explanation.append(f"  - {key}: {value}")
        
        return "\n".join(explanation)


class RiskManager:
    """
    Main class for managing risk assessment and approval decisions.
    """
    
    def __init__(self, risk_config_path: str = None):
        """
        Initialize the risk manager with configuration.
        
        Args:
            risk_config_path: Path to risk configuration file
        """
        self._config = get_config()
        self._risk_config = load_risk_config(risk_config_path)
        
        # Validate risk configuration
        if not validate_risk_config(self._risk_config):
            logger.warning("Using default risk configuration due to validation failure")
            self._risk_config = {}
        
        logger.info("RiskManager initialized")
    
    def assess_risk(self, action_type: HealingActionType, 
                   action_details: Dict[str, Any],
                   context: Dict[str, Any], 
                   confidence_score: float) -> RiskAssessment:
        """
        Perform a comprehensive risk assessment for a healing action.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            confidence_score: Confidence score of the healing action (0.0 to 1.0)
            
        Returns:
            Risk assessment results
        """
        # Assess impact across different categories
        impact_scores = self.assess_impact(action_type, action_details, context)
        
        # Calculate overall risk score
        risk_score = self.calculate_risk_score(action_type, action_details, context, confidence_score)
        
        # Determine if approval is required
        needs_approval = self.requires_approval(action_type, risk_score, confidence_score, context)
        
        # Prepare additional details
        details = {
            'action_type': action_type.value,
            'confidence_score': confidence_score,
            'environment': context.get('environment', 'unknown'),
            'assessment_time': context.get('assessment_time'),
        }
        
        # Create and return risk assessment
        assessment = RiskAssessment(
            risk_score=risk_score,
            impact_scores=impact_scores,
            approval_required=needs_approval,
            details=details
        )
        
        logger.info(f"Risk assessment completed for {action_type.value}: "
                  f"score={risk_score:.2f}, level={assessment.risk_level.name}, "
                  f"approval={'required' if needs_approval else 'not required'}")
        
        return assessment
    
    def calculate_risk_score(self, action_type: HealingActionType, 
                            action_details: Dict[str, Any],
                            context: Dict[str, Any],
                            confidence_score: float) -> float:
        """
        Calculate risk score for a healing action.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            confidence_score: Confidence score of the healing action (0.0 to 1.0)
            
        Returns:
            Risk score between 0.0 and 1.0
        """
        # Get base risk score for action type
        base_risk = self._risk_config.get('action_risks', {}).get(
            action_type.value, 0.5)
        
        # Apply confidence adjustment (lower confidence = higher risk)
        confidence_adjustment = (1 - confidence_score) * DEFAULT_CONFIDENCE_ADJUSTMENT
        risk_score = base_risk + confidence_adjustment
        
        # Apply context-specific risk adjustments
        # Environment-based adjustment
        environment = context.get('environment')
        if environment:
            env_multiplier = {
                'production': 1.2,
                'staging': 1.0,
                'development': 0.8
            }.get(environment, 1.0)
            risk_score *= env_multiplier
        
        # Time-based adjustment (higher risk during business hours)
        if context.get('is_business_hours', False):
            risk_score *= 1.1
        
        # Data volume adjustment
        data_volume = context.get('data_volume', 0)
        if data_volume > 1000000:  # Large data volume
            risk_score *= 1.2
        
        # Apply severity multiplier if present in context
        alert_severity = context.get('alert_severity')
        if alert_severity and isinstance(alert_severity, AlertSeverity):
            severity_multiplier = DEFAULT_SEVERITY_MULTIPLIERS.get(alert_severity, 1.0)
            risk_score *= severity_multiplier
        
        # Ensure risk score is between 0.0 and 1.0
        risk_score = max(0.0, min(1.0, risk_score))
        
        logger.debug(f"Calculated risk score {risk_score:.2f} for action {action_type.value}")
        return risk_score
    
    def assess_impact(self, action_type: HealingActionType,
                     action_details: Dict[str, Any],
                     context: Dict[str, Any]) -> Dict[str, float]:
        """
        Assess impact across different categories.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Dictionary of impact scores by category
        """
        impact_scores = {
            ImpactCategory.DATA.value: 0.0,
            ImpactCategory.PIPELINE.value: 0.0,
            ImpactCategory.BUSINESS.value: 0.0,
            ImpactCategory.RESOURCE.value: 0.0
        }
        
        # Assess data impact
        if action_type == HealingActionType.DATA_CORRECTION:
            impact_scores[ImpactCategory.DATA.value] = 0.7
            
            # Data volume impact
            data_volume = context.get('data_volume', 0)
            if data_volume > 1000000:  # Large data volume
                impact_scores[ImpactCategory.DATA.value] += 0.2
            
            # Data criticality impact
            data_criticality = context.get('data_criticality', 'medium')
            if data_criticality == 'high':
                impact_scores[ImpactCategory.DATA.value] += 0.2
            
        elif action_type == HealingActionType.SCHEMA_EVOLUTION:
            impact_scores[ImpactCategory.DATA.value] = 0.8
        
        # Assess pipeline impact
        if action_type == HealingActionType.PIPELINE_RETRY:
            impact_scores[ImpactCategory.PIPELINE.value] = 0.5
            
            # Consider retry attempt number
            retry_count = context.get('retry_count', 0)
            if retry_count > 2:
                impact_scores[ImpactCategory.PIPELINE.value] += 0.3
                
        elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
            impact_scores[ImpactCategory.PIPELINE.value] = 0.6
        
        # Assess business impact
        # Critical pipelines have higher business impact
        pipeline_criticality = context.get('pipeline_criticality', 'medium')
        if pipeline_criticality == 'high':
            impact_scores[ImpactCategory.BUSINESS.value] = 0.8
        elif pipeline_criticality == 'medium':
            impact_scores[ImpactCategory.BUSINESS.value] = 0.5
        else:
            impact_scores[ImpactCategory.BUSINESS.value] = 0.2
        
        # SLA consideration
        if context.get('approaching_sla', False):
            impact_scores[ImpactCategory.BUSINESS.value] += 0.2
        
        # Assess resource impact
        if action_type == HealingActionType.RESOURCE_SCALING:
            impact_scores[ImpactCategory.RESOURCE.value] = 0.7
            
            # Scale magnitude impacts risk
            scale_factor = action_details.get('scale_factor', 1.0)
            if scale_factor > 2.0:
                impact_scores[ImpactCategory.RESOURCE.value] += 0.2
        else:
            impact_scores[ImpactCategory.RESOURCE.value] = 0.3
        
        # Ensure all impact scores are between 0.0 and 1.0
        for category in impact_scores:
            impact_scores[category] = max(0.0, min(1.0, impact_scores[category]))
        
        # Apply impact weight multipliers from configuration
        impact_weights = self._risk_config.get('impact_weights', {})
        for category in impact_scores:
            weight = impact_weights.get(category, 1.0)
            impact_scores[category] *= weight
        
        logger.debug(f"Assessed impact for {action_type.value}: {impact_scores}")
        return impact_scores
    
    def requires_approval(self, action_type: HealingActionType,
                         risk_score: float,
                         confidence_score: float,
                         context: Dict[str, Any]) -> bool:
        """
        Determine if action requires approval.
        
        Args:
            action_type: Type of healing action
            risk_score: Calculated risk score (0.0 to 1.0)
            confidence_score: Confidence score of the healing action (0.0 to 1.0)
            context: Contextual information about the execution environment
            
        Returns:
            True if approval required, False otherwise
        """
        # Get the current self-healing mode
        healing_mode = self._config.get_self_healing_mode()
        
        # DISABLED mode always requires approval
        if healing_mode == SelfHealingMode.DISABLED:
            logger.debug(f"Approval required: Self-healing is DISABLED")
            return True
        
        # RECOMMENDATION_ONLY mode always requires approval
        if healing_mode == SelfHealingMode.RECOMMENDATION_ONLY:
            logger.debug(f"Approval required: Self-healing is in RECOMMENDATION_ONLY mode")
            return True
        
        # AUTOMATIC mode only requires approval for high-risk actions
        if healing_mode == SelfHealingMode.AUTOMATIC:
            # Only require approval for very high risk actions in automatic mode
            if risk_score > 0.8:
                logger.debug(f"Approval required: High risk score {risk_score:.2f} in AUTOMATIC mode")
                return True
            else:
                logger.debug(f"No approval required: Risk score {risk_score:.2f} acceptable in AUTOMATIC mode")
                return False
        
        # For SEMI_AUTOMATIC mode, compare risk score to threshold
        threshold = self.get_risk_threshold(action_type, context)
        
        # Adjust threshold based on confidence
        # Higher confidence allows higher risk tolerance
        confidence_adjustment = (confidence_score - 0.5) * 0.2
        adjusted_threshold = threshold + confidence_adjustment
        
        # Ensure adjusted threshold is between 0.0 and 1.0
        adjusted_threshold = max(0.0, min(1.0, adjusted_threshold))
        
        # Get action-specific approval rules
        approval_rules = self._risk_config.get('approval_rules', {})
        action_rules = approval_rules.get(action_type.value, {})
        
        # Check for any mandatory approval settings
        if action_rules.get('always_require_approval', False):
            logger.debug(f"Approval required: Action {action_type.value} always requires approval")
            return True
        
        # Check for environment-specific rules
        environment = context.get('environment')
        if environment and action_rules.get(f'{environment}_requires_approval', False):
            logger.debug(f"Approval required: Environment {environment} requires approval for {action_type.value}")
            return True
        
        # Require approval if risk score exceeds adjusted threshold
        requires_approval = risk_score > adjusted_threshold
        
        logger.debug(f"Approval {'required' if requires_approval else 'not required'}: "
                    f"Risk score {risk_score:.2f} vs threshold {adjusted_threshold:.2f}")
        
        return requires_approval
    
    def get_risk_threshold(self, action_type: HealingActionType, context: Dict[str, Any]) -> float:
        """
        Get risk threshold for action and context.
        
        Args:
            action_type: Type of healing action
            context: Contextual information about the execution environment
            
        Returns:
            Risk threshold between 0.0 and 1.0
        """
        # Get base threshold from configuration
        base_threshold = self._risk_config.get('risk_thresholds', {}).get(
            action_type.value, DEFAULT_RISK_THRESHOLD)
        
        # Apply action-specific adjustments
        threshold = base_threshold
        
        # Environment-based adjustment
        environment = context.get('environment')
        if environment:
            if environment == 'production':
                threshold -= 0.1  # Lower threshold (stricter) in production
            elif environment == 'development':
                threshold += 0.1  # Higher threshold (more permissive) in development
        
        # Time-based adjustment
        if context.get('is_business_hours', False):
            threshold -= 0.05  # Stricter during business hours
        else:
            threshold += 0.05  # More permissive outside business hours
        
        # Data criticality adjustment
        data_criticality = context.get('data_criticality', 'medium')
        if data_criticality == 'high':
            threshold -= 0.1  # Stricter for critical data
        
        # Ensure threshold is between 0.0 and 1.0
        threshold = max(0.0, min(1.0, threshold))
        
        logger.debug(f"Determined risk threshold {threshold:.2f} for action {action_type.value}")
        return threshold
    
    def get_risk_level(self, risk_score: float) -> RiskLevel:
        """
        Determine risk level from risk score.
        
        Args:
            risk_score: Risk score between 0.0 and 1.0
            
        Returns:
            Risk level enum value
        """
        if risk_score < 0.3:
            return RiskLevel.LOW
        elif risk_score < 0.6:
            return RiskLevel.MEDIUM
        elif risk_score < 0.8:
            return RiskLevel.HIGH
        else:
            return RiskLevel.CRITICAL
    
    def reload_config(self) -> bool:
        """
        Reload risk configuration settings.
        
        Returns:
            Success status
        """
        try:
            self._risk_config = load_risk_config()
            valid = validate_risk_config(self._risk_config)
            if not valid:
                logger.warning("Invalid risk configuration after reload")
            return valid
        except Exception as e:
            logger.error(f"Error reloading risk configuration: {e}")
            return False


def calculate_risk_score(action_type: HealingActionType, 
                        action_details: Dict[str, Any],
                        context: Dict[str, Any],
                        confidence_score: float) -> float:
    """
    Calculates a risk score for a healing action based on multiple factors.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        confidence_score: Confidence score of the healing action (0.0 to 1.0)
        
    Returns:
        Risk score between 0.0 and 1.0, where higher values indicate higher risk
    """
    # Load risk configuration
    risk_config = load_risk_config(DEFAULT_RISK_CONFIG_PATH)
    
    # Get base risk score for action type
    base_risk = risk_config.get('action_risks', {}).get(action_type.value, 0.5)
    
    # Apply confidence adjustment (lower confidence = higher risk)
    confidence_adjustment = (1 - confidence_score) * DEFAULT_CONFIDENCE_ADJUSTMENT
    risk_score = base_risk + confidence_adjustment
    
    # Apply context-specific risk adjustments
    # Environment-based adjustment
    environment = context.get('environment')
    if environment:
        env_multiplier = {
            'production': 1.2,
            'staging': 1.0,
            'development': 0.8
        }.get(environment, 1.0)
        risk_score *= env_multiplier
    
    # Time-based adjustment (higher risk during business hours)
    if context.get('is_business_hours', False):
        risk_score *= 1.1
    
    # Data volume adjustment
    data_volume = context.get('data_volume', 0)
    if data_volume > 1000000:  # Large data volume
        risk_score *= 1.2
    
    # Apply severity multiplier if present in context
    alert_severity = context.get('alert_severity')
    if alert_severity and isinstance(alert_severity, AlertSeverity):
        severity_multiplier = DEFAULT_SEVERITY_MULTIPLIERS.get(alert_severity, 1.0)
        risk_score *= severity_multiplier
    
    # Ensure risk score is between 0.0 and 1.0
    risk_score = max(0.0, min(1.0, risk_score))
    
    logger.debug(f"Calculated risk score {risk_score} for action {action_type.value} with confidence {confidence_score}")
    return risk_score


def assess_impact(action_type: HealingActionType,
                action_details: Dict[str, Any],
                context: Dict[str, Any]) -> Dict[str, float]:
    """
    Assesses the potential impact of a healing action across different categories.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        
    Returns:
        Dictionary of impact scores by category
    """
    impact_scores = {
        ImpactCategory.DATA.value: 0.0,
        ImpactCategory.PIPELINE.value: 0.0,
        ImpactCategory.BUSINESS.value: 0.0,
        ImpactCategory.RESOURCE.value: 0.0
    }
    
    # Assess data impact
    if action_type == HealingActionType.DATA_CORRECTION:
        impact_scores[ImpactCategory.DATA.value] = 0.7
        
        # Data volume impact
        data_volume = context.get('data_volume', 0)
        if data_volume > 1000000:  # Large data volume
            impact_scores[ImpactCategory.DATA.value] += 0.2
        
        # Data criticality impact
        data_criticality = context.get('data_criticality', 'medium')
        if data_criticality == 'high':
            impact_scores[ImpactCategory.DATA.value] += 0.2
        
    elif action_type == HealingActionType.SCHEMA_EVOLUTION:
        impact_scores[ImpactCategory.DATA.value] = 0.8
    
    # Assess pipeline impact
    if action_type == HealingActionType.PIPELINE_RETRY:
        impact_scores[ImpactCategory.PIPELINE.value] = 0.5
        
        # Consider retry attempt number
        retry_count = context.get('retry_count', 0)
        if retry_count > 2:
            impact_scores[ImpactCategory.PIPELINE.value] += 0.3
            
    elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
        impact_scores[ImpactCategory.PIPELINE.value] = 0.6
    
    # Assess business impact
    # Critical pipelines have higher business impact
    pipeline_criticality = context.get('pipeline_criticality', 'medium')
    if pipeline_criticality == 'high':
        impact_scores[ImpactCategory.BUSINESS.value] = 0.8
    elif pipeline_criticality == 'medium':
        impact_scores[ImpactCategory.BUSINESS.value] = 0.5
    else:
        impact_scores[ImpactCategory.BUSINESS.value] = 0.2
    
    # SLA consideration
    if context.get('approaching_sla', False):
        impact_scores[ImpactCategory.BUSINESS.value] += 0.2
    
    # Assess resource impact
    if action_type == HealingActionType.RESOURCE_SCALING:
        impact_scores[ImpactCategory.RESOURCE.value] = 0.7
        
        # Scale magnitude impacts risk
        scale_factor = action_details.get('scale_factor', 1.0)
        if scale_factor > 2.0:
            impact_scores[ImpactCategory.RESOURCE.value] += 0.2
    else:
        impact_scores[ImpactCategory.RESOURCE.value] = 0.3
    
    # Ensure all impact scores are between 0.0 and 1.0
    for category in impact_scores:
        impact_scores[category] = max(0.0, min(1.0, impact_scores[category]))
    
    logger.debug(f"Assessed impact for {action_type.value}: {impact_scores}")
    return impact_scores


def get_risk_threshold(action_type: HealingActionType, context: Dict[str, Any]) -> float:
    """
    Gets the risk threshold for a specific action type and context.
    
    Args:
        action_type: Type of healing action
        context: Contextual information about the execution environment
        
    Returns:
        Risk threshold value between 0.0 and 1.0
    """
    # Load risk configuration
    risk_config = load_risk_config(DEFAULT_RISK_CONFIG_PATH)
    
    # Get base threshold from configuration
    base_threshold = risk_config.get('risk_thresholds', {}).get(action_type.value, DEFAULT_RISK_THRESHOLD)
    
    # Apply action-specific adjustments
    threshold = base_threshold
    
    # Environment-based adjustment
    environment = context.get('environment')
    if environment:
        if environment == 'production':
            threshold -= 0.1  # Lower threshold (stricter) in production
        elif environment == 'development':
            threshold += 0.1  # Higher threshold (more permissive) in development
    
    # Time-based adjustment
    if context.get('is_business_hours', False):
        threshold -= 0.05  # Stricter during business hours
    else:
        threshold += 0.05  # More permissive outside business hours
    
    # Data criticality adjustment
    data_criticality = context.get('data_criticality', 'medium')
    if data_criticality == 'high':
        threshold -= 0.1  # Stricter for critical data
    
    # Ensure threshold is between 0.0 and 1.0
    threshold = max(0.0, min(1.0, threshold))
    
    logger.debug(f"Determined risk threshold {threshold} for action {action_type.value}")
    return threshold


def requires_approval(action_type: HealingActionType,
                    risk_score: float,
                    confidence_score: float,
                    context: Dict[str, Any]) -> bool:
    """
    Determines if a healing action requires human approval based on risk assessment.
    
    Args:
        action_type: Type of healing action
        risk_score: Calculated risk score (0.0 to 1.0)
        confidence_score: Confidence score of the healing action (0.0 to 1.0)
        context: Contextual information about the execution environment
        
    Returns:
        True if approval is required, False otherwise
    """
    # Get configuration
    config = get_config()
    
    # Get the current self-healing mode
    healing_mode = config.get_self_healing_mode()
    
    # DISABLED mode always requires approval
    if healing_mode == SelfHealingMode.DISABLED:
        logger.debug(f"Approval required: Self-healing is DISABLED")
        return True
    
    # RECOMMENDATION_ONLY mode always requires approval
    if healing_mode == SelfHealingMode.RECOMMENDATION_ONLY:
        logger.debug(f"Approval required: Self-healing is in RECOMMENDATION_ONLY mode")
        return True
    
    # AUTOMATIC mode only requires approval for high-risk actions
    if healing_mode == SelfHealingMode.AUTOMATIC:
        # Only require approval for very high risk actions in automatic mode
        if risk_score > 0.8:
            logger.debug(f"Approval required: High risk score {risk_score} in AUTOMATIC mode")
            return True
        else:
            logger.debug(f"No approval required: Risk score {risk_score} acceptable in AUTOMATIC mode")
            return False
    
    # For SEMI_AUTOMATIC mode, compare risk score to threshold
    threshold = get_risk_threshold(action_type, context)
    
    # Adjust threshold based on confidence
    # Higher confidence allows higher risk tolerance
    confidence_adjustment = (confidence_score - 0.5) * 0.2
    adjusted_threshold = threshold + confidence_adjustment
    
    # Ensure adjusted threshold is between 0.0 and 1.0
    adjusted_threshold = max(0.0, min(1.0, adjusted_threshold))
    
    # Require approval if risk score exceeds adjusted threshold
    requires_approval = risk_score > adjusted_threshold
    
    logger.debug(f"Approval {'required' if requires_approval else 'not required'}: "
                f"Risk score {risk_score} vs threshold {adjusted_threshold}")
    
    return requires_approval


def load_risk_config(config_path: str = None) -> Dict[str, Any]:
    """
    Loads risk management configuration from a YAML file.
    
    Args:
        config_path: Path to the risk configuration file
        
    Returns:
        Risk management configuration dictionary
    """
    # Use default path if not provided
    actual_path = config_path or DEFAULT_RISK_CONFIG_PATH
    
    # Check if file exists
    if not os.path.exists(actual_path):
        logger.warning(f"Risk configuration file not found: {actual_path}")
        return {}
    
    try:
        # Load and parse YAML file
        with open(actual_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
            
            # Validate configuration structure
            if not validate_risk_config(config):
                logger.warning("Invalid risk configuration structure")
                return {}
            
            return config
    except Exception as e:
        logger.error(f"Error loading risk configuration: {e}")
        return {}


def validate_risk_config(config: Dict[str, Any]) -> bool:
    """
    Validates the structure and content of the risk management configuration.
    
    Args:
        config: Risk configuration dictionary
        
    Returns:
        True if configuration is valid, False otherwise
    """
    # Check if config is a dictionary
    if not isinstance(config, dict):
        logger.error("Risk configuration must be a dictionary")
        return False
    
    # Check for required top-level sections
    required_sections = ['risk_thresholds', 'action_risks', 'impact_weights', 'approval_rules']
    for section in required_sections:
        if section not in config:
            logger.error(f"Missing required section '{section}' in risk configuration")
            return False
    
    # Validate risk_thresholds section
    thresholds = config.get('risk_thresholds', {})
    if not isinstance(thresholds, dict):
        logger.error("risk_thresholds must be a dictionary")
        return False
    
    # Validate action_risks section
    action_risks = config.get('action_risks', {})
    if not isinstance(action_risks, dict):
        logger.error("action_risks must be a dictionary")
        return False
    
    # Validate impact_weights section
    impact_weights = config.get('impact_weights', {})
    if not isinstance(impact_weights, dict):
        logger.error("impact_weights must be a dictionary")
        return False
    
    # Validate approval_rules section
    approval_rules = config.get('approval_rules', {})
    if not isinstance(approval_rules, dict):
        logger.error("approval_rules must be a dictionary")
        return False
    
    return True