"""
Implements the impact analysis component of the self-healing AI engine.

This module evaluates the potential impact of healing actions across different dimensions 
including data, pipeline, business, and resource impacts. It provides quantitative impact 
scores to help the decision engine determine whether actions can be executed automatically 
or require human approval.
"""

import enum
import datetime
from typing import Dict, Any, Optional, List, Tuple, Union

from ...constants import HealingActionType, AlertSeverity
from ...config import get_config
from ...utils.logging.logger import get_logger
from ..config.risk_management import ImpactCategory, RiskManager

# Configure logger
logger = get_logger(__name__)

# Default impact thresholds
DEFAULT_IMPACT_THRESHOLD = 0.7
DEFAULT_CRITICAL_IMPACT_THRESHOLD = 0.9
DEFAULT_IMPACT_WEIGHTS = {
    ImpactCategory.DATA: 0.4, 
    ImpactCategory.PIPELINE: 0.3, 
    ImpactCategory.BUSINESS: 0.2, 
    ImpactCategory.RESOURCE: 0.1
}


class ImpactLevel(enum.Enum):
    """Enumeration of impact levels for healing actions."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class ImpactAnalysis:
    """Data class representing the results of an impact analysis."""
    
    def __init__(self, overall_impact: float, category_impacts: Dict[str, float], details: Dict[str, Any] = None):
        """Initialize an impact analysis with calculated scores.
        
        Args:
            overall_impact: Overall impact score (0.0 to 1.0)
            category_impacts: Dictionary of impact scores by category
            details: Additional details about the impact analysis
        """
        self.overall_impact = overall_impact
        self.category_impacts = category_impacts
        self.details = details or {}
        
        # Determine impact level based on overall score
        if overall_impact < 0.3:
            self.impact_level = ImpactLevel.LOW
        elif overall_impact < 0.6:
            self.impact_level = ImpactLevel.MEDIUM
        elif overall_impact < 0.8:
            self.impact_level = ImpactLevel.HIGH
        else:
            self.impact_level = ImpactLevel.CRITICAL
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert impact analysis to dictionary representation.
        
        Returns:
            Dictionary representation of impact analysis
        """
        return {
            'overall_impact': self.overall_impact,
            'impact_level': self.impact_level.name,
            'category_impacts': self.category_impacts,
            'details': self.details
        }
    
    @classmethod
    def from_dict(cls, impact_dict: Dict[str, Any]) -> 'ImpactAnalysis':
        """Create ImpactAnalysis from dictionary representation.
        
        Args:
            impact_dict: Dictionary containing impact analysis data
            
        Returns:
            ImpactAnalysis instance
        """
        return cls(
            overall_impact=impact_dict.get('overall_impact', 0.0),
            category_impacts=impact_dict.get('category_impacts', {}),
            details=impact_dict.get('details', {})
        )
    
    def get_highest_impact_category(self) -> Optional[ImpactCategory]:
        """Get the category with highest impact.
        
        Returns:
            Highest impact category
        """
        if not self.category_impacts:
            return None
        
        highest_category = max(self.category_impacts.items(), key=lambda x: x[1])[0]
        try:
            return ImpactCategory(highest_category)
        except ValueError:
            return None
    
    def exceeds_threshold(self, threshold: float) -> bool:
        """Check if impact exceeds a specified threshold.
        
        Args:
            threshold: Threshold value to compare against
            
        Returns:
            True if impact exceeds threshold
        """
        return self.overall_impact > threshold
    
    def get_explanation(self) -> str:
        """Get human-readable explanation of impact analysis.
        
        Returns:
            Explanation of impact analysis
        """
        explanation = [
            f"Impact Analysis: {self.overall_impact:.2f} ({self.impact_level.name})",
            "\nCategory Impacts:"
        ]
        
        for category, score in self.category_impacts.items():
            explanation.append(f"  - {category}: {score:.2f}")
        
        if self.details:
            explanation.append("\nAdditional Details:")
            for key, value in self.details.items():
                explanation.append(f"  - {key}: {value}")
        
        return "\n".join(explanation)


def calculate_data_impact(action_type: HealingActionType, action_details: dict, context: dict) -> float:
    """Calculates the potential impact on data of a healing action.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        
    Returns:
        Data impact score between 0.0 and 1.0, where higher values indicate higher impact
    """
    try:
        # Base impact score based on action type
        if action_type == HealingActionType.DATA_CORRECTION:
            base_score = 0.7
        elif action_type == HealingActionType.SCHEMA_EVOLUTION:
            base_score = 0.8
        else:
            base_score = 0.3  # Lower base impact for non-data actions
        
        # Adjust based on data volume affected
        data_volume = context.get('data_volume', 0)
        volume_factor = min(1.0, data_volume / 1000000) * 0.2  # Scale by volume, up to 0.2
        
        # Adjust based on data criticality
        data_criticality = context.get('data_criticality', 'medium')
        criticality_factor = {
            'low': 0.0,
            'medium': 0.1,
            'high': 0.2
        }.get(data_criticality, 0.1)
        
        # Adjust based on data visibility/usage
        data_visibility = context.get('data_visibility', 'medium')
        visibility_factor = {
            'low': 0.0,
            'medium': 0.05,
            'high': 0.1
        }.get(data_visibility, 0.05)
        
        # Combine factors
        impact_score = base_score + volume_factor + criticality_factor + visibility_factor
        
        # Ensure score is between 0.0 and 1.0
        impact_score = max(0.0, min(1.0, impact_score))
        
        logger.debug(f"Data impact for {action_type.value}: {impact_score:.2f}")
        return impact_score
    except Exception as e:
        logger.error(f"Error calculating data impact: {e}")
        return 0.5  # Return moderate impact on error


def calculate_pipeline_impact(action_type: HealingActionType, action_details: dict, context: dict) -> float:
    """Calculates the potential impact on pipeline operations of a healing action.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        
    Returns:
        Pipeline impact score between 0.0 and 1.0, where higher values indicate higher impact
    """
    try:
        # Base impact score based on action type
        if action_type == HealingActionType.PIPELINE_RETRY:
            base_score = 0.5
        elif action_type == HealingActionType.PARAMETER_ADJUSTMENT:
            base_score = 0.6
        elif action_type == HealingActionType.DEPENDENCY_RESOLUTION:
            base_score = 0.7
        else:
            base_score = 0.3  # Lower base impact for non-pipeline actions
        
        # Adjust based on execution time impact
        execution_time_impact = context.get('execution_time_impact', 'medium')
        time_factor = {
            'low': 0.0,
            'medium': 0.1,
            'high': 0.2
        }.get(execution_time_impact, 0.1)
        
        # Adjust based on dependencies
        dependency_count = context.get('dependency_count', 0)
        dependency_factor = min(0.2, dependency_count / 20)  # Scale by dependencies, up to 0.2
        
        # Adjust based on pipeline criticality
        pipeline_criticality = context.get('pipeline_criticality', 'medium')
        criticality_factor = {
            'low': 0.0,
            'medium': 0.1,
            'high': 0.2
        }.get(pipeline_criticality, 0.1)
        
        # Combine factors
        impact_score = base_score + time_factor + dependency_factor + criticality_factor
        
        # Ensure score is between 0.0 and 1.0
        impact_score = max(0.0, min(1.0, impact_score))
        
        logger.debug(f"Pipeline impact for {action_type.value}: {impact_score:.2f}")
        return impact_score
    except Exception as e:
        logger.error(f"Error calculating pipeline impact: {e}")
        return 0.5  # Return moderate impact on error


def calculate_business_impact(action_type: HealingActionType, action_details: dict, context: dict) -> float:
    """Calculates the potential business impact of a healing action.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        
    Returns:
        Business impact score between 0.0 and 1.0, where higher values indicate higher impact
    """
    try:
        # Pipeline criticality is a major factor in business impact
        pipeline_criticality = context.get('pipeline_criticality', 'medium')
        base_score = {
            'low': 0.2,
            'medium': 0.5,
            'high': 0.8
        }.get(pipeline_criticality, 0.5)
        
        # Adjust based on SLA implications
        approaching_sla = context.get('approaching_sla', False)
        sla_factor = 0.2 if approaching_sla else 0.0
        
        # Adjust based on visibility to business users
        business_visibility = context.get('business_visibility', 'medium')
        visibility_factor = {
            'low': 0.0,
            'medium': 0.1,
            'high': 0.2
        }.get(business_visibility, 0.1)
        
        # Adjust based on potential reporting/analytics impact
        affects_reporting = context.get('affects_reporting', False)
        reporting_factor = 0.1 if affects_reporting else 0.0
        
        # Combine factors
        impact_score = base_score + sla_factor + visibility_factor + reporting_factor
        
        # Ensure score is between 0.0 and 1.0
        impact_score = max(0.0, min(1.0, impact_score))
        
        logger.debug(f"Business impact for {action_type.value}: {impact_score:.2f}")
        return impact_score
    except Exception as e:
        logger.error(f"Error calculating business impact: {e}")
        return 0.5  # Return moderate impact on error


def calculate_resource_impact(action_type: HealingActionType, action_details: dict, context: dict) -> float:
    """Calculates the potential resource impact of a healing action.
    
    Args:
        action_type: Type of healing action
        action_details: Details of the healing action
        context: Contextual information about the execution environment
        
    Returns:
        Resource impact score between 0.0 and 1.0, where higher values indicate higher impact
    """
    try:
        # Base impact score based on action type
        if action_type == HealingActionType.RESOURCE_SCALING:
            base_score = 0.7
            
            # Scale magnitude affects impact
            scale_factor = action_details.get('scale_factor', 1.0)
            if scale_factor > 2.0:
                base_score += 0.2
        else:
            base_score = 0.3  # Lower base impact for non-resource actions
        
        # Adjust based on compute requirements
        compute_impact = context.get('compute_impact', 'medium')
        compute_factor = {
            'low': 0.0,
            'medium': 0.1,
            'high': 0.2
        }.get(compute_impact, 0.1)
        
        # Adjust based on storage impact
        storage_impact = context.get('storage_impact', 'low')
        storage_factor = {
            'low': 0.0,
            'medium': 0.05,
            'high': 0.1
        }.get(storage_impact, 0.0)
        
        # Adjust based on cost impact
        cost_impact = context.get('cost_impact', 'low')
        cost_factor = {
            'low': 0.0,
            'medium': 0.05,
            'high': 0.1
        }.get(cost_impact, 0.0)
        
        # Combine factors
        impact_score = base_score + compute_factor + storage_factor + cost_factor
        
        # Ensure score is between 0.0 and 1.0
        impact_score = max(0.0, min(1.0, impact_score))
        
        logger.debug(f"Resource impact for {action_type.value}: {impact_score:.2f}")
        return impact_score
    except Exception as e:
        logger.error(f"Error calculating resource impact: {e}")
        return 0.5  # Return moderate impact on error


def calculate_overall_impact(impact_scores: dict, weights: dict) -> float:
    """Calculates the overall impact score based on individual impact categories.
    
    Args:
        impact_scores: Dictionary of impact scores by category
        weights: Dictionary of weights by category
        
    Returns:
        Overall impact score between 0.0 and 1.0
    """
    try:
        if not impact_scores or not weights:
            return 0.0
        
        # Calculate weighted sum
        weighted_sum = 0.0
        total_weight = 0.0
        
        for category, score in impact_scores.items():
            # Get weight for this category
            weight = weights.get(category, 0.0)
            if isinstance(weight, (int, float)):
                weighted_sum += score * weight
                total_weight += weight
        
        # Avoid division by zero
        if total_weight == 0:
            logger.warning("Total weight is zero, returning default impact score of 0.5")
            return 0.5
        
        # Calculate weighted average
        overall_impact = weighted_sum / total_weight
        
        # Ensure score is between 0.0 and 1.0
        overall_impact = max(0.0, min(1.0, overall_impact))
        
        return overall_impact
    except Exception as e:
        logger.error(f"Error calculating overall impact: {e}")
        return 0.5  # Return moderate impact on error


def get_impact_threshold(action_type: HealingActionType, context: dict) -> float:
    """Gets the impact threshold for a specific action type and context.
    
    Args:
        action_type: Type of healing action
        context: Contextual information about the execution environment
        
    Returns:
        Impact threshold value between 0.0 and 1.0
    """
    try:
        # Get configuration
        config = get_config()
        
        # Get base threshold from configuration or use default
        base_threshold = config.get(
            f"self_healing.impact_thresholds.{action_type.value}", 
            DEFAULT_IMPACT_THRESHOLD
        )
        
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
        
        # Ensure threshold is between 0.0 and 1.0
        threshold = max(0.0, min(1.0, threshold))
        
        logger.debug(f"Impact threshold for {action_type.value}: {threshold:.2f}")
        return threshold
    except Exception as e:
        logger.error(f"Error getting impact threshold: {e}")
        return DEFAULT_IMPACT_THRESHOLD  # Return default threshold on error


class ImpactAnalyzer:
    """Main class for analyzing the potential impact of healing actions."""
    
    def __init__(self, risk_manager: RiskManager, config: dict = None):
        """Initialize the impact analyzer with configuration.
        
        Args:
            risk_manager: Risk manager for risk assessment integration
            config: Optional configuration overrides
        """
        self._config = config or get_config()
        self._risk_manager = risk_manager
        
        # Set impact weights from config or defaults
        self._impact_weights = self._config.get(
            "self_healing.impact_weights", 
            DEFAULT_IMPACT_WEIGHTS
        )
        
        # Set threshold values from config or defaults
        self._default_threshold = self._config.get(
            "self_healing.default_impact_threshold", 
            DEFAULT_IMPACT_THRESHOLD
        )
        
        self._critical_threshold = self._config.get(
            "self_healing.critical_impact_threshold", 
            DEFAULT_CRITICAL_IMPACT_THRESHOLD
        )
        
        logger.debug("ImpactAnalyzer initialized")
    
    def analyze_impact(self, action_type: HealingActionType, 
                     action_details: dict, context: dict) -> ImpactAnalysis:
        """Analyze the potential impact of a healing action.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Impact analysis results
        """
        try:
            # Calculate impact for each category
            data_impact = self.calculate_data_impact(action_type, action_details, context)
            pipeline_impact = self.calculate_pipeline_impact(action_type, action_details, context)
            business_impact = self.calculate_business_impact(action_type, action_details, context)
            resource_impact = self.calculate_resource_impact(action_type, action_details, context)
            
            # Create dictionary of category impacts
            category_impacts = {
                ImpactCategory.DATA.value: data_impact,
                ImpactCategory.PIPELINE.value: pipeline_impact,
                ImpactCategory.BUSINESS.value: business_impact,
                ImpactCategory.RESOURCE.value: resource_impact
            }
            
            # Calculate overall impact
            overall_impact = self.calculate_overall_impact(category_impacts)
            
            # Additional context for the impact analysis
            details = {
                'action_type': action_type.value,
                'timestamp': datetime.datetime.now().isoformat(),
                'environment': context.get('environment', 'unknown'),
                'highest_impact_category': max(category_impacts.items(), key=lambda x: x[1])[0]
            }
            
            # If alert severity is in context, add it to details
            if 'alert_severity' in context:
                details['alert_severity'] = context['alert_severity']
            
            # Create impact analysis object
            impact_analysis = ImpactAnalysis(
                overall_impact=overall_impact,
                category_impacts=category_impacts,
                details=details
            )
            
            logger.info(f"Impact analysis for {action_type.value}: "
                      f"score={overall_impact:.2f}, level={impact_analysis.impact_level.name}")
            logger.debug(f"Category impacts: {category_impacts}")
            
            return impact_analysis
        except Exception as e:
            logger.error(f"Error analyzing impact: {e}")
            # Return a default impact analysis with moderate impact
            return ImpactAnalysis(
                overall_impact=0.5,
                category_impacts={
                    ImpactCategory.DATA.value: 0.5,
                    ImpactCategory.PIPELINE.value: 0.5,
                    ImpactCategory.BUSINESS.value: 0.5,
                    ImpactCategory.RESOURCE.value: 0.5
                },
                details={'error': str(e), 'action_type': action_type.value}
            )
    
    def calculate_data_impact(self, action_type: HealingActionType, 
                            action_details: dict, context: dict) -> float:
        """Calculate data impact score.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Data impact score
        """
        return calculate_data_impact(action_type, action_details, context)
    
    def calculate_pipeline_impact(self, action_type: HealingActionType, 
                                action_details: dict, context: dict) -> float:
        """Calculate pipeline impact score.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Pipeline impact score
        """
        return calculate_pipeline_impact(action_type, action_details, context)
    
    def calculate_business_impact(self, action_type: HealingActionType, 
                                action_details: dict, context: dict) -> float:
        """Calculate business impact score.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Business impact score
        """
        return calculate_business_impact(action_type, action_details, context)
    
    def calculate_resource_impact(self, action_type: HealingActionType, 
                                action_details: dict, context: dict) -> float:
        """Calculate resource impact score.
        
        Args:
            action_type: Type of healing action
            action_details: Details of the healing action
            context: Contextual information about the execution environment
            
        Returns:
            Resource impact score
        """
        return calculate_resource_impact(action_type, action_details, context)
    
    def calculate_overall_impact(self, category_impacts: dict) -> float:
        """Calculate overall impact from category scores.
        
        Args:
            category_impacts: Dictionary of impact scores by category
            
        Returns:
            Overall impact score
        """
        return calculate_overall_impact(category_impacts, self._impact_weights)
    
    def get_impact_level(self, impact_score: float) -> ImpactLevel:
        """Determine impact level from impact score.
        
        Args:
            impact_score: Impact score between 0.0 and 1.0
            
        Returns:
            Impact level enum value
        """
        if impact_score < 0.3:
            return ImpactLevel.LOW
        elif impact_score < 0.6:
            return ImpactLevel.MEDIUM
        elif impact_score < 0.8:
            return ImpactLevel.HIGH
        else:
            return ImpactLevel.CRITICAL
    
    def get_impact_threshold(self, action_type: HealingActionType, context: dict) -> float:
        """Get impact threshold for action and context.
        
        Args:
            action_type: Type of healing action
            context: Contextual information about the execution environment
            
        Returns:
            Impact threshold
        """
        return get_impact_threshold(action_type, context)
    
    def exceeds_threshold(self, impact: ImpactAnalysis, 
                         action_type: HealingActionType, context: dict) -> bool:
        """Check if impact exceeds threshold for action.
        
        Args:
            impact: Impact analysis results
            action_type: Type of healing action
            context: Contextual information about the execution environment
            
        Returns:
            True if impact exceeds threshold
        """
        threshold = self.get_impact_threshold(action_type, context)
        return impact.exceeds_threshold(threshold)
    
    def reload_config(self) -> bool:
        """Reload configuration settings.
        
        Returns:
            Success status
        """
        try:
            self._config = get_config()
            
            # Reload impact weights and thresholds
            self._impact_weights = self._config.get(
                "self_healing.impact_weights", 
                DEFAULT_IMPACT_WEIGHTS
            )
            
            self._default_threshold = self._config.get(
                "self_healing.default_impact_threshold", 
                DEFAULT_IMPACT_THRESHOLD
            )
            
            self._critical_threshold = self._config.get(
                "self_healing.critical_impact_threshold", 
                DEFAULT_CRITICAL_IMPACT_THRESHOLD
            )
            
            logger.info("ImpactAnalyzer configuration reloaded")
            return True
        except Exception as e:
            logger.error(f"Error reloading ImpactAnalyzer configuration: {e}")
            return False