"""
Implements quality scoring functionality for data validation results.

This module calculates quality scores based on validation results using different scoring
models and weighting strategies. It provides a flexible framework for assessing overall
data quality across different dimensions.
"""

import enum
from typing import Dict, List, Optional, Union, Any

from ...constants import QualityDimension
from ...config import get_config
from ...utils.logging.logger import get_logger

# Configure module logger
logger = get_logger(__name__)

# Default threshold for quality scores
DEFAULT_QUALITY_THRESHOLD = 0.8

# Default weights for quality dimensions
DEFAULT_DIMENSION_WEIGHTS = {
    QualityDimension.COMPLETENESS: 0.25,
    QualityDimension.ACCURACY: 0.25,
    QualityDimension.CONSISTENCY: 0.2,
    QualityDimension.TIMELINESS: 0.15,
    QualityDimension.UNIQUENESS: 0.15
}


class ScoringModel(enum.Enum):
    """Enumeration of available quality scoring models"""
    SIMPLE = "SIMPLE"
    WEIGHTED = "WEIGHTED"
    IMPACT = "IMPACT"
    ADAPTIVE = "ADAPTIVE"


def calculate_simple_score(validation_results: List[Dict[str, Any]]) -> float:
    """
    Calculates a simple quality score based on pass/fail ratio
    
    Args:
        validation_results: List of validation results
        
    Returns:
        Quality score between 0.0 and 1.0
    """
    if not validation_results:
        return 0.0
        
    total_rules = len(validation_results)
    passed_rules = sum(1 for result in validation_results if result.get('passed', False))
    
    return passed_rules / total_rules if total_rules > 0 else 0.0


def calculate_weighted_score(validation_results: List[Dict[str, Any]], 
                            dimension_weights: Optional[Dict[QualityDimension, float]] = None) -> float:
    """
    Calculates a quality score with dimension-based weighting
    
    Args:
        validation_results: List of validation results
        dimension_weights: Weights for each quality dimension
        
    Returns:
        Weighted quality score between 0.0 and 1.0
    """
    if not validation_results:
        return 0.0
        
    # Use default weights if none provided
    if dimension_weights is None:
        dimension_weights = DEFAULT_DIMENSION_WEIGHTS
        
    # Normalize weights to ensure they sum to 1.0
    dimension_weights = normalize_weights(dimension_weights)
    
    # Group validation results by dimension
    dimension_results = _group_by_dimension(validation_results)
    
    # Calculate score for each dimension
    dimension_scores = {}
    for dimension, results in dimension_results.items():
        if not results:
            dimension_scores[dimension] = 1.0  # No results means perfect score
        else:
            passed = sum(1 for r in results if r.get('passed', False))
            dimension_scores[dimension] = passed / len(results)
    
    # Apply weights to each dimension
    weighted_score = 0.0
    for dimension, weight in dimension_weights.items():
        if dimension in dimension_scores:
            weighted_score += dimension_scores[dimension] * weight
            
    return weighted_score


def calculate_impact_score(validation_results: List[Dict[str, Any]], 
                          impact_factors: Optional[Dict[str, float]] = None) -> float:
    """
    Calculates a quality score based on business impact of failures
    
    Args:
        validation_results: List of validation results
        impact_factors: Impact factors for validation rules
        
    Returns:
        Impact-based quality score between 0.0 and 1.0
    """
    if not validation_results:
        return 0.0
        
    # Default impact factor is 1.0 if not specified
    if impact_factors is None:
        impact_factors = {}
        
    total_impact = 0.0
    failed_impact = 0.0
    
    for result in validation_results:
        rule_id = result.get('rule_id')
        impact = impact_factors.get(rule_id, 1.0)
        
        total_impact += impact
        if not result.get('passed', False):
            failed_impact += impact
    
    # Calculate score (1 - failure ratio)
    if total_impact == 0:
        return 0.0
        
    return 1.0 - (failed_impact / total_impact)


def normalize_weights(weights: Dict[Any, float]) -> Dict[Any, float]:
    """
    Normalizes weights to ensure they sum to 1.0
    
    Args:
        weights: Dictionary of weights
        
    Returns:
        Normalized weights dictionary
    """
    if not weights:
        return {}
        
    total = sum(weights.values())
    
    if total == 0:
        # If sum is zero, return equal weights
        equal_weight = 1.0 / len(weights)
        return {k: equal_weight for k in weights}
        
    # Normalize by dividing each weight by the total
    return {k: v / total for k, v in weights.items()}


def _group_by_dimension(validation_results: List[Dict[str, Any]]) -> Dict[QualityDimension, List[Dict[str, Any]]]:
    """
    Group validation results by quality dimension
    
    Args:
        validation_results: List of validation results
        
    Returns:
        Results grouped by dimension
    """
    dimension_groups = {dim: [] for dim in QualityDimension}
    
    for result in validation_results:
        dimension = result.get('dimension')
        if dimension in dimension_groups:
            dimension_groups[dimension].append(result)
            
    return dimension_groups


class QualityScorer:
    """
    Calculates quality scores from validation results using configurable scoring models
    """
    
    def __init__(self, model: Optional[ScoringModel] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the quality scorer with a scoring model and configuration
        
        Args:
            model: Scoring model to use
            config: Configuration parameters
        """
        # Use weighted model as default if none specified
        self._model = model or ScoringModel.WEIGHTED
        
        # Initialize configuration with defaults and override with provided config
        self._config = {}
        if config:
            self._config.update(config)
        
        # Load dimension weights from config or use defaults
        self._dimension_weights = self._config.get('dimension_weights', DEFAULT_DIMENSION_WEIGHTS)
        
        # Load impact factors from config or use defaults
        self._impact_factors = self._config.get('impact_factors', {})
        
        # Set quality threshold from config or use default
        self._quality_threshold = float(self._config.get('quality_threshold', DEFAULT_QUALITY_THRESHOLD))
        
        # Normalize weights to ensure they sum to 1.0
        self._dimension_weights = normalize_weights(self._dimension_weights)
        
        logger.info(f"Initialized QualityScorer with model {self._model}")
    
    def calculate_score(self, validation_results: List[Dict[str, Any]]) -> float:
        """
        Calculate quality score using the configured scoring model
        
        Args:
            validation_results: List of validation results
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        logger.info(f"Calculating quality score using {self._model} model")
        
        if not validation_results:
            logger.warning("No validation results provided, returning zero score")
            return 0.0
        
        # Select appropriate scoring method based on model
        if self._model == ScoringModel.SIMPLE:
            score = calculate_simple_score(validation_results)
        elif self._model == ScoringModel.WEIGHTED:
            score = calculate_weighted_score(validation_results, self._dimension_weights)
        elif self._model == ScoringModel.IMPACT:
            score = calculate_impact_score(validation_results, self._impact_factors)
        elif self._model == ScoringModel.ADAPTIVE:
            # For adaptive model, select the most appropriate model based on data
            selected_model = self._select_adaptive_model(validation_results)
            
            if selected_model == ScoringModel.SIMPLE:
                score = calculate_simple_score(validation_results)
            elif selected_model == ScoringModel.WEIGHTED:
                score = calculate_weighted_score(validation_results, self._dimension_weights)
            else:  # Default to impact model
                score = calculate_impact_score(validation_results, self._impact_factors)
        else:
            # Default to simple score for unknown models
            logger.warning(f"Unknown scoring model {self._model}, using SIMPLE")
            score = calculate_simple_score(validation_results)
        
        logger.info(f"Calculated quality score: {score:.4f}")
        return score
    
    def set_model(self, model: ScoringModel) -> None:
        """
        Set the scoring model to use
        
        Args:
            model: Scoring model
        """
        if not isinstance(model, ScoringModel):
            raise ValueError(f"Model must be a ScoringModel, got {type(model)}")
            
        self._model = model
        logger.info(f"Set scoring model to {model}")
    
    def set_dimension_weights(self, weights: Dict[QualityDimension, float]) -> None:
        """
        Set custom weights for quality dimensions
        
        Args:
            weights: Dimension weights dictionary
        """
        if not isinstance(weights, dict):
            raise ValueError("Weights must be a dictionary")
            
        # Normalize weights to sum to 1.0
        self._dimension_weights = normalize_weights(weights)
        logger.info(f"Set dimension weights: {self._dimension_weights}")
    
    def set_impact_factors(self, impact_factors: Dict[str, float]) -> None:
        """
        Set impact factors for validation rules
        
        Args:
            impact_factors: Impact factors for rules
        """
        if not isinstance(impact_factors, dict):
            raise ValueError("Impact factors must be a dictionary")
            
        self._impact_factors = impact_factors
        logger.info(f"Set impact factors for {len(impact_factors)} rules")
    
    def set_quality_threshold(self, threshold: float) -> None:
        """
        Set the quality threshold for pass/fail determination
        
        Args:
            threshold: Quality threshold (0.0 to 1.0)
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Quality threshold must be between 0.0 and 1.0")
            
        self._quality_threshold = threshold
        logger.info(f"Set quality threshold to {threshold}")
    
    def get_quality_threshold(self) -> float:
        """
        Get the current quality threshold
        
        Returns:
            Current quality threshold
        """
        return self._quality_threshold
    
    def passes_threshold(self, score: float) -> bool:
        """
        Check if a quality score passes the threshold
        
        Args:
            score: Quality score to check
            
        Returns:
            True if score passes threshold
        """
        return score >= self._quality_threshold
    
    def _select_adaptive_model(self, validation_results: List[Dict[str, Any]]) -> ScoringModel:
        """
        Select the best scoring model based on data characteristics
        
        Args:
            validation_results: List of validation results
            
        Returns:
            Selected scoring model
        """
        # Check if we have dimension information for weighted scoring
        has_dimensions = False
        for result in validation_results:
            if 'dimension' in result:
                has_dimensions = True
                break
                
        # Check if we have impact factors for impact-based scoring
        has_impact_factors = bool(self._impact_factors) and any(
            result.get('rule_id') in self._impact_factors for result in validation_results
        )
        
        # Select model based on available information
        if has_impact_factors:
            selected_model = ScoringModel.IMPACT
        elif has_dimensions:
            selected_model = ScoringModel.WEIGHTED
        else:
            selected_model = ScoringModel.SIMPLE
            
        logger.info(f"Adaptive model selected: {selected_model}")
        return selected_model
    
    def _group_by_dimension(self, validation_results: List[Dict[str, Any]]) -> Dict[QualityDimension, List[Dict[str, Any]]]:
        """
        Group validation results by quality dimension
        
        Args:
            validation_results: List of validation results
            
        Returns:
            Results grouped by dimension
        """
        # Use the standalone function for implementation
        return _group_by_dimension(validation_results)