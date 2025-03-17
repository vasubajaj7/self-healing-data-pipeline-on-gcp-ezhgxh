"""
Initialization file for the query optimization module that exposes key classes and functions
for BigQuery query analysis, optimization, and performance prediction. This module is part of the
self-healing data pipeline's performance optimization layer.
"""

# Internal imports
from .pattern_identifier import PatternIdentifier, QueryPattern  # src/backend/optimization/query/pattern_identifier.py
from .query_analyzer import QueryAnalyzer, QueryAnalysisResult, OptimizationRecommendation  # src/backend/optimization/query/query_analyzer.py
from .query_optimizer import QueryOptimizer, OptimizationResult, OPTIMIZATION_TECHNIQUES  # src/backend/optimization/query/query_optimizer.py
from .performance_predictor import PerformancePredictor, QueryPerformanceMetrics  # src/backend/optimization/query/performance_predictor.py

__all__ = [
    'PatternIdentifier',
    'QueryPattern',
    'QueryAnalyzer',
    'QueryAnalysisResult',
    'OptimizationRecommendation',
    'QueryOptimizer',
    'OptimizationResult',
    'OPTIMIZATION_TECHNIQUES',
    'PerformancePredictor',
    'QueryPerformanceMetrics'
]