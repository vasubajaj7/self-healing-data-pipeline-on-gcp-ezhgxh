"""
Initialization file for the self-healing learning module which provides functionality for collecting feedback, analyzing effectiveness, managing knowledge, and training models to improve the self-healing capabilities of the data pipeline. This module serves as the learning and improvement engine for the self-healing system.
"""

from .feedback_collector import Feedback, FeedbackCollector, serialize_feedback, deserialize_feedback, calculate_feedback_impact
from .effectiveness_analyzer import AnalysisStorageProvider, InMemoryAnalysisStorage, FeedbackData, EffectivenessMetric, ImprovementRecommendation, EffectivenessAnalysis, EffectivenessAnalyzer, calculate_effectiveness_metrics, analyze_failure_patterns, generate_improvement_recommendations, serialize_analysis, deserialize_analysis
from .knowledge_base import KnowledgeEntry, IssueKnowledge, PatternKnowledge, CorrectionKnowledge, EffectivenessKnowledge, KnowledgeBase, KnowledgeStorageProvider, FirestoreKnowledgeStorage, serialize_knowledge_entry, deserialize_knowledge_entry, calculate_knowledge_relevance
from .model_trainer import TrainingConfig, TrainingRun, ModelTrainer, prepare_training_data, evaluate_model_performance, compare_model_versions, generate_model_metadata

__all__ = [
    "Feedback",
    "FeedbackCollector",
    "serialize_feedback",
    "deserialize_feedback",
    "calculate_feedback_impact",
    "AnalysisStorageProvider",
    "InMemoryAnalysisStorage",
    "FeedbackData",
    "EffectivenessMetric",
    "ImprovementRecommendation",
    "EffectivenessAnalysis",
    "EffectivenessAnalyzer",
    "calculate_effectiveness_metrics",
    "analyze_failure_patterns",
    "generate_improvement_recommendations",
    "serialize_analysis",
    "deserialize_analysis",
    "KnowledgeEntry",
    "IssueKnowledge",
    "PatternKnowledge",
    "CorrectionKnowledge",
    "EffectivenessKnowledge",
    "KnowledgeBase",
    "KnowledgeStorageProvider",
    "FirestoreKnowledgeStorage",
    "serialize_knowledge_entry",
    "deserialize_knowledge_entry",
    "calculate_knowledge_relevance",
    "TrainingConfig",
    "TrainingRun",
    "ModelTrainer",
    "prepare_training_data",
    "evaluate_model_performance",
    "compare_model_versions",
    "generate_model_metadata"
]