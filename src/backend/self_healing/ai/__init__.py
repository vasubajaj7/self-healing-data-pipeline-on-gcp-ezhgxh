"""
Initializes the AI module for the self-healing data pipeline. This module
provides AI-driven capabilities for issue classification, root cause analysis,
pattern recognition, and predictive analytics to enable autonomous detection
and correction of pipeline issues.
"""

# Import internal modules for AI capabilities
from src.backend.self_healing.ai import issue_classifier  # Import issue classification capabilities
from src.backend.self_healing.ai import root_cause_analyzer  # Import root cause analysis capabilities
from src.backend.self_healing.ai import pattern_recognizer  # Import pattern recognition capabilities
from src.backend.self_healing.ai import predictive_analyzer  # Import predictive analysis capabilities

# Import logging utility for AI module
from src.backend.utils.logging import logger as logging_util  # Configure logging for the AI module

# Configure logger for this module
logger = logging_util.get_logger(__name__)

# Define the version of the AI module
__version__ = "1.0.0"

# Expose key classes and functions for external use
IssueClassification = issue_classifier.IssueClassification  # Represent a classification result for an issue
IssueClassifier = issue_classifier.IssueClassifier  # Classify pipeline and data quality issues using AI models
RootCause = root_cause_analyzer.RootCause  # Represent an identified root cause for an issue
RootCauseAnalysis = root_cause_analyzer.RootCauseAnalysis  # Represent a complete root cause analysis for an issue
RootCauseAnalyzer = root_cause_analyzer.RootCauseAnalyzer  # Analyze root causes of pipeline and data quality issues
Pattern = pattern_recognizer.Pattern  # Represent a recognized pattern of issues or failures
PatternRecognizer = pattern_recognizer.PatternRecognizer  # Recognize patterns in issues and failures for self-healing
Prediction = predictive_analyzer.Prediction  # Represent a predicted potential issue or failure
PredictiveAnalyzer = predictive_analyzer.PredictiveAnalyzer  # Predict potential issues and failures in the pipeline

# Export utility functions for external use
extract_features_from_error = issue_classifier.extract_features_from_error  # Extract features from error data for classification
calculate_confidence_score = issue_classifier.calculate_confidence_score  # Calculate confidence score for classification results
map_to_healing_action = issue_classifier.map_to_healing_action  # Map issue types to appropriate healing actions
serialize_classification = issue_classifier.serialize_classification  # Serialize classification to JSON format
deserialize_classification = issue_classifier.deserialize_classification  # Deserialize classification from JSON format

extract_causal_features = root_cause_analyzer.extract_causal_features  # Extract features from issue data for causality analysis
build_causality_graph = root_cause_analyzer.build_causality_graph  # Build a graph representing causal relationships between events
calculate_cause_confidence = root_cause_analyzer.calculate_cause_confidence  # Calculate confidence score for identified root causes
find_common_causes = root_cause_analyzer.find_common_causes  # Identify common causes across multiple related issues
serialize_root_cause_analysis = root_cause_analyzer.serialize_root_cause_analysis  # Serialize a root cause analysis to JSON format
deserialize_root_cause_analysis = root_cause_analyzer.deserialize_root_cause_analysis  # Deserialize a root cause analysis from JSON format

calculate_similarity_score = pattern_recognizer.calculate_similarity_score  # Calculate similarity between issues and patterns
extract_pattern_features = pattern_recognizer.extract_pattern_features  # Extract features from issues for pattern matching
serialize_pattern = pattern_recognizer.serialize_pattern  # Serialize patterns for storage
deserialize_pattern = pattern_recognizer.deserialize_pattern  # Deserialize patterns from storage

preprocess_time_series_data = predictive_analyzer.preprocess_time_series_data  # Preprocess time series data for prediction models
calculate_prediction_confidence = predictive_analyzer.calculate_prediction_confidence  # Calculate confidence score for prediction results
serialize_prediction = predictive_analyzer.serialize_prediction  # Serialize predictions for storage
deserialize_prediction = predictive_analyzer.deserialize_prediction  # Deserialize predictions from storage
extract_seasonal_patterns = predictive_analyzer.extract_seasonal_patterns  # Extract seasonal patterns from time series data
detect_trend = predictive_analyzer.detect_trend  # Detect trend in time series data