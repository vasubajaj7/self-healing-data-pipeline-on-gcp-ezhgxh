"""
Initialization module for the self-healing AI model management system.
This module imports and exposes key classes and functions from the model management, registry, evaluation, and serving modules, providing a unified interface for working with machine learning models in the self-healing pipeline.
"""

# Import model management classes
from .model_manager import ModelManager, Model, ModelVersion, ModelMetadata  # src/backend/self_healing/models/model_manager.py

# Import model registry functionality
from .model_registry import ModelRegistry, serialize_model_metadata, deserialize_model_metadata, get_model_path  # src/backend/self_healing/models/model_registry.py

# Import model evaluation functionality
from .model_evaluation import ModelEvaluator, ModelEvaluationResult, serialize_evaluation_result, deserialize_evaluation_result  # src/backend/self_healing/models/model_evaluation.py

# Import model serving functionality
from .model_serving import ModelServer, ModelPrediction, ModelServingCache, serialize_prediction, deserialize_prediction  # src/backend/self_healing/models/model_serving.py

__all__ = [
    "ModelManager",  # Main class for managing ML models used in the self-healing pipeline
    "Model",  # Represents a model with multiple versions
    "ModelVersion",  # Represents a specific version of a model with its artifacts and metadata
    "ModelMetadata",  # Represents metadata for a model version
    "ModelRegistry",  # Central registry for managing AI models used in the self-healing pipeline
    "ModelEvaluator",  # Main class for evaluating machine learning models
    "ModelEvaluationResult",  # Represents the result of a model evaluation run
    "ModelServer",  # Main class for serving machine learning models and making predictions
    "ModelPrediction",  # Represents a prediction result from a model
    "ModelServingCache",  # Cache for model serving to improve prediction performance
    "serialize_model_metadata",  # Serializes model metadata to JSON format
    "deserialize_model_metadata",  # Deserializes model metadata from JSON format
    "get_model_path",  # Constructs the path to a model artifact
    "serialize_evaluation_result",  # Serializes model evaluation result to JSON format
    "deserialize_evaluation_result",  # Deserializes model evaluation result from JSON format
    "serialize_prediction",  # Serializes a prediction result to JSON format
    "deserialize_prediction",  # Deserializes a prediction result from JSON format
]