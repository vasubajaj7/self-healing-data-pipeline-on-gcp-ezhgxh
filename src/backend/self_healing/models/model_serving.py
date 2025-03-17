"""
Implements model serving capabilities for the self-healing AI engine. This module provides a unified interface for making predictions with machine learning models, supporting both local model inference and Vertex AI-based serving. It handles model loading, prediction requests, and result processing for various model types used in the self-healing process.
"""

import typing  # standard library
import os  # standard library
import json  # standard library
from time import time  # standard library

import numpy as np  # version: 1.24.x
import tensorflow as tf  # version: 2.12.x

from src.backend.utils.logging.logger import get_logger  # Internal import
from src.backend.utils.ml import model_utils  # Internal import
from src.backend.utils.ml.vertex_client import VertexAIClient, predict_with_vertex, format_vertex_request, parse_vertex_response  # Internal import
from src.backend.self_healing.models import model_registry  # Internal import
from src.backend.self_healing.models.model_registry import ModelRegistry  # Internal import
from src.backend.config import get_config  # Internal import

# Initialize logger
logger = get_logger(__name__)

# Default prediction timeout
DEFAULT_PREDICTION_TIMEOUT = 30

# Model type mapping
MODEL_TYPE_MAPPING = {"issue_classifier": "classification", "root_cause_analyzer": "classification", "pattern_recognizer": "classification", "data_corrector": "regression", "anomaly_detector": "anomaly"}


class ModelPrediction:
    """
    Represents a prediction result from a model
    """

    def __init__(self, model_id: str, model_version: str, model_type: str, input_data: dict, prediction: dict, confidence: float, latency: float, metadata: dict):
        """
        Initialize a model prediction with its properties

        Args:
            model_id: ID of the model
            model_version: ID of the version
            model_type: Type of the model
            input_data: Input data used for prediction
            prediction: Prediction result
            confidence: Confidence score for the prediction
            latency: Latency of the prediction
            metadata: Additional metadata
        """
        self.prediction_id = str(uuid.uuid4())
        self.model_id = model_id
        self.model_version = model_version
        self.model_type = model_type
        self.input_data = input_data
        self.prediction = prediction
        self.confidence = confidence
        self.latency = latency
        self.metadata = metadata
        self.timestamp = datetime.datetime.now()

    def to_dict(self) -> dict:
        """
        Convert prediction to dictionary representation

        Returns:
            Dictionary representation of prediction
        """
        return {
            "prediction_id": self.prediction_id,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "model_type": self.model_type,
            "input_data": self.input_data,
            "prediction": self.prediction,
            "confidence": self.confidence,
            "latency": self.latency,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat()
        }

    @classmethod
    def from_dict(cls, prediction_dict: dict) -> 'ModelPrediction':
        """
        Create ModelPrediction from dictionary representation

        Args:
            prediction_dict: Dictionary representation of prediction

        Returns:
            ModelPrediction instance
        """
        return cls(
            model_id=prediction_dict["model_id"],
            model_version=prediction_dict["model_version"],
            model_type=prediction_dict["model_type"],
            input_data=prediction_dict["input_data"],
            prediction=prediction_dict["prediction"],
            confidence=prediction_dict["confidence"],
            latency=prediction_dict["latency"],
            metadata=prediction_dict["metadata"]
        )

    def get_summary(self) -> dict:
        """
        Get a summary of the prediction result

        Returns:
            Summary dictionary with key prediction information
        """
        return {
            "prediction_id": self.prediction_id,
            "model_id": self.model_id,
            "model_type": self.model_type,
            "model_version": self.model_version,
            "prediction": self.prediction,
            "confidence": self.confidence,
            "latency": self.latency
        }


class ModelServer:
    """
    Main class for serving machine learning models and making predictions
    """

    def __init__(self, config: dict):
        """
        Initialize the model server with configuration

        Args:
            config: Configuration dictionary
        """
        self._config = config
        self._model_registry = ModelRegistry(config)
        self._loaded_models = {}
        self._use_vertex_ai = config.get("model_serving.use_vertex_ai", False)
        self._vertex_client = VertexAIClient(config) if self._use_vertex_ai else None
        self._prediction_history = {}

    def predict(self, model_id: str, input_data: dict, version_id: str = "latest") -> ModelPrediction:
        """
        Make a prediction using a specified model

        Args:
            model_id: ID of the model
            input_data: Input data for prediction
            version_id: ID of the version

        Returns:
            Prediction result
        """
        model_info = self._model_registry.get_model(model_id)
        if not model_info:
            raise ValueError(f"Model with ID {model_id} not found")

        model_type = model_info["type"]

        formatted_input = format_prediction_input(input_data, model_type)

        start_time = time()
        if self._use_vertex_ai:
            raw_prediction = self.predict_with_vertex_ai(model_id, formatted_input, version_id)
        else:
            raw_prediction = self.predict_with_local_model(model_id, formatted_input, version_id)
        end_time = time()

        latency = end_time - start_time

        prediction = format_prediction_output(raw_prediction, model_type)
        confidence = self.calculate_confidence(prediction, model_type)

        model_prediction = ModelPrediction(
            model_id=model_id,
            model_version=version_id,
            model_type=model_type,
            input_data=input_data,
            prediction=prediction,
            confidence=confidence,
            latency=latency,
            metadata={}
        )

        self._update_prediction_history(model_prediction)

        return model_prediction

    def predict_with_local_model(self, model_id: str, formatted_input: dict, version_id: str) -> object:
        """
        Make a prediction using a locally loaded model

        Args:
            model_id: ID of the model
            formatted_input: Formatted input data for the model
            version_id: ID of the version

        Returns:
            Raw model output
        """
        model = self._ensure_model_loaded(model_id, version_id)
        # Prepare input in the format expected by the model
        input_tensor = tf.convert_to_tensor([formatted_input])
        # Execute prediction with model
        raw_prediction = model(input_tensor).numpy()
        return raw_prediction

    def predict_with_vertex_ai(self, model_id: str, formatted_input: dict, version_id: str) -> object:
        """
        Make a prediction using Vertex AI

        Args:
            model_id: ID of the model
            formatted_input: Formatted input data for Vertex AI
            version_id: ID of the version

        Returns:
            Raw model output
        """
        model_info = self._model_registry.get_model(model_id)
        if not model_info:
            raise ValueError(f"Model with ID {model_id} not found")

        endpoint_id = model_info.get("endpoint_id")
        if not endpoint_id:
            raise ValueError(f"Endpoint ID not found for model {model_id}")

        # Format input for Vertex AI prediction
        vertex_request = format_vertex_request(formatted_input)

        # Call predict_with_vertex function
        raw_prediction = predict_with_vertex(endpoint_id, vertex_request)

        # Parse and return prediction results
        parsed_prediction = parse_vertex_response(raw_prediction)
        return parsed_prediction

    def batch_predict(self, model_id: str, input_data_list: list, version_id: str = "latest") -> list:
        """
        Make batch predictions using a specified model

        Args:
            model_id: ID of the model
            input_data_list: List of input data for prediction
            version_id: ID of the version

        Returns:
            List of ModelPrediction results
        """
        if not isinstance(input_data_list, list):
            raise ValueError("input_data_list must be a list")

        predictions = []
        for input_data in input_data_list:
            prediction = self.predict(model_id, input_data, version_id)
            predictions.append(prediction)

        return predictions

    def load_model(self, model_id: str, version_id: str) -> bool:
        """
        Load a model into memory for local prediction

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            True if model loaded successfully
        """
        if self._use_vertex_ai:
            logger.info("Skipping local model loading, using Vertex AI exclusively")
            return True

        model_info = self._model_registry.get_model(model_id)
        if not model_info:
            raise ValueError(f"Model with ID {model_id} not found")

        version_info = self._model_registry.get_model_version(model_id, version_id)
        if not version_info:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        model_path = version_info["artifact_path"]

        model = model_utils.load_model(model_path)
        self._loaded_models[(model_id, version_id)] = model

        logger.info(f"Model {model_id} version {version_id} loaded successfully")
        return True

    def unload_model(self, model_id: str, version_id: str) -> bool:
        """
        Unload a model from memory

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            True if model unloaded successfully
        """
        if (model_id, version_id) in self._loaded_models:
            del self._loaded_models[(model_id, version_id)]
            logger.info(f"Model {model_id} version {version_id} unloaded successfully")
            return True
        else:
            logger.warning(f"Model {model_id} version {version_id} not loaded")
            return False

    def get_loaded_models(self) -> list:
        """
        Get information about currently loaded models

        Returns:
            List of loaded model information
        """
        loaded_models_info = []
        for (model_id, version_id), model in self._loaded_models.items():
            model_info = {
                "model_id": model_id,
                "version_id": version_id,
                "memory_usage": sys.getsizeof(model)  # Approximate memory usage
            }
            loaded_models_info.append(model_info)
        return loaded_models_info

    def get_prediction_history(self, filters: dict = None) -> list:
        """
        Get prediction history with optional filtering

        Args:
            filters: Dictionary of filters to apply

        Returns:
            Filtered prediction history
        """
        # Apply filters to _prediction_history if provided
        if filters:
            filtered_history = []
            for prediction_id, prediction in self._prediction_history.items():
                # Apply filter logic here based on filter criteria
                # Example: filter by model_id
                if filters.get("model_id") and prediction.model_id != filters["model_id"]:
                    continue
                filtered_history.append(prediction)
            return filtered_history
        else:
            return list(self._prediction_history.values())

    def calculate_confidence(self, model_output: object, model_type: str) -> float:
        """
        Calculate confidence score for a prediction

        Args:
            model_output: Output from the model
            model_type: Type of the model

        Returns:
            Confidence score between 0.0 and 1.0
        """
        # Apply confidence calculation algorithm based on model_type
        if model_type == "classification":
            # For classification, use probability of predicted class
            if isinstance(model_output, np.ndarray):
                confidence = float(np.max(model_output))
            else:
                confidence = 0.0
        elif model_type == "regression":
            # For regression, use prediction interval or error estimate
            confidence = 0.0  # Placeholder
        elif model_type == "anomaly":
            # For anomaly detection, use anomaly score
            confidence = 0.0  # Placeholder
        else:
            confidence = 0.0

        # Ensure final score is between 0.0 and 1.0
        return max(0.0, min(confidence, 1.0))

    def _update_prediction_history(self, prediction: ModelPrediction):
        """
        Update the prediction history with a new result

        Args:
            prediction: Prediction result to add to history
        """
        self._prediction_history[prediction.prediction_id] = prediction

    def _ensure_model_loaded(self, model_id: str, version_id: str) -> object:
        """
        Ensure a model is loaded for local prediction

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            Loaded model object
        """
        if (model_id, version_id) not in self._loaded_models:
            self.load_model(model_id, version_id)
        return self._loaded_models[(model_id, version_id)]


class ModelServingCache:
    """
    Cache for model serving to improve prediction performance
    """

    def __init__(self, max_cache_size: int = 1000, cache_ttl: int = 300):
        """
        Initialize the model serving cache

        Args:
            max_cache_size: Maximum number of entries in the cache
            cache_ttl: Time-to-live for cache entries in seconds
        """
        self._prediction_cache = {}
        self._max_cache_size = max_cache_size
        self._cache_ttl = cache_ttl

    def get(self, cache_key: str) -> typing.Optional[ModelPrediction]:
        """
        Get a cached prediction result

        Args:
            cache_key: Key for the cached prediction

        Returns:
            Cached prediction or None if not found
        """
        if cache_key in self._prediction_cache:
            cache_entry = self._prediction_cache[cache_key]
            if self._is_expired(cache_entry):
                del self._prediction_cache[cache_key]
                return None
            else:
                return cache_entry["prediction"]
        else:
            return None

    def put(self, cache_key: str, prediction: ModelPrediction):
        """
        Store a prediction result in cache

        Args:
            cache_key: Key for the prediction
            prediction: Prediction result to cache
        """
        self._prediction_cache[cache_key] = {"prediction": prediction, "timestamp": time()}
        # Evict oldest entries if cache exceeds max size
        if len(self._prediction_cache) > self._max_cache_size:
            # Sort entries by timestamp and remove the oldest
            sorted_cache = sorted(self._prediction_cache.items(), key=lambda item: item[1]["timestamp"])
            for i in range(len(self._prediction_cache) - self._max_cache_size):
                del self._prediction_cache[sorted_cache[i][0]]

    def invalidate(self, model_id: str, version_id: str) -> int:
        """
        Invalidate cache entries for a specific model

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            Number of invalidated entries
        """
        invalidated_count = 0
        for cache_key in list(self._prediction_cache.keys()):
            if cache_key.startswith(f"{model_id}-{version_id}"):
                del self._prediction_cache[cache_key]
                invalidated_count += 1
        return invalidated_count

    def clear(self):
        """
        Clear all entries from the cache
        """
        self._prediction_cache = {}

    def get_stats(self) -> dict:
        """
        Get cache statistics

        Returns:
            Cache statistics
        """
        cache_size = len(self._prediction_cache)
        hit_count = 0
        miss_count = 0
        total_age = 0

        for cache_key, cache_entry in self._prediction_cache.items():
            total_age += time() - cache_entry["timestamp"]

        average_age = total_age / cache_size if cache_size > 0 else 0

        return {
            "size": cache_size,
            "hit_rate": hit_count / (hit_count + miss_count) if (hit_count + miss_count) > 0 else 0,
            "miss_rate": miss_count / (hit_count + miss_count) if (hit_count + miss_count) > 0 else 0,
            "average_age": average_age
        }

    def _generate_cache_key(self, model_id: str, version_id: str, input_data: dict) -> str:
        """
        Generate a cache key from model and input information

        Args:
            model_id: ID of the model
            version_id: ID of the version
            input_data: Input data for prediction

        Returns:
            Cache key string
        """
        input_hash = hash(json.dumps(input_data, sort_keys=True))
        return f"{model_id}-{version_id}-{input_hash}"

    def _is_expired(self, cache_entry: dict) -> bool:
        """
        Check if a cached entry has expired

        Args:
            cache_entry: Cache entry dictionary

        Returns:
            True if entry has expired
        """
        return time() - cache_entry["timestamp"] > self._cache_ttl


def format_prediction_input(input_data: dict, model_type: str) -> dict:
    """
    Formats input data for model prediction based on model type

    Args:
        input_data: Input data dictionary
        model_type: Type of the model

    Returns:
        Formatted input data ready for prediction
    """
    # Validate input_data structure
    if not isinstance(input_data, dict):
        raise ValueError("Input data must be a dictionary")

    # Determine appropriate formatting based on model_type
    if model_type == "classification":
        # Apply preprocessing specific to classification models
        formatted_input = input_data
    elif model_type == "regression":
        # Apply preprocessing specific to regression models
        formatted_input = input_data
    elif model_type == "anomaly":
        # Apply preprocessing specific to anomaly detection models
        formatted_input = input_data
    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    # Convert to format expected by model (numpy arrays, tensors)
    # Example: formatted_input = np.array(list(formatted_input.values()))
    return formatted_input


def format_prediction_output(model_output: object, model_type: str) -> dict:
    """
    Formats model output into standardized prediction result

    Args:
        model_output: Raw output from the model
        model_type: Type of the model

    Returns:
        Standardized prediction result
    """
    # Process raw model output based on model_type
    if model_type == "classification":
        # Extract prediction values and probabilities
        if isinstance(model_output, np.ndarray):
            prediction = {"class": np.argmax(model_output), "probabilities": model_output.tolist()}
        else:
            prediction = {"class": "unknown", "probabilities": []}
    elif model_type == "regression":
        # Extract prediction values
        prediction = {"value": model_output}
    elif model_type == "anomaly":
        # Extract anomaly score
        prediction = {"score": model_output}
    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    # Add metadata about prediction
    # Example: prediction["units"] = "USD"
    return prediction


def serialize_prediction(prediction: ModelPrediction) -> str:
    """
    Serializes a prediction result to JSON format

    Args:
        prediction: ModelPrediction object

    Returns:
        JSON string representation of the prediction
    """
    return json.dumps(prediction.to_dict())


def deserialize_prediction(prediction_json: str) -> ModelPrediction:
    """
    Deserializes a prediction result from JSON format

    Args:
        prediction_json: JSON string representation of the prediction

    Returns:
        Deserialized ModelPrediction object
    """
    prediction_dict = json.loads(prediction_json)
    return ModelPrediction.from_dict(prediction_dict)