"""
Provides utility functions for making predictions with machine learning models in the self-healing data pipeline.
This module handles prediction preprocessing, batching, error handling, and result formatting for both local models and Vertex AI-hosted models.
"""

import typing
from typing import Any, Dict, List, Union, Tuple, Optional

import numpy as np  # version 1.24.x
import pandas as pd  # version 2.0.x
import tensorflow as tf  # version 2.12.x
from sklearn import preprocessing  # version 1.2.x
import joblib  # version 1.2.0

from utils.logging.logger import get_logger  # from src/backend/utils/logging/logger.py
from utils.config.config_loader import get_config  # from src/backend/utils/config/config_loader.py
from utils.ml import model_utils  # from src/backend/utils/ml/model_utils.py
from utils.ml import feature_utils  # from src/backend/utils/ml/feature_utils.py
from utils.ml import vertex_client  # from src/backend/utils/ml/vertex_client.py

# Initialize logger
logger = get_logger(__name__)

# Global constants
DEFAULT_BATCH_SIZE = 32
DEFAULT_PREDICTION_TIMEOUT = 30
SUPPORTED_MODEL_TYPES = ["tensorflow", "sklearn", "xgboost", "pytorch", "custom"]


class PredictionError(Exception):
    """Exception raised for errors during prediction operations."""

    def __init__(self, message: str, error_type: str, original_exception: Optional[Exception] = None):
        """Initialize the prediction error with details.

        Args:
            message: Error message describing the issue
            error_type: Type of error that occurred
            original_exception: The original exception if provided
        """
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.original_exception = original_exception
        logger.error(f"Prediction Error: {message} (Type: {error_type})", exc_info=original_exception)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary representation

        Returns:
            Dictionary with error details
        """
        error_dict = {
            "message": self.message,
            "error_type": self.error_type,
        }
        if self.original_exception:
            error_dict["original_exception"] = str(self.original_exception)
        return error_dict


class PredictionBatch:
    """Manages batched prediction operations for efficient processing."""

    def __init__(self, model: Any, model_type: str, batch_size: int = DEFAULT_BATCH_SIZE, prediction_options: Optional[Dict[str, Any]] = None):
        """Initialize the prediction batch processor

        Args:
            model: The loaded ML model
            model_type: Type of the model (tensorflow, sklearn, etc.)
            batch_size: Size of each batch for processing
            prediction_options: Options for prediction preprocessing and postprocessing
        """
        self._model = model
        if model_type not in SUPPORTED_MODEL_TYPES:
            raise ValueError(f"Unsupported model type: {model_type}")
        self._model_type = model_type
        self._batch_size = batch_size or DEFAULT_BATCH_SIZE
        self._prediction_options = prediction_options or {}
        self._results: List[Any] = []

    def process(self, data: Union[list, pd.DataFrame, np.ndarray]) -> List[Any]:
        """Process a dataset in batches

        Args:
            data: The dataset to process

        Returns:
            Prediction results for all batches
        """
        if not isinstance(data, (list, pd.DataFrame, np.ndarray)):
            raise ValueError("Input data must be a list, pandas DataFrame, or numpy array")

        # Clear any previous results
        self.clear_results()

        # Split data into batches
        if isinstance(data, pd.DataFrame):
            batches = [data[i:i + self._batch_size] for i in range(0, len(data), self._batch_size)]
        elif isinstance(data, np.ndarray):
            batches = [data[i:i + self._batch_size] for i in range(0, data.shape[0], self._batch_size)]
        else:
            batches = [data[i:i + self._batch_size] for i in range(0, len(data), self._batch_size)]

        # Process each batch
        for batch in batches:
            batch_results = self.process_batch(batch)
            self._results.extend(batch_results)

        return self._results

    def process_batch(self, batch: Union[list, pd.DataFrame, np.ndarray]) -> List[Any]:
        """Process a single batch of data

        Args:
            batch: A single batch of data

        Returns:
            Prediction results for the batch
        """
        # Preprocess batch data
        preprocessed_batch = preprocess_prediction_input(batch, self._model_type, self._prediction_options)

        # Apply model to preprocessed data
        if self._model_type == "tensorflow":
            model_output = self._model.predict(preprocessed_batch)
        else:
            model_output = self._model.predict(preprocessed_batch)

        # Postprocess model outputs
        batch_results = postprocess_prediction_output(model_output, self._model_type, self._prediction_options)
        return batch_results

    def get_results(self) -> List[Any]:
        """Get all processed results

        Returns:
            All prediction results
        """
        return self._results

    def clear_results(self) -> None:
        """Clear stored results"""
        self._results = []


class ModelPredictor:
    """High-level interface for making predictions with ML models"""

    def __init__(self, model: Union[str, Any], model_type: str, is_remote: bool, prediction_options: Optional[Dict[str, Any]] = None):
        """Initialize the model predictor

        Args:
            model: Path to the model or loaded model object
            model_type: Type of the model (tensorflow, sklearn, etc.)
            is_remote: Whether the model is hosted remotely (Vertex AI)
            prediction_options: Options for prediction preprocessing and postprocessing
        """
        self._model: Any = None
        self._model_type = model_type
        self._model_path: Optional[str] = None
        self._is_remote = is_remote
        self._endpoint_id: Optional[str] = None
        self._prediction_options = prediction_options or {}

        if isinstance(model, str):
            # Model is a path string
            self._model_path = model
            if not self._is_remote:
                # Load the model if it's a local model
                self._model = model_utils.load_model(model, model_type)
        else:
            # Model is a loaded model object
            self._model = model

        if self._is_remote:
            # Store endpoint_id if it's a remote model
            self._endpoint_id = self._model

        if self._model_type not in SUPPORTED_MODEL_TYPES:
            raise ValueError(f"Unsupported model type: {model_type}")

    def predict(self, input_data: Union[dict, list, pd.DataFrame, np.ndarray], prediction_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a prediction with the model

        Args:
            input_data: The input data for prediction
            prediction_options: Options for prediction preprocessing and postprocessing

        Returns:
            Prediction results
        """
        # Merge instance prediction_options with method options
        merged_options = self._prediction_options.copy()
        if prediction_options:
            merged_options.update(prediction_options)

        if self._is_remote:
            # Call predict_with_remote_model
            return predict_with_remote_model(self._endpoint_id, input_data, merged_options)
        else:
            # Call predict with local model
            return predict(self._model, input_data, self._model_type, merged_options)

    def batch_predict(self, batch_data: Union[list, pd.DataFrame, np.ndarray], batch_size: int = DEFAULT_BATCH_SIZE, prediction_options: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Make batch predictions with the model

        Args:
            batch_data: The batch data for prediction
            batch_size: Size of each batch for processing
            prediction_options: Options for prediction preprocessing and postprocessing

        Returns:
            Batch prediction results
        """
        # Merge instance prediction_options with method options
        merged_options = self._prediction_options.copy()
        if prediction_options:
            merged_options.update(prediction_options)

        # Create PredictionBatch instance
        batch_processor = PredictionBatch(self._model, self._model_type, batch_size, merged_options)

        # Process data with batch processor
        return batch_processor.process(batch_data)

    def reload_model(self) -> bool:
        """Reload the model (for local models)

        Returns:
            True if reload successful
        """
        if not self._is_remote and self._model_path:
            # Reload model from path
            self._model = model_utils.load_model(self._model_path, self._model_type)
            return True
        return False

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model

        Returns:
            Model information
        """
        model_info = {}
        if self._model_path:
            model_info = model_utils.get_model_info(self._model_path)
        model_info["model_type"] = self._model_type
        if self._is_remote:
            model_info["endpoint_id"] = self._endpoint_id
        return model_info


def predict(model: Any, input_data: Union[dict, list, pd.DataFrame, np.ndarray], model_type: str, prediction_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Makes a prediction using a loaded model with the provided input data"""
    if model is None:
        raise ValueError("Model cannot be None")

    if model_type not in SUPPORTED_MODEL_TYPES:
        raise ValueError(f"Unsupported model type: {model_type}")

    try:
        # Preprocess input data
        preprocessed_data = preprocess_prediction_input(input_data, model_type, prediction_options)

        # Apply model-specific prediction logic
        if model_type == "tensorflow":
            model_output = model.predict(preprocessed_data)
        else:
            model_output = model.predict(preprocessed_data)

        # Format prediction results
        prediction_results = postprocess_prediction_output(model_output, model_type, prediction_options)

        # Add metadata about prediction
        metadata = get_prediction_metadata(model, model_type, {"model_path": getattr(model, 'model_path', None)})
        prediction_results["metadata"] = metadata

        return prediction_results

    except Exception as e:
        # Handle prediction errors
        error_info = handle_prediction_errors(e, model_type, {"model_path": getattr(model, 'model_path', None)})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def batch_predict(model: Any, batch_data: Union[list, pd.DataFrame, np.ndarray], model_type: str, batch_size: int = DEFAULT_BATCH_SIZE, prediction_options: Optional[Dict[str, Any]] = None) -> List[Any]:
    """Performs batch prediction on a dataset using a loaded model"""
    if model is None:
        raise ValueError("Model cannot be None")

    if model_type not in SUPPORTED_MODEL_TYPES:
        raise ValueError(f"Unsupported model type: {model_type}")

    # Set batch_size to DEFAULT_BATCH_SIZE if not provided
    batch_size = batch_size or DEFAULT_BATCH_SIZE

    try:
        # Preprocess batch data
        preprocessed_data = preprocess_prediction_input(batch_data, model_type, prediction_options)

        # Split data into batches
        if isinstance(preprocessed_data, pd.DataFrame):
            batches = [preprocessed_data[i:i + batch_size] for i in range(0, len(preprocessed_data), batch_size)]
        elif isinstance(preprocessed_data, np.ndarray):
            batches = [preprocessed_data[i:i + batch_size] for i in range(0, preprocessed_data.shape[0], batch_size)]
        else:
            batches = [preprocessed_data[i:i + batch_size] for i in range(0, len(preprocessed_data), batch_size)]

        # Process each batch with the model
        results = []
        for batch in batches:
            if model_type == "tensorflow":
                model_output = model.predict(batch)
            else:
                model_output = model.predict(batch)
            results.extend(postprocess_prediction_output(model_output, model_type, prediction_options))

        return results

    except Exception as e:
        # Handle prediction errors
        error_info = handle_prediction_errors(e, model_type, {"model_path": getattr(model, 'model_path', None)})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def predict_with_local_model(model_path: str, input_data: Union[dict, list, pd.DataFrame, np.ndarray], model_type: str, prediction_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Makes a prediction using a locally loaded model"""
    try:
        # Load model from model_path
        model = model_utils.load_model(model_path, model_type)

        # Call predict function with loaded model
        prediction_results = predict(model, input_data, model_type, prediction_options)

        # Add model path to prediction metadata
        prediction_results["metadata"]["model_path"] = model_path

        return prediction_results

    except Exception as e:
        # Handle prediction errors
        error_info = handle_prediction_errors(e, model_type, {"model_path": model_path})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def predict_with_remote_model(endpoint_id: str, input_data: Union[dict, list, pd.DataFrame, np.ndarray], prediction_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Makes a prediction using a remote model (Vertex AI)"""
    try:
        # Format input data for Vertex AI
        vertex_request = vertex_client.format_vertex_request(input_data)

        # Call predict_with_vertex
        vertex_response = vertex_client.predict_with_vertex(endpoint_id, vertex_request)

        # Process and standardize response from Vertex AI
        prediction_results = postprocess_prediction_output(vertex_response, "vertex_ai", prediction_options)

        # Add remote model metadata to results
        metadata = get_prediction_metadata(None, "vertex_ai", {"endpoint_id": endpoint_id})
        prediction_results["metadata"] = metadata

        return prediction_results

    except Exception as e:
        # Handle prediction errors
        error_info = handle_prediction_errors(e, "vertex_ai", {"endpoint_id": endpoint_id})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def preprocess_prediction_input(input_data: Union[dict, list, pd.DataFrame, np.ndarray], model_type: str, preprocessing_options: Optional[Dict[str, Any]] = None) -> Union[dict, np.ndarray]:
    """Preprocesses input data for prediction based on model type"""
    try:
        # Convert input_data to appropriate format based on model_type
        if model_type == "tensorflow":
            if isinstance(input_data, dict):
                # TensorFlow Serving expects a dictionary with "instances" key
                input_data = {"instances": [input_data]}
            elif isinstance(input_data, (list, np.ndarray)):
                # Convert to numpy array if it's a list or numpy array
                input_data = np.array(input_data)
            elif isinstance(input_data, pd.DataFrame):
                # Convert DataFrame to numpy array
                input_data = input_data.to_numpy()
            else:
                raise ValueError("Unsupported input data type for TensorFlow model")
        else:
            # For other model types, ensure it's a numpy array
            if isinstance(input_data, dict):
                # Convert dictionary to numpy array
                input_data = np.array(list(input_data.values()))
            elif isinstance(input_data, list):
                # Convert list to numpy array
                input_data = np.array(input_data)
            elif isinstance(input_data, pd.DataFrame):
                # Convert DataFrame to numpy array
                input_data = input_data.to_numpy()
            elif not isinstance(input_data, np.ndarray):
                raise ValueError("Unsupported input data type for model")

        # Apply model-specific preprocessing steps
        if preprocessing_options and "feature_scaling" in preprocessing_options:
            # Apply feature scaling if required
            scaler_type = preprocessing_options["feature_scaling"]
            if scaler_type == "standard":
                scaler = preprocessing.StandardScaler()
                input_data = scaler.fit_transform(input_data)
            # Add other scaling methods as needed

        # Apply feature transformations specified in preprocessing_options
        if preprocessing_options and "feature_transformations" in preprocessing_options:
            # Apply feature transformations using feature_utils
            input_data = feature_utils.prepare_features_for_model(input_data, preprocessing_options["feature_transformations"])

        # Validate preprocessed data shape and format
        if isinstance(input_data, np.ndarray):
            logger.debug(f"Preprocessed input data shape: {input_data.shape}")

        return input_data

    except Exception as e:
        # Handle preprocessing errors
        error_info = handle_prediction_errors(e, model_type, {"preprocessing_options": preprocessing_options})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def postprocess_prediction_output(model_output: Any, model_type: str, postprocessing_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Postprocesses raw model output into standardized format"""
    try:
        # Convert model_output to appropriate format
        if model_type == "tensorflow":
            # Convert TensorFlow tensor to numpy array
            model_output = model_output.numpy()
        elif isinstance(model_output, list):
            # Convert list to numpy array
            model_output = np.array(model_output)

        # Apply model-specific postprocessing steps
        if postprocessing_options and "output_transformations" in postprocessing_options:
            # Apply output transformations as needed
            pass  # Implement transformations here

        # Format results according to postprocessing_options
        results = {"predictions": model_output.tolist()}

        # Add confidence scores if applicable
        if postprocessing_options and "confidence_calculation" in postprocessing_options:
            confidence_options = postprocessing_options["confidence_calculation"]
            confidence_scores = calculate_prediction_confidence(model_output, model_type, confidence_options)
            results["confidence_scores"] = confidence_scores

        # Structure output in standardized format
        return results

    except Exception as e:
        # Handle postprocessing errors
        error_info = handle_prediction_errors(e, model_type, {"postprocessing_options": postprocessing_options})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def calculate_prediction_confidence(model_output: Any, model_type: str, confidence_options: Optional[Dict[str, Any]] = None) -> Union[float, List[float]]:
    """Calculates confidence scores for prediction results"""
    try:
        # Extract probability values from model output if available
        if model_type == "tensorflow":
            # Assuming model output is a probability distribution
            probabilities = model_output
        else:
            # For other models, extract probabilities as needed
            probabilities = model_output

        # Apply model-specific confidence calculation logic
        if confidence_options and "calibration" in confidence_options:
            # Apply calibration if specified
            pass  # Implement calibration logic here

        # Normalize confidence scores to 0-1 range
        confidence_scores = np.array(probabilities)
        if np.any(confidence_scores < 0) or np.any(confidence_scores > 1):
            # Normalize to 0-1 range if needed
            min_val = np.min(confidence_scores)
            max_val = np.max(confidence_scores)
            confidence_scores = (confidence_scores - min_val) / (max_val - min_val)

        return confidence_scores.tolist()

    except Exception as e:
        # Handle confidence calculation errors
        error_info = handle_prediction_errors(e, model_type, {"confidence_options": confidence_options})
        raise PredictionError(error_info["message"], error_info["error_type"], e)


def validate_prediction_input(input_data: Union[dict, list, pd.DataFrame, np.ndarray], expected_schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validates that input data meets requirements for prediction"""
    errors = []

    # Check input_data is not None or empty
    if input_data is None:
        errors.append("Input data cannot be None")
        return False, errors

    # Validate input data type is appropriate
    if not isinstance(input_data, (dict, list, pd.DataFrame, np.ndarray)):
        errors.append("Input data must be a dict, list, pandas DataFrame, or numpy array")
        return False, errors

    # Check required fields/features are present
    if expected_schema and "required" in expected_schema:
        required_fields = expected_schema["required"]
        if isinstance(input_data, dict):
            for field in required_fields:
                if field not in input_data:
                    errors.append(f"Required field '{field}' is missing")
        elif isinstance(input_data, pd.DataFrame):
            for field in required_fields:
                if field not in input_data.columns:
                    errors.append(f"Required column '{field}' is missing")
        # Add checks for list and numpy array as needed

    # Validate data types of features
    if expected_schema and "properties" in expected_schema:
        properties = expected_schema["properties"]
        if isinstance(input_data, dict):
            for field, schema in properties.items():
                if field in input_data:
                    value = input_data[field]
                    expected_type = schema.get("type")
                    if expected_type == "number" and not isinstance(value, (int, float)):
                        errors.append(f"Field '{field}' must be a number")
                    elif expected_type == "string" and not isinstance(value, str):
                        errors.append(f"Field '{field}' must be a string")
        elif isinstance(input_data, pd.DataFrame):
            for field, schema in properties.items():
                if field in input_data.columns:
                    expected_type = schema.get("type")
                    if expected_type == "number" and not pd.api.types.is_numeric_dtype(input_data[field]):
                        errors.append(f"Column '{field}' must be numeric")
                    elif expected_type == "string" and not pd.api.types.is_string_dtype(input_data[field]):
                        errors.append(f"Column '{field}' must be a string")

    # Check value ranges for numerical features if specified
    if expected_schema and "properties" in expected_schema:
        properties = expected_schema["properties"]
        if isinstance(input_data, dict):
            for field, schema in properties.items():
                if field in input_data:
                    value = input_data[field]
                    if "minimum" in schema and value < schema["minimum"]:
                        errors.append(f"Field '{field}' must be greater than or equal to {schema['minimum']}")
                    if "maximum" in schema and value > schema["maximum"]:
                        errors.append(f"Field '{field}' must be less than or equal to {schema['maximum']}")
        elif isinstance(input_data, pd.DataFrame):
            for field, schema in properties.items():
                if field in input_data.columns:
                    if "minimum" in schema and (input_data[field] < schema["minimum"]).any():
                        errors.append(f"Column '{field}' must be greater than or equal to {schema['minimum']}")
                    if "maximum" in schema and (input_data[field] > schema["maximum"]).any():
                        errors.append(f"Column '{field}' must be less than or equal to {schema['maximum']}")

    # Return validation result and error list
    is_valid = len(errors) == 0
    return is_valid, errors


def get_prediction_metadata(model: Any, model_type: str, additional_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Generates metadata for a prediction operation"""
    metadata = {
        "timestamp": datetime.datetime.now().isoformat(),
        "model_type": model_type,
        "execution_environment": "local"  # Update if running in cloud
    }

    if model:
        # Extract model version information if available
        if hasattr(model, "version"):
            metadata["model_version"] = model.version
        elif hasattr(model, "__version__"):
            metadata["model_version"] = model.__version__

    # Merge with additional_metadata if provided
    if additional_metadata:
        metadata.update(additional_metadata)

    return metadata


def handle_prediction_errors(error: Exception, model_type: str, error_handling_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Handles errors that occur during prediction operations"""
    # Log the error with appropriate level
    logger.exception(f"Error during prediction with model type {model_type}: {str(error)}")

    # Categorize error type (input error, model error, etc.)
    error_type = "unknown_error"
    if isinstance(error, ValueError):
        error_type = "input_error"
    elif isinstance(error, tf.errors.InvalidArgumentError):
        error_type = "model_input_error"
    # Add more error types as needed

    # Generate error message with context
    error_message = f"Prediction failed due to {error_type}: {str(error)}"

    # Determine if error is recoverable
    is_recoverable = False
    if error_type in ["input_error", "model_input_error"]:
        is_recoverable = True  # Input errors might be recoverable with data correction

    # Suggest recovery actions based on error type
    recovery_actions = []
    if error_type == "input_error":
        recovery_actions.append("Check input data format and values")
    elif error_type == "model_input_error":
        recovery_actions.append("Verify input features match model requirements")
    # Add more recovery actions as needed

    # Return structured error information
    return {
        "message": error_message,
        "error_type": error_type,
        "is_recoverable": is_recoverable,
        "recovery_actions": recovery_actions
    }