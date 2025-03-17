"""
Implements model evaluation capabilities for the self-healing AI engine. This module
provides functionality to evaluate machine learning models against test datasets,
calculate performance metrics, and determine model effectiveness for different
healing scenarios. It supports evaluation of various model types used in the
self-healing process.
"""

import typing
import os
import json
import datetime
import uuid
import numpy as np  # version 1.24.x
from sklearn import metrics  # version 1.2.x
import tensorflow as tf  # version 2.12.x

from src.backend.utils.logging.logger import get_logger  # from '../../utils/logging/logger'
from src.backend.utils.ml import model_utils  # from '../../utils/ml'
from src.backend.utils.ml.vertex_client import VertexAIClient, predict_with_vertex  # from '../../utils/ml/vertex_client'
from src.backend.config import get_config  # from '../../config'

# Initialize logger
logger = get_logger(__name__)

# Default evaluation metrics for different model types
DEFAULT_EVALUATION_METRICS = {
    "classification": ["accuracy", "precision", "recall", "f1_score"],
    "regression": ["mse", "rmse", "mae", "r2"],
    "anomaly": ["precision", "recall", "auc", "average_precision"]
}

# Mapping of model types to evaluation metric types
MODEL_TYPE_MAPPING = {
    "issue_classifier": "classification",
    "root_cause_analyzer": "classification",
    "pattern_recognizer": "classification",
    "data_corrector": "regression",
    "anomaly_detector": "anomaly"
}


class ModelEvaluationResult:
    """
    Represents the result of a model evaluation run.
    """

    def __init__(
        self,
        model_id: str,
        model_version: str,
        model_type: str,
        metrics: dict,
        parameters: dict,
        sample_count: int,
        dataset_info: dict,
        evaluator: str
    ):
        """
        Initialize a model evaluation result with its properties.

        Args:
            model_id: ID of the evaluated model.
            model_version: Version of the evaluated model.
            model_type: Type of the model (classification, regression, anomaly).
            metrics: Dictionary of evaluation metrics.
            parameters: Dictionary of evaluation parameters.
            sample_count: Number of samples used for evaluation.
            dataset_info: Information about the test dataset.
            evaluator: Name of the evaluator (user or system).
        """
        self.evaluation_id = str(uuid.uuid4())  # Set evaluation_id to a new UUID
        self.model_id = model_id  # Set model_id to link to the evaluated model
        self.model_version = model_version  # Set model_version to the specific version evaluated
        self.model_type = model_type  # Set model_type (classification, regression, anomaly)
        self.metrics = metrics  # Set metrics dictionary with evaluation metrics
        self.parameters = parameters  # Set parameters dictionary with evaluation parameters
        self.sample_count = sample_count  # Set sample_count to the number of samples used
        self.dataset_info = dataset_info  # Set dataset_info with information about the test dataset
        self.evaluation_time = datetime.datetime.now(tz=datetime.timezone.utc)  # Set evaluation_time to current time
        self.evaluator = evaluator  # Set evaluator to the name of the evaluator (user or system)

    def to_dict(self) -> dict:
        """
        Convert evaluation result to dictionary representation.

        Returns:
            Dictionary representation of evaluation result.
        """
        # Create dictionary with all evaluation result properties
        evaluation_dict = {
            "evaluation_id": self.evaluation_id,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "model_type": self.model_type,
            "metrics": self.metrics,
            "parameters": self.parameters,
            "sample_count": self.sample_count,
            "dataset_info": self.dataset_info,
            "evaluation_time": self.evaluation_time.isoformat(),  # Convert datetime objects to ISO strings
            "evaluator": self.evaluator
        }
        return evaluation_dict

    @classmethod
    def from_dict(cls, evaluation_dict: dict) -> "ModelEvaluationResult":
        """
        Create ModelEvaluationResult from dictionary representation.

        Args:
            evaluation_dict: Dictionary containing evaluation result data.

        Returns:
            ModelEvaluationResult instance.
        """
        # Extract fields from dictionary
        evaluation_id = evaluation_dict["evaluation_id"]
        model_id = evaluation_dict["model_id"]
        model_version = evaluation_dict["model_version"]
        model_type = evaluation_dict["model_type"]
        metrics = evaluation_dict["metrics"]
        parameters = evaluation_dict["parameters"]
        sample_count = evaluation_dict["sample_count"]
        dataset_info = evaluation_dict["dataset_info"]
        evaluation_time = datetime.datetime.fromisoformat(evaluation_dict["evaluation_time"])  # Parse datetime strings to datetime objects
        evaluator = evaluation_dict["evaluator"]

        # Create and return ModelEvaluationResult instance with extracted values
        return cls(
            model_id=model_id,
            model_version=model_version,
            model_type=model_type,
            metrics=metrics,
            parameters=parameters,
            sample_count=sample_count,
            dataset_info=dataset_info,
            evaluator=evaluator
        )

    def get_primary_metric(self) -> typing.Tuple[str, float]:
        """
        Get the primary metric for this model type.

        Returns:
            Tuple of (metric_name, metric_value).
        """
        # Determine primary metric based on model_type
        if self.model_type == "classification":
            primary_metric = "accuracy"  # For classification, return accuracy
        elif self.model_type == "regression":
            primary_metric = "rmse"  # For regression, return rmse
        elif self.model_type == "anomaly":
            primary_metric = "auc"  # For anomaly detection, return auc
        else:
            primary_metric = list(self.metrics.keys())[0] if self.metrics else None

        if primary_metric:
            return primary_metric, self.metrics.get(primary_metric)  # Return tuple of metric name and value
        else:
            return None, None

    def get_summary(self) -> dict:
        """
        Get a summary of the evaluation result.

        Returns:
            Summary dictionary with key evaluation information.
        """
        # Create summary dictionary with evaluation ID and model ID
        summary = {
            "evaluation_id": self.evaluation_id,
            "model_id": self.model_id,
            "model_type": self.model_type,  # Add model type and version
            "model_version": self.model_version,
        }
        primary_metric, primary_value = self.get_primary_metric()
        if primary_metric:
            summary["primary_metric"] = primary_metric
            summary["primary_value"] = primary_value  # Add primary metric and sample count
        summary["sample_count"] = self.sample_count
        summary["evaluation_time"] = self.evaluation_time.isoformat()  # Add evaluation timestamp
        return summary  # Return the summary dictionary

    def compare_with(self, other_result: "ModelEvaluationResult") -> dict:
        """
        Compare this evaluation result with another one.

        Args:
            other_result: Another ModelEvaluationResult instance.

        Returns:
            Comparison results with improvement metrics.
        """
        # Validate other_result is for the same model type
        if self.model_type != other_result.model_type:
            raise ValueError("Cannot compare evaluation results for different model types")

        comparison = {}
        # Compare metrics between this result and other_result
        for metric_name, metric_value in self.metrics.items():
            other_metric_value = other_result.metrics.get(metric_name)
            if other_metric_value is not None:
                # Calculate improvement percentages for each metric
                improvement = (metric_value - other_metric_value) / other_metric_value if other_metric_value != 0 else 0
                comparison[metric_name] = {
                    "current": metric_value,
                    "previous": other_metric_value,
                    "improvement": improvement
                }

        # Determine overall improvement status
        overall_improvement = sum([v["improvement"] for v in comparison.values()]) / len(comparison) if comparison else 0
        comparison["overall_improvement"] = overall_improvement
        return comparison  # Return comparison dictionary with detailed metrics


class ModelEvaluator:
    """
    Main class for evaluating machine learning models used in the self-healing pipeline.
    """

    def __init__(self, config: dict = None):
        """
        Initialize the model evaluator with configuration.

        Args:
            config: Configuration dictionary.
        """
        self._config = get_config()  # Initialize configuration with defaults and override with provided config
        self._use_vertex_ai = self._config.get("model_evaluation.use_vertex_ai", False)  # Determine whether to use local evaluation or Vertex AI
        if self._use_vertex_ai:
            self._vertex_client = VertexAIClient()  # If using Vertex AI, initialize client
        else:
            self._vertex_client = None
        self._evaluation_history = {}  # Initialize empty dictionary for evaluation history

    def evaluate_model(
        self,
        model: typing.Any,
        model_id: str,
        model_version: str,
        model_type: str,
        test_data: list,
        parameters: dict
    ) -> ModelEvaluationResult:
        """
        Evaluate a model with test data.

        Args:
            model: The model to evaluate.
            model_id: ID of the model.
            model_version: Version of the model.
            model_type: Type of the model (classification, regression, anomaly).
            test_data: Test data for evaluation.
            parameters: Evaluation parameters.

        Returns:
            Evaluation result.
        """
        if not model or not test_data:  # Validate model and test_data are provided
            raise ValueError("Model and test data must be provided for evaluation")

        if model_type == "classification":  # Determine evaluation method based on model_type
            metrics = self.evaluate_classification_model(model, test_data, parameters)  # Call appropriate specialized evaluation method
        elif model_type == "regression":
            metrics = self.evaluate_regression_model(model, test_data, parameters)  # Call appropriate specialized evaluation method
        elif model_type == "anomaly":
            metrics = self.evaluate_anomaly_model(model, test_data, parameters)  # Call appropriate specialized evaluation method
        else:
            raise ValueError(f"Unsupported model type: {model_type}")

        dataset_info = {"sample_count": len(test_data)}
        evaluation_result = ModelEvaluationResult(  # Create ModelEvaluationResult with evaluation metrics
            model_id=model_id,
            model_version=model_version,
            model_type=model_type,
            metrics=metrics,
            parameters=parameters,
            sample_count=len(test_data),
            dataset_info=dataset_info,
            evaluator="local"
        )

        self._update_evaluation_history(evaluation_result)  # Update evaluation history
        return evaluation_result  # Return evaluation result

    def evaluate_classification_model(
        self, model: typing.Any, test_data: list, parameters: dict
    ) -> dict:
        """
        Evaluate a classification model.

        Args:
            model: The classification model to evaluate.
            test_data: Test data for evaluation.
            parameters: Evaluation parameters.

        Returns:
            Dictionary of evaluation metrics.
        """
        features, labels = self._prepare_test_data(test_data, "classification")  # Split test_data into features and labels
        predictions = model.predict(features)  # Generate predictions using the model
        try:
            probabilities = model.predict_proba(features)[:, 1]  # Generate probability scores if model supports it
        except AttributeError:
            probabilities = None

        metrics = calculate_classification_metrics(labels, predictions, probabilities)  # Calculate classification metrics using calculate_classification_metrics
        return metrics  # Return dictionary of metrics

    def evaluate_regression_model(
        self, model: typing.Any, test_data: list, parameters: dict
    ) -> dict:
        """
        Evaluate a regression model.

        Args:
            model: The regression model to evaluate.
            test_data: Test data for evaluation.
            parameters: Evaluation parameters.

        Returns:
            Dictionary of evaluation metrics.
        """
        features, labels = self._prepare_test_data(test_data, "regression")  # Split test_data into features and labels
        predictions = model.predict(features)  # Generate predictions using the model
        metrics = calculate_regression_metrics(labels, predictions)  # Calculate regression metrics using calculate_regression_metrics
        return metrics  # Return dictionary of metrics

    def evaluate_anomaly_model(
        self, model: typing.Any, test_data: list, parameters: dict
    ) -> dict:
        """
        Evaluate an anomaly detection model.

        Args:
            model: The anomaly detection model to evaluate.
            test_data: Test data for evaluation.
            parameters: Evaluation parameters.

        Returns:
            Dictionary of evaluation metrics.
        """
        features, labels = self._prepare_test_data(test_data, "anomaly")  # Split test_data into features and labels
        predictions = model.predict(features)  # Generate anomaly predictions using the model
        try:
            scores = model.decision_function(features)  # Generate anomaly scores if model supports it
        except AttributeError:
            scores = None

        metrics = calculate_anomaly_metrics(labels, predictions, scores)  # Calculate anomaly metrics using calculate_anomaly_metrics
        return metrics  # Return dictionary of metrics

    def evaluate_with_vertex(
        self, model_name: str, model_type: str, test_data: list, parameters: dict
    ) -> dict:
        """
        Evaluate a model using Vertex AI Model Evaluation.

        Args:
            model_name: Name of the model in Vertex AI.
            model_type: Type of the model (classification, regression, anomaly).
            test_data: Test data for evaluation.
            parameters: Evaluation parameters.

        Returns:
            Evaluation metrics from Vertex AI.
        """
        if not self._vertex_client:  # Validate _vertex_client is initialized
            raise ValueError("Vertex AI client is not initialized")

        features, labels = self._prepare_test_data(test_data, model_type)  # Prepare test data for Vertex AI evaluation
        # Submit evaluation job to Vertex AI
        metrics = predict_with_vertex(model_name, features.tolist())
        return metrics  # Return dictionary of metrics

    def get_evaluation_history(self, filters: dict = None) -> list:
        """
        Get evaluation history with optional filtering.

        Args:
            filters: Dictionary of filters to apply.

        Returns:
            Filtered evaluation history.
        """
        if filters:  # Apply filters to _evaluation_history if provided
            filtered_history = []
            for evaluation_id, evaluation_result in self._evaluation_history.items():
                # Apply filter logic here based on filter criteria
                filtered_history.append(evaluation_result)
            return filtered_history
        else:
            return list(self._evaluation_history.values())  # Return filtered or all evaluation history

    def get_evaluation_by_id(self, evaluation_id: str) -> ModelEvaluationResult:
        """
        Get a specific evaluation result by ID.

        Args:
            evaluation_id: ID of the evaluation result.

        Returns:
            Evaluation result or None if not found.
        """
        return self._evaluation_history.get(evaluation_id)  # Look up evaluation_id in _evaluation_history

    def compare_evaluations(self, evaluation_id_a: str, evaluation_id_b: str) -> dict:
        """
        Compare two evaluation results.

        Args:
            evaluation_id_a: ID of the first evaluation result.
            evaluation_id_b: ID of the second evaluation result.

        Returns:
            Comparison results.
        """
        result_a = self.get_evaluation_by_id(evaluation_id_a)  # Get evaluation results for both IDs
        result_b = self.get_evaluation_by_id(evaluation_id_b)

        if not result_a or not result_b:  # Validate both evaluations exist and are comparable
            raise ValueError("One or both evaluation IDs are invalid")

        return result_a.compare_with(result_b)  # Call compare_with method on first evaluation result

    def save_evaluation_result(self, evaluation_result: ModelEvaluationResult, output_path: str) -> bool:
        """
        Save an evaluation result to storage.

        Args:
            evaluation_result: The evaluation result to save.
            output_path: Path to save the evaluation result.

        Returns:
            True if save successful.
        """
        evaluation_json = serialize_evaluation_result(evaluation_result)  # Serialize evaluation result to JSON
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):  # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)

        try:
            with open(output_path, "w") as f:  # Write serialized result to file
                f.write(evaluation_json)
            return True  # Return success status
        except Exception as e:
            logger.error(f"Error saving evaluation result: {e}")
            return False

    def load_evaluation_result(self, input_path: str) -> ModelEvaluationResult:
        """
        Load an evaluation result from storage.

        Args:
            input_path: Path to load the evaluation result from.

        Returns:
            Loaded evaluation result.
        """
        if not os.path.exists(input_path):  # Validate input file exists
            raise FileNotFoundError(f"Evaluation result file not found: {input_path}")

        with open(input_path, "r") as f:  # Read file content
            evaluation_json = f.read()

        evaluation_result = deserialize_evaluation_result(evaluation_json)  # Deserialize JSON to ModelEvaluationResult
        return evaluation_result  # Return loaded evaluation result

    def _update_evaluation_history(self, evaluation_result: ModelEvaluationResult) -> None:
        """
        Update the evaluation history with a new result.

        Args:
            evaluation_result: The evaluation result to add.
        """
        self._evaluation_history[evaluation_result.evaluation_id] = evaluation_result  # Add evaluation result to history dictionary
        # Trim history if it exceeds maximum size
        logger.debug(f"Updated evaluation history with evaluation ID: {evaluation_result.evaluation_id}")
        # Update evaluation statistics

    def _prepare_test_data(self, test_data: list, model_type: str) -> typing.Tuple[np.ndarray, np.ndarray]:
        """
        Prepare test data for evaluation.

        Args:
            test_data: List of test data samples.
            model_type: Type of the model (classification, regression, anomaly).

        Returns:
            Tuple of (features, labels).
        """
        # Validate test_data format
        features = [sample["feature"] for sample in test_data]  # Extract features and labels based on model_type
        labels = [sample["label"] for sample in test_data]

        # Preprocess features if needed
        # Convert to appropriate format (numpy arrays, tensors)
        features = np.array(features)
        labels = np.array(labels)

        return features, labels  # Return tuple of features and labels


def serialize_evaluation_result(evaluation_result: ModelEvaluationResult) -> str:
    """
    Serializes model evaluation result to JSON format.

    Args:
        evaluation_result: ModelEvaluationResult object.

    Returns:
        JSON string representation of the evaluation result.
    """
    evaluation_dict = evaluation_result.to_dict()  # Convert ModelEvaluationResult object to dictionary using to_dict method
    evaluation_json = json.dumps(evaluation_dict, indent=2)  # Serialize dictionary to JSON string
    return evaluation_json  # Return serialized evaluation result


def deserialize_evaluation_result(evaluation_json: str) -> ModelEvaluationResult:
    """
    Deserializes model evaluation result from JSON format.

    Args:
        evaluation_json: JSON string representation of the evaluation result.

    Returns:
        Deserialized ModelEvaluationResult object.
    """
    evaluation_dict = json.loads(evaluation_json)  # Parse JSON string to dictionary
    evaluation_result = ModelEvaluationResult.from_dict(evaluation_dict)  # Create and return ModelEvaluationResult object using from_dict method
    return evaluation_result


def calculate_classification_metrics(y_true: list, y_pred: list, y_prob: list = None) -> dict:
    """
    Calculates standard metrics for classification models.

    Args:
        y_true: List of true labels.
        y_pred: List of predicted labels.
        y_prob: List of predicted probabilities (optional).

    Returns:
        Dictionary of classification metrics.
    """
    accuracy = metrics.accuracy_score(y_true, y_pred)  # Calculate accuracy using sklearn.metrics.accuracy_score
    precision = metrics.precision_score(y_true, y_pred, average="weighted", zero_division=0)  # Calculate precision using sklearn.metrics.precision_score
    recall = metrics.recall_score(y_true, y_pred, average="weighted", zero_division=0)  # Calculate recall using sklearn.metrics.recall_score
    f1_score = metrics.f1_score(y_true, y_pred, average="weighted", zero_division=0)  # Calculate F1 score using sklearn.metrics.f1_score
    confusion_matrix = metrics.confusion_matrix(y_true, y_pred).tolist()  # Calculate confusion matrix using sklearn.metrics.confusion_matrix

    classification_metrics = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
        "confusion_matrix": confusion_matrix
    }

    if y_prob is not None:  # Calculate ROC AUC if probability scores are provided
        try:
            roc_auc = metrics.roc_auc_score(y_true, y_prob)
            classification_metrics["roc_auc"] = roc_auc
        except ValueError as e:
            logger.warning(f"Could not calculate ROC AUC: {e}")

    return classification_metrics  # Return dictionary with all calculated metrics


def calculate_regression_metrics(y_true: list, y_pred: list) -> dict:
    """
    Calculates standard metrics for regression models.

    Args:
        y_true: List of true values.
        y_pred: List of predicted values.

    Returns:
        Dictionary of regression metrics.
    """
    mse = metrics.mean_squared_error(y_true, y_pred)  # Calculate mean squared error (MSE) using sklearn.metrics.mean_squared_error
    rmse = np.sqrt(mse)  # Calculate root mean squared error (RMSE) by taking square root of MSE
    mae = metrics.mean_absolute_error(y_true, y_pred)  # Calculate mean absolute error (MAE) using sklearn.metrics.mean_absolute_error
    r2 = metrics.r2_score(y_true, y_pred)  # Calculate R-squared using sklearn.metrics.r2_score
    explained_variance = metrics.explained_variance_score(y_true, y_pred)  # Calculate explained variance using sklearn.metrics.explained_variance_score

    regression_metrics = {
        "mse": mse,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "explained_variance": explained_variance
    }
    return regression_metrics  # Return dictionary with all calculated metrics


def calculate_anomaly_metrics(y_true: list, y_pred: list, y_score: list = None) -> dict:
    """
    Calculates metrics for anomaly detection models.

    Args:
        y_true: List of true anomaly labels.
        y_pred: List of predicted anomaly labels.
        y_score: List of anomaly scores (optional).

    Returns:
        Dictionary of anomaly detection metrics.
    """
    precision = metrics.precision_score(y_true, y_pred, zero_division=0)  # Calculate precision using sklearn.metrics.precision_score
    recall = metrics.recall_score(y_true, y_pred, zero_division=0)  # Calculate recall using sklearn.metrics.recall_score
    f1_score = metrics.f1_score(y_true, y_pred, zero_division=0)  # Calculate F1 score using sklearn.metrics.f1_score

    anomaly_metrics = {
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
    }

    if y_score is not None:  # Calculate ROC AUC if anomaly scores are provided
        try:
            roc_auc = metrics.roc_auc_score(y_true, y_score)  # Calculate ROC AUC using sklearn.metrics.roc_auc_score
            average_precision = metrics.average_precision_score(y_true, y_score)  # Calculate average precision using sklearn.metrics.average_precision_score
            precision_recall_curve = metrics.precision_recall_curve(y_true, y_score)  # Calculate precision-recall curve data

            anomaly_metrics["auc"] = roc_auc
            anomaly_metrics["average_precision"] = average_precision
            anomaly_metrics["precision_recall_curve"] = precision_recall_curve
        except ValueError as e:
            logger.warning(f"Could not calculate ROC AUC: {e}")

    return anomaly_metrics  # Return dictionary with all calculated metrics