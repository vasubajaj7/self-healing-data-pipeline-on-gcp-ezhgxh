"""
Implements a comprehensive model management system for the self-healing AI engine. This module provides centralized functionality for registering, versioning, loading, evaluating, and deploying machine learning models used in the self-healing pipeline. It serves as the main interface between the AI models and the rest of the self-healing system.
"""

import typing  # standard library # IE2: typing library for type annotations
import os  # standard library # IE2: os library for file and path operations
import json  # standard library # IE2: json library for serializing and deserializing model metadata
from datetime import datetime  # standard library # IE2: datetime library for timestamping model operations
import uuid  # standard library # IE2: uuid library for generating unique identifiers for models and versions

from google.cloud import aiplatform  # version: 1.25.0 # IE2: google-cloud-aiplatform library for Vertex AI integration

from src.backend.utils.logging.logger import get_logger  # Internal import # IE1: get_logger function from the utils.logging.logger module for configuring logging
from src.backend.utils.ml import model_utils  # Internal import # IE1: model_utils module for utility functions for model operations
from src.backend.utils.ml.vertex_client import VertexAIClient, upload_model_to_vertex, deploy_model_to_endpoint  # Internal import # IE1: VertexAIClient class and upload_model_to_vertex, deploy_model_to_endpoint functions from the utils.ml.vertex_client module for Vertex AI integration
from src.backend.self_healing.models import model_registry  # Internal import # IE1: model_registry module for model registry functionality
from src.backend.self_healing.models.model_registry import ModelRegistry, serialize_model_metadata, deserialize_model_metadata, get_model_path  # Internal import # IE1: ModelRegistry class and serialize_model_metadata, deserialize_model_metadata, get_model_path functions from the self_healing.models.model_registry module for model registry functionality
from src.backend.self_healing.models.model_evaluation import ModelEvaluator, ModelEvaluationResult  # Internal import # IE1: ModelEvaluator class and ModelEvaluationResult class from the self_healing.models.model_evaluation module for model evaluation capabilities
from src.backend.self_healing.models.model_serving import ModelServingFactory, ModelServer  # Internal import # IE1: ModelServingFactory class and ModelServer class from the self_healing.models.model_serving module for model serving capabilities
from src.backend import config  # Internal import # IE1: config module for accessing application configuration settings

# Initialize logger
logger = get_logger(__name__)

# Define default model base path
DEFAULT_MODEL_BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "models")

# Define supported model types
MODEL_TYPES = {"classification": "Issue Classification", "root_cause": "Root Cause Analysis", "pattern": "Pattern Recognition", "correction": "Data Correction"}


def serialize_model_metadata(dict: typing.Dict) -> str:
    """Serializes model metadata to JSON format

    Args:
        dict: metadata

    Returns:
        str: JSON string representation of the metadata
    """
    def json_converter(o):
        if isinstance(o, datetime):
            return o.__str__()
        if isinstance(o, enum.Enum):
            return o.value
    return json.dumps(dict, default=json_converter)


def deserialize_model_metadata(metadata_json: str) -> typing.Dict:
    """Deserializes model metadata from JSON format

    Args:
        str: metadata_json

    Returns:
        dict: Deserialized metadata dictionary
    """
    def object_hook(d):
        for k, v in d.items():
            if isinstance(v, str):
                try:
                    d[k] = datetime.fromisoformat(v)
                except:
                    pass
        return d
    return json.loads(metadata_json, object_hook=object_hook)


def get_model_path(model_id: str, version_id: str = None, base_path: str = None) -> str:
    """Constructs the path to a model artifact

    Args:
        model_id: Unique identifier for the model
        version_id: Version identifier
        base_path: Base path for models

    Returns:
        Full path to the model artifact
    """
    if not model_id:
        raise ValueError("Model ID cannot be empty")

    version_id = version_id or "latest"
    base_path = base_path or DEFAULT_MODEL_BASE_PATH

    model_path = os.path.join(base_path, model_id, version_id)
    return model_path


class ModelMetadata:
    """Represents metadata for a model version"""
    def __init__(self, model_id: str, version_id: str, name: str, description: str, model_type: str, framework: str, parameters: typing.Dict, metrics: typing.Dict, artifact_path: str):
        """Initialize model metadata with its properties
        Args:
            model_id: ID of the model
            version_id: ID of the version
            name: Name of the model
            description: Description of the model
            model_type: Type of the model
            framework: Framework used to train the model
            parameters: Training parameters
            metrics: Performance metrics
            artifact_path: Path to the model artifact
        """
        self.model_id = model_id
        self.version_id = version_id
        self.name = name
        self.description = description
        self.model_type = model_type
        self.framework = framework
        self.parameters = parameters or {}
        self.metrics = metrics or {}
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.status = "DRAFT"
        self.artifact_path = artifact_path

    def to_dict(self) -> dict:
        """Convert metadata to dictionary representation
        Returns:
            Dictionary representation of metadata
        """
        metadata_dict = {
            "model_id": self.model_id,
            "version_id": self.version_id,
            "name": self.name,
            "description": self.description,
            "model_type": self.model_type,
            "framework": self.framework,
            "parameters": self.parameters,
            "metrics": self.metrics,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "status": self.status,
            "artifact_path": self.artifact_path
        }
        return metadata_dict

    @classmethod
    def from_dict(cls, metadata_dict: dict) -> 'ModelMetadata':
        """Create ModelMetadata from dictionary representation
        Args:
            metadata_dict: Dictionary containing metadata
        Returns:
            ModelMetadata instance
        """
        model_id = metadata_dict["model_id"]
        version_id = metadata_dict["version_id"]
        name = metadata_dict["name"]
        description = metadata_dict["description"]
        model_type = metadata_dict["model_type"]
        framework = metadata_dict["framework"]
        parameters = metadata_dict["parameters"]
        metrics = metadata_dict["metrics"]
        created_at = datetime.fromisoformat(metadata_dict["created_at"])
        updated_at = datetime.fromisoformat(metadata_dict["updated_at"])
        status = metadata_dict["status"]
        artifact_path = metadata_dict["artifact_path"]

        model_metadata = cls(model_id, version_id, name, description, model_type, framework, parameters, metrics, artifact_path)
        model_metadata.created_at = created_at
        model_metadata.updated_at = updated_at
        model_metadata.status = status
        return model_metadata

    def update_metrics(self, new_metrics: dict) -> None:
        """Update metrics with evaluation results
        Args:
            new_metrics: Dictionary of new metrics
        """
        self.metrics.update(new_metrics)
        self.updated_at = datetime.now()


class ModelVersion:
    """Represents a specific version of a model with its artifacts and metadata"""
    def __init__(self, version_id: str, model_id: str, metadata: ModelMetadata):
        """Initialize a model version with its metadata
        Args:
            version_id: ID of the version
            model_id: ID of the model
            metadata: Model metadata
        """
        self.version_id = version_id
        self.model_id = model_id
        self.metadata = metadata
        self.model_instance = None
        self.is_active = False
        self.is_loaded = False

    def load(self) -> object:
        """Load the model from its artifact path
        Returns:
            Loaded model instance
        """
        if self.is_loaded:
            logger.info(f"Model {self.model_id} version {self.version_id} already loaded")
            return self.model_instance

        artifact_path = self.metadata.artifact_path
        model_format = self.metadata.framework

        self.model_instance = model_utils.load_model(artifact_path, model_format)
        self.is_loaded = True
        logger.info(f"Model {self.model_id} version {self.version_id} loaded successfully")
        return self.model_instance

    def unload(self) -> bool:
        """Unload the model from memory
        Returns:
            True if unloaded successfully
        """
        if not self.is_loaded:
            logger.info(f"Model {self.model_id} version {self.version_id} not loaded, cannot unload")
            return True

        self.model_instance = None
        self.is_loaded = False
        logger.info(f"Model {self.model_id} version {self.version_id} unloaded successfully")
        return True

    def activate(self) -> bool:
        """Mark this version as active
        Returns:
            True if activation successful
        """
        self.is_active = True
        self.metadata.status = "ACTIVE"
        logger.info(f"Model {self.model_id} version {self.version_id} activated")
        return True

    def deactivate(self) -> bool:
        """Mark this version as inactive
        Returns:
            True if deactivation successful
        """
        self.is_active = False
        self.metadata.status = "INACTIVE"
        logger.info(f"Model {self.model_id} version {self.version_id} deactivated")
        return True

    def get_model_instance(self) -> object:
        """Get the loaded model instance
        Returns:
            Model instance or None if not loaded
        """
        if self.is_loaded:
            return self.model_instance
        else:
            logger.warning(f"Model {self.model_id} version {self.version_id} not loaded")
            return None


class Model:
    """Represents a model with multiple versions"""
    def __init__(self, model_id: str, name: str, description: str, model_type: str):
        """Initialize a model with its properties
        Args:
            model_id: ID of the model
            name: Name of the model
            description: Description of the model
            model_type: Type of the model
        """
        self.model_id = model_id
        self.name = name
        self.description = description
        self.model_type = model_type
        self.versions = {}
        self.active_version_id = None
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    def add_version(self, version: ModelVersion) -> ModelVersion:
        """Add a new version to the model
        Args:
            version: ModelVersion instance
        Returns:
            Added version
        """
        if not isinstance(version, ModelVersion):
            raise ValueError("version must be a ModelVersion instance")

        self.versions[version.version_id] = version
        self.updated_at = datetime.now()
        logger.info(f"Added version {version.version_id} to model {self.model_id}")
        return version

    def get_version(self, version_id: str) -> ModelVersion:
        """Get a specific version of the model
        Args:
            version_id: ID of the version
        Returns:
            Requested version or None if not found
        """
        return self.versions.get(version_id)

    def get_latest_version(self) -> ModelVersion:
        """Get the latest version of the model
        Returns:
            Latest version or None if no versions
        """
        if not self.versions:
            logger.warning(f"No versions found for model {self.model_id}")
            return None

        sorted_versions = sorted(self.versions.values(), key=lambda v: v.metadata.created_at, reverse=True)
        return sorted_versions[0]

    def get_active_version(self) -> ModelVersion:
        """Get the currently active version of the model
        Returns:
            Active version or None if no active version
        """
        if self.active_version_id:
            return self.versions.get(self.active_version_id)
        else:
            logger.warning(f"No active version set for model {self.model_id}")
            return None

    def set_active_version(self, version_id: str) -> bool:
        """Set the active version of the model
        Args:
            version_id: ID of the version
        Returns:
            True if activation successful
        """
        if version_id not in self.versions:
            raise ValueError(f"Version with ID {version_id} not found in model {self.model_id}")

        # Deactivate current active version if exists
        if self.active_version_id:
            active_version = self.get_version(self.active_version_id)
            active_version.deactivate()

        # Activate new version
        new_active_version = self.get_version(version_id)
        new_active_version.activate()

        self.active_version_id = version_id
        self.updated_at = datetime.now()
        logger.info(f"Set version {version_id} as active for model {self.model_id}")
        return True

    def get_all_versions(self) -> list:
        """Get all versions of the model
        Returns:
            List of all model versions
        """
        return list(self.versions.values())


class ModelManager:
    """Main class for managing ML models used in the self-healing pipeline"""
    def __init__(self, config: dict):
        """Initialize the model manager with configuration
        Args:
            config: Configuration dictionary
        """
        self._config = config
        self._registry = ModelRegistry(config)
        self._evaluator = ModelEvaluator(config)
        self._models = {}
        self._model_servers = {}
        self._vertex_client = VertexAIClient(config)
        self._use_vertex_ai = config.get("model_manager.use_vertex_ai", False)

        self._load_models_from_registry()

    def register_model(self, name: str, description: str, model_type: str, metadata: typing.Dict) -> str:
        """Register a new model in the system
        Args:
            name: Name of the model
            description: Description of the model
            model_type: Type of the model
            metadata: Additional metadata for the model
        Returns:
            ID of the registered model
        """
        if model_type not in MODEL_TYPES:
            raise ValueError(f"Invalid model type: {model_type}. Supported types are: {', '.join(MODEL_TYPES.keys())}")

        model_id = str(uuid.uuid4())
        model = Model(model_id, name, description, model_type)
        self._models[model_id] = model
        logger.info(f"Registered new model: {model_id} with type {model_type}")
        return model_id

    def register_model_version(self, model_id: str, artifact_path: str, framework: str, parameters: typing.Dict, metrics: typing.Dict, description: str) -> str:
        """Register a new version of an existing model
        Args:
            model_id: ID of the model
            artifact_path: Path to the model artifact
            framework: Framework used to train the model
            parameters: Training parameters
            metrics: Performance metrics
            description: Description of the model version
        Returns:
            Version ID of the registered model version
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version_id = str(uuid.uuid4())
        metadata = ModelMetadata(model_id, version_id, model.name, description, model.model_type, framework, parameters, metrics, artifact_path)
        version = ModelVersion(version_id, model_id, metadata)
        model.add_version(version)
        logger.info(f"Registered new version: {version_id} for model: {model_id}")
        return version_id

    def get_model(self, model_id: str) -> Model:
        """Get a model by ID
        Args:
            model_id: ID of the model
        Returns:
            Model instance or None if not found
        """
        return self._models.get(model_id)

    def get_all_models(self) -> typing.List[Model]:
        """Get all registered models
        Returns:
            List of all models
        """
        return list(self._models.values())

    def get_model_by_type(self, model_type: str) -> typing.List[Model]:
        """Get models of a specific type
        Args:
            model_type: Type of the model
        Returns:
            List of models matching the specified type
        """
        return [model for model in self._models.values() if model.model_type == model_type]

    def activate_model_version(self, model_id: str, version_id: str) -> bool:
        """Set the active version of a model
        Args:
            model_id: ID of the model
            version_id: ID of the version
        Returns:
            True if activation successful
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        return model.set_active_version(version_id)

    def get_active_model(self, model_type: str) -> typing.Tuple[Model, ModelVersion]:
        """Get the active version of a model by type
        Args:
            model_type: Type of the model
        Returns:
            (Model, ModelVersion) or (None, None) if not found
        """
        models = self.get_model_by_type(model_type)
        for model in models:
            active_version = model.get_active_version()
            if active_version:
                return model, active_version

        return None, None

    def evaluate_model(self, model_id: str, version_id: str, test_data: list, parameters: dict) -> ModelEvaluationResult:
        """Evaluate a model version with test data
        Args:
            model_id: ID of the model
            version_id: ID of the version
            test_data: Test data for evaluation
            parameters: Evaluation parameters
        Returns:
            Evaluation result
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version = model.get_version(version_id)
        if not version:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        model_instance = version.load()
        model_type = model.model_type

        evaluation_result = self._evaluator.evaluate_model(model_instance, model_id, version_id, model_type, test_data, parameters)
        logger.info(f"Evaluated model {model_id} version {version_id} with result: {evaluation_result.get_summary()}")
        return evaluation_result

    def deploy_model_to_vertex(self, model_id: str, version_id: str, deployment_config: typing.Dict) -> str:
        """Deploy a model version to Vertex AI
        Args:
            model_id: ID of the model
            version_id: ID of the version
            deployment_config: Configuration for deployment
        Returns:
            Vertex AI endpoint URL
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version = model.get_version(version_id)
        if not version:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        endpoint_url = self._vertex_client.deploy_model_to_endpoint(model_id, version_id, version.metadata.artifact_path, deployment_config)
        logger.info(f"Deployed model {model_id} version {version_id} to Vertex AI endpoint: {endpoint_url}")
        return endpoint_url

    def load_model(self, model_id: str, version_id: str) -> object:
        """Load a model version into memory
        Args:
            model_id: ID of the model
            version_id: ID of the version
        Returns:
            Loaded model instance
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version = model.get_version(version_id)
        if not version:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        return version.load()

    def unload_model(self, model_id: str, version_id: str) -> bool:
        """Unload a model version from memory
        Args:
            model_id: ID of the model
            version_id: ID of the version
        Returns:
            True if unload successful
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version = model.get_version(version_id)
        if not version:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        return version.unload()

    def get_model_server(self, serving_mode: str) -> ModelServer:
        """Get or create a model server for a specific serving mode
        Args:
            serving_mode: Serving mode (e.g., "local", "vertex_ai")
        Returns:
            Model server instance
        """
        if serving_mode not in self._model_servers:
            self._model_servers[serving_mode] = ModelServingFactory.create_server(serving_mode, self._config)
        return self._model_servers[serving_mode]

    def predict(self, model_id: str, input_data: dict, serving_mode: str, version_id: str = "latest") -> dict:
        """Make a prediction using a model
        Args:
            model_id: ID of the model
            input_data: Input data for prediction
            serving_mode: Serving mode (e.g., "local", "vertex_ai")
            version_id: ID of the version
        Returns:
            Prediction result
        """
        model = self.get_model(model_id)
        if not model:
            raise ValueError(f"Model with ID {model_id} not found")

        version = model.get_version(version_id)
        if not version:
            raise ValueError(f"Version with ID {version_id} not found for model {model_id}")

        model_server = self.get_model_server(serving_mode)
        return model_server.predict(model_id, input_data, version_id)

    def _load_models_from_registry(self) -> bool:
        """Load models from registry into memory
        Returns:
            True if loading successful
        """
        # Load models from registry
        # For each model, create Model instance
        # For each version, create ModelVersion instance
        # Add models and versions to models dictionary
        return True