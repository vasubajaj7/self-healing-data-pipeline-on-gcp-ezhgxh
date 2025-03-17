"""
Implements a centralized model registry for the self-healing AI engine, providing functionality to register, version, track, and manage machine learning models. This module serves as the source of truth for model metadata and enables seamless integration with both local model storage and Vertex AI.
"""

import typing  # standard library
import os  # standard library
import json  # standard library
from datetime import datetime  # standard library
import uuid  # standard library

from google.cloud import aiplatform  # version: 1.25.0

from src.backend.utils.logging.logger import get_logger  # Internal import
from src.backend.utils.ml import model_utils  # Internal import
from src.backend.utils.ml.vertex_client import VertexAIClient, upload_model_to_vertex, deploy_model_to_endpoint  # Internal import
from src.backend import config  # Internal import

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


class ModelRegistry:
    """Central registry for managing AI models used in the self-healing pipeline"""

    def __init__(self, config: typing.Dict):
        """Initialize the model registry with configuration

        Args:
            config: Configuration dictionary
        """
        self._models = {}
        self._versions = {}
        self._active_versions = {}
        self._base_path = config.get("model_registry.base_path", DEFAULT_MODEL_BASE_PATH)
        self._use_vertex_ai = config.get("model_registry.use_vertex_ai", False)
        self._vertex_client = VertexAIClient(config) if self._use_vertex_ai else None
        self._config = config

        self.load_registry_state()
        if self._use_vertex_ai:
            self.sync_with_vertex()

    def register_model(self, name: str, description: str, model_type: str, metadata: typing.Dict) -> str:
        """Register a new model in the registry

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

        model_id = self._generate_model_id()
        model_entry = {
            "id": model_id,
            "name": name,
            "description": description,
            "type": model_type,
            "metadata": metadata,
            "created_at": datetime.now().isoformat(),
            "versions": []
        }

        self._models[model_id] = model_entry
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.register_model(model_id, name, description, model_type, metadata)

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
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")

        version_id = self._generate_version_id()
        version_entry = {
            "id": version_id,
            "model_id": model_id,
            "artifact_path": artifact_path,
            "framework": framework,
            "parameters": parameters,
            "metrics": metrics,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "status": "DRAFT"
        }

        self._versions[version_id] = version_entry
        self._models[model_id]["versions"].append(version_id)
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.register_model_version(model_id, version_id, artifact_path, framework, parameters, metrics, description)

        return version_id

    def get_model(self, model_id: str) -> typing.Dict:
        """Get a model by ID

        Args:
            model_id: ID of the model

        Returns:
            Model information or None if not found
        """
        return self._models.get(model_id)

    def get_model_version(self, model_id: str, version_id: str) -> typing.Dict:
        """Get a specific version of a model

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            Version information or None if not found
        """
        if model_id not in self._models:
            return None
        if version_id not in self._versions:
            return None
        return self._versions.get(version_id)

    def get_all_models(self) -> typing.List[typing.Dict]:
        """Get all registered models

        Returns:
            List of model information dictionaries
        """
        return list(self._models.values())

    def get_models_by_type(self, model_type: str) -> typing.List[typing.Dict]:
        """Get models of a specific type

        Args:
            model_type: Type of the model

        Returns:
            List of models matching the specified type
        """
        return [model for model in self._models.values() if model["type"] == model_type]

    def get_latest_version(self, model_id: str) -> typing.Dict:
        """Get the latest version of a model

        Args:
            model_id: ID of the model

        Returns:
            Latest version information or None if not found
        """
        if model_id not in self._models:
            return None

        versions = [self._versions[version_id] for version_id in self._models[model_id]["versions"] if version_id in self._versions]
        if not versions:
            return None

        latest_version = max(versions, key=lambda v: v["created_at"])
        return latest_version

    def get_active_version(self, model_id: str) -> typing.Dict:
        """Get the active version of a model

        Args:
            model_id: ID of the model

        Returns:
            Active version information or None if not found
        """
        if model_id not in self._active_versions:
            return None

        version_id = self._active_versions[model_id]
        return self._versions.get(version_id)

    def set_active_version(self, model_id: str, version_id: str) -> bool:
        """Set the active version of a model

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            True if activation successful
        """
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")
        if version_id not in self._versions:
            raise ValueError(f"Version with ID {version_id} not found")

        self._active_versions[model_id] = version_id
        self._versions[version_id]["status"] = "ACTIVE"
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.set_active_version(model_id, version_id)

        return True

    def deploy_to_vertex(self, model_id: str, version_id: str, deployment_config: typing.Dict) -> str:
        """Deploy a model version to Vertex AI

        Args:
            model_id: ID of the model
            version_id: ID of the version
            deployment_config: Configuration for deployment

        Returns:
            Vertex AI endpoint URL
        """
        if not self._use_vertex_ai:
            raise ValueError("Vertex AI integration is not enabled")
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")
        if version_id not in self._versions:
            raise ValueError(f"Version with ID {version_id} not found")

        artifact_path = self._versions[version_id]["artifact_path"]
        endpoint_url = deploy_model_to_endpoint(model_id, version_id, artifact_path, deployment_config)

        self._versions[version_id]["endpoint_url"] = endpoint_url
        self.save_registry_state()

        return endpoint_url

    def delete_model(self, model_id: str) -> bool:
        """Delete a model and all its versions

        Args:
            model_id: ID of the model

        Returns:
            True if deletion successful
        """
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")

        for version_id in self._models[model_id]["versions"]:
            self.delete_model_version(model_id, version_id)

        del self._models[model_id]
        if model_id in self._active_versions:
            del self._active_versions[model_id]
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.delete_model(model_id)

        return True

    def delete_model_version(self, model_id: str, version_id: str) -> bool:
        """Delete a specific version of a model

        Args:
            model_id: ID of the model
            version_id: ID of the version

        Returns:
            True if deletion successful
        """
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")
        if version_id not in self._versions:
            raise ValueError(f"Version with ID {version_id} not found")

        self._models[model_id]["versions"].remove(version_id)
        del self._versions[version_id]
        if model_id in self._active_versions and self._active_versions[model_id] == version_id:
            del self._active_versions[model_id]
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.delete_model_version(model_id, version_id)

        return True

    def update_model_metadata(self, model_id: str, metadata: typing.Dict) -> bool:
        """Update metadata for a model

        Args:
            model_id: ID of the model
            metadata: Metadata to update

        Returns:
            True if update successful
        """
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")

        self._models[model_id]["metadata"].update(metadata)
        self._models[model_id]["updated_at"] = datetime.now().isoformat()
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.update_model_metadata(model_id, metadata)

        return True

    def update_version_metadata(self, model_id: str, version_id: str, metadata: typing.Dict) -> bool:
        """Update metadata for a model version

        Args:
            model_id: ID of the model
            version_id: ID of the version
            metadata: Metadata to update

        Returns:
            True if update successful
        """
        if model_id not in self._models:
            raise ValueError(f"Model with ID {model_id} not found")
        if version_id not in self._versions:
            raise ValueError(f"Version with ID {version_id} not found")

        self._versions[version_id]["metadata"].update(metadata)
        self._versions[version_id]["updated_at"] = datetime.now().isoformat()
        self.save_registry_state()

        if self._use_vertex_ai:
            self._vertex_client.update_version_metadata(model_id, version_id, metadata)

        return True

    def save_registry_state(self) -> bool:
        """Save the registry state to disk

        Returns:
            True if save successful
        """
        registry_dir = self._base_path
        if not os.path.exists(registry_dir):
            os.makedirs(registry_dir, exist_ok=True)

        registry_file = os.path.join(registry_dir, "model_registry.json")
        data = {
            "models": self._models,
            "versions": self._versions,
            "active_versions": self._active_versions
        }

        with open(registry_file, "w") as f:
            json.dump(data, f, indent=4)

        return True

    def load_registry_state(self) -> bool:
        """Load the registry state from disk

        Returns:
            True if load successful
        """
        registry_dir = self._base_path
        registry_file = os.path.join(registry_dir, "model_registry.json")

        if not os.path.exists(registry_file):
            return False

        with open(registry_file, "r") as f:
            data = json.load(f)
            self._models = data.get("models", {})
            self._versions = data.get("versions", {})
            self._active_versions = data.get("active_versions", {})

        return True

    def sync_with_vertex(self) -> bool:
        """Synchronize registry with Vertex AI

        Returns:
            True if sync successful
        """
        if not self._use_vertex_ai:
            return False

        vertex_models = self._vertex_client.list_models()
        for model in vertex_models:
            if model.name not in self._models:
                self._models[model.name] = {
                    "id": model.name,
                    "name": model.display_name,
                    "description": model.description,
                    "type": model.metadata.get("model_type", "unknown"),
                    "metadata": model.metadata,
                    "created_at": model.create_time.isoformat(),
                    "versions": []
                }

        # Upload local models not in Vertex AI
        for model_id, model_data in self._models.items():
            if model_id not in vertex_models:
                upload_model_to_vertex(model_id, model_data["name"], model_data["description"], model_data["type"], model_data["metadata"])

        self.save_registry_state()
        return True

    def _generate_model_id(self) -> str:
        """Generate a unique model ID

        Returns:
            Unique model ID
        """
        return str(uuid.uuid4())

    def _generate_version_id(self) -> str:
        """Generate a unique version ID

        Returns:
            Unique version ID
        """
        return str(uuid.uuid4())