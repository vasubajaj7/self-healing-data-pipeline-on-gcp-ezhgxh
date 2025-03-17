"""
Provides utility functions for machine learning model operations including saving, loading, and managing model artifacts.

This module serves as a foundation for the self-healing AI engine by providing standardized interfaces for
model persistence, metadata handling, and model information retrieval. It supports multiple serialization
formats including pickle, joblib, TensorFlow, Keras, and ONNX.

Functions include:
- Saving and loading models in various formats
- Managing model metadata
- Listing available models and versions
- Standardized path creation for model artifacts
- Model format detection and validation

These utilities enable consistent model management across the self-healing data pipeline.
"""

import os
import json
import pickle
import typing
import datetime
from typing import Dict, List, Optional, Any, Union, Tuple

import joblib  # version 1.2.0
import tensorflow as tf  # version 2.12.x

from utils.logging.logger import get_logger
from utils.config.config_loader import get_config

# Initialize logger
logger = get_logger(__name__)

# Default model directory path
DEFAULT_MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'models')

# Supported model formats and their file extensions
SUPPORTED_FORMATS = {
    "pickle": ".pkl", 
    "joblib": ".joblib", 
    "tensorflow": ".h5", 
    "keras": ".keras", 
    "onnx": ".onnx"
}


class ModelFormatError(Exception):
    """Exception raised for errors related to model format."""
    
    def __init__(self, message: str):
        """Initialize the exception with a message.
        
        Args:
            message: Error message explaining the format issue
        """
        super().__init__(message)


class ModelIOError(Exception):
    """Exception raised for errors related to model I/O operations."""
    
    def __init__(self, message: str):
        """Initialize the exception with a message.
        
        Args:
            message: Error message explaining the I/O issue
        """
        super().__init__(message)


def save_model(model: Any, model_path: str, format: str, metadata: Optional[Dict[str, Any]] = None) -> str:
    """Saves a machine learning model to disk in the specified format.
    
    Args:
        model: The model object to save
        model_path: Path where the model should be saved
        format: Format to save the model in (pickle, joblib, tensorflow, keras, onnx)
        metadata: Optional dictionary of metadata to save alongside the model
        
    Returns:
        Path to the saved model
        
    Raises:
        ModelFormatError: If the specified format is not supported
        ModelIOError: If there's an error saving the model
        ValueError: If model is None
    """
    if model is None:
        raise ValueError("Cannot save None model")
    
    # Validate format is supported
    if not validate_model_format(format):
        raise ModelFormatError(f"Unsupported model format: {format}. "
                              f"Supported formats are: {', '.join(SUPPORTED_FORMATS.keys())}")
    
    # Create directory structure if it doesn't exist
    model_dir = os.path.dirname(model_path)
    if not os.path.exists(model_dir):
        logger.debug(f"Creating model directory: {model_dir}")
        os.makedirs(model_dir, exist_ok=True)
    
    # Determine file extension based on format
    extension = SUPPORTED_FORMATS.get(format)
    if not model_path.endswith(extension):
        model_path = f"{model_path}{extension}"
    
    logger.info(f"Saving model in {format} format to {model_path}")
    
    try:
        # Save model using appropriate method for the format
        if format == "pickle":
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
        elif format == "joblib":
            joblib.dump(model, model_path)
        elif format == "tensorflow" or format == "keras":
            # Handle TensorFlow/Keras models
            if hasattr(model, 'save'):
                model.save(model_path)
            else:
                # For plain TensorFlow saved_model format
                tf.saved_model.save(model, model_path)
        elif format == "onnx":
            # ONNX requires special handling, usually models are already converted
            with open(model_path, 'wb') as f:
                f.write(model.SerializeToString())
        
        # Save metadata if provided
        if metadata:
            metadata_path = save_model_metadata(model_path, metadata)
            logger.debug(f"Saved model metadata to {metadata_path}")
        
        logger.info(f"Successfully saved model to {model_path}")
        return model_path
    
    except Exception as e:
        error_msg = f"Error saving model to {model_path}: {str(e)}"
        logger.error(error_msg)
        raise ModelIOError(error_msg) from e


def load_model(model_path: str, format: Optional[str] = None) -> Any:
    """Loads a machine learning model from disk.
    
    Args:
        model_path: Path to the model file
        format: Format of the model (pickle, joblib, tensorflow, keras, onnx), 
                will be detected from file extension if not specified
    
    Returns:
        Loaded model object
        
    Raises:
        ModelFormatError: If the format is not supported or cannot be determined
        ModelIOError: If there's an error loading the model
        FileNotFoundError: If the model file doesn't exist
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    # Determine format from file extension if not specified
    if format is None:
        format = detect_model_format(model_path)
        if format is None:
            raise ModelFormatError(f"Could not determine model format from path: {model_path}")
    
    # Validate format is supported
    if not validate_model_format(format):
        raise ModelFormatError(f"Unsupported model format: {format}. "
                              f"Supported formats are: {', '.join(SUPPORTED_FORMATS.keys())}")
    
    logger.info(f"Loading model in {format} format from {model_path}")
    
    try:
        # Load model using appropriate method for the format
        if format == "pickle":
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
        elif format == "joblib":
            model = joblib.load(model_path)
        elif format == "tensorflow":
            model = tf.keras.models.load_model(model_path)
        elif format == "keras":
            model = tf.keras.models.load_model(model_path)
        elif format == "onnx":
            # For ONNX, we typically use an ONNX runtime for inference
            import onnx
            model = onnx.load(model_path)
        
        logger.info(f"Successfully loaded model from {model_path}")
        return model
    
    except Exception as e:
        error_msg = f"Error loading model from {model_path}: {str(e)}"
        logger.error(error_msg)
        raise ModelIOError(error_msg) from e


def get_model_info(model_path: str) -> Dict[str, Any]:
    """Retrieves information about a saved model.
    
    Args:
        model_path: Path to the model file
        
    Returns:
        Dictionary containing model information including metadata
        
    Raises:
        FileNotFoundError: If the model file doesn't exist
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    # Initialize model info dictionary
    model_info = {
        "path": model_path,
        "filename": os.path.basename(model_path),
        "format": detect_model_format(model_path),
        "size_bytes": os.path.getsize(model_path),
        "created_at": datetime.datetime.fromtimestamp(os.path.getctime(model_path)).isoformat(),
        "modified_at": datetime.datetime.fromtimestamp(os.path.getmtime(model_path)).isoformat()
    }
    
    # Load metadata if available
    try:
        metadata = load_model_metadata(model_path)
        if metadata:
            model_info["metadata"] = metadata
    except Exception as e:
        logger.warning(f"Could not load metadata for model {model_path}: {str(e)}")
        model_info["metadata"] = {}
    
    # For TensorFlow models, try to get additional info
    if model_info["format"] in ["tensorflow", "keras"]:
        try:
            # Load model summary if possible (without full model loading)
            if tf.io.gfile.exists(f"{model_path}/saved_model.pb"):
                model_info["is_saved_model"] = True
                
                # Extract info from SavedModel
                try:
                    saved_model_info = tf.saved_model.load(model_path, tags=["serve"])
                    signatures = list(saved_model_info.signatures.keys())
                    model_info["signatures"] = signatures
                except Exception as e:
                    logger.debug(f"Could not load SavedModel info: {str(e)}")
        except Exception as e:
            logger.debug(f"Error getting TensorFlow model info: {str(e)}")
    
    logger.debug(f"Retrieved info for model at {model_path}")
    return model_info


def create_model_path(model_id: str, version_id: Optional[str] = None, base_dir: Optional[str] = None) -> str:
    """Creates a standardized path for a model based on ID and version.
    
    Args:
        model_id: Unique identifier for the model
        version_id: Version identifier (default: "latest")
        base_dir: Base directory for models (default: DEFAULT_MODEL_DIR)
        
    Returns:
        Standardized model path
        
    Raises:
        ValueError: If model_id is empty
    """
    if not model_id:
        raise ValueError("Model ID cannot be empty")
    
    # Use default model directory if not specified
    if base_dir is None:
        base_dir = DEFAULT_MODEL_DIR
    
    # Use "latest" as version_id if not specified
    if version_id is None:
        version_id = "latest"
    
    # Construct path with pattern: {base_dir}/{model_id}/{version_id}
    model_path = os.path.join(base_dir, model_id, version_id)
    
    logger.debug(f"Created model path: {model_path}")
    return model_path


def list_models(base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists all models in the specified directory.
    
    Args:
        base_dir: Base directory for models (default: DEFAULT_MODEL_DIR)
        
    Returns:
        List of dictionaries containing model information
    """
    # Use default model directory if not specified
    if base_dir is None:
        base_dir = DEFAULT_MODEL_DIR
    
    # Ensure directory exists
    if not os.path.exists(base_dir):
        logger.warning(f"Model directory does not exist: {base_dir}")
        return []
    
    models = []
    
    # Scan base directory for model folders
    try:
        for model_id in os.listdir(base_dir):
            model_dir = os.path.join(base_dir, model_id)
            
            # Only include directories
            if os.path.isdir(model_dir):
                # Get versions for this model
                versions = []
                try:
                    for version_id in os.listdir(model_dir):
                        version_dir = os.path.join(model_dir, version_id)
                        if os.path.isdir(version_dir):
                            # Check if directory contains model files
                            model_files = [f for f in os.listdir(version_dir) 
                                          if any(f.endswith(ext) for ext in SUPPORTED_FORMATS.values())]
                            
                            if model_files:
                                versions.append(version_id)
                except Exception as e:
                    logger.warning(f"Error reading versions for model {model_id}: {str(e)}")
                
                if versions:
                    model_info = {
                        "model_id": model_id,
                        "versions": versions,
                        "version_count": len(versions),
                        "path": model_dir
                    }
                    models.append(model_info)
    
    except Exception as e:
        logger.error(f"Error listing models in {base_dir}: {str(e)}")
    
    logger.debug(f"Found {len(models)} models in {base_dir}")
    return models


def list_model_versions(model_id: str, base_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists all versions of a specific model.
    
    Args:
        model_id: ID of the model to list versions for
        base_dir: Base directory for models (default: DEFAULT_MODEL_DIR)
        
    Returns:
        List of dictionaries containing version information
        
    Raises:
        ValueError: If model_id is empty
    """
    if not model_id:
        raise ValueError("Model ID cannot be empty")
    
    # Use default model directory if not specified
    if base_dir is None:
        base_dir = DEFAULT_MODEL_DIR
    
    model_dir = os.path.join(base_dir, model_id)
    
    # Check if model directory exists
    if not os.path.exists(model_dir):
        logger.warning(f"Model directory does not exist: {model_dir}")
        return []
    
    versions = []
    
    # Scan model directory for version folders
    try:
        for version_id in os.listdir(model_dir):
            version_dir = os.path.join(model_dir, version_id)
            
            # Only include directories
            if os.path.isdir(version_dir):
                # Find model files in this version directory
                model_files = []
                for file in os.listdir(version_dir):
                    if any(file.endswith(ext) for ext in SUPPORTED_FORMATS.values()):
                        model_files.append(file)
                
                if model_files:
                    # Get creation time of the first model file
                    model_file_path = os.path.join(version_dir, model_files[0])
                    created_at = datetime.datetime.fromtimestamp(os.path.getctime(model_file_path))
                    
                    # Try to load metadata
                    metadata = {}
                    try:
                        metadata = load_model_metadata(model_file_path)
                    except Exception as e:
                        logger.debug(f"Could not load metadata for {model_file_path}: {str(e)}")
                    
                    version_info = {
                        "version_id": version_id,
                        "model_id": model_id,
                        "path": version_dir,
                        "created_at": created_at.isoformat(),
                        "files": model_files,
                        "metadata": metadata
                    }
                    versions.append(version_info)
    
    except Exception as e:
        logger.error(f"Error listing versions for model {model_id}: {str(e)}")
    
    # Sort versions by creation time, newest first
    versions.sort(key=lambda x: x["created_at"], reverse=True)
    
    logger.debug(f"Found {len(versions)} versions for model {model_id}")
    return versions


def delete_model(model_id: str, base_dir: Optional[str] = None) -> bool:
    """Deletes a model and all its versions.
    
    Args:
        model_id: ID of the model to delete
        base_dir: Base directory for models (default: DEFAULT_MODEL_DIR)
        
    Returns:
        True if deletion was successful, False otherwise
        
    Raises:
        ValueError: If model_id is empty
    """
    if not model_id:
        raise ValueError("Model ID cannot be empty")
    
    # Use default model directory if not specified
    if base_dir is None:
        base_dir = DEFAULT_MODEL_DIR
    
    model_dir = os.path.join(base_dir, model_id)
    
    # Check if model directory exists
    if not os.path.exists(model_dir):
        logger.warning(f"Model directory does not exist: {model_dir}")
        return False
    
    try:
        # Recursively delete all model files and directories
        for root, dirs, files in os.walk(model_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        
        # Remove the model directory itself
        os.rmdir(model_dir)
        
        logger.info(f"Successfully deleted model {model_id}")
        return True
    
    except Exception as e:
        logger.error(f"Error deleting model {model_id}: {str(e)}")
        return False


def delete_model_version(model_id: str, version_id: str, base_dir: Optional[str] = None) -> bool:
    """Deletes a specific version of a model.
    
    Args:
        model_id: ID of the model
        version_id: Version ID to delete
        base_dir: Base directory for models (default: DEFAULT_MODEL_DIR)
        
    Returns:
        True if deletion was successful, False otherwise
        
    Raises:
        ValueError: If model_id or version_id is empty
    """
    if not model_id:
        raise ValueError("Model ID cannot be empty")
    
    if not version_id:
        raise ValueError("Version ID cannot be empty")
    
    # Use default model directory if not specified
    if base_dir is None:
        base_dir = DEFAULT_MODEL_DIR
    
    version_dir = os.path.join(base_dir, model_id, version_id)
    
    # Check if version directory exists
    if not os.path.exists(version_dir):
        logger.warning(f"Version directory does not exist: {version_dir}")
        return False
    
    try:
        # Recursively delete all files and subdirectories
        for root, dirs, files in os.walk(version_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        
        # Remove the version directory itself
        os.rmdir(version_dir)
        
        logger.info(f"Successfully deleted version {version_id} of model {model_id}")
        return True
    
    except Exception as e:
        logger.error(f"Error deleting version {version_id} of model {model_id}: {str(e)}")
        return False


def save_model_metadata(model_path: str, metadata: Dict[str, Any]) -> str:
    """Saves metadata for a model.
    
    Args:
        model_path: Path to the model file
        metadata: Dictionary of metadata to save
        
    Returns:
        Path to the saved metadata file
        
    Raises:
        ValueError: If model_path doesn't exist or metadata is not a dictionary
        ModelIOError: If there's an error saving the metadata
    """
    if not os.path.exists(model_path):
        raise ValueError(f"Model file not found: {model_path}")
    
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a dictionary")
    
    # Add timestamp to metadata
    metadata_with_timestamp = metadata.copy()
    metadata_with_timestamp['timestamp'] = datetime.datetime.now().isoformat()
    
    # Determine metadata file path
    metadata_file_path = f"{model_path}.metadata.json"
    
    try:
        # Write metadata to file
        with open(metadata_file_path, 'w') as f:
            json.dump(metadata_with_timestamp, f, indent=2)
        
        logger.debug(f"Saved metadata to {metadata_file_path}")
        return metadata_file_path
    
    except Exception as e:
        error_msg = f"Error saving metadata to {metadata_file_path}: {str(e)}"
        logger.error(error_msg)
        raise ModelIOError(error_msg) from e


def load_model_metadata(model_path: str) -> Dict[str, Any]:
    """Loads metadata for a model.
    
    Args:
        model_path: Path to the model file
        
    Returns:
        Model metadata dictionary or empty dict if not found
        
    Raises:
        ValueError: If model_path doesn't exist
        ModelIOError: If there's an error loading the metadata
    """
    if not os.path.exists(model_path):
        raise ValueError(f"Model file not found: {model_path}")
    
    # Determine metadata file path
    metadata_file_path = f"{model_path}.metadata.json"
    
    # Check if metadata file exists
    if not os.path.exists(metadata_file_path):
        logger.debug(f"No metadata file found at {metadata_file_path}")
        return {}
    
    try:
        # Read and parse metadata file
        with open(metadata_file_path, 'r') as f:
            metadata = json.load(f)
        
        logger.debug(f"Loaded metadata from {metadata_file_path}")
        return metadata
    
    except Exception as e:
        error_msg = f"Error loading metadata from {metadata_file_path}: {str(e)}"
        logger.error(error_msg)
        raise ModelIOError(error_msg) from e


def detect_model_format(model_path: str) -> Optional[str]:
    """Detects the format of a model file based on extension.
    
    Args:
        model_path: Path to the model file
        
    Returns:
        Detected format or None if unknown
        
    Raises:
        ValueError: If model_path doesn't exist
    """
    if not os.path.exists(model_path):
        raise ValueError(f"Model file not found: {model_path}")
    
    # Extract file extension
    _, extension = os.path.splitext(model_path)
    
    # Find format that matches this extension
    for format_name, format_extension in SUPPORTED_FORMATS.items():
        if extension == format_extension:
            return format_name
    
    # Handle TensorFlow SavedModel format (directory-based)
    if os.path.isdir(model_path) and os.path.exists(os.path.join(model_path, "saved_model.pb")):
        return "tensorflow"
    
    # No matching format found
    logger.warning(f"Could not determine model format for {model_path}")
    return None


def validate_model_format(format: str) -> bool:
    """Validates if a specified format is supported.
    
    Args:
        format: Format to validate
        
    Returns:
        True if format is supported, False otherwise
    """
    return format.lower() in SUPPORTED_FORMATS