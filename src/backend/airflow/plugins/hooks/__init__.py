"""
Initialization module for custom Airflow hooks in the self-healing data pipeline.
Imports and exposes all hook classes and utility functions from the hooks package,
making them available for use in Airflow DAGs and operators.
"""

# Import API hook classes for external API integration
from .api_hooks import ApiPaginationConfig  # Import ApiPaginationConfig class
from .api_hooks import ApiHook  # Import ApiHook class
from .api_hooks import SelfHealingApiHook  # Import SelfHealingApiHook class

# Import BigQuery hook classes for data warehouse operations
from .bigquery_hooks import EnhancedBigQueryHook  # Import EnhancedBigQueryHook class
from .bigquery_hooks import SelfHealingBigQueryHook  # Import SelfHealingBigQueryHook class
from .bigquery_hooks import format_schema_field  # Import format_schema_field function
from .bigquery_hooks import format_schema  # Import format_schema function

# Import Cloud SQL hook classes for database operations
from .cloudsql_hooks import EnhancedCloudSQLHook  # Import EnhancedCloudSQLHook class
from .cloudsql_hooks import SelfHealingCloudSQLHook  # Import SelfHealingCloudSQLHook class
from .cloudsql_hooks import build_connection_string  # Import build_connection_string function
from .cloudsql_hooks import map_sqlalchemy_exception_to_pipeline_error  # Import map_sqlalchemy_exception_to_pipeline_error function

# Import GCS hook classes for cloud storage operations
from .gcs_hooks import EnhancedGCSHook  # Import EnhancedGCSHook class
from .gcs_hooks import SelfHealingGCSHook  # Import SelfHealingGCSHook class
from .gcs_hooks import get_file_format_from_extension  # Import get_file_format_from_extension function

# Import Vertex AI hook classes for machine learning operations
from .vertex_hooks import VertexAIHook  # Import VertexAIHook class
from .vertex_hooks import VertexModelHook  # Import VertexModelHook class
from .vertex_hooks import VertexEndpointHook  # Import VertexEndpointHook class
from .vertex_hooks import VertexTrainingHook  # Import VertexTrainingHook class
from .vertex_hooks import VertexDatasetHook  # Import VertexDatasetHook class
from .vertex_hooks import SelfHealingVertexHook  # Import SelfHealingVertexHook class
from .vertex_hooks import format_model_path  # Import format_model_path function
from .vertex_hooks import format_endpoint_path  # Import format_endpoint_path function


__all__ = [  # Define the public interface of the module
    "ApiPaginationConfig",
    "ApiHook",
    "SelfHealingApiHook",
    "EnhancedBigQueryHook",
    "SelfHealingBigQueryHook",
    "format_schema_field",
    "format_schema",
    "EnhancedCloudSQLHook",
    "SelfHealingCloudSQLHook",
    "build_connection_string",
    "map_sqlalchemy_exception_to_pipeline_error",
    "EnhancedGCSHook",
    "SelfHealingGCSHook",
    "get_file_format_from_extension",
    "VertexAIHook",
    "VertexModelHook",
    "VertexEndpointHook",
    "VertexTrainingHook",
    "VertexDatasetHook",
    "SelfHealingVertexHook",
    "format_model_path",
    "format_endpoint_path",
]