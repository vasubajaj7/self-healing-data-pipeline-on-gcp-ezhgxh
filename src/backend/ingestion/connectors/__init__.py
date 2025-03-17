"""
Initialization module for the data ingestion connectors package. This file imports and exposes all connector implementations and the connector factory, enabling a unified interface for data extraction from various source systems. It also handles connector registration with the factory to support dynamic connector selection based on data source type.
"""

from .base_connector import BaseConnector, ConnectorFactory  # src/backend/ingestion/connectors/base_connector.py
from .gcs_connector import GCSConnector  # src/backend/ingestion/connectors/gcs_connector.py
from .cloudsql_connector import CloudSQLConnector  # src/backend/ingestion/connectors/cloudsql_connector.py
from .api_connector import ApiConnector, ApiAuthType, ApiPaginationType  # src/backend/ingestion/connectors/api_connector.py
from .custom_connector import CustomConnector  # src/backend/ingestion/connectors/custom_connector.py
from ...constants import DataSourceType  # src/backend/constants.py


def register_connectors() -> None:
    """Register all connector implementations with the connector factory"""
    ConnectorFactory().register_connector(DataSourceType.GCS, GCSConnector)
    ConnectorFactory().register_connector(DataSourceType.CLOUD_SQL, CloudSQLConnector)
    ConnectorFactory().register_connector(DataSourceType.API, ApiConnector)
    ConnectorFactory().register_connector(DataSourceType.CUSTOM, CustomConnector)


__all__ = [
    "BaseConnector",
    "ConnectorFactory",
    "GCSConnector",
    "CloudSQLConnector",
    "ApiConnector",
    "ApiAuthType",
    "ApiPaginationType",
    "CustomConnector",
    "DataSourceType",
    "register_connectors"
]