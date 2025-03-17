"""
Implements a connector for Cloud SQL databases in the self-healing data pipeline.
This connector enables extraction of data from Google Cloud SQL instances with support for incremental extraction, schema validation, and automatic error recovery.
"""

import json
import typing
from typing import Dict, List, Optional, Any, Tuple, Union
import datetime

import sqlalchemy  # version: 2.0.x
import pandas as pd  # version: 2.0.x
from google.cloud import sql  # package_version: 1.2.x

# Internal imports
from ...constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS  # Import constants for connector configuration and data source types
from .base_connector import BaseConnector  # Import base connector class for implementation
from ...utils.logging.logger import get_logger  # Configure logging for connector operations
from ...utils.auth.gcp_auth import get_credentials_for_service, GCPAuthError  # Handle GCP authentication for Cloud SQL connections
from ..errors import error_handler  # Handle errors during connector operations
from ..errors import retry_manager  # Implement retry logic for database operations

# Set up module logger
logger = get_logger(__name__)

# Define global constants for CloudSQLConnector
DEFAULT_BATCH_SIZE = 10000
DEFAULT_CONNECTION_TIMEOUT = 30
DEFAULT_QUERY_TIMEOUT = 300
RETRYABLE_EXCEPTIONS = [sqlalchemy.exc.OperationalError, sqlalchemy.exc.TimeoutError, sqlalchemy.exc.ResourceClosedError, sqlalchemy.exc.DisconnectionError]


def create_connection_string(connection_config: Dict[str, Any]) -> str:
    """Creates a SQLAlchemy connection string for Cloud SQL.

    Args:
        connection_config: Dictionary containing connection details.

    Returns:
        SQLAlchemy connection string.
    """
    db_type = connection_config.get('db_type')
    instance_connection_name = connection_config.get('instance_connection_name')
    database = connection_config.get('database')
    user = connection_config.get('user')
    password = connection_config.get('password')

    if db_type in ('postgres', 'postgresql'):
        return f'postgresql+pg8000://{user}:{password}@{instance_connection_name}/{database}'
    elif db_type == 'mysql':
        return f'mysql+pymysql://{user}:{password}@{instance_connection_name}/{database}'
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_dialect_module(db_type: str) -> typing.Any:
    """Gets the appropriate SQLAlchemy dialect module based on database type.

    Args:
        db_type: Database type ('postgres' or 'mysql').

    Returns:
        SQLAlchemy dialect module.
    """
    if db_type in ('postgres', 'postgresql'):
        return sqlalchemy.dialects.postgresql
    elif db_type == 'mysql':
        return sqlalchemy.dialects.mysql
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


class CloudSQLConnector(BaseConnector):
    """Connector for extracting data from Google Cloud SQL databases.

    This connector supports full table extraction, custom query extraction, and incremental data extraction.
    It uses SQLAlchemy for database connection and Pandas for data manipulation.
    """

    def __init__(self, source_id: str, source_name: str, connection_config: Dict[str, Any]):
        """Initialize the Cloud SQL connector with source information and connection configuration.

        Args:
            source_id: Unique identifier for the data source.
            source_name: Human-readable name of the data source.
            connection_config: Configuration parameters for connecting to the source.
        """
        super().__init__(source_id, source_name, DataSourceType.CLOUD_SQL, connection_config)

        self.engine: sqlalchemy.engine.Engine = None
        self.connection: sqlalchemy.Connection = None
        self.db_type: str = None
        self.instance_connection_name: str = None
        self.database: str = None
        self.user: str = None
        self.password: str = None
        self.connection_args: Dict[str, Any] = None
        self.circuit_breaker: retry_manager.CircuitBreaker = None

        # Extract connection details from connection_config
        self.db_type = self.connection_config.get('db_type')
        self.instance_connection_name = self.connection_config.get('instance_connection_name')
        self.database = self.connection_config.get('database')
        self.user = self.connection_config.get('user')
        self.password = self.connection_config.get('password')
        self.connection_args = self.connection_config.get('connection_args', {})

        # Initialize circuit breaker for connection management
        self.circuit_breaker = retry_manager.CircuitBreaker(name=f"CloudSQLConnector-{source_id}")

        # Validate connection configuration
        self.validate_connection_config(connection_config)

        logger.info(f"Initialized CloudSQLConnector for {source_name} (ID: {source_id})")

    @error_handler.with_error_handling(context={'connector_type': 'CloudSQL', 'operation': 'connect'}, raise_exception=False)
    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def connect(self) -> bool:
        """Establish connection to the Cloud SQL database.

        Returns:
            True if connection successful, False otherwise.
        """
        if not self.circuit_breaker.allow_request():
            logger.warning(f"Connection attempt blocked by circuit breaker for {self.source_name} (ID: {self.source_id})")
            return False

        try:
            # Create connection string
            connection_string = create_connection_string(self.connection_config)

            # Initialize SQLAlchemy engine
            self.engine = sqlalchemy.create_engine(connection_string, **self.connection_args)

            # Establish database connection
            self.connection = self.engine.connect()

            # Test connection with a simple query
            self.connection.execute(sqlalchemy.text("SELECT 1"))

            # Update connection state
            self._update_connection_state(connected=True, success=True)
            self.circuit_breaker.record_success()

            logger.info(f"Successfully connected to Cloud SQL database for {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            self._update_connection_state(connected=False, success=False)
            self.circuit_breaker.record_failure()
            logger.error(f"Failed to connect to Cloud SQL database for {self.source_name} (ID: {self.source_id}): {str(e)}")
            return False

    @error_handler.with_error_handling(context={'connector_type': 'CloudSQL', 'operation': 'disconnect'}, raise_exception=False)
    def disconnect(self) -> bool:
        """Close connection to the Cloud SQL database.

        Returns:
            True if disconnection successful, False otherwise.
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
                logger.info(f"Closed connection to Cloud SQL database for {self.source_name} (ID: {self.source_id})")

            if self.engine:
                self.engine.dispose()
                logger.info(f"Disposed engine for Cloud SQL database for {self.source_name} (ID: {self.source_id})")

            self.engine = None
            self.connection = None

            self._update_connection_state(connected=False, success=True)
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from Cloud SQL database for {self.source_name} (ID: {self.source_id}): {str(e)}")
            return False

    @error_handler.with_error_handling(context={'connector_type': 'CloudSQL', 'operation': 'extract_data'}, raise_exception=True)
    def extract_data(self, extraction_params: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """Extract data from Cloud SQL based on extraction parameters.

        Args:
            extraction_params: Parameters controlling the extraction process.

        Returns:
            Tuple containing:
                - Extracted data as pandas DataFrame (or None if extraction failed).
                - Metadata dictionary with extraction details.
        """
        if not self._validate_extraction_params(extraction_params):
            raise ValueError("Invalid extraction parameters")

        if not self.connect():
            raise ConnectionError(f"Failed to connect to Cloud SQL database for {self.source_name} (ID: {self.source_id})",
                                  service_name=self.source_name,
                                  connection_details=self.connection_config)

        table_name = extraction_params.get('table_name')
        query = extraction_params.get('query')
        batch_size = extraction_params.get('batch_size', DEFAULT_BATCH_SIZE)
        incremental_column = extraction_params.get('incremental_column')
        last_value = extraction_params.get('last_value')

        if table_name and not query and not incremental_column:
            # Extract all data from a table
            extracted_data = self._extract_full_table(table_name, batch_size)
            row_count = self._get_table_row_count(table_name)
            metadata = {'row_count': row_count, 'extraction_method': 'full_table'}

        elif query and not table_name and not incremental_column:
            # Extract data using a custom query
            query_params = extraction_params.get('query_params', {})
            extracted_data = self._extract_with_query(query, query_params, batch_size)
            metadata = {'row_count': len(extracted_data), 'extraction_method': 'query'}

        elif table_name and incremental_column:
            # Extract data incrementally
            extracted_data = self._extract_incremental(table_name, incremental_column, last_value, batch_size)
            metadata = {'row_count': len(extracted_data), 'extraction_method': 'incremental', 'incremental_column': incremental_column, 'last_value': last_value}

        else:
            raise ValueError("Invalid extraction parameters: specify either table_name, query, or both table_name and incremental_column")

        formatted_metadata = self._format_metadata(metadata)
        return extracted_data, formatted_metadata

    @error_handler.with_error_handling(context={'connector_type': 'CloudSQL', 'operation': 'get_source_schema'}, raise_exception=True)
    def get_source_schema(self, table_name: str) -> Dict[str, Any]:
        """Retrieve the schema information for a database table.

        Args:
            table_name: Name of the table to get schema for.

        Returns:
            Schema definition for the specified table.
        """
        if not self.connect():
            raise ConnectionError(f"Failed to connect to Cloud SQL database for {self.source_name} (ID: {self.source_id})",
                                  service_name=self.source_name,
                                  connection_details=self.connection_config)

        try:
            # Use SQLAlchemy reflection to inspect table schema
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata, autoload_with=self.engine)

            # Extract column names, types, and constraints
            schema = {
                'name': table_name,
                'columns': []
            }
            for column in table.columns:
                col_info = {
                    'name': column.name,
                    'type': str(column.type),
                    'nullable': column.nullable,
                    'primary_key': column.primary_key
                }
                schema['columns'].append(col_info)

            logger.info(f"Retrieved schema for table {table_name} from Cloud SQL database for {self.source_name} (ID: {self.source_id})")
            return schema

        except Exception as e:
            logger.error(f"Failed to retrieve schema for table {table_name} from Cloud SQL database for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    def validate_connection_config(self, config: Dict[str, Any]) -> bool:
        """Validate the Cloud SQL connection configuration.

        Args:
            config: Connection configuration to validate.

        Returns:
            True if configuration is valid, False otherwise.
        """
        required_fields = ['db_type', 'instance_connection_name', 'database', 'user', 'password']
        for field in required_fields:
            if field not in config:
                logger.error(f"Missing required field in connection config: {field}")
                return False

        if config['db_type'] not in ('postgres', 'postgresql', 'mysql'):
            logger.error(f"Unsupported database type: {config['db_type']}")
            return False

        # Basic validation of instance_connection_name format
        if not isinstance(config['instance_connection_name'], str) or ':' not in config['instance_connection_name']:
            logger.error("Invalid instance_connection_name format")
            return False

        # Validate additional connection arguments if provided
        if 'connection_args' in config and not isinstance(config['connection_args'], dict):
            logger.error("connection_args must be a dictionary")
            return False

        logger.info(f"Connection configuration validated for {self.source_name} (ID: {self.source_id})")
        return True

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _extract_full_table(self, table_name: str, batch_size: int) -> pd.DataFrame:
        """Extract all data from a specified table.

        Args:
            table_name: Name of the table to extract.
            batch_size: Number of rows to extract per batch.

        Returns:
            Extracted table data.
        """
        try:
            # Create SQLAlchemy table object using reflection
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata, autoload_with=self.engine)

            # Prepare SELECT query for the table
            select_query = sqlalchemy.select(table)

            # Execute query with batching if table is large
            data = []
            with self.engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(select_query)
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break
                    data.extend(rows)

            # Convert result to pandas DataFrame
            df = pd.DataFrame(data, columns=result.keys())
            logger.info(f"Extracted {len(df)} rows from table {table_name} for {self.source_name} (ID: {self.source_id})")
            return df

        except Exception as e:
            logger.error(f"Failed to extract data from table {table_name} for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _extract_with_query(self, query: str, query_params: Dict[str, Any], batch_size: int) -> pd.DataFrame:
        """Extract data using a custom SQL query.

        Args:
            query: SQL query to execute.
            query_params: Parameters for the query.
            batch_size: Number of rows to extract per batch.

        Returns:
            Query result data.
        """
        try:
            # Prepare parameterized query
            compiled_query = sqlalchemy.text(query)

            # Execute query with appropriate timeout
            data = []
            with self.engine.connect() as conn:
                result = conn.execution_options(stream_results=True, timeout=DEFAULT_QUERY_TIMEOUT).execute(compiled_query, query_params)
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break
                    data.extend(rows)

            # Convert result to pandas DataFrame
            df = pd.DataFrame(data, columns=result.keys())
            logger.info(f"Extracted {len(df)} rows using query for {self.source_name} (ID: {self.source_id})")
            return df

        except Exception as e:
            logger.error(f"Failed to extract data using query for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _extract_incremental(self, table_name: str, incremental_column: str, last_value: Any, batch_size: int) -> pd.DataFrame:
        """Extract data incrementally based on a timestamp or ID column.

        Args:
            table_name: Name of the table to extract.
            incremental_column: Name of the column to use for incremental extraction.
            last_value: The last extracted value of the incremental column.
            batch_size: Number of rows to extract per batch.

        Returns:
            Incrementally extracted data.
        """
        try:
            # Create SQLAlchemy table object using reflection
            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata, autoload_with=self.engine)
            column = table.columns[incremental_column]

            # Determine column type of incremental_column
            column_type = column.type.compile(dialect=self.engine.dialect)

            # Prepare incremental query with appropriate comparison operator
            if last_value is None:
                # If no last_value, extract all rows
                select_query = sqlalchemy.select(table)
            else:
                # Add parameter for last_value with correct type conversion
                if 'INTEGER' in column_type.upper():
                    select_query = sqlalchemy.select(table).where(column > int(last_value))
                elif 'VARCHAR' in column_type.upper() or 'TEXT' in column_type.upper():
                    select_query = sqlalchemy.select(table).where(column > str(last_value))
                elif 'TIMESTAMP' in column_type.upper() or 'DATETIME' in column_type.upper():
                    select_query = sqlalchemy.select(table).where(column > pd.to_datetime(last_value))
                else:
                    select_query = sqlalchemy.select(table).where(column > last_value)

            # Execute query with appropriate timeout
            data = []
            with self.engine.connect() as conn:
                result = conn.execution_options(stream_results=True, timeout=DEFAULT_QUERY_TIMEOUT).execute(select_query)
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break
                    data.extend(rows)

            # Convert result to pandas DataFrame
            df = pd.DataFrame(data, columns=result.keys())
            logger.info(f"Extracted {len(df)} rows incrementally from table {table_name} for {self.source_name} (ID: {self.source_id})")
            return df

        except Exception as e:
            logger.error(f"Failed to extract data incrementally from table {table_name} for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _get_table_row_count(self, table_name: str) -> int:
        """Get the total number of rows in a table.

        Args:
            table_name: Name of the table.

        Returns:
            Row count.
        """
        try:
            # Prepare COUNT(*) query for the table
            count_query = sqlalchemy.select(sqlalchemy.func.count()).select_from(sqlalchemy.text(table_name))

            # Execute query with appropriate timeout
            with self.engine.connect() as conn:
                result = conn.execute(count_query)
                row_count = result.scalar()

            logger.info(f"Retrieved row count for table {table_name} for {self.source_name} (ID: {self.source_id}): {row_count}")
            return row_count

        except Exception as e:
            logger.error(f"Failed to retrieve row count for table {table_name} for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _get_max_value(self, table_name: str, column_name: str) -> Any:
        """Get the maximum value of a column in a table.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.

        Returns:
            Maximum value.
        """
        try:
            # Prepare MAX(column_name) query for the table
            max_query = sqlalchemy.select(sqlalchemy.func.max(sqlalchemy.text(column_name))).select_from(sqlalchemy.text(table_name))

            # Execute query with appropriate timeout
            with self.engine.connect() as conn:
                result = conn.execute(max_query)
                max_value = result.scalar()

            logger.info(f"Retrieved max value for column {column_name} in table {table_name} for {self.source_name} (ID: {self.source_id}): {max_value}")
            return max_value

        except Exception as e:
            logger.error(f"Failed to retrieve max value for column {column_name} in table {table_name} for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise

    @retry_manager.retry_with_backoff(max_retries=MAX_RETRY_ATTEMPTS, backoff_factor=2.0, retryable_exceptions=RETRYABLE_EXCEPTIONS)
    def _list_tables(self) -> List[str]:
        """List all tables in the connected database.

        Returns:
            List of table names.
        """
        try:
            # Use SQLAlchemy inspector to get table names
            inspector = sqlalchemy.inspect(self.engine)
            table_names = inspector.get_table_names()

            # Filter system tables if needed
            user_table_names = [table for table in table_names if not table.startswith('pg_')]

            logger.info(f"Retrieved list of tables for {self.source_name} (ID: {self.source_id}): {user_table_names}")
            return user_table_names

        except Exception as e:
            logger.error(f"Failed to retrieve list of tables for {self.source_name} (ID: {self.source_id}): {str(e)}")
            raise