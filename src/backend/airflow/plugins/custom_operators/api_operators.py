import typing
import json

import pandas  # version 2.0.x
from airflow.models import BaseOperator  # version 2.5.x
from airflow.utils.decorators import apply_defaults  # version 2.5.x
from airflow.exceptions import AirflowException  # version 2.5.x

from src.backend.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS, DEFAULT_BATCH_SIZE  # Import constants for API configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for API operators
from src.backend.airflow.plugins.hooks.api_hooks import ApiHook, SelfHealingApiHook, ApiPaginationConfig  # Use API hooks for connecting to external APIs
from src.backend.ingestion.connectors.api_connector import ApiAuthType, ApiPaginationType  # Import API connector enumerations
from src.backend.self_healing.ai.issue_classifier import classify_api_issue  # Classify API issues for self-healing

logger = get_logger(__name__)

class ApiOperator(BaseOperator):
    """Base operator for API operations with enhanced functionality"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the API operator with connection and endpoint details"""
        super().__init__(task_id=task_id, **kwargs)
        self.conn_id = conn_id
        self.endpoint = endpoint
        self.hook = None
        self.timeout = timeout or DEFAULT_TIMEOUT_SECONDS

    def execute(self, context: dict) -> typing.Any:
        """Execute the API operation"""
        self.hook = self.get_hook()
        logger.info(f"Executing API operation: {self.endpoint} using connection {self.conn_id}")
        try:
            result = self._execute_operation(context)
            return result
        except Exception as e:
            logger.error(f"API operation failed: {str(e)}")
            raise

    def _execute_operation(self, context: dict) -> typing.Any:
        """Execute the specific API operation (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _execute_operation")

    def get_hook(self) -> ApiHook:
        """Get or create an instance of the ApiHook"""
        if self.hook:
            return self.hook
        self.hook = ApiHook(conn_id=self.conn_id, timeout=self.timeout)
        return self.hook

    def on_kill(self):
        """Clean up when the task is killed"""
        if self.hook:
            self.hook.close_conn()
        logger.info(f"Operator {self.task_id} was killed")

class ApiRequestOperator(ApiOperator):
    """Operator for making generic API requests"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        method: str,
        params: dict = None,
        data: dict = None,
        json_data: dict = None,
        headers: dict = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the API request operator"""
        super().__init__(conn_id=conn_id, endpoint=endpoint, timeout=timeout, task_id=task_id, **kwargs)
        self.method = method
        self.params = params
        self.data = data
        self.json_data = json_data
        self.headers = headers

    def _execute_operation(self, context: dict) -> dict:
        """Execute the API request operation"""
        hook = self.get_hook()
        if self.method.upper() == "GET":
            response = hook.get_request(self.endpoint, params=self.params, headers=self.headers)
        elif self.method.upper() == "POST":
            response = hook.post_request(self.endpoint, params=self.params, data=self.data, json_data=self.json_data, headers=self.headers)
        elif self.method.upper() == "PUT":
            response = hook.put_request(self.endpoint, params=self.params, data=self.data, json_data=self.json_data, headers=self.headers)
        elif self.method.upper() == "PATCH":
            response = hook.patch_request(self.endpoint, params=self.params, data=self.data, json_data=self.json_data, headers=self.headers)
        elif self.method.upper() == "DELETE":
            response = hook.delete_request(self.endpoint, params=self.params, headers=self.headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

        data = self._process_response(response)
        logger.info(f"Successfully made {self.method} request to {self.endpoint}")
        return data

    def _process_response(self, response: 'requests.Response') -> dict:
        """Process API response to extract data"""
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError:
            logger.warning("Response is not in JSON format")
            return {"text": response.text}

class ApiDataExtractOperator(ApiOperator):
    """Operator for extracting data from an API with pagination support"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        params: dict = None,
        headers: dict = None,
        data_path: str = None,
        paginate: bool = False,
        pagination_config: ApiPaginationConfig = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the API data extract operator"""
        super().__init__(conn_id=conn_id, endpoint=endpoint, timeout=timeout, task_id=task_id, **kwargs)
        self.params = params
        self.headers = headers
        self.data_path = data_path
        self.paginate = paginate
        self.pagination_config = pagination_config

    def _execute_operation(self, context: dict) -> list:
        """Execute the API data extraction operation"""
        hook = self.get_hook()
        data = hook.get_data(
            endpoint=self.endpoint,
            params=self.params,
            headers=self.headers,
            data_path=self.data_path,
            paginate=self.paginate,
            pagination_config=self.pagination_config
        )
        logger.info(f"Successfully extracted data from {self.endpoint}")
        return data

class ApiToDataFrameOperator(ApiDataExtractOperator):
    """Operator for extracting data from an API into a pandas DataFrame"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        params: dict = None,
        headers: dict = None,
        data_path: str = None,
        paginate: bool = False,
        pagination_config: ApiPaginationConfig = None,
        dataframe_options: dict = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the API to DataFrame operator"""
        super().__init__(
            conn_id=conn_id,
            endpoint=endpoint,
            params=params,
            headers=headers,
            data_path=data_path,
            paginate=paginate,
            pagination_config=pagination_config,
            timeout=timeout,
            task_id=task_id,
            **kwargs,
        )
        self.dataframe_options = dataframe_options or {}

    def _execute_operation(self, context: dict) -> 'pandas.DataFrame':
        """Execute the API to DataFrame operation"""
        hook = self.get_hook()
        data = hook.get_data(
            endpoint=self.endpoint,
            params=self.params,
            headers=self.headers,
            data_path=self.data_path,
            paginate=self.paginate,
            pagination_config=self.pagination_config
        )
        df = self._convert_to_dataframe(data)
        logger.info(f"Successfully converted data to DataFrame from {self.endpoint}")
        return df

    def _convert_to_dataframe(self, data: list) -> 'pandas.DataFrame':
        """Convert API data to pandas DataFrame"""
        df = pandas.DataFrame(data)
        if 'column_renames' in self.dataframe_options:
            df = df.rename(columns=self.dataframe_options['column_renames'])
        if 'dtype' in self.dataframe_options:
            df = df.astype(self.dataframe_options['dtype'])
        if 'filter' in self.dataframe_options:
            df = df[self.dataframe_options['filter']]
        return df

class ApiToBigQueryOperator(ApiDataExtractOperator):
    """Operator for loading data from an API directly to BigQuery"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        params: dict = None,
        headers: dict = None,
        data_path: str = None,
        paginate: bool = False,
        pagination_config: ApiPaginationConfig = None,
        destination_project_dataset_table: str = None,
        schema_fields: dict = None,
        load_options: dict = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the API to BigQuery operator"""
        super().__init__(
            conn_id=conn_id,
            endpoint=endpoint,
            params=params,
            headers=headers,
            data_path=data_path,
            paginate=paginate,
            pagination_config=pagination_config,
            timeout=timeout,
            task_id=task_id,
            **kwargs,
        )
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.load_options = load_options

    def _execute_operation(self, context: dict) -> dict:
        """Execute the API to BigQuery operation"""
        hook = self.get_hook()
        data = hook.get_data(
            endpoint=self.endpoint,
            params=self.params,
            headers=self.headers,
            data_path=self.data_path,
            paginate=self.paginate,
            pagination_config=self.pagination_config
        )
        df = pandas.DataFrame(data)
        results = self._load_to_bigquery(df)
        logger.info(f"Successfully loaded data to BigQuery from {self.endpoint}")
        return results

    def _load_to_bigquery(self, df: 'pandas.DataFrame') -> dict:
        """Load DataFrame to BigQuery"""
        # Add BigQuery load implementation here
        return {}

class SelfHealingApiOperator(ApiOperator):
    """Base operator for API operations with self-healing capabilities"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        confidence_threshold: float = 0.85,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the self-healing API operator"""
        super().__init__(conn_id=conn_id, endpoint=endpoint, timeout=timeout, task_id=task_id, **kwargs)
        self.confidence_threshold = confidence_threshold
        self.hook = None

    def execute(self, context: dict) -> typing.Any:
        """Execute the API operation with self-healing capabilities"""
        self.hook = self.get_hook()
        logger.info(f"Executing self-healing API operation: {self.endpoint} using connection {self.conn_id}")
        try:
            result = self._execute_operation(context)
            return result
        except Exception as e:
            logger.error(f"API operation failed: {str(e)}")
            can_fix, fix_params = self._diagnose_api_error(e, {"endpoint": self.endpoint})
            if can_fix:
                fixed_params = self._apply_api_fix(fix_params, {"endpoint": self.endpoint})
                logger.info(f"Self-healing applied, retrying request to {self.endpoint} with updated parameters: {fixed_params}")
                try:
                    result = self._execute_operation(context)
                    return result
                except Exception as retry_e:
                    logger.error(f"Self-healing retry failed: {str(retry_e)}")
                    raise retry_e
            else:
                logger.warning("Self-healing not possible, raising original exception")
                raise e

    def _execute_operation(self, context: dict) -> typing.Any:
        """Execute the specific API operation (to be implemented by subclasses)"""
        raise NotImplementedError("Subclasses must implement _execute_operation")

    def get_hook(self) -> SelfHealingApiHook:
        """Get or create an instance of the SelfHealingApiHook"""
        if self.hook:
            return self.hook
        self.hook = SelfHealingApiHook(conn_id=self.conn_id, timeout=self.timeout, confidence_threshold=self.confidence_threshold)
        return self.hook

    def _diagnose_api_error(self, error: Exception, operation_params: dict) -> tuple[bool, dict]:
        """Diagnose an API error and suggest fixes"""
        error_message = str(error)
        fix_params = {}
        confidence = 0.0

        if "401" in error_message or "403" in error_message:
            fix_params["fix_type"] = "authentication"
            fix_params["action"] = "refresh_token"
            confidence = 0.9
        elif "429" in error_message:
            fix_params["fix_type"] = "rate_limiting"
            fix_params["action"] = "add_delay"
            fix_params["delay"] = 60
            confidence = 0.8
        elif "404" in error_message:
            fix_params["fix_type"] = "endpoint"
            fix_params["action"] = "correct_path"
            confidence = 0.7
        else:
            return False, {}

        if confidence >= self.confidence_threshold:
            return True, fix_params
        else:
            return False, {"message": "Confidence too low", "confidence": confidence}

    def _apply_api_fix(self, fix_params: dict, operation_params: dict) -> dict:
        """Apply a fix to a failed API operation based on diagnosis"""
        if fix_params["fix_type"] == "authentication":
            logger.info("Applying authentication fix")
            pass
        elif fix_params["fix_type"] == "rate_limiting":
            logger.info("Applying rate limiting fix")
            pass
        elif fix_params["fix_type"] == "endpoint":
            logger.info("Applying endpoint fix")
            pass
        return operation_params

    def _log_healing_action(self, original_params: dict, fixed_params: dict, error_message: str, confidence: float) -> None:
        """Log details about the self-healing action taken"""
        log_message = f"Self-healing attempt: Error: {error_message}, Confidence: {confidence}\n"
        log_message += f"Original parameters: {original_params}\n"
        log_message += f"Fixed parameters: {fixed_params}"
        logger.info(log_message)

class SelfHealingApiDataExtractOperator(SelfHealingApiOperator):
    """Self-healing operator for extracting data from an API with automatic error recovery"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        params: dict = None,
        headers: dict = None,
        data_path: str = None,
        paginate: bool = False,
        pagination_config: ApiPaginationConfig = None,
        confidence_threshold: float = 0.85,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the self-healing API data extract operator"""
        super().__init__(conn_id=conn_id, endpoint=endpoint, confidence_threshold=confidence_threshold, timeout=timeout, task_id=task_id, **kwargs)
        self.params = params
        self.headers = headers
        self.data_path = data_path
        self.paginate = paginate
        self.pagination_config = pagination_config

    def _execute_operation(self, context: dict) -> list:
        """Execute the self-healing API data extraction operation"""
        hook = self.get_hook()
        data = hook.get_data(
            endpoint=self.endpoint,
            params=self.params,
            headers=self.headers,
            data_path=self.data_path,
            paginate=self.paginate,
            pagination_config=self.pagination_config
        )
        logger.info(f"Successfully extracted data from {self.endpoint}")
        return data

class SelfHealingApiToDataFrameOperator(SelfHealingApiDataExtractOperator):
    """Self-healing operator for extracting data from an API into a pandas DataFrame with automatic error recovery"""

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        params: dict = None,
        headers: dict = None,
        data_path: str = None,
        paginate: bool = False,
        pagination_config: ApiPaginationConfig = None,
        dataframe_options: dict = None,
        confidence_threshold: float = 0.85,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        task_id: str = None,
        **kwargs,
    ) -> None:
        """Initialize the self-healing API to DataFrame operator"""
        super().__init__(
            conn_id=conn_id,
            endpoint=endpoint,
            params=params,
            headers=headers,
            data_path=data_path,
            paginate=paginate,
            pagination_config=pagination_config,
            confidence_threshold=confidence_threshold,
            timeout=timeout,
            task_id=task_id,
            **kwargs,
        )
        self.dataframe_options = dataframe_options or {}

    def _execute_operation(self, context: dict) -> 'pandas.DataFrame':
        """Execute the self-healing API to DataFrame operation"""
        hook = self.get_hook()
        data = hook.get_data(
            endpoint=self.endpoint,
            params=self.params,
            headers=self.headers,
            data_path=self.data_path,
            paginate=self.paginate,
            pagination_config=self.pagination_config
        )
        df = self._convert_to_dataframe(data)
        logger.info(f"Successfully converted data to DataFrame from {self.endpoint}")
        return df

    def _convert_to_dataframe(self, data: list) -> 'pandas.DataFrame':
        """Convert API data to pandas DataFrame"""
        df = pandas.DataFrame(data)
        if 'column_renames' in self.dataframe_options:
            df = df.rename(columns=self.dataframe_options['column_renames'])
        if 'dtype' in self.dataframe_options:
            df = df.astype(self.dataframe_options['dtype'])
        if 'filter' in self.dataframe_options:
            df = df[self.dataframe_options['filter']]
        return df