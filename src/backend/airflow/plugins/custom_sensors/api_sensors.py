"""
Custom Airflow sensors for monitoring external API endpoints. These sensors check for API availability, data presence, and response conditions to trigger downstream tasks in data pipelines, with built-in self-healing capabilities.
"""

import typing
import json

import requests.exceptions  # version 2.31.x
from airflow.sensors.base import BaseSensorOperator  # version 2.5.x
from airflow.utils.decorators import apply_defaults  # version 2.5.x
from airflow.exceptions import AirflowException  # version 2.5.x

from src.backend.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR, DEFAULT_CONFIDENCE_THRESHOLD  # Import constants for API configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for API sensors
from src.backend.utils.retry.retry_decorator import retry  # Apply retry logic to API operations
from src.backend.airflow.plugins.hooks.api_hooks import ApiHook, SelfHealingApiHook  # Use API hooks for connecting to external APIs
from src.backend.ingestion.connectors.api_connector import ApiAuthType  # Reuse API connector enumerations

# Initialize logger
logger = get_logger(__name__)


class ApiSensor(BaseSensorOperator):
    """
    Base sensor class for monitoring external API endpoints
    """

    template_fields = ["conn_id", "endpoint"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        task_id: str,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the API sensor with connection and endpoint details

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.conn_id = conn_id
        self.endpoint = endpoint
        self.hook = None

    def poke(self, context: dict) -> bool:
        """
        Abstract method to be implemented by subclasses to check API condition

        :param context: Airflow context dictionary
        :return: True if condition is met, False otherwise
        """
        raise NotImplementedError("Subclasses must implement poke method")

    def get_hook(self) -> ApiHook:
        """
        Get or create an API hook instance

        :return: API hook instance
        """
        if self.hook is None:
            self.hook = ApiHook(conn_id=self.conn_id)
        return self.hook


class ApiAvailabilitySensor(ApiSensor):
    """
    Sensor that checks for the availability of an API endpoint
    """

    template_fields = ["endpoint", "request_params", "headers"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        request_params: dict = None,
        headers: dict = None,
        expected_status: int = 200,
        task_id: str = None,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the API availability sensor

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param request_params: Request parameters for the API call
        :param headers: Headers for the API call
        :param expected_status: Expected HTTP status code for availability
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(conn_id=conn_id, endpoint=endpoint, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.expected_status = expected_status or 200

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: dict) -> bool:
        """
        Check if the API endpoint is available

        :param context: Airflow context dictionary
        :return: True if API is available, False otherwise
        """
        hook = self.get_hook()
        try:
            response = hook.get_request(endpoint=self.endpoint, params=self.request_params, headers=self.headers)
            if response.response.status_code == self.expected_status:
                logger.info(f"API {self.endpoint} is available (Status: {response.response.status_code})")
                return True
            else:
                logger.info(f"API {self.endpoint} not available (Status: {response.response.status_code})")
                return False
        except Exception as e:
            logger.warning(f"Error while checking API availability: {e}")
            return False


class ApiResponseSensor(ApiSensor):
    """
    Sensor that checks for specific conditions in an API response
    """

    template_fields = ["endpoint", "request_params", "headers", "response_check", "data_path"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        request_params: dict = None,
        headers: dict = None,
        response_check: str = None,
        data_path: str = None,
        task_id: str = None,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the API response sensor

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param request_params: Request parameters for the API call
        :param headers: Headers for the API call
        :param response_check: Callable or string expression to evaluate
        :param data_path: Path to extract data from response (default None)
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(conn_id=conn_id, endpoint=endpoint, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.response_check = response_check
        self.data_path = data_path

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: dict) -> bool:
        """
        Check if the API response meets the specified condition

        :param context: Airflow context dictionary
        :return: True if condition is met, False otherwise
        """
        hook = self.get_hook()
        try:
            response = hook.get_request(endpoint=self.endpoint, params=self.request_params, headers=self.headers)
            response_data = response.json()
            if self.data_path:
                response_data = response.extract(self.data_path)
            return self.evaluate_response(response_data)
        except Exception as e:
            logger.warning(f"Error while checking API response: {e}")
            return False

    def evaluate_response(self, response_data: object) -> bool:
        """
        Evaluate the response check against the response data

        :param response_data: Data extracted from the API response
        :return: True if condition is met, False otherwise
        """
        if callable(self.response_check):
            return self.response_check(response_data)
        elif isinstance(self.response_check, str):
            try:
                return eval(self.response_check, {"response_data": response_data})
            except Exception as e:
                logger.warning(f"Error evaluating response check: {e}")
                return False
        else:
            logger.warning("No response check defined")
            return True


class ApiDataAvailabilitySensor(ApiSensor):
    """
    Sensor that checks for data availability in an API response
    """

    template_fields = ["endpoint", "request_params", "headers", "data_path"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        request_params: dict = None,
        headers: dict = None,
        data_path: str = None,
        min_items: int = 1,
        task_id: str = None,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the API data availability sensor

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param request_params: Request parameters for the API call
        :param headers: Headers for the API call
        :param data_path: Path to extract data from response
        :param min_items: Minimum number of items required (default 1)
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(conn_id=conn_id, endpoint=endpoint, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.data_path = data_path
        self.min_items = min_items or 1

    @retry(max_attempts=MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def poke(self, context: dict) -> bool:
        """
        Check if data is available in the API response

        :param context: Airflow context dictionary
        :return: True if data is available, False otherwise
        """
        hook = self.get_hook()
        try:
            response = hook.get_request(endpoint=self.endpoint, params=self.request_params, headers=self.headers)
            data = response.json()
            if self.data_path:
                data = response.extract(self.data_path)
            if isinstance(data, list) and len(data) >= self.min_items:
                logger.info(f"Data available in API {self.endpoint} (Items: {len(data)})")
                return True
            else:
                logger.info(f"Data not available in API {self.endpoint} (Items: {len(data) if isinstance(data, list) else 0})")
                return False
        except Exception as e:
            logger.warning(f"Error while checking API data availability: {e}")
            return False


class SelfHealingApiAvailabilitySensor(ApiAvailabilitySensor):
    """
    API availability sensor with self-healing capabilities
    """

    template_fields = ["endpoint", "request_params", "headers"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        request_params: dict = None,
        headers: dict = None,
        expected_status: int = 200,
        confidence_threshold: float = None,
        task_id: str = None,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the self-healing API availability sensor

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param request_params: Request parameters for the API call
        :param headers: Headers for the API call
        :param expected_status: Expected HTTP status code for availability
        :param confidence_threshold: Confidence threshold for self-healing actions
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(conn_id=conn_id, endpoint=endpoint, request_params=request_params, headers=headers, expected_status=expected_status, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.self_healing_hook = SelfHealingApiHook(conn_id=conn_id)
        self.confidence_threshold = confidence_threshold or DEFAULT_CONFIDENCE_THRESHOLD

    def poke(self, context: dict) -> bool:
        """
        Check if the API endpoint is available with self-healing capability

        :param context: Airflow context dictionary
        :return: True if API is available or was healed, False otherwise
        """
        try:
            # Try to check API availability using parent class poke method
            return super().poke(context)
        except Exception as e:
            logger.warning(f"API {self.endpoint} not available, attempting self-healing: {e}")
            # Attempt self-healing
            if self._attempt_self_healing(e):
                logger.info(f"Self-healing succeeded, API {self.endpoint} is now available")
                return True
            else:
                logger.warning(f"Self-healing failed, API {self.endpoint} remains unavailable")
                return False

    def _attempt_self_healing(self, error: Exception) -> bool:
        """
        Attempt to heal API availability issues

        :param error: The exception that occurred
        :return: True if healing succeeded, False otherwise
        """
        # Analyze error message and type
        error_message = str(error)
        if "Connection refused" in error_message:
            # Try alternative endpoints or parameters
            logger.info("Attempting to switch to a backup endpoint")
            # Add logic to switch to a backup endpoint
            return False
        elif "Timeout" in error_message:
            # Adjust request parameters
            logger.info("Attempting to increase timeout")
            # Add logic to increase timeout
            return False
        else:
            logger.warning("No self-healing strategy available for this error")
            return False


class SelfHealingApiDataAvailabilitySensor(ApiDataAvailabilitySensor):
    """
    API data availability sensor with self-healing capabilities
    """

    template_fields = ["endpoint", "request_params", "headers", "data_path"]

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        endpoint: str,
        request_params: dict = None,
        headers: dict = None,
        data_path: str = None,
        min_items: int = 1,
        confidence_threshold: float = None,
        task_id: str = None,
        poke_interval: float = 60.0,
        timeout: float = 60 * 2,
        soft_fail: bool = False,
        mode: str = "poke",
        **kwargs,
    ):
        """
        Initialize the self-healing API data availability sensor

        :param conn_id: Airflow connection ID for the API
        :param endpoint: API endpoint to monitor
        :param request_params: Request parameters for the API call
        :param headers: Headers for the API call
        :param data_path: Path to extract data from response
        :param min_items: Minimum number of items required (default 1)
        :param confidence_threshold: Confidence threshold for self-healing actions
        :param task_id: Task ID for the sensor
        :param poke_interval: Time in seconds between pokes
        :param timeout: Time in seconds before the sensor times out
        :param soft_fail: Whether to fail softly (skip) or hard (fail)
        :param mode: Poke mode ('poke' or 'reschedule')
        """
        super().__init__(conn_id=conn_id, endpoint=endpoint, request_params=request_params, headers=headers, data_path=data_path, min_items=min_items, task_id=task_id, poke_interval=poke_interval, timeout=timeout, soft_fail=soft_fail, mode=mode, **kwargs)
        self.self_healing_hook = SelfHealingApiHook(conn_id=conn_id)
        self.confidence_threshold = confidence_threshold or DEFAULT_CONFIDENCE_THRESHOLD

    def poke(self, context: dict) -> bool:
        """
        Check if data is available in the API response with self-healing capability

        :param context: Airflow context dictionary
        :return: True if data is available or was healed, False otherwise
        """
        try:
            # Try to check data availability using parent class poke method
            return super().poke(context)
        except Exception as e:
            logger.warning(f"Data not available in API {self.endpoint}, attempting self-healing: {e}")
            # Attempt self-healing
            if self._attempt_self_healing(e):
                logger.info(f"Self-healing succeeded, data is now available in API {self.endpoint}")
                return True
            else:
                logger.warning(f"Self-healing failed, data remains unavailable in API {self.endpoint}")
                return False

    def _attempt_self_healing(self, error: Exception) -> bool:
        """
        Attempt to heal data availability issues

        :param error: The exception that occurred
        :return: True if healing succeeded, False otherwise
        """
        # Analyze error message and type
        error_message = str(error)
        if "KeyError" in error_message:
            # Try alternative data paths
            logger.info("Attempting to switch to a backup data path")
            # Add logic to switch to a backup data path
            return False
        elif "list index out of range" in error_message:
            # Adjust request parameters
            logger.info("Attempting to adjust request parameters")
            # Add logic to adjust request parameters
            return False
        else:
            logger.warning("No self-healing strategy available for this error")
            return False