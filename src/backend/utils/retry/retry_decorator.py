"""
Implements a flexible retry decorator for the self-healing data pipeline that automatically
retries failed operations with configurable backoff strategies, exception filtering, and logging.
Supports integration with circuit breaker pattern for fault tolerance.
"""

import functools
import time
import typing
import inspect

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import ErrorCategory, PipelineError
from ...utils.retry.backoff_strategy import (
    get_backoff_strategy,
    get_backoff_strategy_for_error,
    BackoffStrategy
)
from ...utils.retry.circuit_breaker import (
    CircuitBreaker,
    get_circuit_breaker,
    CircuitBreakerOpenError
)

# Initialize module logger
logger = get_logger(__name__)


class RetryConfig:
    """Configuration class for retry decorator settings."""
    
    def __init__(
        self,
        max_attempts: int = None,
        backoff_strategy: str = "exponential",
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter_factor: float = 0.1,
        exceptions_to_retry: list = None,
        exceptions_to_ignore: list = None,
        use_circuit_breaker: bool = False,
        circuit_breaker_service: str = None
    ):
        """Initialize retry configuration with default or provided values.
        
        Args:
            max_attempts: Maximum number of retry attempts
            backoff_strategy: Strategy for timing between retries
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            jitter_factor: Random jitter factor (0-1)
            exceptions_to_retry: List of exception types to retry
            exceptions_to_ignore: List of exception types to never retry
            use_circuit_breaker: Whether to use circuit breaker pattern
            circuit_breaker_service: Service name for circuit breaker
        """
        self.max_attempts = max_attempts or DEFAULT_MAX_RETRY_ATTEMPTS
        self.backoff_strategy = backoff_strategy
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter_factor = jitter_factor
        self.exceptions_to_retry = exceptions_to_retry or [Exception]
        self.exceptions_to_ignore = exceptions_to_ignore or []
        self.use_circuit_breaker = use_circuit_breaker
        self.circuit_breaker_service = circuit_breaker_service
    
    @classmethod
    def from_config(cls, config_section: str) -> 'RetryConfig':
        """Create RetryConfig from application configuration.
        
        Args:
            config_section: Configuration section name for retry settings
            
        Returns:
            RetryConfig instance with values from application config
        """
        config = get_config()
        
        try:
            # Get retry configuration from specified section
            section_prefix = f"{config_section}."
            
            return cls(
                max_attempts=config.get(f"{section_prefix}max_attempts", DEFAULT_MAX_RETRY_ATTEMPTS),
                backoff_strategy=config.get(f"{section_prefix}backoff_strategy", "exponential"),
                base_delay=float(config.get(f"{section_prefix}base_delay", 1.0)),
                max_delay=float(config.get(f"{section_prefix}max_delay", 60.0)),
                jitter_factor=float(config.get(f"{section_prefix}jitter_factor", 0.1)),
                exceptions_to_retry=config.get(f"{section_prefix}exceptions_to_retry", [Exception]),
                exceptions_to_ignore=config.get(f"{section_prefix}exceptions_to_ignore", []),
                use_circuit_breaker=config.get(f"{section_prefix}use_circuit_breaker", False),
                circuit_breaker_service=config.get(f"{section_prefix}circuit_breaker_service")
            )
        except Exception as e:
            logger.warning(f"Error loading retry configuration from {config_section}: {str(e)}. Using default configuration.")
            return cls()
    
    def get_retry_decorator(self) -> typing.Callable:
        """Create a retry decorator based on this configuration.
        
        Returns:
            Configured retry decorator
        """
        return retry(
            max_attempts=self.max_attempts,
            backoff_strategy=self.backoff_strategy,
            exceptions_to_retry=self.exceptions_to_retry,
            exceptions_to_ignore=self.exceptions_to_ignore,
            use_circuit_breaker=self.use_circuit_breaker,
            circuit_breaker_service=self.circuit_breaker_service
        )


def retry(
    max_attempts: int = None,
    backoff_strategy: typing.Union[str, BackoffStrategy] = "exponential",
    exceptions_to_retry: typing.List[typing.Type[Exception]] = None,
    exceptions_to_ignore: typing.List[typing.Type[Exception]] = None,
    retry_condition: typing.Callable = None,
    on_retry: typing.Callable = None,
    on_permanent_failure: typing.Callable = None,
    use_circuit_breaker: bool = False,
    circuit_breaker_service: str = None
) -> typing.Callable:
    """Decorator that retries a function on specified exceptions with configurable backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        backoff_strategy: Strategy for timing between retries (string name or BackoffStrategy instance)
        exceptions_to_retry: List of exception types to retry
        exceptions_to_ignore: List of exception types to never retry
        retry_condition: Custom function to determine if an exception should be retried
        on_retry: Callback function executed before each retry
        on_permanent_failure: Callback function executed when all retries are exhausted
        use_circuit_breaker: Whether to use circuit breaker pattern
        circuit_breaker_service: Service name for circuit breaker
        
    Returns:
        Decorator function that will retry the decorated function on failure
    """
    # Set default values
    max_retry_attempts = max_attempts or DEFAULT_MAX_RETRY_ATTEMPTS
    exceptions_to_retry = exceptions_to_retry or [Exception]
    exceptions_to_ignore = exceptions_to_ignore or []
    
    # Get a BackoffStrategy instance if a string name was provided
    if isinstance(backoff_strategy, str):
        backoff_strategy = get_backoff_strategy(backoff_strategy)
    
    # Create the decorator function
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Initialize retry counter and circuit breaker
            attempt = 1
            circuit_breaker = None
            
            if use_circuit_breaker and circuit_breaker_service:
                circuit_breaker = get_circuit_breaker(circuit_breaker_service)
            
            while True:
                try:
                    # Check if circuit breaker is open
                    if circuit_breaker and not circuit_breaker.allow_request():
                        raise CircuitBreakerOpenError(
                            service_name=circuit_breaker_service,
                            open_since=circuit_breaker.open_since,
                            reset_timeout=circuit_breaker.config.reset_timeout,
                            failure_history={"count": circuit_breaker.get_failure_count()}
                        )
                    
                    # Try to execute the function
                    result = func(*args, **kwargs)
                    
                    # Record success with circuit breaker if being used
                    if circuit_breaker:
                        circuit_breaker.on_success()
                    
                    return result
                
                except Exception as exc:
                    # Record failure with circuit breaker if being used
                    if circuit_breaker:
                        circuit_breaker.on_failure(exc)
                    
                    # Check if this exception should be retried
                    if not should_retry_exception(
                        exc, exceptions_to_retry, exceptions_to_ignore, retry_condition
                    ):
                        # Re-raise exceptions that should not be retried
                        logger.debug(
                            f"Exception {exc.__class__.__name__} not configured for retry in {func.__name__}. "
                            f"Re-raising."
                        )
                        raise
                    
                    # Check if we've reached max attempts
                    if attempt >= max_retry_attempts:
                        if on_permanent_failure:
                            try:
                                on_permanent_failure(func, exc, attempt, max_retry_attempts)
                            except Exception as callback_exc:
                                logger.warning(
                                    f"Error in on_permanent_failure callback: {str(callback_exc)}"
                                )
                        
                        # We've exhausted all retries, re-raise the exception
                        logger.warning(
                            f"Max retry attempts ({max_retry_attempts}) reached for {func.__name__}. "
                            f"Last error: {str(exc)}"
                        )
                        raise
                    
                    # Determine appropriate backoff strategy based on exception
                    error_category = get_error_category(exc)
                    retry_backoff = backoff_strategy
                    
                    if error_category:
                        # If error category is available, get a specific strategy for it
                        strategy_for_error = get_backoff_strategy_for_error(error_category)
                        if strategy_for_error:
                            retry_backoff = strategy_for_error
                    
                    # Calculate delay before next retry
                    delay = retry_backoff.get_delay(attempt)
                    
                    # Log the retry attempt
                    log_retry_attempt(func, exc, attempt, max_retry_attempts, delay)
                    
                    # Execute on_retry callback if provided
                    if on_retry:
                        try:
                            on_retry(func, exc, attempt, max_retry_attempts, delay)
                        except Exception as callback_exc:
                            logger.warning(
                                f"Error in on_retry callback: {str(callback_exc)}"
                            )
                    
                    # Wait before retrying
                    time.sleep(delay)
                    
                    # Increment attempt counter
                    attempt += 1
        
        return wrapper
    
    return decorator


def retry_with_config(config_section: str) -> typing.Callable:
    """Creates a retry decorator with settings from configuration.
    
    Args:
        config_section: Configuration section name for retry settings
        
    Returns:
        Configured retry decorator
    """
    # Get retry configuration from specified section
    config = RetryConfig.from_config(config_section)
    return config.get_retry_decorator()


def should_retry_exception(
    exception: Exception,
    exceptions_to_retry: typing.List[typing.Type[Exception]],
    exceptions_to_ignore: typing.List[typing.Type[Exception]],
    retry_condition: typing.Callable = None
) -> bool:
    """Determines if an exception should be retried based on type and conditions.
    
    Args:
        exception: The exception to check
        exceptions_to_retry: List of exception types to retry
        exceptions_to_ignore: List of exception types to never retry
        retry_condition: Custom function to determine if an exception should be retried
        
    Returns:
        True if exception should be retried, False otherwise
    """
    # Check if exception is in the ignore list
    for exc_type in exceptions_to_ignore:
        if isinstance(exception, exc_type):
            return False
    
    # If it's a PipelineError, check if it's retryable
    if isinstance(exception, PipelineError):
        retryable = exception.is_retryable()
        if not retryable:
            return False
    
    # Check if exception is instance of any type in the retry list
    should_retry = False
    for exc_type in exceptions_to_retry:
        if isinstance(exception, exc_type):
            should_retry = True
            break
    
    # If a custom retry condition is provided, use it to make final decision
    if retry_condition and callable(retry_condition):
        should_retry = should_retry and retry_condition(exception)
    
    return should_retry


def get_error_category(exception: Exception) -> typing.Optional[ErrorCategory]:
    """Extracts error category from exception if it's a PipelineError.
    
    Args:
        exception: The exception to check
        
    Returns:
        ErrorCategory if exception is a PipelineError, None otherwise
    """
    if isinstance(exception, PipelineError):
        return exception.category
    return None


def log_retry_attempt(
    func: typing.Callable,
    exception: Exception,
    attempt: int,
    max_attempts: int,
    delay: float
) -> None:
    """Logs information about a retry attempt.
    
    Args:
        func: The function being retried
        exception: The exception that caused the retry
        attempt: Current attempt number
        max_attempts: Maximum number of attempts
        delay: Delay before next retry
    """
    # Get function name and module for logging
    func_name = getattr(func, "__name__", "unknown_function")
    module_name = getattr(func, "__module__", "unknown_module")
    
    # Format exception details for logging
    error_category = get_error_category(exception)
    error_category_str = f" [{error_category.name}]" if error_category else ""
    exception_str = f"{exception.__class__.__name__}{error_category_str}: {str(exception)}"
    
    # Log retry attempt with function name, exception, attempt count, and delay
    logger.info(
        f"Retrying {module_name}.{func_name} after exception: {exception_str}. "
        f"Attempt {attempt}/{max_attempts} with delay {delay:.2f}s"
    )
    
    # If attempt is the last attempt, log that max attempts will be reached
    if attempt == max_attempts - 1:
        logger.warning(
            f"Next retry will be the last attempt for {module_name}.{func_name}"
        )