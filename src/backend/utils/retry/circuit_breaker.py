"""
Implements the Circuit Breaker pattern for the self-healing data pipeline to prevent cascading failures
when services are unavailable. Provides a mechanism to detect failures, trip the circuit when failure
thresholds are exceeded, and automatically recover after a reset timeout period.
"""
import enum
import time
import threading
import typing
from collections import deque
from functools import wraps

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import CircuitBreakerOpenError

# Initialize logger
logger = get_logger(__name__)

# Default configuration values
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_RESET_TIMEOUT = 60.0  # seconds
DEFAULT_HALF_OPEN_TIMEOUT = 30.0  # seconds
DEFAULT_WINDOW_SIZE = 10  # number of requests to track

# Dictionary to store circuit breaker instances by service name
_circuit_breakers = {}


@enum.Enum
class CircuitState:
    """Enumeration of possible circuit breaker states."""
    CLOSED = "CLOSED"       # Circuit is closed, requests flow normally
    OPEN = "OPEN"           # Circuit is open, requests are blocked
    HALF_OPEN = "HALF_OPEN" # Circuit is trying to recover, limited requests allowed


class CircuitBreakerConfig:
    """Configuration class for circuit breaker parameters."""
    
    def __init__(
        self, 
        failure_threshold: int = None,
        reset_timeout: float = None,
        half_open_timeout: float = None,
        window_size: int = None
    ):
        """Initialize circuit breaker configuration with default or provided values.
        
        Args:
            failure_threshold: Number of failures before the circuit opens
            reset_timeout: Time in seconds before an open circuit transitions to half-open
            half_open_timeout: Time in seconds between test requests in half-open state
            window_size: Number of recent requests to track for failure counting
        """
        self.failure_threshold = failure_threshold or DEFAULT_FAILURE_THRESHOLD
        self.reset_timeout = reset_timeout or DEFAULT_RESET_TIMEOUT
        self.half_open_timeout = half_open_timeout or DEFAULT_HALF_OPEN_TIMEOUT
        self.window_size = window_size or DEFAULT_WINDOW_SIZE
    
    @classmethod
    def from_config(cls, config_section: str = "circuit_breaker"):
        """Create CircuitBreakerConfig from application configuration.
        
        Args:
            config_section: Configuration section name for circuit breaker settings
            
        Returns:
            CircuitBreakerConfig instance with values from application config
        """
        config = get_config()
        section_prefix = f"{config_section}."
        
        return cls(
            failure_threshold=config.get(f"{section_prefix}failure_threshold", DEFAULT_FAILURE_THRESHOLD),
            reset_timeout=float(config.get(f"{section_prefix}reset_timeout", DEFAULT_RESET_TIMEOUT)),
            half_open_timeout=float(config.get(f"{section_prefix}half_open_timeout", DEFAULT_HALF_OPEN_TIMEOUT)),
            window_size=int(config.get(f"{section_prefix}window_size", DEFAULT_WINDOW_SIZE))
        )


class CircuitBreaker:
    """Implementation of the Circuit Breaker pattern for fault tolerance."""
    
    def __init__(
        self, 
        service_name: str,
        failure_threshold: int = None,
        reset_timeout: float = None,
        half_open_timeout: float = None,
        window_size: int = None
    ):
        """Initialize a circuit breaker for a specific service.
        
        Args:
            service_name: Name of the service this circuit breaker is protecting
            failure_threshold: Number of failures before the circuit opens
            reset_timeout: Time in seconds before an open circuit transitions to half-open
            half_open_timeout: Time in seconds between test requests in half-open state
            window_size: Number of recent requests to track for failure counting
        """
        self.service_name = service_name
        self.config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            reset_timeout=reset_timeout,
            half_open_timeout=half_open_timeout,
            window_size=window_size
        )
        
        # Initialize state
        self.state = CircuitState.CLOSED
        self.last_failure_time = None
        self.open_since = None
        self.failure_history = deque(maxlen=self.config.window_size)
        
        # For thread safety
        self._lock = threading.RLock()
        
        logger.info(f"Circuit breaker initialized for service '{service_name}'")
    
    def is_open(self) -> bool:
        """Checks if the circuit is currently open.
        
        Returns:
            True if circuit is open, False otherwise
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                # Check if reset timeout has elapsed
                current_time = time.time()
                if self.open_since and (current_time - self.open_since) >= self.config.reset_timeout:
                    # Transition to half-open state
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit breaker for '{self.service_name}' transitioned from OPEN to HALF_OPEN")
                    return False
                return True
            return False
    
    def is_half_open(self) -> bool:
        """Checks if the circuit is in half-open state.
        
        Returns:
            True if circuit is half-open, False otherwise
        """
        with self._lock:
            return self.state == CircuitState.HALF_OPEN
    
    def allow_request(self) -> bool:
        """Determines if a request should be allowed through the circuit.
        
        Returns:
            True if request is allowed, False if circuit is open
        """
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            if self.state == CircuitState.OPEN:
                current_time = time.time()
                if self.open_since and (current_time - self.open_since) >= self.config.reset_timeout:
                    # Transition to half-open state
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit breaker for '{self.service_name}' transitioned from OPEN to HALF_OPEN")
                    return True
                return False
            
            if self.state == CircuitState.HALF_OPEN:
                # In half-open state, allow requests periodically to test the service
                current_time = time.time()
                if (self.last_failure_time is None or 
                    (current_time - self.last_failure_time) >= self.config.half_open_timeout):
                    return True
                return False
            
            # Should never reach here
            return False
    
    def on_success(self) -> None:
        """Records a successful operation, potentially closing the circuit."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                # If successful while half-open, close the circuit
                self.state = CircuitState.CLOSED
                self.failure_history.clear()
                self.open_since = None
                logger.info(f"Circuit breaker for '{self.service_name}' closed after successful test request")
    
    def on_failure(self, exception: Exception) -> None:
        """Records a failed operation, potentially opening the circuit.
        
        Args:
            exception: The exception that occurred
        """
        with self._lock:
            current_time = time.time()
            self.last_failure_time = current_time
            self.failure_history.append(current_time)
            
            if self.state == CircuitState.CLOSED:
                # If failure count exceeds threshold, open the circuit
                if len(self.failure_history) >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    self.open_since = current_time
                    logger.warning(
                        f"Circuit breaker for '{self.service_name}' OPENED after "
                        f"{len(self.failure_history)} failures. Last error: {str(exception)}"
                    )
            
            elif self.state == CircuitState.HALF_OPEN:
                # If fails while half-open, re-open the circuit
                self.state = CircuitState.OPEN
                self.open_since = current_time
                logger.warning(
                    f"Circuit breaker for '{self.service_name}' REOPENED after test request failed. "
                    f"Error: {str(exception)}"
                )
    
    def reset(self) -> None:
        """Manually resets the circuit to closed state."""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_history.clear()
            self.open_since = None
            logger.info(f"Circuit breaker for '{self.service_name}' manually reset to CLOSED")
    
    def get_failure_count(self) -> int:
        """Gets the current count of failures in the window.
        
        Returns:
            Number of failures in the current window
        """
        with self._lock:
            return len(self.failure_history)
    
    def get_state(self) -> CircuitState:
        """Gets the current state of the circuit.
        
        Returns:
            Current circuit state
        """
        with self._lock:
            # Check if we need to transition from OPEN to HALF_OPEN
            if self.state == CircuitState.OPEN:
                current_time = time.time()
                if self.open_since and (current_time - self.open_since) >= self.config.reset_timeout:
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit breaker for '{self.service_name}' transitioned from OPEN to HALF_OPEN")
            
            return self.state
    
    def get_status(self) -> dict:
        """Gets detailed status information about the circuit.
        
        Returns:
            Dictionary with circuit status details
        """
        with self._lock:
            current_state = self.get_state()
            status = {
                "service_name": self.service_name,
                "state": current_state.value,
                "failure_count": len(self.failure_history),
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "reset_timeout": self.config.reset_timeout,
                    "half_open_timeout": self.config.half_open_timeout,
                    "window_size": self.config.window_size
                }
            }
            
            if self.open_since:
                status["open_since"] = self.open_since
                if current_state == CircuitState.OPEN:
                    current_time = time.time()
                    status["remaining_timeout"] = max(0, self.config.reset_timeout - (current_time - self.open_since))
            
            if self.last_failure_time:
                status["last_failure_time"] = self.last_failure_time
            
            return status
    
    def execute(self, func: typing.Callable, *args, **kwargs) -> typing.Any:
        """Executes a function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function positional arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Result of the function execution
            
        Raises:
            CircuitBreakerOpenError: If the circuit is open
            Any exception that the function might raise
        """
        if not self.allow_request():
            raise CircuitBreakerOpenError(
                service_name=self.service_name,
                open_since=self.open_since,
                reset_timeout=self.config.reset_timeout,
                failure_history={"count": len(self.failure_history), "last_failure": self.last_failure_time}
            )
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure(e)
            raise


def get_circuit_breaker(
    service_name: str,
    failure_threshold: int = None,
    reset_timeout: float = None,
    half_open_timeout: float = None,
    window_size: int = None
) -> CircuitBreaker:
    """Factory function to get or create a circuit breaker instance for a service.
    
    Args:
        service_name: Name of the service
        failure_threshold: Number of failures before the circuit opens
        reset_timeout: Time in seconds before an open circuit transitions to half-open
        half_open_timeout: Time in seconds between test requests in half-open state
        window_size: Number of recent requests to track for failure counting
        
    Returns:
        Circuit breaker instance for the specified service
    """
    global _circuit_breakers
    
    if service_name in _circuit_breakers:
        return _circuit_breakers[service_name]
    
    circuit_breaker = CircuitBreaker(
        service_name=service_name,
        failure_threshold=failure_threshold,
        reset_timeout=reset_timeout,
        half_open_timeout=half_open_timeout,
        window_size=window_size
    )
    
    _circuit_breakers[service_name] = circuit_breaker
    return circuit_breaker


def circuit_breaker(
    service_name: str,
    failure_threshold: int = None,
    reset_timeout: float = None,
    half_open_timeout: float = None,
    window_size: int = None
) -> typing.Callable:
    """Decorator that wraps a function with circuit breaker functionality.
    
    Args:
        service_name: Name of the service
        failure_threshold: Number of failures before the circuit opens
        reset_timeout: Time in seconds before an open circuit transitions to half-open
        half_open_timeout: Time in seconds between test requests in half-open state
        window_size: Number of recent requests to track for failure counting
        
    Returns:
        Decorator function that applies circuit breaker pattern
    """
    cb = get_circuit_breaker(
        service_name=service_name,
        failure_threshold=failure_threshold,
        reset_timeout=reset_timeout,
        half_open_timeout=half_open_timeout,
        window_size=window_size
    )
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not cb.allow_request():
                raise CircuitBreakerOpenError(
                    service_name=service_name,
                    open_since=cb.open_since,
                    reset_timeout=cb.config.reset_timeout,
                    failure_history={"count": cb.get_failure_count(), "last_failure": cb.last_failure_time}
                )
            
            try:
                result = func(*args, **kwargs)
                cb.on_success()
                return result
            except Exception as e:
                cb.on_failure(e)
                raise
                
        return wrapper
    
    return decorator


def reset_all_circuit_breakers() -> None:
    """Resets all circuit breakers to closed state."""
    global _circuit_breakers
    
    for service_name, cb in _circuit_breakers.items():
        cb.reset()
        logger.info(f"Reset circuit breaker for service '{service_name}'")


def get_circuit_breaker_status() -> dict:
    """Gets the status of all circuit breakers.
    
    Returns:
        Dictionary of circuit breaker statuses by service name
    """
    global _circuit_breakers
    
    result = {}
    for service_name, cb in _circuit_breakers.items():
        result[service_name] = cb.get_status()
    
    return result