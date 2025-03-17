"""
Retry manager for data ingestion pipeline.

This module implements retry functionality with exponential backoff, jitter,
and circuit breaker integration to handle transient failures in data pipelines.
It provides decorators and utility functions to implement fault-tolerant
behavior and enable self-healing capabilities.
"""

import functools
import time
import random
from typing import Callable, List, Dict, Optional, Any, Type, Union, TypeVar

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, AlertSeverity
from ...utils.errors.error_types import (
    ErrorCategory,
    ErrorRecoverability,
    PipelineError,
    CircuitBreakerOpenError
)
from ...utils.logging.logger import get_logger
from .error_classifier import is_transient_error, get_retry_strategy, ErrorClassification

# Set up logger
logger = get_logger(__name__)

# Global registry of circuit breakers
circuit_breakers = {}


def retry_with_backoff(
    max_retries: int = None,
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: List[Type[Exception]] = None
) -> Callable:
    """Decorator that adds retry logic with exponential backoff to functions.
    
    Args:
        max_retries: Maximum number of retry attempts (defaults to DEFAULT_MAX_RETRY_ATTEMPTS)
        backoff_factor: Base factor for exponential backoff calculation
        max_delay: Maximum delay between retries in seconds
        retryable_exceptions: List of exception types that should trigger retry
        
    Returns:
        Decorated function with retry capability
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Set default max_retries if not provided
            retries = max_retries if max_retries is not None else DEFAULT_MAX_RETRY_ATTEMPTS
            attempt = 0
            last_exception = None
            
            while attempt <= retries:
                try:
                    # Attempt to execute the function
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    last_exception = e
                    
                    # Check if this exception should trigger a retry
                    if not is_retryable_exception(e, retryable_exceptions):
                        logger.info(f"Non-retryable exception encountered: {e}")
                        raise
                    
                    # If we've reached max retries, log and raise the last exception
                    if attempt > retries:
                        logger.warning(
                            f"Maximum retry attempts ({retries}) reached. Last error: {e}"
                        )
                        raise
                    
                    # Calculate backoff delay with jitter
                    delay = calculate_backoff_delay(attempt, backoff_factor, max_delay)
                    logger.info(
                        f"Retryable exception caught: {e}. "
                        f"Retrying in {delay:.2f} seconds (attempt {attempt}/{retries})"
                    )
                    
                    # Wait before retry
                    time.sleep(delay)
            
            # This should not be reached, but just in case
            if last_exception:
                raise last_exception
            
            # This should also not be reached
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def calculate_backoff_delay(
    attempt: int,
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    jitter_factor: float = 0.1
) -> float:
    """Calculates the backoff delay time with jitter for a retry attempt.
    
    Args:
        attempt: Current retry attempt number (1-based)
        backoff_factor: Base factor for exponential backoff calculation
        max_delay: Maximum delay in seconds
        jitter_factor: Factor to determine jitter range (0-1)
        
    Returns:
        Calculated backoff time in seconds
    """
    # Calculate base exponential backoff: factor * 2^attempt
    backoff = backoff_factor * (2 ** (attempt - 1))
    
    # Add random jitter to avoid thundering herd problem
    jitter_range = backoff * jitter_factor
    jitter = random.uniform(-jitter_range, jitter_range)
    
    # Apply jitter and ensure we don't exceed max_delay
    final_delay = min(backoff + jitter, max_delay)
    
    # Ensure we never have a negative delay
    return max(0.1, final_delay)


def is_retryable_exception(
    exception: Exception,
    retryable_exceptions: List[Type[Exception]] = None
) -> bool:
    """Determines if an exception should trigger a retry attempt.
    
    Args:
        exception: The exception to check
        retryable_exceptions: List of exception types that should trigger retry
        
    Returns:
        True if the exception is retryable, False otherwise
    """
    # Use empty list as default to avoid mutable default argument issues
    if retryable_exceptions is None:
        retryable_exceptions = []
    
    # If the exception is a PipelineError, use its built-in retryability check
    if isinstance(exception, PipelineError):
        return exception.is_retryable()
    
    # If the exception type is in the list of retryable exceptions, it's retryable
    if any(isinstance(exception, exc_type) for exc_type in retryable_exceptions):
        return True
    
    # If it's a CircuitBreakerOpenError, it's explicitly not retryable
    if isinstance(exception, CircuitBreakerOpenError):
        return False
    
    # Use the error classifier to determine if it's a transient error
    return is_transient_error(exception, str(exception))


class CircuitBreaker:
    """Implementation of the circuit breaker pattern for fault tolerance.
    
    The circuit breaker prevents repeated calls to failing services by
    "opening the circuit" after a threshold of failures is reached.
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0
    ):
        """Initialize the circuit breaker.
        
        Args:
            name: Identifier for this circuit breaker
            failure_threshold: Number of failures before circuit opens
            reset_timeout: Time in seconds before trying to close circuit again
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.is_open = False
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open = False
    
    def record_success(self) -> None:
        """Record a successful operation and reset failure count.
        
        This should be called after a successful operation to indicate 
        that the service is functioning properly.
        """
        # Reset failure count on success
        previous_state = self.is_open
        self.failure_count = 0
        self.is_open = False
        self.half_open = False
        
        # Log circuit status change
        if previous_state:
            logger.info(f"Circuit breaker '{self.name}' closed after successful operation")
    
    def record_failure(self) -> None:
        """Record a failed operation and potentially open the circuit.
        
        This should be called when an operation fails to track failures
        and potentially open the circuit.
        """
        # Update failure stats
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # If in half-open state, immediately open the circuit
        if self.half_open:
            self.is_open = True
            logger.warning(
                f"Circuit breaker '{self.name}' reopened due to failure during half-open state"
            )
            return
        
        # Check if we should open the circuit
        if not self.is_open and self.failure_count >= self.failure_threshold:
            self.is_open = True
            logger.warning(
                f"Circuit breaker '{self.name}' opened after {self.failure_count} "
                f"consecutive failures"
            )
    
    def allow_request(self) -> bool:
        """Check if a request should be allowed through the circuit.
        
        Returns:
            True if request is allowed, False otherwise
        """
        # If circuit is closed, allow the request
        if not self.is_open:
            return True
        
        # If circuit is open, check if reset timeout has elapsed
        current_time = time.time()
        if self.last_failure_time and (current_time - self.last_failure_time) > self.reset_timeout:
            # Allow one test request to see if service has recovered
            self.half_open = True
            logger.info(
                f"Circuit breaker '{self.name}' half-open, allowing test request after "
                f"{current_time - self.last_failure_time:.2f} seconds"
            )
            return True
        
        # Circuit is open and timeout hasn't elapsed - block request
        logger.info(
            f"Circuit breaker '{self.name}' is open, blocking request. "
            f"Retry after {self.reset_timeout - (current_time - self.last_failure_time):.2f} "
            f"seconds"
        )
        return False
    
    def reset(self) -> None:
        """Reset the circuit breaker to closed state.
        
        This can be used to manually reset the circuit breaker.
        """
        previous_state = self.is_open
        self.failure_count = 0
        self.is_open = False
        self.half_open = False
        
        if previous_state:
            logger.info(f"Circuit breaker '{self.name}' manually reset to closed state")
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the circuit breaker.
        
        Returns:
            Dictionary with circuit breaker status
        """
        current_time = time.time()
        time_since_last_failure = (
            current_time - self.last_failure_time if self.last_failure_time else None
        )
        time_until_reset = (
            self.reset_timeout - time_since_last_failure
            if self.is_open and time_since_last_failure is not None
            else None
        )
        
        return {
            'name': self.name,
            'state': 'open' if self.is_open else 'half-open' if self.half_open else 'closed',
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'last_failure_time': self.last_failure_time,
            'time_since_last_failure': time_since_last_failure,
            'reset_timeout': self.reset_timeout,
            'time_until_reset': time_until_reset
        }


class CircuitBreakerException(Exception):
    """Exception raised when a circuit breaker is open."""
    
    def __init__(self, circuit_name: str, reset_timeout: float):
        """Initialize the circuit breaker exception.
        
        Args:
            circuit_name: Name of the circuit breaker that is open
            reset_timeout: Time until circuit may reset and allow requests
        """
        self.circuit_name = circuit_name
        self.reset_timeout = reset_timeout
        message = (
            f"Circuit breaker '{circuit_name}' is open and preventing operations. "
            f"Try again after {reset_timeout:.2f} seconds."
        )
        super().__init__(message)


def get_circuit_breaker(
    service_name: str,
    failure_threshold: int = 5,
    reset_timeout: float = 60.0
) -> CircuitBreaker:
    """Factory function to get or create a circuit breaker instance for a service.
    
    Args:
        service_name: Unique identifier for the service
        failure_threshold: Number of failures before circuit opens
        reset_timeout: Time in seconds before trying to close circuit again
        
    Returns:
        A circuit breaker instance for the specified service
    """
    # Check if a circuit breaker already exists for this service
    if service_name in circuit_breakers:
        return circuit_breakers[service_name]
    
    # Create a new circuit breaker for this service
    circuit = CircuitBreaker(
        name=service_name,
        failure_threshold=failure_threshold,
        reset_timeout=reset_timeout
    )
    
    # Store in registry and return
    circuit_breakers[service_name] = circuit
    logger.debug(f"Created new circuit breaker for service '{service_name}'")
    return circuit