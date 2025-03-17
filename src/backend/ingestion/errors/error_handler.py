"""
Implements comprehensive error handling functionality for the data ingestion pipeline, providing decorators and utilities for error handling, retry mechanisms, and circuit breaker patterns to enable self-healing capabilities.
"""

import functools
import traceback
import typing
import time
from typing import Dict, List, Optional, Any, Tuple, Callable, Union, Type, TypeVar

# Internal imports
from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, AlertSeverity
from ...utils.errors.error_types import (
    ErrorCategory,
    ErrorRecoverability,
    PipelineError,
    CircuitBreakerOpenError
)
from ...utils.logging.logger import get_logger
from .error_classifier import (
    classify_error, 
    is_transient_error, 
    get_retry_strategy, 
    ErrorClassification
)
from .retry_manager import (
    retry_with_backoff, 
    calculate_backoff_delay, 
    CircuitBreaker,
    CircuitBreakerException
)

# Set up logger
logger = get_logger(__name__)

# Global variables
DEFAULT_ERROR_HANDLERS = {}
CIRCUIT_BREAKERS = {}


def handle_error(exception: Exception, context: dict, raise_exception: bool) -> tuple:
    """Main error handling function that processes exceptions based on their classification.
    
    Args:
        exception: The exception to handle
        context: Additional context for error handling
        raise_exception: Whether to re-raise the exception after handling
        
    Returns:
        (ErrorClassification, dict) - Error classification and handling result
    """
    # Log the exception with context information
    logger.error(f"Error occurred: {str(exception)}", exc_info=exception)
    logger.debug(f"Error context: {context}")
    
    # Classify the exception using error_classifier.classify_error
    error_classification = classify_error(exception, context)
    
    # Determine appropriate handling strategy based on classification
    is_self_healable = error_classification.is_self_healable()
    is_retryable = error_classification.is_retryable()
    
    # Create a result dictionary to track handling actions
    result = {
        'handled': False,
        'actions_taken': [],
        'self_healed': False,
        'retry_suggested': is_retryable
    }
    
    # Apply self-healing if error is self-healable
    if is_self_healable:
        healing_actions = error_classification.suggest_healing_actions(exception)
        if healing_actions:
            logger.info(f"Self-healing actions available: {len(healing_actions)}")
            result['healing_actions'] = [action.to_dict() for action in healing_actions]
            
            # Mark as handled if we found healing actions
            result['handled'] = True
            result['actions_taken'].append('self_healing_suggested')
    
    # Apply retry strategy if error is retryable
    if is_retryable:
        retry_strategy = error_classification.get_retry_strategy()
        result['retry_strategy'] = retry_strategy
        result['actions_taken'].append('retry_suggested')
    
    # Log handling result and actions taken
    logger.info(f"Error handling result: {result}")
    
    # Re-raise exception if instructed to do so
    if raise_exception:
        raise exception
    
    # Return error classification and handling result
    return error_classification, result


def with_error_handling(context: dict = None, raise_exception: bool = True):
    """Decorator that adds error handling to functions.
    
    Args:
        context: Additional context for error handling
        raise_exception: Whether to re-raise exceptions after handling
        
    Returns:
        Decorated function with error handling
    """
    context = context or {}
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Include function name and arguments in error context
            func_context = {
                'function': func.__name__,
                'module': func.__module__,
                'args_count': len(args),
                'kwargs_keys': list(kwargs.keys())
            }
            
            # Merge with provided context
            error_context = {**context, **func_context}
            
            # Log function entry
            start_time = time.time()
            logger.debug(f"Entering function {func.__name__}")
            
            try:
                # Execute the wrapped function
                result = func(*args, **kwargs)
                
                # Log function exit with timing information
                execution_time = time.time() - start_time
                logger.debug(f"Exiting function {func.__name__}, execution time: {execution_time:.3f}s")
                
                # Return original function result if successful
                return result
                
            except Exception as e:
                # Log function exception with timing information
                execution_time = time.time() - start_time
                logger.debug(f"Exception in function {func.__name__}, execution time: {execution_time:.3f}s")
                
                # Catch exceptions and pass to handle_error function
                handle_error(e, error_context, raise_exception)
                
                # This will only be reached if raise_exception is False
                return None
                
        return wrapper
    
    return decorator


def retry_with_backoff(max_retries=None, backoff_factor=1.0, max_delay=60.0, retryable_exceptions=None):
    """Decorator that adds retry logic with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Base factor for exponential backoff calculation
        max_delay: Maximum delay between retries in seconds
        retryable_exceptions: List of exception types that should trigger retry
        
    Returns:
        Decorated function with retry capability
    """
    # Import and delegate to retry_manager.retry_with_backoff
    # Apply default values from constants if not provided
    if max_retries is None:
        max_retries = DEFAULT_MAX_RETRY_ATTEMPTS
        
    return retry_manager.retry_with_backoff(
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
        retryable_exceptions=retryable_exceptions
    )


def circuit_breaker(name: str, failure_threshold: int = 5, reset_timeout: float = 60.0):
    """Decorator that implements circuit breaker pattern.
    
    Args:
        name: Name of the circuit breaker
        failure_threshold: Number of failures before circuit opens
        reset_timeout: Time in seconds before circuit tries to close
        
    Returns:
        Decorated function with circuit breaker protection
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get or create circuit breaker for the specified name
            if name not in CIRCUIT_BREAKERS:
                CIRCUIT_BREAKERS[name] = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    reset_timeout=reset_timeout
                )
            
            circuit = CIRCUIT_BREAKERS[name]
            
            # Check if circuit allows request before executing function
            if not circuit.allow_request():
                # If circuit is open, raise CircuitBreakerOpenError
                raise CircuitBreakerOpenError(
                    circuit.name, 
                    circuit.reset_timeout,
                    circuit.get_status()
                )
            
            try:
                # Execute function and record success on completion
                result = func(*args, **kwargs)
                circuit.record_success()
                return result
                
            except Exception as e:
                # Record failure if exception occurs
                circuit.record_failure()
                # Re-raise exception after recording failure
                raise
                
        return wrapper
    
    return decorator


def format_exception(exception: Exception) -> dict:
    """Formats exception details into a structured dictionary.
    
    Args:
        exception: The exception to format
        
    Returns:
        Structured exception details
    """
    # Extract exception type name
    exception_type = type(exception).__name__
    
    # Extract error message
    error_message = str(exception)
    
    # Format traceback information
    tb_str = ''.join(traceback.format_exception(
        type(exception), 
        exception, 
        exception.__traceback__
    ))
    
    # Extract additional attributes from exception
    attributes = {}
    for attr in dir(exception):
        # Skip magic methods and callables
        if not attr.startswith('__') and not callable(getattr(exception, attr)):
            try:
                value = getattr(exception, attr)
                # Only include serializable attributes
                if isinstance(value, (str, int, float, bool, list, dict, tuple, type(None))):
                    attributes[attr] = value
            except Exception:
                pass
    
    # Return dictionary with all extracted details
    return {
        'type': exception_type,
        'message': error_message,
        'traceback': tb_str,
        'attributes': attributes
    }


def register_error_handler(exception_type: type, handler_function: Callable):
    """Registers a custom error handler for specific exception types.
    
    Args:
        exception_type: Type of exception to handle
        handler_function: Function to handle the exception
        
    Returns:
        None
    """
    # Validate handler_function is callable
    if not callable(handler_function):
        raise ValueError("Handler function must be callable")
    
    # Register handler in DEFAULT_ERROR_HANDLERS dictionary
    DEFAULT_ERROR_HANDLERS[exception_type] = handler_function
    
    # Log registration of custom handler
    logger.info(f"Registered custom error handler for {exception_type.__name__}")


def get_error_handler(exception: Exception) -> Callable:
    """Retrieves the appropriate error handler for an exception.
    
    Args:
        exception: The exception to handle
        
    Returns:
        Handler function for the exception
    """
    exception_type = type(exception)
    
    # Check for exact exception type match in handlers
    if exception_type in DEFAULT_ERROR_HANDLERS:
        return DEFAULT_ERROR_HANDLERS[exception_type]
    
    # If not found, check for parent exception types
    for handler_type, handler in DEFAULT_ERROR_HANDLERS.items():
        if issubclass(exception_type, handler_type):
            return handler
    
    # Return default handler if no specific handler found
    return handle_error


class CircuitBreakerOpenException(Exception):
    """Exception raised when a circuit breaker is open."""
    
    def __init__(self, circuit_name: str, reset_timeout: float):
        """Initialize the circuit breaker exception.
        
        Args:
            circuit_name: Name of the circuit breaker
            reset_timeout: Time until circuit reset
        """
        # Set circuit name
        self.circuit_name = circuit_name
        
        # Set reset timeout
        self.reset_timeout = reset_timeout
        
        # Format error message with circuit details
        message = (
            f"Circuit breaker '{circuit_name}' is open and preventing operations. "
            f"Try again after {reset_timeout:.2f} seconds."
        )
        
        # Call parent Exception constructor with message
        super().__init__(message)


class CircuitBreaker:
    """Implementation of the circuit breaker pattern for failure protection."""
    
    def __init__(self, name: str, failure_threshold: int = 5, reset_timeout: float = 60.0):
        """Initialize the circuit breaker.
        
        Args:
            name: Name of the circuit breaker
            failure_threshold: Number of failures before circuit opens
            reset_timeout: Time before attempting reset
        """
        # Set circuit breaker name
        self.name = name
        
        # Set failure threshold
        self.failure_threshold = failure_threshold
        
        # Set reset timeout
        self.reset_timeout = reset_timeout
        
        # Initialize circuit as closed (is_open = False)
        self.is_open = False
        
        # Initialize failure count to 0
        self.failure_count = 0
        
        # Initialize last failure time to None
        self.last_failure_time = None
        
        # Initialize half_open flag to False
        self.half_open = False
    
    def record_success(self):
        """Record a successful operation and reset failure count."""
        # Reset failure count to 0
        self.failure_count = 0
        
        # Close the circuit if it was open or half-open
        previous_state = self.is_open
        self.is_open = False
        
        # Set half_open to False
        self.half_open = False
        
        # Log circuit status change if applicable
        if previous_state:
            logger.info(f"Circuit breaker '{self.name}' closed after successful operation")
    
    def record_failure(self):
        """Record a failed operation and potentially open the circuit."""
        # Increment failure count
        self.failure_count += 1
        
        # Update last failure time
        self.last_failure_time = time.time()
        
        # If in half-open state, immediately open circuit
        if self.half_open:
            self.is_open = True
            logger.warning(
                f"Circuit breaker '{self.name}' reopened due to failure in half-open state"
            )
            return
        
        # Otherwise check if failure threshold is reached
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
        # If circuit is closed, allow request
        if not self.is_open:
            return True
        
        # If circuit is open, check if reset timeout has elapsed
        current_time = time.time()
        elapsed_time = (
            current_time - self.last_failure_time if self.last_failure_time else float('inf')
        )
        
        if elapsed_time > self.reset_timeout:
            # If timeout elapsed, set half_open flag and allow request
            self.half_open = True
            logger.info(
                f"Circuit breaker '{self.name}' half-open, allowing test request"
            )
            return True
        
        # If timeout not elapsed, block request
        logger.info(
            f"Circuit breaker '{self.name}' is open, blocking request. "
            f"Reset in {self.reset_timeout - elapsed_time:.2f} seconds"
        )
        return False
    
    def reset(self):
        """Reset the circuit breaker to closed state."""
        # Reset failure count to 0
        self.failure_count = 0
        
        # Close the circuit (is_open = False)
        previous_state = self.is_open
        self.is_open = False
        
        # Set half_open to False
        self.half_open = False
        
        # Log circuit reset
        if previous_state:
            logger.info(f"Circuit breaker '{self.name}' manually reset")
    
    def get_status(self) -> dict:
        """Get the current status of the circuit breaker.
        
        Returns:
            Dictionary with circuit breaker status
        """
        # Create dictionary with all circuit breaker properties
        current_time = time.time()
        
        # Include time since last failure
        time_since_failure = (
            current_time - self.last_failure_time if self.last_failure_time else None
        )
        
        # Include time until reset if circuit is open
        time_until_reset = (
            self.reset_timeout - time_since_failure 
            if self.is_open and time_since_failure is not None 
            else None
        )
        
        # Return the status dictionary
        return {
            'name': self.name,
            'is_open': self.is_open,
            'half_open': self.half_open,
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'last_failure_time': self.last_failure_time,
            'time_since_failure': time_since_failure,
            'reset_timeout': self.reset_timeout,
            'time_until_reset': time_until_reset
        }


class ErrorHandler:
    """Central manager for error handling, retry, and circuit breaker functionality."""
    
    def __init__(self):
        """Initialize the error handler."""
        # Initialize circuit_breakers dictionary
        self.circuit_breakers = {}
        
        # Initialize error_handlers dictionary with defaults
        self.error_handlers = {}
        
        # Set up default error handlers for common exceptions
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default error handlers for common exceptions."""
        # Example built-in handlers - these would be expanded in a real implementation
        self.register_error_handler(ConnectionError, self._handle_connection_error)
        self.register_error_handler(TimeoutError, self._handle_timeout_error)
    
    def _handle_connection_error(self, exception, context, raise_exception=True):
        """Default handler for connection errors."""
        logger.info(f"Handling connection error: {exception}")
        return classify_error(exception, context), {
            'handled': True,
            'actions_taken': ['connection_retry_suggested'],
            'retry_suggested': True
        }
    
    def _handle_timeout_error(self, exception, context, raise_exception=True):
        """Default handler for timeout errors."""
        logger.info(f"Handling timeout error: {exception}")
        return classify_error(exception, context), {
            'handled': True,
            'actions_taken': ['timeout_retry_suggested'],
            'retry_suggested': True
        }
    
    def register_circuit_breaker(self, name: str, failure_threshold: int = 5, reset_timeout: float = 60.0) -> CircuitBreaker:
        """Register a circuit breaker for a service.
        
        Args:
            name: Name of the service
            failure_threshold: Number of failures before circuit opens
            reset_timeout: Time before reset attempt
            
        Returns:
            The registered circuit breaker
        """
        # Create new circuit breaker with parameters
        circuit = CircuitBreaker(
            name=name,
            failure_threshold=failure_threshold,
            reset_timeout=reset_timeout
        )
        
        # Store in circuit_breakers dictionary
        self.circuit_breakers[name] = circuit
        
        # Log registration of circuit breaker
        logger.info(f"Registered circuit breaker for service '{name}'")
        
        # Return the circuit breaker
        return circuit
    
    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Get an existing circuit breaker by name.
        
        Args:
            name: Name of the circuit breaker
            
        Returns:
            The circuit breaker instance or None if not found
        """
        # Check if circuit breaker exists in dictionary
        return self.circuit_breakers.get(name)
    
    def register_error_handler(self, exception_type: type, handler_function: Callable):
        """Register a custom error handler for specific exception types.
        
        Args:
            exception_type: Type of exception to handle
            handler_function: Function to handle the exception
            
        Returns:
            None
        """
        # Validate handler_function is callable
        if not callable(handler_function):
            raise ValueError("Handler function must be callable")
        
        # Register handler in error_handlers dictionary
        self.error_handlers[exception_type] = handler_function
        
        # Log registration of custom handler
        logger.info(f"Registered custom error handler for {exception_type.__name__}")
    
    def handle_error(self, exception: Exception, context: dict, raise_exception: bool = True) -> tuple:
        """Handle an exception using appropriate strategy.
        
        Args:
            exception: The exception to handle
            context: Additional context for error handling
            raise_exception: Whether to re-raise the exception
            
        Returns:
            (ErrorClassification, dict) - Error classification and handling result
        """
        # Get appropriate error handler for the exception
        handler = self._get_handler_for_exception(exception)
        
        # Call the handler with exception and context
        classification, result = handler(exception, context, raise_exception)
        
        # Apply self-healing if applicable
        if classification.is_self_healable():
            healing_actions = classification.suggest_healing_actions(exception)
            result['healing_actions'] = [action.to_dict() for action in healing_actions]
        
        # Log handling result
        logger.info(f"Error handling result: {result}")
        
        # Re-raise exception if raise_exception is True
        if raise_exception:
            raise exception
        
        # Return error classification and handling result
        return classification, result
    
    def _get_handler_for_exception(self, exception: Exception) -> Callable:
        """Get the appropriate handler for an exception type."""
        exception_type = type(exception)
        
        # Check exact match
        if exception_type in self.error_handlers:
            return self.error_handlers[exception_type]
        
        # Check parent classes
        for handler_type, handler in self.error_handlers.items():
            if issubclass(exception_type, handler_type):
                return handler
        
        # Default to generic error handling
        return handle_error
    
    def get_retry_strategy(self, exception: Exception, context: dict) -> dict:
        """Get appropriate retry strategy for an exception.
        
        Args:
            exception: The exception to handle
            context: Additional context
            
        Returns:
            Retry strategy parameters
        """
        # Classify the exception
        classification = classify_error(exception, context)
        
        # Call error_classifier.get_retry_strategy with classification
        return get_retry_strategy(classification)
    
    def with_error_handling(self, context: dict = None, raise_exception: bool = True):
        """Decorator that adds error handling to functions.
        
        Args:
            context: Additional context for error handling
            raise_exception: Whether to re-raise exceptions
            
        Returns:
            Decorated function with error handling
        """
        context = context or {}
        
        # Create decorator function
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Include function info in context
                func_context = {
                    'function': func.__name__,
                    'module': func.__module__
                }
                full_context = {**context, **func_context}
                
                try:
                    # Execute function
                    return func(*args, **kwargs)
                except Exception as e:
                    # Handle exceptions using self.handle_error
                    self.handle_error(e, full_context, raise_exception)
                    # This will only be reached if raise_exception is False
                    return None
            
            return wrapper
        
        # Return decorated function
        return decorator
    
    def with_circuit_breaker(self, name: str, failure_threshold: int = 5, reset_timeout: float = 60.0):
        """Decorator that adds circuit breaker protection to functions.
        
        Args:
            name: Name of the service
            failure_threshold: Number of failures before circuit opens
            reset_timeout: Time before reset attempt
            
        Returns:
            Decorated function with circuit breaker
        """
        # Get or create circuit breaker
        circuit = self.get_circuit_breaker(name)
        if not circuit:
            circuit = self.register_circuit_breaker(
                name=name,
                failure_threshold=failure_threshold,
                reset_timeout=reset_timeout
            )
        
        # Create decorator function
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Check circuit state before executing function
                if not circuit.allow_request():
                    raise CircuitBreakerOpenError(
                        circuit.name, 
                        circuit.reset_timeout, 
                        circuit.get_status()
                    )
                
                try:
                    # Execute function
                    result = func(*args, **kwargs)
                    # Record success on successful execution
                    circuit.record_success()
                    return result
                except Exception as e:
                    # Record failure on exception
                    circuit.record_failure()
                    # Re-raise the exception
                    raise
            
            return wrapper
        
        # Return decorated function
        return decorator
    
    def with_retry(self, max_retries: int = None, backoff_factor: float = 1.0, max_delay: float = 60.0, retryable_exceptions: List[Type[Exception]] = None):
        """Decorator that adds retry capability to functions.
        
        Args:
            max_retries: Maximum number of retry attempts
            backoff_factor: Base factor for exponential backoff
            max_delay: Maximum delay between retries
            retryable_exceptions: List of exceptions that trigger retry
            
        Returns:
            Decorated function with retry capability
        """
        # Create decorator function
        def decorator(func):
            # Apply retry_with_backoff with parameters
            return retry_with_backoff(
                max_retries=max_retries,
                backoff_factor=backoff_factor,
                max_delay=max_delay,
                retryable_exceptions=retryable_exceptions
            )(func)
        
        # Return decorated function
        return decorator