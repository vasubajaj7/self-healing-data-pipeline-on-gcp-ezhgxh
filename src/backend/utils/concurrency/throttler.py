"""
Implements throttling functionality for the self-healing data pipeline.

This module provides mechanisms to control execution speed and prevent overwhelming 
downstream systems. It offers multiple throttling strategies including fixed delay, 
adaptive, and dynamic backpressure implementations.
"""

import time
import threading
import enum
import typing
from collections import deque
import statistics
import random
import functools

from ...constants import DEFAULT_TIMEOUT_SECONDS
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import RateLimitError, TimeoutError

# Initialize module logger
logger = get_logger(__name__)

# Global registry of throttlers
_throttlers = {}


class ThrottlingStrategy(enum.Enum):
    """Enumeration of throttling strategies."""
    FIXED_DELAY = "fixed_delay"
    ADAPTIVE = "adaptive"
    BACKPRESSURE = "backpressure"
    JITTERED = "jittered"


class ThrottlingError(Exception):
    """Exception raised when throttling fails and blocking is disabled."""
    
    def __init__(self, resource_name, delay_seconds, message=None):
        """Initialize the throttling error.
        
        Args:
            resource_name: The resource that was being throttled
            delay_seconds: The throttling delay in seconds
            message: Custom error message or None for default
        """
        if message is None:
            message = f"Throttling required for resource '{resource_name}' with delay of {delay_seconds}s, but blocking is disabled"
        super().__init__(message)
        self.resource_name = resource_name
        self.delay_seconds = delay_seconds


class Throttler:
    """Abstract base class defining the interface for throttlers."""
    
    def __init__(self, resource_name, delay_seconds):
        """Initialize the throttler with configuration parameters.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Base delay in seconds
        """
        self.resource_name = resource_name
        self.delay_seconds = delay_seconds
        self._lock = threading.RLock()
        
        # Validate delay_seconds is non-negative
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
    
    def throttle(self, blocking=True, timeout=None):
        """Apply throttling delay based on the strategy.
        
        Args:
            blocking: If True, block until throttling is possible; 
                    if False, return immediately
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if throttling was applied, False if throttling would block
            and blocking is False
            
        Raises:
            TimeoutError: If blocking times out
        """
        raise NotImplementedError("Subclasses must implement throttle()")
    
    def reset(self):
        """Reset the throttler to its initial state."""
        raise NotImplementedError("Subclasses must implement reset()")
    
    def get_statistics(self):
        """Get statistics about the throttler's current state.
        
        Returns:
            dict: Dictionary containing throttler statistics
        """
        raise NotImplementedError("Subclasses must implement get_statistics()")


class FixedDelayThrottler(Throttler):
    """Implements fixed delay throttling strategy."""
    
    def __init__(self, resource_name, delay_seconds):
        """Initialize the fixed delay throttler.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Delay in seconds between operations
        """
        super().__init__(resource_name, delay_seconds)
        self.last_execution_time = 0
        self._condition = threading.Condition(self._lock)
    
    def throttle(self, blocking=True, timeout=None):
        """Apply fixed delay throttling.
        
        Args:
            blocking: If True, block until throttling is possible; 
                    if False, return immediately
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if throttling was applied, False if throttling would block
            and blocking is False
            
        Raises:
            TimeoutError: If blocking times out
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.delay_seconds - time_since_last)
            
            if remaining_delay <= 0:
                # No delay needed
                self.last_execution_time = current_time
                return True
            
            if not blocking:
                return False
            
            # If timeout is None, use a default timeout
            if timeout is None:
                timeout = DEFAULT_TIMEOUT_SECONDS
                
            # Wait for the delay time
            wait_success = self._condition.wait(timeout=min(remaining_delay, timeout))
            
            # Check if the wait timed out
            if not wait_success and time.time() - current_time >= timeout:
                raise TimeoutError(
                    message=f"Throttling timed out after {timeout}s for resource '{self.resource_name}'",
                    operation="throttle",
                    timeout_seconds=timeout
                )
            
            # Update last execution time
            self.last_execution_time = time.time()
            return True
    
    def reset(self):
        """Reset the fixed delay throttler."""
        with self._condition:
            self.last_execution_time = 0
            # Notify all waiting threads
            self._condition.notify_all()
    
    def get_statistics(self):
        """Get statistics about the fixed delay throttler.
        
        Returns:
            dict: Dictionary containing throttler statistics
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.delay_seconds - time_since_last)
            
            return {
                "throttler_type": "fixed_delay",
                "resource_name": self.resource_name,
                "delay_seconds": self.delay_seconds,
                "time_since_last_execution": time_since_last,
                "remaining_delay": remaining_delay
            }


class AdaptiveThrottler(Throttler):
    """Implements adaptive throttling based on system load or response times."""
    
    def __init__(self, resource_name, delay_seconds, max_history_size=10):
        """Initialize the adaptive throttler.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Base delay in seconds
            max_history_size: Maximum size of response time history
        """
        super().__init__(resource_name, delay_seconds)
        self.last_execution_time = 0
        self.base_delay_seconds = delay_seconds
        self.current_delay_seconds = delay_seconds
        self.response_times = deque(maxlen=max_history_size)
        self.max_history_size = max_history_size
        self._condition = threading.Condition(self._lock)
    
    def throttle(self, blocking=True, timeout=None):
        """Apply adaptive throttling.
        
        Args:
            blocking: If True, block until throttling is possible; 
                    if False, return immediately
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if throttling was applied, False if throttling would block
            and blocking is False
            
        Raises:
            TimeoutError: If blocking times out
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.current_delay_seconds - time_since_last)
            
            if remaining_delay <= 0:
                # No delay needed
                self.last_execution_time = current_time
                return True
            
            if not blocking:
                return False
            
            # If timeout is None, use a default timeout
            if timeout is None:
                timeout = DEFAULT_TIMEOUT_SECONDS
                
            # Wait for the delay time
            wait_success = self._condition.wait(timeout=min(remaining_delay, timeout))
            
            # Check if the wait timed out
            if not wait_success and time.time() - current_time >= timeout:
                raise TimeoutError(
                    message=f"Adaptive throttling timed out after {timeout}s for resource '{self.resource_name}'",
                    operation="throttle",
                    timeout_seconds=timeout
                )
            
            # Update last execution time
            self.last_execution_time = time.time()
            return True
    
    def update_delay(self, response_time):
        """Update the adaptive delay based on response time.
        
        Args:
            response_time: Measured response time in seconds
            
        Returns:
            float: New delay time in seconds
        """
        with self._condition:
            # Add response time to history
            self.response_times.append(response_time)
            
            # Calculate average response time if we have enough data
            if len(self.response_times) > 0:
                avg_response_time = statistics.mean(self.response_times)
                
                # Adjust delay based on average response time
                # This is a simple linear adjustment, but more complex algorithms could be used
                adjustment_factor = avg_response_time / self.base_delay_seconds
                self.current_delay_seconds = self.base_delay_seconds * adjustment_factor
                
                # Ensure delay doesn't go below half the base delay
                self.current_delay_seconds = max(self.current_delay_seconds, self.base_delay_seconds / 2)
                
                # Ensure delay doesn't go above twice the base delay
                self.current_delay_seconds = min(self.current_delay_seconds, self.base_delay_seconds * 2)
                
                # If delay decreased, notify waiting threads
                if adjustment_factor < 1:
                    self._condition.notify_all()
            
            return self.current_delay_seconds
    
    def reset(self):
        """Reset the adaptive throttler."""
        with self._condition:
            self.last_execution_time = 0
            self.current_delay_seconds = self.base_delay_seconds
            self.response_times.clear()
            self._condition.notify_all()
    
    def get_statistics(self):
        """Get statistics about the adaptive throttler.
        
        Returns:
            dict: Dictionary containing throttler statistics
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.current_delay_seconds - time_since_last)
            
            # Calculate average response time if we have data
            avg_response_time = None
            if len(self.response_times) > 0:
                avg_response_time = statistics.mean(self.response_times)
            
            return {
                "throttler_type": "adaptive",
                "resource_name": self.resource_name,
                "base_delay_seconds": self.base_delay_seconds,
                "current_delay_seconds": self.current_delay_seconds,
                "time_since_last_execution": time_since_last,
                "remaining_delay": remaining_delay,
                "avg_response_time": avg_response_time,
                "response_time_samples": len(self.response_times)
            }


class BackpressureThrottler(Throttler):
    """Implements throttling based on backpressure from downstream systems."""
    
    def __init__(self, resource_name, delay_seconds, max_delay_seconds=None, backpressure_factor=1.5):
        """Initialize the backpressure throttler.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Minimum delay in seconds
            max_delay_seconds: Maximum delay in seconds (default: min_delay * 10)
            backpressure_factor: Factor to increase/decrease delay (default: 1.5)
        """
        super().__init__(resource_name, delay_seconds)
        self.last_execution_time = 0
        self.min_delay_seconds = delay_seconds
        self.max_delay_seconds = max_delay_seconds or (delay_seconds * 10)
        self.current_delay_seconds = delay_seconds
        self.backpressure_factor = backpressure_factor
        self._condition = threading.Condition(self._lock)
    
    def throttle(self, blocking=True, timeout=None):
        """Apply backpressure-based throttling.
        
        Args:
            blocking: If True, block until throttling is possible; 
                    if False, return immediately
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if throttling was applied, False if throttling would block
            and blocking is False
            
        Raises:
            TimeoutError: If blocking times out
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.current_delay_seconds - time_since_last)
            
            if remaining_delay <= 0:
                # No delay needed
                self.last_execution_time = current_time
                return True
            
            if not blocking:
                return False
            
            # If timeout is None, use a default timeout
            if timeout is None:
                timeout = DEFAULT_TIMEOUT_SECONDS
                
            # Wait for the delay time
            wait_success = self._condition.wait(timeout=min(remaining_delay, timeout))
            
            # Check if the wait timed out
            if not wait_success and time.time() - current_time >= timeout:
                raise TimeoutError(
                    message=f"Backpressure throttling timed out after {timeout}s for resource '{self.resource_name}'",
                    operation="throttle",
                    timeout_seconds=timeout
                )
            
            # Update last execution time
            self.last_execution_time = time.time()
            return True
    
    def increase_backpressure(self):
        """Increase throttling delay due to backpressure.
        
        Returns:
            float: New delay time in seconds
        """
        with self._condition:
            # Increase delay by backpressure factor
            self.current_delay_seconds *= self.backpressure_factor
            
            # Ensure delay doesn't exceed maximum
            self.current_delay_seconds = min(self.current_delay_seconds, self.max_delay_seconds)
            
            logger.debug(
                f"Increased backpressure for '{self.resource_name}' to {self.current_delay_seconds:.2f}s "
                f"({(self.current_delay_seconds / self.min_delay_seconds):.1f}x base delay)"
            )
            
            return self.current_delay_seconds
    
    def decrease_backpressure(self):
        """Decrease throttling delay when backpressure reduces.
        
        Returns:
            float: New delay time in seconds
        """
        with self._condition:
            # Decrease delay by dividing by backpressure factor
            self.current_delay_seconds /= self.backpressure_factor
            
            # Ensure delay doesn't go below minimum
            self.current_delay_seconds = max(self.current_delay_seconds, self.min_delay_seconds)
            
            # Notify waiting threads since delay has decreased
            self._condition.notify_all()
            
            logger.debug(
                f"Decreased backpressure for '{self.resource_name}' to {self.current_delay_seconds:.2f}s "
                f"({(self.current_delay_seconds / self.min_delay_seconds):.1f}x base delay)"
            )
            
            return self.current_delay_seconds
    
    def reset(self):
        """Reset the backpressure throttler."""
        with self._condition:
            self.last_execution_time = 0
            self.current_delay_seconds = self.min_delay_seconds
            self._condition.notify_all()
    
    def get_statistics(self):
        """Get statistics about the backpressure throttler.
        
        Returns:
            dict: Dictionary containing throttler statistics
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            remaining_delay = max(0, self.current_delay_seconds - time_since_last)
            
            # Calculate backpressure percentage
            backpressure_pct = (self.current_delay_seconds / self.min_delay_seconds - 1) * 100
            
            return {
                "throttler_type": "backpressure",
                "resource_name": self.resource_name,
                "min_delay_seconds": self.min_delay_seconds,
                "max_delay_seconds": self.max_delay_seconds,
                "current_delay_seconds": self.current_delay_seconds,
                "time_since_last_execution": time_since_last,
                "remaining_delay": remaining_delay,
                "backpressure_percentage": backpressure_pct,
                "backpressure_factor": self.backpressure_factor
            }


class JitteredThrottler(Throttler):
    """Implements throttling with jitter to prevent thundering herd problems."""
    
    def __init__(self, resource_name, delay_seconds, jitter_factor=0.5):
        """Initialize the jittered throttler.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Base delay in seconds
            jitter_factor: Factor to control jitter amount (0.0-1.0)
        """
        super().__init__(resource_name, delay_seconds)
        self.last_execution_time = 0
        self.base_delay_seconds = delay_seconds
        self.jitter_factor = max(0.0, min(1.0, jitter_factor))  # Clamp between 0 and 1
        self._condition = threading.Condition(self._lock)
    
    def throttle(self, blocking=True, timeout=None):
        """Apply throttling with jitter.
        
        Args:
            blocking: If True, block until throttling is possible; 
                    if False, return immediately
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if throttling was applied, False if throttling would block
            and blocking is False
            
        Raises:
            TimeoutError: If blocking times out
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            
            # Calculate jittered delay
            jittered_delay = self._calculate_jittered_delay()
            remaining_delay = max(0, jittered_delay - time_since_last)
            
            if remaining_delay <= 0:
                # No delay needed
                self.last_execution_time = current_time
                return True
            
            if not blocking:
                return False
            
            # If timeout is None, use a default timeout
            if timeout is None:
                timeout = DEFAULT_TIMEOUT_SECONDS
                
            # Wait for the delay time
            wait_success = self._condition.wait(timeout=min(remaining_delay, timeout))
            
            # Check if the wait timed out
            if not wait_success and time.time() - current_time >= timeout:
                raise TimeoutError(
                    message=f"Jittered throttling timed out after {timeout}s for resource '{self.resource_name}'",
                    operation="throttle",
                    timeout_seconds=timeout
                )
            
            # Update last execution time
            self.last_execution_time = time.time()
            return True
    
    def _calculate_jittered_delay(self):
        """Calculate a delay with jitter applied.
        
        Returns:
            float: Delay time with jitter applied
        """
        # Generate jitter factor between (1-jitter_factor) and (1+jitter_factor)
        jitter = 1.0 + random.uniform(-self.jitter_factor, self.jitter_factor)
        
        # Apply jitter to base delay
        return self.base_delay_seconds * jitter
    
    def reset(self):
        """Reset the jittered throttler."""
        with self._condition:
            self.last_execution_time = 0
            self._condition.notify_all()
    
    def get_statistics(self):
        """Get statistics about the jittered throttler.
        
        Returns:
            dict: Dictionary containing throttler statistics
        """
        with self._condition:
            current_time = time.time()
            time_since_last = current_time - self.last_execution_time
            
            # Calculate jittered delay range
            min_jitter = self.base_delay_seconds * (1 - self.jitter_factor)
            max_jitter = self.base_delay_seconds * (1 + self.jitter_factor)
            
            # Current jittered delay
            jittered_delay = self._calculate_jittered_delay()
            remaining_delay = max(0, jittered_delay - time_since_last)
            
            return {
                "throttler_type": "jittered",
                "resource_name": self.resource_name,
                "base_delay_seconds": self.base_delay_seconds,
                "jitter_factor": self.jitter_factor,
                "min_jittered_delay": min_jitter,
                "max_jittered_delay": max_jitter,
                "time_since_last_execution": time_since_last,
                "remaining_delay": remaining_delay
            }


class ThrottlerFactory:
    """Factory class for creating throttlers based on strategy."""
    
    @staticmethod
    def create_throttler(resource_name, delay_seconds, strategy):
        """Create a throttler instance based on the specified strategy.
        
        Args:
            resource_name: Name of the resource being throttled
            delay_seconds: Delay in seconds
            strategy: Throttling strategy to use
            
        Returns:
            Throttler: A throttler instance of the appropriate type
        """
        if strategy == ThrottlingStrategy.FIXED_DELAY or strategy is None:
            return FixedDelayThrottler(resource_name, delay_seconds)
        elif strategy == ThrottlingStrategy.ADAPTIVE:
            return AdaptiveThrottler(resource_name, delay_seconds)
        elif strategy == ThrottlingStrategy.BACKPRESSURE:
            return BackpressureThrottler(resource_name, delay_seconds)
        elif strategy == ThrottlingStrategy.JITTERED:
            return JitteredThrottler(resource_name, delay_seconds)
        else:
            # Default to fixed delay
            logger.warning(f"Unknown throttling strategy: {strategy}. Using FIXED_DELAY instead.")
            return FixedDelayThrottler(resource_name, delay_seconds)


def get_throttler(resource_name, delay_seconds, strategy=ThrottlingStrategy.FIXED_DELAY):
    """Factory function to get or create a throttler instance for a specific resource.
    
    Args:
        resource_name: Identifier for the resource
        delay_seconds: Delay in seconds
        strategy: Throttling strategy to use
        
    Returns:
        Throttler: A throttler instance for the specified resource
    """
    global _throttlers
    
    # Create key from resource name and strategy
    key = f"{resource_name}:{strategy.value if isinstance(strategy, ThrottlingStrategy) else str(strategy)}"
    
    # Check if throttler already exists
    if key in _throttlers:
        return _throttlers[key]
    
    # Create new throttler
    throttler = ThrottlerFactory.create_throttler(resource_name, delay_seconds, strategy)
    
    # Store in registry
    _throttlers[key] = throttler
    
    logger.debug(f"Created new {strategy} throttler for resource '{resource_name}' with {delay_seconds}s delay")
    
    return throttler


def throttle(resource_name, delay_seconds, strategy=ThrottlingStrategy.FIXED_DELAY, blocking=True):
    """Decorator that applies throttling to a function.
    
    Args:
        resource_name: Identifier for the resource
        delay_seconds: Delay in seconds
        strategy: Throttling strategy to use
        blocking: If True, block until throttling is possible; 
                if False, raise ThrottlingError if throttling would block
                
    Returns:
        Callable: Decorated function with throttling applied
        
    Raises:
        ThrottlingError: If throttling would block and blocking is False
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get throttler for this resource
            throttler = get_throttler(resource_name, delay_seconds, strategy)
            
            try:
                # Apply throttling
                throttled = throttler.throttle(blocking=blocking)
                
                if not throttled and not blocking:
                    # Throttling would block but blocking is False
                    raise ThrottlingError(resource_name, delay_seconds)
                
                # Execute function
                return func(*args, **kwargs)
            except RateLimitError:
                # If the operation is rate limited, increase backpressure if using a backpressure throttler
                if isinstance(throttler, BackpressureThrottler):
                    throttler.increase_backpressure()
                raise
            
        return wrapper
    
    return decorator


def reset_throttler(resource_name):
    """Reset a specific throttler to its initial state.
    
    Args:
        resource_name: Identifier for the resource
        
    Returns:
        bool: True if reset successful, False if throttler not found
    """
    global _throttlers
    
    # Check if throttler exists for any strategy
    for key, throttler in list(_throttlers.items()):
        if key.startswith(f"{resource_name}:"):
            throttler.reset()
            logger.debug(f"Reset throttler for resource '{resource_name}'")
            return True
    
    logger.debug(f"No throttler found for resource '{resource_name}'")
    return False


def reset_all_throttlers():
    """Reset all throttlers to their initial state."""
    global _throttlers
    
    # Reset all throttlers
    for throttler in _throttlers.values():
        throttler.reset()
    
    logger.info(f"Reset {len(_throttlers)} throttlers")