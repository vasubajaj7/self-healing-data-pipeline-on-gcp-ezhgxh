"""
Implements rate limiting functionality for the self-healing data pipeline to control 
request frequency and prevent overwhelming external services or internal resources.
Provides multiple rate limiting strategies including token bucket, fixed window,
sliding window, and leaky bucket implementations.
"""

import time
import threading
import enum
from typing import Dict, Any, Optional, Callable
import functools
import collections

from ...constants import DEFAULT_TIMEOUT_SECONDS
from ...config import get_config
from ...utils.logging.logger import get_logger

# Configure module logger
logger = get_logger(__name__)

# Global registry of rate limiters
_rate_limiters = {}


class RateLimiterStrategy(enum.Enum):
    """Enumeration of rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"


class RateLimitExceededError(Exception):
    """Exception raised when rate limit is exceeded and blocking is disabled."""
    
    def __init__(self, resource_name: str, max_calls: int, period: float, message: str = None):
        """Initialize the rate limit exceeded error.
        
        Args:
            resource_name: Name of the rate-limited resource
            max_calls: Maximum calls allowed per period
            period: Time period in seconds
            message: Optional error message
        """
        if message is None:
            message = f"Rate limit exceeded for {resource_name}: {max_calls} calls per {period} seconds"
        super().__init__(message)
        self.resource_name = resource_name
        self.max_calls = max_calls
        self.period = period


class RateLimiter:
    """Abstract base class defining the interface for rate limiters."""
    
    def __init__(self, resource_name: str, max_calls: int, period: float):
        """Initialize the rate limiter with configuration parameters.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum number of calls allowed per period
            period: Time period in seconds
        
        Raises:
            ValueError: If max_calls is not positive or period is not positive
        """
        self.resource_name = resource_name
        self.max_calls = max_calls
        self.period = period
        self._lock = threading.RLock()
        
        # Validate parameters
        if max_calls <= 0:
            raise ValueError("max_calls must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
    
    def acquire(self, blocking: bool = True, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
        """Attempt to acquire permission to proceed under the rate limit.
        
        Args:
            blocking: If True, block until permission is granted or timeout occurs
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if permission is granted, False otherwise
        """
        raise NotImplementedError("Subclasses must implement acquire()")
    
    def reset(self) -> None:
        """Reset the rate limiter to its initial state."""
        raise NotImplementedError("Subclasses must implement reset()")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the rate limiter's current state.
        
        Returns:
            Dictionary containing rate limiter statistics
        """
        raise NotImplementedError("Subclasses must implement get_statistics()")


class TokenBucketRateLimiter(RateLimiter):
    """Implements token bucket algorithm for rate limiting.
    
    In this algorithm, tokens are added to a bucket at a constant rate, and each
    request consumes one token. Once the bucket is empty, additional requests are
    either blocked or rejected until more tokens are added.
    
    This algorithm provides a smoother rate limiting experience than fixed window
    and allows for bursts of traffic up to the bucket capacity.
    """
    
    def __init__(self, resource_name: str, max_calls: int, period: float):
        """Initialize the token bucket rate limiter.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum number of calls allowed per period (bucket capacity)
            period: Time period in seconds over which the rate is enforced
        """
        super().__init__(resource_name, max_calls, period)
        self.tokens = max_calls  # Start with a full bucket
        self.last_refill_time = time.time()
        self._condition = threading.Condition(self._lock)
    
    def acquire(self, blocking: bool = True, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
        """Attempt to acquire a token from the bucket.
        
        Args:
            blocking: If True, block until a token is available or timeout occurs
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if a token was acquired, False otherwise
        """
        end_time = time.time() + timeout
        
        with self._condition:
            # First, refill tokens based on time elapsed
            self._refill_tokens()
            
            # Check if we can consume a token immediately
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            
            # If not blocking, return False immediately
            if not blocking:
                return False
            
            # Block until a token is available or timeout occurs
            remaining_time = end_time - time.time()
            while remaining_time > 0 and self.tokens < 1:
                # Wait for a token to become available or timeout
                self._condition.wait(remaining_time)
                
                # After wait, refill tokens again based on time elapsed
                self._refill_tokens()
                
                # Recalculate remaining time
                remaining_time = end_time - time.time()
            
            # Check if we can now consume a token
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            
            # Timeout occurred
            return False
    
    def reset(self) -> None:
        """Reset the token bucket to its initial state."""
        with self._condition:
            self.tokens = self.max_calls
            self.last_refill_time = time.time()
            self._condition.notify_all()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the token bucket's current state.
        
        Returns:
            Dictionary containing token bucket statistics
        """
        with self._condition:
            # Refill tokens first for accurate statistics
            self._refill_tokens()
            
            return {
                "resource_name": self.resource_name,
                "strategy": RateLimiterStrategy.TOKEN_BUCKET.value,
                "max_calls": self.max_calls,
                "period": self.period,
                "tokens_remaining": self.tokens,
                "refill_rate": self.max_calls / self.period,
                "next_token_time": time.time() + (1 / (self.max_calls / self.period)) if self.tokens < self.max_calls else None
            }
    
    def _refill_tokens(self) -> None:
        """Refill tokens based on time elapsed since last refill."""
        now = time.time()
        elapsed = now - self.last_refill_time
        
        if elapsed > 0:
            # Calculate how many tokens to add based on elapsed time
            new_tokens = elapsed * (self.max_calls / self.period)
            
            # Add tokens up to max_calls
            self.tokens = min(self.tokens + new_tokens, self.max_calls)
            
            # Update last refill time
            self.last_refill_time = now
            
            # If tokens were added, notify waiting threads
            if new_tokens > 0:
                self._condition.notify_all()


class FixedWindowRateLimiter(RateLimiter):
    """Implements fixed window algorithm for rate limiting.
    
    In this algorithm, a fixed time window is established, and a counter tracks
    the number of requests within that window. Once the counter reaches the limit,
    additional requests are either blocked or rejected until the window resets.
    
    This algorithm is simple to implement but can lead to request bunching at
    window boundaries.
    """
    
    def __init__(self, resource_name: str, max_calls: int, period: float):
        """Initialize the fixed window rate limiter.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum number of calls allowed per window
            period: Time window size in seconds
        """
        super().__init__(resource_name, max_calls, period)
        self.call_count = 0
        self.window_start_time = time.time()
        self._condition = threading.Condition(self._lock)
    
    def acquire(self, blocking: bool = True, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
        """Attempt to acquire permission within the current window.
        
        Args:
            blocking: If True, block until permission is granted or timeout occurs
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if permission is granted, False otherwise
        """
        end_time = time.time() + timeout
        
        with self._condition:
            # Check if the window has expired and reset if needed
            window_reset = self._check_window_reset()
            
            # If we haven't hit the limit, allow the request
            if self.call_count < self.max_calls:
                self.call_count += 1
                return True
            
            # If not blocking, return False immediately
            if not blocking:
                return False
            
            # Calculate how long until the window resets
            now = time.time()
            window_end_time = self.window_start_time + self.period
            
            # Block until the window resets or timeout occurs
            remaining_time = min(end_time - now, window_end_time - now)
            
            while remaining_time > 0 and self.call_count >= self.max_calls:
                # Wait for window reset or timeout
                self._condition.wait(remaining_time)
                
                # Check if the window has reset
                window_reset = self._check_window_reset()
                
                # If window reset, we can proceed
                if window_reset:
                    self.call_count += 1
                    return True
                
                # Recalculate remaining time
                now = time.time()
                window_end_time = self.window_start_time + self.period
                remaining_time = min(end_time - now, window_end_time - now)
            
            # Check if we can now allow the request
            if self.call_count < self.max_calls:
                self.call_count += 1
                return True
            
            # Timeout occurred
            return False
    
    def reset(self) -> None:
        """Reset the fixed window to its initial state."""
        with self._condition:
            self.call_count = 0
            self.window_start_time = time.time()
            self._condition.notify_all()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the fixed window's current state.
        
        Returns:
            Dictionary containing fixed window statistics
        """
        with self._condition:
            # Check window reset for accurate statistics
            self._check_window_reset()
            
            now = time.time()
            window_end_time = self.window_start_time + self.period
            time_remaining = max(0, window_end_time - now)
            
            return {
                "resource_name": self.resource_name,
                "strategy": RateLimiterStrategy.FIXED_WINDOW.value,
                "max_calls": self.max_calls,
                "period": self.period,
                "calls_in_window": self.call_count,
                "window_remaining": time_remaining,
                "window_usage_percentage": (self.call_count / self.max_calls) * 100 if self.max_calls > 0 else 0
            }
    
    def _check_window_reset(self) -> bool:
        """Check if the current window has expired and reset if needed.
        
        Returns:
            True if window was reset, False otherwise
        """
        now = time.time()
        elapsed = now - self.window_start_time
        
        if elapsed >= self.period:
            # Reset window
            self.call_count = 0
            self.window_start_time = now
            self._condition.notify_all()
            return True
            
        return False


class SlidingWindowRateLimiter(RateLimiter):
    """Implements sliding window algorithm for rate limiting.
    
    In this algorithm, the timestamps of recent requests are tracked in a sliding
    window. When a new request arrives, old timestamps outside the window are
    dropped, and if the number of timestamps within the window is below the limit,
    the request is allowed.
    
    This approach provides more consistent rate limiting than fixed windows by
    avoiding request bunching at window boundaries.
    """
    
    def __init__(self, resource_name: str, max_calls: int, period: float):
        """Initialize the sliding window rate limiter.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum number of calls allowed within the sliding window
            period: Time window size in seconds
        """
        super().__init__(resource_name, max_calls, period)
        self.timestamps = collections.deque()
        self._condition = threading.Condition(self._lock)
    
    def acquire(self, blocking: bool = True, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
        """Attempt to acquire permission within the sliding window.
        
        Args:
            blocking: If True, block until permission is granted or timeout occurs
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if permission is granted, False otherwise
        """
        end_time = time.time() + timeout
        
        with self._condition:
            # Remove expired timestamps
            self._remove_expired_timestamps()
            
            # If we haven't hit the limit, allow the request
            if len(self.timestamps) < self.max_calls:
                self.timestamps.append(time.time())
                return True
            
            # If not blocking, return False immediately
            if not blocking:
                return False
            
            # Calculate when the oldest timestamp will expire
            now = time.time()
            if self.timestamps:
                oldest_timestamp = self.timestamps[0]
                next_expiry = oldest_timestamp + self.period
            else:
                next_expiry = now  # No timestamps, so immediate
            
            # Block until a timestamp expires or timeout occurs
            remaining_time = min(end_time - now, next_expiry - now)
            
            while remaining_time > 0 and len(self.timestamps) >= self.max_calls:
                # Wait for a timestamp to expire or timeout
                self._condition.wait(remaining_time)
                
                # Remove expired timestamps
                self._remove_expired_timestamps()
                
                # Recalculate remaining time
                now = time.time()
                if self.timestamps:
                    oldest_timestamp = self.timestamps[0]
                    next_expiry = oldest_timestamp + self.period
                else:
                    next_expiry = now  # No timestamps, so immediate
                
                remaining_time = min(end_time - now, next_expiry - now)
            
            # Check if we can now allow the request
            if len(self.timestamps) < self.max_calls:
                self.timestamps.append(time.time())
                return True
            
            # Timeout occurred
            return False
    
    def reset(self) -> None:
        """Reset the sliding window to its initial state."""
        with self._condition:
            self.timestamps.clear()
            self._condition.notify_all()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the sliding window's current state.
        
        Returns:
            Dictionary containing sliding window statistics
        """
        with self._condition:
            # Remove expired timestamps for accurate statistics
            self._remove_expired_timestamps()
            
            now = time.time()
            window_usage = len(self.timestamps)
            oldest_timestamp = self.timestamps[0] if self.timestamps else now
            window_start = now - self.period
            
            return {
                "resource_name": self.resource_name,
                "strategy": RateLimiterStrategy.SLIDING_WINDOW.value,
                "max_calls": self.max_calls,
                "period": self.period,
                "current_usage": window_usage,
                "oldest_call_age": now - oldest_timestamp if self.timestamps else 0,
                "window_usage_percentage": (window_usage / self.max_calls) * 100 if self.max_calls > 0 else 0
            }
    
    def _remove_expired_timestamps(self) -> int:
        """Remove timestamps that are outside the current window.
        
        Returns:
            Number of timestamps removed
        """
        now = time.time()
        cutoff_time = now - self.period
        removed_count = 0
        
        # Remove timestamps older than the cutoff time
        while self.timestamps and self.timestamps[0] <= cutoff_time:
            self.timestamps.popleft()
            removed_count += 1
            
        # If timestamps were removed, notify waiting threads
        if removed_count > 0:
            self._condition.notify_all()
            
        return removed_count


class LeakyBucketRateLimiter(RateLimiter):
    """Implements leaky bucket algorithm for rate limiting.
    
    In this algorithm, requests are added to a bucket, and the bucket leaks at a
    constant rate. If the bucket overflows, the request is either blocked or rejected.
    This simulates a queue with a constant processing rate.
    
    The leaky bucket algorithm enforces a constant outflow rate, making it useful
    for scenarios where consistent spacing between requests is desired.
    """
    
    def __init__(self, resource_name: str, max_calls: int, period: float):
        """Initialize the leaky bucket rate limiter.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum capacity of the bucket
            period: Time period in seconds for processing all requests in a full bucket
        """
        super().__init__(resource_name, max_calls, period)
        self.water_level = 0
        self.last_leak_time = time.time()
        self.leak_rate = max_calls / period  # Units per second
        self._condition = threading.Condition(self._lock)
    
    def acquire(self, blocking: bool = True, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> bool:
        """Attempt to add to the bucket without overflowing.
        
        Args:
            blocking: If True, block until space is available or timeout occurs
            timeout: Maximum time to wait if blocking is True
            
        Returns:
            True if added to bucket, False if bucket would overflow
        """
        end_time = time.time() + timeout
        
        with self._condition:
            # Leak water from the bucket based on time elapsed
            self._leak()
            
            # If there's room in the bucket, add the request
            if self.water_level < self.max_calls:
                self.water_level += 1
                return True
            
            # If not blocking, return False immediately
            if not blocking:
                return False
            
            # Calculate how long until there's space in the bucket
            time_to_available = (1 / self.leak_rate) if self.leak_rate > 0 else float('inf')
            
            # Block until there's space or timeout occurs
            remaining_time = min(end_time - time.time(), time_to_available)
            
            while remaining_time > 0 and self.water_level >= self.max_calls:
                # Wait for space to become available or timeout
                self._condition.wait(remaining_time)
                
                # Leak water from the bucket
                self._leak()
                
                # Recalculate remaining time
                time_to_available = (1 / self.leak_rate) if self.leak_rate > 0 else float('inf')
                remaining_time = min(end_time - time.time(), time_to_available)
            
            # Check if we can now add to the bucket
            if self.water_level < self.max_calls:
                self.water_level += 1
                return True
            
            # Timeout occurred
            return False
    
    def reset(self) -> None:
        """Reset the leaky bucket to its initial state."""
        with self._condition:
            self.water_level = 0
            self.last_leak_time = time.time()
            self._condition.notify_all()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the leaky bucket's current state.
        
        Returns:
            Dictionary containing leaky bucket statistics
        """
        with self._condition:
            # Leak water for accurate statistics
            self._leak()
            
            time_until_empty = self.water_level / self.leak_rate if self.leak_rate > 0 and self.water_level > 0 else 0
            
            return {
                "resource_name": self.resource_name,
                "strategy": RateLimiterStrategy.LEAKY_BUCKET.value,
                "max_calls": self.max_calls,
                "period": self.period,
                "current_level": self.water_level,
                "leak_rate": self.leak_rate,
                "time_until_empty": time_until_empty,
                "bucket_usage_percentage": (self.water_level / self.max_calls) * 100 if self.max_calls > 0 else 0
            }
    
    def _leak(self) -> float:
        """Leak water from the bucket based on time elapsed.
        
        Returns:
            Amount of water leaked
        """
        now = time.time()
        elapsed = now - self.last_leak_time
        
        if elapsed > 0:
            # Calculate how much to leak based on time elapsed
            leak_amount = elapsed * self.leak_rate
            
            # Update water level, ensuring it doesn't go below 0
            old_level = self.water_level
            self.water_level = max(0, self.water_level - leak_amount)
            
            # Update last leak time
            self.last_leak_time = now
            
            # If water was leaked and bucket is no longer full, notify waiting threads
            if old_level > self.water_level and old_level >= self.max_calls:
                self._condition.notify_all()
                
            return old_level - self.water_level
            
        return 0


class RateLimiterFactory:
    """Factory class for creating rate limiters based on strategy."""
    
    @staticmethod
    def create_rate_limiter(resource_name: str, max_calls: int, period: float, strategy: RateLimiterStrategy = None) -> RateLimiter:
        """Create a rate limiter instance based on the specified strategy.
        
        Args:
            resource_name: Name of the resource being rate limited
            max_calls: Maximum number of calls allowed per period
            period: Time period in seconds
            strategy: Rate limiting strategy to use
            
        Returns:
            A rate limiter instance of the appropriate type
        """
        if strategy == RateLimiterStrategy.TOKEN_BUCKET or strategy is None:
            return TokenBucketRateLimiter(resource_name, max_calls, period)
        elif strategy == RateLimiterStrategy.FIXED_WINDOW:
            return FixedWindowRateLimiter(resource_name, max_calls, period)
        elif strategy == RateLimiterStrategy.SLIDING_WINDOW:
            return SlidingWindowRateLimiter(resource_name, max_calls, period)
        elif strategy == RateLimiterStrategy.LEAKY_BUCKET:
            return LeakyBucketRateLimiter(resource_name, max_calls, period)
        else:
            logger.warning(f"Unknown rate limiter strategy: {strategy}, using token bucket")
            return TokenBucketRateLimiter(resource_name, max_calls, period)


def get_rate_limiter(resource_name: str, max_calls: int, period: float, strategy: RateLimiterStrategy = None) -> RateLimiter:
    """Factory function to get or create a rate limiter instance for a specific resource.
    
    Args:
        resource_name: Name of the resource being rate limited
        max_calls: Maximum number of calls allowed per period
        period: Time period in seconds
        strategy: Rate limiting strategy to use
        
    Returns:
        A rate limiter instance for the specified resource
    """
    # Use resource name as key in the registry
    key = resource_name
    
    # Check if rate limiter already exists
    if key in _rate_limiters:
        return _rate_limiters[key]
    
    # Create new rate limiter
    limiter = RateLimiterFactory.create_rate_limiter(resource_name, max_calls, period, strategy)
    _rate_limiters[key] = limiter
    
    logger.debug(f"Created rate limiter for {resource_name}: {max_calls} calls per {period} seconds using {strategy or RateLimiterStrategy.TOKEN_BUCKET}")
    
    return limiter


def rate_limit(resource_name: str, max_calls: int, period: float, strategy: RateLimiterStrategy = None, blocking: bool = True):
    """Decorator that applies rate limiting to a function.
    
    Args:
        resource_name: Name of the resource being rate limited
        max_calls: Maximum number of calls allowed per period
        period: Time period in seconds
        strategy: Rate limiting strategy to use
        blocking: If True, block until rate limit allows execution;
                 if False, raise RateLimitExceededError when limit is exceeded
        
    Returns:
        Decorated function with rate limiting applied
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get or create rate limiter
            limiter = get_rate_limiter(resource_name, max_calls, period, strategy)
            
            # Attempt to acquire permission
            acquired = limiter.acquire(blocking=blocking)
            
            if not acquired:
                # If permission not granted and not blocking, raise exception
                raise RateLimitExceededError(resource_name, max_calls, period)
            
            # Execute the function
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def reset_rate_limiter(resource_name: str) -> bool:
    """Reset a specific rate limiter to its initial state.
    
    Args:
        resource_name: Name of the resource whose rate limiter should be reset
        
    Returns:
        True if reset successful, False if rate limiter not found
    """
    if resource_name in _rate_limiters:
        _rate_limiters[resource_name].reset()
        logger.debug(f"Reset rate limiter for {resource_name}")
        return True
    
    logger.warning(f"Attempted to reset non-existent rate limiter: {resource_name}")
    return False


def reset_all_rate_limiters() -> None:
    """Reset all rate limiters to their initial state."""
    for resource_name, limiter in _rate_limiters.items():
        limiter.reset()
    
    logger.debug(f"Reset all rate limiters ({len(_rate_limiters)} total)")