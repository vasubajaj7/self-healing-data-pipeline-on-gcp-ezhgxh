"""
Implements various backoff strategies for retry mechanisms in the self-healing data pipeline.

Provides configurable algorithms for determining wait times between retry attempts,
including exponential backoff with jitter, linear backoff, and constant backoff patterns.
"""

import abc
import time
import random
import math
from typing import Optional, Dict, Any, Union

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import ErrorCategory

# Initialize logger
logger = get_logger(__name__)

# Default values for backoff strategies
DEFAULT_BASE_DELAY = 1.0  # Base delay in seconds
DEFAULT_MAX_DELAY = 60.0  # Maximum delay in seconds
DEFAULT_JITTER_FACTOR = 0.1  # Jitter as a percentage of the delay


class BackoffStrategy(abc.ABC):
    """
    Abstract base class defining the interface for backoff strategies.
    """
    
    def __init__(self, base_delay: Optional[float] = None, max_delay: Optional[float] = None, jitter_factor: Optional[float] = None):
        """
        Initialize the backoff strategy with configuration parameters.
        
        Args:
            base_delay: Initial delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        """
        self.base_delay = base_delay if base_delay is not None else DEFAULT_BASE_DELAY
        self.max_delay = max_delay if max_delay is not None else DEFAULT_MAX_DELAY
        self.jitter_factor = jitter_factor if jitter_factor is not None else DEFAULT_JITTER_FACTOR
        
        # Validate parameters
        if self.base_delay <= 0:
            raise ValueError("Base delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("Max delay must be greater than or equal to base delay")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError("Jitter factor must be between 0 and 1")
    
    @abc.abstractmethod
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a specific retry attempt.
        
        Args:
            attempt: Current retry attempt number (1-based)
            
        Returns:
            Delay time in seconds
        """
        pass
    
    def apply_jitter(self, delay: float) -> float:
        """
        Apply jitter to a calculated delay.
        
        Args:
            delay: Base delay to apply jitter to
            
        Returns:
            Delay with jitter applied
        """
        return add_jitter(delay, self.jitter_factor)
    
    def wait(self, attempt: int) -> float:
        """
        Wait for the calculated delay period.
        
        Args:
            attempt: Current retry attempt number (1-based)
            
        Returns:
            Actual delay time in seconds
        """
        delay = self.get_delay(attempt)
        jittered_delay = self.apply_jitter(delay)
        
        logger.debug(f"Waiting {jittered_delay:.2f}s before retry attempt {attempt}")
        time.sleep(jittered_delay)
        
        return jittered_delay


class ExponentialBackoffStrategy(BackoffStrategy):
    """
    Implements exponential backoff algorithm where delay increases exponentially with each attempt.
    
    Formula: delay = base_delay * (2 ^ (attempt - 1))
    """
    
    def __init__(self, base_delay: Optional[float] = None, max_delay: Optional[float] = None, jitter_factor: Optional[float] = None):
        """
        Initialize exponential backoff strategy with configuration parameters.
        
        Args:
            base_delay: Initial delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        """
        super().__init__(base_delay, max_delay, jitter_factor)
    
    def get_delay(self, attempt: int) -> float:
        """
        Calculate exponential delay for a specific retry attempt.
        
        Args:
            attempt: Current retry attempt number (1-based)
            
        Returns:
            Delay time in seconds
        """
        # Ensure attempt is at least 1
        attempt = max(1, attempt)
        
        # Calculate exponential delay: base_delay * 2^(attempt-1)
        delay = self.base_delay * (2 ** (attempt - 1))
        
        # Cap at max_delay
        return min(delay, self.max_delay)


class LinearBackoffStrategy(BackoffStrategy):
    """
    Implements linear backoff algorithm where delay increases linearly with each attempt.
    
    Formula: delay = base_delay + (increment * (attempt - 1))
    """
    
    def __init__(self, base_delay: Optional[float] = None, max_delay: Optional[float] = None, 
                 jitter_factor: Optional[float] = None, increment: Optional[float] = None):
        """
        Initialize linear backoff strategy with configuration parameters.
        
        Args:
            base_delay: Initial delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
            increment: Increment amount for each retry, defaults to base_delay
        """
        super().__init__(base_delay, max_delay, jitter_factor)
        self.increment = increment if increment is not None else self.base_delay
    
    def get_delay(self, attempt: int) -> float:
        """
        Calculate linear delay for a specific retry attempt.
        
        Args:
            attempt: Current retry attempt number (1-based)
            
        Returns:
            Delay time in seconds
        """
        # Ensure attempt is at least 1
        attempt = max(1, attempt)
        
        # Calculate linear delay: base_delay + (increment * (attempt-1))
        delay = self.base_delay + (self.increment * (attempt - 1))
        
        # Cap at max_delay
        return min(delay, self.max_delay)


class ConstantBackoffStrategy(BackoffStrategy):
    """
    Implements constant backoff algorithm where delay remains the same for all attempts.
    
    Formula: delay = base_delay
    """
    
    def __init__(self, base_delay: Optional[float] = None, max_delay: Optional[float] = None, jitter_factor: Optional[float] = None):
        """
        Initialize constant backoff strategy with configuration parameters.
        
        Args:
            base_delay: Constant delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY (not used in this strategy)
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        """
        super().__init__(base_delay, max_delay, jitter_factor)
    
    def get_delay(self, attempt: int) -> float:
        """
        Return constant delay for all retry attempts.
        
        Args:
            attempt: Current retry attempt number (1-based, not used in this strategy)
            
        Returns:
            Delay time in seconds
        """
        return self.base_delay


class RandomBackoffStrategy(BackoffStrategy):
    """
    Implements random backoff algorithm where delay is randomly selected within a range.
    
    Formula: delay = random_between(base_delay, max_delay)
    """
    
    def __init__(self, base_delay: Optional[float] = None, max_delay: Optional[float] = None, jitter_factor: Optional[float] = None):
        """
        Initialize random backoff strategy with configuration parameters.
        
        Args:
            base_delay: Minimum delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        """
        super().__init__(base_delay, max_delay, jitter_factor)
    
    def get_delay(self, attempt: int) -> float:
        """
        Generate random delay between base_delay and max_delay.
        
        Args:
            attempt: Current retry attempt number (1-based, not used in this strategy)
            
        Returns:
            Random delay time in seconds
        """
        return random.uniform(self.base_delay, self.max_delay)


def add_jitter(delay: float, jitter_factor: Optional[float] = None) -> float:
    """
    Adds random jitter to a delay value to prevent thundering herd problem.
    
    Args:
        delay: Base delay value in seconds
        jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        
    Returns:
        Delay with jitter applied
    """
    if jitter_factor is None:
        jitter_factor = DEFAULT_JITTER_FACTOR
    
    jitter_amount = delay * jitter_factor
    random_jitter = random.uniform(-jitter_amount, jitter_amount)
    
    # Ensure delay with jitter doesn't go below zero
    return max(0, delay + random_jitter)


def get_backoff_strategy(strategy_name: str, 
                        base_delay: Optional[float] = None, 
                        max_delay: Optional[float] = None, 
                        jitter_factor: Optional[float] = None) -> BackoffStrategy:
    """
    Factory function to create a backoff strategy instance based on strategy name.
    
    Args:
        strategy_name: Name of the backoff strategy ('exponential', 'linear', 'constant', 'random')
        base_delay: Base delay in seconds, defaults to DEFAULT_BASE_DELAY
        max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
        jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        
    Returns:
        BackoffStrategy: Configured backoff strategy instance
    """
    # Set default values if not provided
    base_delay = base_delay if base_delay is not None else DEFAULT_BASE_DELAY
    max_delay = max_delay if max_delay is not None else DEFAULT_MAX_DELAY
    jitter_factor = jitter_factor if jitter_factor is not None else DEFAULT_JITTER_FACTOR
    
    # Create and return the appropriate strategy
    if strategy_name.lower() == 'exponential':
        return ExponentialBackoffStrategy(base_delay, max_delay, jitter_factor)
    elif strategy_name.lower() == 'linear':
        return LinearBackoffStrategy(base_delay, max_delay, jitter_factor)
    elif strategy_name.lower() == 'constant':
        return ConstantBackoffStrategy(base_delay, max_delay, jitter_factor)
    elif strategy_name.lower() == 'random':
        return RandomBackoffStrategy(base_delay, max_delay, jitter_factor)
    else:
        logger.warning(f"Unknown backoff strategy: {strategy_name}, using exponential backoff as default")
        return ExponentialBackoffStrategy(base_delay, max_delay, jitter_factor)


def get_backoff_strategy_for_error(error_category: ErrorCategory, 
                                 base_delay: Optional[float] = None, 
                                 max_delay: Optional[float] = None) -> BackoffStrategy:
    """
    Selects an appropriate backoff strategy based on error category.
    
    Args:
        error_category: Category of the error
        base_delay: Base delay in seconds, defaults to DEFAULT_BASE_DELAY
        max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
        
    Returns:
        BackoffStrategy: Appropriate backoff strategy for the error category
    """
    # Set default values if not provided
    base_delay = base_delay if base_delay is not None else DEFAULT_BASE_DELAY
    max_delay = max_delay if max_delay is not None else DEFAULT_MAX_DELAY
    
    # Choose strategy based on error category
    if error_category in [ErrorCategory.CONNECTION_ERROR, 
                         ErrorCategory.TIMEOUT_ERROR, 
                         ErrorCategory.SERVICE_UNAVAILABLE]:
        # For connection issues, use exponential backoff
        strategy = ExponentialBackoffStrategy(base_delay, max_delay, DEFAULT_JITTER_FACTOR)
    elif error_category == ErrorCategory.RATE_LIMIT_ERROR:
        # For rate limiting, use linear backoff with higher base delay
        strategy = LinearBackoffStrategy(max(base_delay, 2.0), max_delay, DEFAULT_JITTER_FACTOR)
    elif error_category == ErrorCategory.RESOURCE_ERROR:
        # For resource issues, use exponential backoff with higher jitter
        strategy = ExponentialBackoffStrategy(base_delay, max_delay, min(DEFAULT_JITTER_FACTOR * 2, 0.5))
    else:
        # For other errors, use default exponential backoff
        strategy = ExponentialBackoffStrategy(base_delay, max_delay, DEFAULT_JITTER_FACTOR)
    
    logger.debug(f"Selected {strategy.__class__.__name__} for error category {error_category.name}")
    return strategy


class BackoffConfig:
    """
    Configuration class for backoff strategies.
    """
    
    def __init__(self, strategy_name: Optional[str] = None, 
                base_delay: Optional[float] = None, 
                max_delay: Optional[float] = None, 
                jitter_factor: Optional[float] = None):
        """
        Initialize backoff configuration with default or provided values.
        
        Args:
            strategy_name: Name of the backoff strategy, defaults to 'exponential'
            base_delay: Base delay in seconds, defaults to DEFAULT_BASE_DELAY
            max_delay: Maximum delay in seconds, defaults to DEFAULT_MAX_DELAY
            jitter_factor: Factor for jitter as a percentage (0-1), defaults to DEFAULT_JITTER_FACTOR
        """
        self.strategy_name = strategy_name or 'exponential'
        self.base_delay = base_delay if base_delay is not None else DEFAULT_BASE_DELAY
        self.max_delay = max_delay if max_delay is not None else DEFAULT_MAX_DELAY
        self.jitter_factor = jitter_factor if jitter_factor is not None else DEFAULT_JITTER_FACTOR
    
    @classmethod
    def from_config(cls, config_section: str) -> 'BackoffConfig':
        """
        Create BackoffConfig from application configuration.
        
        Args:
            config_section: Section in config where backoff settings are stored
            
        Returns:
            BackoffConfig instance with values from application config
        """
        config = get_config()
        
        # Get backoff configuration from specified section
        strategy_name = config.get(f"{config_section}.strategy", 'exponential')
        base_delay = config.get(f"{config_section}.base_delay", DEFAULT_BASE_DELAY)
        max_delay = config.get(f"{config_section}.max_delay", DEFAULT_MAX_DELAY)
        jitter_factor = config.get(f"{config_section}.jitter_factor", DEFAULT_JITTER_FACTOR)
        
        return cls(strategy_name, float(base_delay), float(max_delay), float(jitter_factor))
    
    def get_strategy(self) -> BackoffStrategy:
        """
        Get a backoff strategy instance based on configuration.
        
        Returns:
            Configured backoff strategy instance
        """
        return get_backoff_strategy(
            self.strategy_name,
            self.base_delay,
            self.max_delay,
            self.jitter_factor
        )