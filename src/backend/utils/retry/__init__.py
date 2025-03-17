"""
Retry utilities package for the self-healing data pipeline.

This package provides comprehensive fault tolerance mechanisms including:
1. Backoff strategies for timing between retry attempts
2. Circuit breaker pattern implementation to prevent cascading failures
3. Retry decorators for automatic retries with configurable behavior

These utilities help the pipeline recover from transient failures and provide
resilience against external system disruptions.
"""

# Import backoff strategy components
from .backoff_strategy import (
    BackoffStrategy,
    ExponentialBackoffStrategy,
    LinearBackoffStrategy,
    ConstantBackoffStrategy,
    RandomBackoffStrategy,
    BackoffConfig,
    get_backoff_strategy,
    get_backoff_strategy_for_error,
    add_jitter
)

# Import circuit breaker components
from .circuit_breaker import (
    CircuitState,
    CircuitBreaker,
    CircuitBreakerConfig,
    get_circuit_breaker,
    circuit_breaker,
    reset_all_circuit_breakers,
    get_circuit_breaker_status
)

# Import retry decorator components
from .retry_decorator import (
    retry,
    retry_with_config,
    RetryConfig,
    should_retry_exception,
    get_error_category
)

__all__ = [
    # Backoff strategy components
    'BackoffStrategy',
    'ExponentialBackoffStrategy',
    'LinearBackoffStrategy',
    'ConstantBackoffStrategy',
    'RandomBackoffStrategy',
    'BackoffConfig',
    'get_backoff_strategy',
    'get_backoff_strategy_for_error',
    'add_jitter',
    
    # Circuit breaker components
    'CircuitState',
    'CircuitBreaker',
    'CircuitBreakerConfig',
    'get_circuit_breaker',
    'circuit_breaker',
    'reset_all_circuit_breakers',
    'get_circuit_breaker_status',
    
    # Retry decorator components
    'retry',
    'retry_with_config',
    'RetryConfig',
    'should_retry_exception',
    'get_error_category'
]