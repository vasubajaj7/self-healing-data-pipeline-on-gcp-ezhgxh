"""
Initialization file for the concurrency module that provides various concurrency control
mechanisms for the self-healing data pipeline. Exposes rate limiting, thread pooling,
and throttling functionality to manage resource utilization and prevent overwhelming
external services or internal components.

This module provides:
- Rate limiting: Control request frequency to services
- Thread pooling: Manage parallel execution of tasks
- Throttling: Control execution speed and prevent system overload

These mechanisms help ensure efficient resource utilization while maintaining system
stability under varying load conditions.
"""

# Import rate limiting functionality
from .rate_limiter import (
    RateLimiterStrategy,
    RateLimiter,
    TokenBucketRateLimiter,
    FixedWindowRateLimiter,
    SlidingWindowRateLimiter,
    LeakyBucketRateLimiter,
    RateLimiterFactory,
    RateLimitExceededError,
    get_rate_limiter,
    rate_limit,
    reset_rate_limiter,
    reset_all_rate_limiters
)

# Import thread pool functionality
from .thread_pool import (
    ThreadPoolStrategy,
    ThreadPool,
    StandardThreadPool,
    ConcurrentFuturesThreadPool,
    CustomThreadPool,
    Future,
    ThreadPoolExecutor,
    create_thread_pool,
    parallel_map,
    parallel_for_each,
    shutdown_thread_pool,
    shutdown_all_thread_pools
)

# Import throttling functionality
from .throttler import (
    ThrottlingStrategy,
    Throttler,
    FixedDelayThrottler,
    AdaptiveThrottler,
    BackpressureThrottler,
    JitteredThrottler,
    ThrottlerFactory,
    ThrottlingError,
    get_throttler,
    throttle,
    reset_throttler,
    reset_all_throttlers
)

# Export all the imported classes and functions
__all__ = [
    # Rate limiting
    'RateLimiterStrategy',
    'RateLimiter',
    'TokenBucketRateLimiter',
    'FixedWindowRateLimiter',
    'SlidingWindowRateLimiter',
    'LeakyBucketRateLimiter',
    'RateLimiterFactory',
    'RateLimitExceededError',
    'get_rate_limiter',
    'rate_limit',
    'reset_rate_limiter',
    'reset_all_rate_limiters',
    
    # Thread pooling
    'ThreadPoolStrategy',
    'ThreadPool',
    'StandardThreadPool',
    'ConcurrentFuturesThreadPool',
    'CustomThreadPool',
    'Future',
    'ThreadPoolExecutor',
    'create_thread_pool',
    'parallel_map',
    'parallel_for_each',
    'shutdown_thread_pool',
    'shutdown_all_thread_pools',
    
    # Throttling
    'ThrottlingStrategy',
    'Throttler',
    'FixedDelayThrottler',
    'AdaptiveThrottler',
    'BackpressureThrottler',
    'JitteredThrottler',
    'ThrottlerFactory',
    'ThrottlingError',
    'get_throttler',
    'throttle',
    'reset_throttler',
    'reset_all_throttlers'
]