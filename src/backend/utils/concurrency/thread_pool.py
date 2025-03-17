"""
Implements thread pool functionality for the self-healing data pipeline to enable efficient parallel processing.
Provides multiple thread pool implementations with a unified interface, supporting different concurrency strategies
and workload patterns.
"""

import threading
import queue
import concurrent.futures
import enum
import typing
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
import functools
import time

from ...constants import DEFAULT_TIMEOUT_SECONDS, MAX_PARALLEL_WORKERS
from ...config import get_config
from ...utils.logging.logger import get_logger

# Configure logger
logger = get_logger(__name__)

# Global registry of thread pools
_thread_pools = {}


class ThreadPoolStrategy(enum.Enum):
    """Enumeration of thread pool implementation strategies."""
    STANDARD = "standard"
    CONCURRENT_FUTURES = "concurrent_futures" 
    CUSTOM = "custom"


class Future:
    """Represents the result of an asynchronous computation."""
    
    def __init__(self):
        """Initialize the future object."""
        self._result = None
        self._exception = None
        self._done = False
        self._completion_event = threading.Event()
        self._callbacks = []
    
    def done(self) -> bool:
        """Return True if the future is done."""
        return self._done
    
    def result(self, timeout: float = None) -> Any:
        """Return the result of the future, blocking if not yet available."""
        if not self._done:
            if not self._completion_event.wait(timeout=timeout):
                raise TimeoutError("Future result timed out")
        
        if self._exception:
            raise self._exception
        
        return self._result
    
    def exception(self, timeout: float = None) -> Optional[Exception]:
        """Return the exception raised by the future, if any."""
        if not self._done:
            if not self._completion_event.wait(timeout=timeout):
                raise TimeoutError("Future exception check timed out")
        
        return self._exception
    
    def add_done_callback(self, callback: Callable[['Future'], None]) -> None:
        """Add a callback to be executed when the future is done."""
        if self._done:
            # If future is already done, execute callback immediately
            try:
                callback(self)
            except Exception as e:
                logger.exception(f"Exception in callback {callback}: {e}")
        else:
            self._callbacks.append(callback)
    
    def set_result(self, result: Any) -> None:
        """Set the result of the future."""
        if self._done:
            raise RuntimeError("Future result has already been set")
        
        self._result = result
        self._done = True
        self._completion_event.set()
        self._invoke_callbacks()
    
    def set_exception(self, exception: Exception) -> None:
        """Set an exception for the future."""
        if self._done:
            raise RuntimeError("Future result has already been set")
        
        self._exception = exception
        self._done = True
        self._completion_event.set()
        self._invoke_callbacks()
    
    def cancel(self) -> bool:
        """Attempt to cancel the future."""
        if self._done:
            return False
        
        self.set_exception(concurrent.futures.CancelledError())
        return True
    
    def cancelled(self) -> bool:
        """Return True if the future was cancelled."""
        return (self._done and 
                isinstance(self._exception, concurrent.futures.CancelledError))
    
    def _invoke_callbacks(self) -> None:
        """Invoke all registered callbacks."""
        for callback in self._callbacks:
            try:
                callback(self)
            except Exception as e:
                logger.exception(f"Exception in callback {callback}: {e}")
        
        # Clear callbacks to avoid memory leaks
        self._callbacks.clear()


class ThreadPool:
    """Abstract base class defining the interface for thread pools."""
    
    def __init__(self, name: str, max_workers: int):
        """Initialize the thread pool with configuration parameters."""
        self.name = name
        self.max_workers = max_workers
        self._shutdown = False
        self._lock = threading.RLock()
        
        # Validate max_workers
        if max_workers <= 0:
            raise ValueError("max_workers must be a positive integer")
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function to be executed with the given arguments."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError(f"Thread pool {self.name} has been shut down")
            
            future = Future()
            self._submit_task(fn, args, kwargs, future)
            return future
    
    def map(self, fn: Callable, iterable: Iterable, timeout: float = None) -> List[Any]:
        """Apply a function to each item in an iterable and return results."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError(f"Thread pool {self.name} has been shut down")
            
            # Convert iterable to list to ensure consistent indexing
            items = list(iterable)
            if not items:
                return []
            
            # Create a future for each item
            futures = []
            for item in items:
                future = self.submit(fn, item)
                futures.append(future)
            
            # Wait for all futures to complete or timeout
            results = []
            end_time = None if timeout is None else time.time() + timeout
            
            for i, future in enumerate(futures):
                if timeout is not None:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        raise TimeoutError(f"map operation timed out after {timeout} seconds")
                else:
                    remaining = None
                
                try:
                    result = future.result(timeout=remaining)
                    results.append(result)
                except Exception as e:
                    # Cancel any remaining futures
                    for f in futures[i+1:]:
                        f.cancel()
                    
                    # Re-raise the exception
                    raise
            
            return results
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool."""
        with self._lock:
            self._shutdown = True
            self._shutdown_implementation(wait)
    
    def _submit_task(self, fn: Callable, args: tuple, kwargs: dict, future: Future) -> None:
        """Implementation-specific method to submit a task."""
        raise NotImplementedError("Subclasses must implement _submit_task")
    
    def _shutdown_implementation(self, wait: bool) -> None:
        """Implementation-specific method for shutdown."""
        raise NotImplementedError("Subclasses must implement _shutdown_implementation")
    
    def get_statistics(self) -> dict:
        """Get statistics about the thread pool's current state."""
        with self._lock:
            return {
                "name": self.name,
                "max_workers": self.max_workers,
                "is_shutdown": self._shutdown,
                # Subclasses should add implementation-specific statistics
            }


class StandardThreadPool(ThreadPool):
    """Thread pool implementation using standard threading and queue."""
    
    def __init__(self, name: str, max_workers: int):
        """Initialize the standard thread pool."""
        super().__init__(name, max_workers)
        self._work_queue = queue.Queue()
        self._workers = []
        self._active_workers = 0
        self._workers_condition = threading.Condition(self._lock)
        
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker,
                daemon=True,
                name=f"{self.name}-worker-{i}"
            )
            worker.start()
            self._workers.append(worker)
    
    def _submit_task(self, fn: Callable, args: tuple, kwargs: dict, future: Future) -> None:
        """Submit a task to the work queue."""
        task = (fn, args, kwargs, future)
        self._work_queue.put(task)
        logger.debug(f"Task submitted to {self.name} pool: {fn.__name__}")
    
    def _shutdown_implementation(self, wait: bool) -> None:
        """Shutdown the standard thread pool."""
        # Signal all worker threads to exit
        for _ in self._workers:
            self._work_queue.put(None)  # Sentinel value to stop worker
        
        # Wait for worker threads to finish if requested
        if wait:
            for worker in self._workers:
                worker.join()
        
        logger.info(f"Thread pool {self.name} has been shut down")
    
    def _worker(self) -> None:
        """Worker thread function that processes tasks from the queue."""
        while True:
            # Get next task from the queue
            task = self._work_queue.get()
            
            # Exit if sentinel value (None) received
            if task is None:
                self._work_queue.task_done()
                break
            
            # Unpack task tuple
            fn, args, kwargs, future = task
            
            # Track active workers count
            with self._workers_condition:
                self._active_workers += 1
            
            try:
                # Execute the function
                result = fn(*args, **kwargs)
                future.set_result(result)
            except Exception as exc:
                # Set exception if execution fails
                future.set_exception(exc)
            finally:
                # Decrement active workers count
                with self._workers_condition:
                    self._active_workers -= 1
                    self._workers_condition.notify_all()
                
                # Mark task as done
                self._work_queue.task_done()
    
    def get_statistics(self) -> dict:
        """Get statistics about the standard thread pool."""
        with self._lock:
            base_stats = super().get_statistics()
            additional_stats = {
                "queue_size": self._work_queue.qsize(),
                "active_workers": self._active_workers,
                "total_workers": len(self._workers),
                "implementation": "standard"
            }
            return {**base_stats, **additional_stats}


class ConcurrentFuturesThreadPool(ThreadPool):
    """Thread pool implementation using concurrent.futures.ThreadPoolExecutor."""
    
    def __init__(self, name: str, max_workers: int):
        """Initialize the concurrent futures thread pool."""
        super().__init__(name, max_workers)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures_map = {}  # Map our Future objects to concurrent.futures.Future objects
    
    def _submit_task(self, fn: Callable, args: tuple, kwargs: dict, future: Future) -> None:
        """Submit a task to the executor."""
        # Create a wrapper function to execute the task
        def wrapper():
            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as e:
                # Just propagate the exception, it will be caught by the callback
                raise
        
        # Submit the wrapper to the executor
        executor_future = self._executor.submit(wrapper)
        
        # Add a callback to handle completion
        executor_future.add_done_callback(self._future_done_callback)
        
        # Store the mapping between our Future and the executor's Future
        self._futures_map[executor_future] = future
        
        logger.debug(f"Task submitted to {self.name} pool: {fn.__name__}")
    
    def _future_done_callback(self, executor_future: concurrent.futures.Future) -> None:
        """Callback for when an executor future completes."""
        # Get our Future object from the mapping
        if executor_future not in self._futures_map:
            logger.error(f"Executor future not found in futures map: {executor_future}")
            return
        
        future = self._futures_map[executor_future]
        
        # Handle the result or exception
        try:
            if executor_future.cancelled():
                future.cancel()
            elif executor_future.exception() is not None:
                future.set_exception(executor_future.exception())
            else:
                future.set_result(executor_future.result())
        except Exception as e:
            future.set_exception(e)
        finally:
            # Remove the mapping
            self._futures_map.pop(executor_future, None)
    
    def _shutdown_implementation(self, wait: bool) -> None:
        """Shutdown the concurrent futures thread pool."""
        self._executor.shutdown(wait=wait)
        self._futures_map.clear()
        logger.info(f"Thread pool {self.name} has been shut down")
    
    def get_statistics(self) -> dict:
        """Get statistics about the concurrent futures thread pool."""
        with self._lock:
            base_stats = super().get_statistics()
            
            # Try to get statistics from the executor
            executor_stats = {}
            if hasattr(self._executor, "_max_workers"):
                executor_stats["max_workers"] = self._executor._max_workers
            if hasattr(self._executor, "_work_queue"):
                executor_stats["queue_size"] = self._executor._work_queue.qsize()
            
            additional_stats = {
                "active_futures": len(self._futures_map),
                "implementation": "concurrent_futures"
            }
            
            return {**base_stats, **additional_stats, **executor_stats}


class CustomThreadPool(ThreadPool):
    """Custom thread pool implementation with advanced features."""
    
    def __init__(self, name: str, max_workers: int):
        """Initialize the custom thread pool."""
        super().__init__(name, max_workers)
        self._work_queue = queue.PriorityQueue()
        self._workers = []
        self._active_workers = 0
        self._workers_condition = threading.Condition(self._lock)
        self._task_counter = 0  # For FIFO ordering of tasks with same priority
        self._task_priorities = {}  # Track task priorities for statistics
        
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker,
                daemon=True,
                name=f"{self.name}-worker-{i}"
            )
            worker.start()
            self._workers.append(worker)
    
    def submit(self, fn: Callable, priority: int = 0, *args, **kwargs) -> Future:
        """Submit a function with priority to be executed."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError(f"Thread pool {self.name} has been shut down")
            
            future = Future()
            self._submit_task_with_priority(fn, priority, args, kwargs, future)
            return future
    
    def _submit_task(self, fn: Callable, args: tuple, kwargs: dict, future: Future) -> None:
        """Submit a task to the priority queue."""
        # Check if priority is in kwargs
        priority = kwargs.pop("priority", 0) if "priority" in kwargs else 0
        self._submit_task_with_priority(fn, priority, args, kwargs, future)
    
    def _submit_task_with_priority(self, fn: Callable, priority: int, args: tuple, kwargs: dict, future: Future) -> None:
        """Submit a task with explicit priority."""
        with self._lock:
            self._task_counter += 1
            # Priority queue takes smallest first, so negate priority
            # Second tuple element is counter to ensure FIFO within same priority
            task = (-priority, self._task_counter, fn, args, kwargs, future)
            self._work_queue.put(task)
            
            # Store task priority for statistics
            self._task_priorities[self._task_counter] = priority
            
            logger.debug(f"Task submitted to {self.name} pool with priority {priority}: {fn.__name__}")
    
    def _shutdown_implementation(self, wait: bool) -> None:
        """Shutdown the custom thread pool."""
        # Signal all worker threads to exit
        for _ in self._workers:
            self._work_queue.put((-float("inf"), 0, None, None, None, None))  # Sentinel with highest priority
        
        # Wait for worker threads to finish if requested
        if wait:
            for worker in self._workers:
                worker.join()
        
        # Clear task priorities
        self._task_priorities.clear()
        
        logger.info(f"Thread pool {self.name} has been shut down")
    
    def _worker(self) -> None:
        """Worker thread function that processes tasks from the priority queue."""
        while True:
            # Get next task from the queue
            task = self._work_queue.get()
            
            # Unpack task tuple
            neg_priority, task_id, fn, args, kwargs, future = task
            
            # Exit if sentinel value received
            if fn is None:
                self._work_queue.task_done()
                break
            
            # Track active workers count
            with self._workers_condition:
                self._active_workers += 1
            
            try:
                # Execute the function
                result = fn(*args, **kwargs)
                future.set_result(result)
            except Exception as exc:
                # Set exception if execution fails
                future.set_exception(exc)
            finally:
                # Decrement active workers count
                with self._workers_condition:
                    self._active_workers -= 1
                    self._workers_condition.notify_all()
                
                # Remove task from priorities dictionary
                with self._lock:
                    self._task_priorities.pop(task_id, None)
                
                # Mark task as done
                self._work_queue.task_done()
    
    def get_statistics(self) -> dict:
        """Get statistics about the custom thread pool."""
        with self._lock:
            base_stats = super().get_statistics()
            
            # Calculate priority distribution
            priority_counts = {}
            for priority in self._task_priorities.values():
                if priority not in priority_counts:
                    priority_counts[priority] = 0
                priority_counts[priority] += 1
            
            additional_stats = {
                "queue_size": self._work_queue.qsize(),
                "active_workers": self._active_workers,
                "total_workers": len(self._workers),
                "priority_distribution": priority_counts,
                "implementation": "custom"
            }
            
            return {**base_stats, **additional_stats}


class ThreadPoolExecutor:
    """Compatibility wrapper for concurrent.futures.ThreadPoolExecutor."""
    
    def __init__(self, max_workers: int = None, thread_name_prefix: str = ""):
        """Initialize the thread pool executor wrapper."""
        self._pool = ConcurrentFuturesThreadPool(
            name=thread_name_prefix or "thread-pool-executor",
            max_workers=max_workers or MAX_PARALLEL_WORKERS
        )
    
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a function to be executed."""
        return self._pool.submit(fn, *args, **kwargs)
    
    def map(self, fn: Callable, *iterables, timeout=None, chunksize=1) -> typing.Iterator:
        """Apply a function to each item in an iterable."""
        # Ignore chunksize parameter for compatibility
        if len(iterables) == 0:
            return []
        
        if len(iterables) == 1:
            return self._pool.map(fn, iterables[0], timeout=timeout)
        
        # For multiple iterables, zip them together
        zipped = zip(*iterables)
        
        # Use a lambda to unpack arguments
        return self._pool.map(lambda args: fn(*args), zipped, timeout=timeout)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor."""
        self._pool.shutdown(wait=wait)
    
    def __enter__(self):
        """Enter the context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        self.shutdown(wait=True)


class ThreadPoolFactory:
    """Factory class for creating thread pools based on strategy."""
    
    @staticmethod
    def create_thread_pool(name: str, max_workers: int, strategy: ThreadPoolStrategy) -> ThreadPool:
        """Create a thread pool instance based on the specified strategy."""
        if strategy == ThreadPoolStrategy.STANDARD:
            return StandardThreadPool(name, max_workers)
        elif strategy == ThreadPoolStrategy.CONCURRENT_FUTURES:
            return ConcurrentFuturesThreadPool(name, max_workers)
        elif strategy == ThreadPoolStrategy.CUSTOM:
            return CustomThreadPool(name, max_workers)
        else:
            # Default to ConcurrentFuturesThreadPool if strategy not specified or invalid
            logger.warning(f"Unknown thread pool strategy: {strategy}, using CONCURRENT_FUTURES")
            return ConcurrentFuturesThreadPool(name, max_workers)


def create_thread_pool(pool_name: str, max_workers: int = None, strategy: ThreadPoolStrategy = None) -> ThreadPool:
    """Factory function to create a thread pool instance of the specified type."""
    with threading.RLock():
        # If pool already exists, return it
        if pool_name in _thread_pools:
            return _thread_pools[pool_name]
        
        # Get max_workers from config if not provided
        if max_workers is None:
            config = get_config()
            max_workers = config.get("threading.max_workers", MAX_PARALLEL_WORKERS)
        
        # Get strategy from config if not provided
        if strategy is None:
            config = get_config()
            strategy_str = config.get("threading.strategy", ThreadPoolStrategy.CONCURRENT_FUTURES.value)
            try:
                strategy = ThreadPoolStrategy(strategy_str)
            except ValueError:
                logger.warning(f"Invalid thread pool strategy from config: {strategy_str}, using CONCURRENT_FUTURES")
                strategy = ThreadPoolStrategy.CONCURRENT_FUTURES
        
        # Create thread pool instance
        pool = ThreadPoolFactory.create_thread_pool(pool_name, max_workers, strategy)
        
        # Store in global registry
        _thread_pools[pool_name] = pool
        
        return pool


def parallel_map(func: Callable, items: Iterable, max_workers: int = None, 
                strategy: ThreadPoolStrategy = None, pool_name: str = None) -> List:
    """Execute a function on multiple inputs in parallel and return results."""
    # Generate pool name if not provided
    if pool_name is None:
        pool_name = f"parallel-map-{id(func)}"
    
    try:
        # Create or get thread pool
        pool = create_thread_pool(pool_name, max_workers, strategy)
        
        # Use pool to map function over items
        return pool.map(func, items)
    except Exception as e:
        logger.exception(f"Error in parallel_map: {e}")
        raise


def parallel_for_each(func: Callable, items: Iterable, max_workers: int = None,
                     strategy: ThreadPoolStrategy = None, pool_name: str = None) -> None:
    """Execute a function on multiple inputs in parallel without collecting results."""
    # Generate pool name if not provided
    if pool_name is None:
        pool_name = f"parallel-for-each-{id(func)}"
    
    try:
        # Create or get thread pool
        pool = create_thread_pool(pool_name, max_workers, strategy)
        
        # Submit each item to the pool
        futures = []
        for item in items:
            future = pool.submit(func, item)
            futures.append(future)
        
        # Wait for all futures to complete
        for future in futures:
            future.result()
    except Exception as e:
        logger.exception(f"Error in parallel_for_each: {e}")
        raise


def shutdown_thread_pool(pool_name: str, wait: bool = True) -> bool:
    """Shutdown a specific thread pool by name."""
    with threading.RLock():
        if pool_name in _thread_pools:
            pool = _thread_pools.pop(pool_name)
            pool.shutdown(wait)
            return True
        return False


def shutdown_all_thread_pools(wait: bool = True) -> None:
    """Shutdown all thread pools."""
    with threading.RLock():
        pool_names = list(_thread_pools.keys())
        for name in pool_names:
            pool = _thread_pools.pop(name)
            pool.shutdown(wait)
        
        logger.info(f"All thread pools have been shut down")