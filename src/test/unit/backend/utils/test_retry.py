import pytest
import unittest.mock as mock
import time

from src.backend.utils.retry.retry_decorator import (
    retry,
    retry_with_config,
    RetryConfig,
    should_retry_exception,
    get_error_category
)
from src.backend.utils.retry.backoff_strategy import (
    BackoffStrategy,
    ExponentialBackoffStrategy,
    LinearBackoffStrategy,
    ConstantBackoffStrategy,
    get_backoff_strategy,
    get_backoff_strategy_for_error
)
from src.backend.utils.retry.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    get_circuit_breaker,
    reset_all_circuit_breakers
)
from src.backend.utils.errors.error_types import (
    PipelineError,
    ErrorCategory,
    CircuitBreakerOpenError
)
from src.backend.config import get_config


class TestRetryFixture:
    """Fixture class for retry tests with common setup and teardown"""

    def setup_method(self, function):
        """Setup method called before each test"""
        # Reset any global state
        reset_all_circuit_breakers()
        
        # Patch time.sleep to avoid actual delays during tests
        self.sleep_patcher = mock.patch('time.sleep')
        self.mock_sleep = self.sleep_patcher.start()

    def teardown_method(self, function):
        """Teardown method called after each test"""
        # Reset circuit breakers
        reset_all_circuit_breakers()
        
        # Stop all patches
        self.sleep_patcher.stop()

    def create_failing_function(self, fail_count, exception_type=Exception, exception_message="Test failure"):
        """Helper to create a function that fails a specified number of times"""
        counter = {'calls': 0}
        
        def failing_function():
            counter['calls'] += 1
            if counter['calls'] <= fail_count:
                raise exception_type(exception_message)
            return "Success after failures"
        
        return failing_function


def test_retry_successful_execution():
    """Tests that a function decorated with retry executes successfully without retries when no exceptions occur"""
    # Create a mock function that returns a value without raising exceptions
    mock_func = mock.Mock(return_value="success")
    
    # Apply the retry decorator to the mock function
    decorated_func = retry()(mock_func)
    
    # Call the decorated function
    result = decorated_func()
    
    # Verify that the mock function was called exactly once
    assert mock_func.call_count == 1
    
    # Verify that the correct return value is received
    assert result == "success"


def test_retry_with_temporary_failure():
    """Tests that a function is retried when it fails temporarily before succeeding"""
    # Create a mock function that fails on first call but succeeds on second call
    mock_func = mock.Mock(side_effect=[Exception("temporary failure"), "success"])
    
    # Apply the retry decorator to the mock function
    decorated_func = retry()(mock_func)
    
    # Call the decorated function
    result = decorated_func()
    
    # Verify that the mock function was called exactly twice
    assert mock_func.call_count == 2
    
    # Verify that the correct return value is received
    assert result == "success"


def test_retry_max_attempts_exceeded():
    """Tests that retry stops after max_attempts and raises the last exception"""
    # Create a mock function that always raises an exception
    exception = Exception("persistent failure")
    mock_func = mock.Mock(side_effect=exception)
    
    # Apply the retry decorator with a specific max_attempts value
    max_attempts = 3
    decorated_func = retry(max_attempts=max_attempts)(mock_func)
    
    # Call the decorated function and expect it to raise an exception
    with pytest.raises(Exception) as excinfo:
        decorated_func()
    
    # Verify that the mock function was called exactly max_attempts times
    assert mock_func.call_count == max_attempts
    assert str(excinfo.value) == str(exception)


def test_retry_with_specific_exceptions():
    """Tests that retry only retries on specified exception types"""
    # Create a mock function that raises different types of exceptions
    value_error = ValueError("value error")
    type_error = TypeError("type error")
    mock_func = mock.Mock(side_effect=[value_error, "success"])
    mock_func2 = mock.Mock(side_effect=[type_error, "success"])
    
    # Apply the retry decorator with specific exceptions_to_retry
    decorated_func = retry(exceptions_to_retry=[ValueError])(mock_func)
    decorated_func2 = retry(exceptions_to_retry=[ValueError])(mock_func2)
    
    # Call the decorated function with an exception in the retry list
    result = decorated_func()
    
    # Verify retry behavior for included exception types
    assert mock_func.call_count == 2
    assert result == "success"
    
    # Call the decorated function with an exception not in the retry list
    with pytest.raises(TypeError):
        decorated_func2()
    
    # Verify that exceptions not in the list are not retried
    assert mock_func2.call_count == 1


def test_retry_with_exceptions_to_ignore():
    """Tests that retry does not retry on exceptions specified in exceptions_to_ignore"""
    # Create a mock function that raises an exception
    value_error = ValueError("value error")
    mock_func = mock.Mock(side_effect=[value_error, "success"])
    
    # Apply the retry decorator with specific exceptions_to_ignore
    decorated_func = retry(exceptions_to_ignore=[ValueError])(mock_func)
    
    # Call the decorated function with an exception in the ignore list
    with pytest.raises(ValueError):
        decorated_func()
    
    # Verify that the function is not retried for ignored exceptions
    assert mock_func.call_count == 1


def test_retry_with_custom_backoff_strategy():
    """Tests retry with different backoff strategy implementations"""
    # Create a mock function that always fails
    mock_func = mock.Mock(side_effect=Exception("failure"))
    
    # Create a mock backoff strategy
    mock_backoff = mock.Mock(spec=BackoffStrategy)
    mock_backoff.get_delay.return_value = 1.0
    
    # Apply the retry decorator with the mock backoff strategy
    max_attempts = 3
    decorated_func = retry(max_attempts=max_attempts, backoff_strategy=mock_backoff)(mock_func)
    
    # Call the decorated function and expect it to fail after max attempts
    with pytest.raises(Exception):
        decorated_func()
    
    # Verify that the backoff strategy's wait method was called correctly
    assert mock_backoff.get_delay.call_count == max_attempts - 1
    mock_backoff.get_delay.assert_has_calls([mock.call(1), mock.call(2)])


def test_retry_with_string_backoff_strategy():
    """Tests retry with backoff strategy specified as a string"""
    # Mock the get_backoff_strategy function
    with mock.patch('src.backend.utils.retry.retry_decorator.get_backoff_strategy') as mock_get_strategy:
        mock_strategy = mock.Mock(spec=BackoffStrategy)
        mock_strategy.get_delay.return_value = 1.0
        mock_get_strategy.return_value = mock_strategy
        
        # Create a mock function that always fails
        mock_func = mock.Mock(side_effect=Exception("failure"))
        
        # Apply the retry decorator with a string backoff strategy name
        max_attempts = 3
        decorated_func = retry(max_attempts=max_attempts, backoff_strategy="exponential")(mock_func)
        
        # Call the decorated function and expect it to fail after max attempts
        with pytest.raises(Exception):
            decorated_func()
        
        # Verify that get_backoff_strategy was called with the correct strategy name
        mock_get_strategy.assert_called_once_with("exponential")
        assert mock_strategy.get_delay.call_count == max_attempts - 1


def test_retry_with_on_retry_callback():
    """Tests that on_retry callback is called correctly during retries"""
    # Create a mock on_retry callback function
    mock_on_retry = mock.Mock()
    
    # Create a mock function that fails temporarily before succeeding
    mock_func = mock.Mock(side_effect=[Exception("failure"), "success"])
    
    # Apply the retry decorator with the on_retry callback
    decorated_func = retry(on_retry=mock_on_retry)(mock_func)
    
    # Call the decorated function
    result = decorated_func()
    
    # Verify that the on_retry callback was called with correct parameters
    assert mock_on_retry.call_count == 1
    # Check callback was called with correct arguments
    args, kwargs = mock_on_retry.call_args
    assert args[0] == mock_func  # function
    assert isinstance(args[1], Exception)  # exception
    assert args[2] == 1  # attempt
    assert args[3] > 1  # max_attempts
    assert args[4] > 0  # delay


def test_retry_with_on_permanent_failure_callback():
    """Tests that on_permanent_failure callback is called when max attempts are exceeded"""
    # Create a mock on_permanent_failure callback function
    mock_on_permanent_failure = mock.Mock()
    
    # Create a mock function that always fails
    exception = Exception("permanent failure")
    mock_func = mock.Mock(side_effect=exception)
    
    # Apply the retry decorator with the on_permanent_failure callback
    max_attempts = 3
    decorated_func = retry(
        max_attempts=max_attempts,
        on_permanent_failure=mock_on_permanent_failure
    )(mock_func)
    
    # Call the decorated function and expect it to fail
    with pytest.raises(Exception):
        decorated_func()
    
    # Verify that the on_permanent_failure callback was called with correct parameters
    assert mock_on_permanent_failure.call_count == 1
    # Check callback was called with correct arguments
    args, kwargs = mock_on_permanent_failure.call_args
    assert args[0] == mock_func  # function
    assert args[1] == exception  # exception
    assert args[2] == max_attempts  # attempt
    assert args[3] == max_attempts  # max_attempts


def test_retry_with_retry_condition():
    """Tests that retry_condition callback determines whether to retry"""
    # Create a mock retry_condition function that returns True/False based on criteria
    def retry_condition(exception):
        return "retry" in str(exception)
    
    # Create a mock function that raises exceptions
    retryable_exception = Exception("should retry this")
    non_retryable_exception = Exception("should not retry")
    
    mock_func1 = mock.Mock(side_effect=[retryable_exception, "success"])
    mock_func2 = mock.Mock(side_effect=non_retryable_exception)
    
    # Apply the retry decorator with the retry_condition callback
    decorated_func1 = retry(retry_condition=retry_condition)(mock_func1)
    decorated_func2 = retry(retry_condition=retry_condition)(mock_func2)
    
    # Call the decorated function with conditions that should retry
    result = decorated_func1()
    assert result == "success"
    assert mock_func1.call_count == 2
    
    # Call the decorated function with conditions that shouldn't retry
    with pytest.raises(Exception) as excinfo:
        decorated_func2()
    assert "should not retry" in str(excinfo.value)
    assert mock_func2.call_count == 1


def test_retry_with_circuit_breaker():
    """Tests integration between retry decorator and circuit breaker"""
    fixture = TestRetryFixture()
    fixture.setup_method(None)
    
    try:
        # Mock the circuit breaker functionality
        with mock.patch('src.backend.utils.retry.retry_decorator.get_circuit_breaker') as mock_get_cb:
            mock_circuit_breaker = mock.Mock()
            mock_circuit_breaker.allow_request.side_effect = [True, True, False]
            mock_circuit_breaker.on_success.return_value = None
            mock_circuit_breaker.on_failure.return_value = None
            mock_get_cb.return_value = mock_circuit_breaker
            
            # Create a mock function that always fails
            mock_func = mock.Mock(side_effect=Exception("failure"))
            
            # Apply the retry decorator with circuit breaker enabled
            decorated_func = retry(
                max_attempts=5,
                use_circuit_breaker=True,
                circuit_breaker_service="test_service"
            )(mock_func)
            
            # First call - circuit allows request but function fails
            with pytest.raises(Exception):
                decorated_func()
            
            assert mock_circuit_breaker.allow_request.call_count == 1
            assert mock_circuit_breaker.on_failure.call_count == 1
            
            # Reset mocks for next test
            mock_circuit_breaker.reset_mock()
            mock_circuit_breaker.allow_request.side_effect = [False]
            
            # Second call - circuit is open
            with pytest.raises(CircuitBreakerOpenError):
                decorated_func()
            
            # Verify CircuitBreakerOpenError is raised when circuit is open
            assert mock_circuit_breaker.allow_request.call_count == 1
            assert mock_circuit_breaker.on_failure.call_count == 0
    finally:
        fixture.teardown_method(None)


def test_retry_with_config():
    """Tests the retry_with_config factory function"""
    # Mock the config.get_config function to return test configuration
    mock_config = mock.MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "test_retry.max_attempts": 2,
        "test_retry.backoff_strategy": "linear",
        "test_retry.base_delay": 0.1,
        "test_retry.max_delay": 1.0,
        "test_retry.jitter_factor": 0.2,
        "test_retry.use_circuit_breaker": False,
    }.get(key, default)
    
    with mock.patch('src.backend.utils.retry.retry_decorator.get_config', return_value=mock_config):
        # Create a mock function that fails temporarily
        mock_func = mock.Mock(side_effect=[Exception("failure"), "success"])
        
        # Apply the retry_with_config decorator with a config section
        decorated_func = retry_with_config("test_retry")(mock_func)
        
        # Call the decorated function
        result = decorated_func()
        
        # Verify that retry behavior matches the configuration
        assert mock_func.call_count == 2
        assert result == "success"


def test_retry_config_class():
    """Tests the RetryConfig class functionality"""
    # Create RetryConfig instances with different parameters
    default_config = RetryConfig()
    assert default_config.max_attempts > 0
    assert default_config.backoff_strategy == "exponential"
    
    custom_config = RetryConfig(
        max_attempts=5,
        backoff_strategy="linear",
        base_delay=2.0,
        max_delay=10.0,
        jitter_factor=0.3,
        exceptions_to_retry=[ValueError, TypeError],
        exceptions_to_ignore=[KeyError],
        use_circuit_breaker=True,
        circuit_breaker_service="test_service"
    )
    
    assert custom_config.max_attempts == 5
    assert custom_config.backoff_strategy == "linear"
    assert custom_config.base_delay == 2.0
    assert custom_config.max_delay == 10.0
    assert custom_config.jitter_factor == 0.3
    assert ValueError in custom_config.exceptions_to_retry
    assert KeyError in custom_config.exceptions_to_ignore
    assert custom_config.use_circuit_breaker is True
    assert custom_config.circuit_breaker_service == "test_service"
    
    # Test the from_config class method with mock configuration
    mock_config = mock.MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "test_retry.max_attempts": 3,
        "test_retry.backoff_strategy": "constant",
        "test_retry.base_delay": 0.5,
    }.get(key, default)
    
    with mock.patch('src.backend.utils.retry.retry_decorator.get_config', return_value=mock_config):
        config_from_app = RetryConfig.from_config("test_retry")
        assert config_from_app.max_attempts == 3
        assert config_from_app.backoff_strategy == "constant"
        assert config_from_app.base_delay == 0.5
    
    # Test the get_retry_decorator method
    with mock.patch('src.backend.utils.retry.retry_decorator.retry') as mock_retry:
        mock_retry.return_value = "decorated_function"
        decorator = custom_config.get_retry_decorator()
        assert decorator == "decorated_function"
        mock_retry.assert_called_once_with(
            max_attempts=5,
            backoff_strategy="linear",
            exceptions_to_retry=[ValueError, TypeError],
            exceptions_to_ignore=[KeyError],
            use_circuit_breaker=True,
            circuit_breaker_service="test_service"
        )


def test_should_retry_exception():
    """Tests the should_retry_exception utility function"""
    # Create various exception instances
    standard_exception = Exception("standard error")
    value_error = ValueError("value error")
    key_error = KeyError("key error")
    
    # Create PipelineError instances with different retry settings
    retryable_pipeline_error = PipelineError("retryable", retryable=True)
    non_retryable_pipeline_error = PipelineError("non-retryable", retryable=False)
    
    # Test with default retry exceptions (Exception)
    assert should_retry_exception(standard_exception, [Exception], []) is True
    assert should_retry_exception(value_error, [Exception], []) is True
    
    # Test with specific retry exceptions
    assert should_retry_exception(value_error, [ValueError], []) is True
    assert should_retry_exception(key_error, [ValueError], []) is False
    
    # Test with exceptions to ignore
    assert should_retry_exception(value_error, [Exception], [ValueError]) is False
    assert should_retry_exception(key_error, [Exception], [ValueError]) is True
    
    # Test with PipelineError instances
    assert should_retry_exception(retryable_pipeline_error, [PipelineError], []) is True
    assert should_retry_exception(non_retryable_pipeline_error, [PipelineError], []) is False
    
    # Test with custom retry_condition function
    def custom_condition(exc):
        return "value" in str(exc)
    
    assert should_retry_exception(value_error, [Exception], [], custom_condition) is True
    assert should_retry_exception(key_error, [Exception], [], custom_condition) is False


def test_get_error_category():
    """Tests the get_error_category utility function"""
    # Create PipelineError instances with different categories
    connection_error = PipelineError("connection failed", category=ErrorCategory.CONNECTION_ERROR)
    timeout_error = PipelineError("timeout occurred", category=ErrorCategory.TIMEOUT_ERROR)
    
    # Test get_error_category with PipelineError instances
    assert get_error_category(connection_error) == ErrorCategory.CONNECTION_ERROR
    assert get_error_category(timeout_error) == ErrorCategory.TIMEOUT_ERROR
    
    # Test with non-PipelineError exceptions
    standard_exception = Exception("standard error")
    value_error = ValueError("value error")
    
    # Verify None is returned for non-PipelineError exceptions
    assert get_error_category(standard_exception) is None
    assert get_error_category(value_error) is None


def test_retry_with_pipeline_error():
    """Tests retry behavior with PipelineError instances"""
    fixture = TestRetryFixture()
    fixture.setup_method(None)
    
    try:
        # Create mock functions that raise different PipelineError types
        retryable_error = PipelineError("retryable error", category=ErrorCategory.CONNECTION_ERROR, retryable=True)
        non_retryable_error = PipelineError("non-retryable error", category=ErrorCategory.CONFIGURATION_ERROR, retryable=False)
        
        mock_retryable_func = mock.Mock(side_effect=[retryable_error, "success"])
        mock_non_retryable_func = mock.Mock(side_effect=non_retryable_error)
        
        # Apply retry decorator to these functions
        decorated_retryable = retry()(mock_retryable_func)
        decorated_non_retryable = retry()(mock_non_retryable_func)
        
        # Test with retryable PipelineErrors
        result = decorated_retryable()
        assert result == "success"
        assert mock_retryable_func.call_count == 2
        
        # Verify retry behavior based on is_retryable() method
        mock_retryable_func.reset_mock()
        mock_retryable_func.side_effect = [retryable_error, retryable_error, "success"]
        result = decorated_retryable()
        assert result == "success"
        assert mock_retryable_func.call_count == 3
        
        # Test with non-retryable PipelineErrors
        with pytest.raises(PipelineError) as excinfo:
            decorated_non_retryable()
        
        # Verify no retry for non-retryable errors
        assert mock_non_retryable_func.call_count == 1
        assert str(excinfo.value) == "non-retryable error"
    finally:
        fixture.teardown_method(None)