"""
Error classification module for data ingestion pipeline.

This module provides functionality to classify errors based on their type, severity,
and recoverability, enabling appropriate handling strategies and self-healing actions.
It includes pattern matching for error detection, retry strategy determination, and
healing action suggestions.
"""

import inspect
import re
import typing
from typing import Dict, List, Optional, Any, Tuple, Union

from ...constants import (
    AlertSeverity,
    DEFAULT_MAX_RETRY_ATTEMPTS,
    DEFAULT_CONFIDENCE_THRESHOLD
)
from ...utils.errors.error_types import (
    ErrorCategory,
    ErrorRecoverability,
    PipelineError
)
from ...utils.logging.logger import get_logger

# Set up logger
logger = get_logger(__name__)

# Regular expression patterns for detecting transient errors
TRANSIENT_ERROR_PATTERNS = [
    re.compile(pattern) for pattern in [
        r'timeout',
        r'connection.*refused',
        r'temporarily unavailable',
        r'service unavailable',
        r'too many requests',
        r'rate limit',
        r'quota exceeded',
        r'insufficient resources',
        r'resource.*constraint',
        r'try again later'
    ]
]

# Standard exception types that are considered retryable
STANDARD_RETRYABLE_EXCEPTIONS = [
    ConnectionError,
    TimeoutError,
    OSError
]


class ErrorClassification:
    """Container for error classification details."""
    
    def __init__(
        self,
        category: ErrorCategory,
        severity: AlertSeverity,
        recoverability: ErrorRecoverability,
        details: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize the error classification.
        
        Args:
            category: The error category
            severity: The error severity level
            recoverability: The error recoverability classification
            details: Additional details about the error
            context: Contextual information about the execution environment
        """
        self.category = category
        self.severity = severity
        self.recoverability = recoverability
        self.details = details or {}
        self.context = context or {}
    
    def is_retryable(self) -> bool:
        """Check if the error can be resolved through retries.
        
        Returns:
            True if the error is retryable, False otherwise
        """
        return self.recoverability in [
            ErrorRecoverability.AUTO_RECOVERABLE,
            ErrorRecoverability.MANUAL_RECOVERABLE
        ]
    
    def is_self_healable(self) -> bool:
        """Check if the error can be automatically healed.
        
        Returns:
            True if the error can be automatically healed, False otherwise
        """
        return self.recoverability == ErrorRecoverability.AUTO_RECOVERABLE
    
    def requires_manual_intervention(self) -> bool:
        """Check if the error requires manual intervention.
        
        Returns:
            True if manual intervention is required, False otherwise
        """
        return self.recoverability in [
            ErrorRecoverability.MANUAL_RECOVERABLE,
            ErrorRecoverability.NON_RECOVERABLE
        ]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert classification to dictionary for serialization.
        
        Returns:
            Dictionary representation of the classification
        """
        return {
            'category': self.category.value,
            'severity': self.severity.value,
            'recoverability': self.recoverability.value,
            'retryable': self.is_retryable(),
            'self_healable': self.is_self_healable(),
            'details': self.details,
            'context': self.context
        }
    
    def get_retry_strategy(self) -> Dict[str, Any]:
        """Get recommended retry strategy for this error.
        
        Returns:
            Retry strategy parameters
        """
        return get_retry_strategy(self)
    
    def suggest_healing_actions(self, exception: Exception) -> List[Any]:
        """Get suggested healing actions for this error.
        
        Args:
            exception: The original exception
            
        Returns:
            List of suggested healing actions
        """
        return suggest_healing_actions(self, exception, self.context)


class HealingAction:
    """Represents a potential self-healing action for an error."""
    
    def __init__(
        self,
        action_type: str,
        description: str,
        parameters: Optional[Dict[str, Any]] = None,
        confidence: float = 0.0
    ):
        """Initialize the healing action.
        
        Args:
            action_type: Type of healing action
            description: Human-readable description of the action
            parameters: Parameters required for the action
            confidence: Confidence score (0.0-1.0) for this action
        """
        self.action_type = action_type
        self.description = description
        self.parameters = parameters or {}
        self.confidence = confidence
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert healing action to dictionary for serialization.
        
        Returns:
            Dictionary representation of the healing action
        """
        return {
            'action_type': self.action_type,
            'description': self.description,
            'parameters': self.parameters,
            'confidence': self.confidence
        }
    
    def is_actionable(self, threshold: float = DEFAULT_CONFIDENCE_THRESHOLD) -> bool:
        """Check if the healing action has sufficient confidence to be applied.
        
        Args:
            threshold: Minimum confidence threshold for action
            
        Returns:
            True if action is actionable, False otherwise
        """
        return self.confidence >= threshold


class ErrorPatternMatcher:
    """Utility class for matching error patterns in messages and exceptions."""
    
    def __init__(self):
        """Initialize the error pattern matcher."""
        # Initialize pattern dictionaries
        self.category_patterns = {}
        self.recoverability_patterns = {}
        
        # Set up default patterns for common error categories
        self._initialize_category_patterns()
        self._initialize_recoverability_patterns()
    
    def _initialize_category_patterns(self):
        """Initialize default category patterns."""
        # Define patterns for each error category
        self.category_patterns = {
            ErrorCategory.CONNECTION_ERROR: [
                re.compile(r'connection', re.IGNORECASE),
                re.compile(r'network', re.IGNORECASE),
                re.compile(r'socket', re.IGNORECASE)
            ],
            ErrorCategory.TIMEOUT_ERROR: [
                re.compile(r'timeout', re.IGNORECASE),
                re.compile(r'timed out', re.IGNORECASE)
            ],
            ErrorCategory.AUTHENTICATION_ERROR: [
                re.compile(r'authent', re.IGNORECASE),
                re.compile(r'login', re.IGNORECASE),
                re.compile(r'credential', re.IGNORECASE)
            ],
            ErrorCategory.AUTHORIZATION_ERROR: [
                re.compile(r'authoriz', re.IGNORECASE),
                re.compile(r'permission', re.IGNORECASE),
                re.compile(r'access denied', re.IGNORECASE)
            ],
            ErrorCategory.RESOURCE_ERROR: [
                re.compile(r'resource', re.IGNORECASE),
                re.compile(r'capacity', re.IGNORECASE),
                re.compile(r'quota', re.IGNORECASE),
                re.compile(r'limit exceeded', re.IGNORECASE)
            ],
            ErrorCategory.RATE_LIMIT_ERROR: [
                re.compile(r'rate limit', re.IGNORECASE),
                re.compile(r'throttl', re.IGNORECASE),
                re.compile(r'too many requests', re.IGNORECASE)
            ],
            ErrorCategory.DATA_ERROR: [
                re.compile(r'data', re.IGNORECASE),
                re.compile(r'value', re.IGNORECASE),
                re.compile(r'format', re.IGNORECASE)
            ],
            ErrorCategory.SCHEMA_ERROR: [
                re.compile(r'schema', re.IGNORECASE),
                re.compile(r'field', re.IGNORECASE),
                re.compile(r'column', re.IGNORECASE),
                re.compile(r'type', re.IGNORECASE)
            ],
            ErrorCategory.SERVICE_UNAVAILABLE: [
                re.compile(r'service unavailable', re.IGNORECASE),
                re.compile(r'server error', re.IGNORECASE),
                re.compile(r'down for maintenance', re.IGNORECASE)
            ],
            ErrorCategory.CONFIGURATION_ERROR: [
                re.compile(r'config', re.IGNORECASE),
                re.compile(r'setting', re.IGNORECASE),
                re.compile(r'parameter', re.IGNORECASE)
            ],
            ErrorCategory.DEPENDENCY_ERROR: [
                re.compile(r'dependency', re.IGNORECASE),
                re.compile(r'required module', re.IGNORECASE),
                re.compile(r'import', re.IGNORECASE)
            ]
        }
    
    def _initialize_recoverability_patterns(self):
        """Initialize default recoverability patterns."""
        # Define patterns for each recoverability type
        self.recoverability_patterns = {
            ErrorRecoverability.AUTO_RECOVERABLE: [
                re.compile(r'retry', re.IGNORECASE),
                re.compile(r'temporary', re.IGNORECASE),
                re.compile(r'transient', re.IGNORECASE),
                re.compile(r'try again', re.IGNORECASE)
            ],
            ErrorRecoverability.MANUAL_RECOVERABLE: [
                re.compile(r'manual', re.IGNORECASE),
                re.compile(r'intervention', re.IGNORECASE),
                re.compile(r'review', re.IGNORECASE)
            ],
            ErrorRecoverability.NON_RECOVERABLE: [
                re.compile(r'fatal', re.IGNORECASE),
                re.compile(r'critical', re.IGNORECASE),
                re.compile(r'permanent', re.IGNORECASE),
                re.compile(r'cannot recover', re.IGNORECASE)
            ]
        }
    
    def match_category(self, error_message: str) -> Optional[ErrorCategory]:
        """Match an error message against category patterns.
        
        Args:
            error_message: The error message to match
            
        Returns:
            Matched category or None if no match
        """
        if not error_message:
            return None
            
        for category, patterns in self.category_patterns.items():
            for pattern in patterns:
                if pattern.search(error_message):
                    return category
        
        return None
    
    def match_recoverability(self, error_message: str) -> Optional[ErrorRecoverability]:
        """Match an error message against recoverability patterns.
        
        Args:
            error_message: The error message to match
            
        Returns:
            Matched recoverability or None if no match
        """
        if not error_message:
            return None
            
        for recoverability, patterns in self.recoverability_patterns.items():
            for pattern in patterns:
                if pattern.search(error_message):
                    return recoverability
        
        return None
    
    def add_category_pattern(self, category: ErrorCategory, pattern: str) -> None:
        """Add a new pattern for error category matching.
        
        Args:
            category: The error category to associate with the pattern
            pattern: Regular expression pattern string
        """
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        
        if category not in self.category_patterns:
            self.category_patterns[category] = []
            
        self.category_patterns[category].append(compiled_pattern)
        logger.debug(f"Added new pattern for category {category.value}: {pattern}")
    
    def add_recoverability_pattern(self, recoverability: ErrorRecoverability, pattern: str) -> None:
        """Add a new pattern for error recoverability matching.
        
        Args:
            recoverability: The recoverability type to associate with pattern
            pattern: Regular expression pattern string
        """
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        
        if recoverability not in self.recoverability_patterns:
            self.recoverability_patterns[recoverability] = []
            
        self.recoverability_patterns[recoverability].append(compiled_pattern)
        logger.debug(f"Added new pattern for recoverability {recoverability.value}: {pattern}")


def classify_error(exception: Exception, context: Optional[Dict[str, Any]] = None) -> ErrorClassification:
    """Classifies an exception into appropriate error category, severity, and recoverability.
    
    Args:
        exception: The exception to classify
        context: Additional context information for classification
        
    Returns:
        Classification details for the error
    """
    context = context or {}
    logger.debug(f"Classifying error: {str(exception)}")
    
    # If it's already a PipelineError with classification, use existing classification
    if isinstance(exception, PipelineError):
        return ErrorClassification(
            category=exception.category,
            severity=exception.severity,
            recoverability=exception.recoverability,
            details=exception.get_context(),
            context=context
        )
    
    # Extract details from the exception
    error_details = extract_error_details(exception)
    error_message = str(exception)
    
    # Determine classification components
    category = determine_error_category(exception, error_message)
    severity = determine_error_severity(category, context)
    recoverability = determine_error_recoverability(category, exception, error_message, context)
    
    # Create classification
    classification = ErrorClassification(
        category=category,
        severity=severity,
        recoverability=recoverability,
        details=error_details,
        context=context
    )
    
    # Log the classification results with appropriate level
    log_level = 'error' if severity in [AlertSeverity.CRITICAL, AlertSeverity.HIGH] else 'info'
    getattr(logger, log_level)(
        f"Classified error as: category={category.value}, severity={severity.value}, "
        f"recoverability={recoverability.value}"
    )
    
    return classification


def determine_error_category(exception: Exception, error_message: str) -> ErrorCategory:
    """Determines the error category based on exception type and message.
    
    Args:
        exception: The exception to categorize
        error_message: The error message string
        
    Returns:
        Determined error category
    """
    # If it's a PipelineError, use its category
    if isinstance(exception, PipelineError):
        return exception.category
    
    # Match against common exception types
    exception_type = type(exception).__name__
    
    # Check exception type mappings
    if any(name in exception_type for name in ['Timeout', 'Deadline']):
        return ErrorCategory.TIMEOUT_ERROR
    elif any(name in exception_type for name in ['Connection', 'Socket']):
        return ErrorCategory.CONNECTION_ERROR
    elif any(name in exception_type for name in ['Auth']):
        if 'permission' in error_message.lower() or 'access' in error_message.lower():
            return ErrorCategory.AUTHORIZATION_ERROR
        else:
            return ErrorCategory.AUTHENTICATION_ERROR
    elif any(name in exception_type for name in ['Resource', 'Capacity']):
        return ErrorCategory.RESOURCE_ERROR
    elif any(name in exception_type for name in ['RateLimit', 'Throttle']):
        return ErrorCategory.RATE_LIMIT_ERROR
    elif any(name in exception_type for name in ['Data', 'Value']):
        return ErrorCategory.DATA_ERROR
    elif any(name in exception_type for name in ['Schema', 'Type', 'Field']):
        return ErrorCategory.SCHEMA_ERROR
    elif any(name in exception_type for name in ['Service', 'Server']):
        return ErrorCategory.SERVICE_UNAVAILABLE
    elif any(name in exception_type for name in ['Config', 'Parameter']):
        return ErrorCategory.CONFIGURATION_ERROR
    elif any(name in exception_type for name in ['Dependency', 'Import']):
        return ErrorCategory.DEPENDENCY_ERROR
    
    # Use pattern matcher for additional categorization based on message
    matcher = ErrorPatternMatcher()
    category = matcher.match_category(error_message)
    if category:
        return category
    
    # Default to UNKNOWN if no match
    return ErrorCategory.UNKNOWN


def determine_error_severity(category: ErrorCategory, context: Dict[str, Any]) -> AlertSeverity:
    """Determines the error severity based on category and context.
    
    Args:
        category: The error category
        context: Contextual information about the execution
        
    Returns:
        Determined alert severity
    """
    # Default severity mappings
    category_severity_map = {
        ErrorCategory.AUTHENTICATION_ERROR: AlertSeverity.HIGH,
        ErrorCategory.AUTHORIZATION_ERROR: AlertSeverity.HIGH,
        ErrorCategory.CONFIGURATION_ERROR: AlertSeverity.HIGH,
        ErrorCategory.DEPENDENCY_ERROR: AlertSeverity.HIGH,
        ErrorCategory.CONNECTION_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.TIMEOUT_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.RATE_LIMIT_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.RESOURCE_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.SERVICE_UNAVAILABLE: AlertSeverity.MEDIUM,
        ErrorCategory.DATA_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.SCHEMA_ERROR: AlertSeverity.MEDIUM,
        ErrorCategory.VALIDATION_ERROR: AlertSeverity.LOW,
        ErrorCategory.UNKNOWN: AlertSeverity.MEDIUM
    }
    
    # Get default severity for category
    severity = category_severity_map.get(category, AlertSeverity.MEDIUM)
    
    # Adjust severity based on context
    # If this is a critical data source or component, increase severity
    is_critical = context.get('is_critical', False)
    if is_critical:
        if severity == AlertSeverity.LOW:
            severity = AlertSeverity.MEDIUM
        elif severity == AlertSeverity.MEDIUM:
            severity = AlertSeverity.HIGH
    
    # If retry count is high, increase severity
    retry_count = context.get('retry_count', 0)
    if retry_count >= DEFAULT_MAX_RETRY_ATTEMPTS:
        if severity == AlertSeverity.LOW:
            severity = AlertSeverity.MEDIUM
        elif severity == AlertSeverity.MEDIUM:
            severity = AlertSeverity.HIGH
    
    return severity


def determine_error_recoverability(
    category: ErrorCategory,
    exception: Exception,
    error_message: str,
    context: Dict[str, Any]
) -> ErrorRecoverability:
    """Determines if an error is recoverable and how it should be handled.
    
    Args:
        category: The error category
        exception: The original exception
        error_message: The error message string
        context: Contextual information about the execution
        
    Returns:
        Determined recoverability classification
    """
    # If it's a PipelineError, use its recoverability
    if isinstance(exception, PipelineError):
        return exception.recoverability
    
    # Check if it's a transient error
    if is_transient_error(exception, error_message):
        return ErrorRecoverability.AUTO_RECOVERABLE
    
    # Default recoverability mappings
    category_recoverability_map = {
        ErrorCategory.CONNECTION_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.TIMEOUT_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.RATE_LIMIT_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.SERVICE_UNAVAILABLE: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.RESOURCE_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.DATA_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.SCHEMA_ERROR: ErrorRecoverability.MANUAL_RECOVERABLE,
        ErrorCategory.AUTHENTICATION_ERROR: ErrorRecoverability.MANUAL_RECOVERABLE,
        ErrorCategory.AUTHORIZATION_ERROR: ErrorRecoverability.MANUAL_RECOVERABLE,
        ErrorCategory.CONFIGURATION_ERROR: ErrorRecoverability.MANUAL_RECOVERABLE,
        ErrorCategory.DEPENDENCY_ERROR: ErrorRecoverability.MANUAL_RECOVERABLE,
        ErrorCategory.VALIDATION_ERROR: ErrorRecoverability.AUTO_RECOVERABLE,
        ErrorCategory.UNKNOWN: ErrorRecoverability.MANUAL_RECOVERABLE
    }
    
    # Get default recoverability for category
    recoverability = category_recoverability_map.get(category, ErrorRecoverability.MANUAL_RECOVERABLE)
    
    # Use pattern matcher for additional recovery classification based on message
    matcher = ErrorPatternMatcher()
    pattern_recoverability = matcher.match_recoverability(error_message)
    if pattern_recoverability:
        recoverability = pattern_recoverability
    
    # If retry count is too high, escalate AUTO_RECOVERABLE to MANUAL_RECOVERABLE
    retry_count = context.get('retry_count', 0)
    if retry_count >= DEFAULT_MAX_RETRY_ATTEMPTS and recoverability == ErrorRecoverability.AUTO_RECOVERABLE:
        recoverability = ErrorRecoverability.MANUAL_RECOVERABLE
        logger.info(f"Changed recoverability to MANUAL after {retry_count} retries")
    
    return recoverability


def is_transient_error(exception: Exception, error_message: str) -> bool:
    """Determines if an error is transient and can be resolved by retrying.
    
    Args:
        exception: The exception to check
        error_message: The error message string
        
    Returns:
        True if error is transient, False otherwise
    """
    # Check if exception type is in standard retryable exceptions list
    if any(isinstance(exception, exc_type) for exc_type in STANDARD_RETRYABLE_EXCEPTIONS):
        return True
    
    # Check error message against transient error patterns
    for pattern in TRANSIENT_ERROR_PATTERNS:
        if pattern.search(error_message.lower()):
            return True
    
    # Check for specific error codes that are typically transient
    error_code = getattr(exception, 'code', getattr(exception, 'status_code', None))
    if error_code is not None:
        # Status codes that typically indicate transient issues (4xx and 5xx)
        transient_codes = [408, 429, 500, 502, 503, 504]
        if error_code in transient_codes:
            return True
    
    return False


def get_retry_strategy(error_classification: ErrorClassification) -> Dict[str, Any]:
    """Determines the appropriate retry strategy for an error.
    
    Args:
        error_classification: Classification details for the error
        
    Returns:
        Dictionary with retry strategy parameters
    """
    # Check if error is retryable
    if not error_classification.is_retryable():
        return {
            'retryable': False,
            'max_retries': 0,
            'backoff_factor': 0,
            'max_delay': 0
        }
    
    # Determine max retries based on category and severity
    max_retries = DEFAULT_MAX_RETRY_ATTEMPTS
    if error_classification.severity == AlertSeverity.CRITICAL:
        max_retries = 1
    elif error_classification.severity == AlertSeverity.HIGH:
        max_retries = 2
    
    # Determine backoff factor based on error type
    # More severe errors should have longer backoff
    backoff_factor = 1.0  # Default
    if error_classification.category == ErrorCategory.RATE_LIMIT_ERROR:
        backoff_factor = 2.0
    elif error_classification.category == ErrorCategory.SERVICE_UNAVAILABLE:
        backoff_factor = 1.5
    elif error_classification.category == ErrorCategory.RESOURCE_ERROR:
        backoff_factor = 1.5
    
    # Determine max delay based on error category
    max_delay = 60  # Default max delay of 60 seconds
    if error_classification.category == ErrorCategory.RATE_LIMIT_ERROR:
        max_delay = 300  # 5 minutes for rate limits
    elif error_classification.category == ErrorCategory.SERVICE_UNAVAILABLE:
        max_delay = 600  # 10 minutes for service unavailability
    
    # Return retry strategy
    return {
        'retryable': True,
        'max_retries': max_retries,
        'backoff_factor': backoff_factor,
        'max_delay': max_delay
    }


def extract_error_details(exception: Exception) -> Dict[str, Any]:
    """Extracts detailed information from an exception object.
    
    Args:
        exception: The exception to extract details from
        
    Returns:
        Dictionary with error details
    """
    # Start with basic details
    details = {
        'type': type(exception).__name__,
        'message': str(exception)
    }
    
    # Extract error code if available
    error_code = getattr(exception, 'code', getattr(exception, 'status_code', None))
    if error_code is not None:
        details['code'] = error_code
    
    # Extract standard attributes
    for attr in ['errno', 'reason', 'strerror', 'response', 'request', 'url']:
        if hasattr(exception, attr):
            attr_value = getattr(exception, attr)
            # Convert complex objects to string if needed
            if not isinstance(attr_value, (str, int, float, bool, type(None))):
                try:
                    attr_value = str(attr_value)
                except Exception:
                    attr_value = f"<unprintable {type(attr_value).__name__}>"
            details[attr] = attr_value
    
    # Extract additional context from PipelineError
    if isinstance(exception, PipelineError) and hasattr(exception, 'context'):
        ctx = exception.get_context()
        if ctx:
            details['context'] = ctx
    
    return details


def suggest_healing_actions(
    error_classification: ErrorClassification,
    exception: Exception,
    context: Dict[str, Any]
) -> List[HealingAction]:
    """Suggests potential self-healing actions for an error.
    
    Args:
        error_classification: Classification details for the error
        exception: The original exception
        context: Contextual information about the execution
        
    Returns:
        List of suggested healing actions with confidence scores
    """
    actions = []
    confidence_threshold = context.get('confidence_threshold', DEFAULT_CONFIDENCE_THRESHOLD)
    
    # Only suggest actions for self-healable errors
    if not error_classification.is_self_healable():
        return []
    
    # Suggest actions based on error category
    if error_classification.category == ErrorCategory.CONNECTION_ERROR:
        actions.append(HealingAction(
            action_type="retry",
            description="Retry the connection after a short delay",
            parameters={"delay_seconds": 5},
            confidence=0.9
        ))
        actions.append(HealingAction(
            action_type="circuit_breaker",
            description="Implement circuit breaker to prevent cascading failures",
            parameters={"failure_threshold": 3, "reset_timeout": 30},
            confidence=0.7
        ))
    
    elif error_classification.category == ErrorCategory.TIMEOUT_ERROR:
        actions.append(HealingAction(
            action_type="retry",
            description="Retry the operation with increased timeout",
            parameters={"timeout_seconds": context.get("timeout", 30) * 1.5},
            confidence=0.85
        ))
        actions.append(HealingAction(
            action_type="batch_size_reduction",
            description="Reduce batch size to process smaller chunks",
            parameters={"reduction_factor": 0.5},
            confidence=0.7
        ))
    
    elif error_classification.category == ErrorCategory.RATE_LIMIT_ERROR:
        actions.append(HealingAction(
            action_type="backoff",
            description="Wait and retry with exponential backoff",
            parameters={"initial_delay": 10, "backoff_factor": 2},
            confidence=0.95
        ))
        actions.append(HealingAction(
            action_type="rate_limit_adjust",
            description="Adjust request rate to stay under limits",
            parameters={"rate_reduction_factor": 0.5},
            confidence=0.85
        ))
    
    elif error_classification.category == ErrorCategory.DATA_ERROR:
        actions.append(HealingAction(
            action_type="data_cleansing",
            description="Attempt to clean data by removing problematic records",
            parameters={"error_pattern": str(exception)},
            confidence=0.75
        ))
        actions.append(HealingAction(
            action_type="value_transformation",
            description="Transform problematic values to acceptable format",
            parameters={"transformation_type": "format_correction"},
            confidence=0.65
        ))
    
    elif error_classification.category == ErrorCategory.SCHEMA_ERROR:
        actions.append(HealingAction(
            action_type="schema_adaptation",
            description="Adapt to schema changes by adjusting expectations",
            parameters={"adaptation_type": "field_mapping"},
            confidence=0.7
        ))
        actions.append(HealingAction(
            action_type="type_conversion",
            description="Convert data types to match expected schema",
            parameters={"strict_mode": False},
            confidence=0.65
        ))
    
    elif error_classification.category == ErrorCategory.RESOURCE_ERROR:
        actions.append(HealingAction(
            action_type="resource_scaling",
            description="Scale up resources to handle the workload",
            parameters={"scale_factor": 1.5},
            confidence=0.8
        ))
        actions.append(HealingAction(
            action_type="workload_distribution",
            description="Distribute workload across more instances",
            parameters={"instance_count": 3},
            confidence=0.7
        ))
    
    elif error_classification.category == ErrorCategory.SERVICE_UNAVAILABLE:
        actions.append(HealingAction(
            action_type="service_failover",
            description="Fail over to backup service if available",
            parameters={"failover_target": context.get("failover_target")},
            confidence=0.8 if context.get("failover_target") else 0.4
        ))
        actions.append(HealingAction(
            action_type="delayed_retry",
            description="Retry after longer delay to allow service recovery",
            parameters={"delay_seconds": 60},
            confidence=0.75
        ))
    
    # For any error type, retry is a reasonable default action if retryable
    if error_classification.is_retryable() and not any(a.action_type == "retry" for a in actions):
        retry_strategy = get_retry_strategy(error_classification)
        actions.append(HealingAction(
            action_type="retry",
            description="Retry the operation",
            parameters=retry_strategy,
            confidence=0.6
        ))
    
    # Filter actions by confidence threshold
    actionable_actions = [action for action in actions if action.confidence >= confidence_threshold]
    
    # Sort by confidence (highest first)
    actionable_actions.sort(key=lambda a: a.confidence, reverse=True)
    
    return actionable_actions