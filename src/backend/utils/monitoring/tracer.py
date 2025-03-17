"""
Implements distributed tracing functionality for the self-healing data pipeline.

This module provides a comprehensive tracing framework that enables tracking request
flows across distributed services and components. It supports both Google Cloud Trace
and OpenTelemetry standards, allowing for flexible integration with monitoring tools.

Key features:
- Creation and management of traces and spans for performance measurement
- Propagation of trace context across service boundaries
- Integration with Google Cloud Trace for visualization and analysis
- OpenTelemetry support for standardized instrumentation
- Decorators and context managers for easy code instrumentation
- Correlation with logging system via correlation IDs

The module can be configured via application settings to control tracing behavior,
sampling rates, and export destinations.
"""

import uuid
import time
import datetime
import functools
import contextlib
import threading
import typing
from typing import Dict, List, Optional, Any, Tuple, Callable, Union

from google.cloud.trace_v2 import TraceServiceClient
from google.cloud.trace_v2.types import Span as TraceSpanProto
from google.cloud.trace_v2.types import Trace, TruncatableString, AttributeValue
from google.cloud.trace_v2.types import Attributes, Links, Status, TimeEvents

from opentelemetry.trace import SpanContext, TracerProvider, Tracer as OTelTracer
from opentelemetry.trace import SpanKind, get_current_span, set_span_in_context
from opentelemetry.sdk.trace import Tracer as SDKTracer, TracerProvider as SDKTracerProvider
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.propagate import extract, inject, set_global_textmap
from opentelemetry.propagators.cloud_trace_propagator import CloudTraceFormatPropagator

from ..logging.logger import get_logger, get_correlation_id, set_correlation_id
from ..auth.gcp_auth import get_default_credentials, get_project_id
from ..retry.retry_decorator import retry
from ...config import get_config
from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS

# Initialize logger
logger = get_logger(__name__)

# Configuration keys
TRACING_ENABLED_CONFIG_KEY = "monitoring.tracing.enabled"
TRACING_EXPORTER_CONFIG_KEY = "monitoring.tracing.exporter"
TRACING_SAMPLING_RATE_CONFIG_KEY = "monitoring.tracing.sampling_rate"

# Default values
DEFAULT_TRACING_ENABLED = True
DEFAULT_TRACING_EXPORTER = "cloud_trace"
DEFAULT_SAMPLING_RATE = 0.1

# Standard trace header name for GCP
TRACE_HEADER_NAME = "X-Cloud-Trace-Context"

# Thread-local storage for trace context
_thread_local = threading.local()

# Singleton instances
_trace_client = None
_opentelemetry_tracer = None


def get_tracing_config() -> Dict[str, Any]:
    """Retrieves tracing configuration settings from application config.
    
    Returns:
        Dictionary containing tracing configuration settings with defaults applied
    """
    config = get_config()
    
    # Get tracing configuration with defaults
    enabled = config.get(TRACING_ENABLED_CONFIG_KEY, DEFAULT_TRACING_ENABLED)
    exporter = config.get(TRACING_EXPORTER_CONFIG_KEY, DEFAULT_TRACING_EXPORTER)
    sampling_rate = float(config.get(TRACING_SAMPLING_RATE_CONFIG_KEY, DEFAULT_SAMPLING_RATE))
    
    return {
        "enabled": enabled,
        "exporter": exporter,
        "sampling_rate": sampling_rate
    }


def is_tracing_enabled() -> bool:
    """Checks if tracing is enabled in the configuration.
    
    Returns:
        True if tracing is enabled, False otherwise
    """
    tracing_config = get_tracing_config()
    return tracing_config["enabled"]


def generate_trace_id() -> str:
    """Generates a unique trace ID.
    
    Returns:
        Unique trace ID as a 32-character hexadecimal string
    """
    # Generate a UUID and format it as a 32-character hex string without dashes
    return uuid.uuid4().hex.lower()


def generate_span_id() -> str:
    """Generates a unique span ID.
    
    Returns:
        Unique span ID as a 16-character hexadecimal string
    """
    # Use first 16 characters (64 bits) of a UUID hex
    return uuid.uuid4().hex[:16].lower()


def parse_trace_context(header_value: Optional[str]) -> Optional[Tuple[str, str, str]]:
    """Parses a trace context header value into trace ID and span ID.
    
    Args:
        header_value: Trace context header value in the format 'TRACE_ID/SPAN_ID;o=TRACE_OPTIONS'
        
    Returns:
        Tuple of (trace_id, span_id, trace_options) or None if the header is invalid
    """
    if not header_value:
        return None
    
    try:
        parts = header_value.split('/')
        trace_id = parts[0]
        
        if len(parts) < 2:
            return trace_id, None, None
        
        span_and_options = parts[1].split(';')
        span_id = span_and_options[0]
        
        trace_options = None
        if len(span_and_options) > 1 and span_and_options[1].startswith('o='):
            trace_options = span_and_options[1][2:]
        
        return trace_id, span_id, trace_options
    except Exception as e:
        logger.warning(f"Failed to parse trace context header: {header_value}, error: {str(e)}")
        return None


def format_trace_context(trace_id: str, span_id: Optional[str] = None, trace_options: Optional[str] = None) -> str:
    """Formats trace ID and span ID into a trace context header value.
    
    Args:
        trace_id: Trace ID
        span_id: Span ID (optional)
        trace_options: Trace options (optional)
        
    Returns:
        Formatted trace context header value
    """
    if not trace_id:
        return ""
    
    result = trace_id
    
    if span_id:
        result += f"/{span_id}"
        if trace_options:
            result += f";o={trace_options}"
    
    return result


def inject_trace_context(headers: Dict[str, str], trace_id: Optional[str] = None, 
                         span_id: Optional[str] = None, trace_options: Optional[str] = None) -> Dict[str, str]:
    """Injects trace context into HTTP headers.
    
    Args:
        headers: HTTP headers dictionary to inject trace context into
        trace_id: Trace ID (optional, will use current context if not provided)
        span_id: Span ID (optional, will use current context if not provided)
        trace_options: Trace options (optional)
        
    Returns:
        Updated headers dictionary with trace context
    """
    if not is_tracing_enabled():
        return headers
    
    # If trace_id not provided, try to get from current context
    if not trace_id:
        current_context = extract_current_trace_data()
        trace_id = current_context.get('trace_id')
        span_id = current_context.get('span_id')
        trace_options = current_context.get('trace_options')
    
    if trace_id:
        header_value = format_trace_context(trace_id, span_id, trace_options)
        headers[TRACE_HEADER_NAME] = header_value
    
    return headers


def extract_current_trace_data() -> Dict[str, str]:
    """Extracts current trace context data from thread-local storage.
    
    Returns:
        Dictionary with current trace data or empty dict if not available
    """
    if not is_tracing_enabled():
        return {}
    
    try:
        return getattr(_thread_local, 'trace_context', {})
    except Exception:
        return {}


class TraceClient:
    """Client for interacting with Google Cloud Trace API.
    
    This class provides a simplified interface for creating and managing traces
    and spans in Google Cloud Trace, handling the details of API interactions.
    """
    
    def __init__(self, project_id: Optional[str] = None, config_override: Optional[Dict[str, Any]] = None):
        """Initializes the TraceClient with project and configuration.
        
        Args:
            project_id: Google Cloud project ID (optional, will be detected if not provided)
            config_override: Override configuration parameters (optional)
        """
        # Get tracing configuration from application settings
        self._config = get_tracing_config()
        
        # Apply config overrides if provided
        if config_override:
            self._config.update(config_override)
        
        # Get project ID (from parameter, auth module, or environment)
        self._project_id = project_id or get_project_id()
        if not self._project_id:
            raise ValueError("Could not determine Google Cloud project ID")
        
        # Format project name for API calls
        self._project_name = f"projects/{self._project_id}"
        
        # Initialize Google Cloud Trace client
        credentials = get_default_credentials()
        self._client = TraceServiceClient(credentials=credentials)
        
        logger.info(f"Initialized TraceClient for project {self._project_id}")
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def create_span(self, name: str, trace_id: Optional[str] = None, 
                    parent_span_id: Optional[str] = None, 
                    attributes: Optional[Dict[str, Any]] = None,
                    start_time: Optional[datetime.datetime] = None) -> 'Span':
        """Creates a new span in a trace.
        
        Args:
            name: Name of the span
            trace_id: Trace ID (optional, will generate a new one if not provided)
            parent_span_id: Parent span ID (optional)
            attributes: Span attributes (optional)
            start_time: Span start time (optional, defaults to current time)
            
        Returns:
            New Span instance
        """
        # Generate trace_id if not provided
        if not trace_id:
            trace_id = generate_trace_id()
        
        # Generate a span_id for the new span
        span_id = generate_span_id()
        
        # Create and return the span object
        return Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            attributes=attributes,
            start_time=start_time,
            trace_client=self
        )
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def end_span(self, span: 'Span', end_time: Optional[datetime.datetime] = None) -> bool:
        """Ends a span and sends it to Cloud Trace.
        
        Args:
            span: The span to end
            end_time: End time for the span (optional, defaults to current time)
            
        Returns:
            True if the span was successfully sent to Cloud Trace, False otherwise
        """
        # Set end time on the span if provided
        if end_time:
            span.end_time = end_time
        elif not span.end_time:
            span.end_time = datetime.datetime.utcnow()
        
        # Create a Trace protobuf with the span
        span_proto = span.to_proto()
        trace_proto = Trace(spans=[span_proto])
        
        try:
            # Call Trace API to batch write the span
            self._client.batch_write_spans(
                name=self._project_name,
                spans=[span_proto]
            )
            logger.debug(f"Successfully sent span '{span.name}' to Cloud Trace")
            return True
        except Exception as e:
            logger.error(f"Failed to send span to Cloud Trace: {str(e)}")
            return False
    
    def create_trace(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> 'Span':
        """Creates a new trace with an initial root span.
        
        Args:
            name: Name for the root span
            attributes: Span attributes (optional)
            
        Returns:
            Root span of the new trace
        """
        # Generate a new trace ID
        trace_id = generate_trace_id()
        
        # Create a root span for this trace
        root_span = self.create_span(
            name=name,
            trace_id=trace_id,
            attributes=attributes
        )
        
        # Store the trace context in thread-local storage
        setattr(_thread_local, 'trace_context', {
            'trace_id': trace_id,
            'span_id': root_span.span_id,
            'trace_options': '1'  # Set sampling bit to 1
        })
        
        return root_span
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def get_trace(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Gets a trace by ID.
        
        Args:
            trace_id: ID of the trace to retrieve
            
        Returns:
            Trace data as a dictionary or None if not found
        """
        try:
            # Format the trace name for the API call
            name = f"{self._project_name}/traces/{trace_id}"
            
            # Call the Trace API to get the trace
            trace = self._client.get_trace(name=name)
            
            # Convert to a dictionary for easier handling
            return {
                'trace_id': trace_id,
                'project_id': self._project_id,
                'spans': [self._span_proto_to_dict(span) for span in trace.spans]
            }
        except Exception as e:
            logger.debug(f"Failed to get trace {trace_id}: {str(e)}")
            return None
    
    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS)
    def list_traces(self, filter_str: Optional[str] = None, 
                   start_time: Optional[datetime.datetime] = None,
                   end_time: Optional[datetime.datetime] = None,
                   page_size: int = 100) -> List[Dict[str, Any]]:
        """Lists traces matching a filter.
        
        Args:
            filter_str: Filter string for traces
            start_time: Start time for the time range
            end_time: End time for the time range
            page_size: Number of traces per response page
            
        Returns:
            List of matching traces
        """
        from google.protobuf.timestamp_pb2 import Timestamp
        
        # Prepare the request
        start_time_proto = None
        end_time_proto = None
        
        if start_time:
            start_time_proto = Timestamp()
            start_time_proto.FromDatetime(start_time)
        
        if end_time:
            end_time_proto = Timestamp()
            end_time_proto.FromDatetime(end_time)
        
        # Call the Trace API to list traces with pagination
        traces = []
        page_token = None
        
        while True:
            # Make the API call
            response = self._client.list_traces(
                project_id=self._project_id,
                filter=filter_str,
                start_time=start_time_proto,
                end_time=end_time_proto,
                page_size=page_size,
                page_token=page_token
            )
            
            # Process the results
            for trace in response.traces:
                trace_dict = {
                    'trace_id': trace.trace_id,
                    'project_id': self._project_id,
                    'spans': [self._span_proto_to_dict(span) for span in trace.spans]
                }
                traces.append(trace_dict)
            
            # Check if there are more pages
            page_token = response.next_page_token
            if not page_token:
                break
        
        return traces
    
    def _span_proto_to_dict(self, span_proto: TraceSpanProto) -> Dict[str, Any]:
        """Converts a span protobuf to a dictionary.
        
        Args:
            span_proto: Span protobuf object
            
        Returns:
            Dictionary representation of the span
        """
        # Extract span attributes
        attributes = {}
        if span_proto.attributes and span_proto.attributes.attribute_map:
            for key, value in span_proto.attributes.attribute_map.items():
                if value.HasField('string_value'):
                    attributes[key] = value.string_value.value
                elif value.HasField('int_value'):
                    attributes[key] = value.int_value
                elif value.HasField('bool_value'):
                    attributes[key] = value.bool_value
                
        # Convert the span protobuf to a dictionary
        span_dict = {
            'name': span_proto.name,
            'span_id': span_proto.span_id,
            'parent_span_id': span_proto.parent_span_id,
            'attributes': attributes,
            'start_time': span_proto.start_time.ToDatetime() if span_proto.start_time else None,
            'end_time': span_proto.end_time.ToDatetime() if span_proto.end_time else None
        }
        
        return span_dict


class Span:
    """Represents a trace span for measuring operation execution.
    
    A span represents a single operation within a trace. It has a name, a unique
    identifier, timestamps for when it starts and ends, and various attributes
    to provide context about the operation.
    """
    
    def __init__(self, name: str, trace_id: str, span_id: str, 
                parent_span_id: Optional[str] = None,
                attributes: Optional[Dict[str, Any]] = None,
                start_time: Optional[datetime.datetime] = None,
                trace_client: Optional[TraceClient] = None):
        """Initializes a new Span instance.
        
        Args:
            name: Name of the span (typically the operation name)
            trace_id: Trace ID this span belongs to
            span_id: Unique identifier for this span
            parent_span_id: Span ID of the parent span (optional)
            attributes: Dictionary of span attributes (optional)
            start_time: Start time of the span (optional, defaults to current time)
            trace_client: TraceClient for sending the span (optional)
        """
        self.name = name
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.attributes = attributes or {}
        self.start_time = start_time or datetime.datetime.utcnow()
        self.end_time = None
        self.ended = False
        self._trace_client = trace_client
    
    def add_attribute(self, key: str, value: Any) -> None:
        """Adds a single attribute to the span.
        
        Args:
            key: Attribute key
            value: Attribute value
        """
        if self.ended:
            logger.warning(f"Cannot add attribute to an ended span: {self.name}")
            return
        
        # Convert value to string if it's not a basic type
        if not isinstance(value, (str, int, float, bool)):
            value = str(value)
            
        self.attributes[key] = value
    
    def add_attributes(self, attributes: Dict[str, Any]) -> None:
        """Adds multiple attributes to the span.
        
        Args:
            attributes: Dictionary of attributes to add
        """
        if self.ended:
            logger.warning(f"Cannot add attributes to an ended span: {self.name}")
            return
        
        # Add each attribute individually to ensure proper type handling
        for key, value in attributes.items():
            self.add_attribute(key, value)
    
    def end(self, end_time: Optional[datetime.datetime] = None) -> bool:
        """Ends the span and sends it to Cloud Trace.
        
        Args:
            end_time: End time for the span (optional, defaults to current time)
            
        Returns:
            True if the span was successfully ended, False otherwise
        """
        if self.ended:
            logger.warning(f"Attempt to end an already ended span: {self.name}")
            return False
        
        # Set end time
        self.end_time = end_time or datetime.datetime.utcnow()
        self.ended = True
        
        # Send to Cloud Trace if we have a trace client
        if self._trace_client:
            return self._trace_client.end_span(self, self.end_time)
        
        return True
    
    def record_exception(self, exception: Exception, end_span: bool = True) -> None:
        """Records an exception that occurred during the span.
        
        Args:
            exception: The exception to record
            end_span: Whether to end the span after recording the exception
        """
        if self.ended:
            logger.warning(f"Cannot record exception on an ended span: {self.name}")
            return
        
        # Add exception details as attributes
        self.add_attribute("error", True)
        self.add_attribute("error.type", exception.__class__.__name__)
        self.add_attribute("error.message", str(exception))
        
        import traceback
        self.add_attribute("error.stack_trace", traceback.format_exc())
        
        # End the span if requested
        if end_span:
            self.end()
    
    def get_duration(self) -> Optional[float]:
        """Gets the duration of the span in seconds.
        
        Returns:
            Duration in seconds or None if the span has not ended
        """
        if not self.ended or not self.start_time or not self.end_time:
            return None
        
        # Calculate duration in seconds
        duration = (self.end_time - self.start_time).total_seconds()
        return duration
    
    def to_proto(self) -> TraceSpanProto:
        """Converts the span to a protobuf object for the API.
        
        Returns:
            Span protobuf object for Google Cloud Trace API
        """
        from google.protobuf.timestamp_pb2 import Timestamp
        
        # Create the span protobuf
        span_pb = TraceSpanProto(
            name=f"projects/{self._trace_client._project_id}/traces/{self.trace_id}/spans/{self.span_id}",
            span_id=self.span_id,
            display_name=TruncatableString(value=self.name),
            start_time=None,
            end_time=None
        )
        
        # Set parent span ID if provided
        if self.parent_span_id:
            span_pb.parent_span_id = self.parent_span_id
        
        # Set start time if available
        if self.start_time:
            start_time_pb = Timestamp()
            start_time_pb.FromDatetime(self.start_time)
            span_pb.start_time = start_time_pb
        
        # Set end time if available
        if self.end_time:
            end_time_pb = Timestamp()
            end_time_pb.FromDatetime(self.end_time)
            span_pb.end_time = end_time_pb
        
        # Add attributes if any
        if self.attributes:
            attrs_pb = Attributes()
            
            for key, value in self.attributes.items():
                attr_value = AttributeValue()
                
                # Set the appropriate value type
                if isinstance(value, str):
                    attr_value.string_value = TruncatableString(value=value)
                elif isinstance(value, bool):
                    attr_value.bool_value = value
                elif isinstance(value, int):
                    attr_value.int_value = value
                else:
                    # Convert other types to string
                    attr_value.string_value = TruncatableString(value=str(value))
                
                attrs_pb.attribute_map[key] = attr_value
            
            span_pb.attributes = attrs_pb
        
        return span_pb


class TraceContext:
    """Context manager for creating and managing spans.
    
    This class provides a convenient way to instrument code blocks with tracing
    using Python's context manager protocol (with statements).
    """
    
    def __init__(self, name: str, trace_id: Optional[str] = None,
                parent_span_id: Optional[str] = None,
                attributes: Optional[Dict[str, Any]] = None):
        """Initializes the trace context manager.
        
        Args:
            name: Name for the span (typically the operation name)
            trace_id: Trace ID (optional, will use current trace context or create new)
            parent_span_id: Parent span ID (optional, will use current span as parent)
            attributes: Span attributes (optional)
        """
        global _trace_client
        
        # Get or create the trace client
        if _trace_client is None:
            _trace_client = TraceClient()
        
        # Store the previous trace context
        self._previous_context = extract_current_trace_data()
        
        # Determine trace ID and parent span ID
        if not trace_id:
            # Try to get trace ID from current context
            current_context = self._previous_context
            trace_id = current_context.get('trace_id')
            
            # If no parent span ID specified, use current span ID as parent
            if parent_span_id is None and trace_id:
                parent_span_id = current_context.get('span_id')
        
        # Create a new trace if we don't have one
        if not trace_id:
            self._span = _trace_client.create_trace(name, attributes)
        else:
            # Create a span within the existing trace
            self._span = _trace_client.create_span(
                name=name,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
                attributes=attributes
            )
    
    def __enter__(self) -> Span:
        """Enters the tracing context and starts the span.
        
        Returns:
            The created span for context variable assignment
        """
        if not is_tracing_enabled():
            return self._span
        
        # Store the current trace context in thread-local storage
        setattr(_thread_local, 'trace_context', {
            'trace_id': self._span.trace_id,
            'span_id': self._span.span_id,
            'trace_options': '1'  # Set sampling bit to 1
        })
        
        # Set correlation ID from trace ID for logging integration
        set_correlation_id(self._span.trace_id)
        
        return self._span
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exits the tracing context and ends the span.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        # Record exception if one occurred
        if exc_val:
            self._span.record_exception(exc_val, end_span=False)
        
        # End the span
        self._span.end()
        
        # Restore previous trace context
        if self._previous_context:
            setattr(_thread_local, 'trace_context', self._previous_context)
        else:
            if hasattr(_thread_local, 'trace_context'):
                delattr(_thread_local, 'trace_context')
        
        # Don't suppress the exception
        return None


class OpenTelemetryTracer:
    """OpenTelemetry integration for standardized tracing.
    
    This class provides integration with the OpenTelemetry tracing standards,
    allowing for more portable instrumentation that can work with multiple
    tracing backends.
    """
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """Initializes the OpenTelemetry tracer.
        
        Args:
            config_override: Override configuration parameters (optional)
        """
        # Get tracing configuration from application settings
        self._config = get_tracing_config()
        
        # Apply config overrides if provided
        if config_override:
            self._config.update(config_override)
        
        # Set up OpenTelemetry trace provider
        provider = SDKTracerProvider(
            sampling_rate=self._config.get("sampling_rate", DEFAULT_SAMPLING_RATE)
        )
        
        # Configure Cloud Trace exporter if specified
        if self._config.get("exporter") == "cloud_trace":
            project_id = get_project_id()
            exporter = CloudTraceSpanExporter(project_id=project_id)
            provider.add_span_processor(BatchSpanProcessor(exporter))
        
        # Use Cloud Trace propagator
        set_global_textmap(CloudTraceFormatPropagator())
        
        # Create tracer
        self._tracer = provider.get_tracer("self-healing-pipeline")
        
        logger.info("Initialized OpenTelemetry tracer")
    
    def start_span(self, name: str, attributes: Optional[Dict[str, Any]] = None,
                 parent_context: Optional['opentelemetry.trace.SpanContext'] = None):
        """Starts a new span.
        
        Args:
            name: Name of the span
            attributes: Span attributes (optional)
            parent_context: Parent span context (optional)
            
        Returns:
            OpenTelemetry span
        """
        # Get current span context if parent not provided
        if parent_context is None:
            current_span = get_current_span()
            if current_span.is_recording():
                parent_context = current_span.get_span_context()
        
        # Start the span
        span = self._tracer.start_span(
            name=name,
            context=parent_context,
            kind=SpanKind.INTERNAL
        )
        
        # Set attributes if provided
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        return span
    
    def end_span(self, span: 'opentelemetry.trace.Span') -> None:
        """Ends a span.
        
        Args:
            span: The span to end
        """
        span.end()
        logger.debug(f"Ended OpenTelemetry span: {span.name}")
    
    def record_exception(self, span: 'opentelemetry.trace.Span', 
                        exception: Exception, end_span: bool = True) -> None:
        """Records an exception on a span.
        
        Args:
            span: The span to record the exception on
            exception: The exception to record
            end_span: Whether to end the span after recording
        """
        # Record the exception on the span
        span.record_exception(exception)
        span.set_status(opentelemetry.trace.Status(
            opentelemetry.trace.StatusCode.ERROR,
            description=str(exception)
        ))
        
        # End the span if requested
        if end_span:
            span.end()
    
    def get_current_span(self) -> Optional['opentelemetry.trace.Span']:
        """Gets the current active span.
        
        Returns:
            Current span or None if no span is active
        """
        span = get_current_span()
        if span.is_recording():
            return span
        return None
    
    def create_span_context(self, trace_id: str, span_id: str, 
                           is_remote: bool = True) -> 'opentelemetry.trace.SpanContext':
        """Creates a span context from trace and span IDs.
        
        Args:
            trace_id: Trace ID (hex string)
            span_id: Span ID (hex string)
            is_remote: Whether this context came from a remote source
            
        Returns:
            SpanContext object
        """
        # Convert hex strings to bytes format required by OTel
        trace_id_bytes = bytes.fromhex(trace_id)
        span_id_bytes = bytes.fromhex(span_id)
        
        # Create and return the span context
        return SpanContext(
            trace_id=trace_id_bytes,
            span_id=span_id_bytes,
            is_remote=is_remote,
            trace_flags=opentelemetry.trace.TraceFlags(opentelemetry.trace.TraceFlags.SAMPLED)
        )


def trace(name: Optional[str] = None, attributes: Optional[Dict[str, Any]] = None):
    """Decorator for tracing function execution.
    
    This decorator creates a span for the function execution, recording the
    execution time and any exceptions that occur.
    
    Args:
        name: Name for the span (defaults to function name)
        attributes: Span attributes (optional)
        
    Returns:
        Decorated function
    """
    def decorator(func):
        if not is_tracing_enabled():
            return func
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use function name if span name not provided
            span_name = name or f"{func.__module__}.{func.__name__}"
            
            # Merge default attributes with any provided attributes
            span_attributes = {
                "function": func.__name__,
                "module": func.__module__
            }
            if attributes:
                span_attributes.update(attributes)
            
            # Execute function within trace context
            with TraceContext(span_name, attributes=span_attributes) as span:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Record the exception on the span
                    span.record_exception(e, end_span=False)
                    # Re-raise the exception
                    raise
        
        return wrapper
    
    # Handle case where decorator is used without parentheses
    if callable(name):
        func = name
        name = None
        return decorator(func)
    
    return decorator