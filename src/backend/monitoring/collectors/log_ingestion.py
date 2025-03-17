"""
Centralizes log collection, processing, and storage for the self-healing data pipeline.

This component ingests logs from various sources (Cloud Logging, application logs, component logs),
parses them into a standardized format, and stores them for analysis, alerting, and self-healing purposes.
It supports structured and unstructured log formats, filtering, and extraction of metrics from logs.
"""

import datetime
import json
import re
import typing
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd  # version 2.0.0+
from google.cloud import logging as cloud_logging  # version 3.5.0+

from constants import AlertSeverity, PipelineStatus
from config import get_config
from logging_config import get_logger
from utils.storage.bigquery_client import BigQueryClient
from utils.storage.firestore_client import FirestoreClient
from utils.logging.log_formatter import JsonFormatter, StructuredFormatter
from monitoring.analyzers.metric_processor import MetricProcessor

# Initialize logger
logger = get_logger(__name__)

# Constants
DEFAULT_LOG_RETENTION_DAYS = 30
LOGS_TABLE_NAME = "pipeline_logs"
LOG_COLLECTION_NAME = "logs"
DEFAULT_BATCH_SIZE = 1000
LOG_SEVERITY_MAPPING = {
    "DEBUG": 100,
    "INFO": 200, 
    "WARNING": 300,
    "ERROR": 400,
    "CRITICAL": 500
}

def parse_log_entry(log_entry: Dict, source: str) -> Dict:
    """
    Parses a log entry into a standardized format.
    
    Args:
        log_entry: Raw log entry from source
        source: Source identifier (e.g., 'cloud_logging', 'app_log')
        
    Returns:
        Standardized log entry dictionary
    """
    # Validate log entry
    if not isinstance(log_entry, dict):
        if isinstance(log_entry, str):
            try:
                log_entry = json.loads(log_entry)
            except json.JSONDecodeError:
                # If it's not valid JSON, create a basic log entry
                return {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "severity": "INFO",
                    "message": log_entry,
                    "source": source
                }
        else:
            # If it's not a dict or string, create a basic log entry
            return {
                "timestamp": datetime.datetime.now().isoformat(),
                "severity": "INFO",
                "message": str(log_entry),
                "source": source
            }
    
    # Extract timestamp - use different keys depending on source
    timestamp = None
    if "timestamp" in log_entry:
        timestamp = log_entry["timestamp"]
    elif "time" in log_entry:
        timestamp = log_entry["time"]
    elif "eventTime" in log_entry:
        timestamp = log_entry["eventTime"]
    else:
        timestamp = datetime.datetime.now().isoformat()
    
    # Ensure timestamp is in ISO format
    if isinstance(timestamp, datetime.datetime):
        timestamp = timestamp.isoformat()
    
    # Extract severity level
    severity = None
    if "severity" in log_entry:
        severity = log_entry["severity"]
    elif "level" in log_entry:
        severity = log_entry["level"]
    elif "levelname" in log_entry:
        severity = log_entry["levelname"]
    else:
        severity = "INFO"
    
    # Normalize severity
    normalized_severity = normalize_log_severity(severity, source)
    
    # Extract message
    message = None
    if "message" in log_entry:
        message = log_entry["message"]
    elif "msg" in log_entry:
        message = log_entry["msg"]
    elif "text" in log_entry:
        message = log_entry["text"]
    elif "textPayload" in log_entry:  # Cloud Logging specific
        message = log_entry["textPayload"]
    else:
        message = "No message provided"
    
    # Extract context information
    context = {}
    context_fields = [
        "correlation_id", "execution_id", "pipeline_id", "component", 
        "task_id", "dag_id", "run_id"
    ]
    
    for field in context_fields:
        if field in log_entry:
            context[field] = log_entry[field]
    
    # Create standardized log entry
    standardized_entry = {
        "timestamp": timestamp,
        "severity": severity,
        "severity_value": normalized_severity,
        "message": message,
        "source": source,
        "context": context
    }
    
    # Add original log data if needed
    if "data" in log_entry:
        standardized_entry["data"] = log_entry["data"]
    elif "jsonPayload" in log_entry:  # Cloud Logging specific
        standardized_entry["data"] = log_entry["jsonPayload"]
    
    return standardized_entry

def extract_log_metrics(log_entries: List[Dict], metric_patterns: Dict) -> List[Dict]:
    """
    Extracts metrics from log entries based on patterns.
    
    Args:
        log_entries: List of log entries to analyze
        metric_patterns: Dictionary of regex patterns to extract metrics
        
    Returns:
        List of extracted metrics
    """
    extracted_metrics = []
    
    # Process each log entry
    for log_entry in log_entries:
        message = log_entry.get("message", "")
        
        # Skip empty messages
        if not message:
            continue
        
        # Apply patterns to extract metrics
        for pattern_name, pattern_config in metric_patterns.items():
            pattern = pattern_config.get("pattern")
            metric_name = pattern_config.get("metric_name")
            
            # Skip if pattern or metric name is missing
            if not pattern or not metric_name:
                continue
            
            # Compile regex if it's a string
            if isinstance(pattern, str):
                try:
                    pattern = re.compile(pattern)
                except re.error:
                    logger.warning(f"Invalid regex pattern for metric {pattern_name}")
                    continue
            
            # Apply pattern to message
            match = pattern.search(message)
            if match:
                try:
                    # Extract value from match
                    if pattern_config.get("value_group"):
                        value_group = pattern_config["value_group"]
                        if isinstance(value_group, int):
                            value = match.group(value_group)
                        else:
                            value = match.group(value_group)
                    else:
                        # Default to group 1 if no value_group specified
                        value = match.group(1)
                    
                    # Convert value to appropriate type
                    value_type = pattern_config.get("value_type", "string")
                    if value_type == "integer":
                        value = int(value)
                    elif value_type == "float":
                        value = float(value)
                    elif value_type == "boolean":
                        value = value.lower() in ("true", "yes", "1")
                    
                    # Create metric record
                    metric = {
                        "timestamp": log_entry.get("timestamp"),
                        "metric_name": metric_name,
                        "value": value,
                        "source": log_entry.get("source"),
                        "labels": {
                            "severity": log_entry.get("severity"),
                        }
                    }
                    
                    # Add context as labels
                    if "context" in log_entry and isinstance(log_entry["context"], dict):
                        metric["labels"].update(log_entry["context"])
                    
                    extracted_metrics.append(metric)
                except (IndexError, ValueError) as e:
                    logger.warning(f"Error extracting metric from pattern {pattern_name}: {e}")
    
    return extracted_metrics

def filter_logs_by_criteria(log_entries: List[Dict], filter_criteria: Dict) -> List[Dict]:
    """
    Filters log entries based on specified criteria.
    
    Args:
        log_entries: List of log entries to filter
        filter_criteria: Dictionary of filter criteria
        
    Returns:
        Filtered list of log entries
    """
    filtered_logs = []
    
    # Process each log entry
    for log_entry in log_entries:
        matches_all_criteria = True
        
        # Check each criterion
        for key, criterion in filter_criteria.items():
            # Handle special case for time ranges
            if key == "time_range":
                if "start" in criterion:
                    start_time = criterion["start"]
                    if isinstance(start_time, str):
                        start_time = datetime.datetime.fromisoformat(start_time)
                    
                    entry_time = log_entry.get("timestamp")
                    if isinstance(entry_time, str):
                        try:
                            entry_time = datetime.datetime.fromisoformat(entry_time)
                        except ValueError:
                            # If we can't parse the timestamp, skip this criterion
                            continue
                    
                    if entry_time < start_time:
                        matches_all_criteria = False
                        break
                
                if "end" in criterion:
                    end_time = criterion["end"]
                    if isinstance(end_time, str):
                        end_time = datetime.datetime.fromisoformat(end_time)
                    
                    entry_time = log_entry.get("timestamp")
                    if isinstance(entry_time, str):
                        try:
                            entry_time = datetime.datetime.fromisoformat(entry_time)
                        except ValueError:
                            # If we can't parse the timestamp, skip this criterion
                            continue
                    
                    if entry_time > end_time:
                        matches_all_criteria = False
                        break
            
            # Handle special case for severity
            elif key == "min_severity":
                entry_severity = log_entry.get("severity_value", 0)
                min_severity_value = normalize_log_severity(criterion, "")
                
                if entry_severity < min_severity_value:
                    matches_all_criteria = False
                    break
            
            # Handle nested fields with dot notation
            elif "." in key:
                parts = key.split(".")
                value = log_entry
                for part in parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        value = None
                        break
                
                if value != criterion:
                    matches_all_criteria = False
                    break
            
            # Handle direct field comparison
            elif key in log_entry:
                entry_value = log_entry[key]
                
                # Handle different comparison types
                if isinstance(criterion, dict) and "operator" in criterion:
                    operator = criterion["operator"]
                    compare_value = criterion["value"]
                    
                    if operator == "eq":
                        if entry_value != compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "ne":
                        if entry_value == compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "gt":
                        if entry_value <= compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "lt":
                        if entry_value >= compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "gte":
                        if entry_value < compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "lte":
                        if entry_value > compare_value:
                            matches_all_criteria = False
                            break
                    elif operator == "contains":
                        if isinstance(entry_value, str) and isinstance(compare_value, str):
                            if compare_value not in entry_value:
                                matches_all_criteria = False
                                break
                        else:
                            matches_all_criteria = False
                            break
                    elif operator == "matches":
                        if isinstance(entry_value, str) and isinstance(compare_value, str):
                            try:
                                pattern = re.compile(compare_value)
                                if not pattern.search(entry_value):
                                    matches_all_criteria = False
                                    break
                            except re.error:
                                matches_all_criteria = False
                                break
                        else:
                            matches_all_criteria = False
                            break
                else:
                    # Simple equality check
                    if entry_value != criterion:
                        matches_all_criteria = False
                        break
            else:
                # Field not found in log entry
                matches_all_criteria = False
                break
        
        # Add to filtered logs if it matches all criteria
        if matches_all_criteria:
            filtered_logs.append(log_entry)
    
    return filtered_logs

def normalize_log_severity(severity: str, source: str) -> int:
    """
    Normalizes log severity levels to a standard scale.
    
    Args:
        severity: Severity string from log
        source: Source identifier for source-specific mappings
        
    Returns:
        Normalized severity level as integer
    """
    if not severity:
        return LOG_SEVERITY_MAPPING.get("INFO", 200)
    
    # Convert to uppercase for consistent comparison
    if isinstance(severity, str):
        severity = severity.upper()
    
    # Handle numeric severity
    if isinstance(severity, (int, float)):
        return int(severity)
    
    # Handle Cloud Logging severity
    if source == "cloud_logging":
        # Cloud Logging specific mapping
        cloud_mapping = {
            "DEFAULT": 0,
            "DEBUG": 100,
            "INFO": 200,
            "NOTICE": 300,
            "WARNING": 400,
            "ERROR": 500,
            "CRITICAL": 600,
            "ALERT": 700,
            "EMERGENCY": 800
        }
        return cloud_mapping.get(severity, 200)
    
    # Use standard mapping
    return LOG_SEVERITY_MAPPING.get(severity, 200)

class LogParser:
    """Base class for log format parsers"""
    
    def __init__(self, parser_name: str, config: Dict = None):
        """
        Initializes the LogParser with configuration.
        
        Args:
            parser_name: Name to identify this parser
            config: Configuration dictionary
        """
        self._parser_name = parser_name
        self._config = config or {}
    
    def parse(self, log_entry: Any, source: str) -> Dict:
        """
        Parses a log entry into standardized format.
        
        Args:
            log_entry: Log entry to parse
            source: Source identifier
            
        Returns:
            Parsed log entry
        """
        # This is an abstract method to be implemented by subclasses
        raise NotImplementedError("Subclasses must implement parse method")
    
    def validate(self, log_entry: Dict) -> bool:
        """
        Validates a log entry structure.
        
        Args:
            log_entry: Log entry to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check for required fields
        required_fields = ["timestamp", "severity", "message"]
        for field in required_fields:
            if field not in log_entry:
                return False
        
        # Validate field types
        if not isinstance(log_entry.get("timestamp"), (str, datetime.datetime)):
            return False
        
        if not isinstance(log_entry.get("severity"), (str, int)):
            return False
        
        if not isinstance(log_entry.get("message"), str):
            return False
        
        return True

class StructuredLogParser(LogParser):
    """Parser for structured JSON log formats"""
    
    def __init__(self, config: Dict = None):
        """
        Initializes the StructuredLogParser.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("structured", config)
        self._field_mapping = self._config.get("field_mapping", {})
    
    def parse(self, log_entry: Any, source: str) -> Dict:
        """
        Parses a structured log entry.
        
        Args:
            log_entry: Log entry to parse (dict or JSON string)
            source: Source identifier
            
        Returns:
            Parsed log entry
        """
        # Convert to dict if it's a string
        if isinstance(log_entry, str):
            try:
                log_entry = json.loads(log_entry)
            except json.JSONDecodeError:
                # If it's not valid JSON, create a basic log entry
                return {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "severity": "INFO",
                    "severity_value": LOG_SEVERITY_MAPPING.get("INFO", 200),
                    "message": log_entry,
                    "source": source
                }
        
        # Map fields according to configuration
        mapped_entry = self.map_fields(log_entry, source)
        
        # Normalize severity
        mapped_entry["severity_value"] = normalize_log_severity(
            mapped_entry.get("severity", "INFO"), 
            source
        )
        
        # Ensure source is set
        mapped_entry["source"] = source
        
        return mapped_entry
    
    def map_fields(self, log_entry: Dict, source: str) -> Dict:
        """
        Maps source-specific fields to standard fields.
        
        Args:
            log_entry: Log entry to map
            source: Source identifier
            
        Returns:
            Mapped log entry
        """
        result = {}
        
        # Get source-specific mapping
        mapping = self._field_mapping.get(source, {})
        
        # Map fields using the field mapping
        for target_field, source_field in mapping.items():
            # Handle nested fields with dot notation
            if isinstance(source_field, str) and "." in source_field:
                value = log_entry
                for part in source_field.split("."):
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        value = None
                        break
                result[target_field] = value
            elif source_field in log_entry:
                result[target_field] = log_entry[source_field]
        
        # Copy unmapped fields
        for field, value in log_entry.items():
            if field not in mapping.values():
                result[field] = value
        
        # Ensure required fields
        if "timestamp" not in result:
            result["timestamp"] = datetime.datetime.now().isoformat()
        
        if "severity" not in result:
            result["severity"] = "INFO"
        
        if "message" not in result:
            result["message"] = "No message provided"
        
        return result

class UnstructuredLogParser(LogParser):
    """Parser for unstructured text log formats"""
    
    def __init__(self, config: Dict = None):
        """
        Initializes the UnstructuredLogParser.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("unstructured", config)
        self._patterns = {}
        
        # Compile regex patterns from configuration
        patterns_config = self._config.get("patterns", {})
        for source, pattern_str in patterns_config.items():
            try:
                self._patterns[source] = re.compile(pattern_str)
            except re.error:
                logger.warning(f"Invalid regex pattern for source {source}")
    
    def parse(self, log_entry: Any, source: str) -> Dict:
        """
        Parses an unstructured log entry.
        
        Args:
            log_entry: Log entry to parse (string)
            source: Source identifier
            
        Returns:
            Parsed log entry
        """
        # Ensure log entry is a string
        if not isinstance(log_entry, str):
            log_entry = str(log_entry)
        
        # Apply regex patterns
        extracted = self.apply_patterns(log_entry, source)
        
        # Create standardized log entry
        result = {
            "timestamp": extracted.get("timestamp", datetime.datetime.now().isoformat()),
            "severity": extracted.get("severity", "INFO"),
            "severity_value": normalize_log_severity(
                extracted.get("severity", "INFO"), 
                source
            ),
            "message": extracted.get("message", log_entry),
            "source": source
        }
        
        # Add extracted context if available
        context = {}
        context_fields = [
            "correlation_id", "execution_id", "pipeline_id", "component", 
            "task_id", "dag_id", "run_id"
        ]
        
        for field in context_fields:
            if field in extracted:
                context[field] = extracted[field]
        
        if context:
            result["context"] = context
        
        # Add other extracted fields
        for key, value in extracted.items():
            if key not in result and key not in context_fields:
                result[key] = value
        
        return result
    
    def apply_patterns(self, log_text: str, source: str) -> Dict:
        """
        Applies regex patterns to extract fields from text.
        
        Args:
            log_text: Log text to analyze
            source: Source identifier
            
        Returns:
            Dictionary of extracted fields
        """
        result = {}
        
        # Get pattern for this source
        pattern = self._patterns.get(source)
        if not pattern:
            # Try default pattern
            pattern = self._patterns.get("default")
        
        if pattern:
            match = pattern.search(log_text)
            if match:
                # Extract named groups
                result.update(match.groupdict())
                
                # If no named groups but groups exist, try to map positionally
                if not result and match.groups():
                    # Basic positional mapping
                    fields = ["timestamp", "severity", "message"]
                    for i, field in enumerate(fields):
                        if i < len(match.groups()):
                            result[field] = match.group(i+1)
        
        # If no pattern match but we have fallback pattern, try it
        if not result and "fallback" in self._patterns:
            fallback_pattern = self._patterns["fallback"]
            match = fallback_pattern.search(log_text)
            if match:
                result.update(match.groupdict())
        
        # If still no match, use the whole text as message
        if not result:
            result["message"] = log_text
        
        return result

class LogFilter:
    """Configurable filter for log entries"""
    
    def __init__(self, filter_name: str, filter_criteria: Dict):
        """
        Initializes the LogFilter with configuration.
        
        Args:
            filter_name: Name to identify this filter
            filter_criteria: Dictionary of filter criteria
        """
        self._filter_name = filter_name
        self._filter_criteria = filter_criteria
        
        # Compile any regex patterns in criteria
        self._compile_regex_patterns()
    
    def apply_filter(self, log_entry: Dict) -> bool:
        """
        Applies filter criteria to a log entry.
        
        Args:
            log_entry: Log entry to filter
            
        Returns:
            True if log passes filter, False otherwise
        """
        # Check each criterion
        for key, criterion in self._filter_criteria.items():
            # Handle special case for time ranges
            if key == "time_range":
                if "start" in criterion:
                    start_time = criterion["start"]
                    if isinstance(start_time, str):
                        start_time = datetime.datetime.fromisoformat(start_time)
                    
                    entry_time = log_entry.get("timestamp")
                    if isinstance(entry_time, str):
                        try:
                            entry_time = datetime.datetime.fromisoformat(entry_time)
                        except ValueError:
                            return False
                    
                    if entry_time < start_time:
                        return False
                
                if "end" in criterion:
                    end_time = criterion["end"]
                    if isinstance(end_time, str):
                        end_time = datetime.datetime.fromisoformat(end_time)
                    
                    entry_time = log_entry.get("timestamp")
                    if isinstance(entry_time, str):
                        try:
                            entry_time = datetime.datetime.fromisoformat(entry_time)
                        except ValueError:
                            return False
                    
                    if entry_time > end_time:
                        return False
            
            # Handle special case for severity
            elif key == "min_severity":
                entry_severity = log_entry.get("severity_value", 0)
                min_severity_value = normalize_log_severity(criterion, "")
                
                if entry_severity < min_severity_value:
                    return False
            
            # Handle nested fields with dot notation
            elif "." in key:
                parts = key.split(".")
                value = log_entry
                for part in parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        return False
                
                if not self._match_criterion(value, criterion):
                    return False
            
            # Handle direct field comparison
            elif key in log_entry:
                if not self._match_criterion(log_entry[key], criterion):
                    return False
            else:
                # Field not found in log entry
                return False
        
        # Passed all criteria
        return True
    
    def update_criteria(self, new_criteria: Dict) -> None:
        """
        Updates the filter criteria.
        
        Args:
            new_criteria: New filter criteria
        """
        self._filter_criteria.update(new_criteria)
        
        # Recompile regex patterns
        self._compile_regex_patterns()
    
    def _compile_regex_patterns(self) -> None:
        """Compiles regex patterns in filter criteria."""
        for key, criterion in self._filter_criteria.items():
            if isinstance(criterion, dict) and criterion.get("operator") == "matches":
                pattern_str = criterion.get("value")
                if isinstance(pattern_str, str):
                    try:
                        criterion["_compiled_pattern"] = re.compile(pattern_str)
                    except re.error:
                        logger.warning(f"Invalid regex pattern in filter {self._filter_name}")
    
    def _match_criterion(self, value: Any, criterion: Any) -> bool:
        """
        Checks if a value matches a criterion.
        
        Args:
            value: Value to check
            criterion: Criterion to match against
            
        Returns:
            True if matches, False otherwise
        """
        # Handle different comparison types
        if isinstance(criterion, dict) and "operator" in criterion:
            operator = criterion["operator"]
            compare_value = criterion["value"]
            
            if operator == "eq":
                return value == compare_value
            elif operator == "ne":
                return value != compare_value
            elif operator == "gt":
                return value > compare_value
            elif operator == "lt":
                return value < compare_value
            elif operator == "gte":
                return value >= compare_value
            elif operator == "lte":
                return value <= compare_value
            elif operator == "contains":
                if isinstance(value, str) and isinstance(compare_value, str):
                    return compare_value in value
                return False
            elif operator == "matches":
                if isinstance(value, str):
                    # Use compiled pattern if available
                    if "_compiled_pattern" in criterion:
                        pattern = criterion["_compiled_pattern"]
                    else:
                        try:
                            pattern = re.compile(compare_value)
                        except re.error:
                            return False
                    return bool(pattern.search(value))
                return False
            else:
                # Unknown operator
                return False
        else:
            # Simple equality check
            return value == criterion

class LogIngestion:
    """Main class for ingesting, processing, and storing logs from various sources"""
    
    def __init__(self, config_override: Dict = None):
        """
        Initializes the LogIngestion with configuration settings.
        
        Args:
            config_override: Override configuration settings
        """
        # Initialize configuration
        app_config = get_config()
        self._config = {
            "log_retention_days": DEFAULT_LOG_RETENTION_DAYS,
            "batch_size": DEFAULT_BATCH_SIZE,
            "bigquery_table": LOGS_TABLE_NAME,
            "firestore_collection": LOG_COLLECTION_NAME,
            "default_storage": "bigquery"
        }
        
        # Apply configuration from app config
        log_config = app_config.get("logging", {})
        if isinstance(log_config, dict):
            self._config.update(log_config)
        
        # Apply configuration overrides
        if config_override:
            self._config.update(config_override)
        
        # Initialize clients
        try:
            self._logging_client = cloud_logging.Client()
        except Exception as e:
            logger.warning(f"Error initializing Cloud Logging client: {e}")
            self._logging_client = None
        
        try:
            self._bigquery_client = BigQueryClient()
        except Exception as e:
            logger.warning(f"Error initializing BigQuery client: {e}")
            self._bigquery_client = None
        
        try:
            self._firestore_client = FirestoreClient()
        except Exception as e:
            logger.warning(f"Error initializing Firestore client: {e}")
            self._firestore_client = None
        
        try:
            self._metric_processor = MetricProcessor()
        except Exception as e:
            logger.warning(f"Error initializing MetricProcessor: {e}")
            self._metric_processor = None
        
        # Initialize log parsers
        self._log_parsers = {
            "structured": StructuredLogParser(self._config.get("parsers", {}).get("structured")),
            "unstructured": UnstructuredLogParser(self._config.get("parsers", {}).get("unstructured"))
        }
        
        # Initialize log filters
        self._log_filters = {}
        filters_config = self._config.get("filters", {})
        for name, criteria in filters_config.items():
            self._log_filters[name] = LogFilter(name, criteria)
        
        # Load metric patterns
        self._metric_patterns = self._config.get("metric_patterns", {})
        
        logger.info("LogIngestion initialized")
    
    def ingest_logs(self, query_parameters: Dict = None) -> Dict:
        """
        Ingests logs from specified sources.
        
        Args:
            query_parameters: Parameters for log ingestion query
            
        Returns:
            Dictionary with ingestion results and statistics
        """
        query_parameters = query_parameters or {}
        
        # Determine which sources to ingest from
        sources = query_parameters.get("sources", ["cloud_logging"])
        if isinstance(sources, str):
            sources = [sources]
        
        results = {
            "status": "success",
            "total_logs": 0,
            "sources": {},
            "errors": []
        }
        
        # Ingest from each source
        for source in sources:
            try:
                source_logs = []
                
                if source == "cloud_logging":
                    source_logs = self.ingest_cloud_logging(query_parameters)
                elif source == "application_logs":
                    source_logs = self.ingest_application_logs(query_parameters)
                elif source == "component_logs":
                    source_logs = self.ingest_component_logs(query_parameters)
                else:
                    results["errors"].append(f"Unknown source: {source}")
                    continue
                
                # Process logs
                processing_params = query_parameters.get("processing", {})
                processed_result = self.process_logs(source_logs, processing_params)
                
                # Store logs if specified
                storage_backends = query_parameters.get("storage", [self._config.get("default_storage")])
                if storage_backends:
                    storage_result = self.store_logs(processed_result["logs"], storage_backends)
                    processed_result["storage"] = storage_result
                
                # Extract metrics if specified
                if query_parameters.get("extract_metrics", False):
                    metrics = self.get_log_metrics(
                        processed_result["logs"], 
                        query_parameters.get("metric_parameters", {})
                    )
                    processed_result["metrics"] = metrics
                
                # Update results
                results["sources"][source] = {
                    "ingested": len(source_logs),
                    "processed": len(processed_result["logs"]),
                    "filtered": processed_result.get("filtered", 0)
                }
                
                if "storage" in processed_result:
                    results["sources"][source]["stored"] = processed_result["storage"].get("total_stored", 0)
                
                if "metrics" in processed_result:
                    results["sources"][source]["metrics"] = len(processed_result["metrics"])
                
                # Update total logs count
                results["total_logs"] += len(processed_result["logs"])
                
            except Exception as e:
                error_message = f"Error ingesting logs from {source}: {str(e)}"
                logger.error(error_message)
                results["errors"].append(error_message)
                results["sources"][source] = {"error": error_message}
        
        # Set overall status
        if results["errors"]:
            results["status"] = "partial_failure" if results["total_logs"] > 0 else "failure"
        
        return results
    
    def ingest_cloud_logging(self, query_parameters: Dict = None) -> List[Dict]:
        """
        Ingests logs from Google Cloud Logging.
        
        Args:
            query_parameters: Parameters for Cloud Logging query
            
        Returns:
            List of log entries from Cloud Logging
        """
        query_parameters = query_parameters or {}
        
        if not self._logging_client:
            logger.error("Cloud Logging client not initialized")
            return []
        
        # Build filter string
        filter_parts = []
        
        # Add project filter
        project = query_parameters.get("project", get_config().get_gcp_project_id())
        if project:
            filter_parts.append(f"resource.labels.project_id = {project}")
        
        # Add severity filter
        min_severity = query_parameters.get("min_severity")
        if min_severity:
            filter_parts.append(f"severity >= {min_severity}")
        
        # Add time range filters
        if "time_range" in query_parameters:
            time_range = query_parameters["time_range"]
            
            if "start" in time_range:
                start_time = time_range["start"]
                if isinstance(start_time, datetime.datetime):
                    start_time = start_time.isoformat()
                filter_parts.append(f"timestamp >= \"{start_time}\"")
            
            if "end" in time_range:
                end_time = time_range["end"]
                if isinstance(end_time, datetime.datetime):
                    end_time = end_time.isoformat()
                filter_parts.append(f"timestamp <= \"{end_time}\"")
        
        # Add custom filters
        custom_filters = query_parameters.get("filters", [])
        filter_parts.extend(custom_filters)
        
        # Combine all filter parts
        filter_string = " AND ".join(filter_parts) if filter_parts else None
        
        # Execute query
        try:
            entries = []
            
            logger.debug(f"Querying Cloud Logging with filter: {filter_string}")
            
            for entry in self._logging_client.list_entries(filter_=filter_string):
                # Convert log entry to dictionary
                entry_dict = {
                    "timestamp": entry.timestamp.isoformat() if entry.timestamp else datetime.datetime.now().isoformat(),
                    "severity": entry.severity,
                    "resource": entry.resource.type if entry.resource else None,
                    "labels": entry.labels,
                }
                
                # Add message from the appropriate payload
                if hasattr(entry, "payload") and entry.payload:
                    entry_dict["message"] = entry.payload
                elif hasattr(entry, "text_payload") and entry.text_payload:
                    entry_dict["message"] = entry.text_payload
                elif hasattr(entry, "json_payload") and entry.json_payload:
                    entry_dict["message"] = str(entry.json_payload)
                    entry_dict["jsonPayload"] = entry.json_payload
                else:
                    entry_dict["message"] = "No message"
                
                entries.append(entry_dict)
            
            return entries
        
        except Exception as e:
            logger.error(f"Error querying Cloud Logging: {e}")
            return []
    
    def ingest_application_logs(self, query_parameters: Dict = None) -> List[Dict]:
        """
        Ingests logs from application log files.
        
        Args:
            query_parameters: Parameters for log file query
            
        Returns:
            List of log entries from application logs
        """
        query_parameters = query_parameters or {}
        
        # Get log paths from parameters
        log_paths = query_parameters.get("log_paths", [])
        if isinstance(log_paths, str):
            log_paths = [log_paths]
        
        if not log_paths:
            log_dir = self._config.get("log_dir", "logs")
            log_file = self._config.get("log_file", "pipeline.log")
            default_path = os.path.join(log_dir, log_file)
            log_paths = [default_path]
        
        entries = []
        
        # Process each log file
        for log_path in log_paths:
            try:
                if not os.path.exists(log_path):
                    logger.warning(f"Log file not found: {log_path}")
                    continue
                
                # Determine parser based on file extension
                parser_name = "structured"
                if log_path.endswith(".log") or log_path.endswith(".txt"):
                    parser_name = "unstructured"
                
                with open(log_path, 'r') as log_file:
                    for line in log_file:
                        line = line.strip()
                        if not line:
                            continue
                        
                        # Parse log entry
                        try:
                            parser = self._log_parsers.get(parser_name)
                            if parser:
                                log_entry = parser.parse(line, "application_logs")
                                entries.append(log_entry)
                            else:
                                # Fallback to basic parsing
                                entries.append(parse_log_entry(
                                    {"message": line}, 
                                    "application_logs"
                                ))
                        except Exception as e:
                            logger.warning(f"Error parsing log entry: {e}")
            
            except Exception as e:
                logger.error(f"Error reading log file {log_path}: {e}")
        
        return entries
    
    def ingest_component_logs(self, query_parameters: Dict = None) -> List[Dict]:
        """
        Ingests logs from specific pipeline components.
        
        Args:
            query_parameters: Parameters for component log query
            
        Returns:
            List of log entries from components
        """
        query_parameters = query_parameters or {}
        
        # Get components to query
        components = query_parameters.get("components", [])
        if isinstance(components, str):
            components = [components]
        
        entries = []
        
        # Handle Composer/Airflow logs
        if "composer" in components or "airflow" in components:
            composer_logs = self._query_composer_logs(query_parameters)
            entries.extend(composer_logs)
        
        # Handle BigQuery logs
        if "bigquery" in components:
            bigquery_logs = self._query_bigquery_logs(query_parameters)
            entries.extend(bigquery_logs)
        
        # Handle Dataflow logs
        if "dataflow" in components:
            dataflow_logs = self._query_dataflow_logs(query_parameters)
            entries.extend(dataflow_logs)
        
        # Add component name to context if not already present
        for entry in entries:
            if "context" not in entry:
                entry["context"] = {}
            
            if "component" not in entry["context"]:
                # Try to infer component from source or other fields
                component = None
                if "source" in entry:
                    if "composer" in entry["source"] or "airflow" in entry["source"]:
                        component = "composer"
                    elif "bigquery" in entry["source"]:
                        component = "bigquery"
                    elif "dataflow" in entry["source"]:
                        component = "dataflow"
                
                if component:
                    entry["context"]["component"] = component
        
        return entries
    
    def _query_composer_logs(self, query_parameters: Dict) -> List[Dict]:
        """Query Composer/Airflow logs based on parameters."""
        # This would typically involve querying Cloud Logging with Composer-specific filters
        # or potentially accessing the Airflow database directly
        
        # For now, we'll just query Cloud Logging with Composer-specific filters
        composer_params = dict(query_parameters)
        
        # Add Composer-specific filters
        composer_filters = [
            'resource.type = "cloud_composer_environment"'
        ]
        
        if "filters" not in composer_params:
            composer_params["filters"] = []
        
        composer_params["filters"].extend(composer_filters)
        
        return self.ingest_cloud_logging(composer_params)
    
    def _query_bigquery_logs(self, query_parameters: Dict) -> List[Dict]:
        """Query BigQuery logs based on parameters."""
        # This would typically involve querying Cloud Logging with BigQuery-specific filters
        # or potentially querying the INFORMATION_SCHEMA views
        
        # For now, we'll just query Cloud Logging with BigQuery-specific filters
        bigquery_params = dict(query_parameters)
        
        # Add BigQuery-specific filters
        bigquery_filters = [
            'resource.type = "bigquery_resource"'
        ]
        
        if "filters" not in bigquery_params:
            bigquery_params["filters"] = []
        
        bigquery_params["filters"].extend(bigquery_filters)
        
        return self.ingest_cloud_logging(bigquery_params)
    
    def _query_dataflow_logs(self, query_parameters: Dict) -> List[Dict]:
        """Query Dataflow logs based on parameters."""
        # This would typically involve querying Cloud Logging with Dataflow-specific filters
        
        # For now, we'll just query Cloud Logging with Dataflow-specific filters
        dataflow_params = dict(query_parameters)
        
        # Add Dataflow-specific filters
        dataflow_filters = [
            'resource.type = "dataflow_step"'
        ]
        
        if "filters" not in dataflow_params:
            dataflow_params["filters"] = []
        
        dataflow_params["filters"].extend(dataflow_filters)
        
        return self.ingest_cloud_logging(dataflow_params)
    
    def process_logs(self, log_entries: List[Dict], processing_parameters: Dict = None) -> Dict:
        """
        Processes ingested logs with parsing and filtering.
        
        Args:
            log_entries: Log entries to process
            processing_parameters: Parameters for log processing
            
        Returns:
            Dictionary with processed logs and statistics
        """
        processing_parameters = processing_parameters or {}
        
        # Initialize result
        result = {
            "logs": [],
            "filtered": 0,
            "metrics_extracted": 0
        }
        
        # Parse logs if needed
        parser_name = processing_parameters.get("parser", "structured")
        parser = self._log_parsers.get(parser_name)
        
        parsed_logs = []
        for entry in log_entries:
            try:
                if parser and isinstance(entry, (dict, str)):
                    source = entry.get("source", "unknown") if isinstance(entry, dict) else "unknown"
                    parsed_entry = parser.parse(entry, source)
                    parsed_logs.append(parsed_entry)
                else:
                    # Use generic parsing
                    if isinstance(entry, dict) and "source" in entry:
                        source = entry["source"]
                    else:
                        source = "unknown"
                    
                    parsed_entry = parse_log_entry(entry, source)
                    parsed_logs.append(parsed_entry)
            except Exception as e:
                logger.warning(f"Error parsing log entry: {e}")
        
        # Apply filters if specified
        filtered_logs = parsed_logs
        
        if "filter" in processing_parameters:
            filter_spec = processing_parameters["filter"]
            
            if isinstance(filter_spec, str) and filter_spec in self._log_filters:
                # Use named filter
                log_filter = self._log_filters[filter_spec]
                filtered_logs = [
                    entry for entry in parsed_logs 
                    if log_filter.apply_filter(entry)
                ]
            elif isinstance(filter_spec, dict):
                # Use inline filter criteria
                filtered_logs = filter_logs_by_criteria(parsed_logs, filter_spec)
        
        # Update result
        result["logs"] = filtered_logs
        result["filtered"] = len(parsed_logs) - len(filtered_logs)
        
        # Extract metrics if requested
        if processing_parameters.get("extract_metrics", False):
            metrics = self.get_log_metrics(
                filtered_logs, 
                processing_parameters.get("metric_parameters", {})
            )
            result["metrics"] = metrics
            result["metrics_extracted"] = len(metrics)
        
        return result
    
    def store_logs(self, log_entries: List[Dict], storage_backends: List[str] = None) -> Dict:
        """
        Stores processed logs in configured storage backends.
        
        Args:
            log_entries: Log entries to store
            storage_backends: List of storage backends to use
            
        Returns:
            Dictionary with storage results and statistics
        """
        if not storage_backends:
            storage_backends = [self._config.get("default_storage", "bigquery")]
        
        if isinstance(storage_backends, str):
            storage_backends = [storage_backends]
        
        result = {
            "total_stored": 0,
            "backends": {}
        }
        
        # Store in each backend
        for backend in storage_backends:
            backend_result = {
                "stored": 0,
                "errors": []
            }
            
            try:
                if backend == "bigquery":
                    success = self.store_logs_bigquery(log_entries)
                    if success:
                        backend_result["stored"] = len(log_entries)
                    else:
                        backend_result["errors"].append("Failed to store logs in BigQuery")
                
                elif backend == "firestore":
                    success = self.store_logs_firestore(log_entries)
                    if success:
                        backend_result["stored"] = len(log_entries)
                    else:
                        backend_result["errors"].append("Failed to store logs in Firestore")
                
                else:
                    backend_result["errors"].append(f"Unknown storage backend: {backend}")
            
            except Exception as e:
                error_message = f"Error storing logs in {backend}: {str(e)}"
                logger.error(error_message)
                backend_result["errors"].append(error_message)
            
            # Update result
            result["backends"][backend] = backend_result
            result["total_stored"] += backend_result["stored"]
        
        return result
    
    def store_logs_bigquery(self, log_entries: List[Dict]) -> bool:
        """
        Stores logs in BigQuery for analysis.
        
        Args:
            log_entries: Log entries to store
            
        Returns:
            Success status
        """
        if not self._bigquery_client:
            logger.error("BigQuery client not initialized")
            return False
        
        if not log_entries:
            return True
        
        try:
            # Prepare logs for BigQuery
            records = []
            for entry in log_entries:
                record = {
                    "timestamp": entry.get("timestamp"),
                    "severity": entry.get("severity"),
                    "severity_value": entry.get("severity_value"),
                    "message": entry.get("message"),
                    "source": entry.get("source"),
                }
                
                # Add context as separate columns
                if "context" in entry and isinstance(entry["context"], dict):
                    for key, value in entry["context"].items():
                        record[f"context_{key}"] = value
                
                # Add data as JSON string
                if "data" in entry:
                    data = entry["data"]
                    if isinstance(data, dict) or isinstance(data, list):
                        record["data"] = json.dumps(data)
                    else:
                        record["data"] = str(data)
                
                records.append(record)
            
            # Get table name
            table_name = self._config.get("bigquery_table", LOGS_TABLE_NAME)
            
            # Insert logs in batches
            batch_size = self._config.get("batch_size", DEFAULT_BATCH_SIZE)
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                result = self._bigquery_client.insert_rows(table_name, batch)
                
                if result and isinstance(result, list) and result:
                    logger.error(f"Error inserting logs to BigQuery: {result}")
                    return False
            
            return True
        
        except Exception as e:
            logger.error(f"Error storing logs in BigQuery: {e}")
            return False
    
    def store_logs_firestore(self, log_entries: List[Dict]) -> bool:
        """
        Stores logs in Firestore for quick access.
        
        Args:
            log_entries: Log entries to store
            
        Returns:
            Success status
        """
        if not self._firestore_client:
            logger.error("Firestore client not initialized")
            return False
        
        if not log_entries:
            return True
        
        try:
            # Get collection name
            collection_name = self._config.get("firestore_collection", LOG_COLLECTION_NAME)
            
            # Insert logs in batches
            batch_size = self._config.get("batch_size", DEFAULT_BATCH_SIZE)
            
            for i in range(0, len(log_entries), batch_size):
                batch = log_entries[i:i+batch_size]
                
                # Create batch write
                batch_write = self._firestore_client.create_batch()
                
                for entry in batch:
                    # Create document ID from timestamp and hash
                    timestamp = entry.get("timestamp")
                    if isinstance(timestamp, datetime.datetime):
                        timestamp_str = timestamp.isoformat()
                    else:
                        timestamp_str = str(timestamp)
                    
                    msg_hash = hash(entry.get("message", ""))
                    doc_id = f"{timestamp_str}-{msg_hash}"
                    
                    # Add document to batch
                    batch_write.set(
                        self._firestore_client.get_document_ref(collection_name, doc_id),
                        entry
                    )
                
                # Commit batch
                batch_write.commit()
            
            return True
        
        except Exception as e:
            logger.error(f"Error storing logs in Firestore: {e}")
            return False
    
    def query_logs(self, query_parameters: Dict = None, limit: int = 1000, order_by: str = "timestamp DESC") -> pd.DataFrame:
        """
        Queries stored logs based on specified criteria.
        
        Args:
            query_parameters: Parameters for the query
            limit: Maximum number of logs to return
            order_by: Field to order results by
            
        Returns:
            Query results as DataFrame
        """
        query_parameters = query_parameters or {}
        
        # Determine query source
        source = query_parameters.get("source", self._config.get("default_storage", "bigquery"))
        
        if source == "bigquery":
            return self._query_logs_bigquery(query_parameters, limit, order_by)
        elif source == "firestore":
            return self._query_logs_firestore(query_parameters, limit, order_by)
        else:
            raise ValueError(f"Unsupported query source: {source}")
    
    def _query_logs_bigquery(self, query_parameters: Dict, limit: int, order_by: str) -> pd.DataFrame:
        """Query logs from BigQuery."""
        if not self._bigquery_client:
            logger.error("BigQuery client not initialized")
            return pd.DataFrame()
        
        try:
            # Get table name
            table_name = self._config.get("bigquery_table", LOGS_TABLE_NAME)
            
            # Build query
            query = f"SELECT * FROM `{table_name}` WHERE 1=1"
            
            # Add filters
            params = {}
            
            # Time range filter
            if "time_range" in query_parameters:
                time_range = query_parameters["time_range"]
                
                if "start" in time_range:
                    start_time = time_range["start"]
                    if isinstance(start_time, datetime.datetime):
                        start_time = start_time.isoformat()
                    query += " AND timestamp >= @start_time"
                    params["start_time"] = start_time
                
                if "end" in time_range:
                    end_time = time_range["end"]
                    if isinstance(end_time, datetime.datetime):
                        end_time = end_time.isoformat()
                    query += " AND timestamp <= @end_time"
                    params["end_time"] = end_time
            
            # Severity filter
            if "min_severity" in query_parameters:
                min_severity = query_parameters["min_severity"]
                min_severity_value = normalize_log_severity(min_severity, "")
                query += " AND severity_value >= @min_severity_value"
                params["min_severity_value"] = min_severity_value
            
            # Source filter
            if "source" in query_parameters:
                source_filter = query_parameters["source"]
                if isinstance(source_filter, list):
                    placeholders = []
                    for i, src in enumerate(source_filter):
                        param_name = f"source_{i}"
                        placeholders.append(f"@{param_name}")
                        params[param_name] = src
                    query += f" AND source IN ({', '.join(placeholders)})"
                else:
                    query += " AND source = @source_filter"
                    params["source_filter"] = source_filter
            
            # Message text filter
            if "message_contains" in query_parameters:
                msg_filter = query_parameters["message_contains"]
                query += " AND message LIKE @msg_filter"
                params["msg_filter"] = f"%{msg_filter}%"
            
            # Context filters
            for key, value in query_parameters.items():
                if key.startswith("context_"):
                    field_name = key  # The column is already prefixed
                    param_name = f"param_{key}"
                    query += f" AND {field_name} = @{param_name}"
                    params[param_name] = value
            
            # Add order by and limit
            query += f" ORDER BY {order_by} LIMIT {limit}"
            
            # Execute query
            df = self._bigquery_client.query_to_dataframe(query, params)
            return df
        
        except Exception as e:
            logger.error(f"Error querying logs from BigQuery: {e}")
            return pd.DataFrame()
    
    def _query_logs_firestore(self, query_parameters: Dict, limit: int, order_by: str) -> pd.DataFrame:
        """Query logs from Firestore."""
        if not self._firestore_client:
            logger.error("Firestore client not initialized")
            return pd.DataFrame()
        
        try:
            # Get collection name
            collection_name = self._config.get("firestore_collection", LOG_COLLECTION_NAME)
            
            # Start query
            query = self._firestore_client.get_collection_ref(collection_name)
            
            # Time range filter
            if "time_range" in query_parameters:
                time_range = query_parameters["time_range"]
                
                if "start" in time_range:
                    start_time = time_range["start"]
                    if isinstance(start_time, str):
                        try:
                            start_time = datetime.datetime.fromisoformat(start_time)
                        except ValueError:
                            pass
                    query = query.where("timestamp", ">=", start_time)
                
                if "end" in time_range:
                    end_time = time_range["end"]
                    if isinstance(end_time, str):
                        try:
                            end_time = datetime.datetime.fromisoformat(end_time)
                        except ValueError:
                            pass
                    query = query.where("timestamp", "<=", end_time)
            
            # Severity filter
            if "min_severity" in query_parameters:
                min_severity = query_parameters["min_severity"]
                min_severity_value = normalize_log_severity(min_severity, "")
                query = query.where("severity_value", ">=", min_severity_value)
            
            # Source filter
            if "source" in query_parameters:
                source_filter = query_parameters["source"]
                if isinstance(source_filter, list):
                    # Firestore doesn't support IN queries directly
                    # This would need to be handled with multiple queries or array-contains
                    query = query.where("source", "in", source_filter)
                else:
                    query = query.where("source", "==", source_filter)
            
            # Message text filter - not directly supported in Firestore
            # Would need to be handled with custom indexing or post-filtering
            
            # Context filters
            for key, value in query_parameters.items():
                if key.startswith("context_"):
                    field_name = key.replace("context_", "context.")
                    query = query.where(field_name, "==", value)
            
            # Add order by and limit
            field, direction = order_by.split()
            query = query.order_by(
                field, 
                direction="DESCENDING" if direction.upper() == "DESC" else "ASCENDING"
            )
            query = query.limit(limit)
            
            # Execute query
            docs = query.stream()
            results = [doc.to_dict() for doc in docs]
            
            return pd.DataFrame(results)
        
        except Exception as e:
            logger.error(f"Error querying logs from Firestore: {e}")
            return pd.DataFrame()
    
    def get_log_metrics(self, log_entries: List[Dict], metric_parameters: Dict = None) -> List[Dict]:
        """
        Extracts and processes metrics from logs.
        
        Args:
            log_entries: Log entries to analyze
            metric_parameters: Parameters for metric extraction
            
        Returns:
            List of extracted metrics
        """
        metric_parameters = metric_parameters or {}
        
        # Determine which patterns to use
        patterns = metric_parameters.get("patterns")
        if not patterns:
            # Use all configured patterns
            patterns = self._metric_patterns
        
        # Extract metrics
        metrics = extract_log_metrics(log_entries, patterns)
        
        # Process metrics if metric processor is available
        if self._metric_processor and metrics:
            try:
                # Apply additional processing
                aggregate = metric_parameters.get("aggregate", False)
                if aggregate:
                    metrics = self._metric_processor.aggregate_metrics(
                        metrics, 
                        metric_parameters.get("aggregate_config", {})
                    )
                
                # Apply transformations
                transform = metric_parameters.get("transform", False)
                if transform:
                    metrics = self._metric_processor.transform_metrics(
                        metrics, 
                        metric_parameters.get("transform_config", {})
                    )
            except Exception as e:
                logger.error(f"Error processing metrics: {e}")
        
        return metrics
    
    def register_log_parser(self, parser_name: str, parser) -> None:
        """
        Registers a custom log parser.
        
        Args:
            parser_name: Name to identify the parser
            parser: Parser instance implementing required interface
        """
        # Validate parser implements required interface
        if not hasattr(parser, "parse") or not callable(getattr(parser, "parse")):
            raise ValueError("Parser must implement parse method")
        
        self._log_parsers[parser_name] = parser
        logger.info(f"Registered log parser: {parser_name}")
    
    def register_log_filter(self, filter_name: str, filter_instance) -> None:
        """
        Registers a custom log filter.
        
        Args:
            filter_name: Name to identify the filter
            filter_instance: Filter instance implementing required interface
        """
        # Validate filter implements required interface
        if not hasattr(filter_instance, "apply_filter") or not callable(getattr(filter_instance, "apply_filter")):
            raise ValueError("Filter must implement apply_filter method")
        
        self._log_filters[filter_name] = filter_instance
        logger.info(f"Registered log filter: {filter_name}")
    
    def register_metric_pattern(self, pattern_name: str, pattern_config: Dict) -> None:
        """
        Registers a pattern for extracting metrics from logs.
        
        Args:
            pattern_name: Name to identify the pattern
            pattern_config: Pattern configuration dictionary
        """
        # Validate pattern configuration
        required_fields = ["pattern", "metric_name"]
        for field in required_fields:
            if field not in pattern_config:
                raise ValueError(f"Pattern configuration must include {field}")
        
        # Compile regex if it's a string
        if isinstance(pattern_config["pattern"], str):
            try:
                pattern_config["pattern"] = re.compile(pattern_config["pattern"])
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")
        
        # Add to patterns
        self._metric_patterns[pattern_name] = pattern_config
        logger.info(f"Registered metric pattern: {pattern_name}")
    
    def cleanup_old_logs(self, days: int = None) -> int:
        """
        Removes logs older than the retention period.
        
        Args:
            days: Number of days to retain logs (defaults to configured value)
            
        Returns:
            Number of log records removed
        """
        # Use configured retention period if not specified
        retention_days = days if days is not None else self._config.get("log_retention_days", DEFAULT_LOG_RETENTION_DAYS)
        
        # Calculate cutoff date
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
        cutoff_date_str = cutoff_date.isoformat()
        
        count = 0
        
        # Cleanup BigQuery logs
        if self._bigquery_client:
            try:
                # Get table name
                table_name = self._config.get("bigquery_table", LOGS_TABLE_NAME)
                
                # Delete old logs
                query = f"DELETE FROM `{table_name}` WHERE timestamp < @cutoff"
                params = {"cutoff": cutoff_date_str}
                
                result = self._bigquery_client.execute_query(query, params)
                
                # Update count if available
                if hasattr(result, "num_dml_affected_rows"):
                    count += result.num_dml_affected_rows
                
                logger.info(f"Deleted {result.num_dml_affected_rows} log records from BigQuery")
            
            except Exception as e:
                logger.error(f"Error cleaning up BigQuery logs: {e}")
        
        # Cleanup Firestore logs
        if self._firestore_client:
            try:
                # Get collection name
                collection_name = self._config.get("firestore_collection", LOG_COLLECTION_NAME)
                
                # Query old logs
                collection_ref = self._firestore_client.get_collection_ref(collection_name)
                query = collection_ref.where("timestamp", "<", cutoff_date)
                
                # Delete in batches
                batch_size = self._config.get("batch_size", DEFAULT_BATCH_SIZE)
                docs = list(query.limit(batch_size).stream())
                
                while docs:
                    batch = self._firestore_client.create_batch()
                    
                    for doc in docs:
                        batch.delete(doc.reference)
                        count += 1
                    
                    batch.commit()
                    
                    # Get next batch
                    docs = list(query.limit(batch_size).stream())
                
                logger.info(f"Deleted {count} log records from Firestore")
            
            except Exception as e:
                logger.error(f"Error cleaning up Firestore logs: {e}")
        
        return count