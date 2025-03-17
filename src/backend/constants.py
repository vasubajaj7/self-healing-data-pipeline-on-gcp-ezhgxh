"""
Constants and enumerations used throughout the self-healing data pipeline.

This module provides centralized definitions for:
- Environment identifiers
- Default configuration values
- Status constants
- Operational enumerations
- Alert categories
- And other common constants

Using these standardized constants promotes consistency across the application
and simplifies configuration management.
"""

import enum
import os
from pathlib import Path

# Environment Constants
ENV_DEVELOPMENT = "development"
ENV_STAGING = "staging"
ENV_PRODUCTION = "production"

# Default Configuration
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', 'default_config.yaml')

# GCP Constants
GCP_PROJECT_ID_ENV_VAR = "GCP_PROJECT_ID"
GCP_LOCATION_ENV_VAR = "GCP_LOCATION"
GCP_DEFAULT_LOCATION = "us-central1"

# Default Settings
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_CONFIDENCE_THRESHOLD = 0.85
DEFAULT_MAX_RETRY_ATTEMPTS = 3

# Pipeline Status Constants
PIPELINE_STATUS_PENDING = "PENDING"
PIPELINE_STATUS_RUNNING = "RUNNING"
PIPELINE_STATUS_SUCCESS = "SUCCESS"
PIPELINE_STATUS_FAILED = "FAILED"
PIPELINE_STATUS_HEALING = "HEALING"

# Task Status Constants
TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_RUNNING = "RUNNING"
TASK_STATUS_SUCCESS = "SUCCESS"
TASK_STATUS_FAILED = "FAILED"
TASK_STATUS_SKIPPED = "SKIPPED"
TASK_STATUS_UPSTREAM_FAILED = "UPSTREAM_FAILED"

# Validation Status Constants
VALIDATION_STATUS_PASSED = "PASSED"
VALIDATION_STATUS_FAILED = "FAILED"
VALIDATION_STATUS_WARNING = "WARNING"

# Healing Status Constants
HEALING_STATUS_PENDING = "PENDING"
HEALING_STATUS_IN_PROGRESS = "IN_PROGRESS"
HEALING_STATUS_SUCCESS = "SUCCESS"
HEALING_STATUS_FAILED = "FAILED"
HEALING_STATUS_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
HEALING_STATUS_APPROVED = "APPROVED"
HEALING_STATUS_REJECTED = "REJECTED"

# Alert Status Constants
ALERT_STATUS_ACTIVE = "ACTIVE"
ALERT_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
ALERT_STATUS_RESOLVED = "RESOLVED"
ALERT_STATUS_SUPPRESSED = "SUPPRESSED"

# Notification Types
NOTIFICATION_TYPE_EMAIL = "EMAIL"
NOTIFICATION_TYPE_TEAMS = "TEAMS"
NOTIFICATION_TYPE_SMS = "SMS"
NOTIFICATION_TYPE_WEBHOOK = "WEBHOOK"

# Metric Types
METRIC_TYPE_COUNTER = "COUNTER"
METRIC_TYPE_GAUGE = "GAUGE"
METRIC_TYPE_HISTOGRAM = "HISTOGRAM"
METRIC_TYPE_SUMMARY = "SUMMARY"

# Rule Types
RULE_TYPE_THRESHOLD = "THRESHOLD"
RULE_TYPE_TREND = "TREND"
RULE_TYPE_ANOMALY = "ANOMALY"
RULE_TYPE_COMPOUND = "COMPOUND"
RULE_TYPE_EVENT = "EVENT"
RULE_TYPE_PATTERN = "PATTERN"

# Operators
OPERATOR_EQUAL = "=="
OPERATOR_NOT_EQUAL = "!="
OPERATOR_GREATER_THAN = ">"
OPERATOR_GREATER_EQUAL = ">="
OPERATOR_LESS_THAN = "<"
OPERATOR_LESS_EQUAL = "<="
OPERATOR_CONTAINS = "CONTAINS"
OPERATOR_NOT_CONTAINS = "NOT_CONTAINS"
OPERATOR_MATCHES = "MATCHES"
OPERATOR_NOT_MATCHES = "NOT_MATCHES"

# Logical Operators
LOGICAL_AND = "AND"
LOGICAL_OR = "OR"
LOGICAL_NOT = "NOT"


class SelfHealingMode(enum.Enum):
    """Enumeration of possible operational modes for the self-healing system."""
    DISABLED = "DISABLED"
    RECOMMENDATION_ONLY = "RECOMMENDATION_ONLY"
    SEMI_AUTOMATIC = "SEMI_AUTOMATIC"
    AUTOMATIC = "AUTOMATIC"


class HealingActionType(enum.Enum):
    """Enumeration of possible healing action types."""
    DATA_CORRECTION = "DATA_CORRECTION"
    PIPELINE_RETRY = "PIPELINE_RETRY"
    PARAMETER_ADJUSTMENT = "PARAMETER_ADJUSTMENT"
    RESOURCE_SCALING = "RESOURCE_SCALING"
    SCHEMA_EVOLUTION = "SCHEMA_EVOLUTION"
    DEPENDENCY_RESOLUTION = "DEPENDENCY_RESOLUTION"


class AlertSeverity(enum.Enum):
    """Enumeration of possible alert severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH" 
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class DataSourceType(enum.Enum):
    """Enumeration of possible data source types."""
    GCS = "GCS"
    CLOUD_SQL = "CLOUD_SQL"
    BIGQUERY = "BIGQUERY"
    API = "API"
    SFTP = "SFTP"
    CUSTOM = "CUSTOM"


class FileFormat(enum.Enum):
    """Enumeration of supported file formats."""
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"
    XML = "XML"
    TEXT = "TEXT"


class QualityDimension(enum.Enum):
    """Enumeration of data quality dimensions."""
    COMPLETENESS = "COMPLETENESS"
    ACCURACY = "ACCURACY"
    CONSISTENCY = "CONSISTENCY"
    VALIDITY = "VALIDITY"
    TIMELINESS = "TIMELINESS"
    UNIQUENESS = "UNIQUENESS"