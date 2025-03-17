"""
Initialization file for the database repositories package that exports all repository classes for the self-healing data pipeline. 
This file provides a centralized access point for all data access repositories, simplifying imports throughout the application.
"""

from .alert_repository import AlertRepository  # Import repository for managing alerts
from .execution_repository import ExecutionRepository  # Import repository for managing pipeline and task executions
from .healing_repository import HealingRepository  # Import repository for managing self-healing actions and executions
from .metrics_repository import MetricsRepository  # Import repository for managing pipeline metrics
from .pipeline_repository import PipelineRepository  # Import repository for managing pipeline definitions
from .quality_repository import QualityRepository  # Import repository for managing data quality rules and validations
from .source_repository import SourceRepository  # Import repository for managing data sources

__all__ = [
    "AlertRepository",  # Repository for managing alerts in the self-healing data pipeline
    "ExecutionRepository",  # Repository for managing pipeline and task execution data in BigQuery
    "HealingRepository",  # Repository for managing healing actions, issue patterns, and healing executions
    "MetricsRepository",  # Repository for managing pipeline metrics and performance data
    "PipelineRepository",  # Repository for managing pipeline definitions and configurations
    "QualityRepository",  # Repository for managing quality rules and validation results
    "SourceRepository",  # Repository for managing source system configurations
]