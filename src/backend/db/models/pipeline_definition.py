"""
Defines the PipelineDefinition model class that represents data pipeline configurations
in the self-healing data pipeline system. This model stores essential information about
pipeline structure, data sources, targets, scheduling, and quality validation rules.

The PipelineDefinition serves as the central configuration unit for each data pipeline 
in the system, providing a complete specification for how data should be extracted,
validated, transformed, and loaded. It also includes self-healing settings and 
performance optimization parameters.
"""

import datetime
import json
import uuid
from typing import Dict, List, Optional, Any, Tuple

from ...constants import (
    DataSourceType,
    PipelineType,
    ScheduleInterval
)
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Configure module logger
logger = get_logger(__name__)

# Table name for pipeline definitions in BigQuery
PIPELINE_DEFINITION_TABLE_NAME = "pipeline_definitions"


def generate_pipeline_id() -> str:
    """
    Generates a unique identifier for a pipeline definition.
    
    Returns:
        str: Unique pipeline ID with 'pipe_' prefix
    """
    return f"pipe_{uuid.uuid4().hex}"


def get_pipeline_definition_table_schema() -> List[SchemaField]:
    """
    Returns the BigQuery table schema for the pipeline definitions table.
    
    Returns:
        List of SchemaField objects defining the table schema
    """
    return [
        get_schema_field("pipeline_id", "STRING", "REQUIRED", "Unique identifier for the pipeline"),
        get_schema_field("name", "STRING", "REQUIRED", "Display name of the pipeline"),
        get_schema_field("description", "STRING", "NULLABLE", "Description of the pipeline"),
        get_schema_field("pipeline_type", "STRING", "REQUIRED", "Type of pipeline"),
        get_schema_field("source_id", "STRING", "REQUIRED", "Source system identifier"),
        get_schema_field("target_dataset", "STRING", "REQUIRED", "Target BigQuery dataset"),
        get_schema_field("target_table", "STRING", "REQUIRED", "Target BigQuery table"),
        get_schema_field("transformation_config", "STRING", "NULLABLE", "JSON configuration for data transformations"),
        get_schema_field("quality_config", "STRING", "NULLABLE", "JSON configuration for data quality rules"),
        get_schema_field("self_healing_config", "STRING", "NULLABLE", "JSON configuration for self-healing settings"),
        get_schema_field("scheduling_config", "STRING", "NULLABLE", "JSON configuration for scheduling"),
        get_schema_field("execution_config", "STRING", "NULLABLE", "JSON configuration for execution parameters"),
        get_schema_field("performance_config", "STRING", "NULLABLE", "JSON configuration for performance settings"),
        get_schema_field("is_active", "BOOLEAN", "REQUIRED", "Whether the pipeline is active"),
        get_schema_field("created_at", "TIMESTAMP", "REQUIRED", "When the pipeline was created"),
        get_schema_field("updated_at", "TIMESTAMP", "REQUIRED", "When the pipeline was last updated"),
        get_schema_field("created_by", "STRING", "REQUIRED", "User who created the pipeline"),
        get_schema_field("updated_by", "STRING", "NULLABLE", "User who last updated the pipeline"),
        get_schema_field("metadata", "STRING", "NULLABLE", "JSON additional metadata"),
        get_schema_field("dag_id", "STRING", "NULLABLE", "Associated Airflow DAG ID"),
        get_schema_field("quality_rule_ids", "STRING", "NULLABLE", "JSON array of quality rule IDs"),
    ]


class PipelineDefinition:
    """
    Model class representing a data pipeline configuration with source, target, and processing settings.
    
    This class encapsulates all configurations required to define and execute a data pipeline,
    including source and target information, transformation logic, quality validation rules,
    self-healing settings, scheduling parameters, and execution configuration.
    """
    
    def __init__(
        self,
        name: str,
        pipeline_type: PipelineType,
        source_id: str,
        target_dataset: str,
        target_table: str,
        description: str = "",
        pipeline_id: str = None,
        transformation_config: Dict[str, Any] = None,
        quality_config: Dict[str, Any] = None,
        self_healing_config: Dict[str, Any] = None,
        scheduling_config: Dict[str, Any] = None,
        execution_config: Dict[str, Any] = None,
        performance_config: Dict[str, Any] = None,
        is_active: bool = True,
        created_by: str = None,
        metadata: Dict[str, Any] = None,
        dag_id: str = None,
        quality_rule_ids: List[str] = None
    ):
        """
        Initialize a new pipeline definition with provided parameters.
        
        Args:
            name: Display name of the pipeline
            pipeline_type: Type of pipeline (from PipelineType enum)
            source_id: Source system identifier
            target_dataset: Target BigQuery dataset
            target_table: Target BigQuery table
            description: Description of the pipeline
            pipeline_id: Unique identifier (generated if not provided)
            transformation_config: Configuration for data transformations
            quality_config: Configuration for data quality rules
            self_healing_config: Configuration for self-healing settings
            scheduling_config: Configuration for scheduling
            execution_config: Configuration for execution parameters
            performance_config: Configuration for performance settings
            is_active: Whether the pipeline is active
            created_by: User who created the pipeline
            metadata: Additional metadata
            dag_id: Associated Airflow DAG ID
            quality_rule_ids: List of quality rule IDs associated with this pipeline
        """
        self.pipeline_id = pipeline_id or generate_pipeline_id()
        self.name = name
        self.description = description
        self.pipeline_type = pipeline_type
        self.source_id = source_id
        self.target_dataset = target_dataset
        self.target_table = target_table
        self.transformation_config = transformation_config or {}
        self.quality_config = quality_config or {}
        self.self_healing_config = self_healing_config or {}
        self.scheduling_config = scheduling_config or {}
        self.execution_config = execution_config or {}
        self.performance_config = performance_config or {}
        self.is_active = is_active
        self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        self.created_by = created_by
        self.updated_by = created_by
        self.metadata = metadata or {}
        self.dag_id = dag_id or f"dag_{self.name.lower().replace(' ', '_')}"
        self.quality_rule_ids = quality_rule_ids or []
        
        logger.info(f"Created new pipeline definition: {self.pipeline_id} - {self.name}")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the pipeline definition to a dictionary representation.
        
        Returns:
            Dict[str, Any]: Dictionary representation of the pipeline definition
        """
        return {
            "pipeline_id": self.pipeline_id,
            "name": self.name,
            "description": self.description,
            "pipeline_type": self.pipeline_type.value if isinstance(self.pipeline_type, PipelineType) else self.pipeline_type,
            "source_id": self.source_id,
            "target_dataset": self.target_dataset,
            "target_table": self.target_table,
            "transformation_config": self.transformation_config,
            "quality_config": self.quality_config,
            "self_healing_config": self.self_healing_config,
            "scheduling_config": self.scheduling_config,
            "execution_config": self.execution_config,
            "performance_config": self.performance_config,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime.datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if isinstance(self.updated_at, datetime.datetime) else self.updated_at,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "metadata": self.metadata,
            "dag_id": self.dag_id,
            "quality_rule_ids": self.quality_rule_ids
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineDefinition':
        """
        Create a PipelineDefinition instance from a dictionary.
        
        Args:
            data: Dictionary containing pipeline definition data
            
        Returns:
            PipelineDefinition: New PipelineDefinition instance
        """
        # Handle enum conversion
        if 'pipeline_type' in data and not isinstance(data['pipeline_type'], PipelineType):
            try:
                data['pipeline_type'] = PipelineType(data['pipeline_type'])
            except ValueError:
                logger.warning(f"Invalid pipeline_type value: {data['pipeline_type']}")
        
        # Handle datetime conversion
        for field in ['created_at', 'updated_at']:
            if field in data and isinstance(data[field], str):
                try:
                    data[field] = datetime.datetime.fromisoformat(data[field])
                except ValueError:
                    logger.warning(f"Invalid datetime format for {field}: {data[field]}")
        
        return cls(
            name=data.get('name'),
            pipeline_type=data.get('pipeline_type'),
            source_id=data.get('source_id'),
            target_dataset=data.get('target_dataset'),
            target_table=data.get('target_table'),
            description=data.get('description', ''),
            pipeline_id=data.get('pipeline_id'),
            transformation_config=data.get('transformation_config', {}),
            quality_config=data.get('quality_config', {}),
            self_healing_config=data.get('self_healing_config', {}),
            scheduling_config=data.get('scheduling_config', {}),
            execution_config=data.get('execution_config', {}),
            performance_config=data.get('performance_config', {}),
            is_active=data.get('is_active', True),
            created_by=data.get('created_by'),
            metadata=data.get('metadata', {}),
            dag_id=data.get('dag_id'),
            quality_rule_ids=data.get('quality_rule_ids', [])
        )
    
    @classmethod
    def from_bigquery_row(cls, row: Dict[str, Any]) -> 'PipelineDefinition':
        """
        Create a PipelineDefinition instance from a BigQuery row.
        
        Args:
            row: Dictionary containing BigQuery table row data
            
        Returns:
            PipelineDefinition: New PipelineDefinition instance
        """
        data = dict(row)
        
        # Parse JSON fields
        for field in [
            'transformation_config', 'quality_config', 'self_healing_config',
            'scheduling_config', 'execution_config', 'performance_config',
            'metadata', 'quality_rule_ids'
        ]:
            if field in data and isinstance(data[field], str):
                try:
                    data[field] = json.loads(data[field])
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON for {field}")
                    data[field] = {}
        
        # Convert string pipeline_type to enum
        if 'pipeline_type' in data and isinstance(data['pipeline_type'], str):
            try:
                data['pipeline_type'] = PipelineType(data['pipeline_type'])
            except ValueError:
                logger.warning(f"Invalid pipeline_type value: {data['pipeline_type']}")
        
        return cls.from_dict(data)
    
    def to_bigquery_row(self) -> Dict[str, Any]:
        """
        Convert the pipeline definition to a format suitable for BigQuery insertion.
        
        Returns:
            Dict[str, Any]: Dictionary formatted for BigQuery insertion
        """
        row = self.to_dict()
        
        # Convert complex objects to JSON strings
        for field in [
            'transformation_config', 'quality_config', 'self_healing_config',
            'scheduling_config', 'execution_config', 'performance_config',
            'metadata', 'quality_rule_ids'
        ]:
            if field in row and not isinstance(row[field], str):
                row[field] = json.dumps(row[field])
        
        return row
    
    def update(self, update_data: Dict[str, Any], updated_by: str) -> None:
        """
        Update the pipeline definition with new values.
        
        Args:
            update_data: Dictionary containing the fields to update
            updated_by: User making the update
        """
        # Check for updateable fields and apply changes
        updateable_fields = [
            'name', 'description', 'pipeline_type', 'source_id',
            'target_dataset', 'target_table', 'is_active', 'dag_id'
        ]
        
        for field in updateable_fields:
            if field in update_data:
                setattr(self, field, update_data[field])
        
        # Handle special updates for config dictionaries
        config_fields = [
            'transformation_config', 'quality_config', 'self_healing_config',
            'scheduling_config', 'execution_config', 'performance_config',
            'metadata'
        ]
        
        for field in config_fields:
            if field in update_data and isinstance(update_data[field], dict):
                current = getattr(self, field) or {}
                current.update(update_data[field])
                setattr(self, field, current)
        
        # Update quality_rule_ids if provided
        if 'quality_rule_ids' in update_data and isinstance(update_data['quality_rule_ids'], list):
            self.quality_rule_ids = update_data['quality_rule_ids']
        
        # Update metadata
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        
        logger.info(f"Updated pipeline definition: {self.pipeline_id} by {updated_by}")
    
    def activate(self, updated_by: str) -> None:
        """
        Activate the pipeline definition.
        
        Args:
            updated_by: User activating the pipeline
        """
        self.is_active = True
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Activated pipeline definition: {self.pipeline_id} by {updated_by}")
    
    def deactivate(self, updated_by: str) -> None:
        """
        Deactivate the pipeline definition.
        
        Args:
            updated_by: User deactivating the pipeline
        """
        self.is_active = False
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Deactivated pipeline definition: {self.pipeline_id} by {updated_by}")
    
    def update_transformation_config(self, transformation_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the transformation configuration for the pipeline.
        
        Args:
            transformation_config: New transformation configuration
            updated_by: User updating the configuration
        """
        self.transformation_config = transformation_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated transformation configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_quality_config(self, quality_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the quality validation configuration for the pipeline.
        
        Args:
            quality_config: New quality validation configuration
            updated_by: User updating the configuration
        """
        self.quality_config = quality_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated quality configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_self_healing_config(self, self_healing_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the self-healing configuration for the pipeline.
        
        Args:
            self_healing_config: New self-healing configuration
            updated_by: User updating the configuration
        """
        self.self_healing_config = self_healing_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated self-healing configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_scheduling_config(self, scheduling_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the scheduling configuration for the pipeline.
        
        Args:
            scheduling_config: New scheduling configuration
            updated_by: User updating the configuration
        """
        self.scheduling_config = scheduling_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated scheduling configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_execution_config(self, execution_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the execution configuration for the pipeline.
        
        Args:
            execution_config: New execution configuration
            updated_by: User updating the configuration
        """
        self.execution_config = execution_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated execution configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_performance_config(self, performance_config: Dict[str, Any], updated_by: str) -> None:
        """
        Update the performance configuration for the pipeline.
        
        Args:
            performance_config: New performance configuration
            updated_by: User updating the configuration
        """
        self.performance_config = performance_config
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated performance configuration for pipeline: {self.pipeline_id} by {updated_by}")
    
    def add_quality_rule(self, rule_id: str, updated_by: str) -> None:
        """
        Add a quality rule to the pipeline.
        
        Args:
            rule_id: ID of the quality rule to add
            updated_by: User adding the rule
        """
        if rule_id not in self.quality_rule_ids:
            self.quality_rule_ids.append(rule_id)
            self.updated_at = datetime.datetime.now()
            self.updated_by = updated_by
            logger.info(f"Added quality rule {rule_id} to pipeline: {self.pipeline_id} by {updated_by}")
    
    def remove_quality_rule(self, rule_id: str, updated_by: str) -> None:
        """
        Remove a quality rule from the pipeline.
        
        Args:
            rule_id: ID of the quality rule to remove
            updated_by: User removing the rule
        """
        if rule_id in self.quality_rule_ids:
            self.quality_rule_ids.remove(rule_id)
            self.updated_at = datetime.datetime.now()
            self.updated_by = updated_by
            logger.info(f"Removed quality rule {rule_id} from pipeline: {self.pipeline_id} by {updated_by}")
    
    def update_metadata(self, metadata: Dict[str, Any], updated_by: str) -> None:
        """
        Update the pipeline metadata.
        
        Args:
            metadata: New metadata dictionary
            updated_by: User updating the metadata
        """
        self.metadata = metadata
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated metadata for pipeline: {self.pipeline_id} by {updated_by}")
    
    def get_dag_config(self) -> Dict[str, Any]:
        """
        Get the Airflow DAG configuration for the pipeline.
        
        Returns:
            Dict[str, Any]: DAG configuration dictionary
        """
        # Base DAG configuration
        dag_config = {
            "dag_id": self.dag_id,
            "pipeline_id": self.pipeline_id,
            "description": self.description,
            "is_active": self.is_active,
            "pipeline_type": self.pipeline_type.value if isinstance(self.pipeline_type, PipelineType) else self.pipeline_type,
        }
        
        # Add scheduling configuration
        dag_config["scheduling"] = self.scheduling_config
        
        # Add source and target information
        dag_config["source"] = {
            "source_id": self.source_id
        }
        
        dag_config["target"] = {
            "dataset": self.target_dataset,
            "table": self.target_table
        }
        
        # Add configuration sections
        dag_config["transformation"] = self.transformation_config
        dag_config["quality"] = self.quality_config
        dag_config["self_healing"] = self.self_healing_config
        dag_config["execution"] = self.execution_config
        dag_config["performance"] = self.performance_config
        
        return dag_config
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate the pipeline definition for completeness and correctness.
        
        Returns:
            Tuple[bool, List[str]]: Validation result and list of validation errors
        """
        errors = []
        
        # Validate required fields
        required_fields = [
            ('pipeline_id', 'Pipeline ID is required'),
            ('name', 'Pipeline name is required'),
            ('pipeline_type', 'Pipeline type is required'),
            ('source_id', 'Source ID is required'),
            ('target_dataset', 'Target dataset is required'),
            ('target_table', 'Target table is required')
        ]
        
        for field, error_msg in required_fields:
            if not getattr(self, field):
                errors.append(error_msg)
        
        # Validate pipeline_type is a valid enum
        if hasattr(self, 'pipeline_type') and self.pipeline_type:
            if not isinstance(self.pipeline_type, PipelineType) and not any(
                self.pipeline_type == t.value for t in PipelineType
            ):
                errors.append(f"Invalid pipeline type: {self.pipeline_type}")
        
        # Validate configuration structure
        config_fields = [
            ('transformation_config', 'Transformation configuration must be a dictionary'),
            ('quality_config', 'Quality configuration must be a dictionary'),
            ('self_healing_config', 'Self-healing configuration must be a dictionary'),
            ('scheduling_config', 'Scheduling configuration must be a dictionary'),
            ('execution_config', 'Execution configuration must be a dictionary'),
            ('performance_config', 'Performance configuration must be a dictionary'),
            ('metadata', 'Metadata must be a dictionary')
        ]
        
        for field, error_msg in config_fields:
            if getattr(self, field) and not isinstance(getattr(self, field), dict):
                errors.append(error_msg)
        
        # Validate quality_rule_ids is a list
        if self.quality_rule_ids and not isinstance(self.quality_rule_ids, list):
            errors.append("Quality rule IDs must be a list")
        
        return len(errors) == 0, errors