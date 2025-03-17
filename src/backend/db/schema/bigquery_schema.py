"""
Provides utilities for defining, managing, and manipulating BigQuery table schemas in the self-healing data pipeline.

This module includes functions for schema creation, validation, conversion, and evolution to support 
the data quality and self-healing capabilities of the pipeline.
"""

import typing
import json
import datetime
import copy
from google.cloud.bigquery import SchemaField as BQSchemaField

from ...constants import DEFAULT_FIELD_MODE
from ...utils.logging.logger import get_logger

# Initialize module logger
logger = get_logger(__name__)

# Mapping from Python types to BigQuery types
FIELD_TYPE_MAPPING = {
    'str': 'STRING',
    'int': 'INTEGER',
    'float': 'FLOAT',
    'bool': 'BOOLEAN',
    'datetime.datetime': 'TIMESTAMP',
    'datetime.date': 'DATE',
    'datetime.time': 'TIME',
    'bytes': 'BYTES',
    'dict': 'RECORD',
    'list': 'RECORD'
}


def get_schema_field(
    name: str, 
    field_type: str, 
    mode: str = None, 
    description: str = None, 
    fields: list = None
) -> BQSchemaField:
    """
    Creates a BigQuery SchemaField with simplified interface.

    Args:
        name: Field name
        field_type: BigQuery field type (STRING, INTEGER, etc.)
        mode: Field mode (NULLABLE, REQUIRED, REPEATED), defaults to NULLABLE
        description: Field description, defaults to empty string
        fields: List of subfields for RECORD type, defaults to None

    Returns:
        BigQuery schema field object
    """
    # Set default values
    mode = mode or DEFAULT_FIELD_MODE
    description = description or ""
    
    # Create and return the SchemaField
    return BQSchemaField(
        name=name,
        field_type=field_type,
        mode=mode,
        description=description,
        fields=fields
    )


def schema_to_json(schema: list) -> list:
    """
    Converts a BigQuery schema to JSON format.

    Args:
        schema: List of BigQuery SchemaField objects

    Returns:
        JSON representation of the schema
    """
    json_schema = []
    for field in schema:
        field_json = {
            'name': field.name,
            'type': field.field_type,
            'mode': field.mode,
            'description': field.description
        }
        
        # Add nested fields if present
        if field.fields:
            field_json['fields'] = schema_to_json(field.fields)
        
        json_schema.append(field_json)
    
    return json_schema


def json_to_schema(json_schema: list) -> list:
    """
    Converts a JSON schema definition to BigQuery SchemaField objects.

    Args:
        json_schema: List of field definitions in JSON format

    Returns:
        List of SchemaField objects
    """
    schema = []
    for field_def in json_schema:
        name = field_def['name']
        field_type = field_def['type']
        mode = field_def.get('mode', DEFAULT_FIELD_MODE)
        description = field_def.get('description', '')
        
        # Handle nested fields for RECORD type
        nested_fields = None
        if 'fields' in field_def and field_def['fields']:
            nested_fields = json_to_schema(field_def['fields'])
        
        # Create SchemaField and add to schema list
        schema.append(
            BQSchemaField(
                name=name,
                field_type=field_type,
                mode=mode,
                description=description,
                fields=nested_fields
            )
        )
    
    return schema


def merge_schemas(
    base_schema: list, 
    new_schema: list, 
    allow_field_addition: bool = True, 
    allow_field_relaxation: bool = True
) -> list:
    """
    Merges two schemas with conflict resolution.

    Args:
        base_schema: Base schema as list of SchemaField objects
        new_schema: New schema to merge as list of SchemaField objects
        allow_field_addition: Whether to allow adding new fields from new_schema
        allow_field_relaxation: Whether to allow relaxing field modes (e.g., REQUIRED to NULLABLE)

    Returns:
        Merged schema as list of SchemaField objects
    """
    # Convert schemas to dictionaries for easier manipulation
    base_dict = {field.name: field for field in base_schema}
    new_dict = {field.name: field for field in new_schema}
    
    # Initialize the merged schema dictionary
    merged_dict = {}
    
    # Add all fields from base schema
    for name, field in base_dict.items():
        merged_dict[name] = field
    
    # Process fields from new schema
    for name, field in new_dict.items():
        if name in base_dict:
            # Field exists in both schemas - check compatibility and merge
            base_field = base_dict[name]
            
            # Check type compatibility
            if base_field.field_type != field.field_type:
                logger.warning(f"Field type conflict for {name}: {base_field.field_type} vs {field.field_type}")
                # Keep the base field type
                continue
            
            # Check mode compatibility and apply field relaxation if allowed
            if base_field.mode == 'REQUIRED' and field.mode == 'NULLABLE':
                if allow_field_relaxation:
                    logger.info(f"Relaxing field mode for {name} from REQUIRED to NULLABLE")
                    merged_dict[name] = field
                else:
                    logger.warning(f"Cannot relax field mode for {name} from REQUIRED to NULLABLE")
            elif base_field.mode == 'NULLABLE' and field.mode == 'REQUIRED':
                # Stricter mode is acceptable
                merged_dict[name] = field
            
            # For RECORD type, recursively merge nested fields
            if base_field.field_type == 'RECORD' and field.field_type == 'RECORD':
                merged_fields = merge_schemas(
                    base_field.fields or [], 
                    field.fields or [], 
                    allow_field_addition, 
                    allow_field_relaxation
                )
                merged_dict[name] = BQSchemaField(
                    name=name,
                    field_type='RECORD',
                    mode=merged_dict[name].mode,
                    description=field.description or base_field.description,
                    fields=merged_fields
                )
        
        elif allow_field_addition:
            # Field only exists in new schema - add it if allowed
            logger.info(f"Adding new field {name} from new schema")
            merged_dict[name] = field
        
        else:
            # Field only exists in new schema but additions not allowed
            logger.warning(f"Field {name} exists only in new schema but field addition not allowed")
    
    # Convert merged dictionary back to list of SchemaField objects
    return list(merged_dict.values())


def validate_schema_compatibility(
    existing_schema: list, 
    new_schema: list, 
    allow_field_addition: bool = True, 
    allow_field_relaxation: bool = True
) -> typing.Tuple[bool, str]:
    """
    Validates that a new schema is compatible with an existing schema.

    Args:
        existing_schema: Existing schema as list of SchemaField objects
        new_schema: New schema as list of SchemaField objects
        allow_field_addition: Whether to allow adding new fields
        allow_field_relaxation: Whether to allow relaxing field modes

    Returns:
        Tuple of (bool, str) - Compatibility status and error message if incompatible
    """
    # Convert schemas to dictionaries for easier field access
    existing_dict = {field.name: field for field in existing_schema}
    new_dict = {field.name: field for field in new_schema}
    
    # Check each field in new schema for compatibility
    for name, new_field in new_dict.items():
        if name in existing_dict:
            # Field exists in both schemas - check compatibility
            existing_field = existing_dict[name]
            
            # Check type compatibility
            if existing_field.field_type != new_field.field_type:
                return False, f"Field type conflict: {name} has type {existing_field.field_type} in existing schema but {new_field.field_type} in new schema"
            
            # Check mode compatibility
            if existing_field.mode == 'NULLABLE' and new_field.mode == 'REQUIRED':
                # This is acceptable - making a field stricter
                pass
            elif existing_field.mode == 'REQUIRED' and new_field.mode == 'NULLABLE':
                # This requires relaxation permission
                if not allow_field_relaxation:
                    return False, f"Field mode conflict: {name} cannot be relaxed from REQUIRED to NULLABLE"
            
            # For RECORD type, recursively validate nested fields
            if existing_field.field_type == 'RECORD' and new_field.field_type == 'RECORD':
                nested_compatible, nested_error = validate_schema_compatibility(
                    existing_field.fields or [], 
                    new_field.fields or [], 
                    allow_field_addition, 
                    allow_field_relaxation
                )
                if not nested_compatible:
                    return False, f"Nested field incompatibility in {name}: {nested_error}"
        
        elif not allow_field_addition:
            # Field only exists in new schema but additions not allowed
            return False, f"Field {name} does not exist in existing schema and field addition not allowed"
    
    # All checks passed
    return True, ""


def get_field_by_name(schema: list, field_name: str) -> typing.Optional[BQSchemaField]:
    """
    Retrieves a field from a schema by name, including nested fields.

    Args:
        schema: List of SchemaField objects
        field_name: Name of the field to find

    Returns:
        The found field or None if not found
    """
    # Check each field at this level
    for field in schema:
        if field.name == field_name:
            return field
        
        # If field is a RECORD type, search its nested fields
        if field.field_type == 'RECORD' and field.fields:
            nested_field = get_field_by_name(field.fields, field_name)
            if nested_field:
                return nested_field
    
    # Field not found
    return None


def create_schema_from_dict(schema_dict: dict) -> list:
    """
    Creates a BigQuery schema from a dictionary definition.

    Args:
        schema_dict: Dictionary with field definitions

    Returns:
        List of SchemaField objects
    """
    schema = []
    
    for field_name, field_def in schema_dict.items():
        # Handle simple field definitions (just type)
        if isinstance(field_def, str):
            field_type = field_def
            mode = DEFAULT_FIELD_MODE
            description = ""
            fields = None
        
        # Handle dictionary field definitions with additional properties
        elif isinstance(field_def, dict):
            field_type = field_def.get('type')
            mode = field_def.get('mode', DEFAULT_FIELD_MODE)
            description = field_def.get('description', '')
            
            # Handle nested fields
            if 'fields' in field_def and isinstance(field_def['fields'], dict):
                fields = create_schema_from_dict(field_def['fields'])
            else:
                fields = None
        
        else:
            logger.warning(f"Invalid field definition for {field_name}, skipping")
            continue
        
        # Create the schema field
        schema.append(
            BQSchemaField(
                name=field_name,
                field_type=field_type,
                mode=mode,
                description=description,
                fields=fields
            )
        )
    
    return schema


def infer_schema_from_data(data_samples: list, sample_size: int = 100) -> list:
    """
    Infers a BigQuery schema from sample data.

    Args:
        data_samples: List of dictionaries representing data samples
        sample_size: Maximum number of samples to analyze

    Returns:
        Inferred schema as list of SchemaField objects
    """
    # Limit sample size for performance
    samples = data_samples[:sample_size]
    
    if not samples:
        logger.warning("No data samples provided for schema inference")
        return []
    
    # Dictionary to track field types across samples
    field_types = {}
    
    # Process each sample
    for sample in samples:
        for field_name, value in sample.items():
            python_type = type(value).__name__
            
            # Handle None values
            if value is None:
                if field_name not in field_types:
                    field_types[field_name] = {'type': None, 'mode': 'NULLABLE', 'nullable_count': 1, 'total': 1}
                else:
                    field_types[field_name]['nullable_count'] += 1
                    field_types[field_name]['total'] += 1
                continue
            
            # Get BigQuery type from Python type
            bq_type = FIELD_TYPE_MAPPING.get(python_type, 'STRING')
            
            # Handle nested records
            nested_fields = None
            if bq_type == 'RECORD' and isinstance(value, dict):
                # For small subsets of data, recursively infer nested schema
                nested_data = [value]
                nested_fields = infer_schema_from_data(nested_data, 1)
            
            # Initialize field type tracking if first time seeing this field
            if field_name not in field_types:
                field_types[field_name] = {
                    'type': bq_type,
                    'mode': 'NULLABLE',
                    'nullable_count': 0,
                    'total': 1,
                    'nested_fields': nested_fields
                }
            else:
                # Update existing field type information
                field_types[field_name]['total'] += 1
                
                # Handle type conflicts
                if field_types[field_name]['type'] != bq_type:
                    # If we see a conflict, default to STRING as most flexible
                    logger.warning(
                        f"Type conflict for field {field_name}: "
                        f"{field_types[field_name]['type']} vs {bq_type}, defaulting to STRING"
                    )
                    field_types[field_name]['type'] = 'STRING'
    
    # Create schema fields from inferred types
    schema = []
    for field_name, type_info in field_types.items():
        # If we saw this field in all samples and never saw NULL, make it REQUIRED
        if type_info['total'] == len(samples) and type_info['nullable_count'] == 0:
            mode = 'REQUIRED'
        else:
            mode = 'NULLABLE'
        
        # Use STRING if we couldn't determine a type
        field_type = type_info['type'] or 'STRING'
        
        schema.append(
            BQSchemaField(
                name=field_name,
                field_type=field_type,
                mode=mode,
                description='',
                fields=type_info.get('nested_fields')
            )
        )
    
    return schema


def compare_schemas(schema1: list, schema2: list) -> dict:
    """
    Compares two schemas and identifies differences.

    Args:
        schema1: First schema as list of SchemaField objects
        schema2: Second schema as list of SchemaField objects

    Returns:
        Dictionary of differences between schemas
    """
    # Convert schemas to dictionaries for easier comparison
    schema1_dict = {field.name: field for field in schema1}
    schema2_dict = {field.name: field for field in schema2}
    
    # Initialize difference tracking
    differences = {
        'added': [],    # Fields in schema2 not in schema1
        'removed': [],  # Fields in schema1 not in schema2
        'modified': []  # Fields that exist in both but have differences
    }
    
    # Find fields in schema2 not in schema1 (added)
    for name in schema2_dict:
        if name not in schema1_dict:
            differences['added'].append({
                'name': name,
                'field': schema_to_json([schema2_dict[name]])[0]
            })
    
    # Find fields in schema1 not in schema2 (removed)
    for name in schema1_dict:
        if name not in schema2_dict:
            differences['removed'].append({
                'name': name,
                'field': schema_to_json([schema1_dict[name]])[0]
            })
    
    # Find fields in both schemas but with differences
    for name in schema1_dict:
        if name in schema2_dict:
            field1 = schema1_dict[name]
            field2 = schema2_dict[name]
            
            # Check for differences
            if (field1.field_type != field2.field_type or
                field1.mode != field2.mode or
                field1.description != field2.description):
                
                differences['modified'].append({
                    'name': name,
                    'from': schema_to_json([field1])[0],
                    'to': schema_to_json([field2])[0]
                })
            
            # If RECORD type, recursively compare nested fields
            elif field1.field_type == 'RECORD' and field2.field_type == 'RECORD':
                nested_diff = compare_schemas(field1.fields or [], field2.fields or [])
                
                # Only add to differences if there are actual nested differences
                if nested_diff['added'] or nested_diff['removed'] or nested_diff['modified']:
                    differences['modified'].append({
                        'name': name,
                        'nested_differences': nested_diff
                    })
    
    return differences


class SchemaField:
    """
    Wrapper class for google.cloud.bigquery.SchemaField with additional functionality.
    
    This class provides a more convenient interface and additional methods for 
    working with BigQuery schema fields.
    """
    
    def __init__(
        self, 
        name: str, 
        field_type: str, 
        mode: str = None, 
        description: str = None, 
        fields: list = None
    ):
        """
        Initialize a new SchemaField with the provided parameters.

        Args:
            name: Field name
            field_type: BigQuery field type (STRING, INTEGER, etc.)
            mode: Field mode (NULLABLE, REQUIRED, REPEATED), defaults to NULLABLE
            description: Field description, defaults to empty string
            fields: List of nested fields for RECORD type, defaults to None
        """
        # Set default values
        self.name = name
        self.field_type = field_type
        self.mode = mode or DEFAULT_FIELD_MODE
        self.description = description or ""
        self.fields = fields
        
        # Create the underlying BigQuery SchemaField
        self._bq_field = BQSchemaField(
            name=self.name,
            field_type=self.field_type,
            mode=self.mode,
            description=self.description,
            fields=self.fields
        )
    
    def to_dict(self) -> dict:
        """
        Convert the schema field to a dictionary representation.

        Returns:
            Dictionary representation of the schema field
        """
        field_dict = {
            'name': self.name,
            'type': self.field_type,
            'mode': self.mode,
            'description': self.description
        }
        
        # Add nested fields if present
        if self.fields:
            if isinstance(self.fields[0], SchemaField):
                field_dict['fields'] = [f.to_dict() for f in self.fields]
            else:
                # Convert BQSchemaField objects to dictionaries
                field_dict['fields'] = schema_to_json(self.fields)
        
        return field_dict
    
    @classmethod
    def from_dict(cls, field_dict: dict) -> 'SchemaField':
        """
        Create a SchemaField from a dictionary representation.

        Args:
            field_dict: Dictionary representation of a schema field

        Returns:
            New SchemaField instance
        """
        name = field_dict['name']
        field_type = field_dict['type']
        mode = field_dict.get('mode', DEFAULT_FIELD_MODE)
        description = field_dict.get('description', '')
        
        # Handle nested fields
        fields = None
        if 'fields' in field_dict and field_dict['fields']:
            fields = [cls.from_dict(f) for f in field_dict['fields']]
        
        return cls(
            name=name,
            field_type=field_type,
            mode=mode,
            description=description,
            fields=fields
        )
    
    def is_nullable(self) -> bool:
        """
        Check if the field is nullable.

        Returns:
            True if field is nullable, False otherwise
        """
        return self.mode == 'NULLABLE'
    
    def is_repeated(self) -> bool:
        """
        Check if the field is repeated (array).

        Returns:
            True if field is repeated, False otherwise
        """
        return self.mode == 'REPEATED'
    
    def is_required(self) -> bool:
        """
        Check if the field is required.

        Returns:
            True if field is required, False otherwise
        """
        return self.mode == 'REQUIRED'
    
    def is_record(self) -> bool:
        """
        Check if the field is a record (nested).

        Returns:
            True if field is a record, False otherwise
        """
        return self.field_type == 'RECORD'
    
    def to_bq_field(self) -> BQSchemaField:
        """
        Convert to native BigQuery SchemaField.

        Returns:
            Native BigQuery SchemaField
        """
        return self._bq_field


class SchemaManager:
    """
    Manages BigQuery schemas with versioning and evolution capabilities.
    
    This class maintains a registry of schemas with version history and provides
    methods for schema evolution with compatibility checks.
    """
    
    def __init__(self):
        """
        Initialize a new SchemaManager.
        """
        # Schema registry stores the latest version of each schema
        self._schema_registry = {}
        
        # Schema history stores all versions of each schema
        self._schema_history = {}
        
        logger.info("SchemaManager initialized")
    
    def register_schema(
        self, 
        schema_id: str, 
        schema: list, 
        version: str = "1.0", 
        description: str = ""
    ) -> bool:
        """
        Register a schema with the manager.

        Args:
            schema_id: Unique identifier for the schema
            schema: List of SchemaField objects
            version: Schema version string
            description: Description of the schema

        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate schema is a list of SchemaField objects
            if not isinstance(schema, list):
                logger.error(f"Invalid schema type for {schema_id}: expected list, got {type(schema)}")
                return False
            
            # Convert schema to JSON for storage
            schema_json = schema_to_json(schema)
            
            # Create schema entry
            schema_entry = {
                'version': version,
                'description': description,
                'schema': schema_json,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            # Initialize history if needed
            if schema_id not in self._schema_history:
                self._schema_history[schema_id] = []
            
            # Add to registry (latest version)
            self._schema_registry[schema_id] = schema_entry
            
            # Add to history
            self._schema_history[schema_id].append(schema_entry)
            
            logger.info(f"Registered schema {schema_id} version {version}")
            return True
        
        except Exception as e:
            logger.error(f"Error registering schema {schema_id}: {str(e)}")
            return False
    
    def get_schema(self, schema_id: str, version: str = None) -> typing.Optional[list]:
        """
        Get a schema by ID and optional version.

        Args:
            schema_id: Schema identifier
            version: Optional version string, gets latest if not specified

        Returns:
            List of SchemaField objects or None if not found
        """
        try:
            # Get specific version if requested
            if version and schema_id in self._schema_history:
                for entry in self._schema_history[schema_id]:
                    if entry['version'] == version:
                        return json_to_schema(entry['schema'])
                
                logger.warning(f"Version {version} not found for schema {schema_id}")
                return None
            
            # Get latest version
            if schema_id in self._schema_registry:
                return json_to_schema(self._schema_registry[schema_id]['schema'])
            
            logger.warning(f"Schema {schema_id} not found in registry")
            return None
        
        except Exception as e:
            logger.error(f"Error retrieving schema {schema_id}: {str(e)}")
            return None
    
    def evolve_schema(
        self, 
        schema_id: str, 
        new_schema: list, 
        new_version: str, 
        description: str = "", 
        allow_field_addition: bool = True, 
        allow_field_relaxation: bool = True
    ) -> typing.Tuple[bool, str]:
        """
        Evolve a schema with compatibility checks.

        Args:
            schema_id: Schema identifier
            new_schema: New schema as list of SchemaField objects
            new_version: New version string
            description: Description of the schema update
            allow_field_addition: Whether to allow adding new fields
            allow_field_relaxation: Whether to allow relaxing field modes

        Returns:
            Tuple of (bool, str) - Success status and error message if failed
        """
        # Get current schema
        current_schema = self.get_schema(schema_id)
        
        # If no current schema, register the new one
        if current_schema is None:
            success = self.register_schema(
                schema_id=schema_id,
                schema=new_schema,
                version=new_version,
                description=description
            )
            return success, "" if success else "Failed to register new schema"
        
        # Validate compatibility between current and new schema
        is_compatible, error_msg = validate_schema_compatibility(
            current_schema, 
            new_schema, 
            allow_field_addition, 
            allow_field_relaxation
        )
        
        if not is_compatible:
            logger.error(f"Schema evolution failed for {schema_id}: {error_msg}")
            return False, error_msg
        
        # Register new schema version
        success = self.register_schema(
            schema_id=schema_id,
            schema=new_schema,
            version=new_version,
            description=description
        )
        
        return success, "" if success else "Failed to register evolved schema"
    
    def get_schema_history(self, schema_id: str) -> list:
        """
        Get the version history for a schema.

        Args:
            schema_id: Schema identifier

        Returns:
            List of schema versions with metadata
        """
        if schema_id not in self._schema_history:
            return []
        
        # Sort versions by timestamp
        history = sorted(
            self._schema_history[schema_id],
            key=lambda entry: entry['timestamp']
        )
        
        # Return history without the full schema to reduce size
        return [
            {
                'version': entry['version'],
                'description': entry['description'],
                'timestamp': entry['timestamp']
            }
            for entry in history
        ]
    
    def export_schema_registry(self) -> dict:
        """
        Export the schema registry to JSON.

        Returns:
            JSON-serializable registry data
        """
        return {
            'registry': self._schema_registry,
            'history': self._schema_history
        }
    
    def import_schema_registry(self, registry_data: dict) -> bool:
        """
        Import a schema registry from JSON.

        Args:
            registry_data: Registry data as exported by export_schema_registry

        Returns:
            True if successful, False otherwise
        """
        try:
            if not isinstance(registry_data, dict):
                logger.error(f"Invalid registry data type: {type(registry_data)}")
                return False
            
            if 'registry' not in registry_data or 'history' not in registry_data:
                logger.error("Invalid registry data structure: missing required keys")
                return False
            
            self._schema_registry = registry_data['registry']
            self._schema_history = registry_data['history']
            
            logger.info("Schema registry imported successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error importing schema registry: {str(e)}")
            return False
    
    def compare_schema_versions(
        self, 
        schema_id: str, 
        version1: str, 
        version2: str
    ) -> dict:
        """
        Compare two versions of a schema.

        Args:
            schema_id: Schema identifier
            version1: First version to compare
            version2: Second version to compare

        Returns:
            Dictionary of differences between versions
        """
        # Get schemas for the specified versions
        schema1 = self.get_schema(schema_id, version1)
        schema2 = self.get_schema(schema_id, version2)
        
        if schema1 is None:
            logger.error(f"Version {version1} not found for schema {schema_id}")
            return {'error': f"Version {version1} not found"}
        
        if schema2 is None:
            logger.error(f"Version {version2} not found for schema {schema_id}")
            return {'error': f"Version {version2} not found"}
        
        # Compare the schemas
        return compare_schemas(schema1, schema2)