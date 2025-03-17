"""
Defines the Firestore database schema for the self-healing data pipeline.

This module provides utilities for creating, validating, and managing Firestore document schemas,
supporting metadata storage, configuration management, and operational state tracking with schema
versioning capabilities.
"""

import typing
import json
import datetime
import copy
from google.cloud import firestore  # version 2.11.0+

from ...constants import DEFAULT_FIELD_MODE
from ...utils.logging.logger import get_logger

# Configure module logger
logger = get_logger(__name__)

# Mapping between Python types and Firestore field types
FIELD_TYPE_MAPPING = {
    'str': 'string',
    'int': 'integer',
    'float': 'float',
    'bool': 'boolean',
    'datetime.datetime': 'timestamp',
    'datetime.date': 'date',
    'datetime.time': 'time',
    'bytes': 'bytes',
    'dict': 'map',
    'list': 'array'
}


def get_schema_field(name: str, field_type: str, required: bool = False, 
                     description: str = "", fields: list = None) -> dict:
    """
    Creates a Firestore schema field definition with simplified interface.
    
    Args:
        name: Field name
        field_type: Field type (string, integer, float, boolean, timestamp, map, array, etc.)
        required: Whether the field is required
        description: Field description
        fields: Nested fields if field_type is 'map' or 'array'
        
    Returns:
        Firestore schema field definition
    """
    field_def = {
        'name': name,
        'type': field_type,
        'required': required,
        'description': description
    }
    
    if fields:
        field_def['fields'] = fields
        
    return field_def


def schema_to_json(schema: dict) -> str:
    """
    Converts a Firestore schema to JSON format.
    
    Args:
        schema: Firestore schema dictionary
        
    Returns:
        JSON representation of the schema
    """
    return json.dumps(schema, indent=2)


def json_to_schema(json_schema: str) -> dict:
    """
    Converts a JSON schema definition to Firestore schema dictionary.
    
    Args:
        json_schema: JSON string containing schema definition
        
    Returns:
        Firestore schema dictionary
    """
    try:
        schema = json.loads(json_schema)
        
        # Basic validation that it's a valid schema
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")
            
        return schema
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON schema: {e}")
        raise ValueError(f"Invalid JSON schema: {e}")


def validate_document(document: dict, schema: dict) -> typing.Tuple[bool, str]:
    """
    Validates a document against a schema definition.
    
    Args:
        document: Document to validate
        schema: Schema to validate against
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check if all required fields are present
    for field_name, field_def in schema.items():
        if field_def.get('required', False) and field_name not in document:
            return False, f"Required field '{field_name}' is missing"
    
    # Validate field types
    for field_name, field_value in document.items():
        if field_name in schema:
            field_def = schema[field_name]
            field_type = field_def.get('type')
            
            # Skip validation if field is not in schema (schema might not be strict)
            if not field_type:
                continue
                
            # Validate based on field type
            if field_type == 'string' and not isinstance(field_value, str):
                return False, f"Field '{field_name}' should be a string"
            elif field_type == 'integer' and not isinstance(field_value, int):
                return False, f"Field '{field_name}' should be an integer"
            elif field_type == 'float' and not isinstance(field_value, (int, float)):
                return False, f"Field '{field_name}' should be a float"
            elif field_type == 'boolean' and not isinstance(field_value, bool):
                return False, f"Field '{field_name}' should be a boolean"
            elif field_type == 'timestamp' and not isinstance(field_value, datetime.datetime):
                return False, f"Field '{field_name}' should be a datetime"
            elif field_type == 'map' and not isinstance(field_value, dict):
                return False, f"Field '{field_name}' should be a dictionary"
            elif field_type == 'array' and not isinstance(field_value, list):
                return False, f"Field '{field_name}' should be a list"
                
            # Recursive validation for nested fields
            if field_type == 'map' and 'fields' in field_def and isinstance(field_value, dict):
                valid, error = validate_document(field_value, field_def['fields'])
                if not valid:
                    return False, f"Invalid nested field '{field_name}': {error}"
            
            # Validate array items if item type is specified
            elif field_type == 'array' and 'items' in field_def and isinstance(field_value, list):
                item_type = field_def['items'].get('type')
                for i, item in enumerate(field_value):
                    if item_type == 'string' and not isinstance(item, str):
                        return False, f"Item {i} in '{field_name}' should be a string"
                    elif item_type == 'integer' and not isinstance(item, int):
                        return False, f"Item {i} in '{field_name}' should be an integer"
                    elif item_type == 'float' and not isinstance(item, (int, float)):
                        return False, f"Item {i} in '{field_name}' should be a float"
                    elif item_type == 'boolean' and not isinstance(item, bool):
                        return False, f"Item {i} in '{field_name}' should be a boolean"
                    elif item_type == 'timestamp' and not isinstance(item, datetime.datetime):
                        return False, f"Item {i} in '{field_name}' should be a datetime"
                    elif item_type == 'map' and not isinstance(item, dict):
                        return False, f"Item {i} in '{field_name}' should be a dictionary"
                        
                    # Recursive validation for array of maps with nested fields
                    if item_type == 'map' and 'fields' in field_def['items'] and isinstance(item, dict):
                        valid, error = validate_document(item, field_def['items']['fields'])
                        if not valid:
                            return False, f"Invalid item {i} in '{field_name}': {error}"
    
    return True, ""


def create_schema_from_dict(schema_dict: dict) -> dict:
    """
    Creates a Firestore schema from a dictionary definition.
    
    Args:
        schema_dict: Dictionary containing schema definition
        
    Returns:
        Firestore schema dictionary
    """
    schema = {}
    
    for field_name, field_def in schema_dict.items():
        field_type = field_def.get('type', 'string')
        required = field_def.get('required', False)
        description = field_def.get('description', '')
        
        # Handle nested fields for map type
        nested_fields = None
        if field_type == 'map' and 'fields' in field_def:
            nested_fields = create_schema_from_dict(field_def['fields'])
            
        # Handle array item type
        elif field_type == 'array' and 'items' in field_def:
            items = field_def['items']
            if isinstance(items, dict) and 'type' in items:
                item_type = items['type']
                
                # Handle nested fields for array of maps
                if item_type == 'map' and 'fields' in items:
                    items['fields'] = create_schema_from_dict(items['fields'])
            
        # Create field definition
        schema[field_name] = get_schema_field(
            name=field_name,
            field_type=field_type,
            required=required,
            description=description,
            fields=nested_fields
        )
        
        # Add array item definition if present
        if field_type == 'array' and 'items' in field_def:
            schema[field_name]['items'] = field_def['items']
    
    return schema


def infer_schema_from_document(document: dict) -> dict:
    """
    Infers a Firestore schema from a sample document.
    
    Args:
        document: Sample document to infer schema from
        
    Returns:
        Inferred schema dictionary
    """
    schema = {}
    
    for field_name, field_value in document.items():
        # Determine field type based on Python type
        if isinstance(field_value, str):
            field_type = 'string'
        elif isinstance(field_value, int):
            field_type = 'integer'
        elif isinstance(field_value, float):
            field_type = 'float'
        elif isinstance(field_value, bool):
            field_type = 'boolean'
        elif isinstance(field_value, datetime.datetime):
            field_type = 'timestamp'
        elif isinstance(field_value, dict):
            field_type = 'map'
        elif isinstance(field_value, list):
            field_type = 'array'
        else:
            # Default to string for unknown types
            field_type = 'string'
            
        # Create basic field definition
        field_def = {
            'name': field_name,
            'type': field_type,
            'required': False,  # Default to not required
            'description': f"Inferred from document value: {str(field_value)[:50]}"
        }
        
        # Handle nested fields for map type
        if field_type == 'map':
            field_def['fields'] = infer_schema_from_document(field_value)
            
        # Handle array items
        elif field_type == 'array' and field_value:
            # Use the type of the first item as the array item type
            first_item = field_value[0]
            
            if isinstance(first_item, str):
                item_type = 'string'
            elif isinstance(first_item, int):
                item_type = 'integer'
            elif isinstance(first_item, float):
                item_type = 'float'
            elif isinstance(first_item, bool):
                item_type = 'boolean'
            elif isinstance(first_item, datetime.datetime):
                item_type = 'timestamp'
            elif isinstance(first_item, dict):
                item_type = 'map'
                # Infer schema for the first item if it's a map
                field_def['items'] = {
                    'type': 'map',
                    'fields': infer_schema_from_document(first_item)
                }
            else:
                # Default to string for unknown types
                item_type = 'string'
                
            if item_type != 'map' or 'items' not in field_def:
                field_def['items'] = {'type': item_type}
        
        schema[field_name] = field_def
    
    return schema


def merge_schemas(base_schema: dict, new_schema: dict, 
                  allow_field_addition: bool = True,
                  allow_field_relaxation: bool = True) -> dict:
    """
    Merges two schemas with conflict resolution.
    
    Args:
        base_schema: Base schema to merge into
        new_schema: New schema to merge from
        allow_field_addition: Whether to allow adding new fields
        allow_field_relaxation: Whether to allow making required fields optional
        
    Returns:
        Merged schema dictionary
    """
    merged_schema = copy.deepcopy(base_schema)
    
    for field_name, field_def in new_schema.items():
        # Field exists in base schema
        if field_name in merged_schema:
            base_field = merged_schema[field_name]
            
            # Check if types are compatible
            if base_field.get('type') != field_def.get('type'):
                logger.warning(f"Field '{field_name}' has different types in schemas: "
                              f"{base_field.get('type')} vs {field_def.get('type')}")
                continue
                
            # Required flag: can make optional field required, but not vice versa
            # unless allow_field_relaxation is True
            if base_field.get('required', False) and not field_def.get('required', False):
                if allow_field_relaxation:
                    base_field['required'] = False
                    logger.info(f"Field '{field_name}' changed from required to optional")
                else:
                    logger.warning(f"Cannot make required field '{field_name}' optional "
                                  "without allow_field_relaxation=True")
            elif not base_field.get('required', False) and field_def.get('required', False):
                base_field['required'] = True
                logger.info(f"Field '{field_name}' changed from optional to required")
                
            # Update description if new one is provided and not empty
            if field_def.get('description') and field_def.get('description') != base_field.get('description', ''):
                base_field['description'] = field_def['description']
                
            # Handle nested fields for map type
            if field_def.get('type') == 'map' and 'fields' in field_def and 'fields' in base_field:
                base_field['fields'] = merge_schemas(
                    base_field['fields'], 
                    field_def['fields'],
                    allow_field_addition,
                    allow_field_relaxation
                )
                
            # Handle array item types
            elif field_def.get('type') == 'array' and 'items' in field_def and 'items' in base_field:
                # If both have map items with fields, merge those fields
                if (field_def['items'].get('type') == 'map' and 
                    base_field['items'].get('type') == 'map' and
                    'fields' in field_def['items'] and 
                    'fields' in base_field['items']):
                    
                    base_field['items']['fields'] = merge_schemas(
                        base_field['items']['fields'],
                        field_def['items']['fields'],
                        allow_field_addition,
                        allow_field_relaxation
                    )
                # Otherwise, use the new item definition if types match
                elif field_def['items'].get('type') == base_field['items'].get('type'):
                    base_field['items'] = field_def['items']
                else:
                    logger.warning(f"Array item types for field '{field_name}' are incompatible: "
                                  f"{base_field['items'].get('type')} vs {field_def['items'].get('type')}")
        
        # Field doesn't exist in base schema
        elif allow_field_addition:
            merged_schema[field_name] = copy.deepcopy(field_def)
            logger.info(f"Added new field '{field_name}' to schema")
        else:
            logger.warning(f"Field '{field_name}' not in base schema and allow_field_addition=False")
    
    return merged_schema


def validate_schema_compatibility(existing_schema: dict, new_schema: dict,
                                 allow_field_addition: bool = True,
                                 allow_field_relaxation: bool = True) -> typing.Tuple[bool, str]:
    """
    Validates that a new schema is compatible with an existing schema.
    
    Args:
        existing_schema: Existing schema to check compatibility against
        new_schema: New schema to check compatibility of
        allow_field_addition: Whether to allow adding new fields
        allow_field_relaxation: Whether to allow making required fields optional
        
    Returns:
        Tuple of (is_compatible, error_message)
    """
    for field_name, field_def in new_schema.items():
        # Field exists in existing schema
        if field_name in existing_schema:
            existing_field = existing_schema[field_name]
            
            # Types must be compatible
            if field_def.get('type') != existing_field.get('type'):
                return False, f"Field '{field_name}' has incompatible types: {existing_field.get('type')} vs {field_def.get('type')}"
                
            # Required flag: can make optional field required, but not vice versa
            # unless allow_field_relaxation is True
            if existing_field.get('required', False) and not field_def.get('required', False):
                if not allow_field_relaxation:
                    return False, f"Cannot make required field '{field_name}' optional without allow_field_relaxation=True"
            
            # Handle nested fields for map type
            if field_def.get('type') == 'map' and 'fields' in field_def and 'fields' in existing_field:
                compatible, error = validate_schema_compatibility(
                    existing_field['fields'],
                    field_def['fields'],
                    allow_field_addition,
                    allow_field_relaxation
                )
                if not compatible:
                    return False, f"Incompatible nested fields in '{field_name}': {error}"
                    
            # Handle array item types
            elif field_def.get('type') == 'array' and 'items' in field_def and 'items' in existing_field:
                # If both have map items with fields, check compatibility of those fields
                if (field_def['items'].get('type') == 'map' and 
                    existing_field['items'].get('type') == 'map' and
                    'fields' in field_def['items'] and 
                    'fields' in existing_field['items']):
                    
                    compatible, error = validate_schema_compatibility(
                        existing_field['items']['fields'],
                        field_def['items']['fields'],
                        allow_field_addition,
                        allow_field_relaxation
                    )
                    if not compatible:
                        return False, f"Incompatible array item fields in '{field_name}': {error}"
                # Otherwise, item types must match
                elif field_def['items'].get('type') != existing_field['items'].get('type'):
                    return False, f"Incompatible array item types for '{field_name}': {existing_field['items'].get('type')} vs {field_def['items'].get('type')}"
        
        # Field doesn't exist in existing schema
        elif not allow_field_addition:
            return False, f"Field '{field_name}' not in existing schema and allow_field_addition=False"
    
    return True, ""


def get_field_by_name(schema: dict, field_name: str) -> typing.Optional[dict]:
    """
    Retrieves a field from a schema by name, including nested fields.
    
    Args:
        schema: Schema to search in
        field_name: Name of the field to find
        
    Returns:
        Field definition dictionary or None if not found
    """
    # Check if field is directly in the schema
    if field_name in schema:
        return schema[field_name]
        
    # Search in nested fields
    for field, field_def in schema.items():
        if field_def.get('type') == 'map' and 'fields' in field_def:
            nested_field = get_field_by_name(field_def['fields'], field_name)
            if nested_field:
                return nested_field
                
        elif field_def.get('type') == 'array' and 'items' in field_def:
            items = field_def['items']
            if items.get('type') == 'map' and 'fields' in items:
                nested_field = get_field_by_name(items['fields'], field_name)
                if nested_field:
                    return nested_field
    
    return None


class FirestoreSchema:
    """
    Class for defining and managing Firestore document schemas.
    """
    
    def __init__(self, collection_name: str, version: str = "1.0", description: str = ""):
        """
        Initialize a new FirestoreSchema with collection name and optional version.
        
        Args:
            collection_name: Firestore collection name this schema applies to
            version: Schema version string
            description: Schema description
        """
        self._fields = {}
        self._collection_name = collection_name
        self._version = version
        self._description = description
        
        logger.info(f"Initialized FirestoreSchema for collection '{collection_name}' version {version}")
    
    def add_field(self, name: str, field_type: str, required: bool = False, 
                 description: str = "", nested_fields: list = None) -> 'FirestoreSchema':
        """
        Adds a field to the schema.
        
        Args:
            name: Field name
            field_type: Field type (string, integer, float, boolean, timestamp, map, array, etc.)
            required: Whether the field is required
            description: Field description
            nested_fields: Nested fields if field_type is 'map'
            
        Returns:
            Self for method chaining
        """
        self._fields[name] = get_schema_field(
            name=name,
            field_type=field_type,
            required=required,
            description=description,
            fields=nested_fields
        )
        
        return self
    
    def add_string_field(self, name: str, required: bool = False, 
                        description: str = "") -> 'FirestoreSchema':
        """
        Adds a string field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'string', required, description)
    
    def add_integer_field(self, name: str, required: bool = False, 
                         description: str = "") -> 'FirestoreSchema':
        """
        Adds an integer field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'integer', required, description)
    
    def add_float_field(self, name: str, required: bool = False, 
                       description: str = "") -> 'FirestoreSchema':
        """
        Adds a float field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'float', required, description)
    
    def add_boolean_field(self, name: str, required: bool = False, 
                         description: str = "") -> 'FirestoreSchema':
        """
        Adds a boolean field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'boolean', required, description)
    
    def add_timestamp_field(self, name: str, required: bool = False, 
                           description: str = "") -> 'FirestoreSchema':
        """
        Adds a timestamp field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'timestamp', required, description)
    
    def add_map_field(self, name: str, required: bool = False, 
                     description: str = "", nested_fields: list = None) -> 'FirestoreSchema':
        """
        Adds a map (nested document) field to the schema.
        
        Args:
            name: Field name
            required: Whether the field is required
            description: Field description
            nested_fields: Nested fields definition
            
        Returns:
            Self for method chaining
        """
        return self.add_field(name, 'map', required, description, nested_fields)
    
    def add_array_field(self, name: str, items_type: str, required: bool = False, 
                       description: str = "", nested_fields: list = None) -> 'FirestoreSchema':
        """
        Adds an array field to the schema.
        
        Args:
            name: Field name
            items_type: Type of items in the array
            required: Whether the field is required
            description: Field description
            nested_fields: Nested fields if items_type is 'map'
            
        Returns:
            Self for method chaining
        """
        field = {
            'name': name,
            'type': 'array',
            'required': required,
            'description': description,
            'items': {
                'type': items_type
            }
        }
        
        if nested_fields and items_type == 'map':
            field['items']['fields'] = nested_fields
            
        self._fields[name] = field
        
        return self
    
    def to_dict(self) -> dict:
        """
        Converts the schema to a dictionary representation.
        
        Returns:
            Dictionary representation of the schema
        """
        return {
            'collection_name': self._collection_name,
            'version': self._version,
            'description': self._description,
            'fields': self._fields
        }
    
    def to_json(self) -> str:
        """
        Converts the schema to a JSON string.
        
        Returns:
            JSON representation of the schema
        """
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, schema_dict: dict) -> 'FirestoreSchema':
        """
        Creates a FirestoreSchema from a dictionary.
        
        Args:
            schema_dict: Dictionary representation of a schema
            
        Returns:
            New FirestoreSchema instance
        """
        if not isinstance(schema_dict, dict):
            raise ValueError("Schema must be a dictionary")
            
        collection_name = schema_dict.get('collection_name', '')
        version = schema_dict.get('version', '1.0')
        description = schema_dict.get('description', '')
        
        schema = cls(collection_name, version, description)
        
        # Add fields from the dictionary
        fields = schema_dict.get('fields', {})
        for field_name, field_def in fields.items():
            # Add field to schema directly
            schema._fields[field_name] = field_def
            
        return schema
    
    @classmethod
    def from_json(cls, json_schema: str) -> 'FirestoreSchema':
        """
        Creates a FirestoreSchema from a JSON string.
        
        Args:
            json_schema: JSON string representation of a schema
            
        Returns:
            New FirestoreSchema instance
        """
        try:
            schema_dict = json.loads(json_schema)
            return cls.from_dict(schema_dict)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON schema: {e}")
            raise ValueError(f"Invalid JSON schema: {e}")
    
    def validate_document(self, document: dict) -> typing.Tuple[bool, str]:
        """
        Validates a document against this schema.
        
        Args:
            document: Document to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        return validate_document(document, self._fields)
    
    def merge(self, other_schema: 'FirestoreSchema', 
             allow_field_addition: bool = True,
             allow_field_relaxation: bool = True) -> 'FirestoreSchema':
        """
        Merges this schema with another schema.
        
        Args:
            other_schema: Another FirestoreSchema to merge with
            allow_field_addition: Whether to allow adding new fields
            allow_field_relaxation: Whether to allow making required fields optional
            
        Returns:
            New merged FirestoreSchema instance
        """
        # Create a new schema with incremented version
        major, minor = self._version.split('.')
        new_version = f"{major}.{int(minor) + 1}"
        
        merged_schema = FirestoreSchema(
            self._collection_name,
            new_version,
            f"Merged schema from versions {self._version} and {other_schema._version}"
        )
        
        # Merge fields
        merged_fields = merge_schemas(
            self._fields,
            other_schema._fields,
            allow_field_addition,
            allow_field_relaxation
        )
        
        # Set fields directly
        merged_schema._fields = merged_fields
        
        return merged_schema
    
    def is_compatible_with(self, other_schema: 'FirestoreSchema',
                          allow_field_addition: bool = True,
                          allow_field_relaxation: bool = True) -> typing.Tuple[bool, str]:
        """
        Checks if this schema is compatible with another schema.
        
        Args:
            other_schema: Another FirestoreSchema to check compatibility with
            allow_field_addition: Whether to allow adding new fields
            allow_field_relaxation: Whether to allow making required fields optional
            
        Returns:
            Tuple of (is_compatible, error_message)
        """
        return validate_schema_compatibility(
            self._fields,
            other_schema._fields,
            allow_field_addition,
            allow_field_relaxation
        )
    
    def get_field(self, field_name: str) -> typing.Optional[dict]:
        """
        Gets a field by name from the schema.
        
        Args:
            field_name: Name of the field to get
            
        Returns:
            Field definition or None if not found
        """
        return get_field_by_name(self._fields, field_name)


class FirestoreSchemaManager:
    """
    Manages Firestore schemas with versioning and evolution capabilities.
    """
    
    def __init__(self):
        """
        Initialize a new FirestoreSchemaManager.
        """
        self._schema_registry = {}  # collection_name -> latest schema
        self._schema_history = {}  # collection_name -> {version -> schema}
        
        logger.info("Initialized FirestoreSchemaManager")
    
    def register_schema(self, schema: FirestoreSchema) -> bool:
        """
        Register a schema with the manager.
        
        Args:
            schema: Schema to register
            
        Returns:
            True if successful, False otherwise
        """
        if not isinstance(schema, FirestoreSchema):
            logger.error("Cannot register non-FirestoreSchema object")
            return False
            
        collection_name = schema._collection_name
        version = schema._version
        
        # Add to registry (latest version for collection)
        self._schema_registry[collection_name] = schema
        
        # Add to history
        if collection_name not in self._schema_history:
            self._schema_history[collection_name] = {}
            
        self._schema_history[collection_name][version] = schema
        
        logger.info(f"Registered schema for collection '{collection_name}' version {version}")
        return True
    
    def get_schema(self, collection_name: str, version: str = None) -> typing.Optional[FirestoreSchema]:
        """
        Get a schema by collection name and optional version.
        
        Args:
            collection_name: Name of the collection
            version: Optional specific version to retrieve
            
        Returns:
            FirestoreSchema instance or None if not found
        """
        # Get specific version if provided
        if version:
            if collection_name in self._schema_history and version in self._schema_history[collection_name]:
                return self._schema_history[collection_name][version]
            logger.warning(f"Schema version {version} not found for collection '{collection_name}'")
            return None
            
        # Get latest version
        if collection_name in self._schema_registry:
            return self._schema_registry[collection_name]
            
        logger.warning(f"No schema found for collection '{collection_name}'")
        return None
    
    def evolve_schema(self, collection_name: str, new_schema: FirestoreSchema,
                     allow_field_addition: bool = True,
                     allow_field_relaxation: bool = True) -> typing.Tuple[bool, str]:
        """
        Evolve a schema with compatibility checks.
        
        Args:
            collection_name: Name of the collection to evolve schema for
            new_schema: New schema to evolve to
            allow_field_addition: Whether to allow adding new fields
            allow_field_relaxation: Whether to allow making required fields optional
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if we have an existing schema
        current_schema = self.get_schema(collection_name)
        
        # If no existing schema, just register the new one
        if not current_schema:
            success = self.register_schema(new_schema)
            return success, "" if success else "Failed to register schema"
            
        # Check compatibility
        compatible, error = current_schema.is_compatible_with(
            new_schema,
            allow_field_addition,
            allow_field_relaxation
        )
        
        if not compatible:
            logger.warning(f"New schema is not compatible with existing schema: {error}")
            return False, error
            
        # Register the new schema
        success = self.register_schema(new_schema)
        return success, "" if success else "Failed to register schema"
    
    def get_schema_history(self, collection_name: str) -> list:
        """
        Get the version history for a collection's schema.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            List of schema versions with metadata
        """
        if collection_name not in self._schema_history:
            return []
            
        history = []
        for version, schema in self._schema_history[collection_name].items():
            history.append({
                'version': version,
                'description': schema._description,
                'field_count': len(schema._fields),
                'schema': schema.to_dict()
            })
            
        # Sort by version (assuming semantic versioning)
        history.sort(key=lambda x: [int(v) for v in x['version'].split('.')])
        
        return history
    
    def export_registry(self) -> str:
        """
        Export the schema registry to JSON.
        
        Returns:
            JSON string of registry data
        """
        export_data = {
            'registry': {},
            'history': {}
        }
        
        # Export registry
        for collection_name, schema in self._schema_registry.items():
            export_data['registry'][collection_name] = schema.to_dict()
            
        # Export history
        for collection_name, versions in self._schema_history.items():
            export_data['history'][collection_name] = {}
            for version, schema in versions.items():
                export_data['history'][collection_name][version] = schema.to_dict()
                
        return json.dumps(export_data, indent=2)
    
    def import_registry(self, json_registry: str) -> bool:
        """
        Import a schema registry from JSON.
        
        Args:
            json_registry: JSON string of registry data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data = json.loads(json_registry)
            
            # Basic validation
            if not isinstance(data, dict) or 'registry' not in data or 'history' not in data:
                logger.error("Invalid registry data format")
                return False
                
            # Clear existing registry and history
            self._schema_registry = {}
            self._schema_history = {}
            
            # Import registry
            for collection_name, schema_dict in data['registry'].items():
                schema = FirestoreSchema.from_dict(schema_dict)
                self._schema_registry[collection_name] = schema
                
            # Import history
            for collection_name, versions in data['history'].items():
                self._schema_history[collection_name] = {}
                for version, schema_dict in versions.items():
                    schema = FirestoreSchema.from_dict(schema_dict)
                    self._schema_history[collection_name][version] = schema
                    
            logger.info("Successfully imported schema registry")
            return True
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Failed to import registry: {e}")
            return False
    
    def validate_document(self, collection_name: str, document: dict, 
                         version: str = None) -> typing.Tuple[bool, str]:
        """
        Validates a document against a collection's schema.
        
        Args:
            collection_name: Name of the collection
            document: Document to validate
            version: Optional specific schema version to validate against
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        schema = self.get_schema(collection_name, version)
        if not schema:
            return False, f"No schema found for collection '{collection_name}'"
            
        return schema.validate_document(document)
    
    def compare_schema_versions(self, collection_name: str, 
                               version1: str, version2: str) -> dict:
        """
        Compare two versions of a collection's schema.
        
        Args:
            collection_name: Name of the collection
            version1: First version to compare
            version2: Second version to compare
            
        Returns:
            Dictionary of differences between versions
        """
        schema1 = self.get_schema(collection_name, version1)
        schema2 = self.get_schema(collection_name, version2)
        
        if not schema1 or not schema2:
            return {'error': 'One or both schema versions not found'}
            
        differences = {
            'added_fields': [],
            'removed_fields': [],
            'modified_fields': [],
            'field_count_diff': len(schema2._fields) - len(schema1._fields)
        }
        
        # Find added and modified fields
        for field_name, field_def in schema2._fields.items():
            if field_name not in schema1._fields:
                differences['added_fields'].append({
                    'name': field_name,
                    'definition': field_def
                })
            elif schema1._fields[field_name] != field_def:
                differences['modified_fields'].append({
                    'name': field_name,
                    'old': schema1._fields[field_name],
                    'new': field_def
                })
                
        # Find removed fields
        for field_name in schema1._fields:
            if field_name not in schema2._fields:
                differences['removed_fields'].append({
                    'name': field_name,
                    'definition': schema1._fields[field_name]
                })
                
        return differences


class CollectionSchemas:
    """
    Predefined schemas for common collections in the self-healing data pipeline.
    """
    
    def __init__(self):
        """
        Not intended to be instantiated.
        """
        raise NotImplementedError("This class is not meant to be instantiated")
    
    @staticmethod
    def pipeline_execution_schema() -> FirestoreSchema:
        """
        Get the schema for pipeline executions collection.
        
        Returns:
            Schema for pipeline executions
        """
        schema = FirestoreSchema('pipeline_executions')
        
        # Add required fields
        schema.add_string_field('execution_id', required=True, 
                               description="Unique identifier for the execution")
        schema.add_string_field('pipeline_id', required=True, 
                               description="Identifier of the pipeline definition")
        schema.add_string_field('dag_run_id', required=True, 
                               description="Airflow DAG run identifier")
        
        # Add status fields
        schema.add_string_field('status', 
                              description="Execution status (PENDING, RUNNING, SUCCESS, FAILED)")
        schema.add_timestamp_field('start_time', 
                                 description="When execution started")
        schema.add_timestamp_field('end_time', 
                                 description="When execution completed")
        
        # Add parameter and metrics fields
        schema.add_map_field('execution_params', 
                           description="Parameters for this execution")
        schema.add_map_field('execution_metrics', 
                           description="Metrics from this execution")
        schema.add_map_field('error_details', 
                           description="Details of any errors encountered")
        
        # Add other metadata
        schema.add_integer_field('retry_count', 
                               description="Number of retry attempts")
        schema.add_float_field('quality_score', 
                             description="Overall data quality score")
        
        # Add self-healing information
        schema.add_array_field('self_healing_attempts', 'map', 
                             description="Self-healing attempts for this execution")
        
        return schema
    
    @staticmethod
    def healing_action_schema() -> FirestoreSchema:
        """
        Get the schema for healing actions collection.
        
        Returns:
            Schema for healing actions
        """
        schema = FirestoreSchema('healing_actions')
        
        # Add required fields
        schema.add_string_field('action_id', required=True, 
                               description="Unique identifier for the action")
        schema.add_string_field('name', required=True, 
                               description="Human-readable name of the action")
        schema.add_string_field('action_type', required=True, 
                               description="Type of healing action (correction, retry, etc.)")
        
        # Add descriptive fields
        schema.add_string_field('description', 
                              description="Detailed description of what the action does")
        schema.add_string_field('pattern_id', 
                              description="ID of the pattern this action is associated with")
        
        # Add configuration fields
        schema.add_map_field('action_parameters', 
                           description="Parameters for executing this action")
        
        # Add metrics fields
        schema.add_integer_field('execution_count', 
                               description="Number of times this action was executed")
        schema.add_integer_field('success_count', 
                               description="Number of successful executions")
        schema.add_float_field('success_rate', 
                             description="Success rate of this action")
        
        # Add status fields
        schema.add_boolean_field('is_active', 
                               description="Whether this action is currently active")
        schema.add_timestamp_field('last_executed', 
                                 description="When this action was last executed")
        schema.add_timestamp_field('created_at', 
                                 description="When this action was created")
        schema.add_timestamp_field('updated_at', 
                                 description="When this action was last updated")
        
        return schema
    
    @staticmethod
    def issue_pattern_schema() -> FirestoreSchema:
        """
        Get the schema for issue patterns collection.
        
        Returns:
            Schema for issue patterns
        """
        schema = FirestoreSchema('issue_patterns')
        
        # Add required fields
        schema.add_string_field('pattern_id', required=True, 
                               description="Unique identifier for the pattern")
        schema.add_string_field('name', required=True, 
                               description="Human-readable name of the pattern")
        schema.add_string_field('pattern_type', required=True, 
                               description="Type of issue pattern (data quality, pipeline failure, etc.)")
        
        # Add descriptive fields
        schema.add_string_field('description', 
                              description="Detailed description of what the pattern detects")
        schema.add_float_field('confidence_threshold', 
                             description="Minimum confidence score for pattern detection")
        
        # Add detection fields
        schema.add_map_field('detection_pattern', 
                           description="Pattern definition for detection")
        
        # Add metrics fields
        schema.add_integer_field('occurrence_count', 
                               description="Number of times this pattern was detected")
        schema.add_integer_field('success_count', 
                               description="Number of successful healings")
        schema.add_float_field('success_rate', 
                             description="Success rate of healing this pattern")
        
        # Add status fields
        schema.add_boolean_field('is_active', 
                               description="Whether this pattern is currently active")
        schema.add_timestamp_field('last_seen', 
                                 description="When this pattern was last detected")
        schema.add_timestamp_field('created_at', 
                                 description="When this pattern was created")
        schema.add_timestamp_field('updated_at', 
                                 description="When this pattern was last updated")
        
        return schema
    
    @staticmethod
    def healing_execution_schema() -> FirestoreSchema:
        """
        Get the schema for healing executions collection.
        
        Returns:
            Schema for healing executions
        """
        schema = FirestoreSchema('healing_executions')
        
        # Add required fields
        schema.add_string_field('healing_id', required=True, 
                               description="Unique identifier for the healing execution")
        schema.add_string_field('execution_id', required=True, 
                               description="Pipeline execution ID this healing is for")
        schema.add_string_field('pattern_id', required=True, 
                               description="ID of the detected pattern")
        schema.add_string_field('action_id', required=True, 
                               description="ID of the healing action applied")
        
        # Add detailed fields
        schema.add_string_field('validation_id', 
                              description="ID of the validation result that triggered healing")
        schema.add_string_field('issue_type', 
                              description="Type of issue being healed")
        schema.add_string_field('action_taken', 
                              description="Description of the action taken")
        
        # Add execution details
        schema.add_timestamp_field('execution_time', 
                                 description="When the healing was executed")
        schema.add_boolean_field('successful', 
                               description="Whether the healing was successful")
        schema.add_map_field('execution_details', 
                           description="Detailed information about the execution")
        
        # Add additional metadata
        schema.add_float_field('confidence_score', 
                             description="Confidence score for the healing action")
        schema.add_timestamp_field('created_at', 
                                 description="When this record was created")
        
        return schema
    
    @staticmethod
    def quality_validation_schema() -> FirestoreSchema:
        """
        Get the schema for quality validations collection.
        
        Returns:
            Schema for quality validations
        """
        schema = FirestoreSchema('quality_validations')
        
        # Add required fields
        schema.add_string_field('validation_id', required=True, 
                               description="Unique identifier for the validation")
        schema.add_string_field('execution_id', required=True, 
                               description="Pipeline execution ID this validation is for")
        schema.add_string_field('rule_id', required=True, 
                               description="ID of the validation rule applied")
        
        # Add result fields
        schema.add_timestamp_field('validation_time', 
                                 description="When validation was performed")
        schema.add_boolean_field('passed', 
                               description="Whether validation passed")
        
        # Add detailed results
        schema.add_map_field('validation_results', 
                           description="Detailed validation results")
        schema.add_map_field('validation_metrics', 
                           description="Metrics from the validation")
        
        # Add additional metadata
        schema.add_float_field('quality_score', 
                             description="Quality score for this validation")
        schema.add_timestamp_field('created_at', 
                                 description="When this record was created")
        
        return schema
    
    @staticmethod
    def alert_schema() -> FirestoreSchema:
        """
        Get the schema for alerts collection.
        
        Returns:
            Schema for alerts
        """
        schema = FirestoreSchema('alerts')
        
        # Add required fields
        schema.add_string_field('alert_id', required=True, 
                               description="Unique identifier for the alert")
        schema.add_string_field('execution_id', required=True, 
                               description="Pipeline execution ID this alert is for")
        schema.add_string_field('alert_type', required=True, 
                               description="Type of alert (data quality, pipeline failure, etc.)")
        schema.add_string_field('severity', required=True, 
                               description="Alert severity (CRITICAL, HIGH, MEDIUM, LOW)")
        
        # Add alert details
        schema.add_timestamp_field('created_at', 
                                 description="When alert was created")
        schema.add_boolean_field('acknowledged', 
                               description="Whether alert has been acknowledged")
        schema.add_timestamp_field('acknowledged_at', 
                                 description="When alert was acknowledged")
        schema.add_string_field('acknowledged_by', 
                              description="Who acknowledged the alert")
        
        # Add detailed information
        schema.add_map_field('alert_details', 
                           description="Detailed information about the alert")
        
        # Add related entity information
        schema.add_string_field('related_entity_id', 
                              description="ID of the entity related to this alert")
        schema.add_string_field('related_entity_type', 
                              description="Type of entity related to this alert")
        
        return schema
    
    @staticmethod
    def configuration_schema() -> FirestoreSchema:
        """
        Get the schema for configuration collection.
        
        Returns:
            Schema for configuration
        """
        schema = FirestoreSchema('configuration')
        
        # Add required fields
        schema.add_string_field('config_id', required=True, 
                               description="Unique identifier for the configuration")
        schema.add_string_field('config_type', required=True, 
                               description="Type of configuration (pipeline, validation, healing, etc.)")
        
        # Add descriptive fields
        schema.add_string_field('name', 
                              description="Human-readable name of the configuration")
        schema.add_string_field('description', 
                              description="Detailed description of the configuration")
        schema.add_timestamp_field('created_at', 
                                 description="When configuration was created")
        schema.add_timestamp_field('updated_at', 
                                 description="When configuration was last updated")
        
        # Add configuration values
        schema.add_map_field('config_values', 
                           description="Configuration values as key-value pairs")
        
        # Add status fields
        schema.add_boolean_field('is_active', 
                               description="Whether this configuration is currently active")
        schema.add_string_field('version', 
                              description="Version of this configuration")
        
        return schema