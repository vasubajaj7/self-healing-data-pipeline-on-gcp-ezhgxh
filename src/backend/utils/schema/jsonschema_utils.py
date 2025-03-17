"""
Utility functions for working with JSON Schema in the self-healing data pipeline.

This module provides comprehensive tools for JSON Schema management, including:
- Schema parsing and validation
- Data validation against schemas
- Schema inference from data samples
- Conversion between JSON Schema and BigQuery schemas
- Schema compatibility checking
- Schema fingerprinting and normalization
- Batched validation with detailed error reporting

These utilities support the pipeline's schema validation framework and enable
self-healing capabilities for schema-related issues.
"""

import json
import typing
import io
import os
import hashlib
from typing import Union, Dict, List, Optional, Any, Tuple

import jsonschema
from jsonschema import validators
from google.cloud.bigquery import SchemaField
import pandas as pd
from genson import SchemaBuilder

from ...constants import FileFormat
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import SchemaError, DataError

# Configure module logger
logger = get_logger(__name__)

# Default JSON Schema draft version
JSON_SCHEMA_DRAFT_VERSION = "http://json-schema.org/draft-07/schema#"


def parse_json_schema(schema_input: Union[str, dict, io.IOBase]) -> dict:
    """
    Parse a JSON Schema from a string, file, or dictionary.
    
    Args:
        schema_input: The schema input, which can be:
                      - A file path (string ending with .json)
                      - A JSON string
                      - A file-like object
                      - A dictionary already containing the schema
    
    Returns:
        Parsed JSON Schema as a dictionary
        
    Raises:
        ValueError: If the input cannot be parsed as a valid JSON Schema
        FileNotFoundError: If the input is a file path that doesn't exist
    """
    schema = None
    
    if isinstance(schema_input, dict):
        schema = schema_input
    elif isinstance(schema_input, str):
        # Check if it's a file path
        if schema_input.endswith('.json') and os.path.isfile(schema_input):
            try:
                with open(schema_input, 'r') as f:
                    schema = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in schema file {schema_input}: {str(e)}")
            except Exception as e:
                raise ValueError(f"Error reading schema file {schema_input}: {str(e)}")
        else:
            # Try to parse as a JSON string
            try:
                schema = json.loads(schema_input)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON schema string: {str(e)}")
    elif hasattr(schema_input, 'read'):
        # File-like object
        try:
            schema = json.load(schema_input)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file object: {str(e)}")
    else:
        raise TypeError(f"Unsupported schema_input type: {type(schema_input)}")
    
    # Validate that the result looks like a JSON Schema
    if not isinstance(schema, dict):
        raise ValueError("Schema must be a JSON object (dictionary)")
    
    # A valid schema usually has type, properties, or $schema properties
    schema_indicators = ['type', 'properties', '$schema', 'items', 'required', 'definitions']
    if not any(indicator in schema for indicator in schema_indicators):
        logger.warning("The parsed schema may not be a valid JSON Schema (missing common properties)")
    
    return schema


def validate_json_schema(schema: Union[dict, str], draft_version: str = JSON_SCHEMA_DRAFT_VERSION) -> bool:
    """
    Validate that a given schema is a valid JSON Schema.
    
    Args:
        schema: The schema to validate, as a dictionary or JSON string
        draft_version: The JSON Schema draft version to validate against
    
    Returns:
        True if the schema is valid, False otherwise
    """
    # Parse schema if it's a string
    if isinstance(schema, str):
        try:
            schema = parse_json_schema(schema)
        except Exception as e:
            logger.error(f"Failed to parse schema: {str(e)}")
            return False
    
    # Get the appropriate validator for the draft version
    try:
        # Map draft URI to validator class
        if draft_version == "http://json-schema.org/draft-07/schema#":
            validator_cls = jsonschema.validators.Draft7Validator
        elif draft_version == "http://json-schema.org/draft-06/schema#":
            validator_cls = jsonschema.validators.Draft6Validator
        elif draft_version == "http://json-schema.org/draft-04/schema#":
            validator_cls = jsonschema.validators.Draft4Validator
        else:
            validator_cls = jsonschema.validators.validator_for(draft_version)
        
        # A schema is valid if it can validate itself (meta-validation)
        meta_schema = validator_cls.META_SCHEMA
        jsonschema.validate(schema, meta_schema)
        
        # Additional check - try creating a validator with the schema
        validator_cls(schema)
        
        return True
    except Exception as e:
        logger.error(f"Invalid JSON schema: {str(e)}")
        return False


def validate_data_against_schema(data: Union[dict, list], schema: Union[dict, str], 
                                raise_exception: bool = False) -> Union[bool, dict]:
    """
    Validate data against a JSON Schema.
    
    Args:
        data: The data to validate (object or array)
        schema: The JSON Schema to validate against
        raise_exception: If True, raises SchemaError on validation failure
    
    Returns:
        If validation succeeds: True
        If validation fails and raise_exception is False: Dict with error details
    
    Raises:
        SchemaError: If validation fails and raise_exception is True
    """
    # Parse the schema if needed
    if isinstance(schema, str):
        schema = parse_json_schema(schema)
    
    # Validate the schema first
    if not validate_json_schema(schema):
        if raise_exception:
            raise SchemaError(
                message="Invalid JSON Schema",
                data_source="schema_validation",
                schema_details={"schema": schema},
                self_healable=False
            )
        return {
            "valid": False,
            "errors": [{"message": "Invalid JSON Schema"}]
        }
    
    # Create a validator for the schema
    validator = jsonschema.Draft7Validator(schema)
    
    # Validate the data
    errors = []
    
    # If data is a list, validate each item
    if isinstance(data, list):
        for i, item in enumerate(data):
            item_errors = list(validator.iter_errors(item))
            if item_errors:
                for error in item_errors:
                    errors.append({
                        "index": i,
                        "path": list(error.path),
                        "message": error.message,
                        "schema_path": list(error.schema_path)
                    })
    else:
        # Validate the single data object
        for error in validator.iter_errors(data):
            errors.append({
                "path": list(error.path),
                "message": error.message,
                "schema_path": list(error.schema_path)
            })
    
    # Check for validation errors
    if errors:
        if raise_exception:
            raise SchemaError(
                message=f"Data validation failed with {len(errors)} errors",
                data_source="data_validation",
                schema_details={
                    "schema": schema,
                    "errors": errors
                }
            )
        return {
            "valid": False,
            "errors": errors
        }
    
    return True


def infer_json_schema(data: Union[dict, list, pd.DataFrame], title: str = None, 
                     description: str = None, options: dict = None) -> dict:
    """
    Infer a JSON Schema from sample data.
    
    Args:
        data: Sample data to infer schema from (dict, list, or DataFrame)
        title: Optional schema title
        description: Optional schema description
        options: Additional options for schema generation:
                 - required: List of required property names
                 - formats: Dict mapping property names to format strings
                 - additional_properties: Bool to control additionalProperties
                 - strict: Bool to enable strict type inference
    
    Returns:
        Inferred JSON Schema as a dictionary
    """
    # Set default options
    options = options or {}
    strict = options.get('strict', False)
    
    # Handle pandas DataFrame input
    if isinstance(data, pd.DataFrame):
        if data.empty:
            # Create an empty schema for an empty DataFrame
            schema = {
                "$schema": JSON_SCHEMA_DRAFT_VERSION,
                "type": "object",
                "properties": {}
            }
        else:
            # Convert DataFrame to a list of records
            records = data.to_dict(orient='records')
            
            # Initialize SchemaBuilder
            builder = SchemaBuilder(schema_uri=JSON_SCHEMA_DRAFT_VERSION)
            builder.add_object(records[0])  # Add first record to establish structure
            
            # Add the rest of the records
            for record in records[1:]:
                builder.add_object(record)
            
            # Generate schema
            schema = builder.to_schema()
            
            # Enhance schema with pandas dtype information
            for col, dtype in data.dtypes.items():
                if col in schema.get('properties', {}):
                    json_type = map_pandas_dtype_to_json_schema(dtype)
                    # Update the property with more specific type information
                    schema['properties'][col].update(json_type)
    else:
        # Handle dict or list input
        builder = SchemaBuilder(schema_uri=JSON_SCHEMA_DRAFT_VERSION)
        
        # Add the data to the builder
        if isinstance(data, list):
            for item in data:
                builder.add_object(item)
        else:
            builder.add_object(data)
        
        # Generate schema
        schema = builder.to_schema()
    
    # Add title and description if provided
    if title:
        schema['title'] = title
    if description:
        schema['description'] = description
    
    # Apply additional options
    if 'required' in options:
        schema['required'] = options['required']
    
    # Apply format specifications
    if 'formats' in options and isinstance(options['formats'], dict):
        for prop, fmt in options['formats'].items():
            if 'properties' in schema and prop in schema['properties']:
                schema['properties'][prop]['format'] = fmt
    
    # Set additionalProperties
    if 'additional_properties' in options:
        schema['additionalProperties'] = bool(options['additional_properties'])
    
    # Additional schema customizations based on options
    if strict:
        # In strict mode, we don't allow type variations
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema.get('type'), list) and len(prop_schema['type']) > 1:
                    # Find the most specific type and use only that
                    type_priority = ['object', 'array', 'string', 'number', 'integer', 'boolean', 'null']
                    for t in type_priority:
                        if t in prop_schema['type']:
                            prop_schema['type'] = t
                            break
    
    return schema


def convert_json_schema_to_bigquery_schema(json_schema: Union[dict, str]) -> List[SchemaField]:
    """
    Convert a JSON Schema to BigQuery table schema format.
    
    Args:
        json_schema: JSON Schema as a dictionary or string
    
    Returns:
        List of BigQuery SchemaField objects
    
    Raises:
        ValueError: If the schema cannot be converted to BigQuery format
    """
    # Parse the schema if it's a string
    if isinstance(json_schema, str):
        json_schema = parse_json_schema(json_schema)
    
    # Validate the schema
    if not validate_json_schema(json_schema):
        raise ValueError("Invalid JSON Schema provided")
    
    # Initialize result list
    bq_schema = []
    
    # Helper function to convert JSON Schema types to BigQuery types
    def convert_type(prop_name, prop_schema):
        # Determine the BigQuery mode
        mode = 'NULLABLE'  # Default
        if 'required' in json_schema and prop_name in json_schema.get('required', []):
            mode = 'REQUIRED'
        
        # Handle arrays
        if prop_schema.get('type') == 'array':
            mode = 'REPEATED'
            items_schema = prop_schema.get('items', {})
            
            # Determine the items type
            if items_schema.get('type') == 'object':
                # Nested object in array
                fields = convert_properties(items_schema.get('properties', {}), 
                                          items_schema.get('required', []))
                return SchemaField(
                    name=prop_name,
                    field_type='RECORD',
                    mode=mode,
                    fields=fields
                )
            else:
                # Simple array
                field_type = json_type_to_bq_type(items_schema.get('type', 'string'))
                return SchemaField(
                    name=prop_name,
                    field_type=field_type,
                    mode=mode
                )
        
        # Handle objects (RECORD type in BigQuery)
        elif prop_schema.get('type') == 'object':
            fields = convert_properties(prop_schema.get('properties', {}), 
                                      prop_schema.get('required', []))
            return SchemaField(
                name=prop_name,
                field_type='RECORD',
                mode=mode,
                fields=fields
            )
        
        # Handle simple types
        else:
            json_type = prop_schema.get('type', 'string')
            # Handle type arrays like ["string", "null"]
            if isinstance(json_type, list):
                if 'null' in json_type:
                    mode = 'NULLABLE'
                    json_type = next((t for t in json_type if t != 'null'), 'string')
                else:
                    json_type = json_type[0]  # Just use the first type
            
            field_type = json_type_to_bq_type(json_type)
            
            # Handle format for dates and times
            if prop_schema.get('format') == 'date-time':
                field_type = 'TIMESTAMP'
            elif prop_schema.get('format') == 'date':
                field_type = 'DATE'
            elif prop_schema.get('format') == 'time':
                field_type = 'TIME'
            
            return SchemaField(
                name=prop_name,
                field_type=field_type,
                mode=mode,
                description=prop_schema.get('description')
            )
    
    # Helper function to map JSON Schema types to BigQuery types
    def json_type_to_bq_type(json_type):
        type_map = {
            'string': 'STRING',
            'integer': 'INTEGER',
            'number': 'FLOAT',
            'boolean': 'BOOLEAN',
            'object': 'RECORD',
            'array': 'RECORD',  # Will be handled differently with REPEATED mode
            'null': 'STRING'    # Default to STRING for null
        }
        return type_map.get(json_type, 'STRING')
    
    # Helper function to convert object properties
    def convert_properties(properties, required_props=None):
        fields = []
        required_props = required_props or []
        
        for prop_name, prop_schema in properties.items():
            # Set required mode for this specific property if in required list
            if prop_name in required_props and isinstance(prop_schema, dict):
                prop_required = True
                if 'type' in prop_schema and isinstance(prop_schema['type'], list) and 'null' in prop_schema['type']:
                    prop_required = False
            else:
                prop_required = False
            
            field = convert_type(prop_name, prop_schema)
            fields.append(field)
        
        return fields
    
    # Start conversion from the root
    if 'properties' in json_schema and json_schema.get('type') == 'object':
        bq_schema = convert_properties(
            json_schema['properties'],
            json_schema.get('required', [])
        )
    elif 'items' in json_schema and json_schema.get('type') == 'array':
        # For array at the root, we need to handle each array item
        items_schema = json_schema['items']
        if items_schema.get('type') == 'object' and 'properties' in items_schema:
            bq_schema = convert_properties(
                items_schema['properties'],
                items_schema.get('required', [])
            )
    else:
        raise ValueError("JSON Schema must have an object or array at the root")
    
    return bq_schema


def convert_bigquery_to_json_schema(bq_schema: List[SchemaField], title: str = None, 
                                  description: str = None) -> dict:
    """
    Convert a BigQuery table schema to JSON Schema format.
    
    Args:
        bq_schema: List of BigQuery SchemaField objects
        title: Optional schema title
        description: Optional schema description
    
    Returns:
        JSON Schema as a dictionary
    
    Raises:
        ValueError: If the BigQuery schema is invalid
    """
    # Validate input
    if not isinstance(bq_schema, list):
        raise ValueError("BigQuery schema must be a list of SchemaField objects")
    
    # Create base JSON Schema
    json_schema = {
        "$schema": JSON_SCHEMA_DRAFT_VERSION,
        "type": "object",
        "properties": {},
        "required": []
    }
    
    # Add title and description if provided
    if title:
        json_schema["title"] = title
    if description:
        json_schema["description"] = description
    
    # Helper function to convert BQ types to JSON Schema types
    def bq_type_to_json_type(field_type):
        type_map = {
            'STRING': {'type': 'string'},
            'INTEGER': {'type': 'integer'},
            'INT64': {'type': 'integer'},
            'FLOAT': {'type': 'number'},
            'FLOAT64': {'type': 'number'},
            'BOOLEAN': {'type': 'boolean'},
            'BOOL': {'type': 'boolean'},
            'BYTES': {'type': 'string', 'contentEncoding': 'base64'},
            'TIMESTAMP': {'type': 'string', 'format': 'date-time'},
            'DATE': {'type': 'string', 'format': 'date'},
            'TIME': {'type': 'string', 'format': 'time'},
            'DATETIME': {'type': 'string', 'format': 'date-time'},
            'NUMERIC': {'type': 'number'},
            'BIGNUMERIC': {'type': 'number'},
            'JSON': {'type': 'object', 'additionalProperties': True}
        }
        return type_map.get(field_type, {'type': 'string'})
    
    # Helper function to convert a SchemaField to JSON Schema property
    def convert_field(field):
        if field.field_type == 'RECORD':
            if field.mode == 'REPEATED':
                # Array of objects
                return {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': convert_fields(field.fields),
                        'required': [f.name for f in field.fields if f.mode == 'REQUIRED']
                    }
                }
            else:
                # Nested object
                return {
                    'type': 'object',
                    'properties': convert_fields(field.fields),
                    'required': [f.name for f in field.fields if f.mode == 'REQUIRED']
                }
        else:
            # Basic type
            json_type = bq_type_to_json_type(field.field_type)
            
            # Handle repeated fields (arrays)
            if field.mode == 'REPEATED':
                return {
                    'type': 'array',
                    'items': json_type
                }
            
            # Handle nullable fields
            if field.mode == 'NULLABLE':
                if isinstance(json_type['type'], list):
                    if 'null' not in json_type['type']:
                        json_type['type'].append('null')
                else:
                    json_type['type'] = [json_type['type'], 'null']
                    
            # Add description if available
            if field.description:
                json_type['description'] = field.description
                
            return json_type
    
    # Helper function to convert multiple fields
    def convert_fields(fields):
        properties = {}
        for field in fields:
            properties[field.name] = convert_field(field)
        return properties
    
    # Convert all fields
    json_schema["properties"] = convert_fields(bq_schema)
    
    # Add required properties
    for field in bq_schema:
        if field.mode == 'REQUIRED':
            json_schema["required"].append(field.name)
    
    # If no required fields, remove the required array
    if not json_schema["required"]:
        del json_schema["required"]
    
    return json_schema


def is_schema_compatible(existing_schema: Union[dict, str], new_schema: Union[dict, str], 
                       compatibility_type: str = "backward") -> bool:
    """
    Check if a new schema is compatible with an existing schema.
    
    Args:
        existing_schema: The existing schema as a dictionary or string
        new_schema: The new schema as a dictionary or string
        compatibility_type: Type of compatibility to check:
                           - "backward": New schema can validate data from old schema
                           - "forward": Old schema can validate data from new schema
                           - "full": Both backward and forward compatibility
    
    Returns:
        True if schemas are compatible, False otherwise
    """
    # Parse schemas if needed
    if isinstance(existing_schema, str):
        existing_schema = parse_json_schema(existing_schema)
    if isinstance(new_schema, str):
        new_schema = parse_json_schema(new_schema)
    
    # Validate both schemas
    if not validate_json_schema(existing_schema) or not validate_json_schema(new_schema):
        return False
    
    # Compatibility checks based on type
    if compatibility_type == "backward":
        return _check_backward_compatibility(existing_schema, new_schema)
    elif compatibility_type == "forward":
        return _check_forward_compatibility(existing_schema, new_schema)
    elif compatibility_type == "full":
        return (_check_backward_compatibility(existing_schema, new_schema) and
                _check_forward_compatibility(existing_schema, new_schema))
    else:
        raise ValueError(f"Unsupported compatibility type: {compatibility_type}")


def _check_backward_compatibility(existing_schema: dict, new_schema: dict) -> bool:
    """
    Check if the new schema is backward compatible with the existing schema.
    
    Args:
        existing_schema: The existing schema
        new_schema: The new schema
    
    Returns:
        True if the new schema is backward compatible, False otherwise
    """
    # In backward compatibility, the new schema should accept data valid under the old schema
    
    # Check if schema type is the same
    if existing_schema.get('type') != new_schema.get('type'):
        return False
    
    # For object schemas, check properties
    if existing_schema.get('type') == 'object' and 'properties' in existing_schema:
        # Get required properties from both schemas
        existing_required = set(existing_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        # New schema cannot add required fields that weren't in the old schema
        if new_required - existing_required:
            return False
        
        # Check properties in existing schema
        for prop_name, prop_schema in existing_schema.get('properties', {}).items():
            # If property exists in new schema, it must be compatible
            if prop_name in new_schema.get('properties', {}):
                if not _is_property_compatible(prop_schema, new_schema['properties'][prop_name]):
                    return False
            # If property doesn't exist in new schema, additionalProperties must be true
            elif not new_schema.get('additionalProperties', True):
                return False
    
    # For array schemas, check items
    elif existing_schema.get('type') == 'array' and 'items' in existing_schema:
        if 'items' not in new_schema:
            return False
        
        # Check if array items are compatible
        if not _is_property_compatible(existing_schema['items'], new_schema['items']):
            return False
    
    # Default to saying it's compatible
    return True


def _check_forward_compatibility(existing_schema: dict, new_schema: dict) -> bool:
    """
    Check if the new schema is forward compatible with the existing schema.
    
    Args:
        existing_schema: The existing schema
        new_schema: The new schema
    
    Returns:
        True if the new schema is forward compatible, False otherwise
    """
    # In forward compatibility, the old schema should accept data valid under the new schema
    
    # Check if schema type is the same
    if existing_schema.get('type') != new_schema.get('type'):
        return False
    
    # For object schemas, check properties
    if new_schema.get('type') == 'object' and 'properties' in new_schema:
        # Get required properties from both schemas
        existing_required = set(existing_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        # Old schema must have all required fields in new schema
        if not existing_required.issuperset(new_required):
            return False
        
        # Check properties in new schema
        for prop_name, prop_schema in new_schema.get('properties', {}).items():
            # If property exists in old schema, it must be compatible
            if prop_name in existing_schema.get('properties', {}):
                if not _is_property_compatible(prop_schema, existing_schema['properties'][prop_name]):
                    return False
            # If property doesn't exist in old schema, additionalProperties must be true
            elif not existing_schema.get('additionalProperties', True):
                return False
    
    # For array schemas, check items
    elif new_schema.get('type') == 'array' and 'items' in new_schema:
        if 'items' not in existing_schema:
            return False
        
        # Check if array items are compatible
        if not _is_property_compatible(new_schema['items'], existing_schema['items']):
            return False
    
    # Default to saying it's compatible
    return True


def _is_property_compatible(schema1: dict, schema2: dict) -> bool:
    """
    Check if two property schemas are compatible.
    
    Args:
        schema1: First property schema
        schema2: Second property schema
    
    Returns:
        True if compatible, False otherwise
    """
    # Get the types from both schemas
    type1 = schema1.get('type')
    type2 = schema2.get('type')
    
    # Handle arrays of types (like ["string", "null"])
    if isinstance(type1, list):
        type1_set = set(type1)
    else:
        type1_set = {type1} if type1 else set()
    
    if isinstance(type2, list):
        type2_set = set(type2)
    else:
        type2_set = {type2} if type2 else set()
    
    # If both have explicit types, check compatibility
    if type1 and type2:
        # If types don't intersect at all, they're not compatible
        if not (type1_set & type2_set) and 'null' not in type1_set and 'null' not in type2_set:
            return False
    
    # For objects, recursively check properties
    if 'object' in type1_set and 'object' in type2_set:
        # Check properties if present in both
        if 'properties' in schema1 and 'properties' in schema2:
            # Recursively check each common property
            common_props = set(schema1['properties']) & set(schema2['properties'])
            for prop in common_props:
                if not _is_property_compatible(schema1['properties'][prop], schema2['properties'][prop]):
                    return False
    
    # For arrays, check item compatibility
    if 'array' in type1_set and 'array' in type2_set:
        if 'items' in schema1 and 'items' in schema2:
            if not _is_property_compatible(schema1['items'], schema2['items']):
                return False
    
    # Default to compatible
    return True


def get_schema_fingerprint(schema: Union[dict, str], algorithm: str = 'sha256') -> str:
    """
    Generate a fingerprint for a JSON Schema to uniquely identify it.
    
    Args:
        schema: The schema to fingerprint
        algorithm: Hash algorithm to use (sha256, sha1, md5)
    
    Returns:
        Schema fingerprint as a string
    """
    # Parse schema if it's a string
    if isinstance(schema, str):
        schema = parse_json_schema(schema)
    
    # Normalize the schema to ensure consistent fingerprints
    normalized_schema = normalize_json_schema(schema)
    
    # Convert to canonical JSON string
    schema_str = json.dumps(normalized_schema, sort_keys=True, separators=(',', ':'))
    
    # Apply the requested hash algorithm
    if algorithm == 'sha256':
        hash_obj = hashlib.sha256(schema_str.encode('utf-8'))
    elif algorithm == 'sha1':
        hash_obj = hashlib.sha1(schema_str.encode('utf-8'))
    elif algorithm == 'md5':
        hash_obj = hashlib.md5(schema_str.encode('utf-8'))
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    # Return the hash as a hexadecimal string
    return hash_obj.hexdigest()


def extract_schema_from_json_file(file_input: Union[str, io.IOBase], title: str = None, 
                                description: str = None, options: dict = None) -> dict:
    """
    Extract or infer a JSON Schema from a JSON file.
    
    Args:
        file_input: Path to JSON file or file-like object
        title: Optional schema title
        description: Optional schema description
        options: Additional options for schema generation
    
    Returns:
        Extracted or inferred JSON Schema
    
    Raises:
        ValueError: If the file cannot be read or parsed
        FileNotFoundError: If the file path doesn't exist
    """
    # Read the JSON data from file
    if isinstance(file_input, str):
        if not os.path.isfile(file_input):
            raise FileNotFoundError(f"File not found: {file_input}")
        
        try:
            with open(file_input, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {file_input}: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error reading file {file_input}: {str(e)}")
    else:
        # File-like object
        try:
            data = json.load(file_input)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file object: {str(e)}")
    
    # Infer schema from the data
    return infer_json_schema(data, title=title, description=description, options=options)


def merge_json_schemas(schemas: list, title: str = None, description: str = None) -> dict:
    """
    Merge multiple JSON Schemas into a single schema.
    
    Args:
        schemas: List of JSON Schemas to merge
        title: Optional title for the merged schema
        description: Optional description for the merged schema
    
    Returns:
        Merged JSON Schema
    
    Raises:
        ValueError: If any schema is invalid or schemas cannot be merged
    """
    # Validate and parse all schemas
    parsed_schemas = []
    for i, schema in enumerate(schemas):
        try:
            if isinstance(schema, str):
                schema = parse_json_schema(schema)
            if not validate_json_schema(schema):
                raise ValueError(f"Invalid JSON Schema at index {i}")
            parsed_schemas.append(schema)
        except Exception as e:
            raise ValueError(f"Failed to parse schema at index {i}: {str(e)}")
    
    if not parsed_schemas:
        raise ValueError("No valid schemas provided for merging")
    
    # Create base merged schema
    merged_schema = {
        "$schema": JSON_SCHEMA_DRAFT_VERSION,
        "type": "object",
        "properties": {},
    }
    
    # Add title and description if provided
    if title:
        merged_schema["title"] = title
    if description:
        merged_schema["description"] = description
    
    # Track required properties across all schemas
    all_required = set()
    
    # Merge all schemas
    for schema in parsed_schemas:
        if schema.get('type') != 'object':
            raise ValueError("Can only merge schemas with root type 'object'")
        
        # Merge properties
        for prop_name, prop_schema in schema.get('properties', {}).items():
            if prop_name in merged_schema['properties']:
                # Property already exists, merge or use oneOf
                existing_prop = merged_schema['properties'][prop_name]
                if not _can_merge_property(existing_prop, prop_schema):
                    # Use oneOf when properties can't be directly merged
                    merged_schema['properties'][prop_name] = {
                        'oneOf': [existing_prop, prop_schema]
                    }
            else:
                # New property, just add it
                merged_schema['properties'][prop_name] = prop_schema
        
        # Collect required properties
        if 'required' in schema:
            all_required.update(schema['required'])
    
    # Add required properties that exist in the merged schema
    if all_required:
        merged_schema['required'] = list(all_required)
    
    return merged_schema


def _can_merge_property(prop1: dict, prop2: dict) -> bool:
    """
    Check if two property schemas can be directly merged.
    
    Args:
        prop1: First property schema
        prop2: Second property schema
    
    Returns:
        True if the properties can be merged, False otherwise
    """
    # Get types
    type1 = prop1.get('type')
    type2 = prop2.get('type')
    
    # Handle arrays of types
    if isinstance(type1, list):
        type1_set = set(type1)
    else:
        type1_set = {type1} if type1 else set()
    
    if isinstance(type2, list):
        type2_set = set(type2)
    else:
        type2_set = {type2} if type2 else set()
    
    # If types are completely different and neither includes null, can't merge
    if not (type1_set & type2_set) and 'null' not in type1_set and 'null' not in type2_set:
        return False
    
    # If both are objects with properties, check if those can be merged
    if 'object' in type1_set and 'object' in type2_set:
        if 'properties' in prop1 and 'properties' in prop2:
            # Check for conflicting property definitions
            common_props = set(prop1['properties']) & set(prop2['properties'])
            for prop in common_props:
                if not _can_merge_property(prop1['properties'][prop], prop2['properties'][prop]):
                    return False
    
    # If both are arrays, check if items can be merged
    if 'array' in type1_set and 'array' in type2_set:
        if 'items' in prop1 and 'items' in prop2:
            return _can_merge_property(prop1['items'], prop2['items'])
    
    # Default to saying they can be merged
    return True


def map_pandas_dtype_to_json_schema(dtype) -> dict:
    """
    Map pandas data types to corresponding JSON Schema types.
    
    Args:
        dtype: pandas data type
    
    Returns:
        Corresponding JSON Schema type definition
    """
    # Convert dtype to string for easier comparison
    dtype_str = str(dtype)
    
    # Integer types
    if 'int' in dtype_str:
        return {'type': 'integer'}
    
    # Float types
    elif 'float' in dtype_str:
        return {'type': 'number'}
    
    # Boolean type
    elif 'bool' in dtype_str:
        return {'type': 'boolean'}
    
    # Date and time types
    elif 'datetime' in dtype_str:
        return {'type': 'string', 'format': 'date-time'}
    elif 'date' in dtype_str:
        return {'type': 'string', 'format': 'date'}
    elif 'time' in dtype_str:
        return {'type': 'string', 'format': 'time'}
    
    # Complex types
    elif 'complex' in dtype_str:
        return {'type': 'string', 'description': 'Complex number stored as string'}
    
    # Category type
    elif 'category' in dtype_str:
        return {'type': 'string'}
    
    # Object type (often strings in pandas)
    elif 'object' in dtype_str:
        return {'type': 'string'}
    
    # Default to string for anything else
    else:
        return {'type': 'string'}


def normalize_json_schema(schema: Union[dict, str]) -> dict:
    """
    Normalize a JSON Schema to a canonical form.
    
    Args:
        schema: The schema to normalize
    
    Returns:
        Normalized JSON Schema
    """
    # Parse schema if it's a string
    if isinstance(schema, str):
        schema = parse_json_schema(schema)
    
    # Create a new schema to ensure we don't modify the input
    normalized = {}
    
    # Helper function for recursive normalization
    def normalize_object(obj):
        if isinstance(obj, dict):
            # Create a new dictionary for the normalized object
            norm_obj = {}
            
            # Process keys in alphabetical order
            for key in sorted(obj.keys()):
                value = obj[key]
                
                # Normalize type fields
                if key == 'type':
                    if isinstance(value, list):
                        # Sort type arrays and remove duplicates
                        value = sorted(set(value))
                        # If there's only one type, use it directly
                        if len(value) == 1:
                            value = value[0]
                
                # Recursively normalize nested objects and arrays
                norm_obj[key] = normalize_object(value)
            
            return norm_obj
        elif isinstance(obj, list):
            # Normalize each item in the list
            return [normalize_object(item) for item in obj]
        else:
            # Return primitive values as-is
            return obj
    
    # Normalize the schema
    normalized = normalize_object(schema)
    
    # Ensure $schema property is present
    if '$schema' not in normalized:
        normalized['$schema'] = JSON_SCHEMA_DRAFT_VERSION
    
    return normalized


class JsonSchemaValidator:
    """
    Class for validating data against JSON Schemas with advanced features.
    
    Provides methods for validating individual data items and batches,
    with detailed error reporting and customizable validation behavior.
    """
    
    def __init__(self, schema: Union[dict, str], options: dict = None):
        """
        Initialize the JSON Schema validator with a schema.
        
        Args:
            schema: JSON Schema as a dictionary or string
            options: Validation options:
                    - format_checker: Enable format validation
                    - required_by_default: Treat properties as required by default
                    - additional_properties: Control additionalProperties default
        """
        # Parse and validate the schema
        if isinstance(schema, str):
            self._schema = parse_json_schema(schema)
        else:
            self._schema = schema
        
        if not validate_json_schema(self._schema):
            raise ValueError("Invalid JSON Schema provided")
        
        # Store options
        self._options = options or {}
        
        # Create validator
        format_checker = None
        if self._options.get('format_checker', True):
            format_checker = jsonschema.FormatChecker()
        
        # Create validator
        self._validator = jsonschema.Draft7Validator(
            self._schema,
            format_checker=format_checker
        )
    
    def validate(self, data: Union[dict, list], raise_exception: bool = False) -> Union[bool, dict]:
        """
        Validate data against the schema.
        
        Args:
            data: The data to validate (object or array)
            raise_exception: If True, raises SchemaError on validation failure
        
        Returns:
            If validation succeeds: True
            If validation fails and raise_exception is False: Dict with error details
        
        Raises:
            SchemaError: If validation fails and raise_exception is True
        """
        # Validate the data
        errors = []
        
        # If data is a list, validate each item
        if isinstance(data, list):
            for i, item in enumerate(data):
                item_errors = list(self._validator.iter_errors(item))
                if item_errors:
                    for error in item_errors:
                        errors.append({
                            "index": i,
                            "path": list(error.path),
                            "message": error.message,
                            "schema_path": list(error.schema_path)
                        })
        else:
            # Validate the single data object
            for error in self._validator.iter_errors(data):
                errors.append({
                    "path": list(error.path),
                    "message": error.message,
                    "schema_path": list(error.schema_path)
                })
        
        # Check for validation errors
        if errors:
            if raise_exception:
                raise SchemaError(
                    message=f"Data validation failed with {len(errors)} errors",
                    data_source="validator",
                    schema_details={
                        "errors": errors
                    }
                )
            return {
                "valid": False,
                "errors": errors
            }
        
        return True
    
    def validate_batch(self, data_items: list, fail_fast: bool = False) -> dict:
        """
        Validate a batch of data items against the schema.
        
        Args:
            data_items: List of data items to validate
            fail_fast: If True, stops validation at first failure
        
        Returns:
            Dict with validation results:
            {
                "valid_count": int,
                "invalid_count": int,
                "success_rate": float,
                "errors": [
                    {
                        "index": int,
                        "errors": [error_details]
                    }
                ]
            }
        """
        valid_count = 0
        invalid_count = 0
        errors = []
        
        # Process each item in the batch
        for i, item in enumerate(data_items):
            item_errors = list(self._validator.iter_errors(item))
            if item_errors:
                invalid_count += 1
                errors.append({
                    "index": i,
                    "errors": self.format_error(item_errors)
                })
                
                if fail_fast:
                    break
            else:
                valid_count += 1
        
        # Calculate success rate
        total_processed = valid_count + invalid_count
        success_rate = (valid_count / total_processed) if total_processed > 0 else 0
        
        return {
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "total_processed": total_processed,
            "total_items": len(data_items),
            "success_rate": success_rate,
            "errors": errors
        }
    
    def get_schema(self) -> dict:
        """
        Get the current schema.
        
        Returns:
            Current JSON Schema
        """
        return dict(self._schema)  # Return a copy to prevent modification
    
    def update_schema(self, schema: Union[dict, str]) -> None:
        """
        Update the validator with a new schema.
        
        Args:
            schema: New JSON Schema as a dictionary or string
        
        Raises:
            ValueError: If the schema is invalid
        """
        # Parse and validate the new schema
        if isinstance(schema, str):
            new_schema = parse_json_schema(schema)
        else:
            new_schema = schema
        
        if not validate_json_schema(new_schema):
            raise ValueError("Invalid JSON Schema provided")
        
        # Update schema and recreate validator
        self._schema = new_schema
        format_checker = None
        if self._options.get('format_checker', True):
            format_checker = jsonschema.FormatChecker()
            
        self._validator = jsonschema.Draft7Validator(
            self._schema,
            format_checker=format_checker
        )
    
    def format_error(self, errors: list) -> list:
        """
        Format validation errors into a readable structure.
        
        Args:
            errors: List of jsonschema ValidationError objects
        
        Returns:
            List of formatted error dictionaries
        """
        formatted_errors = []
        
        for error in errors:
            formatted_error = {
                "path": list(error.path) if error.path else [],
                "message": error.message,
                "schema_path": list(error.schema_path) if error.schema_path else []
            }
            
            # Add validator info if available
            if hasattr(error, 'validator') and error.validator:
                formatted_error["validator"] = error.validator
            
            # Add validator_value if available
            if hasattr(error, 'validator_value') and error.validator_value:
                formatted_error["validator_value"] = error.validator_value
            
            # Add instance value (what failed validation)
            if hasattr(error, 'instance'):
                instance = error.instance
                # Convert complex instances to strings to ensure JSON serialization
                if isinstance(instance, (dict, list)):
                    # For complex objects, we might want to limit the size
                    formatted_error["instance"] = "Complex object"
                else:
                    formatted_error["instance"] = instance
            
            formatted_errors.append(formatted_error)
        
        return formatted_errors