"""
Utility functions for working with Apache Avro schemas in the self-healing data pipeline.

Provides capabilities for schema validation, conversion between Avro and BigQuery schemas,
schema compatibility checking, and Avro-specific data processing.
"""

import json
import typing
import io
import os
from typing import Union, Dict, List, Any, Optional, Tuple

import avro  # version 1.11.x
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pandas as pd  # version 2.0.x
from google.cloud.bigquery import SchemaField  # version 3.11.x

from ...constants import FileFormat
from ...utils.logging.logger import get_logger
from ...utils.errors.error_types import SchemaError, DataError

# Configure logger
logger = get_logger(__name__)


def parse_avro_schema(schema_input: Union[str, dict, io.IOBase]) -> avro.schema.Schema:
    """
    Parse an Avro schema from a string, file, or dictionary.
    
    Args:
        schema_input: Schema input as a file path, JSON string, file object, or dictionary
        
    Returns:
        Parsed Avro schema object
        
    Raises:
        SchemaError: If the schema cannot be parsed or is invalid
    """
    try:
        # Handle different input types
        if isinstance(schema_input, str):
            # Check if it's a file path
            if os.path.isfile(schema_input):
                with open(schema_input, 'r') as f:
                    schema_json = json.load(f)
            else:
                # Assume it's a JSON string
                schema_json = json.loads(schema_input)
        elif isinstance(schema_input, io.IOBase):
            # File object
            schema_json = json.load(schema_input)
        elif isinstance(schema_input, dict):
            # Already a dictionary
            schema_json = schema_input
        else:
            raise ValueError(f"Unsupported schema input type: {type(schema_input)}")
        
        # Parse the schema
        schema = avro.schema.parse(json.dumps(schema_json))
        return schema
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema: {e}")
        raise SchemaError(
            message=f"Invalid JSON in Avro schema: {e}",
            data_source="avro_schema",
            schema_details={"error": str(e)}
        )
    except avro.schema.SchemaParseException as e:
        logger.error(f"Invalid Avro schema: {e}")
        raise SchemaError(
            message=f"Invalid Avro schema: {e}",
            data_source="avro_schema",
            schema_details={"error": str(e)}
        )
    except Exception as e:
        logger.error(f"Error parsing Avro schema: {e}")
        raise SchemaError(
            message=f"Error parsing Avro schema: {e}",
            data_source="avro_schema",
            schema_details={"error": str(e)}
        )


def validate_avro_schema(schema: Union[dict, str, avro.schema.Schema]) -> bool:
    """
    Validate that a given schema is a valid Avro schema.
    
    Args:
        schema: Schema to validate as a dictionary, string, or Avro schema object
        
    Returns:
        True if schema is valid, False otherwise
    """
    try:
        # If schema is not already an Avro schema object, parse it
        if not isinstance(schema, avro.schema.Schema):
            schema = parse_avro_schema(schema)
        
        # If we've reached here, schema was successfully parsed
        return True
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return False


def validate_data_against_avro_schema(
    data: Union[dict, List[dict]],
    schema: Union[dict, str, avro.schema.Schema],
    raise_exception: bool = False
) -> Union[bool, dict]:
    """
    Validate data against an Avro schema.
    
    Args:
        data: Data to validate (single record or list of records)
        schema: Avro schema to validate against
        raise_exception: Whether to raise an exception on validation failure
        
    Returns:
        True if validation passes, or a dictionary with validation errors
        
    Raises:
        SchemaError: If raise_exception is True and validation fails
    """
    # Parse the schema if needed
    if not validate_avro_schema(schema):
        if raise_exception:
            raise SchemaError(
                message="Invalid Avro schema",
                data_source="avro_schema",
                schema_details={"schema": schema}
            )
        return {"valid": False, "errors": ["Invalid Avro schema"]}
    
    if not isinstance(schema, avro.schema.Schema):
        schema = parse_avro_schema(schema)
    
    # Prepare validator
    validator = AvroSchemaValidator(schema)
    
    # Handle single record or list of records
    if isinstance(data, list):
        return validator.validate_batch(data, fail_fast=raise_exception)
    else:
        return validator.validate(data, raise_exception=raise_exception)


def infer_avro_schema(
    data: Union[dict, List[dict], pd.DataFrame],
    namespace: str = "com.example",
    name: str = "Record",
    options: dict = None
) -> dict:
    """
    Infer an Avro schema from sample data.
    
    Args:
        data: Sample data to infer schema from
        namespace: Schema namespace
        name: Record name
        options: Additional options for schema generation
        
    Returns:
        Inferred Avro schema as a dictionary
    """
    options = options or {}
    
    # Convert DataFrame to dict or list of dicts if needed
    if isinstance(data, pd.DataFrame):
        if len(data) == 0:
            data = {col: None for col in data.columns}
        else:
            data = data.to_dict(orient='records')
            if len(data) > 0:
                data = data[0]  # Take first record for schema inference
    
    # Handle list of dicts
    if isinstance(data, list) and len(data) > 0:
        data = data[0]  # Take first record for schema inference
    
    if not isinstance(data, dict):
        raise ValueError(f"Cannot infer schema from data of type {type(data)}")
    
    # Start building the schema
    schema = {
        "type": "record",
        "namespace": namespace,
        "name": name,
        "fields": []
    }
    
    # Add fields based on data
    for field_name, field_value in data.items():
        field = {"name": field_name}
        
        # Determine field type
        if field_value is None:
            field["type"] = ["null", "string"]
        elif isinstance(field_value, str):
            field["type"] = "string"
        elif isinstance(field_value, int):
            field["type"] = "long"
        elif isinstance(field_value, float):
            field["type"] = "double"
        elif isinstance(field_value, bool):
            field["type"] = "boolean"
        elif isinstance(field_value, dict):
            # Recursive schema inference for nested records
            nested_schema = infer_avro_schema(
                field_value, 
                namespace=f"{namespace}.{name}",
                name=f"{field_name.capitalize()}Record"
            )
            field["type"] = nested_schema
        elif isinstance(field_value, list):
            if len(field_value) > 0:
                if isinstance(field_value[0], dict):
                    # Array of records
                    nested_schema = infer_avro_schema(
                        field_value[0],
                        namespace=f"{namespace}.{name}",
                        name=f"{field_name.capitalize()}Item"
                    )
                    field["type"] = {
                        "type": "array",
                        "items": nested_schema
                    }
                else:
                    # Array of primitives
                    item_type = type(field_value[0]).__name__
                    avro_type = {
                        "str": "string",
                        "int": "long",
                        "float": "double",
                        "bool": "boolean"
                    }.get(item_type, "string")
                    field["type"] = {
                        "type": "array",
                        "items": avro_type
                    }
            else:
                # Empty array, default to string items
                field["type"] = {
                    "type": "array",
                    "items": "string"
                }
        else:
            # Default to string for unknown types
            logger.warning(f"Unknown data type {type(field_value)} for field {field_name}, using string")
            field["type"] = "string"
        
        # Add default value if specified in options
        if "defaults" in options and field_name in options["defaults"]:
            field["default"] = options["defaults"][field_name]
        
        # Add documentation if specified in options
        if "docs" in options and field_name in options["docs"]:
            field["doc"] = options["docs"][field_name]
        
        schema["fields"].append(field)
    
    return schema


def convert_avro_to_bigquery_schema(avro_schema: Union[dict, str, avro.schema.Schema]) -> List[SchemaField]:
    """
    Convert an Avro schema to BigQuery table schema format.
    
    Args:
        avro_schema: Avro schema to convert
        
    Returns:
        BigQuery schema as a list of SchemaField objects
    """
    # Parse the schema if needed
    if not isinstance(avro_schema, avro.schema.Schema):
        avro_schema = parse_avro_schema(avro_schema)
    
    # Convert to JSON representation
    avro_schema_json = avro_schema_to_json(avro_schema)
    
    # Map Avro types to BigQuery types
    type_mapping = {
        "null": "STRING",
        "boolean": "BOOLEAN",
        "int": "INTEGER",
        "long": "INTEGER",
        "float": "FLOAT",
        "double": "FLOAT",
        "bytes": "BYTES",
        "string": "STRING",
        "record": "RECORD",
        "enum": "STRING",
        "array": "REPEATED",
        "map": "RECORD",
        "fixed": "BYTES"
    }
    
    def convert_field(field: dict) -> SchemaField:
        """Convert a single Avro field to a BigQuery SchemaField."""
        field_name = field["name"]
        field_type = field["type"]
        field_mode = "REQUIRED"
        fields = None
        
        # Handle union types (e.g., ["null", "string"])
        if isinstance(field_type, list):
            # If the field can be null, it's NULLABLE
            if "null" in field_type:
                field_mode = "NULLABLE"
                # Use the non-null type
                non_null_types = [t for t in field_type if t != "null"]
                if len(non_null_types) == 1:
                    field_type = non_null_types[0]
                else:
                    # Multiple non-null types, use STRING as default
                    field_type = "string"
                    logger.warning(f"Multiple non-null types in union for field {field_name}, using STRING")
            else:
                # Union of non-null types, use STRING as default
                field_type = "string"
                logger.warning(f"Union of non-null types for field {field_name}, using STRING")
        
        # Handle record types (nested structures)
        if isinstance(field_type, dict) and field_type.get("type") == "record":
            bq_type = "RECORD"
            # Recursively convert nested fields
            fields = [convert_field(nested_field) for nested_field in field_type.get("fields", [])]
        
        # Handle array types
        elif isinstance(field_type, dict) and field_type.get("type") == "array":
            field_mode = "REPEATED"
            items_type = field_type.get("items")
            
            # Handle nested records in arrays
            if isinstance(items_type, dict) and items_type.get("type") == "record":
                bq_type = "RECORD"
                fields = [convert_field(nested_field) for nested_field in items_type.get("fields", [])]
            else:
                # For primitive arrays, get the BigQuery type for the items
                if isinstance(items_type, str):
                    bq_type = type_mapping.get(items_type, "STRING")
                else:
                    bq_type = "STRING"
                    logger.warning(f"Complex array items type for field {field_name}, using STRING")
        
        # Handle map types
        elif isinstance(field_type, dict) and field_type.get("type") == "map":
            bq_type = "RECORD"
            # Maps in Avro become REPEATED RECORD with key/value fields in BigQuery
            fields = [
                SchemaField("key", "STRING", "REQUIRED"),
                SchemaField("value", type_mapping.get(field_type.get("values"), "STRING"), "REQUIRED")
            ]
            field_mode = "REPEATED"
        
        # Handle primitive types
        elif isinstance(field_type, str):
            bq_type = type_mapping.get(field_type, "STRING")
        
        # Handle logical types
        elif isinstance(field_type, dict) and "logicalType" in field_type:
            logical_type = field_type.get("logicalType")
            if logical_type == "timestamp-millis" or logical_type == "timestamp-micros":
                bq_type = "TIMESTAMP"
            elif logical_type == "date":
                bq_type = "DATE"
            elif logical_type == "time-millis" or logical_type == "time-micros":
                bq_type = "TIME"
            elif logical_type == "decimal":
                bq_type = "NUMERIC"
            else:
                bq_type = "STRING"
                logger.warning(f"Unknown logical type {logical_type} for field {field_name}, using STRING")
        else:
            bq_type = "STRING"
            logger.warning(f"Complex field type for {field_name}, using STRING")
        
        # Create the SchemaField object
        return SchemaField(
            name=field_name,
            field_type=bq_type,
            mode=field_mode,
            fields=fields,
            description=field.get("doc", None)
        )
    
    # Convert all top-level fields
    if "fields" not in avro_schema_json:
        raise SchemaError(
            message="Invalid Avro schema: missing 'fields' attribute",
            data_source="avro_schema",
            schema_details={"schema": avro_schema_json}
        )
    
    return [convert_field(field) for field in avro_schema_json["fields"]]


def convert_bigquery_to_avro_schema(bq_schema: List[SchemaField], namespace: str = "com.example", name: str = "Record") -> dict:
    """
    Convert a BigQuery table schema to Avro schema format.
    
    Args:
        bq_schema: BigQuery schema as a list of SchemaField objects
        namespace: Avro schema namespace
        name: Avro record name
        
    Returns:
        Avro schema as a dictionary
    """
    # Start building the Avro schema
    avro_schema = {
        "type": "record",
        "namespace": namespace,
        "name": name,
        "fields": []
    }
    
    # Map BigQuery types to Avro types
    type_mapping = {
        "STRING": "string",
        "BYTES": "bytes",
        "INTEGER": "long",
        "FLOAT": "double",
        "NUMERIC": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9},
        "BIGNUMERIC": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 38},
        "BOOLEAN": "boolean",
        "TIMESTAMP": {"type": "long", "logicalType": "timestamp-micros"},
        "DATE": {"type": "int", "logicalType": "date"},
        "TIME": {"type": "long", "logicalType": "time-micros"},
        "DATETIME": {"type": "string", "logicalType": "datetime"},
        "GEOGRAPHY": "string",
        "RECORD": "record"
    }
    
    def convert_field(field: SchemaField) -> dict:
        """Convert a single BigQuery SchemaField to an Avro field."""
        avro_field = {
            "name": field.name
        }
        
        if field.description:
            avro_field["doc"] = field.description
        
        # Handle REPEATED fields
        if field.mode == "REPEATED":
            if field.field_type == "RECORD":
                # Array of records
                nested_fields = [convert_field(nested) for nested in field.fields]
                avro_field["type"] = {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": f"{field.name.capitalize()}Item",
                        "fields": nested_fields
                    }
                }
            else:
                # Array of primitives
                avro_field["type"] = {
                    "type": "array",
                    "items": type_mapping.get(field.field_type, "string")
                }
        
        # Handle NULLABLE fields
        elif field.mode == "NULLABLE":
            if field.field_type == "RECORD":
                # Nullable record
                nested_fields = [convert_field(nested) for nested in field.fields]
                avro_field["type"] = [
                    "null",
                    {
                        "type": "record",
                        "name": f"{field.name.capitalize()}Record",
                        "fields": nested_fields
                    }
                ]
            else:
                # Nullable primitive
                avro_field["type"] = [
                    "null",
                    type_mapping.get(field.field_type, "string")
                ]
        
        # Handle REQUIRED fields
        else:
            if field.field_type == "RECORD":
                # Required record
                nested_fields = [convert_field(nested) for nested in field.fields]
                avro_field["type"] = {
                    "type": "record",
                    "name": f"{field.name.capitalize()}Record",
                    "fields": nested_fields
                }
            else:
                # Required primitive
                avro_field["type"] = type_mapping.get(field.field_type, "string")
        
        return avro_field
    
    # Convert all fields
    avro_schema["fields"] = [convert_field(field) for field in bq_schema]
    
    return avro_schema


def is_avro_schema_compatible(
    existing_schema: Union[dict, str, avro.schema.Schema],
    new_schema: Union[dict, str, avro.schema.Schema],
    compatibility_type: str = "backward"
) -> bool:
    """
    Check if a new schema is compatible with an existing schema.
    
    Args:
        existing_schema: The existing schema
        new_schema: The new schema to check compatibility for
        compatibility_type: Type of compatibility to check
                           'backward' - Can new schema read old data
                           'forward' - Can old schema read new data
                           'full' - Both backward and forward compatible
        
    Returns:
        True if schemas are compatible, False otherwise
    """
    # Parse the schemas if needed
    if not isinstance(existing_schema, avro.schema.Schema):
        existing_schema = parse_avro_schema(existing_schema)
    
    if not isinstance(new_schema, avro.schema.Schema):
        new_schema = parse_avro_schema(new_schema)
    
    try:
        # Use Avro's built-in compatibility checker if available (newer versions)
        if hasattr(avro.io, 'SchemaCompatibility'):
            compatibility = avro.io.SchemaCompatibility()
            
            if compatibility_type == "backward":
                return compatibility.can_read(new_schema, existing_schema)
            elif compatibility_type == "forward":
                return compatibility.can_read(existing_schema, new_schema)
            elif compatibility_type == "full":
                return (compatibility.can_read(new_schema, existing_schema) and 
                        compatibility.can_read(existing_schema, new_schema))
            else:
                logger.error(f"Invalid compatibility type: {compatibility_type}")
                return False
        else:
            # Fall back to manual compatibility checking
            # Schemas must be the same type and both records
            if existing_schema.type != new_schema.type or existing_schema.type != "record":
                logger.error("Schemas must both be record type")
                return False
            
            # Manual compatibility checking
            if compatibility_type == "backward":
                # Check field by field
                for new_field in new_schema.fields:
                    # Find corresponding field in existing schema
                    existing_field = next((f for f in existing_schema.fields if f.name == new_field.name), None)
                    
                    # If field doesn't exist in old schema, it must have a default value
                    if existing_field is None:
                        if not hasattr(new_field, "has_default") or not new_field.has_default:
                            logger.error(f"Field {new_field.name} is required in new schema but doesn't exist in old schema")
                            return False
                    else:
                        # Field exists in both, types should be compatible
                        # This is a simplified check - full compatibility checking is complex
                        if str(existing_field.type) != str(new_field.type):
                            logger.error(f"Field {new_field.name} has incompatible types")
                            return False
                
                return True
            
            elif compatibility_type == "forward":
                # Check field by field
                for existing_field in existing_schema.fields:
                    # Find corresponding field in new schema
                    new_field = next((f for f in new_schema.fields if f.name == existing_field.name), None)
                    
                    # If field doesn't exist in new schema, it must have a default value
                    if new_field is None:
                        if not hasattr(existing_field, "has_default") or not existing_field.has_default:
                            logger.error(f"Field {existing_field.name} is required in old schema but doesn't exist in new schema")
                            return False
                    else:
                        # Field exists in both, types should be compatible
                        # This is a simplified check - full compatibility checking is complex
                        if str(existing_field.type) != str(new_field.type):
                            logger.error(f"Field {existing_field.name} has incompatible types")
                            return False
                
                return True
            
            elif compatibility_type == "full":
                # Both backward and forward compatible
                backward = is_avro_schema_compatible(existing_schema, new_schema, "backward")
                forward = is_avro_schema_compatible(existing_schema, new_schema, "forward")
                return backward and forward
            
            else:
                logger.error(f"Invalid compatibility type: {compatibility_type}")
                return False
    
    except Exception as e:
        logger.error(f"Error checking schema compatibility: {e}")
        return False


def get_avro_schema_fingerprint(
    schema: Union[dict, str, avro.schema.Schema],
    algorithm: str = "md5"
) -> str:
    """
    Generate a fingerprint for an Avro schema to uniquely identify it.
    
    Args:
        schema: The schema to fingerprint
        algorithm: Hashing algorithm to use (md5, sha256)
        
    Returns:
        Schema fingerprint as a string
    """
    # Parse the schema if needed
    if not isinstance(schema, avro.schema.Schema):
        schema = parse_avro_schema(schema)
    
    # Generate the fingerprint
    if hasattr(schema, "fingerprint") and callable(getattr(schema, "fingerprint")):
        # Avro 1.9+ has built-in fingerprint method
        return schema.fingerprint(algorithm).hex()
    else:
        # Fall back to manual fingerprint generation
        schema_json = avro_schema_to_json(schema)
        canonical_schema = json.dumps(schema_json, sort_keys=True)
        
        if algorithm.lower() == "md5":
            import hashlib
            return hashlib.md5(canonical_schema.encode()).hexdigest()
        elif algorithm.lower() in ["sha256", "sha-256"]:
            import hashlib
            return hashlib.sha256(canonical_schema.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported fingerprint algorithm: {algorithm}")


def extract_schema_from_avro_file(file_input: Union[str, io.IOBase]) -> dict:
    """
    Extract the schema from an Avro data file.
    
    Args:
        file_input: Path to an Avro file or a file-like object
        
    Returns:
        Extracted Avro schema as a dictionary
        
    Raises:
        IOError: If the file cannot be read or is not a valid Avro file
    """
    try:
        # Handle file path or file object
        if isinstance(file_input, str):
            file_obj = open(file_input, 'rb')
            should_close = True
        else:
            file_obj = file_input
            should_close = False
        
        try:
            # Read the schema from the Avro file
            reader = DataFileReader(file_obj, DatumReader())
            schema_str = reader.meta.get('avro.schema', b'{}').decode('utf-8')
            schema = json.loads(schema_str)
            reader.close()
            return schema
        finally:
            if should_close:
                file_obj.close()
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Error extracting schema from Avro file: {e}")
        raise IOError(f"Error extracting schema from Avro file: {e}")


def merge_avro_schemas(
    schemas: List[Union[dict, str, avro.schema.Schema]], 
    namespace: str = "com.example", 
    name: str = "MergedRecord"
) -> dict:
    """
    Merge multiple Avro schemas into a single schema.
    
    Args:
        schemas: List of schemas to merge
        namespace: Namespace for the merged schema
        name: Name for the merged schema
        
    Returns:
        Merged Avro schema as a dictionary
        
    Raises:
        SchemaError: If any schema is invalid or schemas cannot be merged
    """
    # Parse schemas if needed
    parsed_schemas = []
    for schema in schemas:
        if not isinstance(schema, avro.schema.Schema):
            parsed_schema = parse_avro_schema(schema)
        else:
            parsed_schema = schema
        
        # Ensure all schemas are record type
        if parsed_schema.type != "record":
            raise SchemaError(
                message=f"Cannot merge schema of type {parsed_schema.type}, only record schemas can be merged",
                data_source="avro_schema",
                schema_details={"schema": avro_schema_to_json(parsed_schema)}
            )
        
        parsed_schemas.append(parsed_schema)
    
    # Initialize merged schema
    merged_schema = {
        "type": "record",
        "namespace": namespace,
        "name": name,
        "fields": []
    }
    
    # Track fields by name
    field_map = {}
    
    # Process each schema
    for schema in parsed_schemas:
        schema_json = avro_schema_to_json(schema)
        
        for field in schema_json.get("fields", []):
            field_name = field["name"]
            
            if field_name not in field_map:
                # New field, add it
                field_map[field_name] = field
            else:
                # Field already exists, check compatibility
                existing_field = field_map[field_name]
                
                # Attempt to create a compatible type
                if existing_field["type"] == field["type"]:
                    # Same type, no changes needed
                    pass
                elif isinstance(existing_field["type"], list) and isinstance(field["type"], list):
                    # Both are unions, merge them
                    merged_types = list(set(existing_field["type"] + field["type"]))
                    field_map[field_name]["type"] = merged_types
                elif isinstance(existing_field["type"], list):
                    # Existing is union, add new type if not already included
                    if field["type"] not in existing_field["type"]:
                        field_map[field_name]["type"].append(field["type"])
                elif isinstance(field["type"], list):
                    # New is union, use it and ensure existing type is included
                    if existing_field["type"] not in field["type"]:
                        field_map[field_name]["type"] = field["type"] + [existing_field["type"]]
                    else:
                        field_map[field_name]["type"] = field["type"]
                else:
                    # Different non-union types, create a union
                    field_map[field_name]["type"] = [existing_field["type"], field["type"]]
    
    # Build merged schema fields
    merged_schema["fields"] = list(field_map.values())
    
    return merged_schema


def avro_schema_to_json(schema: avro.schema.Schema) -> dict:
    """
    Convert an Avro schema object to a JSON-serializable dictionary.
    
    Args:
        schema: Avro schema object
        
    Returns:
        Schema as a JSON-serializable dictionary
    """
    if not isinstance(schema, avro.schema.Schema):
        raise ValueError("Input must be an avro.schema.Schema object")
    
    # Convert to canonical form
    schema_json = json.loads(str(schema))
    return schema_json


def map_pandas_dtype_to_avro(dtype: pd.Series.dtype) -> dict:
    """
    Map pandas data types to corresponding Avro types.
    
    Args:
        dtype: pandas dtype
        
    Returns:
        Corresponding Avro type definition
    """
    dtype_str = str(dtype)
    
    # Map pandas dtypes to Avro types
    if pd.api.types.is_integer_dtype(dtype):
        return "long"
    elif pd.api.types.is_float_dtype(dtype):
        return "double"
    elif pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    elif pd.api.types.is_datetime64_dtype(dtype):
        return {"type": "long", "logicalType": "timestamp-micros"}
    elif pd.api.types.is_timedelta64_dtype(dtype):
        return "long"  # milliseconds
    elif pd.api.types.is_categorical_dtype(dtype):
        return "string"
    elif pd.api.types.is_object_dtype(dtype):
        return "string"
    elif pd.api.types.is_string_dtype(dtype):
        return "string"
    else:
        # Default to string for unknown types
        logger.warning(f"Unknown pandas dtype {dtype}, using string")
        return "string"


def read_avro_file_to_dataframe(file_input: Union[str, io.IOBase]) -> pd.DataFrame:
    """
    Read an Avro file into a pandas DataFrame.
    
    Args:
        file_input: Path to an Avro file or a file-like object
        
    Returns:
        DataFrame containing the Avro file data
        
    Raises:
        IOError: If the file cannot be read or is not a valid Avro file
    """
    try:
        # Handle file path or file object
        if isinstance(file_input, str):
            file_obj = open(file_input, 'rb')
            should_close = True
        else:
            file_obj = file_input
            should_close = False
        
        try:
            # Read records from the Avro file
            reader = DataFileReader(file_obj, DatumReader())
            records = list(reader)
            reader.close()
            
            # Convert to DataFrame
            return pd.DataFrame(records)
        finally:
            if should_close:
                file_obj.close()
    except (IOError, Exception) as e:
        logger.error(f"Error reading Avro file: {e}")
        raise IOError(f"Error reading Avro file: {e}")


def write_dataframe_to_avro(
    df: pd.DataFrame,
    file_output: Union[str, io.IOBase],
    schema: Union[dict, str, avro.schema.Schema] = None
) -> bool:
    """
    Write a pandas DataFrame to an Avro file.
    
    Args:
        df: DataFrame to write
        file_output: Path or file-like object to write to
        schema: Avro schema to use (if None, will be inferred from DataFrame)
        
    Returns:
        True if successful, False otherwise
        
    Raises:
        SchemaError: If schema is invalid or incompatible with DataFrame
        IOError: If the file cannot be written
    """
    try:
        # Infer schema if not provided
        if schema is None:
            logger.info("No schema provided, inferring from DataFrame")
            schema = infer_avro_schema(df)
        
        # Parse the schema if needed
        if not isinstance(schema, avro.schema.Schema):
            schema = parse_avro_schema(schema)
        
        # Convert DataFrame to list of records
        records = df.to_dict(orient='records')
        
        # Handle file path or file object
        if isinstance(file_output, str):
            file_obj = open(file_output, 'wb')
            should_close = True
        else:
            file_obj = file_output
            should_close = False
        
        try:
            # Create writer and write records
            writer = DataFileWriter(file_obj, DatumWriter(), schema)
            for record in records:
                writer.append(record)
            writer.close()
            return True
        finally:
            if should_close:
                file_obj.close()
    except (IOError, Exception) as e:
        logger.error(f"Error writing DataFrame to Avro file: {e}")
        raise IOError(f"Error writing DataFrame to Avro file: {e}")


class AvroSchemaValidator:
    """
    Class for validating data against Avro schemas with advanced features.
    """
    
    def __init__(self, schema: Union[dict, str, avro.schema.Schema], options: dict = None):
        """
        Initialize the Avro schema validator with a schema.
        
        Args:
            schema: Avro schema to validate against
            options: Additional validation options
            
        Raises:
            SchemaError: If the schema is invalid
        """
        # Parse the schema if needed
        if not isinstance(schema, avro.schema.Schema):
            self._schema = parse_avro_schema(schema)
        else:
            self._schema = schema
        
        self._options = options or {}
    
    def validate(self, data: Union[dict, list], raise_exception: bool = False) -> Union[bool, dict]:
        """
        Validate data against the schema.
        
        Args:
            data: Data to validate
            raise_exception: Whether to raise an exception on validation failure
            
        Returns:
            True if validation passes, or a dictionary with validation errors
            
        Raises:
            SchemaError: If raise_exception is True and validation fails
        """
        errors = []
        
        try:
            # Convert the schema to a string and parse it
            # This is a workaround because Avro's validate() functionality is limited
            schema_json = str(self._schema)
            parsed_schema = avro.schema.parse(schema_json)
            
            # Create a writer schema for validation
            writer = avro.io.DatumWriter(parsed_schema)
            
            # Validate data by attempting to write it
            # If it fails, it's not valid
            buffer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(buffer)
            
            try:
                writer.write(data, encoder)
                return True
            except Exception as e:
                errors.append(str(e))
                if raise_exception:
                    raise SchemaError(
                        message=f"Data does not conform to Avro schema: {e}",
                        data_source="avro_validation",
                        schema_details={
                            "schema": avro_schema_to_json(self._schema),
                            "errors": errors
                        }
                    )
                return {
                    "valid": False,
                    "errors": errors
                }
        except Exception as e:
            errors.append(str(e))
            if raise_exception:
                raise SchemaError(
                    message=f"Error during Avro schema validation: {e}",
                    data_source="avro_validation",
                    schema_details={
                        "schema": avro_schema_to_json(self._schema),
                        "errors": errors
                    }
                )
            return {
                "valid": False,
                "errors": errors
            }
    
    def validate_batch(self, data_items: list, fail_fast: bool = False) -> dict:
        """
        Validate a batch of data items against the schema.
        
        Args:
            data_items: List of data items to validate
            fail_fast: Whether to stop at the first failure
            
        Returns:
            Validation results with counts and errors
        """
        results = {
            "total": len(data_items),
            "valid": 0,
            "invalid": 0,
            "errors": []
        }
        
        for i, item in enumerate(data_items):
            validation_result = self.validate(item)
            
            if validation_result is True:
                results["valid"] += 1
            else:
                results["invalid"] += 1
                error_info = {
                    "index": i,
                    "errors": validation_result.get("errors", ["Unknown error"])
                }
                results["errors"].append(error_info)
                
                if fail_fast:
                    break
        
        return results
    
    def get_schema(self) -> dict:
        """
        Get the current schema.
        
        Returns:
            Current Avro schema as a dictionary
        """
        return avro_schema_to_json(self._schema)
    
    def update_schema(self, schema: Union[dict, str, avro.schema.Schema]) -> None:
        """
        Update the validator with a new schema.
        
        Args:
            schema: New schema to use for validation
            
        Raises:
            SchemaError: If the schema is invalid
        """
        # Parse the schema if needed
        if not isinstance(schema, avro.schema.Schema):
            self._schema = parse_avro_schema(schema)
        else:
            self._schema = schema
    
    def format_error(self, errors: list) -> dict:
        """
        Format validation errors into a readable structure.
        
        Args:
            errors: List of error messages
            
        Returns:
            Formatted error information
        """
        formatted = {
            "count": len(errors),
            "messages": errors,
            "details": []
        }
        
        for error in errors:
            # Try to extract useful information from error message
            error_parts = str(error).split(":")
            if len(error_parts) > 1:
                detail = {
                    "message": error_parts[0].strip(),
                    "context": ":".join(error_parts[1:]).strip()
                }
                
                # Try to extract field information
                import re
                field_match = re.search(r"field\s+(\w+)", str(error))
                if field_match:
                    detail["field"] = field_match.group(1)
                
                # Try to extract type information
                type_match = re.search(r"expected\s+(\w+)", str(error))
                if type_match:
                    detail["expected_type"] = type_match.group(1)
                
                formatted["details"].append(detail)
            else:
                formatted["details"].append({"message": str(error)})
        
        return formatted