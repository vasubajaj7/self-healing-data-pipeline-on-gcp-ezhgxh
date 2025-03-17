"""
Provides utilities for generating schema definitions for test data in the self-healing data pipeline.

This module enables the creation of realistic schema structures with configurable
characteristics for various data sources and formats, supporting both simple and
complex nested schemas.
"""

import os
import json
import copy
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Tuple

from src.backend.constants import FileFormat, DataSourceType, QualityDimension
from src.test.utils.test_helpers import create_temp_file, load_test_data

# Directory for sample schemas
SAMPLE_SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'bigquery')

# Default field configurations
DEFAULT_FIELD_TYPES = ["string", "integer", "float", "boolean", "date", "timestamp", "array", "record"]
DEFAULT_FIELD_MODES = ["REQUIRED", "NULLABLE", "REPEATED"]
DEFAULT_FIELD_NAME_PREFIX = "field_"
DEFAULT_MAX_NESTED_DEPTH = 3
DEFAULT_MAX_NESTED_FIELDS = 5

def load_sample_schema(schema_name: str) -> Dict:
    """
    Loads a sample schema from the predefined schema files.
    
    Args:
        schema_name: Name of the schema file (without extension)
        
    Returns:
        Loaded schema definition
    """
    schema_path = os.path.join(SAMPLE_SCHEMAS_DIR, f"{schema_name}.json")
    
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, 'r') as schema_file:
        schema = json.load(schema_file)
    
    return schema

def save_schema(schema: Dict, file_path: str) -> str:
    """
    Saves a schema definition to a file.
    
    Args:
        schema: Schema definition to save
        file_path: Path where to save the schema
        
    Returns:
        Path to the saved schema file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    
    with open(file_path, 'w') as schema_file:
        json.dump(schema, schema_file, indent=2)
    
    return file_path

def generate_field_name(index: int, prefix: str = DEFAULT_FIELD_NAME_PREFIX) -> str:
    """
    Generates a field name for a schema field.
    
    Args:
        index: Index of the field
        prefix: Prefix for the field name
        
    Returns:
        Generated field name
    """
    return f"{prefix}{index}"

def generate_field_description(field_name: str, field_type: str) -> str:
    """
    Generates a description for a schema field.
    
    Args:
        field_name: Name of the field
        field_type: Type of the field
        
    Returns:
        Generated field description
    """
    return f"Field {field_name} of type {field_type}"

def generate_simple_field(field_name: str, field_type: str, mode: str, options: Dict = None) -> Dict:
    """
    Generates a simple (non-nested) schema field.
    
    Args:
        field_name: Name of the field
        field_type: Type of the field
        mode: Mode of the field (REQUIRED, NULLABLE, REPEATED)
        options: Additional options for the field
        
    Returns:
        Generated field definition
    """
    if options is None:
        options = {}
    
    field = {
        "name": field_name,
        "type": field_type,
        "mode": mode
    }
    
    # Add description if provided or generate one
    if "description" in options:
        field["description"] = options["description"]
    else:
        field["description"] = generate_field_description(field_name, field_type)
    
    # Add additional field-specific options
    if field_type in ["integer", "float"] and "precision" in options:
        field["precision"] = options["precision"]
    
    if field_type == "string" and "max_length" in options:
        field["max_length"] = options["max_length"]
    
    return field

def generate_array_field(field_name: str, options: Dict = None) -> Dict:
    """
    Generates an array field with item type specification.
    
    Args:
        field_name: Name of the field
        options: Options for the array field including item type
        
    Returns:
        Generated array field definition
    """
    if options is None:
        options = {}
    
    item_type = options.get("item_type", random.choice(DEFAULT_FIELD_TYPES))
    
    # Don't nest arrays within arrays to avoid complexity
    if item_type == "array":
        item_type = "string"
    
    field = {
        "name": field_name,
        "type": "ARRAY",
        "mode": "REPEATED",
        "item_type": item_type
    }
    
    # Add description if provided or generate one
    if "description" in options:
        field["description"] = options["description"]
    else:
        field["description"] = f"Array of {item_type} values"
    
    return field

def generate_record_field(field_name: str, options: Dict = None, current_depth: int = 0) -> Dict:
    """
    Generates a record (nested) field with subfields.
    
    Args:
        field_name: Name of the field
        options: Options for the record field
        current_depth: Current nesting depth
        
    Returns:
        Generated record field definition
    """
    if options is None:
        options = {}
    
    mode = options.get("mode", "NULLABLE")
    num_fields = options.get("num_fields", random.randint(1, DEFAULT_MAX_NESTED_FIELDS))
    max_depth = options.get("max_depth", DEFAULT_MAX_NESTED_DEPTH)
    
    field = {
        "name": field_name,
        "type": "RECORD",
        "mode": mode,
        "fields": []
    }
    
    # Add description if provided or generate one
    if "description" in options:
        field["description"] = options["description"]
    else:
        field["description"] = f"Record containing {num_fields} fields"
    
    # Check if we've reached maximum depth
    if current_depth >= max_depth:
        # Only add simple fields at max depth
        for i in range(num_fields):
            subfield_name = generate_field_name(i, f"{field_name}_")
            subfield_type = random.choice([t for t in DEFAULT_FIELD_TYPES if t not in ["array", "record"]])
            subfield_mode = random.choice(DEFAULT_FIELD_MODES)
            subfield = generate_simple_field(subfield_name, subfield_type, subfield_mode)
            field["fields"].append(subfield)
    else:
        # Add a mix of simple and nested fields
        for i in range(num_fields):
            subfield_name = generate_field_name(i, f"{field_name}_")
            field_type_choice = random.random()
            
            if field_type_choice < 0.7:  # 70% simple fields
                subfield_type = random.choice([t for t in DEFAULT_FIELD_TYPES if t not in ["array", "record"]])
                subfield_mode = random.choice(DEFAULT_FIELD_MODES)
                subfield = generate_simple_field(subfield_name, subfield_type, subfield_mode)
            elif field_type_choice < 0.85:  # 15% array fields
                subfield = generate_array_field(subfield_name)
            else:  # 15% record fields
                subfield_options = {
                    "mode": random.choice(DEFAULT_FIELD_MODES),
                    "num_fields": random.randint(1, DEFAULT_MAX_NESTED_FIELDS // 2),
                    "max_depth": max_depth
                }
                subfield = generate_record_field(subfield_name, subfield_options, current_depth + 1)
            
            field["fields"].append(subfield)
    
    return field

def convert_to_bigquery_schema(schema: Dict) -> List[Dict]:
    """
    Converts a generic schema to BigQuery schema format.
    
    Args:
        schema: Generic schema definition
        
    Returns:
        BigQuery schema as a list of field definitions
    """
    bq_fields = []
    
    for field in schema.get("fields", []):
        bq_field = {
            "name": field.get("name"),
            "type": field.get("type"),
            "mode": field.get("mode", "NULLABLE"),
            "description": field.get("description", "")
        }
        
        # Handle nested fields (RECORD type)
        if field.get("type") == "RECORD" and "fields" in field:
            bq_field["fields"] = convert_to_bigquery_schema({"fields": field["fields"]})
        
        bq_fields.append(bq_field)
    
    return bq_fields

def convert_to_avro_schema(schema: Dict, namespace: str = "com.example", name: str = "record") -> Dict:
    """
    Converts a generic schema to Avro schema format.
    
    Args:
        schema: Generic schema definition
        namespace: Avro schema namespace
        name: Avro record name
        
    Returns:
        Avro schema definition
    """
    avro_schema = {
        "type": "record",
        "namespace": namespace,
        "name": name,
        "fields": []
    }
    
    def map_type(field_type, field_mode):
        """Maps field type and mode to Avro type."""
        if field_mode == "NULLABLE":
            return ["null", get_avro_type(field_type)]
        return get_avro_type(field_type)
    
    def get_avro_type(field_type):
        """Gets the Avro type for a field type."""
        type_mapping = {
            "string": "string",
            "integer": "long",
            "float": "double",
            "boolean": "boolean",
            "date": {"type": "int", "logicalType": "date"},
            "timestamp": {"type": "long", "logicalType": "timestamp-micros"}
        }
        
        if field_type in type_mapping:
            return type_mapping[field_type]
        elif field_type == "ARRAY":
            return {"type": "array", "items": "string"}  # Default to string items
        elif field_type == "RECORD":
            return "record"  # Will be processed separately
        
        return "string"  # Default to string for unknown types
    
    for field in schema.get("fields", []):
        avro_field = {
            "name": field.get("name"),
            "doc": field.get("description", "")
        }
        
        field_type = field.get("type")
        field_mode = field.get("mode", "NULLABLE")
        
        # Handle RECORD type
        if field_type == "RECORD" and "fields" in field:
            record_schema = convert_to_avro_schema(
                {"fields": field["fields"]},
                namespace,
                field.get("name", "record")
            )
            
            if field_mode == "NULLABLE":
                avro_field["type"] = ["null", record_schema]
            else:
                avro_field["type"] = record_schema
        
        # Handle ARRAY type
        elif field_type == "ARRAY" and "item_type" in field:
            item_type = get_avro_type(field.get("item_type"))
            avro_field["type"] = {
                "type": "array",
                "items": item_type
            }
        
        # Handle other types
        else:
            avro_field["type"] = map_type(field_type, field_mode)
        
        avro_schema["fields"].append(avro_field)
    
    return avro_schema

def convert_to_jsonschema(schema: Dict, title: str = "Schema") -> Dict:
    """
    Converts a generic schema to JSON Schema format.
    
    Args:
        schema: Generic schema definition
        title: JSON Schema title
        
    Returns:
        JSON Schema definition
    """
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": title,
        "type": "object",
        "properties": {},
        "required": []
    }
    
    def map_type(field_type):
        """Maps field type to JSON Schema type."""
        type_mapping = {
            "string": "string",
            "integer": "integer",
            "float": "number",
            "boolean": "boolean",
            "date": "string",
            "timestamp": "string"
        }
        
        if field_type in type_mapping:
            return type_mapping[field_type]
        elif field_type == "ARRAY":
            return "array"
        elif field_type == "RECORD":
            return "object"
        
        return "string"  # Default to string for unknown types
    
    for field in schema.get("fields", []):
        field_name = field.get("name")
        field_type = field.get("type")
        field_mode = field.get("mode", "NULLABLE")
        
        # Create property for this field
        property_schema = {
            "type": map_type(field_type),
            "description": field.get("description", "")
        }
        
        # Add field-specific attributes
        if field_type == "string" and "max_length" in field:
            property_schema["maxLength"] = field["max_length"]
        
        if field_type in ["integer", "float"] and "precision" in field:
            property_schema["format"] = f"precision-{field['precision']}"
        
        # Handle date and timestamp formats
        if field_type == "date":
            property_schema["format"] = "date"
        elif field_type == "timestamp":
            property_schema["format"] = "date-time"
        
        # Handle RECORD type (nested object)
        if field_type == "RECORD" and "fields" in field:
            nested_schema = convert_to_jsonschema(
                {"fields": field["fields"]},
                title=field_name
            )
            property_schema["properties"] = nested_schema["properties"]
            property_schema["required"] = nested_schema["required"]
        
        # Handle ARRAY type
        elif field_type == "ARRAY" and "item_type" in field:
            property_schema["items"] = {
                "type": map_type(field.get("item_type"))
            }
        
        # Add property to schema
        json_schema["properties"][field_name] = property_schema
        
        # Add to required list if field is required
        if field_mode == "REQUIRED":
            json_schema["required"].append(field_name)
    
    return json_schema

def add_constraints_to_schema(schema: Dict, constraints: Dict) -> Dict:
    """
    Adds data quality constraints to a schema definition.
    
    Args:
        schema: Schema definition
        constraints: Dictionary of field constraints
        
    Returns:
        Schema with added constraints
    """
    # Create a deep copy to avoid modifying the original
    updated_schema = copy.deepcopy(schema)
    
    def add_field_constraints(fields, field_constraints):
        """Recursively adds constraints to fields."""
        for field in fields:
            field_name = field.get("name")
            
            if field_name in field_constraints:
                # Add constraints to this field
                constraints_to_add = field_constraints[field_name]
                for constraint_key, constraint_value in constraints_to_add.items():
                    field[constraint_key] = constraint_value
            
            # Process nested fields
            if field.get("type") == "RECORD" and "fields" in field:
                nested_constraints = field_constraints.get(field_name, {})
                add_field_constraints(field["fields"], nested_constraints)
    
    add_field_constraints(updated_schema.get("fields", []), constraints)
    
    return updated_schema

def add_relationships_to_schema(schema: Dict, relationships: List[Dict]) -> Dict:
    """
    Adds relationship definitions to a schema.
    
    Args:
        schema: Schema definition
        relationships: List of relationship definitions
        
    Returns:
        Schema with added relationships
    """
    # Create a deep copy to avoid modifying the original
    updated_schema = copy.deepcopy(schema)
    
    # Add relationships section if not present
    if "relationships" not in updated_schema:
        updated_schema["relationships"] = []
    
    # Helper function to check if fields exist in the schema
    def field_exists(field_path):
        """Check if a field exists in the schema."""
        path_parts = field_path.split('.')
        current_fields = updated_schema.get("fields", [])
        
        for part in path_parts:
            found = False
            for field in current_fields:
                if field.get("name") == part:
                    found = True
                    if field.get("type") == "RECORD" and "fields" in field:
                        current_fields = field["fields"]
                    break
            
            if not found:
                return False
        
        return True
    
    # Validate and add relationships
    for relationship in relationships:
        source_field = relationship.get("source_field")
        target_field = relationship.get("target_field")
        
        # Validate fields exist
        if not source_field or not target_field:
            raise ValueError("Relationship must specify source and target fields")
        
        if not field_exists(source_field):
            raise ValueError(f"Source field {source_field} not found in schema")
        
        # Add relationship to schema
        updated_schema["relationships"].append(relationship)
    
    return updated_schema


class SchemaGenerator:
    """Main class for generating schema definitions with various characteristics."""
    
    def __init__(self):
        """Initialize the SchemaGenerator with default generators and converters."""
        # Initialize field generators
        self._field_generators = {}
        
        # Initialize format converters
        self._format_converters = {}
        
        # Register built-in field generators
        self.register_field_generator("string", lambda name, options, depth: 
                                     generate_simple_field(name, "string", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("integer", lambda name, options, depth: 
                                     generate_simple_field(name, "integer", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("float", lambda name, options, depth: 
                                     generate_simple_field(name, "float", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("boolean", lambda name, options, depth: 
                                     generate_simple_field(name, "boolean", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("date", lambda name, options, depth: 
                                     generate_simple_field(name, "date", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("timestamp", lambda name, options, depth: 
                                     generate_simple_field(name, "timestamp", options.get("mode", "NULLABLE"), options))
        self.register_field_generator("array", lambda name, options, depth: 
                                     generate_array_field(name, options))
        self.register_field_generator("record", lambda name, options, depth: 
                                     generate_record_field(name, options, depth))
        
        # Register built-in format converters
        self.register_format_converter("bigquery", convert_to_bigquery_schema)
        self.register_format_converter("avro", convert_to_avro_schema)
        self.register_format_converter("jsonschema", convert_to_jsonschema)
    
    def register_field_generator(self, field_type: str, generator_func: Callable) -> None:
        """
        Registers a custom field generator function.
        
        Args:
            field_type: Type of field to generate
            generator_func: Function that generates a field definition
        """
        self._field_generators[field_type] = generator_func
    
    def register_format_converter(self, format_name: str, converter_func: Callable) -> None:
        """
        Registers a custom schema format converter.
        
        Args:
            format_name: Name of the format
            converter_func: Function that converts a schema to the specified format
        """
        self._format_converters[format_name] = converter_func
    
    def generate_field(self, field_name: str, field_type: str, options: Dict = None, 
                      current_depth: int = 0) -> Dict:
        """
        Generates a schema field based on type and options.
        
        Args:
            field_name: Name of the field
            field_type: Type of the field
            options: Additional options for the field
            current_depth: Current nesting depth for records
            
        Returns:
            Generated field definition
        """
        if options is None:
            options = {}
        
        # Default to string for unknown types
        if field_type not in self._field_generators:
            field_type = "string"
        
        # Generate field using registered generator
        return self._field_generators[field_type](field_name, options, current_depth)
    
    def generate_schema(self, num_fields: int, options: Dict = None) -> Dict:
        """
        Generates a complete schema with specified characteristics.
        
        Args:
            num_fields: Number of fields to generate
            options: Schema generation options
            
        Returns:
            Generated schema definition
        """
        if options is None:
            options = {}
        
        schema = {"fields": []}
        
        # Extract schema options
        field_types_dist = options.get("field_types_distribution", {})
        naming_convention = options.get("naming_convention", DEFAULT_FIELD_NAME_PREFIX)
        
        # Generate fields
        for i in range(num_fields):
            # Determine field type based on distribution or random selection
            if field_types_dist:
                field_type = random.choices(
                    list(field_types_dist.keys()),
                    weights=list(field_types_dist.values()),
                    k=1
                )[0]
            else:
                field_type = random.choice(DEFAULT_FIELD_TYPES)
            
            # Generate field name
            field_name = generate_field_name(i, naming_convention)
            
            # Generate field options
            field_options = {
                "mode": random.choice(DEFAULT_FIELD_MODES)
            }
            
            # Add type-specific options
            if field_type == "record":
                field_options["max_depth"] = options.get("max_nested_depth", DEFAULT_MAX_NESTED_DEPTH)
                field_options["num_fields"] = random.randint(1, options.get("max_nested_fields", DEFAULT_MAX_NESTED_FIELDS))
            
            # Generate field and add to schema
            field = self.generate_field(field_name, field_type, field_options)
            schema["fields"].append(field)
        
        # Add relationships if specified
        if "relationships" in options:
            schema = add_relationships_to_schema(schema, options["relationships"])
        
        # Add constraints if specified
        if "constraints" in options:
            schema = add_constraints_to_schema(schema, options["constraints"])
        
        return schema
    
    def convert_schema_format(self, schema: Dict, target_format: str, options: Dict = None) -> Union[Dict, List]:
        """
        Converts a schema to a different format.
        
        Args:
            schema: Schema to convert
            target_format: Target format
            options: Conversion options
            
        Returns:
            Converted schema in target format
        """
        if options is None:
            options = {}
        
        # Default to returning the schema as-is if format not supported
        if target_format not in self._format_converters:
            return schema
        
        # Convert using registered converter
        return self._format_converters[target_format](schema, **options)
    
    def generate_bigquery_schema(self, num_fields: int, options: Dict = None) -> List[Dict]:
        """
        Generates a BigQuery-specific schema.
        
        Args:
            num_fields: Number of fields to generate
            options: Schema generation options
            
        Returns:
            BigQuery schema as a list of field definitions
        """
        # Generate generic schema
        schema = self.generate_schema(num_fields, options)
        
        # Convert to BigQuery format
        return self.convert_schema_format(schema, "bigquery")
    
    def generate_avro_schema(self, num_fields: int, options: Dict = None) -> Dict:
        """
        Generates an Avro-specific schema.
        
        Args:
            num_fields: Number of fields to generate
            options: Schema generation options
            
        Returns:
            Avro schema definition
        """
        if options is None:
            options = {}
        
        # Generate generic schema
        schema = self.generate_schema(num_fields, options)
        
        # Extract Avro-specific options
        avro_options = {
            "namespace": options.get("namespace", "com.example"),
            "name": options.get("name", "record")
        }
        
        # Convert to Avro format
        return self.convert_schema_format(schema, "avro", avro_options)
    
    def generate_jsonschema(self, num_fields: int, options: Dict = None) -> Dict:
        """
        Generates a JSON Schema definition.
        
        Args:
            num_fields: Number of fields to generate
            options: Schema generation options
            
        Returns:
            JSON Schema definition
        """
        if options is None:
            options = {}
        
        # Generate generic schema
        schema = self.generate_schema(num_fields, options)
        
        # Extract JSON Schema-specific options
        jsonschema_options = {
            "title": options.get("title", "Schema")
        }
        
        # Convert to JSON Schema format
        return self.convert_schema_format(schema, "jsonschema", jsonschema_options)
    
    def load_schema(self, file_path: str, format_type: str = None) -> Dict:
        """
        Loads a schema from a file.
        
        Args:
            file_path: Path to the schema file
            format_type: Format of the schema file
            
        Returns:
            Loaded schema definition
        """
        with open(file_path, 'r') as schema_file:
            schema = json.load(schema_file)
        
        return schema
    
    def save_schema(self, schema: Dict, file_path: str, format_type: str = None) -> str:
        """
        Saves a schema to a file.
        
        Args:
            schema: Schema to save
            file_path: File path to save to
            format_type: Format to convert schema to before saving
            
        Returns:
            Path to the saved schema file
        """
        # Convert schema if format specified
        if format_type and format_type in self._format_converters:
            schema = self.convert_schema_format(schema, format_type)
        
        # Save schema to file
        return save_schema(schema, file_path)
    
    def add_constraints(self, schema: Dict, constraints: Dict) -> Dict:
        """
        Adds data quality constraints to a schema.
        
        Args:
            schema: Schema definition
            constraints: Dictionary of field constraints
            
        Returns:
            Schema with added constraints
        """
        return add_constraints_to_schema(schema, constraints)
    
    def add_relationships(self, schema: Dict, relationships: List[Dict]) -> Dict:
        """
        Adds relationship definitions to a schema.
        
        Args:
            schema: Schema definition
            relationships: List of relationship definitions
            
        Returns:
            Schema with added relationships
        """
        return add_relationships_to_schema(schema, relationships)
    
    def evolve_schema(self, schema: Dict, evolution_config: Dict) -> Dict:
        """
        Evolves a schema by adding, modifying, or removing fields.
        
        Args:
            schema: Original schema
            evolution_config: Configuration for schema evolution
            
        Returns:
            Evolved schema
        """
        # Create a deep copy to avoid modifying the original
        evolved_schema = copy.deepcopy(schema)
        
        # Process add_fields
        if "add_fields" in evolution_config:
            for field_spec in evolution_config["add_fields"]:
                field_name = field_spec.get("name")
                field_type = field_spec.get("type", "string")
                field_options = field_spec.get("options", {})
                
                # Generate new field
                new_field = self.generate_field(field_name, field_type, field_options)
                
                # Add to schema fields
                evolved_schema["fields"].append(new_field)
        
        # Process modify_fields
        if "modify_fields" in evolution_config:
            for field_mod in evolution_config["modify_fields"]:
                field_name = field_mod.get("name")
                modifications = field_mod.get("modifications", {})
                
                # Find and modify the field
                for field in evolved_schema["fields"]:
                    if field.get("name") == field_name:
                        for key, value in modifications.items():
                            field[key] = value
                        break
        
        # Process remove_fields
        if "remove_fields" in evolution_config:
            field_names_to_remove = evolution_config["remove_fields"]
            evolved_schema["fields"] = [
                f for f in evolved_schema["fields"] 
                if f.get("name") not in field_names_to_remove
            ]
        
        # Process rename_fields
        if "rename_fields" in evolution_config:
            for rename_op in evolution_config["rename_fields"]:
                old_name = rename_op.get("old_name")
                new_name = rename_op.get("new_name")
                
                # Find and rename the field
                for field in evolved_schema["fields"]:
                    if field.get("name") == old_name:
                        field["name"] = new_name
                        break
        
        return evolved_schema
    
    def generate_schema_drift(self, original_schema: Dict, drift_config: Dict) -> Dict:
        """
        Generates a schema drift based on an original schema.
        
        Args:
            original_schema: Original schema
            drift_config: Configuration for schema drift
            
        Returns:
            Schema with drift applied
        """
        # Create a deep copy to avoid modifying the original
        drifted_schema = copy.deepcopy(original_schema)
        
        # Apply type changes
        if "type_changes" in drift_config:
            for type_change in drift_config["type_changes"]:
                field_name = type_change.get("field_name")
                new_type = type_change.get("new_type")
                
                # Find and modify the field
                for field in drifted_schema["fields"]:
                    if field.get("name") == field_name:
                        field["type"] = new_type
                        break
        
        # Apply mode changes
        if "mode_changes" in drift_config:
            for mode_change in drift_config["mode_changes"]:
                field_name = mode_change.get("field_name")
                new_mode = mode_change.get("new_mode")
                
                # Find and modify the field
                for field in drifted_schema["fields"]:
                    if field.get("name") == field_name:
                        field["mode"] = new_mode
                        break
        
        # Apply field additions
        if "field_additions" in drift_config:
            for field_spec in drift_config["field_additions"]:
                field_name = field_spec.get("name")
                field_type = field_spec.get("type", "string")
                field_options = field_spec.get("options", {})
                
                # Generate new field
                new_field = self.generate_field(field_name, field_type, field_options)
                
                # Add to schema fields
                drifted_schema["fields"].append(new_field)
        
        # Apply field removals
        if "field_removals" in drift_config:
            field_names_to_remove = drift_config["field_removals"]
            drifted_schema["fields"] = [
                f for f in drifted_schema["fields"] 
                if f.get("name") not in field_names_to_remove
            ]
        
        return drifted_schema
    
    def generate_schema_variations(self, base_schema: Dict, num_variations: int, 
                                 variation_config: Dict = None) -> List[Dict]:
        """
        Generates multiple variations of a schema.
        
        Args:
            base_schema: Base schema to create variations from
            num_variations: Number of variations to generate
            variation_config: Configuration for variations
            
        Returns:
            List of schema variations
        """
        if variation_config is None:
            variation_config = {}
        
        variations = []
        
        for i in range(num_variations):
            # Determine variation method
            method = variation_config.get("method", "drift")
            
            if method == "evolve":
                # Create random evolution configuration
                evolution_config = {
                    "add_fields": [{
                        "name": f"new_field_{i}_{j}",
                        "type": random.choice(DEFAULT_FIELD_TYPES),
                        "options": {"mode": random.choice(DEFAULT_FIELD_MODES)}
                    } for j in range(random.randint(0, 2))],
                    
                    "modify_fields": [{
                        "name": random.choice([f["name"] for f in base_schema.get("fields", [])]),
                        "modifications": {"description": f"Modified in variation {i}"}
                    } for _ in range(min(1, len(base_schema.get("fields", []))))],
                    
                    "rename_fields": [{
                        "old_name": random.choice([f["name"] for f in base_schema.get("fields", [])]),
                        "new_name": f"renamed_field_{i}"
                    } for _ in range(min(1, len(base_schema.get("fields", []))))]
                }
                
                # Apply evolution
                variation = self.evolve_schema(base_schema, evolution_config)
            
            else:  # Default to drift
                # Create random drift configuration
                drift_config = {
                    "type_changes": [{
                        "field_name": random.choice([f["name"] for f in base_schema.get("fields", [])]),
                        "new_type": random.choice(DEFAULT_FIELD_TYPES)
                    } for _ in range(min(1, len(base_schema.get("fields", []))))],
                    
                    "mode_changes": [{
                        "field_name": random.choice([f["name"] for f in base_schema.get("fields", [])]),
                        "new_mode": random.choice(DEFAULT_FIELD_MODES)
                    } for _ in range(min(1, len(base_schema.get("fields", []))))],
                    
                    "field_additions": [{
                        "name": f"drift_field_{i}_{j}",
                        "type": random.choice(DEFAULT_FIELD_TYPES)
                    } for j in range(random.randint(0, 2))]
                }
                
                # Apply drift
                variation = self.generate_schema_drift(base_schema, drift_config)
            
            # Apply custom variations if specified
            if "custom_variations" in variation_config:
                for custom_var_func in variation_config["custom_variations"]:
                    variation = custom_var_func(variation, i)
            
            variations.append(variation)
        
        return variations


class SchemaCompatibilityChecker:
    """Utility class for checking compatibility between schemas."""
    
    def check_compatibility(self, schema1: Dict, schema2: Dict, 
                           compatibility_mode: str = "backward") -> Tuple[bool, List[str]]:
        """
        Checks compatibility between two schemas.
        
        Args:
            schema1: First schema
            schema2: Second schema
            compatibility_mode: Compatibility mode (backward, forward, full)
            
        Returns:
            Tuple of (compatibility_result, issues_list)
        """
        issues = []
        
        if compatibility_mode not in ["backward", "forward", "full"]:
            raise ValueError("Compatibility mode must be 'backward', 'forward', or 'full'")
        
        if compatibility_mode in ["backward", "full"]:
            # Check if schema2 can read schema1 data
            backward_issues = self._check_backward_compatibility(schema1, schema2)
            issues.extend(backward_issues)
        
        if compatibility_mode in ["forward", "full"]:
            # Check if schema1 can read schema2 data
            forward_issues = self._check_forward_compatibility(schema1, schema2)
            issues.extend(forward_issues)
        
        return len(issues) == 0, issues
    
    def _check_backward_compatibility(self, old_schema: Dict, new_schema: Dict) -> List[str]:
        """Checks if new schema can read old schema data."""
        issues = []
        
        new_fields_map = {f.get("name"): f for f in new_schema.get("fields", [])}
        
        # Check each field in old schema
        for old_field in old_schema.get("fields", []):
            old_field_name = old_field.get("name")
            
            # Field must exist in new schema
            if old_field_name not in new_fields_map:
                issues.append(f"Field '{old_field_name}' exists in old schema but not in new schema")
                continue
            
            new_field = new_fields_map[old_field_name]
            
            # Check type compatibility
            if not self._is_type_compatible(old_field.get("type"), new_field.get("type")):
                issues.append(
                    f"Field '{old_field_name}' type changed from {old_field.get('type')} "
                    f"to {new_field.get('type')} which is incompatible"
                )
            
            # Check mode compatibility
            old_mode = old_field.get("mode", "NULLABLE")
            new_mode = new_field.get("mode", "NULLABLE")
            
            if old_mode == "REQUIRED" and new_mode != "REQUIRED":
                issues.append(
                    f"Field '{old_field_name}' mode changed from REQUIRED to {new_mode} "
                    f"which may cause issues with existing data"
                )
            
            # Check nested fields
            if old_field.get("type") == "RECORD" and new_field.get("type") == "RECORD":
                nested_issues = self._check_backward_compatibility(
                    {"fields": old_field.get("fields", [])},
                    {"fields": new_field.get("fields", [])}
                )
                
                for issue in nested_issues:
                    issues.append(f"In record '{old_field_name}': {issue}")
        
        return issues
    
    def _check_forward_compatibility(self, old_schema: Dict, new_schema: Dict) -> List[str]:
        """Checks if old schema can read new schema data."""
        issues = []
        
        old_fields_map = {f.get("name"): f for f in old_schema.get("fields", [])}
        
        # Check each field in new schema
        for new_field in new_schema.get("fields", []):
            new_field_name = new_field.get("name")
            
            # Field can be added (if nullable or has default)
            if new_field_name not in old_fields_map:
                new_mode = new_field.get("mode", "NULLABLE")
                if new_mode == "REQUIRED" and "default_value" not in new_field:
                    issues.append(
                        f"New field '{new_field_name}' is REQUIRED but has no default value, "
                        f"which breaks forward compatibility"
                    )
                continue
            
            old_field = old_fields_map[new_field_name]
            
            # Check type compatibility
            if not self._is_type_compatible(new_field.get("type"), old_field.get("type")):
                issues.append(
                    f"Field '{new_field_name}' type changed from {old_field.get('type')} "
                    f"to {new_field.get('type')} which is incompatible"
                )
            
            # Check nested fields
            if old_field.get("type") == "RECORD" and new_field.get("type") == "RECORD":
                nested_issues = self._check_forward_compatibility(
                    {"fields": old_field.get("fields", [])},
                    {"fields": new_field.get("fields", [])}
                )
                
                for issue in nested_issues:
                    issues.append(f"In record '{new_field_name}': {issue}")
        
        return issues
    
    def _is_type_compatible(self, type1: str, type2: str) -> bool:
        """Checks if types are compatible."""
        # Same types are always compatible
        if type1 == type2:
            return True
        
        # Define compatibility rules
        compatible_types = {
            "string": ["string"],
            "integer": ["integer", "float"],
            "float": ["float"],
            "boolean": ["boolean"],
            "date": ["date", "string"],
            "timestamp": ["timestamp", "string"],
            "RECORD": ["RECORD"],
            "ARRAY": ["ARRAY"]
        }
        
        return type2 in compatible_types.get(type1, [])
    
    def find_breaking_changes(self, old_schema: Dict, new_schema: Dict) -> List[Dict]:
        """
        Identifies breaking changes between two schemas.
        
        Args:
            old_schema: Old schema
            new_schema: New schema
            
        Returns:
            List of breaking changes
        """
        breaking_changes = []
        
        old_fields_map = {f.get("name"): f for f in old_schema.get("fields", [])}
        new_fields_map = {f.get("name"): f for f in new_schema.get("fields", [])}
        
        # Check for removed fields
        for old_field_name, old_field in old_fields_map.items():
            if old_field_name not in new_fields_map:
                breaking_changes.append({
                    "type": "field_removed",
                    "field_name": old_field_name,
                    "field_type": old_field.get("type"),
                    "severity": "high" if old_field.get("mode") == "REQUIRED" else "medium"
                })
        
        # Check for type and mode changes
        for field_name, old_field in old_fields_map.items():
            if field_name in new_fields_map:
                new_field = new_fields_map[field_name]
                
                # Check type changes
                old_type = old_field.get("type")
                new_type = new_field.get("type")
                
                if old_type != new_type and not self._is_type_compatible(old_type, new_type):
                    breaking_changes.append({
                        "type": "type_change",
                        "field_name": field_name,
                        "old_type": old_type,
                        "new_type": new_type,
                        "severity": "high"
                    })
                
                # Check mode changes
                old_mode = old_field.get("mode", "NULLABLE")
                new_mode = new_field.get("mode", "NULLABLE")
                
                if old_mode != new_mode:
                    if old_mode == "NULLABLE" and new_mode == "REQUIRED":
                        breaking_changes.append({
                            "type": "mode_change",
                            "field_name": field_name,
                            "old_mode": old_mode,
                            "new_mode": new_mode,
                            "severity": "high"
                        })
                    elif old_mode == "REPEATED" and new_mode != "REPEATED":
                        breaking_changes.append({
                            "type": "mode_change",
                            "field_name": field_name,
                            "old_mode": old_mode,
                            "new_mode": new_mode,
                            "severity": "high"
                        })
                
                # Check nested fields
                if old_type == "RECORD" and new_type == "RECORD":
                    nested_breaking_changes = self.find_breaking_changes(
                        {"fields": old_field.get("fields", [])},
                        {"fields": new_field.get("fields", [])}
                    )
                    
                    for change in nested_breaking_changes:
                        change["field_name"] = f"{field_name}.{change['field_name']}"
                        breaking_changes.append(change)
        
        return breaking_changes
    
    def suggest_compatibility_fixes(self, source_schema: Dict, target_schema: Dict, 
                                   compatibility_mode: str = "backward") -> Dict:
        """
        Suggests fixes to make schemas compatible.
        
        Args:
            source_schema: Source schema
            target_schema: Target schema
            compatibility_mode: Compatibility mode
            
        Returns:
            Suggested fixes
        """
        breaking_changes = self.find_breaking_changes(
            source_schema if compatibility_mode == "backward" else target_schema,
            target_schema if compatibility_mode == "backward" else source_schema
        )
        
        fixes = {
            "add_fields": [],
            "modify_fields": [],
            "remove_fields": []
        }
        
        for change in breaking_changes:
            change_type = change.get("type")
            field_name = change.get("field_name")
            
            if change_type == "field_removed":
                # Suggest adding the field back
                source_field = None
                for field in source_schema.get("fields", []):
                    if field.get("name") == field_name:
                        source_field = field
                        break
                
                if source_field:
                    # Make it nullable to handle existing data
                    source_field_copy = copy.deepcopy(source_field)
                    source_field_copy["mode"] = "NULLABLE"
                    fixes["add_fields"].append(source_field_copy)
            
            elif change_type == "type_change":
                # Suggest type modification
                old_type = change.get("old_type")
                new_type = change.get("new_type")
                
                # Suggest a compatible type
                compatible_type = old_type  # Default to original type
                
                # Try to find a more compatible type if possible
                if old_type == "integer" and new_type == "float":
                    compatible_type = "float"  # Integer can be promoted to float
                elif old_type in ["date", "timestamp"] and new_type == "string":
                    compatible_type = "string"  # Dates can be strings
                
                fixes["modify_fields"].append({
                    "field_name": field_name,
                    "type": compatible_type
                })
            
            elif change_type == "mode_change":
                # Suggest mode modification
                old_mode = change.get("old_mode")
                new_mode = change.get("new_mode")
                
                # Generally, make it nullable to handle both cases
                fixes["modify_fields"].append({
                    "field_name": field_name,
                    "mode": "NULLABLE"
                })
        
        return fixes