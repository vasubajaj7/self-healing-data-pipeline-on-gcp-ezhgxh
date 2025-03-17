import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import io
import os
import tempfile
import pandas as pd
import numpy as np
from google.cloud.bigquery import SchemaField

from src.backend.constants import FileFormat
from src.backend.utils.schema.schema_utils import (
    detect_schema_format,
    convert_schema_format,
    infer_schema_from_data,
    validate_schema,
    compare_schemas,
    is_schema_compatible,
    merge_schemas,
    extract_schema_from_file,
    get_schema_fingerprint,
    normalize_schema,
    convert_to_bigquery_schema,
    convert_from_bigquery_schema,
    SchemaRegistry,
    SchemaEvolutionManager
)
from src.backend.utils.schema.jsonschema_utils import (
    parse_json_schema,
    validate_json_schema,
    validate_data_against_schema,
    convert_json_schema_to_bigquery_schema,
    convert_bigquery_to_json_schema,
    JsonSchemaValidator
)
from src.backend.utils.schema.avro_utils import (
    parse_avro_schema,
    validate_avro_schema,
    convert_avro_to_bigquery_schema,
    convert_bigquery_to_avro_schema,
    AvroSchemaValidator
)
from src.backend.utils.errors.error_types import SchemaError, DataError


class TestSchemaFormatDetection(unittest.TestCase):
    """Test cases for schema format detection functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample schemas for different formats
        
        # Create JSON Schema with standard properties
        self.json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create Avro schema with type, namespace, fields
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
        
        # Create BigQuery schema as list of SchemaField objects
        self.bigquery_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("name", "STRING", mode="REQUIRED"),
            SchemaField("email", "STRING", mode="NULLABLE")
        ]
    
    def test_detect_json_schema(self):
        """Test detection of JSON Schema format"""
        schema_format = detect_schema_format(self.json_schema)
        self.assertEqual(schema_format, FileFormat.JSON_SCHEMA)
    
    def test_detect_avro_schema(self):
        """Test detection of Avro schema format"""
        schema_format = detect_schema_format(self.avro_schema)
        self.assertEqual(schema_format, FileFormat.AVRO)
    
    def test_detect_bigquery_schema(self):
        """Test detection of BigQuery schema format"""
        schema_format = detect_schema_format(self.bigquery_schema)
        self.assertEqual(schema_format, FileFormat.BIGQUERY)
    
    def test_detect_from_file(self):
        """Test detection of schema format from file"""
        mock_file = io.StringIO(json.dumps(self.json_schema))
        schema_format = detect_schema_format(mock_file)
        self.assertEqual(schema_format, FileFormat.JSON_SCHEMA)
    
    def test_detect_invalid_schema(self):
        """Test detection with invalid schema"""
        invalid_schema = {"not_a_valid_schema": True}
        schema_format = detect_schema_format(invalid_schema)
        self.assertIsNone(schema_format)


class TestSchemaConversion(unittest.TestCase):
    """Test cases for schema conversion between formats"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample schemas for different formats
        
        # Create JSON Schema with standard properties
        self.json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create Avro schema with type, namespace, fields
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
        
        # Create BigQuery schema as list of SchemaField objects
        self.bigquery_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("name", "STRING", mode="REQUIRED"),
            SchemaField("email", "STRING", mode="NULLABLE")
        ]
    
    def test_convert_json_to_bigquery(self):
        """Test conversion from JSON Schema to BigQuery schema"""
        converted = convert_schema_format(
            self.json_schema, 
            source_format=FileFormat.JSON_SCHEMA, 
            target_format=FileFormat.BIGQUERY
        )
        
        # Verify the converted schema is a list of SchemaField objects
        self.assertIsInstance(converted, list)
        self.assertTrue(all(isinstance(field, SchemaField) for field in converted))
        
        # Verify field names and types
        field_names = [field.name for field in converted]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("email", field_names)
        
        # Check if required fields are marked correctly
        id_field = next(field for field in converted if field.name == "id")
        name_field = next(field for field in converted if field.name == "name")
        email_field = next(field for field in converted if field.name == "email")
        
        self.assertEqual(id_field.mode, "REQUIRED")
        self.assertEqual(name_field.mode, "REQUIRED")
        self.assertEqual(email_field.mode, "NULLABLE")
    
    def test_convert_avro_to_bigquery(self):
        """Test conversion from Avro schema to BigQuery schema"""
        converted = convert_schema_format(
            self.avro_schema, 
            source_format=FileFormat.AVRO, 
            target_format=FileFormat.BIGQUERY
        )
        
        # Verify the converted schema is a list of SchemaField objects
        self.assertIsInstance(converted, list)
        self.assertTrue(all(isinstance(field, SchemaField) for field in converted))
        
        # Verify field names and types
        field_names = [field.name for field in converted]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("email", field_names)
        
        # Check if fields have correct types
        id_field = next(field for field in converted if field.name == "id")
        name_field = next(field for field in converted if field.name == "name")
        email_field = next(field for field in converted if field.name == "email")
        
        self.assertEqual(id_field.field_type, "INTEGER")
        self.assertEqual(name_field.field_type, "STRING")
        self.assertEqual(email_field.field_type, "STRING")
        self.assertEqual(email_field.mode, "NULLABLE")
    
    def test_convert_bigquery_to_json(self):
        """Test conversion from BigQuery schema to JSON Schema"""
        converted = convert_schema_format(
            self.bigquery_schema, 
            source_format=FileFormat.BIGQUERY, 
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the converted schema is a dictionary
        self.assertIsInstance(converted, dict)
        
        # Verify it has the correct structure
        self.assertEqual(converted["type"], "object")
        self.assertIn("properties", converted)
        
        # Verify properties are correctly converted
        self.assertIn("id", converted["properties"])
        self.assertIn("name", converted["properties"])
        self.assertIn("email", converted["properties"])
        
        # Check required fields
        self.assertIn("required", converted)
        self.assertIn("id", converted["required"])
        self.assertIn("name", converted["required"])
        self.assertNotIn("email", converted["required"])
    
    def test_convert_bigquery_to_avro(self):
        """Test conversion from BigQuery schema to Avro schema"""
        converted = convert_schema_format(
            self.bigquery_schema, 
            source_format=FileFormat.BIGQUERY, 
            target_format=FileFormat.AVRO
        )
        
        # Verify the converted schema is a dictionary with Avro structure
        self.assertIsInstance(converted, dict)
        self.assertEqual(converted["type"], "record")
        self.assertIn("fields", converted)
        
        # Verify fields are correctly converted
        field_names = [field["name"] for field in converted["fields"]]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("email", field_names)
        
        # Check field types
        id_field = next(field for field in converted["fields"] if field["name"] == "id")
        name_field = next(field for field in converted["fields"] if field["name"] == "name")
        email_field = next(field for field in converted["fields"] if field["name"] == "email")
        
        self.assertEqual(id_field["type"], "long")
        self.assertEqual(name_field["type"], "string")
        # Email should be nullable
        self.assertIsInstance(email_field["type"], list)
        self.assertIn("null", email_field["type"])
        self.assertIn("string", email_field["type"])
    
    def test_convert_json_to_avro(self):
        """Test conversion from JSON Schema to Avro schema"""
        converted = convert_schema_format(
            self.json_schema, 
            source_format=FileFormat.JSON_SCHEMA, 
            target_format=FileFormat.AVRO
        )
        
        # Verify the converted schema is a dictionary with Avro structure
        self.assertIsInstance(converted, dict)
        self.assertEqual(converted["type"], "record")
        self.assertIn("fields", converted)
        
        # Verify fields are correctly converted
        field_names = [field["name"] for field in converted["fields"]]
        self.assertIn("id", field_names)
        self.assertIn("name", field_names)
        self.assertIn("email", field_names)
        
        # Check field types
        id_field = next(field for field in converted["fields"] if field["name"] == "id")
        name_field = next(field for field in converted["fields"] if field["name"] == "name")
        
        self.assertEqual(id_field["type"], "long")
        self.assertEqual(name_field["type"], "string")
    
    def test_convert_avro_to_json(self):
        """Test conversion from Avro schema to JSON Schema"""
        converted = convert_schema_format(
            self.avro_schema, 
            source_format=FileFormat.AVRO, 
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the converted schema is a dictionary with JSON Schema structure
        self.assertIsInstance(converted, dict)
        self.assertEqual(converted["type"], "object")
        self.assertIn("properties", converted)
        
        # Verify properties are correctly converted
        self.assertIn("id", converted["properties"])
        self.assertIn("name", converted["properties"])
        self.assertIn("email", converted["properties"])
        
        # Check property types
        self.assertEqual(converted["properties"]["id"]["type"], "integer")
        self.assertEqual(converted["properties"]["name"]["type"], "string")
        # Check nullable field
        self.assertIsInstance(converted["properties"]["email"]["type"], list)
        self.assertIn("null", converted["properties"]["email"]["type"])
        self.assertIn("string", converted["properties"]["email"]["type"])
    
    def test_convert_same_format(self):
        """Test conversion to the same format returns original schema"""
        converted = convert_schema_format(
            self.json_schema, 
            source_format=FileFormat.JSON_SCHEMA, 
            target_format=FileFormat.JSON_SCHEMA
        )
        
        self.assertEqual(converted, self.json_schema)
    
    def test_convert_with_auto_detection(self):
        """Test conversion with automatic format detection"""
        converted = convert_schema_format(
            self.json_schema, 
            source_format=None,  # Auto-detect
            target_format=FileFormat.AVRO
        )
        
        # Verify the converted schema is a dictionary with Avro structure
        self.assertIsInstance(converted, dict)
        self.assertEqual(converted["type"], "record")
        self.assertIn("fields", converted)
    
    def test_convert_invalid_schema(self):
        """Test conversion with invalid schema raises error"""
        invalid_schema = {"not_a_valid_schema": True}
        
        with self.assertRaises(SchemaError):
            convert_schema_format(
                invalid_schema, 
                source_format=FileFormat.JSON_SCHEMA, 
                target_format=FileFormat.AVRO
            )


class TestSchemaInference(unittest.TestCase):
    """Test cases for schema inference from data"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample data for schema inference
        
        # Create dictionary with various data types
        self.sample_dict = {
            "id": 12345,
            "name": "Example User",
            "is_active": True,
            "score": 85.5,
            "tags": ["tag1", "tag2"],
            "metadata": {
                "created_at": "2023-01-01T12:00:00Z",
                "source": "API"
            }
        }
        
        # Create list of dictionaries with consistent structure
        self.sample_list = [
            {
                "product_id": 1,
                "product_name": "Product A",
                "price": 10.99,
                "in_stock": True
            },
            {
                "product_id": 2,
                "product_name": "Product B",
                "price": 24.99,
                "in_stock": False
            }
        ]
        
        # Create pandas DataFrame with various data types
        self.sample_df = pd.DataFrame({
            "string_col": ["a", "b", "c"],
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "bool_col": [True, False, True],
            "date_col": pd.date_range("2023-01-01", periods=3)
        })
    
    def test_infer_json_schema_from_dict(self):
        """Test inference of JSON Schema from dictionary"""
        schema = infer_schema_from_data(
            self.sample_dict,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the schema is a valid JSON Schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Verify all fields are present
        for key in self.sample_dict.keys():
            self.assertIn(key, schema["properties"])
        
        # Check some specific types
        self.assertEqual(schema["properties"]["id"]["type"], "integer")
        self.assertEqual(schema["properties"]["name"]["type"], "string")
        self.assertEqual(schema["properties"]["is_active"]["type"], "boolean")
        self.assertEqual(schema["properties"]["score"]["type"], "number")
    
    def test_infer_json_schema_from_list(self):
        """Test inference of JSON Schema from list of dictionaries"""
        schema = infer_schema_from_data(
            self.sample_list,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the schema is a valid JSON Schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Verify all fields from first object are present
        for key in self.sample_list[0].keys():
            self.assertIn(key, schema["properties"])
        
        # Check some specific types
        self.assertEqual(schema["properties"]["product_id"]["type"], "integer")
        self.assertEqual(schema["properties"]["product_name"]["type"], "string")
        self.assertEqual(schema["properties"]["price"]["type"], "number")
        self.assertEqual(schema["properties"]["in_stock"]["type"], "boolean")
    
    def test_infer_json_schema_from_dataframe(self):
        """Test inference of JSON Schema from pandas DataFrame"""
        schema = infer_schema_from_data(
            self.sample_df,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the schema is a valid JSON Schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Verify all columns are present
        for col in self.sample_df.columns:
            self.assertIn(col, schema["properties"])
        
        # Check some specific types
        self.assertEqual(schema["properties"]["string_col"]["type"], "string")
        self.assertEqual(schema["properties"]["int_col"]["type"], "integer")
        self.assertEqual(schema["properties"]["float_col"]["type"], "number")
        self.assertEqual(schema["properties"]["bool_col"]["type"], "boolean")
        # Date columns should have string type with format
        self.assertEqual(schema["properties"]["date_col"]["type"], "string")
        self.assertIn("format", schema["properties"]["date_col"])
    
    def test_infer_avro_schema_from_dict(self):
        """Test inference of Avro schema from dictionary"""
        schema = infer_schema_from_data(
            self.sample_dict,
            schema_format=FileFormat.AVRO
        )
        
        # Verify the schema is a valid Avro schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "record")
        self.assertIn("fields", schema)
        
        # Verify all fields are present
        field_names = [field["name"] for field in schema["fields"]]
        for key in self.sample_dict.keys():
            self.assertIn(key, field_names)
        
        # Check some specific types
        id_field = next(field for field in schema["fields"] if field["name"] == "id")
        name_field = next(field for field in schema["fields"] if field["name"] == "name")
        
        self.assertEqual(id_field["type"], "long")
        self.assertEqual(name_field["type"], "string")
    
    def test_infer_avro_schema_from_list(self):
        """Test inference of Avro schema from list of dictionaries"""
        schema = infer_schema_from_data(
            self.sample_list,
            schema_format=FileFormat.AVRO
        )
        
        # Verify the schema is a valid Avro schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "record")
        self.assertIn("fields", schema)
        
        # Verify all fields from first object are present
        field_names = [field["name"] for field in schema["fields"]]
        for key in self.sample_list[0].keys():
            self.assertIn(key, field_names)
        
        # Check some specific types
        product_id_field = next(field for field in schema["fields"] if field["name"] == "product_id")
        price_field = next(field for field in schema["fields"] if field["name"] == "price")
        
        self.assertEqual(product_id_field["type"], "long")
        self.assertEqual(price_field["type"], "double")
    
    def test_infer_avro_schema_from_dataframe(self):
        """Test inference of Avro schema from pandas DataFrame"""
        schema = infer_schema_from_data(
            self.sample_df,
            schema_format=FileFormat.AVRO
        )
        
        # Verify the schema is a valid Avro schema
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "record")
        self.assertIn("fields", schema)
        
        # Verify all columns are present
        field_names = [field["name"] for field in schema["fields"]]
        for col in self.sample_df.columns:
            self.assertIn(col, field_names)
        
        # Check some specific types
        string_col_field = next(field for field in schema["fields"] if field["name"] == "string_col")
        int_col_field = next(field for field in schema["fields"] if field["name"] == "int_col")
        float_col_field = next(field for field in schema["fields"] if field["name"] == "float_col")
        
        self.assertEqual(string_col_field["type"], "string")
        self.assertEqual(int_col_field["type"], "long")
        self.assertEqual(float_col_field["type"], "double")
    
    def test_infer_with_options(self):
        """Test schema inference with additional options"""
        options = {
            "title": "Test Schema",
            "description": "Schema inferred from test data",
            "required": ["id", "name"]
        }
        
        schema = infer_schema_from_data(
            self.sample_dict,
            schema_format=FileFormat.JSON_SCHEMA,
            options=options
        )
        
        # Verify options were applied
        self.assertEqual(schema["title"], "Test Schema")
        self.assertEqual(schema["description"], "Schema inferred from test data")
        self.assertListEqual(schema["required"], ["id", "name"])
    
    def test_infer_with_invalid_data(self):
        """Test schema inference with invalid data raises error"""
        invalid_data = "not a valid data structure"
        
        with self.assertRaises(TypeError):
            infer_schema_from_data(
                invalid_data,
                schema_format=FileFormat.JSON_SCHEMA
            )


class TestSchemaValidation(unittest.TestCase):
    """Test cases for schema validation functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize valid and invalid schemas for different formats
        
        # Create valid JSON Schema with standard properties
        self.valid_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create invalid JSON Schema with missing required fields
        self.invalid_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "invalid_type"},  # Invalid type
                "name": {"type": "string"}
            }
        }
        
        # Create valid Avro schema with type, namespace, fields
        self.valid_avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
        
        # Create invalid Avro schema with incorrect structure
        self.invalid_avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "invalid_type"},  # Invalid type
                {"name": "name", "type": "string"}
            ]
        }
        
        # Create valid BigQuery schema as list of SchemaField objects
        self.valid_bigquery_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("name", "STRING", mode="REQUIRED"),
            SchemaField("email", "STRING", mode="NULLABLE")
        ]
        
        # Create invalid BigQuery schema with incorrect field types
        self.invalid_bigquery_schema = [
            SchemaField("id", "INVALID_TYPE", mode="REQUIRED"),  # Invalid type
            SchemaField("name", "STRING", mode="REQUIRED")
        ]
    
    def test_validate_valid_json_schema(self):
        """Test validation of valid JSON Schema"""
        result = validate_schema(
            self.valid_json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        self.assertTrue(result)
    
    def test_validate_invalid_json_schema(self):
        """Test validation of invalid JSON Schema"""
        result = validate_schema(
            self.invalid_json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        self.assertFalse(result)
    
    def test_validate_valid_avro_schema(self):
        """Test validation of valid Avro schema"""
        result = validate_schema(
            self.valid_avro_schema,
            schema_format=FileFormat.AVRO
        )
        self.assertTrue(result)
    
    def test_validate_invalid_avro_schema(self):
        """Test validation of invalid Avro schema"""
        result = validate_schema(
            self.invalid_avro_schema,
            schema_format=FileFormat.AVRO
        )
        self.assertFalse(result)
    
    def test_validate_valid_bigquery_schema(self):
        """Test validation of valid BigQuery schema"""
        result = validate_schema(
            self.valid_bigquery_schema,
            schema_format=FileFormat.BIGQUERY
        )
        self.assertTrue(result)
    
    def test_validate_invalid_bigquery_schema(self):
        """Test validation of invalid BigQuery schema"""
        result = validate_schema(
            self.invalid_bigquery_schema,
            schema_format=FileFormat.BIGQUERY
        )
        self.assertFalse(result)
    
    def test_validate_with_auto_detection(self):
        """Test validation with automatic format detection"""
        result = validate_schema(
            self.valid_json_schema,
            schema_format=None  # Auto-detect
        )
        self.assertTrue(result)


class TestSchemaComparison(unittest.TestCase):
    """Test cases for schema comparison functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize pairs of schemas for comparison
        
        # Create base JSON Schema
        self.base_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create JSON Schema with added field
        self.json_schema_added_field = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer"}  # Added field
            },
            "required": ["id", "name"]
        }
        
        # Create JSON Schema with removed field
        self.json_schema_removed_field = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
                # email field removed
            },
            "required": ["id", "name"]
        }
        
        # Create JSON Schema with modified field type
        self.json_schema_modified_field = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},  # Changed from integer to string
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create similar schemas for Avro format
        self.base_avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
    
    def test_compare_identical_schemas(self):
        """Test comparison of identical schemas"""
        result = compare_schemas(
            self.base_json_schema,
            self.base_json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the comparison shows no differences
        self.assertEqual(result["added_fields"], [])
        self.assertEqual(result["removed_fields"], [])
        self.assertEqual(result["modified_fields"], [])
        self.assertTrue(result["is_compatible"])
    
    def test_compare_schemas_with_added_field(self):
        """Test comparison of schemas with added field"""
        result = compare_schemas(
            self.base_json_schema,
            self.json_schema_added_field,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the comparison identifies the added field
        self.assertEqual(len(result["added_fields"]), 1)
        self.assertEqual(result["added_fields"][0], "age")
        self.assertEqual(result["removed_fields"], [])
        self.assertEqual(result["modified_fields"], [])
    
    def test_compare_schemas_with_removed_field(self):
        """Test comparison of schemas with removed field"""
        result = compare_schemas(
            self.base_json_schema,
            self.json_schema_removed_field,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the comparison identifies the removed field
        self.assertEqual(result["added_fields"], [])
        self.assertEqual(len(result["removed_fields"]), 1)
        self.assertEqual(result["removed_fields"][0], "email")
        self.assertEqual(result["modified_fields"], [])
    
    def test_compare_schemas_with_modified_field(self):
        """Test comparison of schemas with modified field"""
        result = compare_schemas(
            self.base_json_schema,
            self.json_schema_modified_field,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the comparison identifies the modified field
        self.assertEqual(result["added_fields"], [])
        self.assertEqual(result["removed_fields"], [])
        self.assertEqual(len(result["modified_fields"]), 1)
        self.assertEqual(result["modified_fields"][0]["field"], "id")
        self.assertEqual(result["modified_fields"][0]["before"]["type"], "integer")
        self.assertEqual(result["modified_fields"][0]["after"]["type"], "string")
    
    def test_compare_different_format_schemas(self):
        """Test comparison of schemas in different formats"""
        result = compare_schemas(
            self.base_json_schema,
            self.base_avro_schema,
            source_format=FileFormat.JSON_SCHEMA,
            target_format=FileFormat.AVRO
        )
        
        # Verify the comparison handles format conversion
        self.assertIn("is_compatible", result)
    
    def test_compare_with_auto_detection(self):
        """Test comparison with automatic format detection"""
        result = compare_schemas(
            self.base_json_schema,
            self.json_schema_added_field,
            schema_format=None  # Auto-detect
        )
        
        # Verify the comparison works with auto-detection
        self.assertEqual(len(result["added_fields"]), 1)
        self.assertEqual(result["added_fields"][0], "age")


class TestSchemaCompatibility(unittest.TestCase):
    """Test cases for schema compatibility checking"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize schemas with different compatibility characteristics
        
        # Create base JSON Schema
        self.base_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create backward compatible schema (added optional field)
        self.backward_compatible_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer"}  # New optional field
            },
            "required": ["id", "name"]  # Same required fields
        }
        
        # Create forward compatible schema (removed optional field)
        self.forward_compatible_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
                # email removed, which was optional
            },
            "required": ["id", "name"]
        }
        
        # Create incompatible schema (changed required field type)
        self.incompatible_json = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},  # Changed from integer to string
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create similar schemas for Avro format
        self.base_avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
        
        self.backward_compatible_avro = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "age", "type": ["null", "long"], "default": None}  # New optional field
            ]
        }
        
        self.forward_compatible_avro = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
                # email removed, which was optional (null)
            ]
        }
        
        self.incompatible_avro = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},  # Changed from long to string
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
    
    def test_backward_compatibility_json(self):
        """Test backward compatibility check for JSON Schema"""
        result = is_schema_compatible(
            self.base_json_schema,
            self.backward_compatible_json,
            schema_format=FileFormat.JSON_SCHEMA,
            compatibility_type="backward"
        )
        self.assertTrue(result)
    
    def test_forward_compatibility_json(self):
        """Test forward compatibility check for JSON Schema"""
        result = is_schema_compatible(
            self.base_json_schema,
            self.forward_compatible_json,
            schema_format=FileFormat.JSON_SCHEMA,
            compatibility_type="forward"
        )
        self.assertTrue(result)
    
    def test_full_compatibility_json(self):
        """Test full compatibility check for JSON Schema"""
        # For this test, we'll use a fully compatible schema that is both backward and forward compatible
        fully_compatible_json = dict(self.base_json_schema)  # Deep copy
        
        result = is_schema_compatible(
            self.base_json_schema,
            fully_compatible_json,
            schema_format=FileFormat.JSON_SCHEMA,
            compatibility_type="full"
        )
        self.assertTrue(result)
    
    def test_incompatible_schemas_json(self):
        """Test incompatible schemas for JSON Schema"""
        result = is_schema_compatible(
            self.base_json_schema,
            self.incompatible_json,
            schema_format=FileFormat.JSON_SCHEMA
        )
        self.assertFalse(result)
    
    def test_backward_compatibility_avro(self):
        """Test backward compatibility check for Avro schema"""
        result = is_schema_compatible(
            self.base_avro_schema,
            self.backward_compatible_avro,
            schema_format=FileFormat.AVRO,
            compatibility_type="backward"
        )
        self.assertTrue(result)
    
    def test_forward_compatibility_avro(self):
        """Test forward compatibility check for Avro schema"""
        result = is_schema_compatible(
            self.base_avro_schema,
            self.forward_compatible_avro,
            schema_format=FileFormat.AVRO,
            compatibility_type="forward"
        )
        self.assertTrue(result)
    
    def test_incompatible_schemas_avro(self):
        """Test incompatible schemas for Avro schema"""
        result = is_schema_compatible(
            self.base_avro_schema,
            self.incompatible_avro,
            schema_format=FileFormat.AVRO
        )
        self.assertFalse(result)
    
    def test_compatibility_with_auto_detection(self):
        """Test compatibility check with automatic format detection"""
        result = is_schema_compatible(
            self.base_json_schema,
            self.backward_compatible_json,
            schema_format=None,  # Auto-detect
            compatibility_type="backward"
        )
        self.assertTrue(result)


class TestSchemaMerging(unittest.TestCase):
    """Test cases for schema merging functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize schemas for merging tests
        
        # Create JSON Schema with first set of fields
        self.json_schema1 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            },
            "required": ["id"]
        }
        
        # Create JSON Schema with second set of fields
        self.json_schema2 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer"}
            },
            "required": ["email"]
        }
        
        # Create JSON Schema with overlapping fields
        self.json_schema3 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "string"},  # Different type for same field
                "phone": {"type": "string"}
            },
            "required": ["phone"]
        }
        
        # Create similar schemas for Avro format
        self.avro_schema1 = {
            "type": "record",
            "namespace": "com.example",
            "name": "User1",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        
        self.avro_schema2 = {
            "type": "record",
            "namespace": "com.example",
            "name": "User2",
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "age", "type": ["null", "long"]}
            ]
        }
    
    def test_merge_non_overlapping_json_schemas(self):
        """Test merging non-overlapping JSON Schemas"""
        merged = merge_schemas(
            [self.json_schema1, self.json_schema2],
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify merged schema has all fields from both schemas
        self.assertIsInstance(merged, dict)
        self.assertEqual(merged["type"], "object")
        self.assertIn("properties", merged)
        
        # Check that all properties are present
        for prop in ["id", "name", "email", "age"]:
            self.assertIn(prop, merged["properties"])
        
        # Check that required fields are preserved
        self.assertIn("required", merged)
        self.assertIn("id", merged["required"])
        self.assertIn("email", merged["required"])
    
    def test_merge_overlapping_json_schemas(self):
        """Test merging JSON Schemas with overlapping fields"""
        merged = merge_schemas(
            [self.json_schema1, self.json_schema3],
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify merged schema has all fields
        for prop in ["id", "name", "phone"]:
            self.assertIn(prop, merged["properties"])
        
        # Check handling of the overlapping "id" field with different types
        id_field = merged["properties"]["id"]
        # The merged field should handle both types (e.g., using oneOf or a union type)
        self.assertTrue(
            "oneOf" in id_field or  # Using oneOf
            (isinstance(id_field["type"], list) and "integer" in id_field["type"] and "string" in id_field["type"])  # Using type array
        )
        
        # Check that required fields are preserved
        self.assertIn("required", merged)
        self.assertIn("id", merged["required"])
        self.assertIn("phone", merged["required"])
    
    def test_merge_with_conflict_resolution_options(self):
        """Test merging with conflict resolution options"""
        options = {
            "conflict_resolution": "first_wins",  # Use first schema's definition for conflicts
            "merge_required": True  # Combine required fields
        }
        
        merged = merge_schemas(
            [self.json_schema1, self.json_schema3],
            schema_format=FileFormat.JSON_SCHEMA,
            options=options
        )
        
        # With first_wins, id should keep the type from first schema
        self.assertEqual(merged["properties"]["id"]["type"], "integer")
        
        # With merge_required, both id and phone should be required
        self.assertIn("id", merged["required"])
        self.assertIn("phone", merged["required"])
    
    def test_merge_non_overlapping_avro_schemas(self):
        """Test merging non-overlapping Avro schemas"""
        merged = merge_schemas(
            [self.avro_schema1, self.avro_schema2],
            schema_format=FileFormat.AVRO
        )
        
        # Verify merged schema has all fields from both schemas
        self.assertIsInstance(merged, dict)
        self.assertEqual(merged["type"], "record")
        self.assertIn("fields", merged)
        
        # Check that all fields are present
        field_names = [field["name"] for field in merged["fields"]]
        for field_name in ["id", "name", "email", "age"]:
            self.assertIn(field_name, field_names)
    
    def test_merge_overlapping_avro_schemas(self):
        """Test merging Avro schemas with overlapping fields"""
        # Create Avro schema with overlapping field
        avro_schema3 = {
            "type": "record",
            "namespace": "com.example",
            "name": "User3",
            "fields": [
                {"name": "id", "type": "string"},  # Different type from schema1
                {"name": "phone", "type": "string"}
            ]
        }
        
        merged = merge_schemas(
            [self.avro_schema1, avro_schema3],
            schema_format=FileFormat.AVRO
        )
        
        # Verify merged schema has all fields
        field_names = [field["name"] for field in merged["fields"]]
        for field_name in ["id", "name", "phone"]:
            self.assertIn(field_name, field_names)
        
        # Check handling of the overlapping "id" field
        id_field = next(field for field in merged["fields"] if field["name"] == "id")
        # The merged field should handle both types (using a union type)
        self.assertIsInstance(id_field["type"], list)
        id_types = id_field["type"] if isinstance(id_field["type"], list) else [id_field["type"]]
        self.assertTrue(any(t == "long" or t == "string" for t in id_types))
    
    def test_merge_with_auto_detection(self):
        """Test merging with automatic format detection"""
        merged = merge_schemas(
            [self.json_schema1, self.json_schema2],
            schema_format=None  # Auto-detect
        )
        
        # Verify merged schema is correct
        self.assertIsInstance(merged, dict)
        self.assertEqual(merged["type"], "object")  # JSON Schema type
        for prop in ["id", "name", "email", "age"]:
            self.assertIn(prop, merged["properties"])
    
    def test_merge_different_format_schemas(self):
        """Test merging schemas in different formats"""
        with self.assertRaises(SchemaError):
            merge_schemas(
                [self.json_schema1, self.avro_schema1]  # Different formats
            )


class TestSchemaExtraction(unittest.TestCase):
    """Test cases for schema extraction from files"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample data files for different formats
        
        # Create JSON data file content
        self.json_data = json.dumps([
            {
                "id": 1,
                "name": "Example",
                "tags": ["tag1", "tag2"],
                "active": True,
                "score": 85.5
            }
        ])
        
        # Create Avro data file mock (we'll mock the actual file reading)
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"}
            ]
        }
        
        # Create CSV data content
        self.csv_data = "id,name,active,score\n1,Example,True,85.5"
        
        # Create DataFrame for parquet mock
        self.parquet_data = pd.DataFrame({
            "id": [1],
            "name": ["Example"],
            "active": [True],
            "score": [85.5]
        })
    
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_schema_from_json_file(self, mock_file):
        """Test schema extraction from JSON file"""
        # Configure mock to return JSON data
        mock_file.return_value.read.return_value = self.json_data
        
        # Mock json.load to return parsed JSON
        with patch("json.load", return_value=json.loads(self.json_data)):
            schema = extract_schema_from_file(
                "test.json",
                target_format=FileFormat.JSON_SCHEMA
            )
        
        # Verify schema extraction was successful
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Check that all properties are present
        for prop in ["id", "name", "tags", "active", "score"]:
            self.assertIn(prop, schema["properties"])
    
    @patch("avro.datafile.DataFileReader")
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_schema_from_avro_file(self, mock_file, mock_avro_reader):
        """Test schema extraction from Avro file"""
        # Configure mock Avro reader
        mock_reader_instance = MagicMock()
        mock_reader_instance.meta.get.return_value = json.dumps(self.avro_schema).encode('utf-8')
        mock_avro_reader.return_value = mock_reader_instance
        
        schema = extract_schema_from_file(
            "test.avro",
            target_format=FileFormat.AVRO
        )
        
        # Verify schema extraction was successful
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "record")
        self.assertIn("fields", schema)
        
        # Check that fields are present
        field_names = [field["name"] for field in schema["fields"]]
        for field in ["id", "name", "tags", "active", "score"]:
            self.assertIn(field, field_names)
    
    @patch("pandas.read_csv")
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_schema_from_csv_file(self, mock_file, mock_read_csv):
        """Test schema inference from CSV file"""
        # Configure pandas mock
        mock_read_csv.return_value = pd.DataFrame({
            "id": [1],
            "name": ["Example"],
            "active": [True],
            "score": [85.5]
        })
        
        schema = extract_schema_from_file(
            "test.csv",
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify schema extraction was successful
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Check that all properties are present
        for prop in ["id", "name", "active", "score"]:
            self.assertIn(prop, schema["properties"])
    
    @patch("pandas.read_parquet")
    def test_extract_schema_from_parquet_file(self, mock_read_parquet):
        """Test schema extraction from Parquet file"""
        # Configure pandas mock
        mock_read_parquet.return_value = self.parquet_data
        
        schema = extract_schema_from_file(
            "test.parquet",
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify schema extraction was successful
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)
        
        # Check that all properties are present
        for prop in ["id", "name", "active", "score"]:
            self.assertIn(prop, schema["properties"])
    
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_with_target_format(self, mock_file):
        """Test extraction with target format conversion"""
        # Configure mock to return JSON data
        mock_file.return_value.read.return_value = self.json_data
        
        # Mock json.load to return parsed JSON
        with patch("json.load", return_value=json.loads(self.json_data)):
            schema = extract_schema_from_file(
                "test.json",
                target_format=FileFormat.AVRO
            )
        
        # Verify schema was converted to Avro format
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema["type"], "record")
        self.assertIn("fields", schema)
        
        # Check that all fields are present
        field_names = [field["name"] for field in schema["fields"]]
        for field in ["id", "name", "tags", "active", "score"]:
            self.assertIn(field, field_names)
    
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_with_options(self, mock_file):
        """Test extraction with additional options"""
        # Configure mock to return JSON data
        mock_file.return_value.read.return_value = self.json_data
        
        options = {
            "title": "Test Schema",
            "description": "Extracted from test data"
        }
        
        # Mock json.load to return parsed JSON
        with patch("json.load", return_value=json.loads(self.json_data)):
            schema = extract_schema_from_file(
                "test.json",
                target_format=FileFormat.JSON_SCHEMA,
                options=options
            )
        
        # Verify options were applied
        self.assertEqual(schema["title"], "Test Schema")
        self.assertEqual(schema["description"], "Extracted from test data")
    
    @patch("builtins.open", side_effect=IOError("File not found"))
    def test_extract_from_invalid_file(self, mock_file):
        """Test extraction from invalid file raises error"""
        with self.assertRaises(IOError):
            extract_schema_from_file("nonexistent.json")


class TestSchemaFingerprinting(unittest.TestCase):
    """Test cases for schema fingerprinting functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample schemas for fingerprinting
        
        # Create JSON Schema with specific structure
        self.json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Create equivalent JSON Schema with different property order
        self.equivalent_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "id": {"type": "integer"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["name", "id"]  # Different order
        }
        
        # Create different JSON Schema
        self.different_json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "phone": {"type": "string"}  # Different field
            },
            "required": ["id", "name"]
        }
        
        # Create similar schemas for Avro format
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
    
    def test_json_schema_fingerprint(self):
        """Test fingerprinting of JSON Schema"""
        fingerprint = get_schema_fingerprint(
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the fingerprint is a non-empty string
        self.assertIsInstance(fingerprint, str)
        self.assertTrue(len(fingerprint) > 0)
        
        # Verify fingerprint format matches expected hash format
        self.assertTrue(all(c in "0123456789abcdef" for c in fingerprint))
    
    def test_avro_schema_fingerprint(self):
        """Test fingerprinting of Avro schema"""
        fingerprint = get_schema_fingerprint(
            self.avro_schema,
            schema_format=FileFormat.AVRO
        )
        
        # Verify the fingerprint is a non-empty string
        self.assertIsInstance(fingerprint, str)
        self.assertTrue(len(fingerprint) > 0)
        
        # Verify fingerprint format matches expected hash format
        self.assertTrue(all(c in "0123456789abcdef" for c in fingerprint))
    
    def test_equivalent_schemas_same_fingerprint(self):
        """Test that equivalent schemas have same fingerprint"""
        fingerprint1 = get_schema_fingerprint(
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        fingerprint2 = get_schema_fingerprint(
            self.equivalent_json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Equivalent schemas should have the same fingerprint
        self.assertEqual(fingerprint1, fingerprint2)
    
    def test_different_schemas_different_fingerprints(self):
        """Test that different schemas have different fingerprints"""
        fingerprint1 = get_schema_fingerprint(
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        fingerprint2 = get_schema_fingerprint(
            self.different_json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Different schemas should have different fingerprints
        self.assertNotEqual(fingerprint1, fingerprint2)
    
    def test_fingerprint_with_different_algorithms(self):
        """Test fingerprinting with different hash algorithms"""
        fingerprint_sha256 = get_schema_fingerprint(
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA,
            algorithm="sha256"
        )
        
        fingerprint_md5 = get_schema_fingerprint(
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA,
            algorithm="md5"
        )
        
        # Different algorithms should produce different fingerprints
        self.assertNotEqual(fingerprint_sha256, fingerprint_md5)
        
        # SHA-256 fingerprint should be 64 characters long
        self.assertEqual(len(fingerprint_sha256), 64)
        
        # MD5 fingerprint should be 32 characters long
        self.assertEqual(len(fingerprint_md5), 32)
    
    def test_fingerprint_with_auto_detection(self):
        """Test fingerprinting with automatic format detection"""
        fingerprint = get_schema_fingerprint(
            self.json_schema,
            schema_format=None  # Auto-detect
        )
        
        # Verify the fingerprint is a valid fingerprint
        self.assertIsInstance(fingerprint, str)
        self.assertTrue(len(fingerprint) > 0)


class TestSchemaNormalization(unittest.TestCase):
    """Test cases for schema normalization functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample schemas for normalization
        
        # Create JSON Schema with unordered properties
        self.json_schema_unordered = {
            "type": "object",
            "required": ["name", "id"],  # Unordered required fields
            "properties": {
                "name": {"type": "string"},
                "id": {"type": "integer"},
                "email": {"type": "string"}
            },
            "$schema": "http://json-schema.org/draft-07/schema#"  # Schema property not first
        }
        
        # Create JSON Schema with inconsistent type formats
        self.json_schema_inconsistent = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "tags": {"type": ["string", "null"]},  # Inconsistent order for union type
                "status": {"type": ["null", "string"]}
            }
        }
        
        # Create Avro schema with unordered fields
        self.avro_schema_unordered = {
            "namespace": "com.example",  # Namespace before type
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "id", "type": "long"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "status", "type": ["string", "null"]}  # Inconsistent order
            ]
        }
        
        # Create BigQuery schema with unordered fields
        self.bigquery_schema_unordered = [
            SchemaField("name", "STRING", mode="REQUIRED"),
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("email", "STRING", mode="NULLABLE")
        ]
    
    def test_normalize_json_schema(self):
        """Test normalization of JSON Schema"""
        normalized = normalize_schema(
            self.json_schema_unordered,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify properties are in alphabetical order
        property_keys = list(normalized["properties"].keys())
        self.assertEqual(property_keys, sorted(property_keys))
        
        # Verify type definitions are in consistent format
        self.assertEqual(normalized["type"], "object")
    
    def test_normalize_avro_schema(self):
        """Test normalization of Avro schema"""
        normalized = normalize_schema(
            self.avro_schema_unordered,
            schema_format=FileFormat.AVRO
        )
        
        # Verify fields are in alphabetical order
        field_names = [field["name"] for field in normalized["fields"]]
        self.assertEqual(field_names, sorted(field_names))
        
        # Verify type definitions are in consistent format
        self.assertEqual(normalized["type"], "record")
    
    def test_normalize_bigquery_schema(self):
        """Test normalization of BigQuery schema"""
        normalized = normalize_schema(
            self.bigquery_schema_unordered,
            schema_format=FileFormat.BIGQUERY
        )
        
        # Verify fields are in alphabetical order
        field_names = [field.name for field in normalized]
        self.assertEqual(field_names, sorted(field_names))
    
    def test_normalize_with_auto_detection(self):
        """Test normalization with automatic format detection"""
        normalized = normalize_schema(
            self.json_schema_unordered,
            schema_format=None  # Auto-detect
        )
        
        # Verify result is properly normalized
        property_keys = list(normalized["properties"].keys())
        self.assertEqual(property_keys, sorted(property_keys))


class TestBigQuerySchemaConversion(unittest.TestCase):
    """Test cases for BigQuery schema conversion"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize sample schemas for BigQuery conversion
        
        # Create JSON Schema with various types
        self.json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "amount": {"type": "number"},
                "is_active": {"type": "boolean"},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": "string", "format": "date-time"}
                    }
                },
                "nullable_field": {"type": ["null", "string"]}
            },
            "required": ["id", "name"]
        }
        
        # Create Avro schema with various types
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "is_active", "type": "boolean"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "metadata", "type": {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                        {"name": "updated_at", "type": {"type": "long", "logicalType": "timestamp-micros"}}
                    ]
                }},
                {"name": "nullable_field", "type": ["null", "string"]}
            ]
        }
        
        # Create BigQuery schema as list of SchemaField objects
        self.bigquery_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("name", "STRING", mode="REQUIRED"),
            SchemaField("amount", "FLOAT", mode="NULLABLE"),
            SchemaField("is_active", "BOOLEAN", mode="NULLABLE"),
            SchemaField("tags", "STRING", mode="REPEATED"),
            SchemaField("metadata", "RECORD", mode="NULLABLE", fields=[
                SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
            ]),
            SchemaField("nullable_field", "STRING", mode="NULLABLE")
        ]
    
    def test_convert_json_schema_to_bigquery(self):
        """Test conversion from JSON Schema to BigQuery schema"""
        bq_schema = convert_to_bigquery_schema(
            self.json_schema,
            source_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the result is a list of SchemaField objects
        self.assertIsInstance(bq_schema, list)
        self.assertTrue(all(isinstance(field, SchemaField) for field in bq_schema))
        
        # Verify field names and types are correctly converted
        field_names = [field.name for field in bq_schema]
        self.assertListEqual(
            sorted(field_names),
            sorted(["id", "name", "amount", "is_active", "tags", "metadata", "nullable_field"])
        )
        
        # Check some specific field properties
        id_field = next(field for field in bq_schema if field.name == "id")
        tags_field = next(field for field in bq_schema if field.name == "tags")
        
        self.assertEqual(id_field.field_type, "INTEGER")
        self.assertEqual(id_field.mode, "REQUIRED")
        self.assertEqual(tags_field.mode, "REPEATED")
    
    def test_convert_avro_schema_to_bigquery(self):
        """Test conversion from Avro schema to BigQuery schema"""
        bq_schema = convert_to_bigquery_schema(
            self.avro_schema,
            source_format=FileFormat.AVRO
        )
        
        # Verify the result is a list of SchemaField objects
        self.assertIsInstance(bq_schema, list)
        self.assertTrue(all(isinstance(field, SchemaField) for field in bq_schema))
        
        # Verify field names and types are correctly converted
        field_names = [field.name for field in bq_schema]
        self.assertListEqual(
            sorted(field_names),
            sorted(["id", "name", "amount", "is_active", "tags", "metadata", "nullable_field"])
        )
        
        # Check some specific field properties
        id_field = next(field for field in bq_schema if field.name == "id")
        nullable_field = next(field for field in bq_schema if field.name == "nullable_field")
        
        self.assertEqual(id_field.field_type, "INTEGER")
        self.assertEqual(nullable_field.mode, "NULLABLE")
    
    def test_convert_bigquery_to_json_schema(self):
        """Test conversion from BigQuery schema to JSON Schema"""
        json_schema = convert_from_bigquery_schema(
            self.bigquery_schema,
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify the result is a valid JSON Schema
        self.assertIsInstance(json_schema, dict)
        self.assertEqual(json_schema["type"], "object")
        self.assertIn("properties", json_schema)
        
        # Verify properties and types are correctly converted
        for prop in ["id", "name", "amount", "is_active", "tags", "metadata", "nullable_field"]:
            self.assertIn(prop, json_schema["properties"])
            
        # Check required fields
        self.assertIn("required", json_schema)
        self.assertIn("id", json_schema["required"])
        self.assertIn("name", json_schema["required"])
    
    def test_convert_bigquery_to_avro_schema(self):
        """Test conversion from BigQuery schema to Avro schema"""
        avro_schema = convert_from_bigquery_schema(
            self.bigquery_schema,
            target_format=FileFormat.AVRO
        )
        
        # Verify the result is a valid Avro schema
        self.assertIsInstance(avro_schema, dict)
        self.assertEqual(avro_schema["type"], "record")
        self.assertIn("fields", avro_schema)
        
        # Verify fields and types are correctly converted
        field_names = [field["name"] for field in avro_schema["fields"]]
        for field_name in ["id", "name", "amount", "is_active", "tags", "metadata", "nullable_field"]:
            self.assertIn(field_name, field_names)
    
    def test_convert_complex_types(self):
        """Test conversion of complex types"""
        # Create schemas with nested objects, arrays, and maps
        complex_json_schema = {
            "type": "object",
            "properties": {
                "nested_array": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "value": {"type": "integer"}
                        }
                    }
                },
                "string_array": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        }
        
        # Convert to BigQuery schema
        bq_schema = convert_to_bigquery_schema(
            complex_json_schema,
            source_format=FileFormat.JSON_SCHEMA
        )
        
        # Check for correct structure
        nested_array_field = next(field for field in bq_schema if field.name == "nested_array")
        string_array_field = next(field for field in bq_schema if field.name == "string_array")
        
        self.assertEqual(nested_array_field.field_type, "RECORD")
        self.assertEqual(nested_array_field.mode, "REPEATED")
        self.assertEqual(string_array_field.field_type, "STRING")
        self.assertEqual(string_array_field.mode, "REPEATED")
        
        # Convert back to JSON Schema
        converted_back = convert_from_bigquery_schema(
            bq_schema,
            target_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify structure was preserved
        self.assertIn("nested_array", converted_back["properties"])
        self.assertEqual(converted_back["properties"]["nested_array"]["type"], "array")
    
    def test_convert_with_options(self):
        """Test conversion with additional options"""
        options = {
            "title": "Test Schema",
            "description": "Converted from BigQuery"
        }
        
        json_schema = convert_from_bigquery_schema(
            self.bigquery_schema,
            target_format=FileFormat.JSON_SCHEMA,
            options=options
        )
        
        # Verify options were applied
        self.assertEqual(json_schema["title"], "Test Schema")
        self.assertEqual(json_schema["description"], "Converted from BigQuery")


class TestSchemaRegistry(unittest.TestCase):
    """Test cases for SchemaRegistry class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize SchemaRegistry instance
        self.registry = SchemaRegistry()
        
        # Create sample schemas for registration
        self.json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        self.avro_schema = {
            "type": "record",
            "namespace": "com.example",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }
    
    def test_register_schema(self):
        """Test schema registration"""
        schema_id = "user_schema"
        fingerprint = self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify a fingerprint is returned
        self.assertIsInstance(fingerprint, str)
        self.assertTrue(len(fingerprint) > 0)
        
        # Verify schema is stored in registry
        registered_schema = self.registry.get_schema(schema_id)
        self.assertEqual(registered_schema, self.json_schema)
    
    def test_get_schema(self):
        """Test schema retrieval"""
        # Register a schema
        schema_id = "test_schema"
        self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Retrieve the schema
        retrieved_schema = self.registry.get_schema(schema_id)
        
        # Verify returned schema matches registered schema
        self.assertEqual(retrieved_schema, self.json_schema)
    
    def test_get_schema_by_fingerprint(self):
        """Test schema retrieval by fingerprint"""
        # Register a schema and get its fingerprint
        schema_id = "test_schema"
        fingerprint = self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Retrieve the schema by fingerprint
        retrieved_schema = self.registry.get_schema_by_fingerprint(fingerprint)
        
        # Verify returned schema matches registered schema
        self.assertEqual(retrieved_schema, self.json_schema)
    
    def test_get_schema_with_format_conversion(self):
        """Test schema retrieval with format conversion"""
        # Register a JSON Schema
        schema_id = "test_schema"
        self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Retrieve the schema as Avro
        avro_schema = self.registry.get_schema(
            schema_id,
            target_format=FileFormat.AVRO
        )
        
        # Verify schema is in Avro format
        self.assertIsInstance(avro_schema, dict)
        self.assertEqual(avro_schema["type"], "record")
        self.assertIn("fields", avro_schema)
    
    def test_update_schema(self):
        """Test schema update"""
        # Register initial schema
        schema_id = "test_schema"
        initial_fingerprint = self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Create an updated version of the schema
        updated_schema = dict(self.json_schema)
        updated_schema["properties"]["age"] = {"type": "integer"}
        
        # Update the schema
        updated_fingerprint = self.registry.update_schema(
            schema_id, 
            updated_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify a new fingerprint is returned
        self.assertIsInstance(updated_fingerprint, str)
        self.assertNotEqual(initial_fingerprint, updated_fingerprint)
        
        # Verify updated schema is stored in registry
        retrieved_schema = self.registry.get_schema(schema_id)
        self.assertEqual(retrieved_schema, updated_schema)
    
    def test_delete_schema(self):
        """Test schema deletion"""
        # Register a schema
        schema_id = "test_schema"
        self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Delete the schema
        result = self.registry.delete_schema(schema_id)
        
        # Verify True is returned
        self.assertTrue(result)
        
        # Verify schema is no longer in registry
        with self.assertRaises(KeyError):
            self.registry.get_schema(schema_id)
    
    def test_list_schemas(self):
        """Test listing schemas"""
        # Register multiple schemas
        self.registry.register_schema(
            "schema1", 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        self.registry.register_schema(
            "schema2", 
            self.avro_schema,
            schema_format=FileFormat.AVRO
        )
        
        # List schemas
        schemas = self.registry.list_schemas()
        
        # Verify all registered schema IDs are in the result
        self.assertIn("schema1", schemas)
        self.assertIn("schema2", schemas)
    
    def test_list_schemas_with_filters(self):
        """Test listing schemas with filters"""
        # Register multiple schemas with different metadata
        self.registry.register_schema(
            "user_schema", 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA,
            metadata={"domain": "user", "version": "1.0"}
        )
        
        self.registry.register_schema(
            "product_schema", 
            self.avro_schema,
            schema_format=FileFormat.AVRO,
            metadata={"domain": "product", "version": "1.0"}
        )
        
        # List schemas with filters
        user_schemas = self.registry.list_schemas(filters={"metadata.domain": "user"})
        
        # Verify only matching schema IDs are in the result
        self.assertIn("user_schema", user_schemas)
        self.assertNotIn("product_schema", user_schemas)
    
    def test_metadata_operations(self):
        """Test metadata operations"""
        # Register a schema with metadata
        schema_id = "test_schema"
        metadata = {"domain": "user", "version": "1.0", "owner": "team_a"}
        
        self.registry.register_schema(
            schema_id, 
            self.json_schema,
            schema_format=FileFormat.JSON_SCHEMA,
            metadata=metadata
        )
        
        # Get metadata
        retrieved_metadata = self.registry.get_metadata(schema_id)
        
        # Verify metadata is correctly updated
        self.assertEqual(retrieved_metadata, metadata)
        
        # Update metadata
        updated_metadata = {"domain": "user", "version": "2.0", "owner": "team_b"}
        self.registry.update_metadata(schema_id, updated_metadata)
        
        # Verify metadata is correctly updated
        retrieved_metadata = self.registry.get_metadata(schema_id)
        self.assertEqual(retrieved_metadata, updated_metadata)


class TestSchemaEvolutionManager(unittest.TestCase):
    """Test cases for SchemaEvolutionManager class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Initialize SchemaRegistry instance
        self.registry = SchemaRegistry()
        
        # Initialize SchemaEvolutionManager with registry
        self.evolution_manager = SchemaEvolutionManager(self.registry)
        
        # Register base schemas in registry
        self.base_schema_id = "user_schema_v1"
        self.registry.register_schema(
            self.base_schema_id,
            {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                },
                "required": ["id", "name"]
            },
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Create sample data for evolution tests
        self.base_data = {
            "id": 1,
            "name": "Test User",
            "email": "test@example.com"
        }
    
    def test_evolve_schema_with_new_field(self):
        """Test schema evolution with new field"""
        # Create data with new field
        new_data = dict(self.base_data)
        new_data["age"] = 30
        
        # Evolve schema based on new data
        evolution_result = self.evolution_manager.evolve_schema(
            self.base_schema_id,
            new_data,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify evolution result contains updated schema
        self.assertIn("evolved_schema", evolution_result)
        self.assertIn("compatibility", evolution_result)
        self.assertIn("changes", evolution_result)
        
        # Verify new field is added to schema
        evolved_schema = evolution_result["evolved_schema"]
        self.assertIn("age", evolved_schema["properties"])
    
    def test_evolve_schema_with_type_change(self):
        """Test schema evolution with type change"""
        # Create data with changed field type
        changed_data = dict(self.base_data)
        changed_data["id"] = "string_id"  # Changed from integer to string
        
        # Evolve schema based on changed data
        evolution_result = self.evolution_manager.evolve_schema(
            self.base_schema_id,
            changed_data,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Verify evolution result contains compatibility assessment
        self.assertIn("evolved_schema", evolution_result)
        self.assertIn("compatibility", evolution_result)
        self.assertIn("changes", evolution_result)
        
        # Check if type change is handled according to evolution options
        evolved_schema = evolution_result["evolved_schema"]
        id_type = evolved_schema["properties"]["id"]["type"]
        
        # Either the type was changed to string, or it became a union type, or it was rejected
        self.assertTrue(
            id_type == "string" or  # Changed to string
            (isinstance(id_type, list) and "string" in id_type) or  # Union type
            id_type == "integer"  # Rejected change (kept original)
        )
    
    def test_evolve_schema_with_options(self):
        """Test schema evolution with options"""
        # Create data with new field
        new_data = dict(self.base_data)
        new_data["age"] = 30
        
        # Create evolution options
        options = {
            "compatibility": "backward",
            "add_if_missing": True,
            "change_types": False,
            "make_required": False
        }
        
        # Evolve schema with options
        evolution_result = self.evolution_manager.evolve_schema(
            self.base_schema_id,
            new_data,
            schema_format=FileFormat.JSON_SCHEMA,
            options=options
        )
        
        # Verify evolution follows the specified options
        evolved_schema = evolution_result["evolved_schema"]
        
        # Should add age field since add_if_missing is True
        self.assertIn("age", evolved_schema["properties"])
        
        # New field should not be required since make_required is False
        if "required" in evolved_schema:
            self.assertNotIn("age", evolved_schema["required"])
    
    def test_check_compatibility(self):
        """Test compatibility checking between schemas"""
        # Register a second schema in registry
        second_schema_id = "user_schema_v2"
        self.registry.register_schema(
            second_schema_id,
            {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "age": {"type": "integer"}  # Added field
                },
                "required": ["id", "name"]
            },
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        # Check compatibility between schemas
        compatibility_result = self.evolution_manager.check_compatibility(
            self.base_schema_id,
            second_schema_id,
            compatibility_type="backward"
        )
        
        # Verify compatibility assessment is returned
        self.assertIn("is_compatible", compatibility_result)
        self.assertIn("details", compatibility_result)
    
    def test_get_evolution_history(self):
        """Test retrieving evolution history"""
        # Evolve a schema multiple times
        # First evolution: add age field
        new_data1 = dict(self.base_data)
        new_data1["age"] = 30
        self.evolution_manager.evolve_schema(
            self.base_schema_id,
            new_data1,
            schema_format=FileFormat.JSON_SCHEMA,
            create_version=True
        )
        
        # Second evolution: add address field
        new_data2 = dict(new_data1)
        new_data2["address"] = "123 Main St"
        self.evolution_manager.evolve_schema(
            self.base_schema_id,
            new_data2,
            schema_format=FileFormat.JSON_SCHEMA,
            create_version=True
        )
        
        # Get evolution history
        history = self.evolution_manager.get_evolution_history(self.base_schema_id)
        
        # Verify history contains all evolution events
        self.assertIsInstance(history, list)
        self.assertEqual(len(history), 2)  # Should have two evolution events
    
    def test_compatibility_rules(self):
        """Test compatibility rule management"""
        # Set compatibility rule with schema_id and rule
        schema_id = "test_schema"
        self.registry.register_schema(
            schema_id,
            self.base_data,
            schema_format=FileFormat.JSON_SCHEMA
        )
        
        rule = "backward"
        self.evolution_manager.set_compatibility_rule(schema_id, rule)
        
        # Get compatibility rule
        returned_rule = self.evolution_manager.get_compatibility_rule(schema_id)
        
        # Verify returned rule matches the set rule
        self.assertEqual(returned_rule, rule)