"""
Initialization module for the schema utilities package that provides a unified interface for working with various schema formats (JSON Schema, Avro, BigQuery) in the self-healing data pipeline. This module imports and exposes key functionality from the specialized schema utility modules.
"""

from .schema_utils import (
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
# Import general schema utility functions and classes

from .jsonschema_utils import (
    parse_json_schema,
    validate_json_schema,
    validate_data_against_schema,
    infer_json_schema,
    convert_json_schema_to_bigquery_schema,
    convert_bigquery_to_json_schema,
    extract_schema_from_json_file,
    merge_json_schemas,
    map_pandas_dtype_to_json_schema,
    normalize_json_schema,
    JsonSchemaValidator
)
# Import JSON Schema specific utilities

from .avro_utils import (
    parse_avro_schema,
    validate_avro_schema,
    validate_data_against_avro_schema,
    infer_avro_schema,
    convert_avro_to_bigquery_schema,
    convert_bigquery_to_avro_schema,
    is_avro_schema_compatible,
    get_avro_schema_fingerprint,
    extract_schema_from_avro_file,
    merge_avro_schemas,
    avro_schema_to_json,
    map_pandas_dtype_to_avro,
    read_avro_file_to_dataframe,
    write_dataframe_to_avro,
    AvroSchemaValidator
)
# Import Avro Schema specific utilities

__all__ = [
    "detect_schema_format",
    "convert_schema_format",
    "infer_schema_from_data",
    "validate_schema",
    "compare_schemas",
    "is_schema_compatible",
    "merge_schemas",
    "extract_schema_from_file",
    "get_schema_fingerprint",
    "normalize_schema",
    "convert_to_bigquery_schema",
    "convert_from_bigquery_schema",
    "SchemaRegistry",
    "SchemaEvolutionManager",
    "parse_json_schema",
    "validate_json_schema",
    "validate_data_against_schema",
    "infer_json_schema",
    "convert_json_schema_to_bigquery_schema",
    "convert_bigquery_to_json_schema",
    "extract_schema_from_json_file",
    "merge_json_schemas",
    "map_pandas_dtype_to_json_schema",
    "normalize_json_schema",
    "JsonSchemaValidator",
    "parse_avro_schema",
    "validate_avro_schema",
    "validate_data_against_avro_schema",
    "infer_avro_schema",
    "convert_avro_to_bigquery_schema",
    "convert_bigquery_to_avro_schema",
    "is_avro_schema_compatible",
    "get_avro_schema_fingerprint",
    "extract_schema_from_avro_file",
    "merge_avro_schemas",
    "avro_schema_to_json",
    "map_pandas_dtype_to_avro",
    "read_avro_file_to_dataframe",
    "write_dataframe_to_avro",
    "AvroSchemaValidator"
]