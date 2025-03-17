"""Implements schema validation for data quality checks, focusing on validating
the structure and format of datasets including column existence, data types,
and schema consistency. This validator is a core component of the data
quality validation framework that ensures data conforms to expected schemas
before further processing.
"""

import typing
import pandas  # version 2.0.x
from google.cloud import bigquery  # version 3.11.0+

from src.backend import constants  # src/backend/constants.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py
from src.backend.quality.engines.validation_engine import ValidationResult, create_validation_result  # ../engines/validation_engine
from src.backend.quality.engines.execution_engine import ExecutionContext  # ../engines/execution_engine
from src.backend.quality.integrations.great_expectations_adapter import GreatExpectationsAdapter  # ../integrations/great_expectations_adapter
from src.backend.quality.integrations.bigquery_adapter import BigQueryAdapter  # ../integrations/bigquery_adapter

# Initialize logger
logger = get_logger(__name__)

# Set default validation timeout
DEFAULT_VALIDATION_TIMEOUT = constants.DEFAULT_TIMEOUT_SECONDS


@retry(max_attempts=constants.DEFAULT_MAX_RETRY_ATTEMPTS)
def validate_column_existence(dataset: typing.Any, columns: list) -> dict:
    """Validates that specified columns exist in the dataset

    Args:
        dataset (Any): dataset
        columns (list): columns

    Returns:
        dict: Validation result with details about missing columns
    """
    try:
        # Check if dataset is pandas DataFrame or BigQuery table
        if isinstance(dataset, pandas.DataFrame):
            # For pandas: Use dataset.columns to check for column existence
            missing_columns = [col for col in columns if col not in dataset.columns]
            total_columns = len(columns)
            missing_count = len(missing_columns)
        elif isinstance(dataset, bigquery.table.Table):
            # For BigQuery: Query INFORMATION_SCHEMA.COLUMNS to check for column existence
            # This requires a BigQuery client and proper permissions
            client = bigquery.Client()
            query = f"""
                SELECT column_name
                FROM `{dataset.project}.{dataset.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{dataset.table_id}'
                AND column_name IN UNNEST(@columns)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("columns", bigquery.enums.SqlTypeNames.STRING, columns)
                ]
            )
            query_job = client.query(query, job_config=job_config)
            results = query_job.result()
            existing_columns = [row.column_name for row in results]
            missing_columns = [col for col in columns if col not in existing_columns]
            total_columns = len(columns)
            missing_count = len(missing_columns)
        else:
            raise TypeError("Unsupported dataset type. Must be pandas DataFrame or BigQuery table.")

        # Identify missing columns
        # Calculate percentage of missing columns
        missing_percentage = (missing_count / total_columns) * 100 if total_columns > 0 else 0

        # Return validation result with success status and details about missing columns
        details = {
            "missing_columns": missing_columns,
            "missing_count": missing_count,
            "total_columns": total_columns,
            "missing_percentage": missing_percentage
        }
        success = missing_count == 0
        return {"success": success, "details": details}
    except Exception as e:
        logger.error(f"Error validating column existence: {e}")
        return {"success": False, "details": {"error": str(e)}}


@retry(max_attempts=constants.DEFAULT_MAX_RETRY_ATTEMPTS)
def validate_column_types(dataset: typing.Any, column_types: dict) -> dict:
    """Validates that columns have the expected data types

    Args:
        dataset (Any): dataset
        column_types (dict): column_types

    Returns:
        dict: Validation result with details about type mismatches
    """
    try:
        # Check if dataset is pandas DataFrame or BigQuery table
        if isinstance(dataset, pandas.DataFrame):
            # For pandas: Use dataset.dtypes to check column types
            type_mismatches = {}
            for column, expected_type in column_types.items():
                actual_type = dataset[column].dtype
                if str(actual_type) != expected_type:
                    type_mismatches[column] = {"actual": str(actual_type), "expected": expected_type}
            total_columns = len(column_types)
            mismatch_count = len(type_mismatches)
        elif isinstance(dataset, bigquery.table.Table):
            # For BigQuery: Query INFORMATION_SCHEMA.COLUMNS to check column types
            client = bigquery.Client()
            query = f"""
                SELECT column_name, data_type
                FROM `{dataset.project}.{dataset.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{dataset.table_id}'
                AND column_name IN UNNEST(@columns)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("columns", bigquery.enums.SqlTypeNames.STRING, list(column_types.keys()))
                ]
            )
            query_job = client.query(query, job_config=job_config)
            results = query_job.result()
            type_mismatches = {}
            for row in results:
                column_name = row.column_name
                expected_type = column_types[column_name]
                actual_type = row.data_type
                if actual_type != expected_type:
                    type_mismatches[column_name] = {"actual": actual_type, "expected": expected_type}
            total_columns = len(column_types)
            mismatch_count = len(type_mismatches)
        else:
            raise TypeError("Unsupported dataset type. Must be pandas DataFrame or BigQuery table.")

        # Identify type mismatches
        # Calculate percentage of type mismatches
        mismatch_percentage = (mismatch_count / total_columns) * 100 if total_columns > 0 else 0

        # Return validation result with success status and details about type mismatches
        details = {
            "type_mismatches": type_mismatches,
            "mismatch_count": mismatch_count,
            "total_columns": total_columns,
            "mismatch_percentage": mismatch_percentage
        }
        success = mismatch_count == 0
        return {"success": success, "details": details}
    except Exception as e:
        logger.error(f"Error validating column types: {e}")
        return {"success": False, "details": {"error": str(e)}}


@retry(max_attempts=constants.DEFAULT_MAX_RETRY_ATTEMPTS)
def validate_schema_consistency(dataset: typing.Any, expected_schema: dict) -> dict:
    """Validates that the dataset schema matches an expected schema definition

    Args:
        dataset (Any): dataset
        expected_schema (dict): expected_schema

    Returns:
        dict: Validation result with details about schema inconsistencies
    """
    try:
        # Check if dataset is pandas DataFrame or BigQuery table
        if isinstance(dataset, pandas.DataFrame):
            # Extract actual schema from DataFrame
            actual_schema = {col: str(dtype) for col, dtype in dataset.dtypes.items()}
        elif isinstance(dataset, bigquery.table.Table):
            # Extract actual schema from BigQuery table
            client = bigquery.Client()
            table = client.get_table(dataset)
            actual_schema = {field.name: field.field_type for field in table.schema}
        else:
            raise TypeError("Unsupported dataset type. Must be pandas DataFrame or BigQuery table.")

        # Compare actual schema with expected schema
        missing_columns = [col for col in expected_schema if col not in actual_schema]
        extra_columns = [col for col in actual_schema if col not in expected_schema]
        type_mismatches = {}
        for col, expected_type in expected_schema.items():
            if col in actual_schema and actual_schema[col] != expected_type:
                type_mismatches[col] = {"actual": actual_schema[col], "expected": expected_type}

        # Calculate schema consistency score
        total_columns = len(expected_schema)
        inconsistencies = len(missing_columns) + len(extra_columns) + len(type_mismatches)
        consistency_score = (total_columns - inconsistencies) / total_columns if total_columns > 0 else 0

        # Return validation result with success status and details about inconsistencies
        details = {
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "type_mismatches": type_mismatches,
            "consistency_score": consistency_score
        }
        success = inconsistencies == 0
        return {"success": success, "details": details}
    except Exception as e:
        logger.error(f"Error validating schema consistency: {e}")
        return {"success": False, "details": {"error": str(e)}}


@retry(max_attempts=constants.DEFAULT_MAX_RETRY_ATTEMPTS)
def validate_primary_key(dataset: typing.Any, key_columns: list) -> dict:
    """Validates that specified columns form a unique primary key

    Args:
        dataset (Any): dataset
        key_columns (list): key_columns

    Returns:
        dict: Validation result with details about duplicate keys
    """
    try:
        # Check if dataset is pandas DataFrame or BigQuery table
        if isinstance(dataset, pandas.DataFrame):
            # For pandas: Use duplicated() to check for duplicate key values
            duplicates = dataset.duplicated(subset=key_columns, keep=False)
            duplicate_count = duplicates.sum()
            total_rows = len(dataset)
        elif isinstance(dataset, bigquery.table.Table):
            # For BigQuery: Use GROUP BY and HAVING COUNT(*) > 1 to find duplicates
            client = bigquery.Client()
            select_columns = ", ".join(key_columns)
            query = f"""
                SELECT {select_columns}, COUNT(*) as row_count
                FROM `{dataset.project}.{dataset.dataset_id}.{dataset.table_id}`
                GROUP BY {select_columns}
                HAVING COUNT(*) > 1
            """
            query_job = client.query(query)
            results = query_job.result()
            duplicate_count = sum(row.row_count for row in results)
            total_rows = dataset.num_rows
        else:
            raise TypeError("Unsupported dataset type. Must be pandas DataFrame or BigQuery table.")

        # Count duplicate key occurrences
        # Calculate percentage of duplicate keys
        duplicate_percentage = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0

        # Return validation result with success status and details about duplicate keys
        details = {
            "duplicate_count": duplicate_count,
            "total_rows": total_rows,
            "duplicate_percentage": duplicate_percentage
        }
        success = duplicate_count == 0
        return {"success": success, "details": details}
    except Exception as e:
        logger.error(f"Error validating primary key: {e}")
        return {"success": False, "details": {"error": str(e)}}


class SchemaValidator:
    """Validator class for schema-based data quality validations"""

    _ge_adapter: GreatExpectationsAdapter
    _bq_adapter: BigQueryAdapter
    _config: dict
    _schema_cache: dict

    def __init__(self, config: dict):
        """Initialize the schema validator with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}
        # Create GreatExpectationsAdapter for validation operations if enabled
        self._ge_adapter = GreatExpectationsAdapter(self._config) if self._config.get("use_great_expectations", True) else None
        # Create BigQueryAdapter for large dataset validation if needed
        self._bq_adapter = BigQueryAdapter(self._config) if self._config.get("use_bigquery", True) else None
        # Initialize schema cache dictionary for storing extracted schemas
        self._schema_cache = {}
        # Initialize validator properties
        logger.info("SchemaValidator initialized")

    def validate(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate schema rules against a dataset

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Filter rules to include only schema validation rules
        schema_rules = [rule for rule in rules if constants.ValidationRuleType(rule["rule_type"]) == constants.ValidationRuleType.SCHEMA]

        # Determine optimal validation approach based on dataset and rules
        if isinstance(dataset, pandas.DataFrame):
            # For small datasets or pandas dataframes, use in-memory validation
            results = self.validate_in_memory(dataset, schema_rules, context)
        elif isinstance(dataset, bigquery.table.Table):
            # For large datasets or BigQuery tables, use BigQuery-based validation
            results = self.validate_with_bigquery(dataset, schema_rules, context)
        elif self._ge_adapter:
            # For complex validations, use Great Expectations if available
            results = self.validate_with_great_expectations(dataset, schema_rules, context)
        else:
            raise ValueError("No suitable validation method available for this dataset type.")

        # Process and return validation results
        return results

    def validate_rule(self, dataset: typing.Any, rule: dict) -> ValidationResult:
        """Validate a single schema rule against a dataset

        Args:
            dataset (Any): dataset
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Verify rule is a schema validation rule
        if constants.ValidationRuleType(rule["rule_type"]) != constants.ValidationRuleType.SCHEMA:
            raise ValueError("This method is only for schema validation rules.")

        # Extract rule parameters and validation type
        rule_parameters = rule.get("parameters", {})
        validation_type = rule_parameters.get("subtype")

        # Call appropriate validation function based on rule type
        if validation_type == "column_existence":
            result = validate_column_existence(dataset, [rule_parameters.get("column_name")])
        elif validation_type == "column_type":
            result = validate_column_types(dataset, {rule_parameters.get("column_name"): rule_parameters.get("data_type")})
        elif validation_type == "schema_consistency":
            result = validate_schema_consistency(dataset, rule_parameters.get("expected_schema"))
        elif validation_type == "primary_key":
            result = validate_primary_key(dataset, rule_parameters.get("key_columns"))
        else:
            raise ValueError(f"Unsupported schema validation type: {validation_type}")

        # Return validation result
        return create_validation_result(rule, result["success"], result["details"])

    def validate_in_memory(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate schema rules using in-memory validation

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Convert dataset to pandas DataFrame if not already
        if not isinstance(dataset, pandas.DataFrame):
            dataset = pandas.DataFrame(dataset)

        # Initialize results list
        results = []

        # For each rule, call appropriate validation function
        for rule in rules:
            result = self.validate_rule(dataset, rule)
            results.append(result.to_dict())
            context.increment_stat("rules_executed", 1)

        # Return list of validation results
        return results

    def validate_with_bigquery(self, dataset_id: str, table_id: str, rules: list, context: ExecutionContext) -> list:
        """Validate schema rules using BigQuery

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Use BigQueryAdapter to validate rules against BigQuery table
        bq_adapter = BigQueryAdapter(self._config)
        results = bq_adapter.validate_rules(dataset_id, table_id, rules, context)

        # Process and return validation results
        return results

    def validate_with_great_expectations(self, dataset: typing.Any, rules: list, context: ExecutionContext) -> list:
        """Validate schema rules using Great Expectations

        Args:
            dataset (Any): dataset
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Use GreatExpectationsAdapter to validate rules against dataset
        results = self._ge_adapter.validate(dataset, rules, context)

        # Process and return validation results
        return results

    def extract_schema(self, dataset: typing.Any) -> dict:
        """Extract schema information from a dataset

        Args:
            dataset (Any): dataset

        Returns:
            dict: Schema information including columns and data types
        """
        # Check if dataset is pandas DataFrame or BigQuery table
        if isinstance(dataset, pandas.DataFrame):
            # For pandas: Extract column names and data types from DataFrame
            schema = {col: str(dtype) for col, dtype in dataset.dtypes.items()}
        elif isinstance(dataset, bigquery.table.Table):
            # For BigQuery: Query INFORMATION_SCHEMA.COLUMNS to get schema
            client = bigquery.Client()
            table = client.get_table(dataset)
            schema = {field.name: field.field_type for field in table.schema}
        else:
            raise TypeError("Unsupported dataset type. Must be pandas DataFrame or BigQuery table.")

        # Format schema information into standardized structure
        # Cache schema for future use
        # Return schema information
        return schema

    def map_rule_to_validation_function(self, rule: dict) -> typing.Callable:
        """Map a schema rule to the appropriate validation function

        Args:
            rule (dict): rule

        Returns:
            callable: Validation function for the rule
        """
        # Extract rule subtype from rule definition
        rule_subtype = rule.get("subtype")

        # Return appropriate validation function based on subtype
        if rule_subtype == "column_existence":
            return validate_column_existence
        elif rule_subtype == "column_type":
            return validate_column_types
        elif rule_subtype == "schema_consistency":
            return validate_schema_consistency
        elif rule_subtype == "primary_key":
            return validate_primary_key
        else:
            raise ValueError(f"Unsupported schema rule subtype: {rule_subtype}")

    def close(self) -> None:
        """Close the validator and release resources"""
        # Close GreatExpectationsAdapter if it exists
        if self._ge_adapter and hasattr(self._ge_adapter, "close") and callable(self._ge_adapter.close):
            self._ge_adapter.close()

        # Close BigQueryAdapter if it exists
        if self._bq_adapter and hasattr(self._bq_adapter, "close") and callable(self._bq_adapter.close):
            self._bq_adapter.close()

        # Clear schema cache
        self._schema_cache.clear()

        # Release any other resources
        logger.info("SchemaValidator closed")


# Standalone functions for schema validation
def validate_column_existence(dataset: typing.Any, columns: list) -> dict:
    """Validates that specified columns exist in the dataset"""
    return validate_column_existence(dataset, columns)


def validate_column_types(dataset: typing.Any, column_types: dict) -> dict:
    """Validates that columns have the expected data types"""
    return validate_column_types(dataset, column_types)


def validate_schema_consistency(dataset: typing.Any, expected_schema: dict) -> dict:
    """Validates that the dataset schema matches an expected schema definition"""
    return validate_schema_consistency(dataset, expected_schema)


def validate_primary_key(dataset: typing.Any, key_columns: list) -> dict:
    """Validates that specified columns form a unique primary key"""
    return validate_primary_key(dataset, key_columns)