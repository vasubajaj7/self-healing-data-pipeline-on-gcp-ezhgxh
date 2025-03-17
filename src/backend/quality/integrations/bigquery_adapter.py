# src/backend/quality/integrations/bigquery_adapter.py
"""Adapter for integrating BigQuery with the data quality validation framework.
Provides functionality to execute validation rules directly against BigQuery tables,
optimizing performance for large datasets by pushing validation logic to BigQuery's query engine.
"""

import typing
import time
import json
import pandas  # version 2.0.x
from google.cloud import bigquery  # version 3.11.0+

from src.backend import constants  # src/backend/constants.py
from src.backend.config import get_config  # src/backend/config.py
from src.backend.utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from src.backend.utils.retry.retry_decorator import retry  # src/backend/utils/retry/retry_decorator.py
from src.backend.utils.storage import bigquery_client  # src/backend/utils/storage/bigquery_client.py
from src.backend.quality.engines import validation_engine  # ../engines/validation_engine.py
from src.backend.quality.engines import execution_engine  # ../engines/execution_engine.py

# Initialize logger
logger = get_logger(__name__)

# Set default validation timeout
DEFAULT_VALIDATION_TIMEOUT = constants.DEFAULT_TIMEOUT_SECONDS

# Default query timeout
DEFAULT_QUERY_TIMEOUT = 300


def generate_validation_query(rule: dict, dataset_id: str, table_id: str) -> typing.Tuple[str, typing.Dict]:
    """Generates a BigQuery SQL query for a validation rule

    Args:
        rule (dict): rule
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        tuple: (query_string, query_parameters)
    """
    # Determine the rule type and validation logic
    rule_type = constants.ValidationRuleType(rule['type'])

    # Generate appropriate SQL based on rule type
    if rule_type == constants.ValidationRuleType.SCHEMA:
        query_string, query_parameters = generate_schema_validation_query(rule, dataset_id, table_id)
    elif rule_type == constants.ValidationRuleType.CONTENT:
        query_string, query_parameters = generate_content_validation_query(rule, dataset_id, table_id)
    elif rule_type == constants.ValidationRuleType.RELATIONSHIP:
        query_string, query_parameters = generate_relationship_validation_query(rule, dataset_id, table_id)
    elif rule_type == constants.ValidationRuleType.STATISTICAL:
        query_string, query_parameters = generate_statistical_validation_query(rule, dataset_id, table_id)
    else:
        raise ValueError(f"Unsupported rule type: {rule_type}")

    # Format table reference as fully qualified name
    fully_qualified_table_name = f"`{dataset_id}.{table_id}`"
    query_string = query_string.replace("{{table}}", fully_qualified_table_name)

    # Return the generated query string and parameters
    return query_string, {}


def parse_validation_results(query_results: pandas.DataFrame, rule: dict) -> validation_engine.ValidationResult:
    """Parses BigQuery query results into validation results

    Args:
        query_results (pandas.DataFrame): query_results
        rule (dict): rule

    Returns:
        ValidationResult: Validation result object
    """
    # Extract validation metrics from query results
    if query_results.empty:
        success = True
        details = {"message": "No data found, validation passed"}
    else:
        # Determine validation success based on rule criteria
        success = query_results['is_valid'].iloc[0]
        details = {"rows_invalid": query_results['rows_invalid'].iloc[0]}

    # Create ValidationResult object with rule information
    validation_result = validation_engine.ValidationResult(rule['rule_id'], constants.ValidationRuleType(rule['type']), constants.QualityDimension(rule['dimension']))

    # Set success status based on validation outcome
    validation_result.set_status(success)

    # Add detailed results as validation details
    validation_result.set_details(details)

    # Return the ValidationResult object
    return validation_result


def generate_schema_validation_query(rule: dict, dataset_id: str, table_id: str) -> typing.Tuple[str, typing.Dict]:
    """Generates a BigQuery SQL query for schema validation

    Args:
        rule (dict): rule
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        tuple: (query_string, query_parameters)
    """
    # Extract schema validation subtype from rule
    subtype = rule['parameters'].get('subtype', 'column_existence')

    if subtype == 'column_existence':
        # For column existence, generate query using INFORMATION_SCHEMA.COLUMNS
        column_name = rule['parameters']['column_name']
        query = f"""
            SELECT EXISTS(
                SELECT 1
                FROM `{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_id}'
                AND column_name = '{column_name}'
            ) AS is_valid
        """
    elif subtype == 'column_type':
        # For column type validation, generate query to check column data types
        column_name = rule['parameters']['column_name']
        data_type = rule['parameters']['data_type']
        query = f"""
            SELECT EXISTS(
                SELECT 1
                FROM `{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table_id}'
                AND column_name = '{column_name}'
                AND data_type = '{data_type}'
            ) AS is_valid
        """
    elif subtype == 'primary_key':
        column_name = rule['parameters']['column_name']
        query = f"""
            SELECT COUNT(*) > COUNT(DISTINCT {column_name}) AS is_valid, COUNT(*) - COUNT(DISTINCT {column_name}) AS rows_invalid
            FROM `{{table}}`
        """
    elif subtype == 'not_null':
        column_name = rule['parameters']['column_name']
        query = f"""
            SELECT COUNTIF({column_name} IS NULL) = 0 AS is_valid, COUNTIF({column_name} IS NULL) AS rows_invalid
            FROM `{{table}}`
        """
    else:
        raise ValueError(f"Unsupported schema validation subtype: {subtype}")

    # Return the generated query string and parameters
    return query, {}


def generate_content_validation_query(rule: dict, dataset_id: str, table_id: str) -> typing.Tuple[str, typing.Dict]:
    """Generates a BigQuery SQL query for content validation

    Args:
        rule (dict): rule
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        tuple: (query_string, query_parameters)
    """
    # Extract content validation subtype from rule
    subtype = rule['parameters'].get('subtype', 'null_check')
    column_name = rule['parameters']['column_name']

    if subtype == 'null_check':
        # For null checks, generate query to count null values
        query = f"""
            SELECT COUNTIF({column_name} IS NULL) = 0 AS is_valid, COUNTIF({column_name} IS NULL) AS rows_invalid
            FROM `{{table}}`
        """
    elif subtype == 'value_range':
        # For value range validation, generate query with range conditions
        min_value = rule['parameters']['min_value']
        max_value = rule['parameters']['max_value']
        query = f"""
            SELECT COUNTIF({column_name} < {min_value} OR {column_name} > {max_value}) = 0 AS is_valid, COUNTIF({column_name} < {min_value} OR {column_name} > {max_value}) AS rows_invalid
            FROM `{{table}}`
        """
    elif subtype == 'pattern_matching':
        # For pattern matching, generate query with REGEXP_CONTAINS
        pattern = rule['parameters']['pattern']
        query = f"""
            SELECT COUNTIF(NOT REGEXP_CONTAINS({column_name}, r'{pattern}')) = 0 AS is_valid, COUNTIF(NOT REGEXP_CONTAINS({column_name}, r'{pattern}')) AS rows_invalid
            FROM `{{table}}`
        """
    elif subtype == 'categorical_validation':
        # For categorical validation, generate query with IN operator
        categories = rule['parameters']['categories']
        categories_str = ", ".join([f"'{c}'" for c in categories])
        query = f"""
            SELECT COUNTIF({column_name} NOT IN ({categories_str})) = 0 AS is_valid, COUNTIF({column_name} NOT IN ({categories_str})) AS rows_invalid
            FROM `{{table}}`
        """
    else:
        raise ValueError(f"Unsupported content validation subtype: {subtype}")

    # Return the generated query string and parameters
    return query, {}


def generate_relationship_validation_query(rule: dict, dataset_id: str, table_id: str) -> typing.Tuple[str, typing.Dict]:
    """Generates a BigQuery SQL query for relationship validation

    Args:
        rule (dict): rule
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        tuple: (query_string, query_parameters)
    """
    # Extract relationship validation subtype from rule
    subtype = rule['parameters'].get('subtype', 'referential_integrity')

    if subtype == 'referential_integrity':
        # For referential integrity, generate query with LEFT JOIN and NULL check
        source_column = rule['parameters']['source_column']
        target_dataset = rule['parameters']['target_dataset']
        target_table = rule['parameters']['target_table']
        target_column = rule['parameters']['target_column']
        query = f"""
            SELECT COUNT(t1.{source_column}) = COUNT(t2.{target_column}) AS is_valid, COUNT(t1.{source_column}) - COUNT(t2.{target_column}) AS rows_invalid
            FROM `{{table}}` t1
            LEFT JOIN `{target_dataset}.{target_table}` t2 ON t1.{source_column} = t2.{target_column}
        """
    elif subtype == 'cardinality_check':
        source_column = rule['parameters']['source_column']
        min_count = rule['parameters']['min_count']
        max_count = rule['parameters']['max_count']
        query = f"""
            SELECT COUNT(*) >= {min_count} AND COUNT(*) <= {max_count} AS is_valid, IF(COUNT(*) < {min_count}, {min_count} - COUNT(*), IF(COUNT(*) > {max_count}, COUNT(*) - {max_count}, 0)) AS rows_invalid
            FROM (
                SELECT {source_column}
                FROM `{{table}}`
                GROUP BY {source_column}
                HAVING COUNT(*) > 1
            )
        """
    else:
        raise ValueError(f"Unsupported relationship validation subtype: {subtype}")

    # Return the generated query string and parameters
    return query, {}


def generate_statistical_validation_query(rule: dict, dataset_id: str, table_id: str) -> typing.Tuple[str, typing.Dict]:
    """Generates a BigQuery SQL query for statistical validation

    Args:
        rule (dict): rule
        dataset_id (str): dataset_id
        table_id (str): table_id

    Returns:
        tuple: (query_string, query_parameters)
    """
    # Extract statistical validation subtype from rule
    subtype = rule['parameters'].get('subtype', 'distribution_check')
    column_name = rule['parameters']['column_name']

    if subtype == 'distribution_check':
        # For distribution checks, generate query with statistical functions
        mean = rule['parameters']['mean']
        std_dev = rule['parameters']['std_dev']
        query = f"""
            SELECT STDDEV({column_name}) BETWEEN {mean} - {std_dev} AND {mean} + {std_dev} AS is_valid, ABS(STDDEV({column_name}) - {mean}) AS rows_invalid
            FROM `{{table}}`
        """
    elif subtype == 'outlier_detection':
        # For outlier detection, generate query with z-score or IQR calculation
        threshold = rule['parameters']['threshold']
        query = f"""
            SELECT COUNTIF(ABS(({column_name} - (SELECT AVG({column_name}) FROM `{{table}}`)) / (SELECT STDDEV({column_name}) FROM `{{table}}`)) > {threshold}) = 0 AS is_valid, COUNTIF(ABS(({column_name} - (SELECT AVG({column_name}) FROM `{{table}}`)) / (SELECT STDDEV({column_name}) FROM `{{table}}`)) > {threshold}) AS rows_invalid
            FROM `{{table}}`
        """
    else:
        raise ValueError(f"Unsupported statistical validation subtype: {subtype}")

    # Return the generated query string and parameters
    return query, {}


class BigQueryAdapter:
    """Adapter class for executing data quality validations directly in BigQuery"""

    def __init__(self, config: dict):
        """Initialize the BigQuery adapter with configuration

        Args:
            config (dict): config
        """
        # Initialize configuration with defaults and override with provided config
        self._config = config or {}

        # Create BigQueryClient for executing queries
        self._bq_client = bigquery_client.BigQueryClient()

        # Initialize query cache dictionary
        self._query_cache = {}

        # Initialize schema cache dictionary
        self._schema_cache = {}

        # Log successful initialization
        logger.info("BigQueryAdapter initialized")

    def validate_rules(self, dataset_id: str, table_id: str, rules: list, context: execution_engine.ExecutionContext) -> list:
        """Validate a set of rules against a BigQuery table

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id
            rules (list): rules
            context (ExecutionContext): context

        Returns:
            list: List of validation results
        """
        # Verify BigQuery table exists
        if not self.table_exists(dataset_id, table_id):
            raise ValueError(f"BigQuery table {dataset_id}.{table_id} does not exist")

        # Group rules by validation type for efficient execution
        grouped_rules = self.group_rules_by_type(rules)

        # Initialize results list
        results = []

        # For each rule, generate and execute validation query
        for rule_type, rules_for_type in grouped_rules.items():
            for rule in rules_for_type:
                # Generate and execute validation query
                result = self.validate_rule(dataset_id, table_id, rule)

                # Parse query results into validation results
                results.append(result.to_dict())

                # Update execution context with statistics
                context.increment_stat("rules_executed", 1)

        # Return list of validation results
        return results

    def validate_rule(self, dataset_id: str, table_id: str, rule: dict) -> validation_engine.ValidationResult:
        """Validate a single rule against a BigQuery table

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id
            rule (dict): rule

        Returns:
            ValidationResult: Validation result for the rule
        """
        # Generate validation query for the rule
        query, query_parameters = generate_validation_query(rule, dataset_id, table_id)

        # Execute query against BigQuery
        query_results = self.execute_validation_query(query, query_parameters, DEFAULT_VALIDATION_TIMEOUT)

        # Parse query results into validation result
        validation_result = parse_validation_results(query_results, rule)

        # Return validation result
        return validation_result

    @retry(max_attempts=constants.MAX_RETRY_ATTEMPTS)
    def execute_validation_query(self, query: str, parameters: dict, timeout: int) -> pandas.DataFrame:
        """Execute a validation query against BigQuery

        Args:
            query (str): query
            parameters (dict): parameters
            timeout (int): timeout

        Returns:
            pandas.DataFrame: Query results as DataFrame
        """
        start_time = time.time()

        # Format query parameters if provided
        if parameters:
            formatted_params = bigquery_client.format_query_parameters(parameters)
        else:
            formatted_params = []

        # Execute query using BigQueryClient
        query_results = self._bq_client.run_query(query, query_parameters=formatted_params, timeout_seconds=timeout)

        # Convert query results to pandas DataFrame
        df = query_results.to_dataframe()

        end_time = time.time()
        execution_time = end_time - start_time

        # Log query execution details
        logger.debug(f"Executed query in {execution_time:.2f}s: {query}")

        # Return query results as DataFrame
        return df

    def get_table_schema(self, dataset_id: str, table_id: str) -> dict:
        """Retrieve the schema of a BigQuery table

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id

        Returns:
            dict: Table schema information
        """
        # Check if schema is in cache
        cache_key = f"{dataset_id}.{table_id}"
        if cache_key in self._schema_cache:
            # If cached, return cached schema
            logger.debug(f"Using cached schema for {cache_key}")
            return self._schema_cache[cache_key]

        # If not cached, retrieve schema from BigQuery
        table = self._bq_client.get_table(dataset_id, table_id)

        # Format schema information into standardized structure
        schema_info = {
            "fields": [
                {"name": field.name, "type": field.field_type, "mode": field.mode}
                for field in table.schema
            ]
        }

        # Cache schema for future use
        self._schema_cache[cache_key] = schema_info
        logger.debug(f"Cached schema for {cache_key}")

        # Return schema information
        return schema_info

    def group_rules_by_type(self, rules: list) -> dict:
        """Group validation rules by their rule type

        Args:
            rules (list): rules

        Returns:
            dict: Rules grouped by ValidationRuleType
        """
        # Initialize empty dictionary with ValidationRuleType keys
        grouped_rules = {rule_type: [] for rule_type in constants.ValidationRuleType}

        # Iterate through rules
        for rule in rules:
            # Extract rule_type from each rule
            rule_type = constants.ValidationRuleType(rule['type'])

            # Add rule to appropriate group in dictionary
            grouped_rules[rule_type].append(rule)

        # Return grouped rules dictionary
        return grouped_rules

    def get_validation_query_generator(self, rule_type: constants.ValidationRuleType) -> typing.Callable:
        """Get the appropriate query generator function for a rule type

        Args:
            rule_type (ValidationRuleType): rule_type

        Returns:
            callable: Query generator function
        """
        # Map ValidationRuleType to corresponding query generator function
        if rule_type == constants.ValidationRuleType.SCHEMA:
            return generate_schema_validation_query
        elif rule_type == constants.ValidationRuleType.CONTENT:
            return generate_content_validation_query
        elif rule_type == constants.ValidationRuleType.RELATIONSHIP:
            return generate_relationship_validation_query
        elif rule_type == constants.ValidationRuleType.STATISTICAL:
            return generate_statistical_validation_query
        else:
            raise ValueError(f"Unsupported rule type: {rule_type}")

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a BigQuery table exists

        Args:
            dataset_id (str): dataset_id
            table_id (str): table_id

        Returns:
            bool: True if table exists, False otherwise
        """
        # Use BigQueryClient to check if table exists
        return self._bq_client.table_exists(dataset_id, table_id)

    def close(self) -> None:
        """Close the adapter and release resources"""
        # Clear query cache
        self._query_cache.clear()

        # Clear schema cache
        self._schema_cache.clear()

        # Release any other resources
        logger.info("BigQueryAdapter closed")