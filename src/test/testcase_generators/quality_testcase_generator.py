"""
Specialized test case generator for data quality validation testing.

This module provides functionality for creating test cases with various quality issues,
validation rules, and expected outcomes to facilitate thorough testing of the
data quality validation framework.
"""

import os
import json
import random
import uuid
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import numpy as np
from faker import Faker

from src.test.testcase_generators.schema_data_generator import TestCaseGenerator, SchemaDataTestCase, generate_schema_data_pair
from src.backend.constants import ValidationRuleType, QualityDimension, FileFormat
from src.test.utils.test_helpers import create_test_dataframe, generate_unique_id

# Constants
QUALITY_TEST_CASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'quality')
DEFAULT_NUM_VARIATIONS = 5
DEFAULT_DATA_SIZE = 100
DEFAULT_FILE_FORMAT = FileFormat.JSON

# Types of data quality issues
SCHEMA_ISSUE_TYPES = ["missing_column", "extra_column", "type_mismatch", "constraint_violation"]
CONTENT_ISSUE_TYPES = ["null_values", "out_of_range", "pattern_mismatch", "invalid_categorical", "duplicate_values"]
RELATIONSHIP_ISSUE_TYPES = ["referential_integrity", "cardinality_violation", "hierarchical_violation"]
STATISTICAL_ISSUE_TYPES = ["outliers", "distribution_shift", "correlation_change", "trend_violation"]


def generate_validation_rule(rule_type: ValidationRuleType, subtype: str, parameters: Dict) -> Dict:
    """
    Generates a validation rule definition based on rule type and parameters.
    
    Args:
        rule_type: Type of validation rule
        subtype: Subtype or specific rule variation
        parameters: Additional parameters for the rule
        
    Returns:
        Validation rule definition as a dictionary
    """
    # Generate a unique rule ID
    rule_id = f"rule-{generate_unique_id()}"
    
    # Create base rule dictionary
    rule = {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "subtype": subtype,
    }
    
    # Add quality dimension based on rule type
    if rule_type == ValidationRuleType.SCHEMA:
        rule["dimension"] = QualityDimension.VALIDITY
    elif rule_type == ValidationRuleType.CONTENT:
        if subtype == "null_values":
            rule["dimension"] = QualityDimension.COMPLETENESS
        elif subtype in ["out_of_range", "pattern_mismatch", "invalid_categorical"]:
            rule["dimension"] = QualityDimension.VALIDITY
        elif subtype == "duplicate_values":
            rule["dimension"] = QualityDimension.UNIQUENESS
    elif rule_type == ValidationRuleType.RELATIONSHIP:
        rule["dimension"] = QualityDimension.CONSISTENCY
    elif rule_type == ValidationRuleType.STATISTICAL:
        rule["dimension"] = QualityDimension.ACCURACY
    
    # Add parameters to rule
    rule["parameters"] = parameters
    
    return rule


def inject_schema_issues(schema: Dict, data: pd.DataFrame, issues: List) -> Tuple[Dict, pd.DataFrame]:
    """
    Injects schema-related issues into a dataset.
    
    Args:
        schema: Original schema definition
        data: Original data as DataFrame
        issues: List of issues to inject
        
    Returns:
        Tuple of (modified_schema, modified_data) with injected issues
    """
    # Create copies to avoid modifying originals
    modified_schema = schema.copy()
    modified_data = data.copy()
    
    for issue in issues:
        issue_type = issue.get("type")
        config = issue.get("config", {})
        
        if issue_type == "missing_column":
            # Remove a column from the schema
            column_name = config.get("column_name", random.choice([f["name"] for f in modified_schema["fields"]]))
            modified_schema["fields"] = [f for f in modified_schema["fields"] if f["name"] != column_name]
            
        elif issue_type == "extra_column":
            # Add an unexpected column to schema
            column_name = config.get("column_name", f"unexpected_column_{random.randint(1, 100)}")
            column_type = config.get("column_type", "string")
            modified_schema["fields"].append({
                "name": column_name,
                "type": column_type,
                "unexpected": True
            })
            
        elif issue_type == "type_mismatch":
            # Change column type in schema
            column_name = config.get("column_name", random.choice([f["name"] for f in modified_schema["fields"]]))
            original_type = next((f["type"] for f in modified_schema["fields"] if f["name"] == column_name), None)
            
            if original_type:
                # Choose a different type
                new_type = config.get("new_type", None)
                if not new_type:
                    types = ["string", "integer", "float", "boolean", "timestamp", "date"]
                    types.remove(original_type)
                    new_type = random.choice(types)
                
                # Update the type in schema
                for field in modified_schema["fields"]:
                    if field["name"] == column_name:
                        field["type"] = new_type
                        break
        
        elif issue_type == "constraint_violation":
            # Modify constraints in schema
            column_name = config.get("column_name", random.choice([f["name"] for f in modified_schema["fields"]]))
            constraint_type = config.get("constraint_type", "required")
            constraint_value = config.get("constraint_value", True)
            
            for field in modified_schema["fields"]:
                if field["name"] == column_name:
                    field[constraint_type] = constraint_value
                    break
    
    return modified_schema, modified_data


def inject_content_issues(data: pd.DataFrame, issues: List) -> pd.DataFrame:
    """
    Injects content-related issues into a dataset.
    
    Args:
        data: Original data as DataFrame
        issues: List of issues to inject
        
    Returns:
        Modified data with content issues
    """
    # Create a copy to avoid modifying original
    modified_data = data.copy()
    
    for issue in issues:
        issue_type = issue.get("type")
        config = issue.get("config", {})
        
        if issue_type == "null_values":
            # Replace values with None
            columns = config.get("columns", [random.choice(modified_data.columns)])
            percentage = config.get("percentage", random.uniform(0.05, 0.2))
            
            for column in columns:
                if column in modified_data.columns:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    modified_data.loc[indices, column] = None
        
        elif issue_type == "out_of_range":
            # Replace values with out-of-range values
            columns = config.get("columns", [])
            
            # If no columns specified, choose numeric ones
            if not columns:
                numeric_cols = modified_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    columns = [random.choice(numeric_cols)]
            
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            multiplier = config.get("multiplier", random.uniform(1.5, 10))
            
            for column in columns:
                if column in modified_data.columns:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    if modified_data[column].dtype in [np.float64, np.int64]:
                        # For numeric columns, multiply by a factor to create outliers
                        modified_data.loc[indices, column] = modified_data.loc[indices, column] * multiplier
        
        elif issue_type == "pattern_mismatch":
            # Replace values with pattern-violating values
            columns = config.get("columns", [])
            
            # If no columns specified, choose string ones
            if not columns:
                string_cols = modified_data.select_dtypes(include=['object']).columns
                if len(string_cols) > 0:
                    columns = [random.choice(string_cols)]
            
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            invalid_pattern = config.get("invalid_pattern", "###INVALID###")
            
            for column in columns:
                if column in modified_data.columns:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    modified_data.loc[indices, column] = invalid_pattern
        
        elif issue_type == "invalid_categorical":
            # Replace values with invalid categories
            columns = config.get("columns", [])
            
            # If no columns specified, try to find categorical ones
            if not columns:
                cat_cols = modified_data.select_dtypes(include=['category']).columns
                if len(cat_cols) > 0:
                    columns = [random.choice(cat_cols)]
                else:
                    # Try to find string columns with few unique values
                    str_cols = modified_data.select_dtypes(include=['object']).columns
                    cat_candidates = []
                    for col in str_cols:
                        if len(modified_data[col].unique()) < len(modified_data) * 0.2:
                            cat_candidates.append(col)
                    
                    if cat_candidates:
                        columns = [random.choice(cat_candidates)]
            
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            invalid_values = config.get("invalid_values", ["INVALID_CATEGORY", "UNKNOWN_CATEGORY"])
            
            for column in columns:
                if column in modified_data.columns:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    modified_data.loc[indices, column] = random.choice(invalid_values)
        
        elif issue_type == "duplicate_values":
            # Create duplicate values in the data
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            columns = config.get("columns", modified_data.columns.tolist())
            
            # Select some rows to duplicate
            num_duplicates = int(len(modified_data) * percentage)
            if num_duplicates > 0:
                indices = random.sample(range(len(modified_data)), min(num_duplicates, len(modified_data)))
                
                # Create duplicates
                duplicates = modified_data.iloc[indices].copy()
                modified_data = pd.concat([modified_data, duplicates], ignore_index=True)
    
    return modified_data


def inject_relationship_issues(data: pd.DataFrame, reference_data: pd.DataFrame, issues: List) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Injects relationship-related issues into a dataset.
    
    Args:
        data: Original data as DataFrame
        reference_data: Reference data for relationships
        issues: List of issues to inject
        
    Returns:
        Tuple of (modified_data, modified_reference_data) with relationship issues
    """
    # Create copies to avoid modifying originals
    modified_data = data.copy()
    modified_reference_data = reference_data.copy()
    
    for issue in issues:
        issue_type = issue.get("type")
        config = issue.get("config", {})
        
        if issue_type == "referential_integrity":
            # Modify keys to break references
            source_column = config.get("source_column", None)
            target_column = config.get("target_column", None)
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            
            # If columns not specified, try to find key columns
            if not source_column or not target_column:
                # In a real implementation, we'd analyze the data to find likely key relationships
                # For this generator, we'll just pick columns if not specified
                if not source_column and len(modified_data.columns) > 0:
                    source_column = random.choice(modified_data.columns)
                
                if not target_column and len(modified_reference_data.columns) > 0:
                    target_column = random.choice(modified_reference_data.columns)
            
            if source_column and source_column in modified_data.columns:
                # Replace some values with values not in the reference data
                indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                
                if target_column and target_column in modified_reference_data.columns:
                    # Generate values not in reference data
                    ref_values = set(modified_reference_data[target_column].unique())
                    
                    for idx in indices:
                        current_value = modified_data.loc[idx, source_column]
                        
                        # Generate a value not in the reference set
                        if isinstance(current_value, (int, float, np.number)):
                            # For numeric values, add a large offset
                            new_value = current_value + 1000000
                        elif isinstance(current_value, str):
                            # For strings, prepend "INVALID_"
                            new_value = f"INVALID_{current_value}"
                        else:
                            # For other types, use a generic invalid value
                            new_value = "INVALID_REFERENCE"
                        
                        # Make sure the new value is not in the reference data
                        while new_value in ref_values:
                            if isinstance(new_value, (int, float, np.number)):
                                new_value += 1
                            elif isinstance(new_value, str):
                                new_value += "_"
                            
                        modified_data.loc[idx, source_column] = new_value
        
        elif issue_type == "cardinality_violation":
            # Modify data to violate cardinality constraints
            source_column = config.get("source_column", None)
            target_column = config.get("target_column", None)
            expected_cardinality = config.get("expected_cardinality", "many_to_one")
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            
            # If columns not specified, select some
            if not source_column and len(modified_data.columns) > 0:
                source_column = random.choice(modified_data.columns)
            
            if not target_column and len(modified_reference_data.columns) > 0:
                target_column = random.choice(modified_reference_data.columns)
            
            if source_column and source_column in modified_data.columns:
                indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                
                if expected_cardinality == "many_to_one":
                    # For many-to-one, create duplicates on the "one" side
                    if len(indices) > 1:
                        # Pick a reference value to duplicate
                        ref_value = modified_data.loc[indices[0], source_column]
                        
                        # Assign this value to multiple rows
                        for idx in indices[1:]:
                            modified_data.loc[idx, source_column] = ref_value
                
                elif expected_cardinality == "one_to_one":
                    # For one-to-one, create duplicates which violates the constraint
                    if len(indices) > 1 and target_column and target_column in modified_reference_data.columns:
                        # Pick a reference value to duplicate
                        ref_value = modified_reference_data[target_column].iloc[0]
                        
                        # Assign this value to multiple rows
                        for idx in indices:
                            modified_data.loc[idx, source_column] = ref_value
        
        elif issue_type == "hierarchical_violation":
            # Modify hierarchical relationships
            parent_column = config.get("parent_column", None)
            child_column = config.get("child_column", None)
            percentage = config.get("percentage", random.uniform(0.05, 0.15))
            
            # If columns not specified, select some
            if not parent_column and len(modified_data.columns) > 0:
                parent_column = random.choice(modified_data.columns)
            
            if not child_column and len(modified_data.columns) > 0:
                cols = [c for c in modified_data.columns if c != parent_column]
                if cols:
                    child_column = random.choice(cols)
            
            if parent_column and child_column and parent_column in modified_data.columns and child_column in modified_data.columns:
                indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                
                # Create circular references or other hierarchical violations
                for idx in indices:
                    # Create a circular reference
                    parent_val = modified_data.loc[idx, parent_column]
                    modified_data.loc[idx, child_column] = parent_val
    
    return modified_data, modified_reference_data


def inject_statistical_issues(data: pd.DataFrame, issues: List) -> pd.DataFrame:
    """
    Injects statistical issues into a dataset.
    
    Args:
        data: Original data as DataFrame
        issues: List of issues to inject
        
    Returns:
        Modified data with statistical issues
    """
    # Create a copy to avoid modifying original
    modified_data = data.copy()
    
    for issue in issues:
        issue_type = issue.get("type")
        config = issue.get("config", {})
        
        if issue_type == "outliers":
            # Inject outlier values
            columns = config.get("columns", [])
            
            # If no columns specified, choose numeric ones
            if not columns:
                numeric_cols = modified_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    columns = [random.choice(numeric_cols)]
            
            percentage = config.get("percentage", random.uniform(0.01, 0.05))
            outlier_factor = config.get("outlier_factor", random.uniform(5, 20))
            
            for column in columns:
                if column in modified_data.columns and modified_data[column].dtype in [np.float64, np.int64]:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    
                    # Compute column statistics
                    col_mean = modified_data[column].mean()
                    col_std = modified_data[column].std()
                    
                    if col_std > 0:
                        # Generate outliers
                        for idx in indices:
                            # Randomly assign high or low outlier
                            if random.random() > 0.5:
                                # High outlier
                                modified_data.loc[idx, column] = col_mean + (col_std * outlier_factor)
                            else:
                                # Low outlier
                                modified_data.loc[idx, column] = col_mean - (col_std * outlier_factor)
        
        elif issue_type == "distribution_shift":
            # Modify distribution of values
            columns = config.get("columns", [])
            
            # If no columns specified, choose numeric ones
            if not columns:
                numeric_cols = modified_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    columns = [random.choice(numeric_cols)]
            
            percentage = config.get("percentage", random.uniform(0.3, 0.7))
            shift_factor = config.get("shift_factor", random.uniform(1.5, 3))
            
            for column in columns:
                if column in modified_data.columns and modified_data[column].dtype in [np.float64, np.int64]:
                    indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                    
                    # Apply multiplicative shift
                    modified_data.loc[indices, column] = modified_data.loc[indices, column] * shift_factor
        
        elif issue_type == "correlation_change":
            # Modify correlation between columns
            column_pairs = config.get("column_pairs", [])
            
            # If no column pairs specified, try to find correlated numeric columns
            if not column_pairs:
                numeric_cols = modified_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) >= 2:
                    col1 = random.choice(numeric_cols)
                    remaining_cols = [c for c in numeric_cols if c != col1]
                    col2 = random.choice(remaining_cols)
                    column_pairs = [(col1, col2)]
            
            percentage = config.get("percentage", random.uniform(0.3, 0.7))
            
            for col1, col2 in column_pairs:
                if col1 in modified_data.columns and col2 in modified_data.columns:
                    if modified_data[col1].dtype in [np.float64, np.int64] and modified_data[col2].dtype in [np.float64, np.int64]:
                        indices = random.sample(range(len(modified_data)), int(len(modified_data) * percentage))
                        
                        # Modify one column to break correlation
                        # Calculate current correlation
                        try:
                            current_corr = modified_data[col1].corr(modified_data[col2])
                            
                            # If correlation is positive, create negative correlation for the subset
                            if current_corr > 0:
                                modified_data.loc[indices, col2] = -1 * modified_data.loc[indices, col1]
                            else:
                                modified_data.loc[indices, col2] = modified_data.loc[indices, col1]
                        except:
                            # If correlation calculation fails, just randomize the values
                            col2_mean = modified_data[col2].mean()
                            col2_std = modified_data[col2].std()
                            modified_data.loc[indices, col2] = np.random.normal(col2_mean, col2_std, size=len(indices))
        
        elif issue_type == "trend_violation":
            # Modify time series trend
            time_column = config.get("time_column", None)
            value_column = config.get("value_column", None)
            
            # Try to identify time and value columns if not specified
            if not time_column:
                # Look for datetime columns
                datetime_cols = modified_data.select_dtypes(include=['datetime64']).columns
                if len(datetime_cols) > 0:
                    time_column = datetime_cols[0]
            
            if not value_column:
                # Look for numeric columns
                numeric_cols = modified_data.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    value_column = numeric_cols[0]
            
            percentage = config.get("percentage", random.uniform(0.1, 0.3))
            trend_factor = config.get("trend_factor", random.uniform(1.5, 3))
            
            if time_column and value_column and time_column in modified_data.columns and value_column in modified_data.columns:
                # Sort by time column
                modified_data = modified_data.sort_values(by=time_column)
                
                # Identify trend violation segment
                segment_size = int(len(modified_data) * percentage)
                segment_start = random.randint(0, len(modified_data) - segment_size - 1)
                segment_end = segment_start + segment_size
                
                # Modify values to create trend violation
                if modified_data[value_column].dtype in [np.float64, np.int64]:
                    # Introduce a sudden jump or drop
                    if random.random() > 0.5:
                        # Jump
                        modified_data.loc[segment_start:segment_end, value_column] = modified_data.loc[segment_start:segment_end, value_column] * trend_factor
                    else:
                        # Drop
                        modified_data.loc[segment_start:segment_end, value_column] = modified_data.loc[segment_start:segment_end, value_column] / trend_factor
    
    return modified_data


def generate_expected_validation_results(validation_rules: List, issues: List) -> List:
    """
    Generates expected validation results based on injected issues.
    
    Args:
        validation_rules: List of validation rules
        issues: List of injected issues
        
    Returns:
        Expected validation results
    """
    # Initialize results list
    expected_results = []
    
    # Map issues to rule types
    issue_to_rule_type = {
        "missing_column": ValidationRuleType.SCHEMA,
        "extra_column": ValidationRuleType.SCHEMA,
        "type_mismatch": ValidationRuleType.SCHEMA,
        "constraint_violation": ValidationRuleType.SCHEMA,
        
        "null_values": ValidationRuleType.CONTENT,
        "out_of_range": ValidationRuleType.CONTENT,
        "pattern_mismatch": ValidationRuleType.CONTENT,
        "invalid_categorical": ValidationRuleType.CONTENT,
        "duplicate_values": ValidationRuleType.CONTENT,
        
        "referential_integrity": ValidationRuleType.RELATIONSHIP,
        "cardinality_violation": ValidationRuleType.RELATIONSHIP,
        "hierarchical_violation": ValidationRuleType.RELATIONSHIP,
        
        "outliers": ValidationRuleType.STATISTICAL,
        "distribution_shift": ValidationRuleType.STATISTICAL,
        "correlation_change": ValidationRuleType.STATISTICAL,
        "trend_violation": ValidationRuleType.STATISTICAL
    }
    
    # Map issues to specific rule subtypes (simplified mapping)
    issue_to_subtype = {
        "missing_column": "schema_completeness",
        "extra_column": "schema_consistency",
        "type_mismatch": "type_validation",
        "constraint_violation": "constraint_validation",
        
        "null_values": "null_check",
        "out_of_range": "range_check",
        "pattern_mismatch": "pattern_check",
        "invalid_categorical": "categorical_check",
        "duplicate_values": "uniqueness_check",
        
        "referential_integrity": "referential_integrity",
        "cardinality_violation": "cardinality_check",
        "hierarchical_violation": "hierarchy_check",
        
        "outliers": "outlier_detection",
        "distribution_shift": "distribution_check",
        "correlation_change": "correlation_check",
        "trend_violation": "trend_check"
    }
    
    # Extract issue types from the issues list
    issue_types = [issue.get("type") for issue in issues]
    
    # For each validation rule, determine expected success status
    for rule in validation_rules:
        rule_type = rule.get("rule_type")
        rule_subtype = rule.get("subtype")
        
        # Determine if rule should fail based on injected issues
        will_fail = False
        failure_details = {}
        
        for issue_type in issue_types:
            if issue_type in issue_to_subtype and issue_to_rule_type.get(issue_type) == rule_type:
                if issue_to_subtype.get(issue_type) == rule_subtype:
                    will_fail = True
                    failure_details = {
                        "issue_type": issue_type,
                        "expected_failure": True,
                        "details": f"Validation failed due to injected {issue_type} issue"
                    }
                    break
        
        # Create expected result
        expected_result = {
            "rule_id": rule.get("rule_id"),
            "rule_type": rule_type,
            "subtype": rule_subtype,
            "dimension": rule.get("dimension"),
            "success": not will_fail,
            "timestamp": "2023-01-01T00:00:00Z"  # Placeholder timestamp
        }
        
        if will_fail:
            expected_result["failure_details"] = failure_details
        
        expected_results.append(expected_result)
    
    return expected_results


def save_quality_test_case(test_case: Dict, test_case_name: str, output_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
    """
    Saves a quality test case to files.
    
    Args:
        test_case: Test case to save
        test_case_name: Name of the test case
        output_dir: Directory to save the test case
        file_format: Format to save data files
        
    Returns:
        Dictionary with paths to saved files
    """
    # Create output directory if it doesn't exist
    test_case_dir = os.path.join(output_dir, test_case_name)
    os.makedirs(test_case_dir, exist_ok=True)
    
    # Extract components
    schema = test_case.get("schema", {})
    clean_data = test_case.get("clean_data", pd.DataFrame())
    data_with_issues = test_case.get("data_with_issues", pd.DataFrame())
    validation_rules = test_case.get("validation_rules", [])
    expected_results = test_case.get("expected_results", [])
    
    # Ensure data is in DataFrame format
    if isinstance(clean_data, list):
        clean_data = pd.DataFrame(clean_data)
    if isinstance(data_with_issues, list):
        data_with_issues = pd.DataFrame(data_with_issues)
    
    # Determine file extension based on format
    ext = ".json"
    if file_format == FileFormat.CSV:
        ext = ".csv"
    elif file_format == FileFormat.PARQUET:
        ext = ".parquet"
    elif file_format == FileFormat.AVRO:
        ext = ".avro"
    
    # Save schema
    schema_path = os.path.join(test_case_dir, "schema.json")
    with open(schema_path, 'w') as f:
        json.dump(schema, f, indent=2)
    
    # Save clean data
    clean_data_path = os.path.join(test_case_dir, f"clean_data{ext}")
    if file_format == FileFormat.CSV:
        clean_data.to_csv(clean_data_path, index=False)
    elif file_format == FileFormat.JSON:
        clean_data.to_json(clean_data_path, orient='records', date_format='iso')
    elif file_format == FileFormat.PARQUET:
        clean_data.to_parquet(clean_data_path, index=False)
    elif file_format == FileFormat.AVRO:
        # Avro requires additional library
        try:
            import fastavro
            schema_dict = {
                "type": "record",
                "name": "Data",
                "fields": [{"name": col, "type": ["null", "string"]} for col in clean_data.columns]
            }
            with open(clean_data_path, 'wb') as out:
                fastavro.writer(out, schema_dict, clean_data.to_dict('records'))
        except ImportError:
            raise ImportError("fastavro library is required to save in Avro format")
    
    # Save data with issues
    issues_data_path = os.path.join(test_case_dir, f"data_with_issues{ext}")
    if file_format == FileFormat.CSV:
        data_with_issues.to_csv(issues_data_path, index=False)
    elif file_format == FileFormat.JSON:
        data_with_issues.to_json(issues_data_path, orient='records', date_format='iso')
    elif file_format == FileFormat.PARQUET:
        data_with_issues.to_parquet(issues_data_path, index=False)
    elif file_format == FileFormat.AVRO:
        # Avro requires additional library
        try:
            import fastavro
            schema_dict = {
                "type": "record",
                "name": "Data",
                "fields": [{"name": col, "type": ["null", "string"]} for col in data_with_issues.columns]
            }
            with open(issues_data_path, 'wb') as out:
                fastavro.writer(out, schema_dict, data_with_issues.to_dict('records'))
        except ImportError:
            raise ImportError("fastavro library is required to save in Avro format")
    
    # Save validation rules
    rules_path = os.path.join(test_case_dir, "validation_rules.json")
    with open(rules_path, 'w') as f:
        json.dump(validation_rules, f, indent=2)
    
    # Save expected results
    expected_results_path = os.path.join(test_case_dir, "expected_results.json")
    with open(expected_results_path, 'w') as f:
        json.dump(expected_results, f, indent=2)
    
    # Return paths to saved files
    return {
        "test_case_name": test_case_name,
        "schema_path": schema_path,
        "clean_data_path": clean_data_path,
        "issues_data_path": issues_data_path,
        "validation_rules_path": rules_path,
        "expected_results_path": expected_results_path
    }


def load_quality_test_case(test_case_name: str, input_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
    """
    Loads a previously saved quality test case.
    
    Args:
        test_case_name: Name of the test case
        input_dir: Directory containing the test case
        file_format: Format of the data files
        
    Returns:
        Loaded test case
    """
    # Construct paths to test case files
    test_case_dir = os.path.join(input_dir, test_case_name)
    
    # Verify directory exists
    if not os.path.exists(test_case_dir):
        raise FileNotFoundError(f"Test case directory not found: {test_case_dir}")
    
    # Determine file extension based on format
    ext = ".json"
    if file_format == FileFormat.CSV:
        ext = ".csv"
    elif file_format == FileFormat.PARQUET:
        ext = ".parquet"
    elif file_format == FileFormat.AVRO:
        ext = ".avro"
    
    # Construct file paths
    schema_path = os.path.join(test_case_dir, "schema.json")
    clean_data_path = os.path.join(test_case_dir, f"clean_data{ext}")
    issues_data_path = os.path.join(test_case_dir, f"data_with_issues{ext}")
    rules_path = os.path.join(test_case_dir, "validation_rules.json")
    expected_results_path = os.path.join(test_case_dir, "expected_results.json")
    
    # Load schema
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    # Load clean data
    if file_format == FileFormat.CSV:
        clean_data = pd.read_csv(clean_data_path)
    elif file_format == FileFormat.JSON:
        clean_data = pd.read_json(clean_data_path, orient='records')
    elif file_format == FileFormat.PARQUET:
        clean_data = pd.read_parquet(clean_data_path)
    elif file_format == FileFormat.AVRO:
        try:
            import fastavro
            with open(clean_data_path, 'rb') as f:
                records = list(fastavro.reader(f))
            clean_data = pd.DataFrame(records)
        except ImportError:
            raise ImportError("fastavro library is required to load Avro format")
    
    # Load data with issues
    if file_format == FileFormat.CSV:
        data_with_issues = pd.read_csv(issues_data_path)
    elif file_format == FileFormat.JSON:
        data_with_issues = pd.read_json(issues_data_path, orient='records')
    elif file_format == FileFormat.PARQUET:
        data_with_issues = pd.read_parquet(issues_data_path)
    elif file_format == FileFormat.AVRO:
        try:
            import fastavro
            with open(issues_data_path, 'rb') as f:
                records = list(fastavro.reader(f))
            data_with_issues = pd.DataFrame(records)
        except ImportError:
            raise ImportError("fastavro library is required to load Avro format")
    
    # Load validation rules
    with open(rules_path, 'r') as f:
        validation_rules = json.load(f)
    
    # Load expected results
    with open(expected_results_path, 'r') as f:
        expected_results = json.load(f)
    
    # Return loaded test case
    return {
        "test_case_name": test_case_name,
        "schema": schema,
        "clean_data": clean_data,
        "data_with_issues": data_with_issues,
        "validation_rules": validation_rules,
        "expected_results": expected_results,
        "file_paths": {
            "schema_path": schema_path,
            "clean_data_path": clean_data_path,
            "issues_data_path": issues_data_path,
            "validation_rules_path": rules_path,
            "expected_results_path": expected_results_path
        }
    }


class QualityTestCase:
    """Class representing a test case for data quality validation testing."""
    
    def __init__(self, schema: Dict, clean_data: pd.DataFrame, data_with_issues: pd.DataFrame, 
                 validation_rules: List, expected_results: List, metadata: Dict = None):
        """
        Initialize a QualityTestCase.
        
        Args:
            schema: Schema definition
            clean_data: Data without issues
            data_with_issues: Data with injected issues
            validation_rules: Validation rules to apply
            expected_results: Expected validation results
            metadata: Additional metadata for the test case
        """
        self.schema = schema
        self.clean_data = clean_data
        self.data_with_issues = data_with_issues
        self.validation_rules = validation_rules
        self.expected_results = expected_results
        self.metadata = metadata or {}
        self.file_paths = {}
    
    def save(self, test_case_name: str, output_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Save the test case to files.
        
        Args:
            test_case_name: Name for the test case
            output_dir: Directory to save the test case
            file_format: Format to save data files
            
        Returns:
            Dictionary with paths to saved files
        """
        test_case_dict = {
            "schema": self.schema,
            "clean_data": self.clean_data,
            "data_with_issues": self.data_with_issues,
            "validation_rules": self.validation_rules,
            "expected_results": self.expected_results,
            "metadata": self.metadata
        }
        
        file_paths = save_quality_test_case(test_case_dict, test_case_name, output_dir, file_format)
        self.file_paths = file_paths
        return file_paths
    
    def to_dict(self) -> Dict:
        """
        Convert the test case to a dictionary representation.
        
        Returns:
            Dictionary representation of the test case
        """
        return {
            "schema": self.schema,
            "clean_data": self.clean_data.to_dict(orient='records') if isinstance(self.clean_data, pd.DataFrame) else self.clean_data,
            "data_with_issues": self.data_with_issues.to_dict(orient='records') if isinstance(self.data_with_issues, pd.DataFrame) else self.data_with_issues,
            "validation_rules": self.validation_rules,
            "expected_results": self.expected_results,
            "metadata": self.metadata,
            "file_paths": self.file_paths
        }
    
    @classmethod
    def from_dict(cls, test_case_dict: Dict) -> 'QualityTestCase':
        """
        Create a QualityTestCase from a dictionary.
        
        Args:
            test_case_dict: Dictionary representation of a test case
            
        Returns:
            QualityTestCase instance
        """
        schema = test_case_dict.get("schema", {})
        
        clean_data = test_case_dict.get("clean_data", [])
        if isinstance(clean_data, list):
            clean_data = pd.DataFrame(clean_data)
        
        data_with_issues = test_case_dict.get("data_with_issues", [])
        if isinstance(data_with_issues, list):
            data_with_issues = pd.DataFrame(data_with_issues)
        
        validation_rules = test_case_dict.get("validation_rules", [])
        expected_results = test_case_dict.get("expected_results", [])
        metadata = test_case_dict.get("metadata", {})
        
        test_case = cls(schema, clean_data, data_with_issues, validation_rules, expected_results, metadata)
        
        if "file_paths" in test_case_dict:
            test_case.file_paths = test_case_dict["file_paths"]
        
        return test_case
    
    @classmethod
    def load(cls, test_case_name: str, input_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> 'QualityTestCase':
        """
        Load a test case from files.
        
        Args:
            test_case_name: Name of the test case
            input_dir: Directory containing the test case
            file_format: Format of the data files
            
        Returns:
            QualityTestCase instance
        """
        loaded_test_case = load_quality_test_case(test_case_name, input_dir, file_format)
        
        return cls(
            schema=loaded_test_case["schema"],
            clean_data=loaded_test_case["clean_data"],
            data_with_issues=loaded_test_case["data_with_issues"],
            validation_rules=loaded_test_case["validation_rules"],
            expected_results=loaded_test_case["expected_results"],
            metadata={"test_case_name": test_case_name}
        )


class QualityTestCaseGenerator(TestCaseGenerator):
    """Generator for creating test cases specifically for data quality validation testing."""
    
    def __init__(self, output_dir: str = QUALITY_TEST_CASE_DIR):
        """
        Initialize the QualityTestCaseGenerator.
        
        Args:
            output_dir: Directory to save generated test cases
        """
        # Call parent constructor
        super().__init__(output_dir)
        
        # Initialize issue generators for different issue types
        self._issue_generators = {
            ValidationRuleType.SCHEMA: inject_schema_issues,
            ValidationRuleType.CONTENT: inject_content_issues,
            ValidationRuleType.RELATIONSHIP: inject_relationship_issues,
            ValidationRuleType.STATISTICAL: inject_statistical_issues
        }
        
        # Initialize schema generators for different data types
        self._schema_generators = {}  # This would be populated with specialized generators if needed
    
    def generate_schema_validation_test_case(self, schema_config: Dict, data_config: Dict, 
                                           schema_issues: List, test_case_name: str, 
                                           save_files: bool = True) -> Dict:
        """
        Generates a test case for schema validation testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            schema_issues: List of schema issues to inject
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, data, issues, and expected results
        """
        # Generate base schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate validation rules for schema validation
        validation_rules = self.generate_validation_rules(schema, ValidationRuleType.SCHEMA, schema_issues)
        
        # Create copies of schema and data
        modified_schema, modified_data = inject_schema_issues(schema, clean_data, schema_issues)
        
        # Generate expected validation results
        expected_results = generate_expected_validation_results(validation_rules, schema_issues)
        
        # Create test case
        test_case = QualityTestCase(
            schema=schema,
            clean_data=clean_data,
            data_with_issues=modified_data,
            validation_rules=validation_rules,
            expected_results=expected_results,
            metadata={
                "test_case_name": test_case_name,
                "test_case_type": "schema_validation",
                "schema_config": schema_config,
                "data_config": data_config,
                "schema_issues": schema_issues
            }
        )
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
            test_case.save(test_case_name, self._output_dir, file_format)
        
        return test_case.to_dict()
    
    def generate_content_validation_test_case(self, schema_config: Dict, data_config: Dict, 
                                            content_issues: List, test_case_name: str, 
                                            save_files: bool = True) -> Dict:
        """
        Generates a test case for content validation testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            content_issues: List of content issues to inject
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, data, issues, and expected results
        """
        # Generate base schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate validation rules for content validation
        validation_rules = self.generate_validation_rules(schema, ValidationRuleType.CONTENT, content_issues)
        
        # Create a copy of data and inject content issues
        modified_data = inject_content_issues(clean_data, content_issues)
        
        # Generate expected validation results
        expected_results = generate_expected_validation_results(validation_rules, content_issues)
        
        # Create test case
        test_case = QualityTestCase(
            schema=schema,
            clean_data=clean_data,
            data_with_issues=modified_data,
            validation_rules=validation_rules,
            expected_results=expected_results,
            metadata={
                "test_case_name": test_case_name,
                "test_case_type": "content_validation",
                "schema_config": schema_config,
                "data_config": data_config,
                "content_issues": content_issues
            }
        )
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
            test_case.save(test_case_name, self._output_dir, file_format)
        
        return test_case.to_dict()
    
    def generate_relationship_validation_test_case(self, schema_config: Dict, ref_schema_config: Dict, 
                                                 data_config: Dict, relationship_issues: List, 
                                                 test_case_name: str, save_files: bool = True) -> Dict:
        """
        Generates a test case for relationship validation testing.
        
        Args:
            schema_config: Configuration for main schema generation
            ref_schema_config: Configuration for reference schema generation
            data_config: Configuration for data generation
            relationship_issues: List of relationship issues to inject
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schemas, data, reference data, issues, and expected results
        """
        # Generate base schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate reference schema and data
        ref_schema, ref_data = generate_schema_data_pair(ref_schema_config, data_config)
        
        # Generate validation rules for relationship validation
        validation_rules = self.generate_validation_rules(schema, ValidationRuleType.RELATIONSHIP, relationship_issues)
        
        # Create copies of data and reference data and inject relationship issues
        modified_data, modified_ref_data = inject_relationship_issues(clean_data, ref_data, relationship_issues)
        
        # Generate expected validation results
        expected_results = generate_expected_validation_results(validation_rules, relationship_issues)
        
        # Create test case
        test_case = {
            "schema": schema,
            "ref_schema": ref_schema,
            "clean_data": clean_data,
            "ref_data": ref_data,
            "data_with_issues": modified_data,
            "modified_ref_data": modified_ref_data,
            "validation_rules": validation_rules,
            "expected_results": expected_results,
            "metadata": {
                "test_case_name": test_case_name,
                "test_case_type": "relationship_validation",
                "schema_config": schema_config,
                "ref_schema_config": ref_schema_config,
                "data_config": data_config,
                "relationship_issues": relationship_issues
            }
        }
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            # Determine file extension based on format
            ext = ".json"
            if file_format == FileFormat.CSV:
                ext = ".csv"
            elif file_format == FileFormat.PARQUET:
                ext = ".parquet"
            elif file_format == FileFormat.AVRO:
                ext = ".avro"
            
            # Save schemas
            schema_path = os.path.join(test_case_dir, "schema.json")
            with open(schema_path, 'w') as f:
                json.dump(schema, f, indent=2)
            
            ref_schema_path = os.path.join(test_case_dir, "ref_schema.json")
            with open(ref_schema_path, 'w') as f:
                json.dump(ref_schema, f, indent=2)
            
            # Save data
            clean_data_path = os.path.join(test_case_dir, f"clean_data{ext}")
            if file_format == FileFormat.CSV:
                clean_data.to_csv(clean_data_path, index=False)
            elif file_format == FileFormat.JSON:
                clean_data.to_json(clean_data_path, orient='records', date_format='iso')
            elif file_format == FileFormat.PARQUET:
                clean_data.to_parquet(clean_data_path, index=False)
            
            ref_data_path = os.path.join(test_case_dir, f"ref_data{ext}")
            if file_format == FileFormat.CSV:
                ref_data.to_csv(ref_data_path, index=False)
            elif file_format == FileFormat.JSON:
                ref_data.to_json(ref_data_path, orient='records', date_format='iso')
            elif file_format == FileFormat.PARQUET:
                ref_data.to_parquet(ref_data_path, index=False)
            
            modified_data_path = os.path.join(test_case_dir, f"data_with_issues{ext}")
            if file_format == FileFormat.CSV:
                modified_data.to_csv(modified_data_path, index=False)
            elif file_format == FileFormat.JSON:
                modified_data.to_json(modified_data_path, orient='records', date_format='iso')
            elif file_format == FileFormat.PARQUET:
                modified_data.to_parquet(modified_data_path, index=False)
            
            modified_ref_data_path = os.path.join(test_case_dir, f"modified_ref_data{ext}")
            if file_format == FileFormat.CSV:
                modified_ref_data.to_csv(modified_ref_data_path, index=False)
            elif file_format == FileFormat.JSON:
                modified_ref_data.to_json(modified_ref_data_path, orient='records', date_format='iso')
            elif file_format == FileFormat.PARQUET:
                modified_ref_data.to_parquet(modified_ref_data_path, index=False)
            
            # Save validation rules and expected results
            rules_path = os.path.join(test_case_dir, "validation_rules.json")
            with open(rules_path, 'w') as f:
                json.dump(validation_rules, f, indent=2)
            
            expected_results_path = os.path.join(test_case_dir, "expected_results.json")
            with open(expected_results_path, 'w') as f:
                json.dump(expected_results, f, indent=2)
            
            test_case["file_paths"] = {
                "schema_path": schema_path,
                "ref_schema_path": ref_schema_path,
                "clean_data_path": clean_data_path,
                "ref_data_path": ref_data_path,
                "modified_data_path": modified_data_path,
                "modified_ref_data_path": modified_ref_data_path,
                "validation_rules_path": rules_path,
                "expected_results_path": expected_results_path
            }
        
        return test_case
    
    def generate_statistical_validation_test_case(self, schema_config: Dict, data_config: Dict, 
                                               statistical_issues: List, test_case_name: str, 
                                               save_files: bool = True) -> Dict:
        """
        Generates a test case for statistical validation testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            statistical_issues: List of statistical issues to inject
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, data, issues, and expected results
        """
        # Generate base schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate validation rules for statistical validation
        validation_rules = self.generate_validation_rules(schema, ValidationRuleType.STATISTICAL, statistical_issues)
        
        # Create a copy of data and inject statistical issues
        modified_data = inject_statistical_issues(clean_data, statistical_issues)
        
        # Generate expected validation results
        expected_results = generate_expected_validation_results(validation_rules, statistical_issues)
        
        # Create test case
        test_case = QualityTestCase(
            schema=schema,
            clean_data=clean_data,
            data_with_issues=modified_data,
            validation_rules=validation_rules,
            expected_results=expected_results,
            metadata={
                "test_case_name": test_case_name,
                "test_case_type": "statistical_validation",
                "schema_config": schema_config,
                "data_config": data_config,
                "statistical_issues": statistical_issues
            }
        )
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
            test_case.save(test_case_name, self._output_dir, file_format)
        
        return test_case.to_dict()
    
    def generate_comprehensive_quality_test_suite(self, suite_config: Dict, suite_name: str, 
                                               save_files: bool = True) -> Dict:
        """
        Generates a comprehensive test suite with multiple quality test cases.
        
        Args:
            suite_config: Configuration for the test suite
            suite_name: Name for the test suite
            save_files: Whether to save the test cases to files
            
        Returns:
            Complete test suite with multiple test cases
        """
        # Create output directory for test suite
        suite_dir = os.path.join(self._output_dir, suite_name)
        os.makedirs(suite_dir, exist_ok=True)
        
        # Initialize suite results
        suite_results = {
            "suite_name": suite_name,
            "test_cases": {},
            "manifest": {
                "schema_validation_cases": [],
                "content_validation_cases": [],
                "relationship_validation_cases": [],
                "statistical_validation_cases": []
            }
        }
        
        # Generate schema validation test cases
        if "schema_validation_cases" in suite_config:
            for case_config in suite_config["schema_validation_cases"]:
                case_name = case_config.get("name", f"schema_validation_{generate_unique_id()}")
                
                test_case = self.generate_schema_validation_test_case(
                    case_config.get("schema_config", {}),
                    case_config.get("data_config", {}),
                    case_config.get("schema_issues", []),
                    case_name,
                    save_files
                )
                
                suite_results["test_cases"][case_name] = test_case
                suite_results["manifest"]["schema_validation_cases"].append(case_name)
        
        # Generate content validation test cases
        if "content_validation_cases" in suite_config:
            for case_config in suite_config["content_validation_cases"]:
                case_name = case_config.get("name", f"content_validation_{generate_unique_id()}")
                
                test_case = self.generate_content_validation_test_case(
                    case_config.get("schema_config", {}),
                    case_config.get("data_config", {}),
                    case_config.get("content_issues", []),
                    case_name,
                    save_files
                )
                
                suite_results["test_cases"][case_name] = test_case
                suite_results["manifest"]["content_validation_cases"].append(case_name)
        
        # Generate relationship validation test cases
        if "relationship_validation_cases" in suite_config:
            for case_config in suite_config["relationship_validation_cases"]:
                case_name = case_config.get("name", f"relationship_validation_{generate_unique_id()}")
                
                test_case = self.generate_relationship_validation_test_case(
                    case_config.get("schema_config", {}),
                    case_config.get("ref_schema_config", {}),
                    case_config.get("data_config", {}),
                    case_config.get("relationship_issues", []),
                    case_name,
                    save_files
                )
                
                suite_results["test_cases"][case_name] = test_case
                suite_results["manifest"]["relationship_validation_cases"].append(case_name)
        
        # Generate statistical validation test cases
        if "statistical_validation_cases" in suite_config:
            for case_config in suite_config["statistical_validation_cases"]:
                case_name = case_config.get("name", f"statistical_validation_{generate_unique_id()}")
                
                test_case = self.generate_statistical_validation_test_case(
                    case_config.get("schema_config", {}),
                    case_config.get("data_config", {}),
                    case_config.get("statistical_issues", []),
                    case_name,
                    save_files
                )
                
                suite_results["test_cases"][case_name] = test_case
                suite_results["manifest"]["statistical_validation_cases"].append(case_name)
        
        # Generate test suite manifest
        if save_files:
            manifest_path = os.path.join(suite_dir, "manifest.json")
            with open(manifest_path, 'w') as f:
                json.dump(suite_results["manifest"], f, indent=2)
            
            suite_results["manifest_path"] = manifest_path
        
        return suite_results
    
    def save_quality_test_case(self, test_case: Dict, test_case_name: str, 
                             file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Saves a quality test case to files.
        
        Args:
            test_case: Test case to save
            test_case_name: Name for the test case
            file_format: Format to save data files
            
        Returns:
            Updated test case with file paths
        """
        # Extract components
        file_paths = save_quality_test_case(test_case, test_case_name, self._output_dir, file_format)
        
        # Update test case with file paths
        test_case["file_paths"] = file_paths
        
        return test_case
    
    def load_quality_test_case(self, test_case_name: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Loads a previously saved quality test case.
        
        Args:
            test_case_name: Name of the test case
            file_format: Format of the data files
            
        Returns:
            Loaded test case
        """
        return load_quality_test_case(test_case_name, self._output_dir, file_format)
    
    def generate_validation_rules(self, schema: Dict, rule_type: ValidationRuleType, issues: List) -> List:
        """
        Generates validation rules based on schema and issue types.
        
        Args:
            schema: Schema definition
            rule_type: Type of validation rules to generate
            issues: List of issues that will be injected
            
        Returns:
            List of validation rules
        """
        validation_rules = []
        
        # Extract issue subtypes
        issue_subtypes = set()
        for issue in issues:
            issue_type = issue.get("type")
            
            # Map issue types to rule subtypes
            if rule_type == ValidationRuleType.SCHEMA:
                if issue_type == "missing_column":
                    issue_subtypes.add("schema_completeness")
                elif issue_type == "extra_column":
                    issue_subtypes.add("schema_consistency")
                elif issue_type == "type_mismatch":
                    issue_subtypes.add("type_validation")
                elif issue_type == "constraint_violation":
                    issue_subtypes.add("constraint_validation")
            
            elif rule_type == ValidationRuleType.CONTENT:
                if issue_type == "null_values":
                    issue_subtypes.add("null_check")
                elif issue_type == "out_of_range":
                    issue_subtypes.add("range_check")
                elif issue_type == "pattern_mismatch":
                    issue_subtypes.add("pattern_check")
                elif issue_type == "invalid_categorical":
                    issue_subtypes.add("categorical_check")
                elif issue_type == "duplicate_values":
                    issue_subtypes.add("uniqueness_check")
            
            elif rule_type == ValidationRuleType.RELATIONSHIP:
                if issue_type == "referential_integrity":
                    issue_subtypes.add("referential_integrity")
                elif issue_type == "cardinality_violation":
                    issue_subtypes.add("cardinality_check")
                elif issue_type == "hierarchical_violation":
                    issue_subtypes.add("hierarchy_check")
            
            elif rule_type == ValidationRuleType.STATISTICAL:
                if issue_type == "outliers":
                    issue_subtypes.add("outlier_detection")
                elif issue_type == "distribution_shift":
                    issue_subtypes.add("distribution_check")
                elif issue_type == "correlation_change":
                    issue_subtypes.add("correlation_check")
                elif issue_type == "trend_violation":
                    issue_subtypes.add("trend_check")
        
        # Generate rules for each subtype
        for subtype in issue_subtypes:
            # Create appropriate parameters based on rule type and subtype
            parameters = {}
            
            if rule_type == ValidationRuleType.SCHEMA:
                if subtype == "schema_completeness":
                    parameters = {
                        "required_columns": [field["name"] for field in schema["fields"] if field.get("required", False)]
                    }
                elif subtype == "schema_consistency":
                    parameters = {
                        "expected_columns": [field["name"] for field in schema["fields"]]
                    }
                elif subtype == "type_validation":
                    parameters = {
                        "column_types": {field["name"]: field["type"] for field in schema["fields"]}
                    }
                elif subtype == "constraint_validation":
                    parameters = {
                        "constraints": {
                            field["name"]: {k: v for k, v in field.items() if k not in ["name", "type"]}
                            for field in schema["fields"] if any(k for k in field.keys() if k not in ["name", "type"])
                        }
                    }
            
            elif rule_type == ValidationRuleType.CONTENT:
                if subtype == "null_check":
                    parameters = {
                        "columns": [field["name"] for field in schema["fields"] if not field.get("nullable", True)]
                    }
                elif subtype == "range_check":
                    parameters = {
                        "ranges": {
                            field["name"]: {"min": field.get("min"), "max": field.get("max")}
                            for field in schema["fields"] if field["type"] in ["integer", "float"] and ("min" in field or "max" in field)
                        }
                    }
                elif subtype == "pattern_check":
                    parameters = {
                        "patterns": {
                            field["name"]: field.get("pattern", ".*")
                            for field in schema["fields"] if field["type"] == "string" and "pattern" in field
                        }
                    }
                elif subtype == "categorical_check":
                    parameters = {
                        "categories": {
                            field["name"]: field.get("enum", [])
                            for field in schema["fields"] if "enum" in field
                        }
                    }
                elif subtype == "uniqueness_check":
                    parameters = {
                        "columns": [field["name"] for field in schema["fields"] if field.get("unique", False)]
                    }
            
            elif rule_type == ValidationRuleType.RELATIONSHIP:
                # Extract relevant columns from issues
                columns = []
                for issue in issues:
                    if issue.get("type") == "referential_integrity":
                        config = issue.get("config", {})
                        source_column = config.get("source_column")
                        if source_column:
                            columns.append(source_column)
                
                if subtype == "referential_integrity":
                    parameters = {
                        "relationships": [
                            {
                                "source_column": issue.get("config", {}).get("source_column", ""),
                                "target_table": issue.get("config", {}).get("target_table", ""),
                                "target_column": issue.get("config", {}).get("target_column", "")
                            }
                            for issue in issues if issue.get("type") == "referential_integrity"
                        ]
                    }
                elif subtype == "cardinality_check":
                    parameters = {
                        "cardinality_rules": [
                            {
                                "source_column": issue.get("config", {}).get("source_column", ""),
                                "target_column": issue.get("config", {}).get("target_column", ""),
                                "expected_cardinality": issue.get("config", {}).get("expected_cardinality", "many_to_one")
                            }
                            for issue in issues if issue.get("type") == "cardinality_violation"
                        ]
                    }
                elif subtype == "hierarchy_check":
                    parameters = {
                        "hierarchy_rules": [
                            {
                                "parent_column": issue.get("config", {}).get("parent_column", ""),
                                "child_column": issue.get("config", {}).get("child_column", "")
                            }
                            for issue in issues if issue.get("type") == "hierarchical_violation"
                        ]
                    }
            
            elif rule_type == ValidationRuleType.STATISTICAL:
                if subtype == "outlier_detection":
                    parameters = {
                        "columns": [
                            issue.get("config", {}).get("columns", [])[0] if issue.get("config", {}).get("columns") else ""
                            for issue in issues if issue.get("type") == "outliers"
                        ],
                        "method": "zscore",
                        "threshold": 3.0
                    }
                elif subtype == "distribution_check":
                    parameters = {
                        "columns": [
                            issue.get("config", {}).get("columns", [])[0] if issue.get("config", {}).get("columns") else ""
                            for issue in issues if issue.get("type") == "distribution_shift"
                        ],
                        "method": "ks_test",
                        "threshold": 0.05
                    }
                elif subtype == "correlation_check":
                    pairs = []
                    for issue in issues:
                        if issue.get("type") == "correlation_change":
                            config = issue.get("config", {})
                            column_pairs = config.get("column_pairs", [])
                            if column_pairs:
                                pairs.extend(column_pairs)
                    
                    parameters = {
                        "column_pairs": pairs,
                        "threshold": 0.2
                    }
                elif subtype == "trend_check":
                    parameters = {
                        "time_column": next((
                            issue.get("config", {}).get("time_column", "")
                            for issue in issues if issue.get("type") == "trend_violation"
                        ), ""),
                        "value_column": next((
                            issue.get("config", {}).get("value_column", "")
                            for issue in issues if issue.get("type") == "trend_violation"
                        ), ""),
                        "method": "moving_average",
                        "window_size": 3,
                        "threshold": 2.0
                    }
            
            # Generate rule with these parameters
            rule = generate_validation_rule(rule_type, subtype, parameters)
            validation_rules.append(rule)
        
        return validation_rules