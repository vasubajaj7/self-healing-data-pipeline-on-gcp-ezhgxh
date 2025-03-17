"""
Specialized test case generator for self-healing AI testing.

Creates test cases with various data quality issues and pipeline failures
along with expected correction outcomes to facilitate thorough testing
of the self-healing capabilities of the data pipeline.
"""

import os
import json
import random
import uuid
import datetime
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import numpy as np
from faker import Faker

from src.test.testcase_generators.schema_data_generator import TestCaseGenerator, SchemaDataTestCase
from src.backend.constants import HealingActionType, FileFormat
from src.test.utils.test_helpers import create_test_dataframe, generate_unique_id

# Constants
HEALING_TEST_CASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'healing')
DEFAULT_NUM_VARIATIONS = 5
DEFAULT_DATA_SIZE = 100
DEFAULT_FILE_FORMAT = FileFormat.JSON

# Types of data quality issues for testing
DATA_QUALITY_ISSUE_TYPES = [
    "missing_values",
    "outliers",
    "format_errors",
    "schema_drift",
    "data_corruption",
    "referential_integrity"
]

# Types of pipeline issues for testing
PIPELINE_ISSUE_TYPES = [
    "resource_exhaustion",
    "timeout",
    "dependency_failure",
    "configuration_error",
    "permission_error",
    "service_unavailable"
]

# Mapping of issue types to possible correction strategies
CORRECTION_STRATEGIES = {
    "missing_values": ["mean_imputation", "median_imputation", "mode_imputation", "constant_imputation"],
    "outliers": ["winsorization", "trimming", "iqr_filtering", "z_score_filtering"],
    "format_errors": ["date_format_correction", "number_format_correction", "string_format_correction", "type_conversion"],
    "schema_drift": ["column_mapping", "type_casting", "default_values"],
    "data_corruption": ["checksum_validation", "reconstruction", "fallback_to_previous"],
    "referential_integrity": ["reference_repair", "deduplication", "removal"]
}

def generate_healing_action(issue_type: str, correction_strategy: str, parameters: dict) -> dict:
    """
    Generates a healing action definition based on issue type and parameters.
    
    Args:
        issue_type: Type of issue to heal
        correction_strategy: Strategy to use for correction
        parameters: Additional parameters for the healing action
    
    Returns:
        Healing action definition
    """
    action_id = f"action-{uuid.uuid4()}"
    
    action = {
        "action_id": action_id,
        "issue_type": issue_type,
        "correction_strategy": correction_strategy,
        "parameters": parameters,
        "confidence_score": random.uniform(0.7, 0.98)  # Generate a realistic confidence score
    }
    
    # Adjust confidence based on issue type and strategy
    if issue_type == "missing_values" and correction_strategy in ["mean_imputation", "median_imputation"]:
        action["confidence_score"] = random.uniform(0.85, 0.98)  # Higher confidence for standard imputation
    elif issue_type == "outliers" and correction_strategy in ["iqr_filtering"]:
        action["confidence_score"] = random.uniform(0.8, 0.95)
    elif issue_type == "format_errors":
        action["confidence_score"] = random.uniform(0.75, 0.9)
    elif issue_type == "schema_drift":
        action["confidence_score"] = random.uniform(0.7, 0.85)  # Lower confidence for schema changes
    elif issue_type == "resource_exhaustion":
        action["confidence_score"] = random.uniform(0.8, 0.95)
    elif issue_type == "timeout":
        action["confidence_score"] = random.uniform(0.75, 0.9)
    elif issue_type == "dependency_failure":
        action["confidence_score"] = random.uniform(0.7, 0.85)
    elif issue_type == "configuration_error":
        action["confidence_score"] = random.uniform(0.8, 0.95)
    elif issue_type == "permission_error":
        action["confidence_score"] = random.uniform(0.75, 0.9)
    elif issue_type == "service_unavailable":
        action["confidence_score"] = random.uniform(0.6, 0.8)  # Lower confidence for service issues
    
    return action

def inject_data_quality_issues(schema: dict, data: pd.DataFrame, issues: list) -> Tuple[dict, pd.DataFrame]:
    """
    Injects data quality issues into a dataset.
    
    Args:
        schema: Original schema definition
        data: Clean DataFrame
        issues: List of issue specifications to inject
    
    Returns:
        Tuple of (modified schema, data with issues)
    """
    # Create copies to avoid modifying originals
    modified_schema = schema.copy()
    data_with_issues = data.copy()
    
    for issue in issues:
        issue_type = issue["type"]
        params = issue.get("params", {})
        
        if issue_type == "missing_values":
            # Inject missing values
            columns = params.get("columns", random.sample(list(data.columns), k=min(random.randint(1, 3), len(data.columns))))
            percentage = params.get("percentage", random.uniform(0.1, 0.3))
            
            for col in columns:
                null_mask = np.random.random(size=len(data_with_issues)) < percentage
                data_with_issues.loc[null_mask, col] = None
        
        elif issue_type == "outliers":
            # Inject outlier values
            columns = params.get("columns", [col for col in data.select_dtypes(include=['number']).columns])
            percentage = params.get("percentage", random.uniform(0.05, 0.15))
            
            for col in columns:
                if col in data_with_issues.columns and data_with_issues[col].dtype in [np.float64, np.int64]:
                    outlier_mask = np.random.random(size=len(data_with_issues)) < percentage
                    # Calculate extreme values
                    col_mean = data_with_issues[col].mean()
                    col_std = max(data_with_issues[col].std(), 1e-6)  # Prevent division by zero
                    multiplier = params.get("multiplier", random.uniform(4.0, 10.0))
                    
                    # Generate outliers above and below the mean
                    if random.random() > 0.5:
                        data_with_issues.loc[outlier_mask, col] = col_mean + col_std * multiplier
                    else:
                        data_with_issues.loc[outlier_mask, col] = col_mean - col_std * multiplier
        
        elif issue_type == "format_errors":
            # Inject format errors
            columns = params.get("columns", random.sample(list(data.select_dtypes(include=['object']).columns), 
                                                         k=min(random.randint(1, 3), len(data.select_dtypes(include=['object']).columns))))
            percentage = params.get("percentage", random.uniform(0.1, 0.2))
            
            for col in columns:
                if col in data_with_issues.columns:
                    error_mask = np.random.random(size=len(data_with_issues)) < percentage
                    data_with_issues.loc[error_mask, col] = "INVALID_FORMAT_" + data_with_issues.loc[error_mask, col].astype(str)
        
        elif issue_type == "schema_drift":
            # Modify schema structure
            drift_type = params.get("drift_type", random.choice(["rename_column", "change_type", "drop_column"]))
            
            if drift_type == "rename_column" and len(modified_schema["fields"]) > 0:
                # Rename a column
                field_idx = random.randint(0, len(modified_schema["fields"]) - 1)
                old_name = modified_schema["fields"][field_idx]["name"]
                new_name = f"renamed_{old_name}"
                
                # Update schema
                modified_schema["fields"][field_idx]["name"] = new_name
                
                # Rename in DataFrame
                if old_name in data_with_issues.columns:
                    data_with_issues = data_with_issues.rename(columns={old_name: new_name})
            
            elif drift_type == "change_type" and len(modified_schema["fields"]) > 0:
                # Change a column type
                field_idx = random.randint(0, len(modified_schema["fields"]) - 1)
                field = modified_schema["fields"][field_idx]
                old_type = field["type"]
                
                # Select a different type
                new_type = random.choice([t for t in ["string", "integer", "float", "boolean"] if t != old_type])
                
                # Update schema
                modified_schema["fields"][field_idx]["type"] = new_type
                
                # No need to change the data as this will create a type mismatch
            
            elif drift_type == "drop_column" and len(modified_schema["fields"]) > 1:
                # Drop a column
                field_idx = random.randint(0, len(modified_schema["fields"]) - 1)
                field_name = modified_schema["fields"][field_idx]["name"]
                
                # Update schema
                modified_schema["fields"].pop(field_idx)
                
                # Drop from DataFrame
                if field_name in data_with_issues.columns:
                    data_with_issues = data_with_issues.drop(columns=[field_name])
        
        elif issue_type == "data_corruption":
            # Corrupt data values
            columns = params.get("columns", random.sample(list(data.columns), k=min(random.randint(1, 3), len(data.columns))))
            percentage = params.get("percentage", random.uniform(0.05, 0.15))
            
            for col in columns:
                if col in data_with_issues.columns:
                    corruption_mask = np.random.random(size=len(data_with_issues)) < percentage
                    
                    # Corrupt based on data type
                    if data_with_issues[col].dtype in [np.float64, np.int64]:
                        # Replace with NaN or extreme values
                        if random.random() > 0.5:
                            data_with_issues.loc[corruption_mask, col] = float('nan')
                        else:
                            data_with_issues.loc[corruption_mask, col] = random.choice([-1e9, 1e9, -1, 0])
                    else:
                        # Replace with garbage string
                        data_with_issues.loc[corruption_mask, col] = "###CORRUPTED###"
        
        elif issue_type == "referential_integrity":
            # Break referential relationships
            # This is a simplification; actual implementation would depend on the data model
            ref_column = params.get("column", random.choice(list(data.columns)))
            percentage = params.get("percentage", random.uniform(0.05, 0.15))
            
            if ref_column in data_with_issues.columns:
                integrity_mask = np.random.random(size=len(data_with_issues)) < percentage
                
                # Replace with invalid references
                if data_with_issues[ref_column].dtype in [np.float64, np.int64]:
                    # For numeric columns, use negative values as invalid references
                    data_with_issues.loc[integrity_mask, ref_column] = -1 * (data_with_issues.loc[integrity_mask, ref_column].abs() + 1)
                else:
                    # For other columns, prefix with INVALID_REF_
                    data_with_issues.loc[integrity_mask, ref_column] = "INVALID_REF_" + data_with_issues.loc[integrity_mask, ref_column].astype(str)
    
    return modified_schema, data_with_issues

def inject_pipeline_issues(pipeline_config: dict, issues: list) -> dict:
    """
    Generates pipeline issue metadata for testing.
    
    Args:
        pipeline_config: Original pipeline configuration
        issues: List of issue specifications to inject
    
    Returns:
        Pipeline issue metadata
    """
    pipeline_issues = {
        "pipeline_id": pipeline_config.get("pipeline_id", f"pipeline-{uuid.uuid4()}"),
        "issues": []
    }
    
    for issue in issues:
        issue_type = issue["type"]
        params = issue.get("params", {})
        
        issue_metadata = {
            "issue_id": f"issue-{uuid.uuid4()}",
            "type": issue_type,
            "timestamp": params.get("timestamp", datetime.datetime.now().isoformat()),
            "details": {}
        }
        
        if issue_type == "resource_exhaustion":
            # Resource limits reached
            resource_type = params.get("resource_type", random.choice(["memory", "cpu", "disk", "slots"]))
            
            issue_metadata["details"] = {
                "resource_type": resource_type,
                "limit": params.get("limit", f"{random.randint(1, 16)}G" if resource_type in ["memory", "disk"] else f"{random.randint(1, 8)}"),
                "current_usage": params.get("current_usage", f"{random.randint(16, 32)}G" if resource_type in ["memory", "disk"] else f"{random.randint(8, 16)}"),
                "component": params.get("component", random.choice(["BQ_Processor", "Dataflow", "Composer", "Function"]))
            }
        
        elif issue_type == "timeout":
            # Operation timeout
            issue_metadata["details"] = {
                "operation": params.get("operation", random.choice(["query", "extraction", "transformation", "loading"])),
                "timeout_after": params.get("timeout_after", f"{random.randint(30, 300)} seconds"),
                "normal_duration": params.get("normal_duration", f"{random.randint(10, 60)} seconds"),
                "component": params.get("component", random.choice(["BQ_Processor", "Dataflow", "Composer", "Function"]))
            }
        
        elif issue_type == "dependency_failure":
            # Dependent service failure
            issue_metadata["details"] = {
                "dependency": params.get("dependency", random.choice(["BigQuery", "GCS", "CloudSQL", "API"])),
                "error_code": params.get("error_code", random.choice(["500", "503", "429", "403"])),
                "error_message": params.get("error_message", "The service is currently unavailable"),
                "component": params.get("component", "DataExtractor")
            }
        
        elif issue_type == "configuration_error":
            # Misconfiguration
            issue_metadata["details"] = {
                "config_element": params.get("config_element", random.choice(["connection_string", "query_parameters", "credentials", "project_id"])),
                "error_message": params.get("error_message", "Invalid configuration value"),
                "component": params.get("component", random.choice(["Pipeline", "Connector", "Transformer"]))
            }
        
        elif issue_type == "permission_error":
            # Permission/authorization issues
            issue_metadata["details"] = {
                "resource": params.get("resource", random.choice(["BigQuery.Dataset", "GCS.Bucket", "Secret", "Function"])),
                "required_permission": params.get("required_permission", random.choice(["read", "write", "admin"])),
                "principal": params.get("principal", f"service-account-{random.randint(1, 999)}@project.iam.gserviceaccount.com"),
                "error_code": params.get("error_code", "403")
            }
        
        elif issue_type == "service_unavailable":
            # Service outage or unavailability
            issue_metadata["details"] = {
                "service": params.get("service", random.choice(["BigQuery", "Cloud Storage", "Vertex AI", "Cloud SQL"])),
                "status": params.get("status", "unavailable"),
                "error_code": params.get("error_code", random.choice(["500", "503", "429"])),
                "estimated_recovery": params.get("estimated_recovery", f"{random.randint(5, 60)} minutes")
            }
        
        pipeline_issues["issues"].append(issue_metadata)
    
    return pipeline_issues

def generate_expected_corrections(healing_actions: list, issues: list, 
                               original_data: pd.DataFrame, data_with_issues: pd.DataFrame) -> list:
    """
    Generates expected correction results based on injected issues.
    
    Args:
        healing_actions: List of healing actions to be applied
        issues: List of issues that were injected
        original_data: Original clean data
        data_with_issues: Data with injected issues
    
    Returns:
        Expected correction results
    """
    expected_corrections = []
    
    # Map issues to healing actions
    issue_to_action = {}
    for action in healing_actions:
        issue_type = action["issue_type"]
        if issue_type not in issue_to_action:
            issue_to_action[issue_type] = []
        issue_to_action[issue_type].append(action)
    
    # Generate expected corrections based on action type
    for issue in issues:
        issue_type = issue["type"]
        
        if issue_type in issue_to_action:
            for action in issue_to_action[issue_type]:
                correction_strategy = action["correction_strategy"]
                params = action["parameters"]
                
                expected_result = {
                    "action_id": action["action_id"],
                    "issue_type": issue_type,
                    "success": True,
                    "confidence_score": action["confidence_score"],
                    "details": {}
                }
                
                # Generate expected results based on issue type and strategy
                if issue_type == "missing_values":
                    if correction_strategy == "mean_imputation":
                        expected_result["details"]["method"] = "mean"
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["imputed_count"] = random.randint(1, len(data_with_issues))
                    
                    elif correction_strategy == "median_imputation":
                        expected_result["details"]["method"] = "median"
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["imputed_count"] = random.randint(1, len(data_with_issues))
                    
                    elif correction_strategy == "mode_imputation":
                        expected_result["details"]["method"] = "mode"
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["imputed_count"] = random.randint(1, len(data_with_issues))
                    
                    elif correction_strategy == "constant_imputation":
                        expected_result["details"]["method"] = "constant"
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["imputed_count"] = random.randint(1, len(data_with_issues))
                        expected_result["details"]["constant_value"] = params.get("value", 0)
                
                elif issue_type == "outliers":
                    if correction_strategy in ["winsorization", "trimming", "iqr_filtering", "z_score_filtering"]:
                        expected_result["details"]["method"] = correction_strategy
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["outliers_detected"] = random.randint(1, int(len(data_with_issues) * 0.1))
                        expected_result["details"]["outliers_corrected"] = random.randint(1, expected_result["details"]["outliers_detected"])
                
                elif issue_type == "format_errors":
                    if correction_strategy in ["date_format_correction", "number_format_correction", "string_format_correction", "type_conversion"]:
                        expected_result["details"]["method"] = correction_strategy
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                        expected_result["details"]["errors_detected"] = random.randint(1, int(len(data_with_issues) * 0.1))
                        expected_result["details"]["errors_corrected"] = random.randint(1, expected_result["details"]["errors_detected"])
                
                elif issue_type == "schema_drift":
                    if correction_strategy in ["column_mapping", "type_casting", "default_values"]:
                        expected_result["details"]["method"] = correction_strategy
                        expected_result["details"]["schema_changes"] = random.randint(1, 3)
                        expected_result["details"]["affected_columns"] = params.get("columns", [])
                
                elif issue_type == "data_corruption":
                    if correction_strategy in ["checksum_validation", "reconstruction", "fallback_to_previous"]:
                        expected_result["details"]["method"] = correction_strategy
                        expected_result["details"]["corrupted_records"] = random.randint(1, int(len(data_with_issues) * 0.1))
                        expected_result["details"]["recovered_records"] = random.randint(1, expected_result["details"]["corrupted_records"])
                
                elif issue_type == "referential_integrity":
                    if correction_strategy in ["reference_repair", "deduplication", "removal"]:
                        expected_result["details"]["method"] = correction_strategy
                        expected_result["details"]["integrity_violations"] = random.randint(1, int(len(data_with_issues) * 0.1))
                        expected_result["details"]["corrected_violations"] = random.randint(1, expected_result["details"]["integrity_violations"])
                
                expected_corrections.append(expected_result)
    
    return expected_corrections

def save_healing_test_case(test_case: dict, test_case_name: str, output_dir: str, 
                         file_format: FileFormat = DEFAULT_FILE_FORMAT) -> dict:
    """
    Saves a healing test case to files.
    
    Args:
        test_case: The test case to save
        test_case_name: Name of the test case
        output_dir: Directory to save the test case
        file_format: Format to save data files
    
    Returns:
        Dictionary with paths to saved files
    """
    # Create output directory if it doesn't exist
    test_case_dir = os.path.join(output_dir, test_case_name)
    os.makedirs(test_case_dir, exist_ok=True)
    
    file_paths = {
        "test_case_name": test_case_name
    }
    
    # Save schema if present
    if "schema" in test_case:
        schema_path = os.path.join(test_case_dir, "schema.json")
        with open(schema_path, 'w') as f:
            json.dump(test_case["schema"], f, indent=2)
        file_paths["schema_path"] = schema_path
    
    # Save clean data if present
    if "clean_data" in test_case:
        clean_data = pd.DataFrame(test_case["clean_data"]) if isinstance(test_case["clean_data"], list) else test_case["clean_data"]
        
        # Determine file extension
        file_ext = ".json"
        if file_format == FileFormat.CSV:
            file_ext = ".csv"
        elif file_format == FileFormat.PARQUET:
            file_ext = ".parquet"
        elif file_format == FileFormat.AVRO:
            file_ext = ".avro"
        
        clean_data_path = os.path.join(test_case_dir, f"clean_data{file_ext}")
        
        # Save based on format
        if file_format == FileFormat.CSV:
            clean_data.to_csv(clean_data_path, index=False)
        elif file_format == FileFormat.JSON:
            clean_data.to_json(clean_data_path, orient="records", date_format="iso")
        elif file_format == FileFormat.PARQUET:
            clean_data.to_parquet(clean_data_path, index=False)
        
        file_paths["clean_data_path"] = clean_data_path
    
    # Save data with issues if present
    if "data_with_issues" in test_case:
        data_with_issues = pd.DataFrame(test_case["data_with_issues"]) if isinstance(test_case["data_with_issues"], list) else test_case["data_with_issues"]
        
        # Determine file extension as above
        file_ext = ".json"
        if file_format == FileFormat.CSV:
            file_ext = ".csv"
        elif file_format == FileFormat.PARQUET:
            file_ext = ".parquet"
        elif file_format == FileFormat.AVRO:
            file_ext = ".avro"
        
        issues_data_path = os.path.join(test_case_dir, f"data_with_issues{file_ext}")
        
        # Save based on format
        if file_format == FileFormat.CSV:
            data_with_issues.to_csv(issues_data_path, index=False)
        elif file_format == FileFormat.JSON:
            data_with_issues.to_json(issues_data_path, orient="records", date_format="iso")
        elif file_format == FileFormat.PARQUET:
            data_with_issues.to_parquet(issues_data_path, index=False)
        
        file_paths["issues_data_path"] = issues_data_path
    
    # Save healing actions if present
    if "healing_actions" in test_case:
        actions_path = os.path.join(test_case_dir, "healing_actions.json")
        with open(actions_path, 'w') as f:
            json.dump(test_case["healing_actions"], f, indent=2)
        file_paths["actions_path"] = actions_path
    
    # Save expected corrections if present
    if "expected_corrections" in test_case:
        corrections_path = os.path.join(test_case_dir, "expected_corrections.json")
        with open(corrections_path, 'w') as f:
            json.dump(test_case["expected_corrections"], f, indent=2)
        file_paths["corrections_path"] = corrections_path
    
    # Save pipeline issues if present
    if "pipeline_issues" in test_case:
        issues_path = os.path.join(test_case_dir, "pipeline_issues.json")
        with open(issues_path, 'w') as f:
            json.dump(test_case["pipeline_issues"], f, indent=2)
        file_paths["issues_path"] = issues_path
    
    return file_paths

def load_healing_test_case(test_case_name: str, input_dir: str, 
                         file_format: FileFormat = DEFAULT_FILE_FORMAT) -> dict:
    """
    Loads a previously saved healing test case.
    
    Args:
        test_case_name: Name of the test case
        input_dir: Directory containing the test case
        file_format: Format of the data files
    
    Returns:
        Loaded test case
    """
    test_case_dir = os.path.join(input_dir, test_case_name)
    
    if not os.path.exists(test_case_dir):
        raise FileNotFoundError(f"Test case directory not found: {test_case_dir}")
    
    test_case = {
        "test_case_name": test_case_name
    }
    
    # Load schema if exists
    schema_path = os.path.join(test_case_dir, "schema.json")
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            test_case["schema"] = json.load(f)
    
    # Determine file extension
    file_ext = ".json"
    if file_format == FileFormat.CSV:
        file_ext = ".csv"
    elif file_format == FileFormat.PARQUET:
        file_ext = ".parquet"
    elif file_format == FileFormat.AVRO:
        file_ext = ".avro"
    
    # Load clean data if exists
    clean_data_path = os.path.join(test_case_dir, f"clean_data{file_ext}")
    if os.path.exists(clean_data_path):
        # Load based on format
        if file_format == FileFormat.CSV:
            test_case["clean_data"] = pd.read_csv(clean_data_path)
        elif file_format == FileFormat.JSON:
            test_case["clean_data"] = pd.read_json(clean_data_path, orient="records")
        elif file_format == FileFormat.PARQUET:
            test_case["clean_data"] = pd.read_parquet(clean_data_path)
    
    # Load data with issues if exists
    issues_data_path = os.path.join(test_case_dir, f"data_with_issues{file_ext}")
    if os.path.exists(issues_data_path):
        # Load based on format
        if file_format == FileFormat.CSV:
            test_case["data_with_issues"] = pd.read_csv(issues_data_path)
        elif file_format == FileFormat.JSON:
            test_case["data_with_issues"] = pd.read_json(issues_data_path, orient="records")
        elif file_format == FileFormat.PARQUET:
            test_case["data_with_issues"] = pd.read_parquet(issues_data_path)
    
    # Load healing actions if exists
    actions_path = os.path.join(test_case_dir, "healing_actions.json")
    if os.path.exists(actions_path):
        with open(actions_path, 'r') as f:
            test_case["healing_actions"] = json.load(f)
    
    # Load expected corrections if exists
    corrections_path = os.path.join(test_case_dir, "expected_corrections.json")
    if os.path.exists(corrections_path):
        with open(corrections_path, 'r') as f:
            test_case["expected_corrections"] = json.load(f)
    
    # Load pipeline issues if exists
    issues_path = os.path.join(test_case_dir, "pipeline_issues.json")
    if os.path.exists(issues_path):
        with open(issues_path, 'r') as f:
            test_case["pipeline_issues"] = json.load(f)
    
    return test_case

class HealingTestCaseGenerator(TestCaseGenerator):
    """
    Generator for creating test cases specifically for self-healing AI testing.
    
    This class extends the basic TestCaseGenerator to create test scenarios with
    intentional data quality issues and pipeline failures, along with expected correction
    outcomes to test the self-healing capabilities of the data pipeline.
    """
    
    def __init__(self, output_dir: str = HEALING_TEST_CASE_DIR):
        """
        Initialize the HealingTestCaseGenerator.
        
        Args:
            output_dir: Directory to save generated test cases
        """
        super().__init__(output_dir)
        
        # Initialize issue generators for different issue types
        self._issue_generators = {}
        
        # Initialize correction generators for different correction strategies
        self._correction_generators = {}
        
        # Set up schema generators for different data types
        self._schema_generators = {}
    
    def generate_data_quality_healing_test_case(
        self, 
        schema_config: dict, 
        data_config: dict, 
        data_quality_issues: list,
        healing_strategies: list,
        test_case_name: str,
        save_files: bool = True
    ) -> dict:
        """
        Generates a test case for data quality healing testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            data_quality_issues: List of data quality issues to inject
            healing_strategies: List of healing strategies to apply
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, data, issues, healing actions, and expected corrections
        """
        from src.test.testcase_generators.schema_data_generator import generate_schema_data_pair
        
        # Generate base schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate healing actions
        healing_actions = self.generate_healing_actions(data_quality_issues, healing_strategies)
        
        # Inject data quality issues
        modified_schema, data_with_issues = inject_data_quality_issues(schema, clean_data, data_quality_issues)
        
        # Generate expected corrections
        expected_corrections = generate_expected_corrections(
            healing_actions, data_quality_issues, clean_data, data_with_issues
        )
        
        # Create test case
        test_case = {
            "test_case_name": test_case_name,
            "schema": schema,
            "clean_data": clean_data,
            "data_with_issues": data_with_issues,
            "healing_actions": healing_actions,
            "expected_corrections": expected_corrections,
            "data_quality_issues": data_quality_issues,
            "metadata": {
                "schema_config": schema_config,
                "data_config": data_config,
                "healing_strategies": healing_strategies
            }
        }
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
            file_paths = save_healing_test_case(test_case, test_case_name, self._output_dir, file_format)
            test_case["file_paths"] = file_paths
        
        return test_case
    
    def generate_pipeline_healing_test_case(
        self, 
        pipeline_config: dict, 
        pipeline_issues: list,
        healing_strategies: list,
        test_case_name: str,
        save_files: bool = True
    ) -> dict:
        """
        Generates a test case for pipeline healing testing.
        
        Args:
            pipeline_config: Configuration for the pipeline
            pipeline_issues: List of pipeline issues to inject
            healing_strategies: List of healing strategies to apply
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with pipeline configuration, issues, healing actions, and expected corrections
        """
        # Generate pipeline metadata
        if "pipeline_id" not in pipeline_config:
            pipeline_config["pipeline_id"] = f"pipeline-{uuid.uuid4()}"
        
        # Generate healing actions
        healing_actions = self.generate_healing_actions(pipeline_issues, healing_strategies)
        
        # Inject pipeline issues
        pipeline_issues_metadata = inject_pipeline_issues(pipeline_config, pipeline_issues)
        
        # Generate expected corrections (simplified for pipeline issues)
        expected_corrections = []
        for action in healing_actions:
            expected_corrections.append({
                "action_id": action["action_id"],
                "issue_type": action["issue_type"],
                "success": True,
                "confidence_score": action["confidence_score"],
                "details": {
                    "strategy": action["correction_strategy"],
                    "parameters": action["parameters"]
                }
            })
        
        # Create test case
        test_case = {
            "test_case_name": test_case_name,
            "pipeline_config": pipeline_config,
            "pipeline_issues": pipeline_issues_metadata,
            "healing_actions": healing_actions,
            "expected_corrections": expected_corrections,
            "metadata": {
                "pipeline_issues": pipeline_issues,
                "healing_strategies": healing_strategies
            }
        }
        
        # Save test case if requested
        if save_files:
            file_paths = save_healing_test_case(test_case, test_case_name, self._output_dir)
            test_case["file_paths"] = file_paths
        
        return test_case
    
    def generate_comprehensive_healing_test_suite(
        self, 
        suite_config: dict, 
        suite_name: str,
        save_files: bool = True
    ) -> dict:
        """
        Generates a comprehensive test suite with multiple healing test cases.
        
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
        
        suite_results = {
            "suite_name": suite_name,
            "test_cases": {},
            "manifest": {
                "data_quality_test_cases": [],
                "pipeline_test_cases": [],
                "mixed_test_cases": []
            }
        }
        
        # Generate data quality healing test cases
        if "data_quality_test_cases" in suite_config:
            for tc_config in suite_config["data_quality_test_cases"]:
                tc_name = tc_config.get("name", f"data_quality_{generate_unique_id()}")
                
                test_case = self.generate_data_quality_healing_test_case(
                    tc_config.get("schema_config", {}),
                    tc_config.get("data_config", {}),
                    tc_config.get("data_quality_issues", [{"type": "missing_values"}]),
                    tc_config.get("healing_strategies", ["mean_imputation"]),
                    tc_name,
                    save_files
                )
                
                suite_results["test_cases"][tc_name] = test_case
                suite_results["manifest"]["data_quality_test_cases"].append(tc_name)
        
        # Generate pipeline healing test cases
        if "pipeline_test_cases" in suite_config:
            for tc_config in suite_config["pipeline_test_cases"]:
                tc_name = tc_config.get("name", f"pipeline_{generate_unique_id()}")
                
                test_case = self.generate_pipeline_healing_test_case(
                    tc_config.get("pipeline_config", {}),
                    tc_config.get("pipeline_issues", [{"type": "resource_exhaustion"}]),
                    tc_config.get("healing_strategies", ["resource_scaling"]),
                    tc_name,
                    save_files
                )
                
                suite_results["test_cases"][tc_name] = test_case
                suite_results["manifest"]["pipeline_test_cases"].append(tc_name)
        
        # Generate mixed healing test cases (both data quality and pipeline issues)
        if "mixed_test_cases" in suite_config:
            for tc_config in suite_config["mixed_test_cases"]:
                tc_name = tc_config.get("name", f"mixed_{generate_unique_id()}")
                from src.test.testcase_generators.schema_data_generator import generate_schema_data_pair
                
                # Generate schema and data
                schema_config = tc_config.get("schema_config", {})
                data_config = tc_config.get("data_config", {})
                schema, clean_data = generate_schema_data_pair(schema_config, data_config)
                
                # Generate pipeline config
                pipeline_config = tc_config.get("pipeline_config", {"pipeline_id": f"pipeline-{uuid.uuid4()}"})
                
                # Generate issues and healing actions
                data_quality_issues = tc_config.get("data_quality_issues", [{"type": "missing_values"}])
                pipeline_issues = tc_config.get("pipeline_issues", [{"type": "resource_exhaustion"}])
                healing_strategies = tc_config.get("healing_strategies", ["mean_imputation", "resource_scaling"])
                
                # Inject issues
                modified_schema, data_with_issues = inject_data_quality_issues(schema, clean_data, data_quality_issues)
                pipeline_issues_metadata = inject_pipeline_issues(pipeline_config, pipeline_issues)
                
                # Generate healing actions
                healing_actions = self.generate_healing_actions(
                    data_quality_issues + pipeline_issues, healing_strategies
                )
                
                # Generate expected corrections
                expected_corrections = generate_expected_corrections(
                    healing_actions, data_quality_issues + pipeline_issues, clean_data, data_with_issues
                )
                
                # Create test case
                test_case = {
                    "test_case_name": tc_name,
                    "schema": schema,
                    "clean_data": clean_data,
                    "data_with_issues": data_with_issues,
                    "pipeline_config": pipeline_config,
                    "pipeline_issues": pipeline_issues_metadata,
                    "healing_actions": healing_actions,
                    "expected_corrections": expected_corrections,
                    "metadata": {
                        "schema_config": schema_config,
                        "data_config": data_config,
                        "data_quality_issues": data_quality_issues,
                        "pipeline_issues": pipeline_issues,
                        "healing_strategies": healing_strategies
                    }
                }
                
                # Save test case if requested
                if save_files:
                    file_format = data_config.get("file_format", DEFAULT_FILE_FORMAT)
                    file_paths = save_healing_test_case(test_case, tc_name, suite_dir, file_format)
                    test_case["file_paths"] = file_paths
                
                suite_results["test_cases"][tc_name] = test_case
                suite_results["manifest"]["mixed_test_cases"].append(tc_name)
        
        # Generate test suite manifest
        if save_files:
            manifest_path = os.path.join(suite_dir, "manifest.json")
            with open(manifest_path, 'w') as f:
                json.dump(suite_results["manifest"], f, indent=2)
            
            suite_results["manifest_path"] = manifest_path
        
        return suite_results
    
    def save_healing_test_case(self, test_case: dict, test_case_name: str, 
                             file_format: FileFormat = DEFAULT_FILE_FORMAT) -> dict:
        """
        Saves a healing test case to files.
        
        Args:
            test_case: Test case to save
            test_case_name: Name for the test case
            file_format: Format to save data files
            
        Returns:
            Updated test case with file paths
        """
        file_paths = save_healing_test_case(test_case, test_case_name, self._output_dir, file_format)
        test_case["file_paths"] = file_paths
        return test_case
    
    def load_healing_test_case(self, test_case_name: str, 
                             file_format: FileFormat = DEFAULT_FILE_FORMAT) -> dict:
        """
        Loads a previously saved healing test case.
        
        Args:
            test_case_name: Name of the test case
            file_format: Format of the data files
            
        Returns:
            Loaded test case
        """
        return load_healing_test_case(test_case_name, self._output_dir, file_format)
    
    def generate_healing_actions(self, issues: list, strategies: list) -> list:
        """
        Generates healing actions based on issue types and strategies.
        
        Args:
            issues: List of issues to heal
            strategies: List of healing strategies to use
            
        Returns:
            List of healing actions
        """
        healing_actions = []
        
        # Map strategies to issue types
        strategy_map = {}
        for issue in issues:
            issue_type = issue["type"]
            if issue_type not in strategy_map:
                strategy_map[issue_type] = []
            
            # Find applicable strategies for this issue type
            applicable_strategies = []
            if issue_type in CORRECTION_STRATEGIES:
                # Use strategies from CORRECTION_STRATEGIES if available
                applicable_strategies = [s for s in strategies if s in CORRECTION_STRATEGIES[issue_type]]
            else:
                # Otherwise use any strategy
                applicable_strategies = strategies
            
            if applicable_strategies:
                # Use specified strategies or choose random ones
                strategy = random.choice(applicable_strategies)
                strategy_map[issue_type].append(strategy)
        
        # Generate actions for each issue-strategy pair
        for issue_type, strategies in strategy_map.items():
            for strategy in strategies:
                # Prepare parameters based on issue type and strategy
                parameters = {}
                
                if issue_type == "missing_values":
                    columns = []
                    for issue in issues:
                        if issue["type"] == issue_type and "params" in issue and "columns" in issue["params"]:
                            columns.extend(issue["params"]["columns"])
                    
                    parameters = {
                        "columns": columns or ["column_name"],
                        "threshold": random.uniform(0.1, 0.5)
                    }
                    
                    if strategy == "constant_imputation":
                        parameters["value"] = 0  # Default value
                
                elif issue_type == "outliers":
                    columns = []
                    for issue in issues:
                        if issue["type"] == issue_type and "params" in issue and "columns" in issue["params"]:
                            columns.extend(issue["params"]["columns"])
                    
                    parameters = {
                        "columns": columns or ["numeric_column"],
                        "threshold": random.uniform(1.5, 3.0)
                    }
                
                elif issue_type == "format_errors":
                    columns = []
                    for issue in issues:
                        if issue["type"] == issue_type and "params" in issue and "columns" in issue["params"]:
                            columns.extend(issue["params"]["columns"])
                    
                    parameters = {
                        "columns": columns or ["string_column"],
                        "format_type": random.choice(["date", "number", "string"])
                    }
                
                elif issue_type == "schema_drift":
                    parameters = {
                        "drift_type": random.choice(["rename", "type_change", "missing_column"]),
                        "confidence_threshold": random.uniform(0.7, 0.9)
                    }
                
                elif issue_type == "resource_exhaustion":
                    parameters = {
                        "resource_type": random.choice(["memory", "cpu", "disk", "slots"]),
                        "scaling_factor": random.uniform(1.5, 3.0)
                    }
                
                # Generate the healing action
                action = generate_healing_action(issue_type, strategy, parameters)
                healing_actions.append(action)
        
        return healing_actions

class HealingTestCase:
    """
    Class representing a test case for self-healing AI testing.
    
    This class encapsulates all components of a healing test case, including
    schema, clean data, data with issues, healing actions, and expected corrections.
    """
    
    def __init__(
        self,
        schema: dict = None,
        clean_data: pd.DataFrame = None,
        data_with_issues: pd.DataFrame = None,
        healing_actions: list = None,
        expected_corrections: list = None,
        pipeline_issues: dict = None,
        metadata: dict = None
    ):
        """
        Initialize a HealingTestCase.
        
        Args:
            schema: Schema definition (None for pipeline-only test cases)
            clean_data: Clean data without issues (None for pipeline-only test cases)
            data_with_issues: Data with injected issues (None for pipeline-only test cases)
            healing_actions: List of healing actions to test
            expected_corrections: List of expected correction results
            pipeline_issues: Pipeline issues metadata (None for data-only test cases)
            metadata: Additional metadata about the test case
        """
        self.schema = schema
        self.clean_data = clean_data
        self.data_with_issues = data_with_issues
        self.healing_actions = healing_actions or []
        self.expected_corrections = expected_corrections or []
        self.pipeline_issues = pipeline_issues
        self.metadata = metadata or {}
        self.file_paths = {}
    
    def save(self, test_case_name: str, output_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> dict:
        """
        Save the test case to files.
        
        Args:
            test_case_name: Name for the test case
            output_dir: Directory to save the test case
            file_format: Format to save data files
            
        Returns:
            Dictionary with paths to saved files
        """
        file_paths = save_healing_test_case(self.to_dict(), test_case_name, output_dir, file_format)
        self.file_paths = file_paths
        return file_paths
    
    def to_dict(self) -> dict:
        """
        Convert the test case to a dictionary representation.
        
        Returns:
            Dictionary representation of the test case
        """
        test_case_dict = {
            "schema": self.schema,
            "healing_actions": self.healing_actions,
            "expected_corrections": self.expected_corrections,
            "metadata": self.metadata,
            "file_paths": self.file_paths
        }
        
        # Convert DataFrames to records if present
        if self.clean_data is not None:
            test_case_dict["clean_data"] = self.clean_data
        
        if self.data_with_issues is not None:
            test_case_dict["data_with_issues"] = self.data_with_issues
        
        if self.pipeline_issues is not None:
            test_case_dict["pipeline_issues"] = self.pipeline_issues
        
        return test_case_dict
    
    @classmethod
    def from_dict(cls, test_case_dict: dict) -> 'HealingTestCase':
        """
        Create a HealingTestCase from a dictionary.
        
        Args:
            test_case_dict: Dictionary representation of a test case
            
        Returns:
            HealingTestCase instance
        """
        schema = test_case_dict.get("schema")
        clean_data = test_case_dict.get("clean_data")
        data_with_issues = test_case_dict.get("data_with_issues")
        healing_actions = test_case_dict.get("healing_actions", [])
        expected_corrections = test_case_dict.get("expected_corrections", [])
        pipeline_issues = test_case_dict.get("pipeline_issues")
        metadata = test_case_dict.get("metadata", {})
        
        # Convert clean_data to DataFrame if it's a list
        if isinstance(clean_data, list):
            clean_data = pd.DataFrame(clean_data)
        
        # Convert data_with_issues to DataFrame if it's a list
        if isinstance(data_with_issues, list):
            data_with_issues = pd.DataFrame(data_with_issues)
        
        test_case = cls(
            schema=schema,
            clean_data=clean_data,
            data_with_issues=data_with_issues,
            healing_actions=healing_actions,
            expected_corrections=expected_corrections,
            pipeline_issues=pipeline_issues,
            metadata=metadata
        )
        
        # Set file paths if present
        if "file_paths" in test_case_dict:
            test_case.file_paths = test_case_dict["file_paths"]
        
        return test_case
    
    @classmethod
    def load(cls, test_case_name: str, input_dir: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> 'HealingTestCase':
        """
        Load a test case from files.
        
        Args:
            test_case_name: Name of the test case
            input_dir: Directory containing the test case
            file_format: Format of the data files
            
        Returns:
            HealingTestCase instance
        """
        test_case_dict = load_healing_test_case(test_case_name, input_dir, file_format)
        return cls.from_dict(test_case_dict)