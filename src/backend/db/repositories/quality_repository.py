"""
Repository class for managing data quality rules and validation results in the self-healing data pipeline.

This module provides a comprehensive repository implementation for storing, retrieving,
and analyzing quality rules and validation results. It supports the data quality validation 
framework and self-healing capabilities of the pipeline through efficient storage and 
retrieval patterns using both BigQuery (for analytical queries) and Firestore (for fast lookups).
"""

import datetime
import json
import typing
import pandas as pd
from google.cloud.exceptions import NotFound

from db.models.quality_rule import (
    QualityRule, 
    generate_rule_id,
    get_quality_rule_table_schema,
    QUALITY_RULE_TABLE_NAME
)
from db.models.quality_validation import (
    QualityValidation,
    generate_validation_id,
    get_quality_validation_table_schema,
    QUALITY_VALIDATION_TABLE_NAME
)
from utils.storage.bigquery_client import BigQueryClient
from utils.storage.firestore_client import FirestoreClient
from utils.logging.logger import get_logger
from constants import (
    ValidationRuleType, 
    QualityDimension,
    VALIDATION_STATUS_PASSED,
    VALIDATION_STATUS_FAILED,
    VALIDATION_STATUS_WARNING
)
from config import get_config

# Setup module logger
logger = get_logger(__name__)

# Constants for Firestore collections
QUALITY_RULES_COLLECTION = "quality_rules"
QUALITY_VALIDATIONS_COLLECTION = "quality_validations"


class QualityRepository:
    """Repository for managing quality rules and validation results in the self-healing data pipeline"""
    
    def __init__(self, bq_client=None, fs_client=None, project_id=None, dataset_id=None):
        """Initialize the quality repository with database clients

        Args:
            bq_client (BigQueryClient, optional): BigQuery client. Defaults to None (will create new instance).
            fs_client (FirestoreClient, optional): Firestore client. Defaults to None (will create new instance).
            project_id (str, optional): GCP project ID. Defaults to None (will get from config).
            dataset_id (str, optional): BigQuery dataset ID. Defaults to None (will get from config).
        """
        config = get_config()
        
        # Set BigQuery client
        self._bq_client = bq_client or BigQueryClient()
        
        # Set Firestore client
        self._fs_client = fs_client or FirestoreClient()
        
        # Set project and dataset IDs
        self._project_id = project_id or config.get_gcp_project_id()
        self._dataset_id = dataset_id or config.get_bigquery_dataset()
        
        # Get configuration
        self._config = config
        
        # Ensure storage is initialized
        self.initialize_storage()
        
        logger.info(f"Initialized QualityRepository with project {self._project_id}, dataset {self._dataset_id}")
    
    def initialize_storage(self) -> bool:
        """Ensures required BigQuery tables and Firestore collections exist

        Returns:
            bool: True if initialization successful
        """
        try:
            # Check and create BigQuery tables if needed
            rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
            validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
            
            # Check if tables exist, create if they don't
            try:
                self._bq_client.get_table(rules_table_id)
                logger.debug(f"Quality rules table already exists: {rules_table_id}")
            except NotFound:
                logger.info(f"Creating quality rules table: {rules_table_id}")
                self._bq_client.create_table(
                    rules_table_id,
                    get_quality_rule_table_schema(),
                    description="Quality validation rules for the self-healing data pipeline"
                )
                
            try:
                self._bq_client.get_table(validations_table_id)
                logger.debug(f"Quality validations table already exists: {validations_table_id}")
            except NotFound:
                logger.info(f"Creating quality validations table: {validations_table_id}")
                self._bq_client.create_table(
                    validations_table_id,
                    get_quality_validation_table_schema(),
                    description="Quality validation results for the self-healing data pipeline"
                )
            
            # Firestore collections are created automatically when documents are added
            logger.info("Quality repository storage initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize quality repository storage: {str(e)}")
            return False
    
    #
    # Quality Rule Methods
    #
    
    def create_quality_rule(
        self, 
        name: str, 
        rule_type: ValidationRuleType, 
        subtype: str, 
        dimension: QualityDimension, 
        description: str, 
        parameters: dict = None
    ) -> QualityRule:
        """Creates a new quality rule

        Args:
            name (str): Name of the quality rule
            rule_type (ValidationRuleType): Type of validation rule
            subtype (str): Specific rule subtype
            dimension (QualityDimension): Quality dimension the rule addresses
            description (str): Description of the rule
            parameters (dict, optional): Rule parameters. Defaults to None.

        Returns:
            QualityRule: Created quality rule
        """
        # Generate a new rule ID
        rule_id = generate_rule_id()
        
        # Create new QualityRule instance
        rule = QualityRule(
            name=name,
            rule_type=rule_type,
            subtype=subtype,
            dimension=dimension,
            description=description,
            parameters=parameters,
            rule_id=rule_id
        )
        
        # Insert into BigQuery
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        self._bq_client.insert_rows(rules_table_id, [rule.to_bigquery_row()])
        
        # Store in Firestore for faster retrieval
        self._fs_client.set_document(
            QUALITY_RULES_COLLECTION,
            rule_id,
            rule.to_dict()
        )
        
        logger.info(f"Created quality rule: {name} ({rule_id})")
        return rule
    
    def get_quality_rule(self, rule_id: str) -> typing.Optional[QualityRule]:
        """Retrieves a quality rule by ID

        Args:
            rule_id (str): ID of the quality rule to retrieve

        Returns:
            QualityRule: Retrieved quality rule or None if not found
        """
        # Try to get from Firestore first (faster)
        rule_dict = self._fs_client.get_document(QUALITY_RULES_COLLECTION, rule_id)
        
        if rule_dict:
            rule = QualityRule.from_dict(rule_dict)
            logger.debug(f"Retrieved quality rule from Firestore: {rule_id}")
            return rule
        
        # If not in Firestore, try BigQuery
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        query = f"""
        SELECT * FROM `{rules_table_id}`
        WHERE rule_id = @rule_id
        LIMIT 1
        """
        
        query_params = [
            {"name": "rule_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": rule_id}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        rows = list(results)
        if not rows:
            logger.warning(f"Quality rule not found: {rule_id}")
            return None
        
        # Convert row to QualityRule
        rule = QualityRule.from_bigquery_row(rows[0])
        
        # Store in Firestore for faster future retrievals
        self._fs_client.set_document(
            QUALITY_RULES_COLLECTION,
            rule_id,
            rule.to_dict()
        )
        
        logger.debug(f"Retrieved quality rule from BigQuery: {rule_id}")
        return rule
    
    def get_quality_rules(
        self, 
        rule_type: ValidationRuleType = None, 
        dimension: QualityDimension = None, 
        enabled_only: bool = True, 
        limit: int = None
    ) -> list:
        """Retrieves quality rules with optional filtering

        Args:
            rule_type (ValidationRuleType, optional): Filter by rule type. Defaults to None.
            dimension (QualityDimension, optional): Filter by quality dimension. Defaults to None.
            enabled_only (bool, optional): Whether to return only enabled rules. Defaults to True.
            limit (int, optional): Max number of rules to return. Defaults to None.

        Returns:
            list: List of QualityRule objects
        """
        # Construct query with filters
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        query = f"SELECT * FROM `{rules_table_id}` WHERE 1=1"
        query_params = []
        
        # Add filters
        if rule_type:
            query += " AND rule_type = @rule_type"
            query_params.append({
                "name": "rule_type", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": rule_type.value}
            })
        
        if dimension:
            query += " AND dimension = @dimension"
            query_params.append({
                "name": "dimension", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": dimension.value}
            })
        
        if enabled_only:
            query += " AND enabled = TRUE"
        
        # Add order by and limit
        query += " ORDER BY created_at DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Convert rows to QualityRule objects
        rules = [QualityRule.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(rules)} quality rules")
        return rules
    
    def update_quality_rule(self, rule_id: str, updates: dict) -> QualityRule:
        """Updates an existing quality rule

        Args:
            rule_id (str): ID of the rule to update
            updates (dict): Dictionary of fields to update

        Returns:
            QualityRule: Updated quality rule

        Raises:
            ValueError: If rule is not found
        """
        # Get existing rule
        rule = self.get_quality_rule(rule_id)
        if not rule:
            raise ValueError(f"Quality rule not found: {rule_id}")
        
        # Apply updates
        rule.update(updates)
        
        # Update in BigQuery by inserting an updated row
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        
        # First delete the existing rule
        delete_query = f"""
        DELETE FROM `{rules_table_id}`
        WHERE rule_id = @rule_id
        """
        delete_params = [
            {"name": "rule_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": rule_id}}
        ]
        self._bq_client.query(delete_query, delete_params)
        
        # Then insert the updated rule
        self._bq_client.insert_rows(rules_table_id, [rule.to_bigquery_row()])
        
        # Update in Firestore
        self._fs_client.set_document(
            QUALITY_RULES_COLLECTION,
            rule_id,
            rule.to_dict()
        )
        
        logger.info(f"Updated quality rule: {rule.name} ({rule_id})")
        return rule
    
    def delete_quality_rule(self, rule_id: str) -> bool:
        """Deletes a quality rule

        Args:
            rule_id (str): ID of the rule to delete

        Returns:
            bool: True if deletion successful
        """
        # Check if rule exists
        rule = self.get_quality_rule(rule_id)
        if not rule:
            logger.warning(f"Cannot delete quality rule that doesn't exist: {rule_id}")
            return False
        
        # Delete from BigQuery
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        query = f"""
        DELETE FROM `{rules_table_id}`
        WHERE rule_id = @rule_id
        """
        query_params = [
            {"name": "rule_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": rule_id}}
        ]
        
        self._bq_client.query(query, query_params)
        
        # Delete from Firestore
        self._fs_client.delete_document(QUALITY_RULES_COLLECTION, rule_id)
        
        logger.info(f"Deleted quality rule: {rule_id}")
        return True
    
    def enable_quality_rule(self, rule_id: str) -> bool:
        """Enables a quality rule

        Args:
            rule_id (str): ID of the rule to enable

        Returns:
            bool: True if enabling successful
        """
        # Get existing rule
        rule = self.get_quality_rule(rule_id)
        if not rule:
            logger.warning(f"Cannot enable quality rule that doesn't exist: {rule_id}")
            return False
        
        # Enable the rule
        rule.enable()
        
        # Update rule in storage
        try:
            self.update_quality_rule(rule_id, {"enabled": True})
            logger.info(f"Enabled quality rule: {rule_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to enable quality rule: {str(e)}")
            return False
    
    def disable_quality_rule(self, rule_id: str) -> bool:
        """Disables a quality rule

        Args:
            rule_id (str): ID of the rule to disable

        Returns:
            bool: True if disabling successful
        """
        # Get existing rule
        rule = self.get_quality_rule(rule_id)
        if not rule:
            logger.warning(f"Cannot disable quality rule that doesn't exist: {rule_id}")
            return False
        
        # Disable the rule
        rule.disable()
        
        # Update rule in storage
        try:
            self.update_quality_rule(rule_id, {"enabled": False})
            logger.info(f"Disabled quality rule: {rule_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to disable quality rule: {str(e)}")
            return False
    
    def update_rule_parameters(self, rule_id: str, parameters: dict) -> QualityRule:
        """Updates parameters for a quality rule

        Args:
            rule_id (str): ID of the rule
            parameters (dict): New parameters dictionary

        Returns:
            QualityRule: Updated quality rule

        Raises:
            ValueError: If rule is not found
        """
        # Get existing rule
        rule = self.get_quality_rule(rule_id)
        if not rule:
            raise ValueError(f"Quality rule not found: {rule_id}")
        
        # Update parameters
        rule.update_parameters(parameters)
        
        # Update rule in storage
        updated_rule = self.update_quality_rule(rule_id, {"parameters": parameters})
        logger.info(f"Updated parameters for rule: {rule_id}")
        return updated_rule
    
    def update_rule_metadata(self, rule_id: str, metadata: dict) -> QualityRule:
        """Updates metadata for a quality rule

        Args:
            rule_id (str): ID of the rule
            metadata (dict): New metadata dictionary

        Returns:
            QualityRule: Updated quality rule

        Raises:
            ValueError: If rule is not found
        """
        # Get existing rule
        rule = self.get_quality_rule(rule_id)
        if not rule:
            raise ValueError(f"Quality rule not found: {rule_id}")
        
        # Update metadata
        rule.update_metadata(metadata)
        
        # Update rule in storage
        updated_rule = self.update_quality_rule(rule_id, {"metadata": metadata})
        logger.info(f"Updated metadata for rule: {rule_id}")
        return updated_rule
    
    #
    # Quality Validation Methods
    #
    
    def create_validation(
        self, 
        execution_id: str, 
        rule_id: str, 
        status: str, 
        details: dict = None, 
        metrics: dict = None, 
        execution_time: float = None
    ) -> QualityValidation:
        """Creates a new quality validation record

        Args:
            execution_id (str): ID of the pipeline execution
            rule_id (str): ID of the quality rule
            status (str): Validation status (PASSED, FAILED, WARNING)
            details (dict, optional): Detailed validation results. Defaults to None.
            metrics (dict, optional): Validation metrics. Defaults to None.
            execution_time (float, optional): Time taken to execute validation. Defaults to None.

        Returns:
            QualityValidation: Created validation record
        """
        # Generate validation ID
        validation_id = generate_validation_id()
        
        # Create validation record
        validation = QualityValidation(validation_id, execution_id, rule_id, status)
        
        # Set additional fields
        if details:
            validation.set_details(details)
        
        if metrics:
            validation.set_metrics(metrics)
        
        if execution_time:
            validation.set_execution_time(execution_time)
        
        # Get rule metadata to associate with validation
        try:
            rule = self.get_quality_rule(rule_id)
            if rule:
                validation.set_rule_metadata(
                    rule.rule_type,
                    rule.dimension,
                    rule.get_metadata("severity", "MEDIUM")
                )
                
                # Set issue count if available in details
                if details and "issue_count" in details:
                    validation.set_issue_count(details["issue_count"])
        except Exception as e:
            logger.warning(f"Couldn't associate rule metadata with validation: {str(e)}")
        
        # Store in BigQuery
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        self._bq_client.insert_rows(validations_table_id, [validation.to_bigquery_row()])
        
        # Store in Firestore for faster retrieval
        self._fs_client.set_document(
            QUALITY_VALIDATIONS_COLLECTION,
            validation_id,
            validation.to_dict()
        )
        
        logger.info(f"Created validation record {validation_id} for execution {execution_id}, rule {rule_id}")
        return validation
    
    def get_validation(self, validation_id: str) -> typing.Optional[QualityValidation]:
        """Retrieves a validation record by ID

        Args:
            validation_id (str): ID of the validation to retrieve

        Returns:
            QualityValidation: Retrieved validation or None if not found
        """
        # Try to get from Firestore first (faster)
        validation_dict = self._fs_client.get_document(QUALITY_VALIDATIONS_COLLECTION, validation_id)
        
        if validation_dict:
            validation = QualityValidation.from_dict(validation_dict)
            logger.debug(f"Retrieved validation from Firestore: {validation_id}")
            return validation
        
        # If not in Firestore, try BigQuery
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        query = f"""
        SELECT * FROM `{validations_table_id}`
        WHERE validation_id = @validation_id
        LIMIT 1
        """
        
        query_params = [
            {"name": "validation_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": validation_id}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        rows = list(results)
        if not rows:
            logger.warning(f"Validation record not found: {validation_id}")
            return None
        
        # Convert row to QualityValidation
        validation = QualityValidation.from_bigquery_row(rows[0])
        
        # Store in Firestore for faster future retrievals
        self._fs_client.set_document(
            QUALITY_VALIDATIONS_COLLECTION,
            validation_id,
            validation.to_dict()
        )
        
        logger.debug(f"Retrieved validation from BigQuery: {validation_id}")
        return validation
    
    def get_validations_by_execution(
        self, 
        execution_id: str, 
        status: str = None, 
        requires_healing: bool = None
    ) -> list:
        """Retrieves validation records for a specific execution

        Args:
            execution_id (str): ID of the pipeline execution
            status (str, optional): Filter by validation status. Defaults to None.
            requires_healing (bool, optional): Filter by healing requirement. Defaults to None.

        Returns:
            list: List of QualityValidation objects
        """
        # Construct query with filters
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        query = f"""
        SELECT * FROM `{validations_table_id}`
        WHERE execution_id = @execution_id
        """
        
        query_params = [
            {"name": "execution_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution_id}}
        ]
        
        # Add optional filters
        if status:
            query += " AND status = @status"
            query_params.append({
                "name": "status", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": status}
            })
        
        if requires_healing is not None:
            query += " AND requires_healing = @requires_healing"
            query_params.append({
                "name": "requires_healing", 
                "parameterType": {"type": "BOOL"}, 
                "parameterValue": {"value": requires_healing}
            })
        
        query += " ORDER BY validation_time DESC"
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Convert rows to QualityValidation objects
        validations = [QualityValidation.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(validations)} validations for execution {execution_id}")
        return validations
    
    def get_validations_by_rule(
        self, 
        rule_id: str, 
        status: str = None, 
        limit: int = None
    ) -> list:
        """Retrieves validation records for a specific rule

        Args:
            rule_id (str): ID of the quality rule
            status (str, optional): Filter by validation status. Defaults to None.
            limit (int, optional): Max number of records to return. Defaults to None.

        Returns:
            list: List of QualityValidation objects
        """
        # Construct query with filters
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        query = f"""
        SELECT * FROM `{validations_table_id}`
        WHERE rule_id = @rule_id
        """
        
        query_params = [
            {"name": "rule_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": rule_id}}
        ]
        
        # Add optional filters
        if status:
            query += " AND status = @status"
            query_params.append({
                "name": "status", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": status}
            })
        
        query += " ORDER BY validation_time DESC"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Convert rows to QualityValidation objects
        validations = [QualityValidation.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(validations)} validations for rule {rule_id}")
        return validations
    
    def update_validation(self, validation_id: str, updates: dict) -> QualityValidation:
        """Updates an existing validation record

        Args:
            validation_id (str): ID of the validation to update
            updates (dict): Dictionary of fields to update

        Returns:
            QualityValidation: Updated validation record

        Raises:
            ValueError: If validation is not found
        """
        # Get existing validation
        validation = self.get_validation(validation_id)
        if not validation:
            raise ValueError(f"Validation record not found: {validation_id}")
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(validation, key):
                setattr(validation, key, value)
        
        # Update in BigQuery by replacing the row
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        
        # First delete the existing validation
        delete_query = f"""
        DELETE FROM `{validations_table_id}`
        WHERE validation_id = @validation_id
        """
        delete_params = [
            {"name": "validation_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": validation_id}}
        ]
        self._bq_client.query(delete_query, delete_params)
        
        # Then insert the updated validation
        self._bq_client.insert_rows(validations_table_id, [validation.to_bigquery_row()])
        
        # Update in Firestore
        self._fs_client.set_document(
            QUALITY_VALIDATIONS_COLLECTION,
            validation_id,
            validation.to_dict()
        )
        
        logger.info(f"Updated validation record: {validation_id}")
        return validation
    
    def mark_validation_for_healing(self, validation_id: str) -> bool:
        """Marks a validation as requiring self-healing

        Args:
            validation_id (str): ID of the validation

        Returns:
            bool: True if marking successful
        """
        # Get existing validation
        validation = self.get_validation(validation_id)
        if not validation:
            logger.warning(f"Cannot mark non-existent validation for healing: {validation_id}")
            return False
        
        # Mark for healing
        validation.mark_for_healing()
        
        # Update in storage
        try:
            self.update_validation(validation_id, {"requires_healing": True})
            logger.info(f"Marked validation {validation_id} for self-healing")
            return True
        except Exception as e:
            logger.error(f"Failed to mark validation for healing: {str(e)}")
            return False
    
    def set_validation_healing_id(self, validation_id: str, healing_id: str) -> bool:
        """Sets the healing ID for a validation that has been addressed

        Args:
            validation_id (str): ID of the validation
            healing_id (str): ID of the healing execution

        Returns:
            bool: True if update successful
        """
        # Get existing validation
        validation = self.get_validation(validation_id)
        if not validation:
            logger.warning(f"Cannot set healing ID for non-existent validation: {validation_id}")
            return False
        
        # Set healing ID
        validation.set_healing_id(healing_id)
        
        # Update in storage
        try:
            self.update_validation(validation_id, {"healing_id": healing_id})
            logger.info(f"Set healing ID {healing_id} for validation {validation_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to set healing ID: {str(e)}")
            return False
    
    def get_validations_requiring_healing(self, limit: int = None) -> list:
        """Retrieves validation records that require self-healing

        Args:
            limit (int, optional): Max number of records to return. Defaults to None.

        Returns:
            list: List of QualityValidation objects requiring healing
        """
        # Construct query with filters
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        query = f"""
        SELECT * FROM `{validations_table_id}`
        WHERE requires_healing = TRUE
        AND (healing_id IS NULL OR healing_id = '')
        """
        
        query += " ORDER BY validation_time ASC"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        results = self._bq_client.query(query)
        
        # Convert rows to QualityValidation objects
        validations = [QualityValidation.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(validations)} validations requiring healing")
        return validations
    
    #
    # Analysis and Metrics Methods
    #
    
    def get_quality_metrics(
        self, 
        start_time: datetime.datetime, 
        end_time: datetime.datetime,
        dimension: str = None,
        rule_type: str = None
    ) -> dict:
        """Retrieves quality metrics for a specific time period

        Args:
            start_time (datetime): Start of the time period
            end_time (datetime): End of the time period
            dimension (str, optional): Filter by quality dimension. Defaults to None.
            rule_type (str, optional): Filter by rule type. Defaults to None.

        Returns:
            dict: Quality metrics including pass rate, issue counts, etc.
        """
        # Construct query to calculate quality metrics
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        
        query = f"""
        WITH validation_stats AS (
            SELECT
                COUNT(*) as total_validations,
                COUNTIF(status = @passed_status) as passed_validations,
                COUNTIF(status = @failed_status) as failed_validations,
                COUNTIF(status = @warning_status) as warning_validations,
                COUNTIF(requires_healing = TRUE) as requires_healing_count,
                COUNTIF(healing_id IS NOT NULL) as healed_count,
                AVG(issue_count) as avg_issue_count,
                MAX(issue_count) as max_issue_count,
                SUM(issue_count) as total_issues
            FROM `{validations_table_id}`
            WHERE validation_time BETWEEN @start_time AND @end_time
        """
        
        query_params = [
            {"name": "passed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_PASSED}},
            {"name": "failed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_FAILED}},
            {"name": "warning_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_WARNING}},
            {"name": "start_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_time.isoformat()}},
            {"name": "end_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_time.isoformat()}},
        ]
        
        # Add optional filters
        if dimension:
            query += " AND dimension = @dimension"
            query_params.append({
                "name": "dimension", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": dimension}
            })
        
        if rule_type:
            query += " AND rule_type = @rule_type"
            query_params.append({
                "name": "rule_type", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": rule_type}
            })
        
        query += ")"
        
        # Calculate metrics
        query += """
        SELECT
            total_validations,
            passed_validations,
            failed_validations,
            warning_validations,
            requires_healing_count,
            healed_count,
            avg_issue_count,
            max_issue_count,
            total_issues,
            SAFE_DIVIDE(passed_validations, total_validations) * 100 as pass_rate,
            SAFE_DIVIDE(failed_validations, total_validations) * 100 as fail_rate,
            SAFE_DIVIDE(warning_validations, total_validations) * 100 as warning_rate,
            SAFE_DIVIDE(healed_count, requires_healing_count) * 100 as healing_rate
        FROM validation_stats
        """
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Construct metrics dictionary from results
        metrics = {}
        rows = list(results)
        if rows:
            row = rows[0]
            # Convert row to dictionary
            for key, value in row.items():
                metrics[key] = value
        
        logger.debug(f"Retrieved quality metrics for period {start_time} to {end_time}")
        return metrics
    
    def get_quality_trend(
        self, 
        start_time: datetime.datetime, 
        end_time: datetime.datetime,
        interval: str = "day",
        dimension: str = None
    ) -> pd.DataFrame:
        """Retrieves quality trend data over time

        Args:
            start_time (datetime): Start of the time period
            end_time (datetime): End of the time period
            interval (str, optional): Time grouping interval (hour, day, week, month). Defaults to "day".
            dimension (str, optional): Filter by quality dimension. Defaults to None.

        Returns:
            pandas.DataFrame: Time series data of quality metrics
        """
        # Map interval to SQL time grouping
        interval_mapping = {
            "hour": "TIMESTAMP_TRUNC(validation_time, HOUR)",
            "day": "TIMESTAMP_TRUNC(validation_time, DAY)",
            "week": "TIMESTAMP_TRUNC(validation_time, WEEK)",
            "month": "TIMESTAMP_TRUNC(validation_time, MONTH)"
        }
        
        time_group = interval_mapping.get(interval.lower(), "TIMESTAMP_TRUNC(validation_time, DAY)")
        
        # Construct query to calculate quality metrics by time interval
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        
        query = f"""
        SELECT
            {time_group} as time_period,
            COUNT(*) as total_validations,
            COUNTIF(status = @passed_status) as passed_validations,
            COUNTIF(status = @failed_status) as failed_validations,
            COUNTIF(status = @warning_status) as warning_validations,
            SAFE_DIVIDE(COUNTIF(status = @passed_status), COUNT(*)) * 100 as pass_rate,
            SAFE_DIVIDE(COUNTIF(status = @failed_status), COUNT(*)) * 100 as fail_rate,
            AVG(issue_count) as avg_issue_count,
            SUM(issue_count) as total_issues
        FROM `{validations_table_id}`
        WHERE validation_time BETWEEN @start_time AND @end_time
        """
        
        query_params = [
            {"name": "passed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_PASSED}},
            {"name": "failed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_FAILED}},
            {"name": "warning_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_WARNING}},
            {"name": "start_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_time.isoformat()}},
            {"name": "end_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_time.isoformat()}},
        ]
        
        # Add optional dimension filter
        if dimension:
            query += " AND dimension = @dimension"
            query_params.append({
                "name": "dimension", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": dimension}
            })
        
        # Group by time period and order
        query += f"""
        GROUP BY time_period
        ORDER BY time_period ASC
        """
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Convert to pandas DataFrame
        df = results.to_dataframe()
        
        logger.debug(f"Retrieved quality trend data with {len(df)} time periods")
        return df
    
    def get_failing_rules_summary(
        self, 
        start_time: datetime.datetime, 
        end_time: datetime.datetime,
        limit: int = 10
    ) -> list:
        """Retrieves summary of most frequently failing rules

        Args:
            start_time (datetime): Start of the time period
            end_time (datetime): End of the time period
            limit (int, optional): Max number of rules to return. Defaults to 10.

        Returns:
            list: List of rules with failure counts and details
        """
        # Construct query to get failing rules
        validations_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_VALIDATION_TABLE_NAME}"
        rules_table_id = f"{self._project_id}.{self._dataset_id}.{QUALITY_RULE_TABLE_NAME}"
        
        query = f"""
        SELECT
            v.rule_id,
            r.name as rule_name,
            r.rule_type,
            r.dimension,
            COUNT(*) as failure_count,
            AVG(v.issue_count) as avg_issue_count,
            MAX(v.issue_count) as max_issue_count,
            COUNTIF(v.requires_healing = TRUE) as requires_healing_count,
            COUNTIF(v.healing_id IS NOT NULL) as healed_count
        FROM `{validations_table_id}` v
        JOIN `{rules_table_id}` r
            ON v.rule_id = r.rule_id
        WHERE v.validation_time BETWEEN @start_time AND @end_time
        AND v.status = @failed_status
        GROUP BY v.rule_id, r.name, r.rule_type, r.dimension
        ORDER BY failure_count DESC
        """
        
        query_params = [
            {"name": "start_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_time.isoformat()}},
            {"name": "end_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_time.isoformat()}},
            {"name": "failed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": VALIDATION_STATUS_FAILED}}
        ]
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        results = self._bq_client.query(query, query_params)
        
        # Convert to list of dictionaries
        summary = []
        for row in results:
            healing_rate = 0
            if row.get("requires_healing_count", 0) > 0:
                healing_rate = (row.get("healed_count", 0) / row.get("requires_healing_count", 0)) * 100
                
            summary.append({
                "rule_id": row.get("rule_id"),
                "rule_name": row.get("rule_name"),
                "rule_type": row.get("rule_type"),
                "dimension": row.get("dimension"),
                "failure_count": row.get("failure_count"),
                "avg_issue_count": row.get("avg_issue_count"),
                "max_issue_count": row.get("max_issue_count"),
                "requires_healing_count": row.get("requires_healing_count"),
                "healed_count": row.get("healed_count"),
                "healing_rate": healing_rate
            })
        
        logger.debug(f"Retrieved summary of {len(summary)} failing rules")
        return summary
    
    def close(self) -> None:
        """Closes the repository and releases resources"""
        # Close clients if we created them internally
        logger.info("Closing quality repository")