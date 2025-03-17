import datetime
import typing
import json
import pandas as pd
from typing import List, Dict, Optional, Any, Tuple, Union

from ...constants import HealingActionType
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.storage.firestore_client import FirestoreClient

from ..models.healing_action import (
    HealingAction, 
    create_healing_action, 
    HEALING_ACTION_TABLE_NAME,
    get_healing_action_table_schema
)
from ..models.healing_execution import (
    HealingExecution, 
    create_healing_execution, 
    generate_healing_id,
    HEALING_EXECUTION_TABLE_NAME,
    get_healing_execution_table_schema
)
from ..models.issue_pattern import (
    IssuePattern, 
    create_issue_pattern, 
    generate_pattern_id,
    ISSUE_PATTERN_TABLE_NAME,
    get_issue_pattern_table_schema
)

# Initialize logger
logger = get_logger(__name__)

class HealingRepository:
    """Repository for managing healing actions, issue patterns, and healing executions in BigQuery and Firestore"""
    
    def __init__(self, bq_client: BigQueryClient, fs_client: FirestoreClient, 
                 dataset_id: str = None, project_id: str = None):
        """
        Initializes the HealingRepository with BigQuery and Firestore clients and configuration.
        
        Args:
            bq_client: BigQuery client for data storage
            fs_client: Firestore client for real-time data access
            dataset_id: BigQuery dataset ID (optional, can be loaded from config)
            project_id: GCP project ID (optional, can be loaded from config)
        """
        self._bq_client = bq_client
        self._fs_client = fs_client
        
        # Get dataset and project from config if not provided
        config = get_config()
        self._dataset_id = dataset_id or config.get_bigquery_dataset()
        self._project_id = project_id or config.get_gcp_project_id()
        
        # Ensure tables exist
        self.ensure_tables_exist()
        
        logger.info(f"HealingRepository initialized with dataset {self._dataset_id}")

    def ensure_tables_exist(self) -> bool:
        """
        Ensures that required tables exist in BigQuery for healing data.
        
        Returns:
            bool: True if tables exist or were created successfully
        """
        try:
            # Check and create healing actions table
            if not self._bq_client.table_exists(
                self._dataset_id, HEALING_ACTION_TABLE_NAME
            ):
                logger.info(f"Creating table {HEALING_ACTION_TABLE_NAME} in dataset {self._dataset_id}")
                self._bq_client.create_table(
                    self._dataset_id,
                    HEALING_ACTION_TABLE_NAME,
                    get_healing_action_table_schema()
                )
            
            # Check and create issue patterns table
            if not self._bq_client.table_exists(
                self._dataset_id, ISSUE_PATTERN_TABLE_NAME
            ):
                logger.info(f"Creating table {ISSUE_PATTERN_TABLE_NAME} in dataset {self._dataset_id}")
                self._bq_client.create_table(
                    self._dataset_id,
                    ISSUE_PATTERN_TABLE_NAME,
                    get_issue_pattern_table_schema()
                )
            
            # Check and create healing executions table
            if not self._bq_client.table_exists(
                self._dataset_id, HEALING_EXECUTION_TABLE_NAME
            ):
                logger.info(f"Creating table {HEALING_EXECUTION_TABLE_NAME} in dataset {self._dataset_id}")
                self._bq_client.create_table(
                    self._dataset_id,
                    HEALING_EXECUTION_TABLE_NAME,
                    get_healing_execution_table_schema()
                )
                
            logger.info("All healing tables exist in BigQuery")
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {str(e)}")
            return False

    #
    # Healing Action Methods
    #
    def create_healing_action(
        self, 
        name: str, 
        action_type: HealingActionType, 
        description: str, 
        action_parameters: Dict[str, Any], 
        pattern_id: str
    ) -> HealingAction:
        """
        Creates a new healing action in the database.
        
        Args:
            name: Name of the healing action
            action_type: Type of healing action
            description: Description of what the action does
            action_parameters: Parameters required for executing the action
            pattern_id: ID of the issue pattern this action is associated with
            
        Returns:
            Created HealingAction object
        """
        try:
            # Create HealingAction object
            action = create_healing_action(
                name=name,
                action_type=action_type,
                description=description,
                action_parameters=action_parameters,
                pattern_id=pattern_id
            )
            
            # Insert into BigQuery
            row = action.to_dict()
            self._bq_client.insert_rows(
                self._dataset_id,
                HEALING_ACTION_TABLE_NAME,
                [row]
            )
            
            # Store in Firestore for fast access
            self._fs_client.set_document(
                f"healing_actions/{action.action_id}",
                row
            )
            
            logger.info(f"Created healing action {action.action_id} of type {action_type.value} for pattern {pattern_id}")
            return action
            
        except Exception as e:
            logger.error(f"Error creating healing action: {str(e)}")
            raise

    def batch_create_healing_actions(self, actions: List[HealingAction]) -> List[HealingAction]:
        """
        Creates multiple healing actions in a single batch operation.
        
        Args:
            actions: List of HealingAction objects to create
            
        Returns:
            List of created HealingAction objects
        """
        try:
            # Validate actions
            for action in actions:
                if not action.name or not action.action_type or not action.pattern_id:
                    logger.error("Invalid healing action missing required fields")
                    raise ValueError("Healing action missing required fields")
            
            # Insert rows into BigQuery
            rows = [action.to_dict() for action in actions]
            self._bq_client.insert_rows(
                self._dataset_id,
                HEALING_ACTION_TABLE_NAME,
                rows
            )
            
            # Store in Firestore for fast access
            for action in actions:
                self._fs_client.set_document(
                    f"healing_actions/{action.action_id}",
                    action.to_dict()
                )
            
            logger.info(f"Created {len(actions)} healing actions in batch")
            return actions
            
        except Exception as e:
            logger.error(f"Error batch creating healing actions: {str(e)}")
            raise
    
    def get_healing_action(self, action_id: str) -> Optional[HealingAction]:
        """
        Retrieves a healing action by its ID.
        
        Args:
            action_id: ID of the healing action to retrieve
            
        Returns:
            HealingAction object if found, None otherwise
        """
        try:
            # Try Firestore first for faster retrieval
            fs_doc = self._fs_client.get_document(f"healing_actions/{action_id}")
            if fs_doc and fs_doc.exists:
                return HealingAction.from_dict(fs_doc.to_dict())
            
            # Not found in Firestore, query BigQuery
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
                WHERE action_id = @action_id
            """
            
            query_params = [
                {"name": "action_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action_id}}
            ]
            
            results = self._bq_client.query(query, query_params)
            rows = list(results)
            
            if not rows:
                logger.info(f"Healing action not found: {action_id}")
                return None
            
            # Convert to HealingAction object
            action = HealingAction.from_dict(dict(rows[0]))
            
            # Cache in Firestore for future retrievals
            self._fs_client.set_document(
                f"healing_actions/{action_id}",
                action.to_dict()
            )
            
            logger.info(f"Retrieved healing action: {action_id}")
            return action
            
        except Exception as e:
            logger.error(f"Error retrieving healing action {action_id}: {str(e)}")
            return None
    
    def update_healing_action(self, action: HealingAction) -> bool:
        """
        Updates an existing healing action in the database.
        
        Args:
            action: HealingAction object with updated fields
            
        Returns:
            True if update was successful
        """
        try:
            # Validate action
            if not action.action_id or not action.name or not action.action_type:
                logger.error("Invalid healing action missing required fields")
                raise ValueError("Healing action missing required fields")
            
            # Update in BigQuery
            action_dict = action.to_dict()
            
            query = f"""
                UPDATE `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
                SET 
                    name = @name,
                    action_type = @action_type,
                    description = @description,
                    action_parameters = @action_parameters,
                    pattern_id = @pattern_id,
                    execution_count = @execution_count,
                    success_count = @success_count,
                    success_rate = @success_rate,
                    is_active = @is_active,
                    last_executed = @last_executed,
                    updated_at = @updated_at
                WHERE action_id = @action_id
            """
            
            # Create query parameters
            query_params = [
                {"name": "action_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action.action_id}},
                {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action.name}},
                {"name": "action_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action.action_type.value}},
                {"name": "description", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action.description or ""}},
                {"name": "action_parameters", "parameterType": {"type": "JSON"}, "parameterValue": {"value": json.dumps(action.action_parameters)}},
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action.pattern_id}},
                {"name": "execution_count", "parameterType": {"type": "INT64"}, "parameterValue": {"value": action.execution_count}},
                {"name": "success_count", "parameterType": {"type": "INT64"}, "parameterValue": {"value": action.success_count}},
                {"name": "success_rate", "parameterType": {"type": "FLOAT64"}, "parameterValue": {"value": action.success_rate}},
                {"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": action.is_active}},
                {"name": "updated_at", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": action.updated_at.isoformat()}}
            ]
            
            # Handle nullable fields
            if action.last_executed:
                query_params.append({
                    "name": "last_executed", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": action.last_executed.isoformat()}
                })
            else:
                query_params.append({
                    "name": "last_executed", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": None}
                })
            
            # Execute update
            self._bq_client.query(query, query_params)
            
            # Update in Firestore
            self._fs_client.set_document(
                f"healing_actions/{action.action_id}",
                action_dict
            )
            
            logger.info(f"Updated healing action: {action.action_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating healing action: {str(e)}")
            return False
    
    def delete_healing_action(self, action_id: str) -> bool:
        """
        Deletes a healing action from the database.
        
        Args:
            action_id: ID of the healing action to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            # Delete from BigQuery
            query = f"""
                DELETE FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
                WHERE action_id = @action_id
            """
            
            query_params = [
                {"name": "action_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action_id}}
            ]
            
            self._bq_client.query(query, query_params)
            
            # Delete from Firestore
            self._fs_client.delete_document(f"healing_actions/{action_id}")
            
            logger.info(f"Deleted healing action: {action_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting healing action {action_id}: {str(e)}")
            return False

    def get_healing_actions_by_type(
        self, 
        action_type: HealingActionType, 
        active_only: bool = True,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingAction]:
        """
        Retrieves healing actions filtered by action type.
        
        Args:
            action_type: Type of healing actions to retrieve
            active_only: Whether to return only active actions
            limit: Maximum number of actions to return
            offset: Number of actions to skip
            
        Returns:
            List of HealingAction objects matching the type
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
                WHERE action_type = @action_type
            """
            
            query_params = [
                {"name": "action_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action_type.value}}
            ]
            
            if active_only:
                query += " AND is_active = @is_active"
                query_params.append(
                    {"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": True}}
                )
            
            # Add ordering and pagination
            query += """
                ORDER BY created_at DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingAction objects
            actions = [HealingAction.from_dict(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(actions)} healing actions of type {action_type.value}")
            return actions
            
        except Exception as e:
            logger.error(f"Error retrieving healing actions by type {action_type.value}: {str(e)}")
            return []
    
    def get_healing_actions_by_pattern(
        self, 
        pattern_id: str, 
        active_only: bool = True,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingAction]:
        """
        Retrieves healing actions associated with a specific issue pattern.
        
        Args:
            pattern_id: ID of the issue pattern
            active_only: Whether to return only active actions
            limit: Maximum number of actions to return
            offset: Number of actions to skip
            
        Returns:
            List of HealingAction objects for the pattern
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
                WHERE pattern_id = @pattern_id
            """
            
            query_params = [
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern_id}}
            ]
            
            if active_only:
                query += " AND is_active = @is_active"
                query_params.append(
                    {"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": True}}
                )
            
            # Add ordering and pagination
            query += """
                ORDER BY success_rate DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingAction objects
            actions = [HealingAction.from_dict(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(actions)} healing actions for pattern {pattern_id}")
            return actions
            
        except Exception as e:
            logger.error(f"Error retrieving healing actions for pattern {pattern_id}: {str(e)}")
            return []
    
    def get_all_healing_actions(
        self, 
        active_only: bool = True,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingAction]:
        """
        Retrieves all healing actions with optional filtering.
        
        Args:
            active_only: Whether to return only active actions
            limit: Maximum number of actions to return
            offset: Number of actions to skip
            
        Returns:
            List of HealingAction objects
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}`
            """
            
            query_params = []
            
            if active_only:
                query += " WHERE is_active = @is_active"
                query_params.append(
                    {"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": True}}
                )
            
            # Add ordering and pagination
            query += """
                ORDER BY created_at DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingAction objects
            actions = [HealingAction.from_dict(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(actions)} healing actions")
            return actions
            
        except Exception as e:
            logger.error(f"Error retrieving healing actions: {str(e)}")
            return []

    def update_healing_action_stats(self, action_id: str, success: bool) -> bool:
        """
        Updates the statistics of a healing action based on execution results.
        
        Args:
            action_id: ID of the healing action
            success: Whether the execution was successful
            
        Returns:
            True if update was successful
        """
        try:
            # Get the action
            action = self.get_healing_action(action_id)
            if not action:
                logger.error(f"Healing action not found: {action_id}")
                return False
            
            # Update the stats
            action.update_stats(success)
            
            # Save the updated action
            return self.update_healing_action(action)
            
        except Exception as e:
            logger.error(f"Error updating healing action stats {action_id}: {str(e)}")
            return False
    
    def activate_healing_action(self, action_id: str) -> bool:
        """
        Activates a healing action.
        
        Args:
            action_id: ID of the healing action to activate
            
        Returns:
            True if activation was successful
        """
        try:
            # Get the action
            action = self.get_healing_action(action_id)
            if not action:
                logger.error(f"Healing action not found: {action_id}")
                return False
            
            # Activate the action
            action.activate()
            
            # Save the updated action
            return self.update_healing_action(action)
            
        except Exception as e:
            logger.error(f"Error activating healing action {action_id}: {str(e)}")
            return False
    
    def deactivate_healing_action(self, action_id: str) -> bool:
        """
        Deactivates a healing action.
        
        Args:
            action_id: ID of the healing action to deactivate
            
        Returns:
            True if deactivation was successful
        """
        try:
            # Get the action
            action = self.get_healing_action(action_id)
            if not action:
                logger.error(f"Healing action not found: {action_id}")
                return False
            
            # Deactivate the action
            action.deactivate()
            
            # Save the updated action
            return self.update_healing_action(action)
            
        except Exception as e:
            logger.error(f"Error deactivating healing action {action_id}: {str(e)}")
            return False
    
    def update_healing_action_parameters(self, action_id: str, parameters: Dict[str, Any]) -> bool:
        """
        Updates the parameters of a healing action.
        
        Args:
            action_id: ID of the healing action
            parameters: New parameters to set
            
        Returns:
            True if update was successful
        """
        try:
            # Get the action
            action = self.get_healing_action(action_id)
            if not action:
                logger.error(f"Healing action not found: {action_id}")
                return False
            
            # Update the parameters
            action.update_parameters(parameters)
            
            # Save the updated action
            return self.update_healing_action(action)
            
        except Exception as e:
            logger.error(f"Error updating healing action parameters {action_id}: {str(e)}")
            return False
    
    #
    # Issue Pattern Methods
    #
    def create_issue_pattern(
        self, 
        name: str, 
        pattern_type: str, 
        description: str, 
        features: Dict[str, Any], 
        confidence_threshold: float = None
    ) -> IssuePattern:
        """
        Creates a new issue pattern in the database.
        
        Args:
            name: Name of the pattern
            pattern_type: Type of pattern (data_quality, pipeline, system, resource)
            description: Description of the pattern
            features: Dictionary of pattern detection features
            confidence_threshold: Minimum confidence threshold for matching
            
        Returns:
            Created IssuePattern object
        """
        try:
            # Create IssuePattern object
            pattern = create_issue_pattern(
                name=name,
                pattern_type=pattern_type,
                description=description,
                features=features,
                confidence_threshold=confidence_threshold
            )
            
            # Insert into BigQuery
            row = pattern.to_bigquery_row()
            self._bq_client.insert_rows(
                self._dataset_id,
                ISSUE_PATTERN_TABLE_NAME,
                [row]
            )
            
            # Store in Firestore for fast access
            self._fs_client.set_document(
                f"issue_patterns/{pattern.pattern_id}",
                pattern.to_dict()
            )
            
            logger.info(f"Created issue pattern {pattern.pattern_id} of type {pattern_type}")
            return pattern
            
        except Exception as e:
            logger.error(f"Error creating issue pattern: {str(e)}")
            raise
    
    def batch_create_issue_patterns(self, patterns: List[IssuePattern]) -> List[IssuePattern]:
        """
        Creates multiple issue patterns in a single batch operation.
        
        Args:
            patterns: List of IssuePattern objects to create
            
        Returns:
            List of created IssuePattern objects
        """
        try:
            # Validate patterns
            for pattern in patterns:
                if not pattern.name or not pattern.pattern_type or not pattern.features:
                    logger.error("Invalid issue pattern missing required fields")
                    raise ValueError("Issue pattern missing required fields")
            
            # Insert rows into BigQuery
            rows = [pattern.to_bigquery_row() for pattern in patterns]
            self._bq_client.insert_rows(
                self._dataset_id,
                ISSUE_PATTERN_TABLE_NAME,
                rows
            )
            
            # Store in Firestore for fast access
            for pattern in patterns:
                self._fs_client.set_document(
                    f"issue_patterns/{pattern.pattern_id}",
                    pattern.to_dict()
                )
            
            logger.info(f"Created {len(patterns)} issue patterns in batch")
            return patterns
            
        except Exception as e:
            logger.error(f"Error batch creating issue patterns: {str(e)}")
            raise
    
    def get_issue_pattern(self, pattern_id: str) -> Optional[IssuePattern]:
        """
        Retrieves an issue pattern by its ID.
        
        Args:
            pattern_id: ID of the issue pattern to retrieve
            
        Returns:
            IssuePattern object if found, None otherwise
        """
        try:
            # Try Firestore first for faster retrieval
            fs_doc = self._fs_client.get_document(f"issue_patterns/{pattern_id}")
            if fs_doc and fs_doc.exists:
                return IssuePattern.from_dict(fs_doc.to_dict())
            
            # Not found in Firestore, query BigQuery
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                WHERE pattern_id = @pattern_id
            """
            
            query_params = [
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern_id}}
            ]
            
            results = self._bq_client.query(query, query_params)
            rows = list(results)
            
            if not rows:
                logger.info(f"Issue pattern not found: {pattern_id}")
                return None
            
            # Convert to IssuePattern object
            pattern = IssuePattern.from_bigquery_row(dict(rows[0]))
            
            # Cache in Firestore for future retrievals
            self._fs_client.set_document(
                f"issue_patterns/{pattern_id}",
                pattern.to_dict()
            )
            
            logger.info(f"Retrieved issue pattern: {pattern_id}")
            return pattern
            
        except Exception as e:
            logger.error(f"Error retrieving issue pattern {pattern_id}: {str(e)}")
            return None
    
    def update_issue_pattern(self, pattern: IssuePattern) -> bool:
        """
        Updates an existing issue pattern in the database.
        
        Args:
            pattern: IssuePattern object with updated fields
            
        Returns:
            True if update was successful
        """
        try:
            # Validate pattern
            if not pattern.pattern_id or not pattern.name or not pattern.pattern_type:
                logger.error("Invalid issue pattern missing required fields")
                raise ValueError("Issue pattern missing required fields")
            
            # Update in BigQuery
            row = pattern.to_bigquery_row()
            
            query = f"""
                UPDATE `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                SET 
                    name = @name,
                    pattern_type = @pattern_type,
                    description = @description,
                    features = @features,
                    confidence_threshold = @confidence_threshold,
                    occurrence_count = @occurrence_count,
                    success_rate = @success_rate,
                    last_seen = @last_seen,
                    updated_at = @updated_at
                WHERE pattern_id = @pattern_id
            """
            
            # Create query parameters
            query_params = [
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern.pattern_id}},
                {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern.name}},
                {"name": "pattern_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern.pattern_type}},
                {"name": "description", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern.description or ""}},
                {"name": "features", "parameterType": {"type": "JSON"}, "parameterValue": {"value": json.dumps(pattern.features)}},
                {"name": "confidence_threshold", "parameterType": {"type": "FLOAT64"}, "parameterValue": {"value": pattern.confidence_threshold}},
                {"name": "occurrence_count", "parameterType": {"type": "INT64"}, "parameterValue": {"value": pattern.occurrence_count}},
                {"name": "success_rate", "parameterType": {"type": "FLOAT64"}, "parameterValue": {"value": pattern.success_rate}},
                {"name": "last_seen", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": pattern.last_seen.isoformat()}},
                {"name": "updated_at", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": pattern.updated_at.isoformat()}}
            ]
            
            # Execute update
            self._bq_client.query(query, query_params)
            
            # Update in Firestore
            self._fs_client.set_document(
                f"issue_patterns/{pattern.pattern_id}",
                pattern.to_dict()
            )
            
            logger.info(f"Updated issue pattern: {pattern.pattern_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating issue pattern: {str(e)}")
            return False
    
    def delete_issue_pattern(self, pattern_id: str) -> bool:
        """
        Deletes an issue pattern from the database.
        
        Args:
            pattern_id: ID of the issue pattern to delete
            
        Returns:
            True if deletion was successful
        """
        try:
            # Delete from BigQuery
            query = f"""
                DELETE FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                WHERE pattern_id = @pattern_id
            """
            
            query_params = [
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern_id}}
            ]
            
            self._bq_client.query(query, query_params)
            
            # Delete from Firestore
            self._fs_client.delete_document(f"issue_patterns/{pattern_id}")
            
            logger.info(f"Deleted issue pattern: {pattern_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting issue pattern {pattern_id}: {str(e)}")
            return False
    
    def get_issue_patterns_by_type(
        self, 
        pattern_type: str,
        limit: int = 100, 
        offset: int = 0
    ) -> List[IssuePattern]:
        """
        Retrieves issue patterns filtered by pattern type.
        
        Args:
            pattern_type: Type of patterns to retrieve
            limit: Maximum number of patterns to return
            offset: Number of patterns to skip
            
        Returns:
            List of IssuePattern objects matching the type
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                WHERE pattern_type = @pattern_type
                ORDER BY success_rate DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params = [
                {"name": "pattern_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern_type}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to IssuePattern objects
            patterns = [IssuePattern.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(patterns)} issue patterns of type {pattern_type}")
            return patterns
            
        except Exception as e:
            logger.error(f"Error retrieving issue patterns by type {pattern_type}: {str(e)}")
            return []
    
    def get_all_issue_patterns(
        self, 
        limit: int = 100, 
        offset: int = 0
    ) -> List[IssuePattern]:
        """
        Retrieves all issue patterns with optional filtering.
        
        Args:
            limit: Maximum number of patterns to return
            offset: Number of patterns to skip
            
        Returns:
            List of IssuePattern objects
        """
        try:
            # Build query
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                ORDER BY created_at DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params = [
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to IssuePattern objects
            patterns = [IssuePattern.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(patterns)} issue patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error retrieving issue patterns: {str(e)}")
            return []
    
    def update_issue_pattern_stats(self, pattern_id: str, healing_success: bool) -> bool:
        """
        Updates the statistics of an issue pattern based on detection and healing results.
        
        Args:
            pattern_id: ID of the issue pattern
            healing_success: Whether the healing action was successful
            
        Returns:
            True if update was successful
        """
        try:
            # Get the pattern
            pattern = self.get_issue_pattern(pattern_id)
            if not pattern:
                logger.error(f"Issue pattern not found: {pattern_id}")
                return False
            
            # Update the stats
            pattern.update_stats(healing_success)
            
            # Save the updated pattern
            return self.update_issue_pattern(pattern)
            
        except Exception as e:
            logger.error(f"Error updating issue pattern stats {pattern_id}: {str(e)}")
            return False
    
    def find_matching_patterns(
        self, 
        issue_data: Dict[str, Any], 
        min_confidence: float = None,
        limit: int = 10
    ) -> List[Tuple[IssuePattern, float]]:
        """
        Finds issue patterns that match a given issue.
        
        Args:
            issue_data: Issue data to match against patterns
            min_confidence: Minimum confidence threshold for matches
            limit: Maximum number of matching patterns to return
            
        Returns:
            List of tuples with (IssuePattern, confidence_score) sorted by confidence
        """
        try:
            # Get all patterns
            all_patterns = self.get_all_issue_patterns(limit=1000)
            
            # Check each pattern for a match
            matches = []
            for pattern in all_patterns:
                matches_issue, confidence = pattern.matches_issue(issue_data, min_confidence)
                if matches_issue:
                    matches.append((pattern, confidence))
            
            # Sort by confidence score (descending)
            matches.sort(key=lambda x: x[1], reverse=True)
            
            # Limit the number of results
            if limit and limit < len(matches):
                matches = matches[:limit]
            
            logger.info(f"Found {len(matches)} matching patterns for issue")
            return matches
            
        except Exception as e:
            logger.error(f"Error finding matching patterns: {str(e)}")
            return []
    
    #
    # Healing Execution Methods
    #
    def create_healing_execution(
        self, 
        execution_id: str, 
        pattern_id: str, 
        action_id: str, 
        issue_details: Dict[str, Any],
        validation_id: str = None
    ) -> HealingExecution:
        """
        Creates a new healing execution record in the database.
        
        Args:
            execution_id: ID of the pipeline execution
            pattern_id: ID of the detected issue pattern
            action_id: ID of the healing action being applied
            issue_details: Details about the issue being addressed
            validation_id: Optional ID of the validation that triggered healing
            
        Returns:
            Created HealingExecution object
        """
        try:
            # Create HealingExecution object
            healing_exec = create_healing_execution(
                execution_id=execution_id,
                pattern_id=pattern_id,
                action_id=action_id,
                issue_details=issue_details,
                validation_id=validation_id
            )
            
            # Insert into BigQuery
            row = healing_exec.to_bigquery_row()
            self._bq_client.insert_rows(
                self._dataset_id,
                HEALING_EXECUTION_TABLE_NAME,
                [row]
            )
            
            # Store in Firestore for fast access
            self._fs_client.set_document(
                f"healing_executions/{healing_exec.healing_id}",
                healing_exec.to_dict()
            )
            
            logger.info(f"Created healing execution {healing_exec.healing_id} for execution {execution_id}, pattern {pattern_id}, action {action_id}")
            return healing_exec
            
        except Exception as e:
            logger.error(f"Error creating healing execution: {str(e)}")
            raise
    
    def get_healing_execution(self, healing_id: str) -> Optional[HealingExecution]:
        """
        Retrieves a healing execution by its ID.
        
        Args:
            healing_id: ID of the healing execution to retrieve
            
        Returns:
            HealingExecution object if found, None otherwise
        """
        try:
            # Try Firestore first for faster retrieval
            fs_doc = self._fs_client.get_document(f"healing_executions/{healing_id}")
            if fs_doc and fs_doc.exists:
                return HealingExecution.from_dict(fs_doc.to_dict())
            
            # Not found in Firestore, query BigQuery
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                WHERE healing_id = @healing_id
            """
            
            query_params = [
                {"name": "healing_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": healing_id}}
            ]
            
            results = self._bq_client.query(query, query_params)
            rows = list(results)
            
            if not rows:
                logger.info(f"Healing execution not found: {healing_id}")
                return None
            
            # Convert to HealingExecution object
            healing_exec = HealingExecution.from_bigquery_row(dict(rows[0]))
            
            # Cache in Firestore for future retrievals
            self._fs_client.set_document(
                f"healing_executions/{healing_id}",
                healing_exec.to_dict()
            )
            
            logger.info(f"Retrieved healing execution: {healing_id}")
            return healing_exec
            
        except Exception as e:
            logger.error(f"Error retrieving healing execution {healing_id}: {str(e)}")
            return None
    
    def update_healing_execution(self, execution: HealingExecution) -> bool:
        """
        Updates an existing healing execution in the database.
        
        Args:
            execution: HealingExecution object with updated fields
            
        Returns:
            True if update was successful
        """
        try:
            # Validate execution
            if not execution.healing_id or not execution.execution_id:
                logger.error("Invalid healing execution missing required fields")
                raise ValueError("Healing execution missing required fields")
            
            # Update in BigQuery
            row = execution.to_bigquery_row()
            
            # Build query with all fields
            query = f"""
                UPDATE `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                SET 
                    execution_id = @execution_id,
                    pattern_id = @pattern_id,
                    action_id = @action_id,
                    validation_id = @validation_id,
                    status = @status,
                    execution_time = @execution_time,
                    completion_time = @completion_time,
                    successful = @successful,
                    confidence_score = @confidence_score,
                    issue_details = @issue_details,
                    execution_details = @execution_details,
                    metrics = @metrics
                WHERE healing_id = @healing_id
            """
            
            # Create query parameters
            query_params = [
                {"name": "healing_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.healing_id}},
                {"name": "execution_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.execution_id}},
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.pattern_id}},
                {"name": "action_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.action_id}},
                {"name": "status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.status}},
                {"name": "execution_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": execution.execution_time.isoformat()}},
                {"name": "issue_details", "parameterType": {"type": "JSON"}, "parameterValue": {"value": json.dumps(execution.issue_details)}},
                {"name": "execution_details", "parameterType": {"type": "JSON"}, "parameterValue": {"value": json.dumps(execution.execution_details)}},
                {"name": "metrics", "parameterType": {"type": "JSON"}, "parameterValue": {"value": json.dumps(execution.metrics)}}
            ]
            
            # Handle nullable fields
            query_params.extend([
                {"name": "validation_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution.validation_id}},
                {"name": "completion_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": execution.completion_time.isoformat() if execution.completion_time else None}},
                {"name": "successful", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": execution.successful}},
                {"name": "confidence_score", "parameterType": {"type": "FLOAT64"}, "parameterValue": {"value": execution.confidence_score}}
            ])
            
            # Execute update
            self._bq_client.query(query, query_params)
            
            # Update in Firestore
            self._fs_client.set_document(
                f"healing_executions/{execution.healing_id}",
                execution.to_dict()
            )
            
            logger.info(f"Updated healing execution: {execution.healing_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating healing execution: {str(e)}")
            return False
    
    def start_healing_execution(self, healing_id: str, confidence_score: float) -> bool:
        """
        Marks a healing execution as started with confidence score.
        
        Args:
            healing_id: ID of the healing execution
            confidence_score: Confidence score for the healing action
            
        Returns:
            True if update was successful
        """
        try:
            # Get the execution
            execution = self.get_healing_execution(healing_id)
            if not execution:
                logger.error(f"Healing execution not found: {healing_id}")
                return False
            
            # Start the execution
            execution.start_execution(confidence_score)
            
            # Save the updated execution
            return self.update_healing_execution(execution)
            
        except Exception as e:
            logger.error(f"Error starting healing execution {healing_id}: {str(e)}")
            return False
    
    def complete_healing_execution(
        self, 
        healing_id: str, 
        successful: bool, 
        execution_details: Dict[str, Any] = None, 
        metrics: Dict[str, Any] = None
    ) -> bool:
        """
        Marks a healing execution as complete with outcome and details.
        
        Args:
            healing_id: ID of the healing execution
            successful: Whether the healing was successful
            execution_details: Optional execution details to add
            metrics: Optional metrics to add
            
        Returns:
            True if update was successful
        """
        try:
            # Get the execution
            execution = self.get_healing_execution(healing_id)
            if not execution:
                logger.error(f"Healing execution not found: {healing_id}")
                return False
            
            # Complete the execution
            execution.complete(successful, execution_details, metrics)
            
            # Save the updated execution
            result = self.update_healing_execution(execution)
            
            # Update pattern and action stats if successful is not None
            if result and successful is not None:
                self.update_issue_pattern_stats(execution.pattern_id, successful)
                self.update_healing_action_stats(execution.action_id, successful)
            
            return result
            
        except Exception as e:
            logger.error(f"Error completing healing execution {healing_id}: {str(e)}")
            return False
    
    def get_healing_executions_by_execution(
        self, 
        execution_id: str,
        successful_only: bool = None,
        failed_only: bool = None,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingExecution]:
        """
        Retrieves healing executions for a specific pipeline execution.
        
        Args:
            execution_id: ID of the pipeline execution
            successful_only: Whether to return only successful executions
            failed_only: Whether to return only failed executions
            limit: Maximum number of executions to return
            offset: Number of executions to skip
            
        Returns:
            List of HealingExecution objects for the execution
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                WHERE execution_id = @execution_id
            """
            
            query_params = [
                {"name": "execution_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution_id}}
            ]
            
            # Add success/failure filters
            if successful_only is True:
                query += " AND successful = TRUE"
            elif failed_only is True:
                query += " AND successful = FALSE"
            
            # Add ordering and pagination
            query += """
                ORDER BY execution_time DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingExecution objects
            executions = [HealingExecution.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(executions)} healing executions for execution {execution_id}")
            return executions
            
        except Exception as e:
            logger.error(f"Error retrieving healing executions for execution {execution_id}: {str(e)}")
            return []
    
    def get_healing_executions_by_pattern(
        self, 
        pattern_id: str,
        successful_only: bool = None,
        failed_only: bool = None,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingExecution]:
        """
        Retrieves healing executions for a specific issue pattern.
        
        Args:
            pattern_id: ID of the issue pattern
            successful_only: Whether to return only successful executions
            failed_only: Whether to return only failed executions
            limit: Maximum number of executions to return
            offset: Number of executions to skip
            
        Returns:
            List of HealingExecution objects for the pattern
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                WHERE pattern_id = @pattern_id
            """
            
            query_params = [
                {"name": "pattern_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pattern_id}}
            ]
            
            # Add success/failure filters
            if successful_only is True:
                query += " AND successful = TRUE"
            elif failed_only is True:
                query += " AND successful = FALSE"
            
            # Add ordering and pagination
            query += """
                ORDER BY execution_time DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingExecution objects
            executions = [HealingExecution.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(executions)} healing executions for pattern {pattern_id}")
            return executions
            
        except Exception as e:
            logger.error(f"Error retrieving healing executions for pattern {pattern_id}: {str(e)}")
            return []
    
    def get_healing_executions_by_action(
        self, 
        action_id: str,
        successful_only: bool = None,
        failed_only: bool = None,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingExecution]:
        """
        Retrieves healing executions for a specific healing action.
        
        Args:
            action_id: ID of the healing action
            successful_only: Whether to return only successful executions
            failed_only: Whether to return only failed executions
            limit: Maximum number of executions to return
            offset: Number of executions to skip
            
        Returns:
            List of HealingExecution objects for the action
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                WHERE action_id = @action_id
            """
            
            query_params = [
                {"name": "action_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": action_id}}
            ]
            
            # Add success/failure filters
            if successful_only is True:
                query += " AND successful = TRUE"
            elif failed_only is True:
                query += " AND successful = FALSE"
            
            # Add ordering and pagination
            query += """
                ORDER BY execution_time DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingExecution objects
            executions = [HealingExecution.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(executions)} healing executions for action {action_id}")
            return executions
            
        except Exception as e:
            logger.error(f"Error retrieving healing executions for action {action_id}: {str(e)}")
            return []
    
    def get_healing_executions_by_time_range(
        self, 
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        successful_only: bool = None,
        failed_only: bool = None,
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingExecution]:
        """
        Retrieves healing executions within a specific time range.
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            successful_only: Whether to return only successful executions
            failed_only: Whether to return only failed executions
            limit: Maximum number of executions to return
            offset: Number of executions to skip
            
        Returns:
            List of HealingExecution objects in the time range
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                WHERE execution_time >= @start_time
                AND execution_time <= @end_time
            """
            
            query_params = [
                {"name": "start_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_time.isoformat()}},
                {"name": "end_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_time.isoformat()}}
            ]
            
            # Add success/failure filters
            if successful_only is True:
                query += " AND successful = TRUE"
            elif failed_only is True:
                query += " AND successful = FALSE"
            
            # Add ordering and pagination
            query += """
                ORDER BY execution_time DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingExecution objects
            executions = [HealingExecution.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(executions)} healing executions in time range")
            return executions
            
        except Exception as e:
            logger.error(f"Error retrieving healing executions by time range: {str(e)}")
            return []
    
    #
    # Analytics and Metrics Methods
    #
    def get_healing_metrics(
        self,
        execution_id: str = None,
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None
    ) -> Dict[str, Any]:
        """
        Retrieves comprehensive healing metrics for analysis.
        
        Args:
            execution_id: Optional filter by pipeline execution ID
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering
            
        Returns:
            Dictionary with various healing metrics
        """
        try:
            metrics = {}
            
            # Build base query parts
            where_clauses = []
            query_params = []
            
            if execution_id:
                where_clauses.append("execution_id = @execution_id")
                query_params.append({
                    "name": "execution_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": execution_id}
                })
            
            if start_time:
                where_clauses.append("execution_time >= @start_time")
                query_params.append({
                    "name": "start_time", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": start_time.isoformat()}
                })
            
            if end_time:
                where_clauses.append("execution_time <= @end_time")
                query_params.append({
                    "name": "end_time", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": end_time.isoformat()}
                })
            
            where_clause = ""
            if where_clauses:
                where_clause = "WHERE " + " AND ".join(where_clauses)
            
            # Get overall healing success rate
            query = f"""
                SELECT
                    COUNT(*) as total_executions,
                    COUNTIF(successful = TRUE) as successful_executions,
                    SAFE_DIVIDE(COUNTIF(successful = TRUE), COUNT(*)) as success_rate,
                    AVG(confidence_score) as avg_confidence,
                    AVG(TIMESTAMP_DIFF(completion_time, execution_time, SECOND)) as avg_duration_seconds
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                {where_clause}
            """
            
            results = self._bq_client.query(query, query_params)
            rows = list(results)
            
            if rows:
                row = dict(rows[0])
                metrics["overall"] = {
                    "total_executions": row["total_executions"],
                    "successful_executions": row["successful_executions"],
                    "failed_executions": row["total_executions"] - row["successful_executions"],
                    "success_rate": row["success_rate"],
                    "avg_confidence": row["avg_confidence"],
                    "avg_duration_seconds": row["avg_duration_seconds"]
                }
            
            # Get success rates by action type
            action_type_query = f"""
                SELECT
                    a.action_type,
                    COUNT(e.healing_id) as total_executions,
                    COUNTIF(e.successful = TRUE) as successful_executions,
                    SAFE_DIVIDE(COUNTIF(e.successful = TRUE), COUNT(e.healing_id)) as success_rate
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}` e
                JOIN `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}` a
                ON e.action_id = a.action_id
                {where_clause}
                GROUP BY a.action_type
                ORDER BY total_executions DESC
            """
            
            action_type_results = self._bq_client.query(action_type_query, query_params)
            
            metrics["by_action_type"] = {
                row["action_type"]: {
                    "total_executions": row["total_executions"],
                    "successful_executions": row["successful_executions"],
                    "success_rate": row["success_rate"]
                } for row in action_type_results
            }
            
            # Get success rates by pattern type
            pattern_type_query = f"""
                SELECT
                    p.pattern_type,
                    COUNT(e.healing_id) as total_executions,
                    COUNTIF(e.successful = TRUE) as successful_executions,
                    SAFE_DIVIDE(COUNTIF(e.successful = TRUE), COUNT(e.healing_id)) as success_rate
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}` e
                JOIN `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}` p
                ON e.pattern_id = p.pattern_id
                {where_clause}
                GROUP BY p.pattern_type
                ORDER BY total_executions DESC
            """
            
            pattern_type_results = self._bq_client.query(pattern_type_query, query_params)
            
            metrics["by_pattern_type"] = {
                row["pattern_type"]: {
                    "total_executions": row["total_executions"],
                    "successful_executions": row["successful_executions"],
                    "success_rate": row["success_rate"]
                } for row in pattern_type_results
            }
            
            # Get top patterns by occurrence
            top_patterns_query = f"""
                SELECT
                    e.pattern_id,
                    p.name as pattern_name,
                    p.pattern_type,
                    COUNT(e.healing_id) as occurrences,
                    COUNTIF(e.successful = TRUE) as successful_executions,
                    SAFE_DIVIDE(COUNTIF(e.successful = TRUE), COUNT(e.healing_id)) as success_rate
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}` e
                JOIN `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}` p
                ON e.pattern_id = p.pattern_id
                {where_clause}
                GROUP BY e.pattern_id, p.name, p.pattern_type
                ORDER BY occurrences DESC
                LIMIT 10
            """
            
            top_patterns_results = self._bq_client.query(top_patterns_query, query_params)
            
            metrics["top_patterns"] = [
                {
                    "pattern_id": row["pattern_id"],
                    "pattern_name": row["pattern_name"],
                    "pattern_type": row["pattern_type"],
                    "occurrences": row["occurrences"],
                    "successful_executions": row["successful_executions"],
                    "success_rate": row["success_rate"]
                } for row in top_patterns_results
            ]
            
            # Get top actions by success rate
            top_actions_query = f"""
                SELECT
                    e.action_id,
                    a.name as action_name,
                    a.action_type,
                    COUNT(e.healing_id) as executions,
                    COUNTIF(e.successful = TRUE) as successful_executions,
                    SAFE_DIVIDE(COUNTIF(e.successful = TRUE), COUNT(e.healing_id)) as success_rate
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}` e
                JOIN `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}` a
                ON e.action_id = a.action_id
                {where_clause}
                GROUP BY e.action_id, a.name, a.action_type
                HAVING executions >= 5
                ORDER BY success_rate DESC, executions DESC
                LIMIT 10
            """
            
            top_actions_results = self._bq_client.query(top_actions_query, query_params)
            
            metrics["top_actions"] = [
                {
                    "action_id": row["action_id"],
                    "action_name": row["action_name"],
                    "action_type": row["action_type"],
                    "executions": row["executions"],
                    "successful_executions": row["successful_executions"],
                    "success_rate": row["success_rate"]
                } for row in top_actions_results
            ]
            
            logger.info("Retrieved healing metrics")
            return metrics
            
        except Exception as e:
            logger.error(f"Error retrieving healing metrics: {str(e)}")
            return {"error": str(e)}
    
    def get_healing_trend(
        self,
        interval: str = "daily",
        num_intervals: int = 30,
        pattern_id: str = None,
        action_id: str = None
    ) -> pd.DataFrame:
        """
        Retrieves healing success/failure trend over time intervals.
        
        Args:
            interval: Time interval for grouping ('hourly', 'daily', 'weekly')
            num_intervals: Number of intervals to return
            pattern_id: Optional filter by pattern ID
            action_id: Optional filter by action ID
            
        Returns:
            DataFrame with time intervals and healing counts
        """
        try:
            # Validate interval
            valid_intervals = ["hourly", "daily", "weekly"]
            if interval not in valid_intervals:
                logger.error(f"Invalid interval: {interval}. Must be one of {valid_intervals}")
                raise ValueError(f"Invalid interval: {interval}")
            
            # Determine time function and format based on interval
            if interval == "hourly":
                time_function = "DATETIME_TRUNC(DATETIME(execution_time), HOUR)"
                time_format = "%Y-%m-%d %H:00:00"
            elif interval == "daily":
                time_function = "DATE(execution_time)"
                time_format = "%Y-%m-%d"
            else:  # weekly
                time_function = "DATE_TRUNC(DATE(execution_time), WEEK)"
                time_format = "%Y-%m-%d"
            
            # Calculate start time based on interval and num_intervals
            start_date = None
            end_date = datetime.datetime.now()
            
            if interval == "hourly":
                start_date = end_date - datetime.timedelta(hours=num_intervals)
            elif interval == "daily":
                start_date = end_date - datetime.timedelta(days=num_intervals)
            else:  # weekly
                start_date = end_date - datetime.timedelta(weeks=num_intervals)
            
            # Build query with filters
            where_clauses = [f"execution_time >= '{start_date.isoformat()}'"]
            query_params = []
            
            if pattern_id:
                where_clauses.append("pattern_id = @pattern_id")
                query_params.append({
                    "name": "pattern_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": pattern_id}
                })
            
            if action_id:
                where_clauses.append("action_id = @action_id")
                query_params.append({
                    "name": "action_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": action_id}
                })
            
            where_clause = "WHERE " + " AND ".join(where_clauses)
            
            # Build query
            query = f"""
                SELECT
                    {time_function} as time_interval,
                    COUNT(*) as total_executions,
                    COUNTIF(successful = TRUE) as successful_executions,
                    COUNTIF(successful = FALSE) as failed_executions
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
                {where_clause}
                GROUP BY time_interval
                ORDER BY time_interval
            """
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to DataFrame
            df = pd.DataFrame([dict(row) for row in results])
            
            # Handle empty results
            if df.empty:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=['time_interval', 'total_executions', 'successful_executions', 'failed_executions'])
            
            # Convert time_interval to string for consistent formatting
            if interval == "hourly":
                df['time_interval'] = df['time_interval'].dt.strftime(time_format)
            else:
                df['time_interval'] = df['time_interval'].astype(str)
            
            # Fill in missing intervals with zeros
            all_intervals = []
            current = start_date
            
            while current <= end_date:
                if interval == "hourly":
                    all_intervals.append(current.strftime(time_format))
                    current += datetime.timedelta(hours=1)
                elif interval == "daily":
                    all_intervals.append(current.strftime(time_format))
                    current += datetime.timedelta(days=1)
                else:  # weekly
                    # Get start of week
                    week_start = current - datetime.timedelta(days=current.weekday())
                    all_intervals.append(week_start.strftime(time_format))
                    current += datetime.timedelta(weeks=1)
            
            # Create DataFrame with all intervals
            all_intervals_df = pd.DataFrame({'time_interval': all_intervals})
            
            # Merge with actual data, filling NaN with 0
            result_df = pd.merge(all_intervals_df, df, on='time_interval', how='left')
            result_df = result_df.fillna(0)
            
            # Ensure columns are numeric
            for col in ['total_executions', 'successful_executions', 'failed_executions']:
                result_df[col] = result_df[col].astype(int)
            
            logger.info(f"Retrieved healing trend with {interval} interval for {num_intervals} periods")
            return result_df
            
        except Exception as e:
            logger.error(f"Error retrieving healing trend: {str(e)}")
            raise
    
    def get_top_healing_actions(
        self,
        limit: int = 10,
        pattern_type: str = None
    ) -> List[HealingAction]:
        """
        Retrieves the most effective healing actions by success rate.
        
        Args:
            limit: Maximum number of actions to return
            pattern_type: Optional filter by pattern type
            
        Returns:
            List of HealingAction objects sorted by success rate
        """
        try:
            # Build query with filters
            query = f"""
                SELECT a.*
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_ACTION_TABLE_NAME}` a
                WHERE a.execution_count > 0
            """
            
            query_params = []
            
            if pattern_type:
                query += """
                    AND a.pattern_id IN (
                        SELECT pattern_id
                        FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                        WHERE pattern_type = @pattern_type
                    )
                """
                query_params.append({
                    "name": "pattern_type", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": pattern_type}
                })
            
            # Add ordering and pagination
            query += f"""
                ORDER BY a.success_rate DESC, a.execution_count DESC
                LIMIT @limit
            """
            
            query_params.append({
                "name": "limit", 
                "parameterType": {"type": "INT64"}, 
                "parameterValue": {"value": limit}
            })
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingAction objects
            actions = [HealingAction.from_dict(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(actions)} top healing actions")
            return actions
            
        except Exception as e:
            logger.error(f"Error retrieving top healing actions: {str(e)}")
            return []
    
    def get_top_issue_patterns(
        self,
        limit: int = 10,
        pattern_type: str = None
    ) -> List[IssuePattern]:
        """
        Retrieves the most frequently detected issue patterns.
        
        Args:
            limit: Maximum number of patterns to return
            pattern_type: Optional filter by pattern type
            
        Returns:
            List of IssuePattern objects sorted by occurrence count
        """
        try:
            # Build query with filters
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{ISSUE_PATTERN_TABLE_NAME}`
                WHERE occurrence_count > 0
            """
            
            query_params = []
            
            if pattern_type:
                query += " AND pattern_type = @pattern_type"
                query_params.append({
                    "name": "pattern_type", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": pattern_type}
                })
            
            # Add ordering and pagination
            query += f"""
                ORDER BY occurrence_count DESC
                LIMIT @limit
            """
            
            query_params.append({
                "name": "limit", 
                "parameterType": {"type": "INT64"}, 
                "parameterValue": {"value": limit}
            })
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to IssuePattern objects
            patterns = [IssuePattern.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Retrieved {len(patterns)} top issue patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error retrieving top issue patterns: {str(e)}")
            return []
    
    def search_healing_executions(
        self, 
        search_criteria: Dict[str, Any],
        limit: int = 100, 
        offset: int = 0
    ) -> List[HealingExecution]:
        """
        Searches healing executions based on multiple criteria.
        
        Args:
            search_criteria: Dictionary with search parameters
                - execution_id: Pipeline execution ID
                - pattern_id: Issue pattern ID
                - action_id: Healing action ID
                - start_time: Minimum execution time
                - end_time: Maximum execution time
                - status: Execution status
                - successful: Whether execution was successful
            limit: Maximum number of executions to return
            offset: Number of executions to skip
            
        Returns:
            List of HealingExecution objects matching criteria
        """
        try:
            # Build query with filters
            where_clauses = []
            query_params = []
            
            # Extract search parameters
            execution_id = search_criteria.get('execution_id')
            pattern_id = search_criteria.get('pattern_id')
            action_id = search_criteria.get('action_id')
            validation_id = search_criteria.get('validation_id')
            start_time = search_criteria.get('start_time')
            end_time = search_criteria.get('end_time')
            status = search_criteria.get('status')
            successful = search_criteria.get('successful')
            
            # Add filters for each provided parameter
            if execution_id:
                where_clauses.append("execution_id = @execution_id")
                query_params.append({
                    "name": "execution_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": execution_id}
                })
            
            if pattern_id:
                where_clauses.append("pattern_id = @pattern_id")
                query_params.append({
                    "name": "pattern_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": pattern_id}
                })
            
            if action_id:
                where_clauses.append("action_id = @action_id")
                query_params.append({
                    "name": "action_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": action_id}
                })
            
            if validation_id:
                where_clauses.append("validation_id = @validation_id")
                query_params.append({
                    "name": "validation_id", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": validation_id}
                })
            
            if start_time:
                where_clauses.append("execution_time >= @start_time")
                query_params.append({
                    "name": "start_time", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": start_time.isoformat() if isinstance(start_time, datetime.datetime) else start_time}
                })
            
            if end_time:
                where_clauses.append("execution_time <= @end_time")
                query_params.append({
                    "name": "end_time", 
                    "parameterType": {"type": "TIMESTAMP"}, 
                    "parameterValue": {"value": end_time.isoformat() if isinstance(end_time, datetime.datetime) else end_time}
                })
            
            if status:
                where_clauses.append("status = @status")
                query_params.append({
                    "name": "status", 
                    "parameterType": {"type": "STRING"}, 
                    "parameterValue": {"value": status}
                })
            
            if successful is not None:
                where_clauses.append("successful = @successful")
                query_params.append({
                    "name": "successful", 
                    "parameterType": {"type": "BOOL"}, 
                    "parameterValue": {"value": successful}
                })
            
            # Construct the query
            query = f"""
                SELECT *
                FROM `{self._project_id}.{self._dataset_id}.{HEALING_EXECUTION_TABLE_NAME}`
            """
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Add ordering and pagination
            query += """
                ORDER BY execution_time DESC
                LIMIT @limit
                OFFSET @offset
            """
            
            query_params.extend([
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ])
            
            # Execute query
            results = self._bq_client.query(query, query_params)
            
            # Convert to HealingExecution objects
            executions = [HealingExecution.from_bigquery_row(dict(row)) for row in results]
            
            logger.info(f"Search returned {len(executions)} healing executions")
            return executions
            
        except Exception as e:
            logger.error(f"Error searching healing executions: {str(e)}")
            return []