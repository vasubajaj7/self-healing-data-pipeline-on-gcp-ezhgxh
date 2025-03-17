"""
Database model for healing actions in the self-healing data pipeline.

This module defines the HealingAction model, which represents specific corrective 
measures that can be applied to resolve issues detected in data or pipeline execution.
It provides functionality for creating, retrieving, updating, and deleting healing actions,
as well as tracking their execution history and success rates.
"""

import uuid
import json
import typing
import datetime
from typing import Dict, List, Optional, Any, Union

from ....constants import HealingActionType
from ....utils.logging.logger import get_logger

# Module logger
logger = get_logger(__name__)

# Constants
HEALING_ACTION_TABLE_NAME = "healing_actions"
DEFAULT_SUCCESS_RATE = 0.0


class HealingAction:
    """
    Database model representing a specific action that can be taken to resolve an issue.
    
    Healing actions are linked to issue patterns and contain the parameters and logic 
    required to automatically fix detected problems. They also track execution statistics
    to measure effectiveness over time.
    """
    
    def __init__(self,
                 action_id: Optional[str] = None,
                 name: Optional[str] = None,
                 action_type: Optional[HealingActionType] = None,
                 description: Optional[str] = None,
                 action_parameters: Optional[Dict[str, Any]] = None,
                 pattern_id: Optional[str] = None,
                 execution_count: int = 0,
                 success_count: int = 0,
                 success_rate: float = DEFAULT_SUCCESS_RATE,
                 is_active: bool = True,
                 last_executed: Optional[datetime.datetime] = None,
                 created_at: Optional[datetime.datetime] = None,
                 updated_at: Optional[datetime.datetime] = None):
        """
        Initialize a healing action instance.
        
        Args:
            action_id: Unique identifier for the healing action
            name: Descriptive name of the healing action
            action_type: Type of healing action (DATA_CORRECTION, PIPELINE_RETRY, etc.)
            description: Detailed description of what the action does
            action_parameters: Parameters required for executing the action
            pattern_id: ID of the issue pattern this action is associated with
            execution_count: Number of times this action has been executed
            success_count: Number of successful executions
            success_rate: Success rate as a decimal (0.0 to 1.0)
            is_active: Whether this action is currently active for use
            last_executed: When this action was last executed
            created_at: When this action was created
            updated_at: When this action was last updated
        """
        self.action_id = action_id or str(uuid.uuid4())
        self.name = name
        self.action_type = action_type
        self.description = description
        self.action_parameters = action_parameters or {}
        self.pattern_id = pattern_id
        self.execution_count = execution_count
        self.success_count = success_count
        self.success_rate = success_rate
        self.is_active = is_active
        self.last_executed = last_executed
        self.created_at = created_at or datetime.datetime.utcnow()
        self.updated_at = updated_at or datetime.datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert healing action to dictionary representation.
        
        Returns:
            Dictionary representation of the healing action
        """
        return {
            'action_id': self.action_id,
            'name': self.name,
            'action_type': self.action_type.value if self.action_type else None,
            'description': self.description,
            'action_parameters': self.action_parameters,
            'pattern_id': self.pattern_id,
            'execution_count': self.execution_count,
            'success_count': self.success_count,
            'success_rate': self.success_rate,
            'is_active': self.is_active,
            'last_executed': self.last_executed.isoformat() if self.last_executed else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    def from_dict(cls, action_dict: Dict[str, Any]) -> 'HealingAction':
        """
        Create HealingAction from dictionary representation.
        
        Args:
            action_dict: Dictionary representation of a healing action
            
        Returns:
            HealingAction instance
        """
        # Convert action_type string to enum if present
        action_type = None
        if action_dict.get('action_type'):
            try:
                action_type = HealingActionType(action_dict['action_type'])
            except ValueError:
                logger.warning(f"Invalid action_type value: {action_dict['action_type']}")
        
        # Parse datetime strings
        last_executed = None
        if action_dict.get('last_executed'):
            try:
                last_executed = datetime.datetime.fromisoformat(action_dict['last_executed'])
            except ValueError:
                logger.warning(f"Invalid last_executed timestamp: {action_dict['last_executed']}")
        
        created_at = None
        if action_dict.get('created_at'):
            try:
                created_at = datetime.datetime.fromisoformat(action_dict['created_at'])
            except ValueError:
                logger.warning(f"Invalid created_at timestamp: {action_dict['created_at']}")
        
        updated_at = None
        if action_dict.get('updated_at'):
            try:
                updated_at = datetime.datetime.fromisoformat(action_dict['updated_at'])
            except ValueError:
                logger.warning(f"Invalid updated_at timestamp: {action_dict['updated_at']}")
        
        # Create and return instance
        return cls(
            action_id=action_dict.get('action_id'),
            name=action_dict.get('name'),
            action_type=action_type,
            description=action_dict.get('description'),
            action_parameters=action_dict.get('action_parameters', {}),
            pattern_id=action_dict.get('pattern_id'),
            execution_count=action_dict.get('execution_count', 0),
            success_count=action_dict.get('success_count', 0),
            success_rate=action_dict.get('success_rate', DEFAULT_SUCCESS_RATE),
            is_active=action_dict.get('is_active', True),
            last_executed=last_executed,
            created_at=created_at,
            updated_at=updated_at
        )
    
    def update_stats(self, success: bool) -> None:
        """
        Update action statistics based on execution results.
        
        Args:
            success: Whether the execution was successful
        """
        self.execution_count += 1
        if success:
            self.success_count += 1
        
        # Recalculate success rate
        if self.execution_count > 0:
            self.success_rate = self.success_count / self.execution_count
        
        # Update timestamps
        self.last_executed = datetime.datetime.utcnow()
        self.updated_at = datetime.datetime.utcnow()
        
        logger.debug(f"Updated stats for action {self.action_id}: success={success}, "
                     f"success_rate={self.success_rate:.2f}, execution_count={self.execution_count}")
    
    def activate(self) -> None:
        """
        Activate this action for use in healing.
        """
        self.is_active = True
        self.updated_at = datetime.datetime.utcnow()
        logger.info(f"Activated healing action: {self.action_id} - {self.name}")
    
    def deactivate(self) -> None:
        """
        Deactivate this action to exclude from healing.
        """
        self.is_active = False
        self.updated_at = datetime.datetime.utcnow()
        logger.info(f"Deactivated healing action: {self.action_id} - {self.name}")
    
    def update_parameters(self, new_parameters: Dict[str, Any]) -> None:
        """
        Update the action parameters.
        
        Args:
            new_parameters: New parameter values to use
        """
        # Validate required fields based on action type
        if self.action_type == HealingActionType.DATA_CORRECTION:
            required_fields = ['correction_logic', 'target_fields']
            for field in required_fields:
                if field not in new_parameters:
                    logger.warning(f"Missing required parameter '{field}' for DATA_CORRECTION action")
        
        elif self.action_type == HealingActionType.PIPELINE_RETRY:
            required_fields = ['max_retries', 'retry_delay']
            for field in required_fields:
                if field not in new_parameters:
                    logger.warning(f"Missing required parameter '{field}' for PIPELINE_RETRY action")
        
        # Update parameters and timestamp
        self.action_parameters = new_parameters
        self.updated_at = datetime.datetime.utcnow()
        logger.info(f"Updated parameters for healing action: {self.action_id}")
    
    def get_success_rate(self) -> float:
        """
        Get the current success rate of this action.
        
        Returns:
            Success rate as a percentage (0.0 to 1.0)
        """
        return self.success_rate
    
    def is_applicable_for_issue(self, issue_data: Dict[str, Any]) -> bool:
        """
        Check if this action is applicable for a given issue.
        
        Args:
            issue_data: Issue data to check applicability for
            
        Returns:
            True if action is applicable for the issue
        """
        # Check if action is active
        if not self.is_active:
            return False
        
        # Validate issue data has required fields
        if not issue_data.get('issue_type') or not issue_data.get('pattern_id'):
            logger.warning("Issue data missing required fields: issue_type or pattern_id")
            return False
        
        # Check if issue matches the pattern this action is designed for
        if issue_data.get('pattern_id') != self.pattern_id:
            return False
        
        # Additional applicability checks based on action type
        if self.action_type == HealingActionType.DATA_CORRECTION:
            # Check if the action can handle the specific data fields in the issue
            affected_fields = issue_data.get('affected_fields', [])
            target_fields = self.action_parameters.get('target_fields', [])
            
            # Check if there's overlap between affected fields and target fields
            return any(field in target_fields for field in affected_fields)
            
        elif self.action_type == HealingActionType.PIPELINE_RETRY:
            # Check if the failure type is something a retry can fix
            return issue_data.get('failure_type') in ['timeout', 'transient_error', 'resource_constraint']
        
        # Default case
        return True


def create_healing_action(
    name: str,
    action_type: HealingActionType,
    description: str,
    action_parameters: Dict[str, Any],
    pattern_id: str
) -> HealingAction:
    """
    Creates a new healing action record in the database.
    
    Args:
        name: Name of the healing action
        action_type: Type of healing action
        description: Description of what the action does
        action_parameters: Parameters needed for execution
        pattern_id: ID of the associated issue pattern
        
    Returns:
        Newly created healing action instance
    """
    # Generate a unique action_id
    action_id = str(uuid.uuid4())
    
    # Validate action_type
    if not isinstance(action_type, HealingActionType):
        raise ValueError(f"Invalid action_type: {action_type}")
    
    # Validate action_parameters has required fields based on action_type
    if action_type == HealingActionType.DATA_CORRECTION:
        required_fields = ['correction_logic', 'target_fields']
        for field in required_fields:
            if field not in action_parameters:
                raise ValueError(f"Missing required parameter '{field}' for DATA_CORRECTION action")
    
    elif action_type == HealingActionType.PIPELINE_RETRY:
        required_fields = ['max_retries', 'retry_delay']
        for field in required_fields:
            if field not in action_parameters:
                raise ValueError(f"Missing required parameter '{field}' for PIPELINE_RETRY action")
    
    # Create a new HealingAction instance
    now = datetime.datetime.utcnow()
    action = HealingAction(
        action_id=action_id,
        name=name,
        action_type=action_type,
        description=description,
        action_parameters=action_parameters,
        pattern_id=pattern_id,
        execution_count=0,
        success_count=0,
        success_rate=DEFAULT_SUCCESS_RATE,
        is_active=True,
        last_executed=None,
        created_at=now,
        updated_at=now
    )
    
    logger.info(f"Created new healing action: {action_id} - {name} for pattern {pattern_id}")
    
    return action


def get_healing_action(action_id: str) -> Optional[HealingAction]:
    """
    Retrieves a healing action by its ID.
    
    Args:
        action_id: ID of the healing action to retrieve
        
    Returns:
        Retrieved healing action or None if not found
    """
    # This would typically query the database for the healing action
    # For now, stub implementation
    logger.debug(f"Getting healing action with ID: {action_id}")
    # In a real implementation, you would query the database here
    # return db.query(HealingAction).filter_by(action_id=action_id).first()
    return None


def get_healing_actions_by_type(action_type: HealingActionType) -> List[HealingAction]:
    """
    Retrieves all healing actions of a specific type.
    
    Args:
        action_type: Type of healing actions to retrieve
        
    Returns:
        List of HealingAction instances of the specified type
    """
    # This would typically query the database for healing actions of the specified type
    # For now, stub implementation
    logger.debug(f"Getting healing actions of type: {action_type.value}")
    # In a real implementation, you would query the database here
    # return db.query(HealingAction).filter_by(action_type=action_type).all()
    return []


def get_healing_actions_by_pattern(pattern_id: str) -> List[HealingAction]:
    """
    Retrieves all healing actions associated with a specific issue pattern.
    
    Args:
        pattern_id: ID of the issue pattern
        
    Returns:
        List of HealingAction instances for the pattern
    """
    # This would typically query the database for healing actions for the specified pattern
    # For now, stub implementation
    logger.debug(f"Getting healing actions for pattern: {pattern_id}")
    # In a real implementation, you would query the database here
    # return db.query(HealingAction).filter_by(pattern_id=pattern_id).all()
    return []


def update_healing_action(action_id: str, update_data: Dict[str, Any]) -> Optional[HealingAction]:
    """
    Updates an existing healing action.
    
    Args:
        action_id: ID of the healing action to update
        update_data: Dictionary of fields to update
        
    Returns:
        Updated healing action instance or None if not found
    """
    # Get the existing healing action
    action = get_healing_action(action_id)
    if not action:
        logger.warning(f"Healing action not found: {action_id}")
        return None
    
    # Update fields
    if 'name' in update_data:
        action.name = update_data['name']
    
    if 'description' in update_data:
        action.description = update_data['description']
    
    if 'action_parameters' in update_data:
        action.update_parameters(update_data['action_parameters'])
    
    if 'is_active' in update_data:
        if update_data['is_active']:
            action.activate()
        else:
            action.deactivate()
    
    # Cannot update action_type or pattern_id as these are fundamental to the action
    if 'action_type' in update_data or 'pattern_id' in update_data:
        logger.warning("Cannot update action_type or pattern_id for existing healing action")
    
    # Update timestamp
    action.updated_at = datetime.datetime.utcnow()
    
    logger.info(f"Updated healing action: {action_id}")
    
    # In a real implementation, you would save to database here
    # db.save(action)
    
    return action


def delete_healing_action(action_id: str) -> bool:
    """
    Deletes a healing action by its ID.
    
    Args:
        action_id: ID of the healing action to delete
        
    Returns:
        True if action was deleted successfully
    """
    # This would typically delete the healing action from the database
    # For now, stub implementation
    logger.info(f"Deleting healing action: {action_id}")
    # In a real implementation, you would delete from the database here
    # result = db.query(HealingAction).filter_by(action_id=action_id).delete()
    # return result > 0
    return True


def get_healing_action_table_schema() -> List[Dict[str, Any]]:
    """
    Returns the BigQuery table schema for healing actions.
    
    Returns:
        List of BigQuery SchemaField objects defining the table schema
    """
    schema = [
        {"name": "action_id", "type": "STRING", "mode": "REQUIRED", "description": "Unique identifier for the healing action"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED", "description": "Name of the healing action"},
        {"name": "action_type", "type": "STRING", "mode": "REQUIRED", "description": "Type of the healing action"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE", "description": "Description of what the action does"},
        {"name": "action_parameters", "type": "JSON", "mode": "NULLABLE", "description": "Parameters needed for execution"},
        {"name": "pattern_id", "type": "STRING", "mode": "REQUIRED", "description": "ID of the associated issue pattern"},
        {"name": "execution_count", "type": "INTEGER", "mode": "REQUIRED", "description": "Number of times this action has been executed"},
        {"name": "success_count", "type": "INTEGER", "mode": "REQUIRED", "description": "Number of successful executions"},
        {"name": "success_rate", "type": "FLOAT", "mode": "REQUIRED", "description": "Success rate as a decimal (0.0 to 1.0)"},
        {"name": "is_active", "type": "BOOLEAN", "mode": "REQUIRED", "description": "Whether this action is currently active for use"},
        {"name": "last_executed", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "When this action was last executed"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "When this action was created"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED", "description": "When this action was last updated"}
    ]
    
    return schema