"""
Controller module for the self-healing functionality of the data pipeline API.
Handles requests for managing healing patterns, actions, executions, and configuration.
Acts as an intermediary between API routes and the healing service layer.
"""

import typing
import uuid

# Import internal modules
from src.backend.utils.logging.logger import get_logger  # Configure logging for the healing controller
from src.backend.api.services import healing_service  # Service layer functions for self-healing operations
from src.backend.api.models import response_models  # Response models for API endpoints
from src.backend.api.models import error_models  # Error handling models for API responses

# Initialize logger
logger = get_logger(__name__)


def get_healing_patterns(pagination: dict, issue_type: str) -> response_models.IssuePatternListResponse:
    """Retrieves a paginated list of healing patterns with optional filtering

    Args:
        pagination (dict): Pagination parameters (page, page_size)
        issue_type (str): Optional filter by issue type

    Returns:
        response_models.IssuePatternListResponse: Paginated list of healing patterns
    """
    logger.info("Retrieving healing patterns with pagination: %s and issue_type: %s", pagination, issue_type)
    try:
        patterns, pagination_metadata = healing_service.get_healing_patterns(pagination, issue_type)
        items = [response_models.IssuePatternResponse(data=pattern) for pattern in patterns]
        return response_models.IssuePatternListResponse(
            items=items,
            pagination=response_models.PaginationMetadata(**pagination_metadata)
        )
    except Exception as e:
        logger.error(f"Error retrieving healing patterns: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing patterns: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_pattern_by_id(pattern_id: str) -> response_models.IssuePatternResponse:
    """Retrieves a specific healing pattern by ID

    Args:
        pattern_id (str): ID of the healing pattern

    Returns:
        response_models.IssuePatternResponse: Healing pattern details
    """
    logger.info("Retrieving healing pattern details for pattern_id: %s", pattern_id)
    try:
        pattern = healing_service.get_healing_pattern_by_id(pattern_id)
        if not pattern:
            raise error_models.ResourceNotFoundError(resource_type="Healing Pattern", resource_id=pattern_id)
        return response_models.IssuePatternResponse(data=pattern)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing pattern not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error retrieving healing pattern: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing pattern: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def create_healing_pattern(pattern_data: dict) -> response_models.IssuePatternResponse:
    """Creates a new healing pattern in the system

    Args:
        pattern_data (dict): Data for the new pattern

    Returns:
        response_models.IssuePatternResponse: Created healing pattern details
    """
    logger.info("Creating a new healing pattern with data: %s", pattern_data)
    try:
        pattern = healing_service.create_healing_pattern(pattern_data)
        return response_models.IssuePatternResponse(data=pattern)
    except ValueError as e:
        logger.error(f"Validation error creating healing pattern: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error creating healing pattern: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to create healing pattern: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def update_healing_pattern(pattern_id: str, pattern_data: dict) -> response_models.IssuePatternResponse:
    """Updates an existing healing pattern

    Args:
        pattern_id (str): ID of the pattern to update
        pattern_data (dict): Data to update

    Returns:
        response_models.IssuePatternResponse: Updated healing pattern details
    """
    logger.info("Updating healing pattern with pattern_id: %s and data: %s", pattern_id, pattern_data)
    try:
        pattern = healing_service.update_healing_pattern(pattern_id, pattern_data)
        if not pattern:
            raise error_models.ResourceNotFoundError(resource_type="Healing Pattern", resource_id=pattern_id)
        return response_models.IssuePatternResponse(data=pattern)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing pattern not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except ValueError as e:
        logger.error(f"Validation error updating healing pattern: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error updating healing pattern: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to update healing pattern: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def delete_healing_pattern(pattern_id: str) -> response_models.DataResponse:
    """Deletes a healing pattern from the system

    Args:
        pattern_id (str): ID of the pattern to delete

    Returns:
        response_models.DataResponse: Deletion confirmation
    """
    logger.info("Deleting healing pattern with pattern_id: %s", pattern_id)
    try:
        success = healing_service.delete_healing_pattern(pattern_id)
        if not success:
            raise error_models.ResourceNotFoundError(resource_type="Healing Pattern", resource_id=pattern_id)
        return response_models.DataResponse(data={"deleted": True})
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing pattern not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error deleting healing pattern: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to delete healing pattern: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_actions(pagination: dict, pattern_id: str, action_type: str, active_only: bool) -> response_models.HealingActionListResponse:
    """Retrieves a paginated list of healing actions with optional filtering

    Args:
        pagination (dict): Pagination parameters (page, page_size)
        pattern_id (str): Optional filter by pattern ID
        action_type (str): Optional filter by action type
        active_only (bool): Optional filter for active actions only

    Returns:
        response_models.HealingActionListResponse: Paginated list of healing actions
    """
    logger.info("Retrieving healing actions with pagination: %s, pattern_id: %s, action_type: %s, active_only: %s", pagination, pattern_id, action_type, active_only)
    try:
        actions, pagination_metadata = healing_service.get_healing_actions(pagination, pattern_id, action_type, active_only)
        items = [response_models.HealingActionResponse(data=action) for action in actions]
        return response_models.HealingActionListResponse(
            items=items,
            pagination=response_models.PaginationMetadata(**pagination_metadata)
        )
    except Exception as e:
        logger.error(f"Error retrieving healing actions: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing actions: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_action_by_id(action_id: str) -> response_models.HealingActionResponse:
    """Retrieves a specific healing action by ID

    Args:
        action_id (str): ID of the healing action

    Returns:
        response_models.HealingActionResponse: Healing action details
    """
    logger.info("Retrieving healing action details for action_id: %s", action_id)
    try:
        action = healing_service.get_healing_action_by_id(action_id)
        if not action:
            raise error_models.ResourceNotFoundError(resource_type="Healing Action", resource_id=action_id)
        return response_models.HealingActionResponse(data=action)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing action not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error retrieving healing action: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing action: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def create_healing_action(action_data: dict) -> response_models.HealingActionResponse:
    """Creates a new healing action in the system

    Args:
        action_data (dict): Data for the new action

    Returns:
        response_models.HealingActionResponse: Created healing action details
    """
    logger.info("Creating a new healing action with data: %s", action_data)
    try:
        action = healing_service.create_healing_action(action_data)
        return response_models.HealingActionResponse(data=action)
    except ValueError as e:
        logger.error(f"Validation error creating healing action: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Resource not found creating healing action: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error creating healing action: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to create healing action: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def update_healing_action(action_id: str, action_data: dict) -> response_models.HealingActionResponse:
    """Updates an existing healing action

    Args:
        action_id (str): ID of the action to update
        action_data (dict): Data to update

    Returns:
        response_models.HealingActionResponse: Updated healing action details
    """
    logger.info("Updating healing action with action_id: %s and data: %s", action_id, action_data)
    try:
        action = healing_service.update_healing_action(action_id, action_data)
        if not action:
            raise error_models.ResourceNotFoundError(resource_type="Healing Action", resource_id=action_id)
        return response_models.HealingActionResponse(data=action)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing action not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except ValueError as e:
        logger.error(f"Validation error updating healing action: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error updating healing action: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to update healing action: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def delete_healing_action(action_id: str) -> response_models.DataResponse:
    """Deletes a healing action from the system

    Args:
        action_id (str): ID of the action to delete

    Returns:
        response_models.DataResponse: Deletion confirmation
    """
    logger.info("Deleting healing action with action_id: %s", action_id)
    try:
        success = healing_service.delete_healing_action(action_id)
        if not success:
            raise error_models.ResourceNotFoundError(resource_type="Healing Action", resource_id=action_id)
        return response_models.DataResponse(data={"deleted": True})
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing action not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error deleting healing action: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to delete healing action: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_executions(pagination: dict, date_range: dict, execution_id: str, pattern_id: str, action_id: str, successful_only: bool) -> response_models.HealingExecutionListResponse:
    """Retrieves a paginated list of healing executions with optional filtering

    Args:
        pagination (dict): Pagination parameters (page, page_size)
        date_range (dict): Optional filter by date range (start_date, end_date)
        execution_id (str): Optional filter by execution ID
        pattern_id (str): Optional filter by pattern ID
        action_id (str): Optional filter by action ID
        successful_only (bool): Optional filter for successful executions only

    Returns:
        response_models.HealingExecutionListResponse: Paginated list of healing executions
    """
    logger.info("Retrieving healing executions with pagination: %s, date_range: %s, execution_id: %s, pattern_id: %s, action_id: %s, successful_only: %s", pagination, date_range, execution_id, pattern_id, action_id, successful_only)
    try:
        executions, pagination_metadata = healing_service.get_healing_executions(pagination, date_range, execution_id, pattern_id, action_id, successful_only)
        items = [response_models.HealingExecutionResponse(data=execution) for execution in executions]
        return response_models.HealingExecutionListResponse(
            items=items,
            pagination=response_models.PaginationMetadata(**pagination_metadata)
        )
    except Exception as e:
        logger.error(f"Error retrieving healing executions: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing executions: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_execution_by_id(healing_id: str) -> response_models.HealingExecutionResponse:
    """Retrieves a specific healing execution by ID

    Args:
        healing_id (str): ID of the healing execution

    Returns:
        response_models.HealingExecutionResponse: Healing execution details
    """
    logger.info("Retrieving healing execution details for healing_id: %s", healing_id)
    try:
        execution = healing_service.get_healing_execution_by_id(healing_id)
        if not execution:
            raise error_models.ResourceNotFoundError(resource_type="Healing Execution", resource_id=healing_id)
        return response_models.HealingExecutionResponse(data=execution)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Healing execution not found: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error retrieving healing execution: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing execution: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_config() -> response_models.HealingConfigResponse:
    """Retrieves the current self-healing configuration

    Returns:
        response_models.HealingConfigResponse: Self-healing configuration
    """
    logger.info("Retrieving self-healing configuration")
    try:
        config = healing_service.get_healing_config()
        return response_models.HealingConfigResponse(data=config)
    except Exception as e:
        logger.error(f"Error retrieving healing configuration: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve healing configuration: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def update_healing_config(config_data: dict) -> response_models.HealingConfigResponse:
    """Updates the self-healing configuration

    Args:
        config_data (dict): Data to update the configuration

    Returns:
        response_models.HealingConfigResponse: Updated self-healing configuration
    """
    logger.info("Updating self-healing configuration with data: %s", config_data)
    try:
        config = healing_service.update_healing_config(config_data)
        return response_models.HealingConfigResponse(data=config)
    except ValueError as e:
        logger.error(f"Validation error updating healing configuration: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error updating healing configuration: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to update healing configuration: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def execute_manual_healing(healing_request: dict) -> response_models.ManualHealingResponse:
    """Manually triggers a healing action for a specific issue

    Args:
        healing_request (dict): Request details for manual healing

    Returns:
        response_models.ManualHealingResponse: Result of manual healing execution
    """
    logger.info("Executing manual healing with request: %s", healing_request)
    try:
        result = healing_service.execute_manual_healing(healing_request)
        return response_models.ManualHealingResponse(**result)
    except error_models.ResourceNotFoundError as e:
        logger.error(f"Resource not found executing manual healing: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=404,
            error_type="Resource Not Found",
            category=error_models.ErrorCategory.RESOURCE_NOT_FOUND,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except ValueError as e:
        logger.error(f"Validation error executing manual healing: {e}")
        return error_models.ErrorResponse(
            message=str(e),
            metadata=response_models.ResponseMetadata(),
            status_code=400,
            error_type="Validation Error",
            category=error_models.ErrorCategory.VALIDATION,
            severity=error_models.ErrorSeverity.MEDIUM
        )
    except Exception as e:
        logger.error(f"Error executing manual healing: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to execute manual healing: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )


def get_healing_statistics(date_range: dict) -> response_models.DataResponse:
    """Retrieves statistics about self-healing operations

    Args:
        date_range (dict): Optional filter by date range (start_date, end_date)

    Returns:
        response_models.DataResponse: Self-healing statistics
    """
    logger.info("Retrieving self-healing statistics with date_range: %s", date_range)
    try:
        statistics = healing_service.get_healing_statistics(date_range)
        return response_models.DataResponse(data=statistics)
    except Exception as e:
        logger.error(f"Error retrieving self-healing statistics: {e}")
        return error_models.ErrorResponse(
            message=f"Failed to retrieve self-healing statistics: {e}",
            metadata=response_models.ResponseMetadata(),
            status_code=500,
            error_type="Internal Server Error",
            category=error_models.ErrorCategory.SYSTEM,
            severity=error_models.ErrorSeverity.MEDIUM
        )