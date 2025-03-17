from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException, status

from src.backend.api.controllers import quality_controller
from src.backend.api.models.request_models import QualityRuleCreateRequest, QualityRuleUpdateRequest
from src.backend.api.models.data_models import ValidationStatus
from src.backend.utils.logging.logger import get_logger  # version 1.0.0

# Initialize logger
logger = get_logger(__name__)

router = APIRouter(prefix="/api/quality", tags=["Quality"])


@router.get("/rules",
            summary="Retrieve paginated list of quality rules")
def get_rules(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    target_dataset: Optional[str] = Query(None, description="Filter by target dataset"),
    target_table: Optional[str] = Query(None, description="Filter by target table"),
    rule_type: Optional[str] = Query(None, description="Filter by rule type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status")
):
    """Endpoint to retrieve a paginated list of quality rules with optional filtering"""
    logger.info(f"Request for quality rules with filters: page={page}, page_size={page_size}, "
                f"target_dataset={target_dataset}, target_table={target_table}, rule_type={rule_type}, is_active={is_active}")

    rules = quality_controller.get_quality_rules(page, page_size, target_dataset, target_table, rule_type, is_active)

    return rules


@router.get("/rules/{rule_id}",
            summary="Retrieve specific quality rule by ID")
def get_rule(rule_id: str):
    """Endpoint to retrieve a specific quality rule by ID"""
    logger.info(f"Request for quality rule by ID: {rule_id}")

    rule = quality_controller.get_quality_rule_by_id(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"Quality rule with ID '{rule_id}' not found")

    return rule


@router.post("/rules", status_code=status.HTTP_201_CREATED,
             summary="Create new quality rule")
def create_rule(rule_request: QualityRuleCreateRequest):
    """Endpoint to create a new quality rule"""
    logger.info(f"Request to create a new quality rule: {rule_request.rule_name}")

    rule = quality_controller.create_quality_rule(rule_request)
    return rule


@router.put("/rules/{rule_id}",
            summary="Update existing quality rule")
def update_rule(rule_id: str, rule_update: QualityRuleUpdateRequest):
    """Endpoint to update an existing quality rule"""
    logger.info(f"Request to update quality rule: {rule_id}")

    rule = quality_controller.update_quality_rule(rule_id, rule_update)
    return rule


@router.delete("/rules/{rule_id}",
               summary="Delete quality rule")
def delete_rule(rule_id: str):
    """Endpoint to delete a quality rule"""
    logger.info(f"Request to delete quality rule: {rule_id}")

    success = quality_controller.delete_quality_rule(rule_id)
    return success


@router.get("/validations",
            summary="Retrieve paginated list of quality validations")
def get_validations(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering (ISO format)"),
    execution_id: Optional[str] = Query(None, description="Filter by execution ID"),
    rule_id: Optional[str] = Query(None, description="Filter by rule ID"),
    status: Optional[ValidationStatus] = Query(None, description="Filter by validation status")
):
    """Endpoint to retrieve a paginated list of quality validations with optional filtering"""
    logger.info(f"Request for quality validations with filters: page={page}, page_size={page_size}, "
                f"start_date={start_date}, end_date={end_date}, execution_id={execution_id}, rule_id={rule_id}, status={status}")

    validations = quality_controller.get_quality_validations(page, page_size, start_date, end_date, execution_id, rule_id, status)

    return validations


@router.get("/validations/{validation_id}",
            summary="Retrieve specific validation by ID")
def get_validation(validation_id: str):
    """Endpoint to retrieve a specific validation result by ID"""
    logger.info(f"Request for validation by ID: {validation_id}")

    validation = quality_controller.get_validation_by_id(validation_id)
    if validation is None:
        raise HTTPException(status_code=404, detail=f"Validation with ID '{validation_id}' not found")

    return validation


@router.post("/validate")
def execute_validation(
    dataset: str,
    table: Optional[str] = Query(None, description="Table to validate"),
    execution_id: Optional[str] = Query(None, description="Execution ID to associate with validation"),
    rule_ids: Optional[List[str]] = Query(None, description="List of rule IDs to execute")
):
    """Endpoint to execute quality validation for a dataset"""
    logger.info(f"Request to execute validation for dataset: {dataset}, table: {table}, "
                f"execution_id={execution_id}, rule_ids={rule_ids}")

    validation_results = quality_controller.execute_validation(dataset, table, execution_id, rule_ids)
    return validation_results


@router.get("/score")
def get_quality_score(
    dataset: str,
    table: Optional[str] = Query(None, description="Table to get score for"),
    as_of_date: Optional[datetime] = Query(None, description="Date to get score as of (ISO format)")
):
    """Endpoint to retrieve the quality score for a dataset"""
    logger.info(f"Request for quality score for dataset: {dataset}, table: {table}, as_of_date={as_of_date}")

    score_details = quality_controller.get_quality_score(dataset, table, as_of_date)
    return score_details


@router.get("/issues")
def get_issues(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    dataset: str = Query(..., description="Dataset to get issues for"),
    table: Optional[str] = Query(None, description="Table to get issues for"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    is_resolved: Optional[bool] = Query(None, description="Filter by resolution status"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering (ISO format)")
):
    """Endpoint to retrieve quality issues for a dataset"""
    logger.info(f"Request for quality issues: dataset={dataset}, table={table}, severity={severity}, "
                f"is_resolved={is_resolved}, start_date={start_date}, end_date={end_date}")

    issues = quality_controller.get_quality_issues(page, page_size, dataset, table, severity, is_resolved, start_date, end_date)
    return issues

@router.get("/trend")
def get_validation_trend(
    dataset: str,
    table: Optional[str] = Query(None, description="Table to get trend for"),
    interval: str = Query("day", description="Interval to group by"),
    num_intervals: int = Query(30, description="Number of intervals to return")
):
    """Endpoint to retrieve validation trend over time"""
    logger.info(f"Request for validation trend: dataset={dataset}, table={table}, interval={interval}, num_intervals={num_intervals}")

    trend_data = quality_controller.get_validation_trend(dataset, table, interval, num_intervals)
    return trend_data

# Export the router
router = router