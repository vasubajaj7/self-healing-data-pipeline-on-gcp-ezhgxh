from typing import List, Dict, Optional
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.backend.api.services.quality_service import (
    get_quality_rules,
    get_quality_rule_by_id,
    create_quality_rule,
    update_quality_rule,
    delete_quality_rule,
    get_quality_validations,
    get_validation_by_id,
    execute_validation,
    get_quality_score,
    get_quality_issues,
)
from src.backend.api.models.response_models import (
    QualityRuleResponse,
    QualityRuleListResponse,
    QualityValidationResponse,
    QualityValidationListResponse,
    QualityScoreResponse,
    DataResponse,
    BaseResponse,
)
from src.backend.api.models.data_models import QualityRule, QualityValidation, ValidationStatus
from src.backend.api.models.request_models import QualityRuleCreateRequest, QualityRuleUpdateRequest
from src.backend.api.models.error_models import ResourceNotFoundError, ValidationError, ResponseStatus
from src.backend.utils.logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

router = APIRouter(
    prefix="/quality",
    tags=["Quality"],
    responses={404: {"description": "Not found"}},
)

@router.get("/rules", 
            response_model=QualityRuleListResponse,
            summary="Retrieve paginated list of quality rules")
def get_quality_rules_endpoint(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    target_dataset: Optional[str] = Query(None, description="Filter by target dataset"),
    target_table: Optional[str] = Query(None, description="Filter by target table"),
    rule_type: Optional[str] = Query(None, description="Filter by rule type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status")
):
    """Retrieves a paginated list of quality rules with optional filtering"""
    logger.info(f"Request for quality rules with filters: page={page}, page_size={page_size}, "
                f"target_dataset={target_dataset}, target_table={target_table}, rule_type={rule_type}, is_active={is_active}")

    rules, total_count = get_quality_rules(page, page_size, target_dataset, target_table, rule_type, is_active)

    total_pages = (total_count + page_size - 1) // page_size
    pagination = {
        "page": page,
        "page_size": page_size,
        "total_items": total_count,
        "total_pages": total_pages
    }

    return QualityRuleListResponse(items=rules, pagination=pagination)

@router.get("/rules/{rule_id}", 
            response_model=QualityRuleResponse,
            summary="Retrieve specific quality rule by ID")
def get_quality_rule_endpoint(rule_id: str):
    """Retrieves a specific quality rule by ID"""
    logger.info(f"Request for quality rule by ID: {rule_id}")

    rule = get_quality_rule_by_id(rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"Quality rule with ID '{rule_id}' not found")

    return QualityRuleResponse(data=rule)

@router.post("/rules", 
             response_model=QualityRuleResponse, 
             status_code=status.HTTP_201_CREATED,
             summary="Create new quality rule")
def create_quality_rule_endpoint(rule_request: QualityRuleCreateRequest):
    """Creates a new quality rule"""
    logger.info(f"Request to create a new quality rule: {rule_request.rule_name}")

    try:
        rule = create_quality_rule(rule_request)
        return QualityRuleResponse(data=rule, message="Quality rule created successfully")
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))

@router.put("/rules/{rule_id}", 
            response_model=QualityRuleResponse,
            summary="Update existing quality rule")
def update_quality_rule_endpoint(rule_id: str, rule_update: QualityRuleUpdateRequest):
    """Updates an existing quality rule"""
    logger.info(f"Request to update quality rule: {rule_id}")

    try:
        rule = update_quality_rule(rule_id, rule_update)
        return QualityRuleResponse(data=rule, message="Quality rule updated successfully")
    except ResourceNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))

@router.delete("/rules/{rule_id}", 
               response_model=DataResponse[bool],
               summary="Delete quality rule")
def delete_quality_rule_endpoint(rule_id: str):
    """Deletes a quality rule"""
    logger.info(f"Request to delete quality rule: {rule_id}")

    try:
        success = delete_quality_rule(rule_id)
        return DataResponse[bool](data=success, message="Quality rule deleted successfully")
    except ResourceNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/validations", 
            response_model=QualityValidationListResponse,
            summary="Retrieve paginated list of quality validations")
def get_quality_validations_endpoint(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering (ISO format)"),
    execution_id: Optional[str] = Query(None, description="Filter by execution ID"),
    rule_id: Optional[str] = Query(None, description="Filter by rule ID"),
    status: Optional[ValidationStatus] = Query(None, description="Filter by validation status")
):
    """Retrieves a paginated list of quality validations with optional filtering"""
    logger.info(f"Request for quality validations with filters: page={page}, page_size={page_size}, "
                f"start_date={start_date}, end_date={end_date}, execution_id={execution_id}, rule_id={rule_id}, status={status}")

    validations, total_count = get_quality_validations(page, page_size, start_date, end_date, execution_id, rule_id, status)

    total_pages = (total_count + page_size - 1) // page_size
    pagination = {
        "page": page,
        "page_size": page_size,
        "total_items": total_count,
        "total_pages": total_pages
    }

    return QualityValidationListResponse(items=validations, pagination=pagination)

@router.get("/validations/{validation_id}", 
            response_model=QualityValidationResponse,
            summary="Retrieve specific validation by ID")
def get_validation_endpoint(validation_id: str):
    """Retrieves a specific validation result by ID"""
    logger.info(f"Request for validation by ID: {validation_id}")

    validation = get_validation_by_id(validation_id)
    if validation is None:
        raise HTTPException(status_code=404, detail=f"Validation with ID '{validation_id}' not found")

    return QualityValidationResponse(data=validation)

@router.post("/validations/execute", 
             response_model=QualityValidationResponse,
             summary="Execute quality validation for a dataset")
def execute_validation_endpoint(
    dataset: str,
    table: Optional[str] = Query(None, description="Table to validate"),
    execution_id: Optional[str] = Query(None, description="Execution ID to associate with validation"),
    rule_ids: Optional[List[str]] = Query(None, description="List of rule IDs to execute")
):
    """Executes quality validation for a dataset"""
    logger.info(f"Request to execute validation for dataset: {dataset}, table: {table}, "
                f"execution_id={execution_id}, rule_ids={rule_ids}")

    validation_results = execute_validation(dataset, table, execution_id, rule_ids)
    return QualityValidationResponse(data=validation_results)

@router.get("/scores/{dataset}", 
            response_model=QualityScoreResponse,
            summary="Retrieve quality score for a dataset")
def get_quality_score_endpoint(
    dataset: str,
    table: Optional[str] = Query(None, description="Table to get score for"),
    as_of_date: Optional[datetime] = Query(None, description="Date to get score as of (ISO format)")
):
    """Retrieves the quality score for a dataset"""
    logger.info(f"Request for quality score for dataset: {dataset}, table: {table}, as_of_date={as_of_date}")

    score_details = get_quality_score(dataset, table, as_of_date)
    return QualityScoreResponse(
        overall_score=score_details.get("overall_score", 0.0),
        dimension_scores=score_details.get("dimension_scores", {}),
        quality_metrics=score_details.get("quality_metrics", {})
    )

@router.get("/issues/{dataset}", 
            response_model=DataResponse,
            summary="Retrieve quality issues for a dataset")
def get_quality_issues_endpoint(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    dataset: str = Query(..., description="Dataset to get issues for"),
    table: Optional[str] = Query(None, description="Table to get issues for"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    is_resolved: Optional[bool] = Query(None, description="Filter by resolution status"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering (ISO format)"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering (ISO format)")
):
    """Retrieves quality issues for a dataset"""
    logger.info(f"Request for quality issues: dataset={dataset}, table={table}, severity={severity}, "
                f"is_resolved={is_resolved}, start_date={start_date}, end_date={end_date}")

    issues, total_count = get_quality_issues(page, page_size, dataset, table, severity, is_resolved, start_date, end_date)

    total_pages = (total_count + page_size - 1) // page_size
    pagination = {
        "page": page,
        "page_size": page_size,
        "total_items": total_count,
        "total_pages": total_pages
    }

    return DataResponse(data={"issues": issues, "pagination": pagination})