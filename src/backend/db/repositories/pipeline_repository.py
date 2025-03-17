import datetime
import json
from typing import List, Dict, Optional, Any, Union

import pandas as pd

from db.models.pipeline_definition import PipelineDefinition, PIPELINE_DEFINITION_TABLE_NAME
from db.models.pipeline_execution import PipelineExecution, PipelineStatus, PIPELINE_EXECUTION_TABLE_NAME
from utils.storage.bigquery_client import BigQueryClient
from utils.logging.logger import get_logger
from utils.errors.error_types import ResourceError, DataError
from config import get_config

# Set up logger
logger = get_logger(__name__)

class PipelineRepository:
    """Repository class for managing pipeline definitions and executions in BigQuery."""
    
    def __init__(self, bq_client: BigQueryClient, dataset_id: str):
        """Initialize the pipeline repository with BigQuery client.
        
        Args:
            bq_client: BigQuery client instance
            dataset_id: BigQuery dataset ID for pipeline tables
        """
        self._bq_client = bq_client
        self._project_id = bq_client.project_id or get_config().get_gcp_project_id()
        self._dataset_id = dataset_id
        
        logger.info(f"Initialized PipelineRepository with dataset: {self._dataset_id}")
        
    def create_pipeline_definition(self, pipeline_definition: PipelineDefinition) -> PipelineDefinition:
        """Create a new pipeline definition in the database.
        
        Args:
            pipeline_definition: Pipeline definition object to create
            
        Returns:
            Created pipeline definition with updated metadata
            
        Raises:
            DataError: If pipeline definition is invalid
            ResourceError: If a pipeline with the same name already exists
        """
        # Validate pipeline definition
        is_valid, errors = pipeline_definition.validate()
        if not is_valid:
            error_msg = f"Invalid pipeline definition: {', '.join(errors)}"
            logger.error(error_msg)
            raise DataError(error_msg, "pipeline_definition", {"errors": errors})
        
        # Check if pipeline with same name already exists
        existing = self.get_pipeline_definition_by_name(pipeline_definition.name)
        if existing:
            error_msg = f"Pipeline with name '{pipeline_definition.name}' already exists"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_definition", 
                pipeline_definition.name, 
                {"pipeline_id": existing.pipeline_id}
            )
        
        # Convert to BigQuery row and insert
        row = pipeline_definition.to_bigquery_row()
        self._bq_client.insert_rows(
            self._dataset_id,
            PIPELINE_DEFINITION_TABLE_NAME,
            [row]
        )
        
        logger.info(f"Created pipeline definition: {pipeline_definition.pipeline_id} - {pipeline_definition.name}")
        return pipeline_definition
    
    def get_pipeline_definition(self, pipeline_id: str) -> Optional[PipelineDefinition]:
        """Get a pipeline definition by ID.
        
        Args:
            pipeline_id: ID of the pipeline definition to retrieve
            
        Returns:
            Retrieved pipeline definition or None if not found
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_DEFINITION_TABLE_NAME}`
        WHERE pipeline_id = @pipeline_id
        """
        
        query_params = [
            {"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            logger.info(f"Pipeline definition not found with ID: {pipeline_id}")
            return None
        
        return PipelineDefinition.from_bigquery_row(results[0])
    
    def get_pipeline_definition_by_name(self, name: str) -> Optional[PipelineDefinition]:
        """Get a pipeline definition by name.
        
        Args:
            name: Name of the pipeline definition to retrieve
            
        Returns:
            Retrieved pipeline definition or None if not found
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_DEFINITION_TABLE_NAME}`
        WHERE name = @name
        """
        
        query_params = [
            {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": name}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            logger.info(f"Pipeline definition not found with name: {name}")
            return None
        
        return PipelineDefinition.from_bigquery_row(results[0])
    
    def list_pipeline_definitions(
        self, 
        active_only: bool = False, 
        source_id: str = None, 
        pipeline_type: str = None, 
        limit: int = 100, 
        offset: int = 0
    ) -> List[PipelineDefinition]:
        """List pipeline definitions with optional filtering.
        
        Args:
            active_only: If True, only active pipelines are returned
            source_id: Filter by source system ID
            pipeline_type: Filter by pipeline type
            limit: Maximum number of results to return
            offset: Offset for pagination
            
        Returns:
            List of pipeline definition objects
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_DEFINITION_TABLE_NAME}`
        WHERE 1=1
        """
        
        query_params = []
        
        if active_only:
            query += " AND is_active = @is_active"
            query_params.append({"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": True}})
        
        if source_id:
            query += " AND source_id = @source_id"
            query_params.append({"name": "source_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": source_id}})
        
        if pipeline_type:
            query += " AND pipeline_type = @pipeline_type"
            query_params.append({"name": "pipeline_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_type}})
        
        query += f" ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"
        
        results = self._bq_client.query(query, query_params)
        
        return [PipelineDefinition.from_bigquery_row(row) for row in results]
    
    def update_pipeline_definition(self, pipeline_definition: PipelineDefinition) -> PipelineDefinition:
        """Update an existing pipeline definition.
        
        Args:
            pipeline_definition: Pipeline definition with updated values
            
        Returns:
            Updated pipeline definition
            
        Raises:
            DataError: If pipeline definition is invalid
            ResourceError: If pipeline definition with given ID does not exist
        """
        # Validate pipeline definition
        is_valid, errors = pipeline_definition.validate()
        if not is_valid:
            error_msg = f"Invalid pipeline definition: {', '.join(errors)}"
            logger.error(error_msg)
            raise DataError(error_msg, "pipeline_definition", {"errors": errors})
        
        # Check if pipeline exists
        existing = self.get_pipeline_definition(pipeline_definition.pipeline_id)
        if not existing:
            error_msg = f"Pipeline definition with ID '{pipeline_definition.pipeline_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_definition", 
                pipeline_definition.pipeline_id, 
                {"not_found": True}
            )
        
        # Convert to BigQuery row and update
        row = pipeline_definition.to_bigquery_row()
        updated = self._bq_client.update_rows(
            self._dataset_id,
            PIPELINE_DEFINITION_TABLE_NAME,
            [row],
            "pipeline_id"
        )
        
        if not updated:
            logger.error(f"Failed to update pipeline definition: {pipeline_definition.pipeline_id}")
            raise ResourceError(
                f"Failed to update pipeline definition: {pipeline_definition.pipeline_id}",
                "pipeline_definition",
                pipeline_definition.pipeline_id,
                {"update_failed": True}
            )
        
        logger.info(f"Updated pipeline definition: {pipeline_definition.pipeline_id}")
        return pipeline_definition
    
    def delete_pipeline_definition(self, pipeline_id: str) -> bool:
        """Delete a pipeline definition by ID.
        
        Args:
            pipeline_id: ID of the pipeline definition to delete
            
        Returns:
            True if deleted, False if not found
        """
        # Check if pipeline exists
        existing = self.get_pipeline_definition(pipeline_id)
        if not existing:
            logger.warning(f"Attempted to delete non-existent pipeline definition: {pipeline_id}")
            return False
        
        query = f"""
        DELETE FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_DEFINITION_TABLE_NAME}`
        WHERE pipeline_id = @pipeline_id
        """
        
        query_params = [
            {"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}}
        ]
        
        self._bq_client.execute_query(query, query_params)
        logger.info(f"Deleted pipeline definition: {pipeline_id}")
        return True
    
    def activate_pipeline(self, pipeline_id: str, updated_by: str) -> PipelineDefinition:
        """Activate a pipeline definition.
        
        Args:
            pipeline_id: ID of the pipeline to activate
            updated_by: User activating the pipeline
            
        Returns:
            Updated pipeline definition
            
        Raises:
            ResourceError: If pipeline definition with given ID does not exist
        """
        # Get pipeline definition
        pipeline_def = self.get_pipeline_definition(pipeline_id)
        if not pipeline_def:
            error_msg = f"Pipeline definition with ID '{pipeline_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_definition", 
                pipeline_id, 
                {"not_found": True}
            )
        
        # Activate pipeline
        pipeline_def.activate(updated_by)
        
        # Update in database
        return self.update_pipeline_definition(pipeline_def)
    
    def deactivate_pipeline(self, pipeline_id: str, updated_by: str) -> PipelineDefinition:
        """Deactivate a pipeline definition.
        
        Args:
            pipeline_id: ID of the pipeline to deactivate
            updated_by: User deactivating the pipeline
            
        Returns:
            Updated pipeline definition
            
        Raises:
            ResourceError: If pipeline definition with given ID does not exist
        """
        # Get pipeline definition
        pipeline_def = self.get_pipeline_definition(pipeline_id)
        if not pipeline_def:
            error_msg = f"Pipeline definition with ID '{pipeline_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_definition", 
                pipeline_id, 
                {"not_found": True}
            )
        
        # Deactivate pipeline
        pipeline_def.deactivate(updated_by)
        
        # Update in database
        return self.update_pipeline_definition(pipeline_def)
    
    def create_pipeline_execution(self, pipeline_execution: PipelineExecution) -> PipelineExecution:
        """Create a new pipeline execution record.
        
        Args:
            pipeline_execution: Pipeline execution object to create
            
        Returns:
            Created pipeline execution with updated metadata
            
        Raises:
            ResourceError: If referenced pipeline does not exist
        """
        # Check if referenced pipeline exists
        pipeline_def = self.get_pipeline_definition(pipeline_execution.pipeline_id)
        if not pipeline_def:
            error_msg = f"Referenced pipeline with ID '{pipeline_execution.pipeline_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_definition", 
                pipeline_execution.pipeline_id, 
                {"not_found": True}
            )
        
        # Convert to BigQuery row and insert
        row = pipeline_execution.to_bigquery_row()
        self._bq_client.insert_rows(
            self._dataset_id,
            PIPELINE_EXECUTION_TABLE_NAME,
            [row]
        )
        
        logger.info(f"Created pipeline execution: {pipeline_execution.execution_id} for pipeline {pipeline_execution.pipeline_id}")
        return pipeline_execution
    
    def get_pipeline_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get a pipeline execution by ID.
        
        Args:
            execution_id: ID of the pipeline execution to retrieve
            
        Returns:
            Retrieved pipeline execution or None if not found
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
        WHERE execution_id = @execution_id
        """
        
        query_params = [
            {"name": "execution_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution_id}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            logger.info(f"Pipeline execution not found with ID: {execution_id}")
            return None
        
        return PipelineExecution.from_bigquery_row(results[0])
    
    def list_pipeline_executions(
        self,
        pipeline_id: str = None,
        status: PipelineStatus = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[PipelineExecution]:
        """List pipeline executions with optional filtering.
        
        Args:
            pipeline_id: Filter by pipeline ID
            status: Filter by execution status
            start_date: Filter by start date (inclusive)
            end_date: Filter by end date (inclusive)
            limit: Maximum number of results to return
            offset: Offset for pagination
            
        Returns:
            List of pipeline execution objects
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
        WHERE 1=1
        """
        
        query_params = []
        
        if pipeline_id:
            query += " AND pipeline_id = @pipeline_id"
            query_params.append({"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}})
        
        if status:
            status_value = status.value if isinstance(status, PipelineStatus) else status
            query += " AND status = @status"
            query_params.append({"name": "status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": status_value}})
        
        if start_date:
            query += " AND start_time >= @start_date"
            query_params.append({"name": "start_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_date.isoformat()}})
        
        if end_date:
            query += " AND start_time <= @end_date"
            query_params.append({"name": "end_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_date.isoformat()}})
        
        query += f" ORDER BY start_time DESC LIMIT {limit} OFFSET {offset}"
        
        results = self._bq_client.query(query, query_params)
        
        return [PipelineExecution.from_bigquery_row(row) for row in results]
    
    def update_pipeline_execution(self, pipeline_execution: PipelineExecution) -> PipelineExecution:
        """Update an existing pipeline execution.
        
        Args:
            pipeline_execution: Pipeline execution with updated values
            
        Returns:
            Updated pipeline execution
            
        Raises:
            ResourceError: If pipeline execution with given ID does not exist
        """
        # Check if execution exists
        existing = self.get_pipeline_execution(pipeline_execution.execution_id)
        if not existing:
            error_msg = f"Pipeline execution with ID '{pipeline_execution.execution_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                error_msg, 
                "pipeline_execution", 
                pipeline_execution.execution_id, 
                {"not_found": True}
            )
        
        # Convert to BigQuery row and update
        row = pipeline_execution.to_bigquery_row()
        updated = self._bq_client.update_rows(
            self._dataset_id,
            PIPELINE_EXECUTION_TABLE_NAME,
            [row],
            "execution_id"
        )
        
        if not updated:
            logger.error(f"Failed to update pipeline execution: {pipeline_execution.execution_id}")
            raise ResourceError(
                f"Failed to update pipeline execution: {pipeline_execution.execution_id}",
                "pipeline_execution",
                pipeline_execution.execution_id,
                {"update_failed": True}
            )
        
        logger.info(f"Updated pipeline execution: {pipeline_execution.execution_id}")
        return pipeline_execution
    
    def get_pipeline_execution_metrics(
        self,
        pipeline_id: str = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None
    ) -> Dict[str, Any]:
        """Get aggregated metrics for pipeline executions.
        
        Args:
            pipeline_id: Filter by pipeline ID (optional)
            start_date: Start date for filtering executions (optional)
            end_date: End date for filtering executions (optional)
            
        Returns:
            Dictionary of aggregated metrics
        """
        query = f"""
        SELECT
            COUNT(*) as total_executions,
            COUNTIF(status = 'SUCCESS') as successful_executions,
            COUNTIF(status = 'FAILED') as failed_executions,
            COUNTIF(status = 'PARTIALLY_SUCCEEDED') as partial_executions,
            COUNTIF(retry_count > 0) as retried_executions,
            COUNTIF(LENGTH(self_healing_attempts) > 5) as self_healed_executions,
            AVG(TIMESTAMP_DIFF(IFNULL(end_time, CURRENT_TIMESTAMP()), start_time, SECOND)) as avg_duration_seconds,
            AVG(records_processed) as avg_records_processed,
            AVG(quality_score) as avg_quality_score,
            AVG(records_failed / NULLIF(records_processed, 0)) as avg_failure_rate
        FROM 
            `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
        WHERE 1=1
        """
        
        query_params = []
        
        if pipeline_id:
            query += " AND pipeline_id = @pipeline_id"
            query_params.append({"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}})
        
        if start_date:
            query += " AND start_time >= @start_date"
            query_params.append({"name": "start_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_date.isoformat()}})
        
        if end_date:
            query += " AND start_time <= @end_date"
            query_params.append({"name": "end_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_date.isoformat()}})
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "partial_executions": 0,
                "retried_executions": 0,
                "self_healed_executions": 0,
                "avg_duration_seconds": 0,
                "avg_records_processed": 0,
                "avg_quality_score": 0,
                "avg_failure_rate": 0,
                "success_rate": 0
            }
        
        # Get the first (and only) row of results
        row = results[0]
        
        # Calculate success rate
        total = row.get("total_executions", 0)
        successful = row.get("successful_executions", 0)
        success_rate = (successful / total * 100) if total > 0 else 0
        
        metrics = {
            "total_executions": row.get("total_executions", 0),
            "successful_executions": row.get("successful_executions", 0),
            "failed_executions": row.get("failed_executions", 0),
            "partial_executions": row.get("partial_executions", 0),
            "retried_executions": row.get("retried_executions", 0),
            "self_healed_executions": row.get("self_healed_executions", 0),
            "avg_duration_seconds": row.get("avg_duration_seconds", 0),
            "avg_records_processed": row.get("avg_records_processed", 0),
            "avg_quality_score": row.get("avg_quality_score", 0),
            "avg_failure_rate": row.get("avg_failure_rate", 0),
            "success_rate": success_rate
        }
        
        return metrics
    
    def get_pipeline_execution_history(
        self,
        pipeline_id: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        interval: str = 'DAY'
    ) -> pd.DataFrame:
        """Get execution history for a pipeline with time-series metrics.
        
        Args:
            pipeline_id: Pipeline ID to get history for
            start_date: Start date for history
            end_date: End date for history
            interval: Time grouping interval (DAY, HOUR, WEEK)
            
        Returns:
            DataFrame with time-series execution metrics
        """
        # Validate interval
        valid_intervals = ['DAY', 'HOUR', 'WEEK', 'MONTH']
        if interval not in valid_intervals:
            interval = 'DAY'
        
        # Define the timestamp truncation based on interval
        if interval == 'HOUR':
            trunc_expr = "TIMESTAMP_TRUNC(start_time, HOUR)"
        elif interval == 'WEEK':
            trunc_expr = "TIMESTAMP_TRUNC(start_time, WEEK)"
        elif interval == 'MONTH':
            trunc_expr = "TIMESTAMP_TRUNC(start_time, MONTH)"
        else:  # Default to DAY
            trunc_expr = "TIMESTAMP_TRUNC(start_time, DAY)"
        
        query = f"""
        SELECT
            {trunc_expr} as time_period,
            COUNT(*) as total_executions,
            COUNTIF(status = 'SUCCESS') as successful_executions,
            COUNTIF(status = 'FAILED') as failed_executions,
            COUNTIF(retry_count > 0) as retried_executions,
            COUNTIF(LENGTH(self_healing_attempts) > 5) as self_healed_executions,
            AVG(TIMESTAMP_DIFF(IFNULL(end_time, CURRENT_TIMESTAMP()), start_time, SECOND)) as avg_duration_seconds,
            AVG(records_processed) as avg_records_processed,
            AVG(quality_score) as avg_quality_score
        FROM 
            `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
        WHERE 
            pipeline_id = @pipeline_id
            AND start_time >= @start_date
            AND start_time <= @end_date
        GROUP BY 
            time_period
        ORDER BY 
            time_period ASC
        """
        
        query_params = [
            {"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}},
            {"name": "start_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_date.isoformat()}},
            {"name": "end_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_date.isoformat()}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(results)
        
        # If no results, return empty DataFrame with expected columns
        if df.empty:
            return pd.DataFrame(columns=[
                'time_period', 'total_executions', 'successful_executions', 
                'failed_executions', 'retried_executions', 'self_healed_executions',
                'avg_duration_seconds', 'avg_records_processed', 'avg_quality_score'
            ])
        
        # Calculate success rate
        df['success_rate'] = (df['successful_executions'] / df['total_executions'] * 100).fillna(0)
        
        return df
    
    def get_recent_failures(
        self,
        limit: int = 10,
        since: datetime.datetime = None
    ) -> List[PipelineExecution]:
        """Get recent pipeline execution failures.
        
        Args:
            limit: Maximum number of failures to return
            since: Only return failures after this time (optional)
            
        Returns:
            List of failed PipelineExecution objects
        """
        query = f"""
        SELECT * 
        FROM `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`
        WHERE status = 'FAILED'
        """
        
        query_params = []
        
        if since:
            query += " AND start_time >= @since"
            query_params.append({"name": "since", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": since.isoformat()}})
        
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        results = self._bq_client.query(query, query_params)
        
        return [PipelineExecution.from_bigquery_row(row) for row in results]
    
    def get_self_healing_statistics(
        self,
        pipeline_id: str = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None
    ) -> Dict[str, Any]:
        """Get statistics on self-healing attempts and success rates.
        
        Args:
            pipeline_id: Filter by pipeline ID (optional)
            start_date: Start date for filtering executions (optional)
            end_date: End date for filtering executions (optional)
            
        Returns:
            Dictionary of self-healing statistics
        """
        # This requires extracting data from the self_healing_attempts JSON array
        query = f"""
        WITH healing_data AS (
            SELECT
                execution_id,
                pipeline_id,
                JSON_EXTRACT_SCALAR(healing, '$.healing_id') as healing_id,
                JSON_EXTRACT_SCALAR(healing, '$.issue_type') as issue_type,
                JSON_EXTRACT_SCALAR(healing, '$.action_taken') as action_taken,
                CAST(JSON_EXTRACT_SCALAR(healing, '$.successful') AS BOOL) as successful,
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_EXTRACT_SCALAR(healing, '$.timestamp')) as timestamp
            FROM
                `{self._project_id}.{self._dataset_id}.{PIPELINE_EXECUTION_TABLE_NAME}`,
                UNNEST(JSON_EXTRACT_ARRAY(self_healing_attempts, '$')) as healing
            WHERE 
                LENGTH(self_healing_attempts) > 5
        )
        SELECT
            COUNT(*) as total_attempts,
            COUNTIF(successful) as successful_attempts,
            COUNTIF(NOT successful) as failed_attempts,
            COUNT(DISTINCT execution_id) as affected_executions,
            COUNT(DISTINCT pipeline_id) as affected_pipelines,
            AVG(IF(successful, 1, 0)) * 100 as success_rate,
            ARRAY_AGG(STRUCT(issue_type, COUNT(*) as count) ORDER BY COUNT(*) DESC LIMIT 5) as top_issues,
            ARRAY_AGG(STRUCT(action_taken, COUNT(*) as count) ORDER BY COUNT(*) DESC LIMIT 5) as top_actions
        FROM 
            healing_data
        WHERE 1=1
        """
        
        query_params = []
        
        if pipeline_id:
            query += " AND pipeline_id = @pipeline_id"
            query_params.append({"name": "pipeline_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": pipeline_id}})
        
        if start_date:
            query += " AND timestamp >= @start_date"
            query_params.append({"name": "start_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_date.isoformat()}})
        
        if end_date:
            query += " AND timestamp <= @end_date"
            query_params.append({"name": "end_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_date.isoformat()}})
        
        try:
            results = self._bq_client.query(query, query_params)
            
            if not results or len(results) == 0:
                return {
                    "total_attempts": 0,
                    "successful_attempts": 0,
                    "failed_attempts": 0,
                    "affected_executions": 0,
                    "affected_pipelines": 0,
                    "success_rate": 0,
                    "top_issues": [],
                    "top_actions": []
                }
            
            # Get the first (and only) row of results
            row = results[0]
            
            # Process the top issues and actions
            top_issues = []
            if row.get("top_issues"):
                for issue in row.get("top_issues"):
                    top_issues.append({
                        "issue_type": issue.get("issue_type"),
                        "count": issue.get("count")
                    })
            
            top_actions = []
            if row.get("top_actions"):
                for action in row.get("top_actions"):
                    top_actions.append({
                        "action_taken": action.get("action_taken"),
                        "count": action.get("count")
                    })
            
            statistics = {
                "total_attempts": row.get("total_attempts", 0),
                "successful_attempts": row.get("successful_attempts", 0),
                "failed_attempts": row.get("failed_attempts", 0),
                "affected_executions": row.get("affected_executions", 0),
                "affected_pipelines": row.get("affected_pipelines", 0),
                "success_rate": row.get("success_rate", 0),
                "top_issues": top_issues,
                "top_actions": top_actions
            }
            
            return statistics
        
        except Exception as e:
            logger.error(f"Error retrieving self-healing statistics: {str(e)}")
            # Return a default statistics dictionary on error
            return {
                "total_attempts": 0,
                "successful_attempts": 0,
                "failed_attempts": 0,
                "affected_executions": 0,
                "affected_pipelines": 0,
                "success_rate": 0,
                "top_issues": [],
                "top_actions": [],
                "error": str(e)
            }
    
    def ensure_tables_exist(self) -> bool:
        """Ensure that required BigQuery tables exist, creating them if necessary.
        
        Returns:
            True if tables exist or were created successfully
        """
        # Import here to avoid circular imports
        from db.models.pipeline_definition import get_pipeline_definition_table_schema
        from db.models.pipeline_execution import get_pipeline_execution_table_schema
        
        try:
            # Make sure dataset exists
            dataset_exists = self._bq_client.dataset_exists(self._dataset_id)
            if not dataset_exists:
                logger.info(f"Creating dataset: {self._dataset_id}")
                self._bq_client.create_dataset(self._dataset_id)
            
            # Make sure pipeline definitions table exists
            definition_table_exists = self._bq_client.table_exists(self._dataset_id, PIPELINE_DEFINITION_TABLE_NAME)
            if not definition_table_exists:
                logger.info(f"Creating table: {PIPELINE_DEFINITION_TABLE_NAME}")
                definition_schema = get_pipeline_definition_table_schema()
                self._bq_client.create_table(self._dataset_id, PIPELINE_DEFINITION_TABLE_NAME, definition_schema)
            
            # Make sure pipeline executions table exists
            execution_table_exists = self._bq_client.table_exists(self._dataset_id, PIPELINE_EXECUTION_TABLE_NAME)
            if not execution_table_exists:
                logger.info(f"Creating table: {PIPELINE_EXECUTION_TABLE_NAME}")
                execution_schema = get_pipeline_execution_table_schema()
                self._bq_client.create_table(self._dataset_id, PIPELINE_EXECUTION_TABLE_NAME, execution_schema)
            
            return True
        
        except Exception as e:
            logger.error(f"Error ensuring tables exist: {str(e)}")
            return False