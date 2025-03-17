"""
Repository for managing alerts in the self-healing data pipeline.

This module provides a repository class for interacting with the alerts database,
enabling storage, retrieval, and querying of alert records in BigQuery with support
for alert correlation, notification tracking, and advanced filtering.
"""

import datetime
import json
import typing
import pandas as pd
from typing import Dict, List, Optional, Any, Union, Tuple

from ...constants import AlertSeverity, NotificationChannel
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.bigquery_client import BigQueryClient
from ..models.alert import (
    Alert, 
    get_alert_table_schema, 
    ALERT_TABLE_NAME,
    ALERT_STATUS_NEW,
    ALERT_STATUS_ACKNOWLEDGED,
    ALERT_STATUS_RESOLVED,
    ALERT_STATUS_SUPPRESSED
)

# Configure module logger
logger = get_logger(__name__)


class AlertRepository:
    """Repository for managing alerts in BigQuery"""

    def __init__(self, bq_client: BigQueryClient, dataset_id: str = None, project_id: str = None):
        """
        Initializes the AlertRepository with BigQuery client and configuration.

        Args:
            bq_client: BigQuery client for database operations
            dataset_id: BigQuery dataset ID (defaults to config value if not provided)
            project_id: GCP project ID (defaults to config value if not provided)
        """
        self._bq_client = bq_client
        
        # Get configuration
        config = get_config()
        
        # Set dataset ID from parameter or config
        self._dataset_id = dataset_id or config.get_bigquery_dataset()
        if not self._dataset_id:
            raise ValueError("BigQuery dataset ID is required")
        
        # Set project ID from parameter or config
        self._project_id = project_id or config.get_gcp_project_id()
        if not self._project_id:
            raise ValueError("GCP project ID is required")
        
        # Ensure alert table exists
        if self.ensure_table_exists():
            logger.info(f"Alert repository initialized with dataset {self._dataset_id}")
        else:
            logger.error(f"Failed to initialize alert repository: table creation error")

    def ensure_table_exists(self) -> bool:
        """
        Ensures the alert table exists in BigQuery, creating it if necessary.

        Returns:
            True if table exists or was created successfully
        """
        try:
            # Check if table exists
            table_exists = self._bq_client.table_exists(
                self._project_id, 
                self._dataset_id, 
                ALERT_TABLE_NAME
            )
            
            if not table_exists:
                # Get table schema
                schema = get_alert_table_schema()
                
                # Create the table
                self._bq_client.create_table(
                    self._project_id,
                    self._dataset_id,
                    ALERT_TABLE_NAME,
                    schema,
                    "Alert records for the self-healing pipeline"
                )
                logger.info(f"Created alerts table in {self._dataset_id}.{ALERT_TABLE_NAME}")
            else:
                logger.debug(f"Alerts table {self._dataset_id}.{ALERT_TABLE_NAME} already exists")
            
            return True
        except Exception as e:
            logger.error(f"Error ensuring alerts table exists: {e}")
            return False

    def create_alert(self, alert: Alert) -> str:
        """
        Creates a new alert record in the database.

        Args:
            alert: Alert object to create

        Returns:
            ID of the created alert record
        """
        try:
            # Validate alert
            if not alert.alert_id or not alert.alert_type or not alert.description:
                raise ValueError("Alert requires id, type, and description")
            
            # Convert to BigQuery row format
            row = alert.to_bigquery_row()
            
            # Insert into BigQuery
            self._bq_client.insert_rows(
                self._project_id,
                self._dataset_id,
                ALERT_TABLE_NAME,
                [row]
            )
            
            logger.info(f"Created alert: {alert.alert_id} - {alert.severity.value} - {alert.alert_type}")
            return alert.alert_id
        except Exception as e:
            logger.error(f"Error creating alert: {e}")
            raise

    def batch_create_alerts(self, alerts: List[Alert]) -> List[str]:
        """
        Creates multiple alert records in a single batch operation.

        Args:
            alerts: List of Alert objects to create

        Returns:
            List of created alert IDs
        """
        try:
            if not alerts:
                return []
            
            # Validate alerts
            for alert in alerts:
                if not alert.alert_id or not alert.alert_type or not alert.description:
                    raise ValueError("Each alert requires id, type, and description")
            
            # Convert to BigQuery row format
            rows = [alert.to_bigquery_row() for alert in alerts]
            
            # Insert into BigQuery
            self._bq_client.insert_rows(
                self._project_id,
                self._dataset_id,
                ALERT_TABLE_NAME,
                rows
            )
            
            alert_ids = [alert.alert_id for alert in alerts]
            logger.info(f"Created {len(alerts)} alerts in batch operation")
            return alert_ids
        except Exception as e:
            logger.error(f"Error batch creating alerts: {e}")
            raise

    def get_alert(self, alert_id: str) -> Optional[Alert]:
        """
        Retrieves an alert by its ID.

        Args:
            alert_id: Unique identifier of the alert

        Returns:
            Alert object if found, None otherwise
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE alert_id = @alert_id
            """
            
            # Set query parameters
            query_params = [
                {"name": "alert_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": alert_id}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process result
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                logger.debug(f"Retrieved alert: {alert_id}")
                return alert
            
            logger.debug(f"Alert not found: {alert_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving alert {alert_id}: {e}")
            return None

    def update_alert(self, alert: Alert) -> bool:
        """
        Updates an existing alert record in the database.

        Args:
            alert: Alert object with updated fields

        Returns:
            True if update was successful
        """
        try:
            # Validate alert has ID
            if not alert.alert_id:
                raise ValueError("Alert ID is required for update")
            
            # Convert to BigQuery row format
            row = alert.to_bigquery_row()
            
            # Construct query to update alert
            fields = []
            params = []
            param_idx = 0
            
            for key, value in row.items():
                if key != "alert_id" and key != "created_at":  # Don't update these fields
                    param_name = f"p{param_idx}"
                    fields.append(f"{key} = @{param_name}")
                    params.append({"name": param_name, "value": value})
                    param_idx += 1
            
            # Add alert_id parameter
            alert_id_param = {"name": "alert_id", "value": alert.alert_id}
            params.append(alert_id_param)
            
            # Build the query
            query = f"""
            UPDATE `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            SET {", ".join(fields)}
            WHERE alert_id = @alert_id
            """
            
            # Execute query
            query_params = []
            for param in params:
                param_type = "STRING"
                if isinstance(param["value"], bool):
                    param_type = "BOOL"
                elif isinstance(param["value"], (int, float)):
                    param_type = "NUMERIC"
                elif isinstance(param["value"], datetime.datetime):
                    param_type = "TIMESTAMP"
                    
                query_params.append({
                    "name": param["name"],
                    "parameterType": {"type": param_type},
                    "parameterValue": {"value": param["value"]}
                })
            
            result = self._bq_client.query(query, query_params)
            
            logger.info(f"Updated alert: {alert.alert_id} - Status: {alert.status}")
            return True
        except Exception as e:
            logger.error(f"Error updating alert {alert.alert_id}: {e}")
            return False

    def get_alerts_by_status(self, status: str, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts filtered by status.

        Args:
            status: Alert status (NEW, ACKNOWLEDGED, RESOLVED, SUPPRESSED)
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects matching the status
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE status = @status
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": status}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts with status '{status}'")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts by status {status}: {e}")
            return []

    def get_alerts_by_severity(self, severity: AlertSeverity, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts filtered by severity.

        Args:
            severity: Alert severity level
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects matching the severity
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE severity = @severity
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "severity", "parameterType": {"type": "STRING"}, "parameterValue": {"value": severity.value}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts with severity '{severity.value}'")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts by severity {severity.value}: {e}")
            return []

    def get_alerts_by_component(self, component: str, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts for a specific component.

        Args:
            component: Component name
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects for the component
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE component = @component
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "component", "parameterType": {"type": "STRING"}, "parameterValue": {"value": component}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts for component '{component}'")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts for component {component}: {e}")
            return []

    def get_alerts_by_execution_id(self, execution_id: str, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts for a specific pipeline execution.

        Args:
            execution_id: Pipeline execution ID
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects for the execution
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE execution_id = @execution_id
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "execution_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": execution_id}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts for execution '{execution_id}'")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts for execution {execution_id}: {e}")
            return []

    def get_alerts_by_time_range(self, start_time: datetime.datetime, end_time: datetime.datetime, 
                                limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts within a specific time range.

        Args:
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects in the time range
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE created_at >= @start_time AND created_at <= @end_time
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "start_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_time.isoformat()}},
                {"name": "end_time", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_time.isoformat()}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts between {start_time} and {end_time}")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts by time range: {e}")
            return []

    def get_active_alerts(self, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves active alerts (not resolved or suppressed).

        Args:
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of active Alert objects
        """
        try:
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE status NOT IN (@resolved_status, @suppressed_status)
            ORDER BY 
                CASE 
                    WHEN severity = 'CRITICAL' THEN 1
                    WHEN severity = 'HIGH' THEN 2
                    WHEN severity = 'MEDIUM' THEN 3
                    WHEN severity = 'LOW' THEN 4
                    ELSE 5
                END,
                created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "resolved_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": ALERT_STATUS_RESOLVED}},
                {"name": "suppressed_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": ALERT_STATUS_SUPPRESSED}},
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Retrieved {len(alerts)} active alerts")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving active alerts: {e}")
            return []

    def get_related_alerts(self, alert_id: str, limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Retrieves alerts related to a specific alert.

        Args:
            alert_id: ID of the alert
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of related Alert objects
        """
        try:
            # Get the alert to retrieve its related_alerts list
            alert = self.get_alert(alert_id)
            if not alert or not alert.related_alerts:
                return []
            
            # Escape the list of related alert IDs for SQL
            related_ids = [f"'{id}'" for id in alert.related_alerts]
            related_ids_str = ", ".join(related_ids)
            
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE alert_id IN ({related_ids_str})
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Set query parameters
            query_params = [
                {"name": "limit", "parameterType": {"type": "INT64"}, "parameterValue": {"value": limit}},
                {"name": "offset", "parameterType": {"type": "INT64"}, "parameterValue": {"value": offset}}
            ]
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            alerts = []
            for row in result:
                related_alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(related_alert)
            
            logger.debug(f"Retrieved {len(alerts)} alerts related to {alert_id}")
            return alerts
        except Exception as e:
            logger.error(f"Error retrieving alerts related to {alert_id}: {e}")
            return []

    def acknowledge_alert(self, alert_id: str, acknowledged_by: str, notes: str = None) -> bool:
        """
        Acknowledges an alert, updating its status.

        Args:
            alert_id: ID of the alert to acknowledge
            acknowledged_by: User or system that acknowledged the alert
            notes: Optional notes for the acknowledgment

        Returns:
            True if acknowledgment was successful
        """
        try:
            # Get the alert
            alert = self.get_alert(alert_id)
            if not alert:
                logger.warning(f"Cannot acknowledge alert {alert_id}: not found")
                return False
            
            # Acknowledge the alert
            if not alert.acknowledge(acknowledged_by, notes):
                return False
            
            # Update the alert in the database
            success = self.update_alert(alert)
            if success:
                logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
            
            return success
        except Exception as e:
            logger.error(f"Error acknowledging alert {alert_id}: {e}")
            return False

    def resolve_alert(self, alert_id: str, resolved_by: str, resolution_details: Dict[str, Any] = None) -> bool:
        """
        Resolves an alert, updating its status.

        Args:
            alert_id: ID of the alert to resolve
            resolved_by: User or system that resolved the alert
            resolution_details: Details about the resolution

        Returns:
            True if resolution was successful
        """
        try:
            # Get the alert
            alert = self.get_alert(alert_id)
            if not alert:
                logger.warning(f"Cannot resolve alert {alert_id}: not found")
                return False
            
            # Resolve the alert
            if not alert.resolve(resolved_by, resolution_details):
                return False
            
            # Update the alert in the database
            success = self.update_alert(alert)
            if success:
                logger.info(f"Alert {alert_id} resolved by {resolved_by}")
            
            return success
        except Exception as e:
            logger.error(f"Error resolving alert {alert_id}: {e}")
            return False

    def suppress_alert(self, alert_id: str, reason: str) -> bool:
        """
        Suppresses an alert, updating its status.

        Args:
            alert_id: ID of the alert to suppress
            reason: Reason for suppressing the alert

        Returns:
            True if suppression was successful
        """
        try:
            # Get the alert
            alert = self.get_alert(alert_id)
            if not alert:
                logger.warning(f"Cannot suppress alert {alert_id}: not found")
                return False
            
            # Suppress the alert
            if not alert.suppress(reason):
                return False
            
            # Update the alert in the database
            success = self.update_alert(alert)
            if success:
                logger.info(f"Alert {alert_id} suppressed: {reason}")
            
            return success
        except Exception as e:
            logger.error(f"Error suppressing alert {alert_id}: {e}")
            return False

    def add_related_alert(self, alert_id: str, related_alert_id: str) -> bool:
        """
        Adds a related alert to an existing alert.

        Args:
            alert_id: ID of the primary alert
            related_alert_id: ID of the related alert to add

        Returns:
            True if addition was successful
        """
        try:
            # Get the alert
            alert = self.get_alert(alert_id)
            if not alert:
                logger.warning(f"Cannot add related alert to {alert_id}: not found")
                return False
            
            # Add the related alert
            if not alert.add_related_alert(related_alert_id):
                return False
            
            # Update the alert in the database
            success = self.update_alert(alert)
            if success:
                logger.debug(f"Added related alert {related_alert_id} to {alert_id}")
            
            return success
        except Exception as e:
            logger.error(f"Error adding related alert {related_alert_id} to {alert_id}: {e}")
            return False

    def add_notification(self, alert_id: str, channel: NotificationChannel, 
                        recipient: str, success: bool, details: str = None) -> bool:
        """
        Records a notification sent for an alert.

        Args:
            alert_id: ID of the alert
            channel: Notification channel used
            recipient: Recipient of the notification
            success: Whether the notification was successfully delivered
            details: Additional details about the notification

        Returns:
            True if addition was successful
        """
        try:
            # Get the alert
            alert = self.get_alert(alert_id)
            if not alert:
                logger.warning(f"Cannot add notification to alert {alert_id}: not found")
                return False
            
            # Add the notification
            alert.add_notification(channel, recipient, success, details)
            
            # Update the alert in the database
            success = self.update_alert(alert)
            if success:
                logger.debug(f"Added {channel.value} notification for alert {alert_id}")
            
            return success
        except Exception as e:
            logger.error(f"Error adding notification to alert {alert_id}: {e}")
            return False

    def get_alert_count_by_status(self, time_window_hours: int = None) -> Dict[str, int]:
        """
        Counts alerts grouped by status.

        Args:
            time_window_hours: Optional time window in hours to restrict the count

        Returns:
            Dictionary with status as keys and counts as values
        """
        try:
            # Construct time window clause
            time_clause = ""
            if time_window_hours:
                time_clause = "WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
            
            # Construct query
            query = f"""
            SELECT 
                status, 
                COUNT(*) as count
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            {time_clause}
            GROUP BY status
            ORDER BY count DESC
            """
            
            # Set query parameters
            query_params = []
            if time_window_hours:
                query_params.append({
                    "name": "hours",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": time_window_hours}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            status_counts = {}
            total = 0
            for row in result:
                status_counts[row['status']] = row['count']
                total += row['count']
            
            logger.debug(f"Retrieved alert count by status: {total} total alerts")
            return status_counts
        except Exception as e:
            logger.error(f"Error getting alert count by status: {e}")
            return {}

    def get_alert_count_by_severity(self, time_window_hours: int = None) -> Dict[str, int]:
        """
        Counts alerts grouped by severity.

        Args:
            time_window_hours: Optional time window in hours to restrict the count

        Returns:
            Dictionary with severity as keys and counts as values
        """
        try:
            # Construct time window clause
            time_clause = ""
            if time_window_hours:
                time_clause = "WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
            
            # Construct query
            query = f"""
            SELECT 
                severity, 
                COUNT(*) as count
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            {time_clause}
            GROUP BY severity
            ORDER BY 
                CASE 
                    WHEN severity = 'CRITICAL' THEN 1
                    WHEN severity = 'HIGH' THEN 2
                    WHEN severity = 'MEDIUM' THEN 3
                    WHEN severity = 'LOW' THEN 4
                    ELSE 5
                END
            """
            
            # Set query parameters
            query_params = []
            if time_window_hours:
                query_params.append({
                    "name": "hours",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": time_window_hours}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            severity_counts = {}
            total = 0
            for row in result:
                severity_counts[row['severity']] = row['count']
                total += row['count']
            
            logger.debug(f"Retrieved alert count by severity: {total} total alerts")
            return severity_counts
        except Exception as e:
            logger.error(f"Error getting alert count by severity: {e}")
            return {}

    def get_alert_count_by_component(self, time_window_hours: int = None) -> Dict[str, int]:
        """
        Counts alerts grouped by component.

        Args:
            time_window_hours: Optional time window in hours to restrict the count

        Returns:
            Dictionary with component as keys and counts as values
        """
        try:
            # Construct time window clause
            time_clause = ""
            if time_window_hours:
                time_clause = "WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
            
            # Construct query
            query = f"""
            SELECT 
                component, 
                COUNT(*) as count
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            {time_clause}
            GROUP BY component
            ORDER BY count DESC
            """
            
            # Set query parameters
            query_params = []
            if time_window_hours:
                query_params.append({
                    "name": "hours",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": time_window_hours}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            component_counts = {}
            total = 0
            for row in result:
                component = row['component'] or 'unknown'
                component_counts[component] = row['count']
                total += row['count']
            
            logger.debug(f"Retrieved alert count by component: {total} total alerts")
            return component_counts
        except Exception as e:
            logger.error(f"Error getting alert count by component: {e}")
            return {}

    def get_alert_trend(self, interval: str = 'hourly', 
                      num_intervals: int = 24, 
                      severity: AlertSeverity = None) -> pd.DataFrame:
        """
        Retrieves alert trend over time intervals.

        Args:
            interval: Time interval ('hourly', 'daily', 'weekly')
            num_intervals: Number of intervals to retrieve
            severity: Optional severity filter

        Returns:
            DataFrame with time intervals and alert counts
        """
        try:
            # Validate interval
            if interval not in ('hourly', 'daily', 'weekly'):
                raise ValueError("Interval must be 'hourly', 'daily', or 'weekly'")
            
            # Determine timestamp format and interval expression
            if interval == 'hourly':
                timestamp_fmt = "FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', created_at)"
                interval_expr = "INTERVAL @num_intervals HOUR"
            elif interval == 'daily':
                timestamp_fmt = "FORMAT_TIMESTAMP('%Y-%m-%d', created_at)"
                interval_expr = "INTERVAL @num_intervals DAY"
            else:  # weekly
                timestamp_fmt = "FORMAT_TIMESTAMP('%Y-%m-%d', DATE_TRUNC(created_at, WEEK))"
                interval_expr = "INTERVAL @num_intervals * 7 DAY"
            
            # Add severity filter if specified
            severity_clause = ""
            if severity:
                severity_clause = "AND severity = @severity"
            
            # Construct query
            query = f"""
            SELECT 
                {timestamp_fmt} as time_interval,
                COUNT(*) as alert_count
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), {interval_expr})
            {severity_clause}
            GROUP BY time_interval
            ORDER BY time_interval
            """
            
            # Set query parameters
            query_params = [
                {"name": "num_intervals", "parameterType": {"type": "INT64"}, "parameterValue": {"value": num_intervals}}
            ]
            
            if severity:
                query_params.append({
                    "name": "severity",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": severity.value}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results into DataFrame
            data = []
            for row in result:
                data.append({
                    'time_interval': row['time_interval'],
                    'alert_count': row['alert_count']
                })
            
            df = pd.DataFrame(data)
            
            # If no data was returned, create an empty DataFrame with columns
            if len(df) == 0:
                df = pd.DataFrame(columns=['time_interval', 'alert_count'])
            
            # Fill in missing intervals with zero counts
            # This logic would need to be expanded in a real implementation
            # to handle filling in intervals properly based on the interval type
            
            logger.debug(f"Retrieved alert trend data with {len(df)} intervals")
            return df
        except Exception as e:
            logger.error(f"Error getting alert trend: {e}")
            return pd.DataFrame(columns=['time_interval', 'alert_count'])

    def search_alerts(self, search_criteria: Dict[str, Any], limit: int = 100, offset: int = 0) -> List[Alert]:
        """
        Searches alerts based on multiple criteria.

        Args:
            search_criteria: Dictionary of search criteria
            limit: Maximum number of alerts to return
            offset: Number of alerts to skip for pagination

        Returns:
            List of Alert objects matching criteria
        """
        try:
            # Extract search parameters
            alert_type = search_criteria.get('alert_type')
            severity = search_criteria.get('severity')
            component = search_criteria.get('component')
            status = search_criteria.get('status')
            execution_id = search_criteria.get('execution_id')
            start_time = search_criteria.get('start_time')
            end_time = search_criteria.get('end_time')
            text_search = search_criteria.get('text_search')
            
            # Build query
            conditions = []
            params = []
            
            if alert_type:
                conditions.append("alert_type = @alert_type")
                params.append({
                    "name": "alert_type",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": alert_type}
                })
            
            if severity:
                severity_value = severity.value if isinstance(severity, AlertSeverity) else severity
                conditions.append("severity = @severity")
                params.append({
                    "name": "severity",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": severity_value}
                })
            
            if component:
                conditions.append("component = @component")
                params.append({
                    "name": "component",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": component}
                })
            
            if status:
                conditions.append("status = @status")
                params.append({
                    "name": "status",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": status}
                })
            
            if execution_id:
                conditions.append("execution_id = @execution_id")
                params.append({
                    "name": "execution_id",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": execution_id}
                })
            
            if start_time:
                conditions.append("created_at >= @start_time")
                params.append({
                    "name": "start_time",
                    "parameterType": {"type": "TIMESTAMP"},
                    "parameterValue": {"value": start_time.isoformat() if isinstance(start_time, datetime.datetime) else start_time}
                })
            
            if end_time:
                conditions.append("created_at <= @end_time")
                params.append({
                    "name": "end_time",
                    "parameterType": {"type": "TIMESTAMP"},
                    "parameterValue": {"value": end_time.isoformat() if isinstance(end_time, datetime.datetime) else end_time}
                })
            
            if text_search:
                conditions.append("(description LIKE @text_search OR JSON_EXTRACT_SCALAR(context, '$') LIKE @text_search)")
                params.append({
                    "name": "text_search",
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": f"%{text_search}%"}
                })
            
            # Add pagination parameters
            params.append({
                "name": "limit",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": limit}
            })
            params.append({
                "name": "offset",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": offset}
            })
            
            # Construct WHERE clause
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            # Construct query
            query = f"""
            SELECT * 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            {where_clause}
            ORDER BY created_at DESC
            LIMIT @limit
            OFFSET @offset
            """
            
            # Execute query
            result = self._bq_client.query(query, params)
            
            # Process results
            alerts = []
            for row in result:
                alert = Alert.from_bigquery_row(dict(row.items()))
                alerts.append(alert)
            
            logger.debug(f"Search returned {len(alerts)} alerts")
            return alerts
        except Exception as e:
            logger.error(f"Error searching alerts: {e}")
            return []

    def delete_old_alerts(self, cutoff_date: datetime.datetime) -> int:
        """
        Deletes alerts older than a specified date.

        Args:
            cutoff_date: Alerts created before this date will be deleted

        Returns:
            Number of alerts deleted
        """
        try:
            # Count affected rows
            count_query = f"""
            SELECT COUNT(*) as count
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE created_at < @cutoff_date
            """
            
            # Set query parameters
            query_params = [
                {"name": "cutoff_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": cutoff_date.isoformat()}}
            ]
            
            # Execute count query
            count_result = self._bq_client.query(count_query, query_params)
            count = 0
            for row in count_result:
                count = row['count']
            
            # Perform deletion
            delete_query = f"""
            DELETE 
            FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
            WHERE created_at < @cutoff_date
            """
            
            # Execute delete query
            self._bq_client.query(delete_query, query_params)
            
            logger.info(f"Deleted {count} alerts older than {cutoff_date}")
            return count
        except Exception as e:
            logger.error(f"Error deleting old alerts: {e}")
            return 0

    def get_notification_stats(self, time_window_hours: int = None) -> Dict[str, Any]:
        """
        Retrieves statistics about alert notifications.

        Args:
            time_window_hours: Optional time window in hours to restrict the analysis

        Returns:
            Dictionary with notification statistics
        """
        try:
            # Construct time window clause
            time_clause = ""
            if time_window_hours:
                time_clause = "WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
            
            # Construct query
            query = f"""
            WITH notification_data AS (
                SELECT
                    JSON_EXTRACT_ARRAY(notifications) as notifications_array
                FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
                {time_clause}
                WHERE notifications IS NOT NULL
            ),
            flattened AS (
                SELECT
                    JSON_EXTRACT_SCALAR(notification, '$.channel') as channel,
                    JSON_EXTRACT_SCALAR(notification, '$.success') as success
                FROM notification_data,
                UNNEST(JSON_EXTRACT_ARRAY(notifications_array)) as notification
            )
            SELECT
                channel,
                COUNTIF(success = 'true') as success_count,
                COUNTIF(success = 'false') as failure_count,
                COUNT(*) as total_count,
                SAFE_DIVIDE(COUNTIF(success = 'true'), COUNT(*)) as success_rate
            FROM flattened
            GROUP BY channel
            ORDER BY total_count DESC
            """
            
            # Set query parameters
            query_params = []
            if time_window_hours:
                query_params.append({
                    "name": "hours",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": time_window_hours}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            stats = {
                'channels': {},
                'total_success': 0,
                'total_failure': 0,
                'total_notifications': 0
            }
            
            for row in result:
                channel = row['channel']
                stats['channels'][channel] = {
                    'success': row['success_count'],
                    'failure': row['failure_count'],
                    'total': row['total_count'],
                    'success_rate': row['success_rate']
                }
                stats['total_success'] += row['success_count']
                stats['total_failure'] += row['failure_count']
                stats['total_notifications'] += row['total_count']
            
            if stats['total_notifications'] > 0:
                stats['overall_success_rate'] = stats['total_success'] / stats['total_notifications']
            else:
                stats['overall_success_rate'] = 0
            
            logger.debug(f"Retrieved notification stats: {stats['total_notifications']} total notifications")
            return stats
        except Exception as e:
            logger.error(f"Error getting notification stats: {e}")
            return {'channels': {}, 'total_success': 0, 'total_failure': 0, 'total_notifications': 0}

    def get_resolution_time_stats(self, time_window_hours: int = None) -> Dict[str, Any]:
        """
        Calculates statistics about alert resolution times.

        Args:
            time_window_hours: Optional time window in hours to restrict the analysis

        Returns:
            Dictionary with resolution time statistics
        """
        try:
            # Construct time window clause
            time_clause = ""
            if time_window_hours:
                time_clause = "AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)"
            
            # Construct query
            query = f"""
            WITH resolution_times AS (
                SELECT
                    severity,
                    TIMESTAMP_DIFF(resolved_at, created_at, SECOND) as resolution_time_seconds
                FROM `{self._project_id}.{self._dataset_id}.{ALERT_TABLE_NAME}`
                WHERE 
                    status = @resolved_status
                    AND resolved_at IS NOT NULL
                    {time_clause}
            )
            SELECT
                severity,
                COUNT(*) as count,
                AVG(resolution_time_seconds) as avg_seconds,
                MIN(resolution_time_seconds) as min_seconds,
                MAX(resolution_time_seconds) as max_seconds,
                APPROX_QUANTILES(resolution_time_seconds, 100)[OFFSET(50)] as median_seconds,
                APPROX_QUANTILES(resolution_time_seconds, 100)[OFFSET(95)] as p95_seconds
            FROM resolution_times
            GROUP BY severity
            ORDER BY 
                CASE 
                    WHEN severity = 'CRITICAL' THEN 1
                    WHEN severity = 'HIGH' THEN 2
                    WHEN severity = 'MEDIUM' THEN 3
                    WHEN severity = 'LOW' THEN 4
                    ELSE 5
                END
            """
            
            # Set query parameters
            query_params = [
                {"name": "resolved_status", "parameterType": {"type": "STRING"}, "parameterValue": {"value": ALERT_STATUS_RESOLVED}}
            ]
            
            if time_window_hours:
                query_params.append({
                    "name": "hours",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": time_window_hours}
                })
            
            # Execute query
            result = self._bq_client.query(query, query_params)
            
            # Process results
            stats = {
                'by_severity': {},
                'total_resolved': 0,
                'overall_avg_seconds': 0,
                'overall_min_seconds': float('inf'),
                'overall_max_seconds': 0
            }
            
            total_weighted_avg = 0
            for row in result:
                severity = row['severity']
                stats['by_severity'][severity] = {
                    'count': row['count'],
                    'avg_seconds': row['avg_seconds'],
                    'min_seconds': row['min_seconds'],
                    'max_seconds': row['max_seconds'],
                    'median_seconds': row['median_seconds'],
                    'p95_seconds': row['p95_seconds'],
                    'avg_minutes': row['avg_seconds'] / 60,
                    'avg_hours': row['avg_seconds'] / 3600
                }
                
                stats['total_resolved'] += row['count']
                total_weighted_avg += row['avg_seconds'] * row['count']
                stats['overall_min_seconds'] = min(stats['overall_min_seconds'], row['min_seconds'])
                stats['overall_max_seconds'] = max(stats['overall_max_seconds'], row['max_seconds'])
            
            # Calculate overall statistics
            if stats['total_resolved'] > 0:
                stats['overall_avg_seconds'] = total_weighted_avg / stats['total_resolved']
                stats['overall_avg_minutes'] = stats['overall_avg_seconds'] / 60
                stats['overall_avg_hours'] = stats['overall_avg_seconds'] / 3600
            else:
                stats['overall_min_seconds'] = 0
            
            logger.debug(f"Retrieved resolution time stats for {stats['total_resolved']} resolved alerts")
            return stats
        except Exception as e:
            logger.error(f"Error getting resolution time stats: {e}")
            return {'by_severity': {}, 'total_resolved': 0}