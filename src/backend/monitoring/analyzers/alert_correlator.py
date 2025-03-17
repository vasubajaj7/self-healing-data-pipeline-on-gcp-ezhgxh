"""
Alert correlation functionality for the self-healing data pipeline.

This module provides classes and functions to correlate related alerts,
identify root causes, and reduce alert noise through intelligent grouping.
It helps prevent alert fatigue by identifying patterns and causal relationships
between alerts.
"""

import datetime
import uuid
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import numpy as np

from constants import AlertSeverity
from config import get_config
from logging_config import get_logger
from db.models.alert import Alert
from db.repositories.alert_repository import AlertRepository

# Configure logger
logger = get_logger(__name__)

# Default configuration values
DEFAULT_SIMILARITY_THRESHOLD = 0.7
DEFAULT_TIME_WINDOW_MINUTES = 60
DEFAULT_GROUP_TTL_MINUTES = 120
DEFAULT_MAX_GROUP_SIZE = 50


def calculate_similarity_score(alert1: Alert, alert2: Alert) -> float:
    """
    Calculates a similarity score between two alerts based on their properties
    
    Args:
        alert1: First alert to compare
        alert2: Second alert to compare
        
    Returns:
        Similarity score between 0.0 and 1.0
    """
    # Extract key alert features for comparison
    type_match = 1.0 if alert1.alert_type == alert2.alert_type else 0.0
    
    # Component similarity
    component_match = 1.0 if alert1.component == alert2.component and alert1.component is not None else 0.0
    
    # Execution similarity
    execution_match = 1.0 if alert1.execution_id == alert2.execution_id and alert1.execution_id is not None else 0.0
    
    # Context similarity (Jaccard similarity on keys and values)
    context_similarity = 0.0
    if alert1.context and alert2.context:
        # Convert context items to sets for comparison
        context1_items = set(f"{k}:{v}" for k, v in alert1.context.items())
        context2_items = set(f"{k}:{v}" for k, v in alert2.context.items())
        
        # Calculate Jaccard similarity: intersection/union
        if context1_items or context2_items:
            intersection = len(context1_items.intersection(context2_items))
            union = len(context1_items.union(context2_items))
            context_similarity = intersection / union if union > 0 else 0.0
    
    # Temporal similarity - alerts closer in time are more similar
    time_diff_seconds = abs((alert1.created_at - alert2.created_at).total_seconds())
    time_window_seconds = DEFAULT_TIME_WINDOW_MINUTES * 60
    temporal_similarity = max(0, 1 - (time_diff_seconds / time_window_seconds))
    
    # Weighted combination of all similarity factors
    weights = {
        'type': 0.3,
        'component': 0.2,
        'execution': 0.2,
        'context': 0.2,
        'temporal': 0.1
    }
    
    similarity = (
        weights['type'] * type_match +
        weights['component'] * component_match +
        weights['execution'] * execution_match +
        weights['context'] * context_similarity +
        weights['temporal'] * temporal_similarity
    )
    
    return similarity


def is_potential_cause(potential_cause: Alert, potential_effect: Alert) -> bool:
    """
    Determines if one alert is a potential cause of another alert
    
    Args:
        potential_cause: Alert that might be a cause
        potential_effect: Alert that might be an effect
        
    Returns:
        True if potential_cause is likely a cause of potential_effect
    """
    # Temporal check - cause must precede effect
    if potential_cause.created_at >= potential_effect.created_at:
        return False
    
    # Check if alerts are related by component or execution
    related_by_component = potential_cause.component == potential_effect.component and potential_cause.component is not None
    related_by_execution = potential_cause.execution_id == potential_effect.execution_id and potential_effect.execution_id is not None
    
    if not (related_by_component or related_by_execution):
        # If not directly related, they are less likely to have a causal relationship
        # Still possible but would require a high contextual similarity
        context_similarity = 0.0
        if potential_cause.context and potential_effect.context:
            # Convert context items to sets for comparison
            context1_items = set(f"{k}:{v}" for k, v in potential_cause.context.items())
            context2_items = set(f"{k}:{v}" for k, v in potential_effect.context.items())
            
            if context1_items or context2_items:
                intersection = len(context1_items.intersection(context2_items))
                union = len(context1_items.union(context2_items))
                context_similarity = intersection / union if union > 0 else 0.0
        
        # Require very high context similarity if not related by component or execution
        if context_similarity < 0.8:
            return False
    
    # Severity check - cause usually has higher or equal severity
    # Convert severity to numeric value for comparison
    severity_values = {
        AlertSeverity.CRITICAL: 4,
        AlertSeverity.HIGH: 3,
        AlertSeverity.MEDIUM: 2,
        AlertSeverity.LOW: 1,
        AlertSeverity.INFO: 0
    }
    
    cause_severity = severity_values.get(potential_cause.severity, 0)
    effect_severity = severity_values.get(potential_effect.severity, 0)
    
    # Causes typically have higher or equal severity than effects
    # But this is a heuristic, not a strict rule
    if cause_severity < effect_severity - 1:  # Allow one level of difference
        return False
    
    # Time window check - effects typically occur within a limited time after causes
    time_diff_seconds = (potential_effect.created_at - potential_cause.created_at).total_seconds()
    max_cause_effect_window = DEFAULT_TIME_WINDOW_MINUTES * 60
    
    if time_diff_seconds > max_cause_effect_window:
        return False
    
    # Look for specific causal indicators in context
    cause_indicators = False
    if (potential_cause.context and potential_effect.context and
        'resource_id' in potential_cause.context and 'resource_id' in potential_effect.context and
        potential_cause.context['resource_id'] == potential_effect.context['resource_id']):
        cause_indicators = True
    
    # Additional causal indicators could be added here
    
    # Final decision based on all factors
    return (related_by_component or related_by_execution or cause_indicators)


def extract_alert_features(alert: Alert) -> Dict[str, Any]:
    """
    Extracts key features from an alert for correlation analysis
    
    Args:
        alert: Alert to extract features from
        
    Returns:
        Dictionary of extracted features
    """
    features = {
        'alert_type': alert.alert_type,
        'component': alert.component,
        'execution_id': alert.execution_id,
        'created_at': alert.created_at,
        'severity': alert.severity,
        'context_keys': set(alert.context.keys()) if alert.context else set(),
    }
    
    # Extract specific context values that are useful for correlation
    if alert.context:
        for key in ['resource_id', 'service', 'pipeline_id', 'task_id', 'error_code']:
            if key in alert.context:
                features[f'context_{key}'] = alert.context[key]
    
    return features


class AlertGroup:
    """
    Represents a group of correlated alerts with shared context and root cause analysis
    """
    
    def __init__(self, name: str = None, ttl_minutes: int = DEFAULT_GROUP_TTL_MINUTES):
        """
        Initializes a new alert group
        
        Args:
            name: Name for the group, generated from first alert if not provided
            ttl_minutes: Time-to-live in minutes for the group
        """
        self.group_id = str(uuid.uuid4())
        self.name = name or f"Alert Group {self.group_id[:8]}"
        self.alerts = []  # List of alerts in this group
        self.root_causes = []  # List of alerts identified as root causes
        self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        self.expires_at = self.created_at + datetime.timedelta(minutes=ttl_minutes)
        self.suppression_enabled = False  # Whether to suppress similar alerts
    
    def add_alert(self, alert: Alert) -> bool:
        """
        Adds an alert to the group if not already present
        
        Args:
            alert: Alert to add to the group
            
        Returns:
            True if alert was added, False if already in group
        """
        # Check if alert is already in the group
        for existing_alert in self.alerts:
            if existing_alert.alert_id == alert.alert_id:
                return False
        
        # Add alert to the group
        self.alerts.append(alert)
        self.updated_at = datetime.datetime.now()
        
        # If this is the first alert, use it to name the group
        if len(self.alerts) == 1 and self.name.startswith("Alert Group"):
            self.name = f"{alert.alert_type} - {alert.component or 'Unknown'}"
        
        # Extend expiration time
        self.extend_expiration(DEFAULT_GROUP_TTL_MINUTES // 2)  # Extend by half the default TTL
        
        return True
    
    def set_root_causes(self, root_cause_alerts: List[Alert]) -> None:
        """
        Sets the root cause alerts for this group
        
        Args:
            root_cause_alerts: List of alerts that are root causes
        """
        # Validate all root causes are in the group
        for alert in root_cause_alerts:
            if alert not in self.alerts:
                raise ValueError(f"Root cause alert {alert.alert_id} is not in the group")
        
        self.root_causes = root_cause_alerts
        self.updated_at = datetime.datetime.now()
    
    def should_suppress(self, alert: Alert) -> bool:
        """
        Determines if a new alert should be suppressed based on group policy
        
        Args:
            alert: Alert to check for suppression
            
        Returns:
            True if alert should be suppressed
        """
        if not self.suppression_enabled:
            return False
        
        # Don't suppress if the group is empty or expired
        if not self.alerts or not self.is_active():
            return False
        
        # Don't suppress critical alerts
        if alert.severity == AlertSeverity.CRITICAL:
            return False
        
        # Check similarity with existing alerts in the group
        for existing_alert in self.alerts:
            similarity = calculate_similarity_score(alert, existing_alert)
            
            # Suppress if very similar to an existing alert
            if similarity > 0.9:
                return True
        
        return False
    
    def is_active(self) -> bool:
        """
        Checks if the alert group is still active (not expired)
        
        Returns:
            True if active, False if expired
        """
        return datetime.datetime.now() < self.expires_at
    
    def extend_expiration(self, minutes: int) -> None:
        """
        Extends the expiration time of the group
        
        Args:
            minutes: Number of minutes to extend by
        """
        self.expires_at = self.expires_at + datetime.timedelta(minutes=minutes)
        self.updated_at = datetime.datetime.now()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Generates a summary of the alert group
        
        Returns:
            Dictionary containing group summary information
        """
        # Count alerts by severity
        severity_counts = {}
        for alert in self.alerts:
            severity = alert.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        # Get most recent alert
        most_recent = max(self.alerts, key=lambda a: a.created_at) if self.alerts else None
        
        summary = {
            'group_id': self.group_id,
            'name': self.name,
            'alert_count': len(self.alerts),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'expires_at': self.expires_at.isoformat(),
            'is_active': self.is_active(),
            'severity_distribution': severity_counts,
            'root_cause_count': len(self.root_causes),
            'most_recent_alert': most_recent.alert_id if most_recent else None,
            'most_recent_time': most_recent.created_at.isoformat() if most_recent else None,
        }
        
        # Add root cause info if available
        if self.root_causes:
            summary['root_causes'] = [rc.alert_id for rc in self.root_causes]
            causes_summary = []
            for rc in self.root_causes:
                causes_summary.append({
                    'id': rc.alert_id,
                    'type': rc.alert_type,
                    'component': rc.component,
                    'description': rc.description
                })
            summary['root_causes_details'] = causes_summary
        
        return summary
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the group to a dictionary representation for serialization
        
        Returns:
            Dictionary representation of the group
        """
        return {
            'group_id': self.group_id,
            'name': self.name,
            'alerts': [alert.alert_id for alert in self.alerts],
            'root_causes': [rc.alert_id for rc in self.root_causes],
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'expires_at': self.expires_at.isoformat(),
            'suppression_enabled': self.suppression_enabled
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], alert_repository: AlertRepository) -> 'AlertGroup':
        """
        Creates an AlertGroup from a dictionary representation
        
        Args:
            data: Dictionary containing group data
            alert_repository: Repository to load alerts
            
        Returns:
            Alert group instance
        """
        group = cls(name=data.get('name'))
        
        # Set group properties
        group.group_id = data['group_id']
        group.created_at = datetime.datetime.fromisoformat(data['created_at'])
        group.updated_at = datetime.datetime.fromisoformat(data['updated_at'])
        group.expires_at = datetime.datetime.fromisoformat(data['expires_at'])
        group.suppression_enabled = data.get('suppression_enabled', False)
        
        # Load alerts from repository
        for alert_id in data.get('alerts', []):
            alert = alert_repository.get_alert(alert_id)
            if alert:
                group.alerts.append(alert)
        
        # Load root causes
        root_causes = []
        for rc_id in data.get('root_causes', []):
            # Check if already in alerts list first for efficiency
            rc = next((a for a in group.alerts if a.alert_id == rc_id), None)
            if not rc:
                rc = alert_repository.get_alert(rc_id)
            if rc:
                root_causes.append(rc)
        
        group.root_causes = root_causes
        
        return group


class AlertCorrelator:
    """
    Correlates related alerts to reduce noise and identify root causes
    """
    
    def __init__(self, alert_repository: AlertRepository = None, config_override: Dict[str, Any] = None):
        """
        Initializes the AlertCorrelator with configuration and dependencies
        
        Args:
            alert_repository: Repository for alert storage and retrieval
            config_override: Optional configuration overrides
        """
        # Set alert repository
        self._alert_repository = alert_repository
        
        # Load configuration
        config = get_config()
        self._config = {
            'similarity_threshold': config.get('alerts.correlation.similarity_threshold', DEFAULT_SIMILARITY_THRESHOLD),
            'time_window_minutes': config.get('alerts.correlation.time_window_minutes', DEFAULT_TIME_WINDOW_MINUTES),
            'group_ttl_minutes': config.get('alerts.correlation.group_ttl_minutes', DEFAULT_GROUP_TTL_MINUTES),
            'max_group_size': config.get('alerts.correlation.max_group_size', DEFAULT_MAX_GROUP_SIZE)
        }
        
        # Apply any configuration overrides
        if config_override:
            self._config.update(config_override)
        
        # Set instance variables from config
        self._similarity_threshold = self._config['similarity_threshold']
        self._time_window_minutes = self._config['time_window_minutes']
        self._group_ttl_minutes = self._config['group_ttl_minutes']
        self._max_group_size = self._config['max_group_size']
        
        # Initialize alert groups dictionary
        self._alert_groups = {}
        
        # Try to load existing groups if available
        self.load_groups()
        
        logger.info(f"AlertCorrelator initialized with {len(self._alert_groups)} groups")
    
    def correlate_alerts(self, alerts: List[Alert]) -> Dict[str, AlertGroup]:
        """
        Correlates a list of alerts into logical groups
        
        Args:
            alerts: List of alerts to correlate
            
        Returns:
            Dictionary of alert groups with correlated alerts
        """
        # Clean up expired groups first
        self.cleanup_old_groups()
        
        # Process each alert
        for alert in alerts:
            self.process_alert(alert)
        
        # Identify root causes within each group
        for group_id, group in self._alert_groups.items():
            if group.is_active() and not group.root_causes:
                root_causes = self.identify_root_causes(group)
                if root_causes:
                    group.set_root_causes(root_causes)
        
        # Return active groups
        return {gid: group for gid, group in self._alert_groups.items() if group.is_active()}
    
    def process_alert(self, alert: Alert) -> str:
        """
        Processes a single alert for correlation
        
        Args:
            alert: Alert to process
            
        Returns:
            Group ID that the alert was assigned to
        """
        # Find the most similar group for this alert
        group_id, similarity = self.find_similar_group(alert)
        
        if group_id and similarity >= self._similarity_threshold:
            # Add alert to existing group
            group = self._alert_groups[group_id]
            group.add_alert(alert)
            logger.debug(f"Added alert {alert.alert_id} to existing group {group_id} with similarity {similarity:.2f}")
        else:
            # Create a new group for this alert
            group = AlertGroup(ttl_minutes=self._group_ttl_minutes)
            group.add_alert(alert)
            self._alert_groups[group.group_id] = group
            group_id = group.group_id
            logger.debug(f"Created new group {group_id} for alert {alert.alert_id}")
        
        # Update group root causes if we have enough alerts
        if len(self._alert_groups[group_id].alerts) >= 2:
            root_causes = self.identify_root_causes(self._alert_groups[group_id])
            if root_causes:
                self._alert_groups[group_id].set_root_causes(root_causes)
                logger.debug(f"Identified {len(root_causes)} root causes for group {group_id}")
        
        # Save groups after processing
        self.save_groups()
        
        return group_id
    
    def find_similar_group(self, alert: Alert) -> Tuple[Optional[str], float]:
        """
        Finds the most similar alert group for an alert
        
        Args:
            alert: Alert to find a group for
            
        Returns:
            Tuple of (group_id, similarity_score) or (None, 0) if no match
        """
        best_group_id = None
        best_similarity = 0.0
        
        # Check each active group for similarity
        for group_id, group in self._alert_groups.items():
            if not group.is_active():
                continue
                
            # Skip groups that are already at max capacity
            if len(group.alerts) >= self._max_group_size:
                continue
                
            # Calculate similarity with this group
            similarity = self.calculate_group_similarity(alert, group)
            
            # Update best match if this is better
            if similarity > best_similarity:
                best_similarity = similarity
                best_group_id = group_id
        
        return best_group_id, best_similarity
    
    def calculate_group_similarity(self, alert: Alert, group: AlertGroup) -> float:
        """
        Calculates similarity between an alert and a group
        
        Args:
            alert: Alert to compare
            group: Alert group to compare against
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        # If group is empty, no similarity
        if not group.alerts:
            return 0.0
        
        # Calculate similarity with each alert in the group
        similarities = [calculate_similarity_score(alert, group_alert) for group_alert in group.alerts]
        
        # Return the highest similarity score
        return max(similarities) if similarities else 0.0
    
    def identify_root_causes(self, group: AlertGroup) -> List[Alert]:
        """
        Identifies potential root cause alerts within a group
        
        Args:
            group: Alert group to analyze
            
        Returns:
            List of alerts identified as root causes
        """
        if not group.alerts or len(group.alerts) < 2:
            return []
        
        # Build a graph of causal relationships
        # causes[alert_id] = list of alerts caused by this alert
        causes = defaultdict(list)
        
        # effects[alert_id] = list of alerts that caused this alert
        effects = defaultdict(list)
        
        # Check all pairs of alerts for causal relationships
        for i, alert1 in enumerate(group.alerts):
            for j, alert2 in enumerate(group.alerts):
                if i == j:
                    continue
                
                # Check if alert1 is a potential cause of alert2
                if is_potential_cause(alert1, alert2):
                    causes[alert1.alert_id].append(alert2)
                    effects[alert2.alert_id].append(alert1)
        
        # Identify alerts that are causes but not effects (or fewer effects than causes)
        root_candidates = []
        for alert in group.alerts:
            # If alert causes others but is not caused by others, it's a root candidate
            if alert.alert_id in causes and (
                alert.alert_id not in effects or 
                len(causes[alert.alert_id]) > len(effects[alert.alert_id])
            ):
                root_candidates.append(alert)
        
        # If no root candidates found, use the earliest alert as fallback
        if not root_candidates and group.alerts:
            earliest_alert = min(group.alerts, key=lambda a: a.created_at)
            root_candidates.append(earliest_alert)
        
        # If we have too many candidates, prioritize by severity and timestamp
        if len(root_candidates) > 3:  # Limit to top 3 root causes
            # Sort by severity (highest first) and then by timestamp (earliest first)
            severity_order = {
                AlertSeverity.CRITICAL: 0,
                AlertSeverity.HIGH: 1,
                AlertSeverity.MEDIUM: 2,
                AlertSeverity.LOW: 3,
                AlertSeverity.INFO: 4
            }
            root_candidates.sort(
                key=lambda a: (severity_order.get(a.severity, 5), a.created_at)
            )
            root_candidates = root_candidates[:3]
        
        return root_candidates
    
    def should_suppress_alert(self, alert: Alert, group_id: str = None) -> bool:
        """
        Determines if an alert should be suppressed based on correlation
        
        Args:
            alert: Alert to check for suppression
            group_id: Optional group ID to check against
            
        Returns:
            True if alert should be suppressed
        """
        # If group_id provided, check only that group
        if group_id:
            group = self.get_alert_group(group_id)
            if group:
                return group.should_suppress(alert)
            return False
        
        # Otherwise check all active groups
        for group in self._alert_groups.values():
            if group.is_active() and group.should_suppress(alert):
                return True
        
        return False
    
    def get_alert_group(self, group_id: str) -> Optional[AlertGroup]:
        """
        Gets an alert group by ID
        
        Args:
            group_id: ID of the group to retrieve
            
        Returns:
            AlertGroup if found and active, None otherwise
        """
        group = self._alert_groups.get(group_id)
        if group and group.is_active():
            return group
        return None
    
    def get_all_groups(self) -> Dict[str, AlertGroup]:
        """
        Gets all active alert groups
        
        Returns:
            Dictionary of active alert groups
        """
        return {gid: group for gid, group in self._alert_groups.items() if group.is_active()}
    
    def cleanup_old_groups(self) -> int:
        """
        Removes expired groups from the correlator
        
        Returns:
            Number of groups removed
        """
        expired_groups = [gid for gid, group in self._alert_groups.items() if not group.is_active()]
        
        for group_id in expired_groups:
            del self._alert_groups[group_id]
        
        if expired_groups:
            logger.info(f"Cleaned up {len(expired_groups)} expired alert groups")
        
        return len(expired_groups)
    
    def save_groups(self) -> bool:
        """
        Saves alert groups to persistent storage
        
        Returns:
            True if save was successful
        """
        try:
            # Convert groups to serializable format
            groups_data = {
                group_id: group.to_dict()
                for group_id, group in self._alert_groups.items()
                if group.is_active()  # Only save active groups
            }
            
            # TODO: In a real implementation, this would save to a persistent store
            # For example, to BigQuery, Firestore, or a file
            
            # For now, we'll just log the success
            logger.debug(f"Saved {len(groups_data)} alert groups")
            return True
        except Exception as e:
            logger.error(f"Error saving alert groups: {e}")
            return False
    
    def load_groups(self) -> bool:
        """
        Loads alert groups from persistent storage
        
        Returns:
            True if load was successful
        """
        try:
            # TODO: In a real implementation, this would load from a persistent store
            # For example, from BigQuery, Firestore, or a file
            
            # For now, we'll just log that no groups were loaded
            logger.debug("No saved alert groups found (persistent storage not implemented)")
            return True
        except Exception as e:
            logger.error(f"Error loading alert groups: {e}")
            return False