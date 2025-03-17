"""
Implements a state tracking system for monitoring the status and transitions of pipeline components.

This module collects, stores, and provides access to the current and historical states of various
pipeline components, enabling monitoring, alerting, and self-healing capabilities based on state changes.
It uses Firestore for current state storage and BigQuery for historical state analysis.
"""

import datetime
import uuid
import json
from typing import Dict, List, Optional, Any, Union, Tuple

from datetime import datetime

from ....constants import (
    PIPELINE_STATUS_PENDING,
    PIPELINE_STATUS_RUNNING,
    PIPELINE_STATUS_SUCCESS,
    PIPELINE_STATUS_FAILED,
    PIPELINE_STATUS_HEALING,
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCESS,
    TASK_STATUS_FAILED
)
from ....config import get_config
from ....logging_config import get_logger
from ....utils.storage.firestore_client import FirestoreClient
from ....utils.storage.bigquery_client import BigQueryClient

# Module logger
logger = get_logger(__name__)

# Collection and table names for state storage
STATE_COLLECTION_NAME = "component_states"
STATE_HISTORY_TABLE = "component_state_history"
DEFAULT_STATE_RETENTION_DAYS = 30


def generate_state_id(component_id: str, component_type: str) -> str:
    """Generates a unique identifier for a component state record.

    Args:
        component_id: Unique identifier of the component
        component_type: Type of component (pipeline, task, etc.)

    Returns:
        Unique state ID with 'state_' prefix
    """
    unique_id = str(uuid.uuid4())
    return f"state_{unique_id}"


def validate_state_transition(current_state: str, new_state: str, allowed_transitions: List[Tuple[str, str]]) -> bool:
    """Validates if a state transition is allowed based on transition rules.

    Args:
        current_state: The current state of the component
        new_state: The new state to transition to
        allowed_transitions: List of allowed transition tuples (from_state, to_state)

    Returns:
        True if transition is valid, False otherwise
    """
    # Same state is always valid
    if current_state == new_state:
        return True
    
    # If no transitions defined, allow all
    if not allowed_transitions:
        return True
    
    # Check if transition is in allowed list
    return (current_state, new_state) in allowed_transitions


class ComponentState:
    """Represents the state of a pipeline component with metadata.
    
    This class stores the current state of a component along with its history
    and additional metadata, providing methods to update the state and 
    calculate state durations.
    """
    
    def __init__(
        self, 
        component_id: str, 
        component_type: str, 
        state: str, 
        state_id: Optional[str] = None, 
        metadata: Optional[Dict[str, Any]] = None, 
        transition_reason: Optional[str] = None
    ):
        """Initialize a new ComponentState instance.
        
        Args:
            component_id: Unique identifier of the component
            component_type: Type of component (pipeline, task, etc.)
            state: Current state of the component
            state_id: Optional unique identifier for this state record
            metadata: Optional metadata about the component state
            transition_reason: Optional reason for the state transition
        """
        self.state_id = state_id or generate_state_id(component_id, component_type)
        self.component_id = component_id
        self.component_type = component_type
        self.state = state
        self.timestamp = datetime.now()
        self.previous_state = None
        self.previous_timestamp = None
        self.metadata = metadata or {}
        self.transition_reason = transition_reason
        
        logger.debug(
            f"Created new component state: {self.component_type}:{self.component_id} -> {self.state}"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the component state to a dictionary representation.
        
        Returns:
            Dictionary representation of the component state
        """
        return {
            'state_id': self.state_id,
            'component_id': self.component_id,
            'component_type': self.component_type,
            'state': self.state,
            'timestamp': self.timestamp.isoformat(),
            'previous_state': self.previous_state,
            'previous_timestamp': self.previous_timestamp.isoformat() if self.previous_timestamp else None,
            'metadata': self.metadata,
            'transition_reason': self.transition_reason
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComponentState':
        """Create a ComponentState instance from a dictionary.
        
        Args:
            data: Dictionary containing component state data
            
        Returns:
            New ComponentState instance
        """
        state = cls(
            component_id=data['component_id'],
            component_type=data['component_type'],
            state=data['state'],
            state_id=data.get('state_id'),
            metadata=data.get('metadata', {}),
            transition_reason=data.get('transition_reason')
        )
        
        # Convert string timestamps to datetime objects
        if 'timestamp' in data:
            state.timestamp = datetime.fromisoformat(data['timestamp'])
        
        state.previous_state = data.get('previous_state')
        if data.get('previous_timestamp'):
            state.previous_timestamp = datetime.fromisoformat(data['previous_timestamp'])
            
        return state
    
    def update_state(
        self, 
        new_state: str, 
        transition_reason: Optional[str] = None, 
        metadata_updates: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update the state with a new value.
        
        Args:
            new_state: New state value
            transition_reason: Reason for the state transition
            metadata_updates: Updates to apply to the metadata
            
        Returns:
            True if state was updated, False if unchanged
        """
        if new_state == self.state:
            return False
        
        self.previous_state = self.state
        self.previous_timestamp = self.timestamp
        self.state = new_state
        self.timestamp = datetime.now()
        self.transition_reason = transition_reason
        
        if metadata_updates:
            self.metadata.update(metadata_updates)
        
        logger.debug(
            f"State transition: {self.component_type}:{self.component_id} "
            f"{self.previous_state} -> {self.state} "
            f"(Reason: {self.transition_reason})"
        )
        
        return True
    
    def get_duration(self) -> float:
        """Get the duration the component has been in the current state.
        
        Returns:
            Duration in seconds
        """
        return (datetime.now() - self.timestamp).total_seconds()
    
    def get_previous_duration(self) -> Optional[float]:
        """Get the duration the component was in the previous state.
        
        Returns:
            Duration in seconds or None if no previous state
        """
        if not self.previous_state or not self.previous_timestamp:
            return None
        
        return (self.timestamp - self.previous_timestamp).total_seconds()


class StateTransitionRule:
    """Defines rules for allowed state transitions between component states.
    
    This class stores the rules that determine which state transitions are allowed
    for different component types, with optional conditions that can further
    restrict when transitions can occur.
    """
    
    def __init__(
        self, 
        component_type: str, 
        from_state: str, 
        to_states: Optional[List[str]] = None, 
        conditions: Optional[Dict[str, Any]] = None, 
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Initialize a new StateTransitionRule instance.
        
        Args:
            component_type: Type of component this rule applies to
            from_state: Source state for this transition rule
            to_states: List of allowed destination states
            conditions: Optional conditions that determine when rule applies
            metadata: Optional metadata about the rule
        """
        self.component_type = component_type
        self.from_state = from_state
        self.to_states = to_states or []
        self.conditions = conditions or {}
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the transition rule to a dictionary representation.
        
        Returns:
            Dictionary representation of the transition rule
        """
        return {
            'component_type': self.component_type,
            'from_state': self.from_state,
            'to_states': self.to_states,
            'conditions': self.conditions,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StateTransitionRule':
        """Create a StateTransitionRule instance from a dictionary.
        
        Args:
            data: Dictionary containing rule data
            
        Returns:
            New StateTransitionRule instance
        """
        return cls(
            component_type=data['component_type'],
            from_state=data['from_state'],
            to_states=data.get('to_states', []),
            conditions=data.get('conditions', {}),
            metadata=data.get('metadata', {})
        )
    
    def is_transition_allowed(self, to_state: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Check if a transition to a new state is allowed by this rule.
        
        Args:
            to_state: Destination state to check
            context: Optional context data for evaluating conditions
            
        Returns:
            True if transition is allowed, False otherwise
        """
        # Check if destination state is in allowed list
        if to_state not in self.to_states:
            return False
        
        # If conditions exist, evaluate them against context
        if self.conditions and context:
            return self.evaluate_conditions(context)
        
        # No conditions or no context, allow the transition
        return True
    
    def evaluate_conditions(self, context: Dict[str, Any]) -> bool:
        """Evaluate transition conditions against a context.
        
        Args:
            context: Context data for evaluating conditions
            
        Returns:
            True if conditions are met, False otherwise
        """
        # If no conditions, always return True
        if not self.conditions:
            return True
        
        # Evaluate each condition
        # A more sophisticated implementation would handle different condition types
        for condition_key, condition_value in self.conditions.items():
            if condition_key in context:
                if context[condition_key] != condition_value:
                    return False
            else:
                # If required context not provided, fail
                return False
        
        # All conditions passed
        return True


class StateTracker:
    """Main class for tracking and managing component states.
    
    This class provides methods to track, update, and query component states,
    with support for validating state transitions and analyzing state history.
    It uses Firestore for current state storage and BigQuery for historical analysis.
    """
    
    def __init__(
        self, 
        firestore_client: FirestoreClient, 
        bigquery_client: BigQueryClient, 
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the StateTracker with configuration.
        
        Args:
            firestore_client: Firestore client for state storage
            bigquery_client: BigQuery client for historical analysis
            config: Optional configuration overrides
        """
        # Initialize configuration from application settings
        self._config = get_config().get("monitoring", {}).get("state_tracking", {})
        
        # Override with any provided config
        if config:
            self._config.update(config)
        
        # Store client instances
        self._firestore_client = firestore_client
        self._bigquery_client = bigquery_client
        
        # Initialize other properties
        self._transition_rules = {}  # Format: {component_type: {from_state: [rules]}}
        self._state_cache = {}  # Format: {component_type:component_id: state}
        
        # Load transition rules from configuration
        self.load_transition_rules()
        
        logger.info("StateTracker initialized successfully")
    
    def update_state(
        self, 
        component_id: str, 
        component_type: str, 
        new_state: str, 
        transition_reason: Optional[str] = None, 
        metadata: Optional[Dict[str, Any]] = None, 
        validate_transition: bool = True
    ) -> ComponentState:
        """Update the state of a component.
        
        Args:
            component_id: Unique identifier of the component
            component_type: Type of component (pipeline, task, etc.)
            new_state: New state value
            transition_reason: Optional reason for the state transition
            metadata: Optional metadata about the component state
            validate_transition: Whether to validate the state transition
            
        Returns:
            Updated component state
            
        Raises:
            ValueError: If the state transition is invalid
        """
        # Get current state of the component
        current_state = self.get_current_state(component_id, component_type)
        
        # Validate transition if required
        if validate_transition and current_state:
            context = {}
            if metadata:
                context.update(metadata)
            
            if not self.validate_transition(
                component_type, 
                current_state.state, 
                new_state, 
                context
            ):
                error_msg = (
                    f"Invalid state transition for {component_type}:{component_id} "
                    f"from '{current_state.state}' to '{new_state}'"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Create new state if doesn't exist
        if not current_state:
            current_state = ComponentState(
                component_id=component_id,
                component_type=component_type,
                state=new_state,
                metadata=metadata,
                transition_reason=transition_reason
            )
            logger.info(
                f"Created new state for {component_type}:{component_id} -> {new_state}"
            )
        else:
            # Update existing state
            updated = current_state.update_state(
                new_state,
                transition_reason,
                metadata
            )
            if not updated:
                logger.debug(
                    f"State unchanged for {component_type}:{component_id} (already {new_state})"
                )
                return current_state
            
            logger.info(
                f"Updated state for {component_type}:{component_id}: "
                f"{current_state.previous_state} -> {new_state}"
            )
        
        # Store in Firestore
        try:
            self._firestore_client.set_document(
                collection=STATE_COLLECTION_NAME,
                document_id=f"{component_type}_{component_id}",
                data=current_state.to_dict()
            )
        except Exception as e:
            logger.error(
                f"Failed to store state in Firestore: {e}", 
                exc_info=True
            )
        
        # Add to history in BigQuery
        self.add_state_to_history(current_state)
        
        # Update cache
        cache_key = f"{component_type}:{component_id}"
        self._state_cache[cache_key] = current_state
        
        return current_state
    
    def get_current_state(
        self, 
        component_id: str, 
        component_type: str, 
        use_cache: bool = True
    ) -> Optional[ComponentState]:
        """Get the current state of a component.
        
        Args:
            component_id: Unique identifier of the component
            component_type: Type of component (pipeline, task, etc.)
            use_cache: Whether to use cached state if available
            
        Returns:
            Current component state or None if not found
        """
        # Check cache if enabled
        cache_key = f"{component_type}:{component_id}"
        if use_cache and cache_key in self._state_cache:
            logger.debug(f"Using cached state for {cache_key}")
            return self._state_cache[cache_key]
        
        # Query Firestore for the state
        try:
            document_id = f"{component_type}_{component_id}"
            state_data = self._firestore_client.get_document(
                collection=STATE_COLLECTION_NAME,
                document_id=document_id
            )
            
            if state_data:
                state = ComponentState.from_dict(state_data)
                
                # Update cache
                self._state_cache[cache_key] = state
                
                return state
            
            logger.debug(f"No state found for {component_type}:{component_id}")
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to get state from Firestore: {e}", 
                exc_info=True
            )
            return None
    
    def get_state_history(
        self, 
        component_id: str, 
        component_type: str, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        limit: Optional[int] = None
    ) -> List[ComponentState]:
        """Get the state history for a component.
        
        Args:
            component_id: Unique identifier of the component
            component_type: Type of component (pipeline, task, etc.)
            start_time: Optional start time for history range
            end_time: Optional end time for history range
            limit: Optional maximum number of history records to return
            
        Returns:
            List of historical component states
        """
        query = f"""
            SELECT *
            FROM `{STATE_HISTORY_TABLE}`
            WHERE component_id = @component_id
            AND component_type = @component_type
        """
        
        query_params = [
            {"name": "component_id", "parameterType": {"type": "STRING"}, "parameterValue": {"stringValue": component_id}},
            {"name": "component_type", "parameterType": {"type": "STRING"}, "parameterValue": {"stringValue": component_type}}
        ]
        
        # Add time range filters if provided
        if start_time:
            query += " AND timestamp >= @start_time"
            query_params.append({
                "name": "start_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": start_time.isoformat()}
            })
        
        if end_time:
            query += " AND timestamp <= @end_time"
            query_params.append({
                "name": "end_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": end_time.isoformat()}
            })
        
        # Order by timestamp descending
        query += " ORDER BY timestamp DESC"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            # Execute query
            results = self._bigquery_client.execute_query(query, query_params)
            
            # Convert results to ComponentState objects
            state_history = []
            for row in results:
                # Convert row to dict
                row_dict = dict(row.items())
                
                # Convert BigQuery format to ComponentState format if needed
                if 'metadata' in row_dict and isinstance(row_dict['metadata'], str):
                    try:
                        row_dict['metadata'] = json.loads(row_dict['metadata'])
                    except json.JSONDecodeError:
                        row_dict['metadata'] = {}
                
                state = ComponentState.from_dict(row_dict)
                state_history.append(state)
            
            return state_history
            
        except Exception as e:
            logger.error(
                f"Failed to get state history from BigQuery: {e}", 
                exc_info=True
            )
            return []
    
    def get_components_by_state(
        self, 
        state: str, 
        component_type: Optional[str] = None
    ) -> List[ComponentState]:
        """Get all components currently in a specific state.
        
        Args:
            state: State to filter by
            component_type: Optional component type to filter by
            
        Returns:
            List of component states matching the criteria
        """
        # Build Firestore query
        query_params = [
            ("state", "==", state)
        ]
        
        if component_type:
            query_params.append(("component_type", "==", component_type))
        
        try:
            # Execute query
            results = self._firestore_client.query_documents(
                collection=STATE_COLLECTION_NAME,
                query_params=query_params
            )
            
            # Convert results to ComponentState objects
            components = []
            for result in results:
                components.append(ComponentState.from_dict(result))
            
            return components
            
        except Exception as e:
            logger.error(
                f"Failed to query components by state: {e}", 
                exc_info=True
            )
            return []
    
    def get_state_duration_metrics(
        self, 
        component_type: Optional[str] = None, 
        state: Optional[str] = None, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get metrics about state durations for components.
        
        Args:
            component_type: Optional component type to filter by
            state: Optional state to filter by
            start_time: Optional start time for metrics calculation
            end_time: Optional end time for metrics calculation
            
        Returns:
            Dictionary of state duration metrics
        """
        # Build base query
        query = """
            WITH state_durations AS (
                SELECT
                    component_id,
                    component_type,
                    state,
                    TIMESTAMP_DIFF(
                        COALESCE(
                            LEAD(timestamp) OVER (PARTITION BY component_id, component_type ORDER BY timestamp),
                            CURRENT_TIMESTAMP()
                        ),
                        timestamp,
                        SECOND
                    ) AS duration_seconds
                FROM `{table}`
                WHERE 1=1
        """.format(table=STATE_HISTORY_TABLE)
        
        query_params = []
        
        # Add filters
        if component_type:
            query += " AND component_type = @component_type"
            query_params.append({
                "name": "component_type", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"stringValue": component_type}
            })
        
        if state:
            query += " AND state = @state"
            query_params.append({
                "name": "state", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"stringValue": state}
            })
        
        if start_time:
            query += " AND timestamp >= @start_time"
            query_params.append({
                "name": "start_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": start_time.isoformat()}
            })
        
        if end_time:
            query += " AND timestamp <= @end_time"
            query_params.append({
                "name": "end_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": end_time.isoformat()}
            })
        
        # Close CTE and add metrics calculation
        query += """
            )
            SELECT
                component_type,
                state,
                COUNT(*) AS count,
                AVG(duration_seconds) AS avg_duration,
                MIN(duration_seconds) AS min_duration,
                MAX(duration_seconds) AS max_duration,
                APPROX_QUANTILES(duration_seconds, 100)[OFFSET(50)] AS median_duration,
                APPROX_QUANTILES(duration_seconds, 100)[OFFSET(95)] AS p95_duration,
                APPROX_QUANTILES(duration_seconds, 100)[OFFSET(99)] AS p99_duration
            FROM state_durations
            GROUP BY component_type, state
            ORDER BY component_type, state
        """
        
        try:
            # Execute query
            results = self._bigquery_client.execute_query(query, query_params)
            
            # Format results as dictionary
            metrics = {}
            for row in results:
                row_dict = dict(row.items())
                component_type = row_dict.pop('component_type')
                state = row_dict.pop('state')
                
                if component_type not in metrics:
                    metrics[component_type] = {}
                
                metrics[component_type][state] = row_dict
            
            return metrics
            
        except Exception as e:
            logger.error(
                f"Failed to get state duration metrics: {e}", 
                exc_info=True
            )
            return {}
    
    def get_state_transition_metrics(
        self, 
        component_type: Optional[str] = None, 
        from_state: Optional[str] = None, 
        to_state: Optional[str] = None, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get metrics about state transitions for components.
        
        Args:
            component_type: Optional component type to filter by
            from_state: Optional source state to filter by
            to_state: Optional destination state to filter by
            start_time: Optional start time for metrics calculation
            end_time: Optional end time for metrics calculation
            
        Returns:
            Dictionary of state transition metrics
        """
        # Build base query
        query = """
            WITH transitions AS (
                SELECT
                    component_id,
                    component_type,
                    previous_state AS from_state,
                    state AS to_state,
                    timestamp
                FROM `{table}`
                WHERE previous_state IS NOT NULL
        """.format(table=STATE_HISTORY_TABLE)
        
        query_params = []
        
        # Add filters
        if component_type:
            query += " AND component_type = @component_type"
            query_params.append({
                "name": "component_type", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"stringValue": component_type}
            })
        
        if from_state:
            query += " AND previous_state = @from_state"
            query_params.append({
                "name": "from_state", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"stringValue": from_state}
            })
        
        if to_state:
            query += " AND state = @to_state"
            query_params.append({
                "name": "to_state", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"stringValue": to_state}
            })
        
        if start_time:
            query += " AND timestamp >= @start_time"
            query_params.append({
                "name": "start_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": start_time.isoformat()}
            })
        
        if end_time:
            query += " AND timestamp <= @end_time"
            query_params.append({
                "name": "end_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"stringValue": end_time.isoformat()}
            })
        
        # Close CTE and add metrics calculation
        query += """
            )
            SELECT
                component_type,
                from_state,
                to_state,
                COUNT(*) AS transition_count,
                MIN(timestamp) AS first_seen,
                MAX(timestamp) AS last_seen
            FROM transitions
            GROUP BY component_type, from_state, to_state
            ORDER BY component_type, from_state, to_state
        """
        
        try:
            # Execute query
            results = self._bigquery_client.execute_query(query, query_params)
            
            # Format results as dictionary
            metrics = {}
            for row in results:
                row_dict = dict(row.items())
                component_type = row_dict.pop('component_type')
                from_state = row_dict.pop('from_state')
                to_state = row_dict.pop('to_state')
                
                if component_type not in metrics:
                    metrics[component_type] = {}
                
                if from_state not in metrics[component_type]:
                    metrics[component_type][from_state] = {}
                
                metrics[component_type][from_state][to_state] = row_dict
            
            return metrics
            
        except Exception as e:
            logger.error(
                f"Failed to get state transition metrics: {e}", 
                exc_info=True
            )
            return {}
    
    def register_transition_rule(self, rule: StateTransitionRule) -> None:
        """Register a new state transition rule.
        
        Args:
            rule: The transition rule to register
        """
        # Validate rule structure
        if not rule.component_type or not rule.from_state:
            logger.error("Invalid rule: component_type and from_state are required")
            return
        
        # Initialize component type dictionary if needed
        if rule.component_type not in self._transition_rules:
            self._transition_rules[rule.component_type] = {}
        
        # Initialize from_state list if needed
        if rule.from_state not in self._transition_rules[rule.component_type]:
            self._transition_rules[rule.component_type][rule.from_state] = []
        
        # Add rule to transition_rules
        self._transition_rules[rule.component_type][rule.from_state].append(rule)
        
        logger.info(
            f"Registered transition rule for {rule.component_type} from {rule.from_state} "
            f"to {rule.to_states}"
        )
    
    def load_transition_rules(self) -> None:
        """Load transition rules from configuration."""
        # Clear existing rules
        self._transition_rules = {}
        
        # Get rules from configuration
        rules_config = self._config.get("transition_rules", [])
        
        # Convert each rule definition to a StateTransitionRule
        rules_added = 0
        for rule_def in rules_config:
            try:
                rule = StateTransitionRule.from_dict(rule_def)
                self.register_transition_rule(rule)
                rules_added += 1
            except Exception as e:
                logger.error(f"Failed to load transition rule: {e}")
        
        logger.info(f"Loaded {rules_added} transition rules from configuration")
    
    def validate_transition(
        self, 
        component_type: str, 
        current_state: str, 
        new_state: str, 
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Validate if a state transition is allowed.
        
        Args:
            component_type: Type of component
            current_state: Current state of the component
            new_state: New state to transition to
            context: Optional context for evaluating conditions
            
        Returns:
            True if transition is valid, False otherwise
        """
        # Same state is always valid
        if current_state == new_state:
            return True
        
        # Get rules for this component_type and from_state
        component_rules = self._transition_rules.get(component_type, {})
        state_rules = component_rules.get(current_state, [])
        
        # If no rules found, allow the transition
        if not state_rules:
            logger.debug(
                f"No transition rules found for {component_type} from {current_state}, "
                f"allowing transition to {new_state}"
            )
            return True
        
        # Check if any rule allows the transition
        context = context or {}
        for rule in state_rules:
            if rule.is_transition_allowed(new_state, context):
                logger.debug(
                    f"Transition from {current_state} to {new_state} allowed by rule"
                )
                return True
        
        logger.warning(
            f"Transition from {current_state} to {new_state} not allowed by any rule"
        )
        return False
    
    def add_state_to_history(self, state: ComponentState) -> bool:
        """Add a state transition to the history in BigQuery.
        
        Args:
            state: The component state to add to history
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert ComponentState to BigQuery row format
            row = state.to_dict()
            
            # Convert metadata to JSON string if it's a dict
            if 'metadata' in row and isinstance(row['metadata'], dict):
                row['metadata'] = json.dumps(row['metadata'])
            
            # Insert row into BigQuery
            self._bigquery_client.insert_rows(
                table=STATE_HISTORY_TABLE,
                rows=[row]
            )
            
            logger.debug(
                f"Added state transition to history: {state.component_type}:{state.component_id} "
                f"{state.previous_state} -> {state.state}"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to add state to history: {e}", 
                exc_info=True
            )
            return False
    
    def clear_old_history(self, days_to_retain: Optional[int] = None) -> int:
        """Clear old state history records based on retention policy.
        
        Args:
            days_to_retain: Number of days to retain history (default from config)
            
        Returns:
            Number of records deleted
        """
        # Use provided value or get from config, or use default
        retention_days = days_to_retain or self._config.get(
            "history_retention_days", 
            DEFAULT_STATE_RETENTION_DAYS
        )
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - datetime.timedelta(days=retention_days)
        
        # Build deletion query
        query = f"""
            DELETE FROM `{STATE_HISTORY_TABLE}`
            WHERE timestamp < @cutoff_date
        """
        
        query_params = [{
            "name": "cutoff_date", 
            "parameterType": {"type": "TIMESTAMP"}, 
            "parameterValue": {"stringValue": cutoff_date.isoformat()}
        }]
        
        try:
            # Execute query
            results = self._bigquery_client.execute_query(query, query_params)
            
            # Get number of deleted rows
            deleted_rows = results.num_dml_affected_rows
            
            logger.info(
                f"Cleared {deleted_rows} state history records older than {retention_days} days"
            )
            
            return deleted_rows
            
        except Exception as e:
            logger.error(
                f"Failed to clear old history: {e}", 
                exc_info=True
            )
            return 0
    
    def clear_cache(
        self, 
        component_id: Optional[str] = None, 
        component_type: Optional[str] = None
    ) -> None:
        """Clear the state cache.
        
        Args:
            component_id: Optional component ID to clear specific cache
            component_type: Optional component type to clear specific cache
        """
        if component_id and component_type:
            # Clear specific cache entry
            cache_key = f"{component_type}:{component_id}"
            if cache_key in self._state_cache:
                del self._state_cache[cache_key]
                logger.debug(f"Cleared cache for {cache_key}")
        else:
            # Clear entire cache
            self._state_cache = {}
            logger.debug("Cleared entire state cache")