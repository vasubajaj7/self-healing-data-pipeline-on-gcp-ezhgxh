"""
Rule Engine for Alert Generation and Processing

This module implements the rule engine for the monitoring and alerting system
in the self-healing data pipeline. It defines, manages, and evaluates alert rules
against metrics and events to determine when alerts should be triggered.

The module supports various rule types including:
- Threshold-based rules (comparing metrics against thresholds)
- Trend-based rules (detecting patterns over time)
- Anomaly detection rules (identifying statistical outliers)
- Compound rules (combining multiple conditions with logical operators)
- Event-based rules (matching specific events)
- Pattern-matching rules (applying regex patterns to logs/events)
"""

import logging
import json
import datetime
import uuid
import re
from typing import Any, Dict, List, Optional, Union, Tuple

from constants import (
    AlertSeverity,
    RULE_TYPE_THRESHOLD,
    RULE_TYPE_TREND,
    RULE_TYPE_ANOMALY,
    RULE_TYPE_COMPOUND,
    RULE_TYPE_EVENT,
    RULE_TYPE_PATTERN,
    OPERATOR_EQUAL,
    OPERATOR_NOT_EQUAL,
    OPERATOR_GREATER_THAN,
    OPERATOR_GREATER_EQUAL,
    OPERATOR_LESS_THAN,
    OPERATOR_LESS_EQUAL,
    LOGICAL_AND,
    LOGICAL_OR
)
from config import get_config
from logging_config import get_logger
from anomaly_detector import AnomalyDetector

# Configure module logger
logger = get_logger(__name__)


def evaluate_condition(condition: Dict, value: Any) -> bool:
    """
    Evaluates a single condition against a value.
    
    Args:
        condition: Dictionary containing operator and expected value
        value: The actual value to evaluate against the condition
        
    Returns:
        True if condition is met, False otherwise
    """
    if value is None:
        return False
    
    operator = condition.get('operator')
    expected = condition.get('value')
    
    # Handle different operators
    if operator == OPERATOR_EQUAL:
        return value == expected
    elif operator == OPERATOR_NOT_EQUAL:
        return value != expected
    elif operator == OPERATOR_GREATER_THAN:
        # Ensure we can compare the values
        try:
            return float(value) > float(expected)
        except (ValueError, TypeError):
            logger.warning(f"Cannot compare {value} > {expected} - non-numeric values")
            return False
    elif operator == OPERATOR_GREATER_EQUAL:
        try:
            return float(value) >= float(expected)
        except (ValueError, TypeError):
            logger.warning(f"Cannot compare {value} >= {expected} - non-numeric values")
            return False
    elif operator == OPERATOR_LESS_THAN:
        try:
            return float(value) < float(expected)
        except (ValueError, TypeError):
            logger.warning(f"Cannot compare {value} < {expected} - non-numeric values")
            return False
    elif operator == OPERATOR_LESS_EQUAL:
        try:
            return float(value) <= float(expected)
        except (ValueError, TypeError):
            logger.warning(f"Cannot compare {value} <= {expected} - non-numeric values")
            return False
    else:
        logger.warning(f"Unknown operator: {operator}")
        return False


def evaluate_compound_expression(expression: Dict, data: Dict) -> bool:
    """
    Evaluates a compound logical expression with AND/OR operators.
    
    Args:
        expression: Dictionary containing logical operator and conditions
        data: Data dictionary to evaluate against
        
    Returns:
        Result of the compound expression evaluation
    """
    logical_op = expression.get('operator', LOGICAL_AND)
    conditions = expression.get('conditions', [])
    
    if not conditions:
        return False
    
    # Initialize the result based on the logical operator
    if logical_op == LOGICAL_AND:
        result = True
    else:  # LOGICAL_OR
        result = False
    
    for condition in conditions:
        # Check if this is a nested compound condition
        if 'operator' in condition and 'conditions' in condition:
            condition_result = evaluate_compound_expression(condition, data)
        else:
            # Get the metric_path and extract value from data
            metric_path = condition.get('metric_path')
            value = get_metric_value(data, metric_path)
            condition_result = evaluate_condition(condition, value)
        
        # Apply the logical operator
        if logical_op == LOGICAL_AND:
            result = result and condition_result
            if not result:  # Short-circuit for AND
                return False
        else:  # LOGICAL_OR
            result = result or condition_result
            if result:  # Short-circuit for OR
                return True
    
    return result


def get_metric_value(metrics: Dict, metric_path: str) -> Any:
    """
    Extracts a metric value from metrics data using a path expression.
    
    Args:
        metrics: Dictionary containing metrics data
        metric_path: Dot-notation path to the metric (e.g., "cpu.utilization")
        
    Returns:
        Extracted metric value or None if not found
    """
    if not metric_path or not metrics:
        return None
    
    try:
        # Split path by dots and traverse the metrics dictionary
        parts = metric_path.split('.')
        current = metrics
        
        for part in parts:
            if part in current:
                current = current[part]
            else:
                return None
        
        return current
    except (KeyError, TypeError):
        # Return None if path doesn't exist or metrics is not a dict
        return None


class Rule:
    """
    Represents an alert rule with conditions and actions.
    
    A rule defines when an alert should be triggered based on metrics,
    events, or other data, and what actions should be taken in response.
    """
    
    def __init__(self, 
                 rule_id: str = None,
                 name: str = "",
                 description: str = "",
                 rule_type: str = RULE_TYPE_THRESHOLD,
                 conditions: Dict = None,
                 severity: AlertSeverity = AlertSeverity.MEDIUM,
                 actions: Dict = None,
                 enabled: bool = True,
                 metadata: Dict = None):
        """
        Initializes a new Rule instance.
        
        Args:
            rule_id: Unique identifier for the rule (will be generated if None)
            name: Human-readable name of the rule
            description: Detailed description of the rule purpose
            rule_type: Type of rule (threshold, trend, anomaly, compound, event, pattern)
            conditions: Dictionary containing rule-specific conditions
            severity: Severity level of alerts generated by this rule
            actions: Dictionary of actions to take when rule is triggered
            enabled: Whether the rule is currently active
            metadata: Additional metadata for the rule (e.g., owner, category)
        """
        self.id = rule_id or str(uuid.uuid4())
        self.name = name
        self.description = description
        self.rule_type = rule_type
        self.conditions = conditions or {}
        self.severity = severity
        self.actions = actions or {}
        self.enabled = enabled
        self.metadata = metadata or {}
        
        # Validate the rule upon creation
        if not self.validate():
            logger.warning(f"Created rule {self.id} with invalid configuration")
    
    def validate(self) -> bool:
        """
        Validates the rule structure and parameters.
        
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not self.name or not self.rule_type:
            logger.error(f"Rule {self.id} missing required fields: name or type")
            return False
        
        # Validate rule type
        valid_rule_types = [
            RULE_TYPE_THRESHOLD,
            RULE_TYPE_TREND,
            RULE_TYPE_ANOMALY,
            RULE_TYPE_COMPOUND,
            RULE_TYPE_EVENT,
            RULE_TYPE_PATTERN
        ]
        if self.rule_type not in valid_rule_types:
            logger.error(f"Rule {self.id} has invalid type: {self.rule_type}")
            return False
        
        # Validate conditions based on rule type
        if not self.conditions:
            logger.error(f"Rule {self.id} has no conditions")
            return False
        
        # Specific validation based on rule type
        if self.rule_type == RULE_TYPE_THRESHOLD:
            if 'metric_path' not in self.conditions or 'operator' not in self.conditions or 'value' not in self.conditions:
                logger.error(f"Threshold rule {self.id} missing required condition fields")
                return False
        elif self.rule_type == RULE_TYPE_TREND:
            if 'metric_path' not in self.conditions or 'window' not in self.conditions or 'trend_type' not in self.conditions:
                logger.error(f"Trend rule {self.id} missing required condition fields")
                return False
        elif self.rule_type == RULE_TYPE_ANOMALY:
            if 'metric_path' not in self.conditions or 'sensitivity' not in self.conditions:
                logger.error(f"Anomaly rule {self.id} missing required condition fields")
                return False
        elif self.rule_type == RULE_TYPE_COMPOUND:
            if 'operator' not in self.conditions or 'conditions' not in self.conditions:
                logger.error(f"Compound rule {self.id} missing required condition fields")
                return False
        elif self.rule_type == RULE_TYPE_EVENT:
            if 'event_type' not in self.conditions:
                logger.error(f"Event rule {self.id} missing required condition fields")
                return False
        elif self.rule_type == RULE_TYPE_PATTERN:
            if 'pattern' not in self.conditions or 'field' not in self.conditions:
                logger.error(f"Pattern rule {self.id} missing required condition fields")
                return False
        
        # Validate severity
        if not isinstance(self.severity, AlertSeverity):
            logger.error(f"Rule {self.id} has invalid severity: {self.severity}")
            return False
        
        # Validate actions (at minimum, should have notification info)
        if not self.actions:
            logger.warning(f"Rule {self.id} has no actions defined")
        
        return True
    
    def evaluate(self, data: Dict, context: Dict = None) -> 'RuleEvaluationResult':
        """
        Evaluates the rule against provided data.
        
        Args:
            data: Data to evaluate the rule against (metrics, events, etc.)
            context: Additional context for evaluation
            
        Returns:
            Result of the rule evaluation
        """
        if not self.enabled:
            logger.debug(f"Rule {self.id} is disabled, skipping evaluation")
            return RuleEvaluationResult(
                self.id, self.name, self.rule_type, 
                False, self.severity, 
                {'status': 'skipped', 'reason': 'rule_disabled'},
                context or {}
            )
        
        context = context or {}
        triggered = False
        details = {}
        
        try:
            # Select the appropriate evaluation method based on rule type
            if self.rule_type == RULE_TYPE_THRESHOLD:
                triggered = self.evaluate_threshold_rule(data, context)
                details['evaluation_type'] = 'threshold'
            elif self.rule_type == RULE_TYPE_TREND:
                triggered = self.evaluate_trend_rule(data, context)
                details['evaluation_type'] = 'trend'
            elif self.rule_type == RULE_TYPE_ANOMALY:
                triggered = self.evaluate_anomaly_rule(data, context)
                details['evaluation_type'] = 'anomaly'
            elif self.rule_type == RULE_TYPE_COMPOUND:
                triggered = self.evaluate_compound_rule(data, context)
                details['evaluation_type'] = 'compound'
            elif self.rule_type == RULE_TYPE_EVENT:
                triggered = self.evaluate_event_rule(data, context)
                details['evaluation_type'] = 'event'
            elif self.rule_type == RULE_TYPE_PATTERN:
                triggered = self.evaluate_pattern_rule(data, context)
                details['evaluation_type'] = 'pattern'
            else:
                logger.error(f"Unknown rule type: {self.rule_type}")
                details['error'] = f"Unknown rule type: {self.rule_type}"
                
            details['rule_conditions'] = self.conditions
            
            if triggered:
                logger.info(f"Rule {self.id} ({self.name}) triggered")
                details['status'] = 'triggered'
            else:
                logger.debug(f"Rule {self.id} ({self.name}) not triggered")
                details['status'] = 'not_triggered'
                
        except Exception as e:
            logger.exception(f"Error evaluating rule {self.id}: {e}")
            triggered = False
            details = {
                'status': 'error',
                'error': str(e),
                'rule_conditions': self.conditions
            }
        
        # Create and return the evaluation result
        return RuleEvaluationResult(
            self.id, self.name, self.rule_type, 
            triggered, self.severity, details, context
        )
    
    def evaluate_threshold_rule(self, metrics: Dict, context: Dict) -> bool:
        """
        Evaluates a threshold-based rule against metrics.
        
        Args:
            metrics: Metrics data to evaluate
            context: Additional context for evaluation
            
        Returns:
            True if threshold is breached, False otherwise
        """
        metric_path = self.conditions.get('metric_path')
        operator = self.conditions.get('operator')
        threshold = self.conditions.get('value')
        
        # Get the actual metric value
        actual_value = get_metric_value(metrics, metric_path)
        
        # If metric not found, rule is not triggered
        if actual_value is None:
            logger.debug(f"Metric {metric_path} not found in data")
            return False
        
        # Create a condition dictionary for the evaluate_condition function
        condition = {'operator': operator, 'value': threshold}
        return evaluate_condition(condition, actual_value)
    
    def evaluate_trend_rule(self, metrics: Dict, context: Dict) -> bool:
        """
        Evaluates a trend-based rule against time series metrics.
        
        Args:
            metrics: Time series metrics data to evaluate
            context: Additional context for evaluation
            
        Returns:
            True if trend condition is met, False otherwise
        """
        metric_path = self.conditions.get('metric_path')
        window = self.conditions.get('window')
        trend_type = self.conditions.get('trend_type')
        threshold = self.conditions.get('threshold', 0)
        
        # Get time series data
        time_series = get_metric_value(metrics, metric_path)
        
        # If no time series data found, rule is not triggered
        if not time_series or not isinstance(time_series, list):
            logger.debug(f"Time series data for {metric_path} not found or invalid")
            return False
        
        # Ensure we have enough data points for the window
        if len(time_series) < 2:
            logger.debug(f"Insufficient data points for trend analysis: {len(time_series)}")
            return False
        
        # Apply window if specified
        if window and window < len(time_series):
            time_series = time_series[-window:]
        
        # Calculate trend based on trend_type
        if trend_type == 'slope':
            # Simple linear regression
            x = list(range(len(time_series)))
            y = [float(val) for val in time_series]
            
            # Calculate slope using least squares method
            n = len(x)
            x_mean = sum(x) / n
            y_mean = sum(y) / n
            
            numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
            denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
            
            if denominator == 0:
                slope = 0
            else:
                slope = numerator / denominator
            
            trend_value = slope
            
        elif trend_type == 'percent_change':
            # Percentage change from start to end
            start_val = float(time_series[0])
            end_val = float(time_series[-1])
            
            if start_val == 0:
                # Avoid division by zero
                trend_value = 100.0 if end_val > 0 else (-100.0 if end_val < 0 else 0.0)
            else:
                trend_value = ((end_val - start_val) / abs(start_val)) * 100.0
                
        elif trend_type == 'absolute_change':
            # Absolute change from start to end
            start_val = float(time_series[0])
            end_val = float(time_series[-1])
            trend_value = end_val - start_val
            
        else:
            logger.warning(f"Unknown trend type: {trend_type}")
            return False
        
        # Check if the trend meets the condition
        direction = self.conditions.get('direction', 'increasing')
        
        if direction == 'increasing':
            return trend_value > threshold
        elif direction == 'decreasing':
            return trend_value < -abs(threshold)
        else:
            # If direction is 'any' or invalid, check absolute value
            return abs(trend_value) > abs(threshold)
    
    def evaluate_anomaly_rule(self, metrics: Dict, context: Dict) -> bool:
        """
        Evaluates an anomaly detection rule against metrics.
        
        Args:
            metrics: Metrics data to evaluate
            context: Additional context for evaluation
            
        Returns:
            True if anomaly is detected, False otherwise
        """
        metric_path = self.conditions.get('metric_path')
        sensitivity = self.conditions.get('sensitivity', 2.0)  # Default to 2 std deviations
        min_data_points = self.conditions.get('min_data_points', 5)
        algorithm = self.conditions.get('algorithm', 'z_score')
        
        # Get the metric value (could be a single value or time series)
        metric_value = get_metric_value(metrics, metric_path)
        
        # If metric not found, rule is not triggered
        if metric_value is None:
            logger.debug(f"Metric {metric_path} not found in data")
            return False
        
        # Get or create an anomaly detector
        anomaly_detector = context.get('anomaly_detector')
        if not anomaly_detector:
            # Create a new detector if one wasn't provided
            from anomaly_detector import AnomalyDetector
            anomaly_detector = AnomalyDetector()
        
        # Detect anomaly based on algorithm
        is_anomaly = False
        
        try:
            if isinstance(metric_value, list):
                # Handle time series data
                if len(metric_value) < min_data_points:
                    logger.debug(f"Insufficient data points for anomaly detection: {len(metric_value)}")
                    return False
                
                is_anomaly = anomaly_detector.detect_anomaly(
                    metric_value, 
                    algorithm=algorithm,
                    sensitivity=sensitivity,
                    metric_name=metric_path
                )
            else:
                # Handle single value with historical context
                historical_data = context.get('historical_data', {})
                historical_values = historical_data.get(metric_path, [])
                
                if len(historical_values) < min_data_points - 1:
                    logger.debug(f"Insufficient historical data for anomaly detection")
                    return False
                
                # Combine historical values with current value
                all_values = historical_values + [metric_value]
                
                is_anomaly = anomaly_detector.detect_anomaly(
                    all_values,
                    algorithm=algorithm,
                    sensitivity=sensitivity,
                    metric_name=metric_path
                )
                
            return is_anomaly
            
        except Exception as e:
            logger.error(f"Error in anomaly detection for {metric_path}: {e}")
            return False
    
    def evaluate_compound_rule(self, data: Dict, context: Dict) -> bool:
        """
        Evaluates a compound rule with multiple conditions.
        
        Args:
            data: Data to evaluate against
            context: Additional context for evaluation
            
        Returns:
            True if compound condition is met, False otherwise
        """
        # Extract compound expression
        expression = self.conditions
        
        # Evaluate the compound expression
        return evaluate_compound_expression(expression, data)
    
    def evaluate_event_rule(self, event: Dict, context: Dict) -> bool:
        """
        Evaluates an event-based rule against event data.
        
        Args:
            event: Event data to evaluate
            context: Additional context for evaluation
            
        Returns:
            True if event matches conditions, False otherwise
        """
        event_type = self.conditions.get('event_type')
        event_source = self.conditions.get('event_source')
        properties = self.conditions.get('properties', {})
        
        # Check if event has the specified type
        if event.get('type') != event_type:
            return False
        
        # If event source is specified, check that too
        if event_source and event.get('source') != event_source:
            return False
        
        # Check event properties if specified
        for prop_name, condition in properties.items():
            if prop_name not in event:
                return False
            
            prop_value = event[prop_name]
            if not evaluate_condition(condition, prop_value):
                return False
        
        # All conditions met
        return True
    
    def evaluate_pattern_rule(self, data: Dict, context: Dict) -> bool:
        """
        Evaluates a pattern-matching rule against log or event data.
        
        Args:
            data: Data to match patterns against
            context: Additional context for evaluation
            
        Returns:
            True if pattern is matched, False otherwise
        """
        pattern = self.conditions.get('pattern')
        field = self.conditions.get('field')
        match_type = self.conditions.get('match_type', 'regex')
        
        # Get the value to match against
        value = get_metric_value(data, field)
        
        # If value not found, rule is not triggered
        if value is None:
            logger.debug(f"Field {field} not found in data")
            return False
        
        # Convert value to string for pattern matching
        if not isinstance(value, str):
            value = str(value)
        
        # Match based on match_type
        if match_type == 'regex':
            try:
                return bool(re.search(pattern, value))
            except re.error as e:
                logger.error(f"Invalid regex pattern '{pattern}': {e}")
                return False
        elif match_type == 'contains':
            return pattern in value
        elif match_type == 'starts_with':
            return value.startswith(pattern)
        elif match_type == 'ends_with':
            return value.endswith(pattern)
        else:
            logger.warning(f"Unknown match type: {match_type}")
            return False
    
    def to_dict(self) -> Dict:
        """
        Converts the rule to a dictionary representation.
        
        Returns:
            Dictionary representation of the rule
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'rule_type': self.rule_type,
            'conditions': self.conditions,
            'severity': self.severity.value,
            'actions': self.actions,
            'enabled': self.enabled,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, rule_dict: Dict) -> 'Rule':
        """
        Creates a Rule instance from a dictionary.
        
        Args:
            rule_dict: Dictionary containing rule properties
            
        Returns:
            Rule instance
        """
        # Convert severity string to enum
        severity_str = rule_dict.get('severity', AlertSeverity.MEDIUM.value)
        
        try:
            severity = AlertSeverity(severity_str)
        except ValueError:
            logger.warning(f"Invalid severity value: {severity_str}, using MEDIUM")
            severity = AlertSeverity.MEDIUM
            
        # Create and return new Rule instance
        return cls(
            rule_id=rule_dict.get('id'),
            name=rule_dict.get('name', ''),
            description=rule_dict.get('description', ''),
            rule_type=rule_dict.get('rule_type', RULE_TYPE_THRESHOLD),
            conditions=rule_dict.get('conditions', {}),
            severity=severity,
            actions=rule_dict.get('actions', {}),
            enabled=rule_dict.get('enabled', True),
            metadata=rule_dict.get('metadata', {})
        )


class RuleEvaluationResult:
    """
    Represents the result of a rule evaluation.
    
    This class captures the outcome of evaluating a rule against data,
    including whether the rule was triggered, its severity, and details
    about the evaluation.
    """
    
    def __init__(self, 
                 rule_id: str,
                 rule_name: str,
                 rule_type: str,
                 triggered: bool,
                 severity: AlertSeverity,
                 details: Dict = None,
                 context: Dict = None):
        """
        Initializes a new RuleEvaluationResult instance.
        
        Args:
            rule_id: ID of the evaluated rule
            rule_name: Name of the evaluated rule
            rule_type: Type of the evaluated rule
            triggered: Whether the rule was triggered
            severity: Severity level of the rule
            details: Additional details about the evaluation
            context: Context information for the evaluation
        """
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.rule_type = rule_type
        self.triggered = triggered
        self.severity = severity
        self.details = details or {}
        self.context = context or {}
        self.evaluation_time = datetime.datetime.now()
    
    def to_dict(self) -> Dict:
        """
        Converts the evaluation result to a dictionary representation.
        
        Returns:
            Dictionary representation of the evaluation result
        """
        return {
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'rule_type': self.rule_type,
            'triggered': self.triggered,
            'severity': self.severity.value,
            'details': self.details,
            'context': self.context,
            'evaluation_time': self.evaluation_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, result_dict: Dict) -> 'RuleEvaluationResult':
        """
        Creates a RuleEvaluationResult instance from a dictionary.
        
        Args:
            result_dict: Dictionary containing evaluation result properties
            
        Returns:
            RuleEvaluationResult instance
        """
        # Convert severity string to enum
        severity_str = result_dict.get('severity', AlertSeverity.MEDIUM.value)
        
        try:
            severity = AlertSeverity(severity_str)
        except ValueError:
            severity = AlertSeverity.MEDIUM
        
        # Create instance
        result = cls(
            rule_id=result_dict.get('rule_id', ''),
            rule_name=result_dict.get('rule_name', ''),
            rule_type=result_dict.get('rule_type', ''),
            triggered=result_dict.get('triggered', False),
            severity=severity,
            details=result_dict.get('details', {}),
            context=result_dict.get('context', {})
        )
        
        # Set evaluation time if present
        eval_time_str = result_dict.get('evaluation_time')
        if eval_time_str:
            try:
                result.evaluation_time = datetime.datetime.fromisoformat(eval_time_str)
            except (ValueError, TypeError):
                # Keep the default evaluation time
                pass
        
        return result


class RuleEngine:
    """
    Engine for managing and evaluating alert rules against metrics and events.
    
    This class provides functionality to create, manage, and evaluate rules
    that determine when alerts should be triggered based on metrics, events,
    and other data.
    """
    
    def __init__(self, anomaly_detector: AnomalyDetector = None):
        """
        Initializes the RuleEngine with configuration settings.
        
        Args:
            anomaly_detector: Optional anomaly detector instance to use for evaluations
        """
        self._rules = {}  # Dictionary of rules by ID
        self._rule_groups = {}  # Groups of rules
        self._config = get_config()  # Application configuration
        
        # Initialize anomaly detector
        self._anomaly_detector = anomaly_detector or AnomalyDetector()
        
        # Load predefined rules from configuration
        self.load_rules()
        logger.info(f"Rule Engine initialized with {len(self._rules)} rules")
    
    def load_rules(self) -> None:
        """
        Loads rules from configuration or storage.
        """
        try:
            # Get rules configuration
            rules_config = self._config.get('alerting.rules', {})
            
            if not rules_config:
                logger.info("No predefined rules found in configuration")
                return
            
            # Process each rule
            for rule_dict in rules_config:
                try:
                    rule = Rule.from_dict(rule_dict)
                    self.add_rule(rule)
                    
                    # Add to group if specified
                    group = rule.metadata.get('group')
                    if group:
                        if group not in self._rule_groups:
                            self.create_rule_group(group, f"Group for {group} rules")
                        self.add_rule_to_group(rule.id, group)
                        
                except Exception as e:
                    logger.error(f"Error loading rule from configuration: {e}")
            
            logger.info(f"Loaded {len(self._rules)} rules from configuration")
            
        except Exception as e:
            logger.error(f"Error loading rules: {e}")
    
    def add_rule(self, rule: Rule) -> str:
        """
        Adds a new rule to the engine.
        
        Args:
            rule: Rule instance to add
            
        Returns:
            ID of the added rule
        """
        # Validate rule before adding
        if not rule.validate():
            logger.error(f"Cannot add invalid rule: {rule.id}")
            raise ValueError(f"Invalid rule configuration: {rule.id}")
        
        # Add rule to rules dictionary
        self._rules[rule.id] = rule
        
        # Add rule to group if specified in metadata
        group = rule.metadata.get('group')
        if group:
            if group not in self._rule_groups:
                self.create_rule_group(group, f"Group for {group} rules")
            self.add_rule_to_group(rule.id, group)
            
        logger.info(f"Added rule {rule.id}: {rule.name}")
        return rule.id
    
    def add_rule_from_dict(self, rule_dict: Dict) -> str:
        """
        Creates and adds a rule from a dictionary definition.
        
        Args:
            rule_dict: Dictionary containing rule definition
            
        Returns:
            ID of the added rule
        """
        rule = Rule.from_dict(rule_dict)
        return self.add_rule(rule)
    
    def get_rule(self, rule_id: str) -> Optional[Rule]:
        """
        Retrieves a rule by its ID.
        
        Args:
            rule_id: ID of the rule to retrieve
            
        Returns:
            Rule instance if found, None otherwise
        """
        return self._rules.get(rule_id)
    
    def update_rule(self, rule: Rule) -> bool:
        """
        Updates an existing rule.
        
        Args:
            rule: Rule instance with updated properties
            
        Returns:
            True if update was successful
        """
        # Validate rule before updating
        if not rule.validate():
            logger.error(f"Cannot update with invalid rule: {rule.id}")
            return False
        
        # Check if rule exists
        if rule.id not in self._rules:
            logger.error(f"Cannot update non-existent rule: {rule.id}")
            return False
        
        # Get existing rule for group management
        existing_rule = self._rules[rule.id]
        existing_group = existing_rule.metadata.get('group')
        new_group = rule.metadata.get('group')
        
        # Update rule
        self._rules[rule.id] = rule
        
        # Handle group changes if needed
        if existing_group != new_group:
            # Remove from old group if any
            if existing_group and existing_group in self._rule_groups:
                group_rules = self._rule_groups[existing_group].get('rules', [])
                if rule.id in group_rules:
                    group_rules.remove(rule.id)
            
            # Add to new group if any
            if new_group:
                if new_group not in self._rule_groups:
                    self.create_rule_group(new_group, f"Group for {new_group} rules")
                self.add_rule_to_group(rule.id, new_group)
        
        logger.info(f"Updated rule {rule.id}: {rule.name}")
        return True
    
    def delete_rule(self, rule_id: str) -> bool:
        """
        Deletes a rule by its ID.
        
        Args:
            rule_id: ID of the rule to delete
            
        Returns:
            True if deletion was successful
        """
        # Check if rule exists
        if rule_id not in self._rules:
            logger.warning(f"Cannot delete non-existent rule: {rule_id}")
            return False
        
        # Remove rule from groups
        for group_name, group_info in self._rule_groups.items():
            group_rules = group_info.get('rules', [])
            if rule_id in group_rules:
                group_rules.remove(rule_id)
        
        # Remove rule from rules dictionary
        rule = self._rules.pop(rule_id)
        
        logger.info(f"Deleted rule {rule_id}: {rule.name}")
        return True
    
    def get_rules(self, group_name: str = None) -> List[Rule]:
        """
        Retrieves all rules or rules in a specific group.
        
        Args:
            group_name: Optional group name to filter rules
            
        Returns:
            List of Rule instances
        """
        if group_name:
            # Get rules in the specified group
            if group_name not in self._rule_groups:
                logger.warning(f"Group not found: {group_name}")
                return []
            
            group_rule_ids = self._rule_groups[group_name].get('rules', [])
            return [self._rules[rule_id] for rule_id in group_rule_ids 
                   if rule_id in self._rules]
        else:
            # Return all rules
            return list(self._rules.values())
    
    def create_rule_group(self, group_name: str, description: str = "") -> bool:
        """
        Creates a new rule group.
        
        Args:
            group_name: Name of the group to create
            description: Description of the group
            
        Returns:
            True if group was created successfully
        """
        if group_name in self._rule_groups:
            logger.warning(f"Group already exists: {group_name}")
            return False
        
        self._rule_groups[group_name] = {
            'name': group_name,
            'description': description,
            'rules': []
        }
        
        logger.info(f"Created rule group: {group_name}")
        return True
    
    def add_rule_to_group(self, rule_id: str, group_name: str) -> bool:
        """
        Adds a rule to a rule group.
        
        Args:
            rule_id: ID of the rule to add
            group_name: Name of the group to add the rule to
            
        Returns:
            True if rule was added to group successfully
        """
        # Check if rule exists
        if rule_id not in self._rules:
            logger.warning(f"Cannot add non-existent rule to group: {rule_id}")
            return False
        
        # Check if group exists
        if group_name not in self._rule_groups:
            logger.warning(f"Cannot add rule to non-existent group: {group_name}")
            return False
        
        # Add rule to group if not already there
        group_rules = self._rule_groups[group_name].get('rules', [])
        if rule_id not in group_rules:
            group_rules.append(rule_id)
            self._rule_groups[group_name]['rules'] = group_rules
            
            # Update rule metadata
            self._rules[rule_id].metadata['group'] = group_name
            
            logger.info(f"Added rule {rule_id} to group {group_name}")
        
        return True
    
    def evaluate_rule(self, rule_id: str, data: Dict, context: Dict = None) -> Optional[RuleEvaluationResult]:
        """
        Evaluates a specific rule against provided data.
        
        Args:
            rule_id: ID of the rule to evaluate
            data: Data to evaluate against (metrics, events, etc.)
            context: Additional context for evaluation
            
        Returns:
            Result of the rule evaluation or None if rule not found
        """
        # Get rule by ID
        rule = self._rules.get(rule_id)
        if not rule:
            logger.warning(f"Cannot evaluate non-existent rule: {rule_id}")
            return None
        
        # Ensure anomaly detector is in context
        context = context or {}
        if 'anomaly_detector' not in context:
            context['anomaly_detector'] = self._anomaly_detector
        
        # Evaluate the rule
        return rule.evaluate(data, context)
    
    def evaluate_rules(self, rule_ids: List[str], data: Dict, context: Dict = None) -> List[RuleEvaluationResult]:
        """
        Evaluates multiple rules against provided data.
        
        Args:
            rule_ids: List of rule IDs to evaluate
            data: Data to evaluate against (metrics, events, etc.)
            context: Additional context for evaluation
            
        Returns:
            List of rule evaluation results
        """
        results = []
        
        # Ensure anomaly detector is in context
        context = context or {}
        if 'anomaly_detector' not in context:
            context['anomaly_detector'] = self._anomaly_detector
        
        # Evaluate each rule
        for rule_id in rule_ids:
            result = self.evaluate_rule(rule_id, data, context)
            if result:
                results.append(result)
        
        return results
    
    def evaluate_group(self, group_name: str, data: Dict, context: Dict = None) -> List[RuleEvaluationResult]:
        """
        Evaluates all rules in a group against provided data.
        
        Args:
            group_name: Name of the group to evaluate
            data: Data to evaluate against (metrics, events, etc.)
            context: Additional context for evaluation
            
        Returns:
            List of rule evaluation results for the group
        """
        # Check if group exists
        if group_name not in self._rule_groups:
            logger.warning(f"Cannot evaluate non-existent group: {group_name}")
            return []
        
        # Get rules in the group
        group_rule_ids = self._rule_groups[group_name].get('rules', [])
        
        # Evaluate rules
        return self.evaluate_rules(group_rule_ids, data, context)
    
    def evaluate_all_rules(self, data: Dict, context: Dict = None) -> List[RuleEvaluationResult]:
        """
        Evaluates all rules against provided data.
        
        Args:
            data: Data to evaluate against (metrics, events, etc.)
            context: Additional context for evaluation
            
        Returns:
            List of all rule evaluation results
        """
        # Get all rule IDs
        rule_ids = list(self._rules.keys())
        
        # Evaluate all rules
        return self.evaluate_rules(rule_ids, data, context)
    
    def evaluate_metrics(self, metrics: Dict, context: Dict = None) -> List[RuleEvaluationResult]:
        """
        Evaluates metrics-based rules against metrics data.
        
        Args:
            metrics: Metrics data to evaluate
            context: Additional context for evaluation
            
        Returns:
            List of rule evaluation results for metrics rules
        """
        # Filter rules for metrics-related types
        metric_rule_types = [RULE_TYPE_THRESHOLD, RULE_TYPE_TREND, RULE_TYPE_ANOMALY, RULE_TYPE_COMPOUND]
        rule_ids = [rule.id for rule in self._rules.values() 
                   if rule.rule_type in metric_rule_types]
        
        # Evaluate filtered rules
        return self.evaluate_rules(rule_ids, metrics, context)
    
    def evaluate_events(self, events: Dict, context: Dict = None) -> List[RuleEvaluationResult]:
        """
        Evaluates event-based rules against event data.
        
        Args:
            events: Event data to evaluate
            context: Additional context for evaluation
            
        Returns:
            List of rule evaluation results for event rules
        """
        # Filter rules for event-related types
        event_rule_types = [RULE_TYPE_EVENT, RULE_TYPE_PATTERN]
        rule_ids = [rule.id for rule in self._rules.values() 
                  if rule.rule_type in event_rule_types]
        
        # Evaluate filtered rules
        return self.evaluate_rules(rule_ids, events, context)
    
    def get_triggered_rules(self, evaluation_results: List[RuleEvaluationResult]) -> List[RuleEvaluationResult]:
        """
        Filters evaluation results to get only triggered rules.
        
        Args:
            evaluation_results: List of rule evaluation results
            
        Returns:
            List of evaluation results where triggered is True
        """
        return [result for result in evaluation_results if result.triggered]
    
    def export_rules(self) -> Dict:
        """
        Exports all rules to a dictionary format.
        
        Returns:
            Dictionary of rule definitions
        """
        rules_dict = {}
        for rule_id, rule in self._rules.items():
            rules_dict[rule_id] = rule.to_dict()
        return rules_dict
    
    def import_rules(self, rules_dict: Dict, replace_existing: bool = False) -> int:
        """
        Imports rules from a dictionary format.
        
        Args:
            rules_dict: Dictionary of rule definitions
            replace_existing: Whether to replace existing rules
            
        Returns:
            Number of rules imported
        """
        if replace_existing:
            # Clear existing rules
            self._rules = {}
            self._rule_groups = {}
        
        imported_count = 0
        
        # Import each rule
        for rule_id, rule_dict in rules_dict.items():
            try:
                # Ensure rule_id is set in the dictionary
                rule_dict['id'] = rule_id
                
                # Create and add rule
                rule = Rule.from_dict(rule_dict)
                self.add_rule(rule)
                
                imported_count += 1
            except Exception as e:
                logger.error(f"Error importing rule {rule_id}: {e}")
        
        logger.info(f"Imported {imported_count} rules")
        return imported_count