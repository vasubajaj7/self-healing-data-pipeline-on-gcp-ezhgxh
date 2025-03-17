"""
Database model for issue patterns in the self-healing data pipeline.

This module defines the IssuePattern class, which represents recognized patterns
of issues or failures that can be detected and resolved automatically.
The model stores pattern definitions, detection features, confidence thresholds,
and relationships to healing actions.
"""

import typing
import datetime
import uuid
import json

from ...constants import HealingActionType
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Initialize logger
logger = get_logger(__name__)

# Define table name
ISSUE_PATTERN_TABLE_NAME = "issue_patterns"

# Define default confidence threshold
DEFAULT_CONFIDENCE_THRESHOLD = 0.7

# Define pattern types
PATTERN_TYPES = ["data_quality", "pipeline", "system", "resource"]


def generate_pattern_id(prefix: str = "pattern_") -> str:
    """
    Generates a unique identifier for an issue pattern.
    
    Args:
        prefix: Prefix for the pattern ID
        
    Returns:
        Unique pattern ID with the specified prefix
    """
    return f"{prefix}{str(uuid.uuid4())}"


def create_issue_pattern(
    name: str,
    pattern_type: str,
    description: str,
    features: dict,
    confidence_threshold: float = None
) -> 'IssuePattern':
    """
    Creates a new issue pattern record in the database.
    
    Args:
        name: Name of the pattern
        pattern_type: Type of pattern (data_quality, pipeline, system, resource)
        description: Description of the pattern
        features: Dictionary of pattern detection features
        confidence_threshold: Minimum confidence threshold for matching
        
    Returns:
        Newly created issue pattern instance
    """
    # Generate pattern ID
    pattern_id = generate_pattern_id()
    
    # Validate pattern type
    if pattern_type not in PATTERN_TYPES:
        logger.error(f"Invalid pattern type: {pattern_type}. Must be one of {PATTERN_TYPES}")
        raise ValueError(f"Invalid pattern type: {pattern_type}")
    
    # Use default confidence threshold if not provided
    if confidence_threshold is None:
        confidence_threshold = DEFAULT_CONFIDENCE_THRESHOLD
    
    # Create and return new issue pattern
    now = datetime.datetime.now()
    pattern = IssuePattern(
        pattern_id=pattern_id,
        name=name,
        pattern_type=pattern_type,
        description=description,
        features=features,
        confidence_threshold=confidence_threshold,
        occurrence_count=0,
        success_rate=0.0,
        last_seen=now,
        created_at=now,
        updated_at=now
    )
    
    logger.info(f"Created new issue pattern: {pattern_id} - {name}")
    return pattern


def get_issue_pattern(pattern_id: str) -> typing.Optional['IssuePattern']:
    """
    Retrieves an issue pattern by its ID.
    
    Args:
        pattern_id: The unique identifier of the pattern
        
    Returns:
        The issue pattern if found, None otherwise
    """
    # In a real implementation, this would query the database
    # For now, we'll just log the request
    logger.info(f"Retrieving issue pattern: {pattern_id}")
    
    # This is a placeholder - in a real implementation, this would query the database
    # and return the issue pattern or None if not found
    return None


def get_issue_patterns_by_type(pattern_type: str) -> typing.List['IssuePattern']:
    """
    Retrieves all issue patterns of a specific type.
    
    Args:
        pattern_type: The type of patterns to retrieve
        
    Returns:
        List of issue patterns of the specified type
    """
    # Validate pattern type
    if pattern_type not in PATTERN_TYPES:
        logger.error(f"Invalid pattern type: {pattern_type}. Must be one of {PATTERN_TYPES}")
        raise ValueError(f"Invalid pattern type: {pattern_type}")
    
    logger.info(f"Retrieving issue patterns of type: {pattern_type}")
    
    # This is a placeholder - in a real implementation, this would query the database
    # and return a list of issue patterns of the specified type
    return []


def get_all_issue_patterns() -> typing.List['IssuePattern']:
    """
    Retrieves all issue patterns from the database.
    
    Returns:
        List of all issue patterns
    """
    logger.info("Retrieving all issue patterns")
    
    # This is a placeholder - in a real implementation, this would query the database
    # and return a list of all issue patterns
    return []


def update_issue_pattern(pattern_id: str, update_data: dict) -> typing.Optional['IssuePattern']:
    """
    Updates an existing issue pattern.
    
    Args:
        pattern_id: The unique identifier of the pattern to update
        update_data: Dictionary of fields to update
        
    Returns:
        Updated issue pattern instance if successful, None otherwise
    """
    # Get the issue pattern
    pattern = get_issue_pattern(pattern_id)
    if pattern is None:
        logger.error(f"Issue pattern not found: {pattern_id}")
        return None
    
    # Validate pattern type if included in update data
    if 'pattern_type' in update_data and update_data['pattern_type'] not in PATTERN_TYPES:
        logger.error(f"Invalid pattern type: {update_data['pattern_type']}. Must be one of {PATTERN_TYPES}")
        raise ValueError(f"Invalid pattern type: {update_data['pattern_type']}")
    
    # Update the pattern with the provided data
    for key, value in update_data.items():
        if hasattr(pattern, key):
            setattr(pattern, key, value)
    
    # Update the updated_at timestamp
    pattern.updated_at = datetime.datetime.now()
    
    logger.info(f"Updated issue pattern: {pattern_id}")
    return pattern


def delete_issue_pattern(pattern_id: str) -> bool:
    """
    Deletes an issue pattern by its ID.
    
    Args:
        pattern_id: The unique identifier of the pattern to delete
        
    Returns:
        True if the pattern was deleted successfully, False otherwise
    """
    logger.info(f"Deleting issue pattern: {pattern_id}")
    
    # This is a placeholder - in a real implementation, this would delete the pattern
    # from the database and return a boolean indicating success or failure
    return True


def get_issue_pattern_table_schema() -> typing.List[SchemaField]:
    """
    Returns the BigQuery table schema for issue patterns.
    
    Returns:
        List of BigQuery SchemaField objects defining the table schema
    """
    schema = [
        get_schema_field("pattern_id", "STRING", "REQUIRED", "Unique identifier for the pattern"),
        get_schema_field("name", "STRING", "REQUIRED", "Name of the pattern"),
        get_schema_field("pattern_type", "STRING", "REQUIRED", "Type of pattern (data_quality, pipeline, system, resource)"),
        get_schema_field("description", "STRING", "NULLABLE", "Description of the pattern"),
        get_schema_field("features", "JSON", "REQUIRED", "Pattern detection features as JSON"),
        get_schema_field("confidence_threshold", "FLOAT", "REQUIRED", "Minimum confidence threshold for matching"),
        get_schema_field("occurrence_count", "INTEGER", "REQUIRED", "Number of times this pattern has been detected"),
        get_schema_field("success_rate", "FLOAT", "REQUIRED", "Rate of successful healing actions for this pattern"),
        get_schema_field("last_seen", "TIMESTAMP", "REQUIRED", "Timestamp when this pattern was last detected"),
        get_schema_field("created_at", "TIMESTAMP", "REQUIRED", "Timestamp when this pattern was created"),
        get_schema_field("updated_at", "TIMESTAMP", "REQUIRED", "Timestamp when this pattern was last updated")
    ]
    
    return schema


class IssuePattern:
    """
    Database model representing a recognized pattern of issues or failures
    that can be detected and resolved automatically.
    """
    
    def __init__(
        self,
        pattern_id: str,
        name: str,
        pattern_type: str,
        description: str,
        features: dict,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        occurrence_count: int = 0,
        success_rate: float = 0.0,
        last_seen: datetime.datetime = None,
        created_at: datetime.datetime = None,
        updated_at: datetime.datetime = None
    ):
        """
        Initialize an issue pattern instance.
        
        Args:
            pattern_id: Unique identifier for the pattern
            name: Name of the pattern
            pattern_type: Type of pattern (data_quality, pipeline, system, resource)
            description: Description of the pattern
            features: Dictionary of pattern detection features
            confidence_threshold: Minimum confidence threshold for matching
            occurrence_count: Number of times this pattern has been detected
            success_rate: Rate of successful healing actions for this pattern
            last_seen: Timestamp when this pattern was last detected
            created_at: Timestamp when this pattern was created
            updated_at: Timestamp when this pattern was last updated
        """
        self.pattern_id = pattern_id
        self.name = name
        self.pattern_type = pattern_type
        self.description = description
        self.features = features
        self.confidence_threshold = confidence_threshold
        self.occurrence_count = occurrence_count
        self.success_rate = success_rate
        self.last_seen = last_seen or datetime.datetime.now()
        self.created_at = created_at or datetime.datetime.now()
        self.updated_at = updated_at or datetime.datetime.now()
    
    def to_dict(self) -> dict:
        """
        Convert issue pattern to dictionary representation.
        
        Returns:
            Dictionary representation of the issue pattern
        """
        return {
            "pattern_id": self.pattern_id,
            "name": self.name,
            "pattern_type": self.pattern_type,
            "description": self.description,
            "features": self.features,
            "confidence_threshold": self.confidence_threshold,
            "occurrence_count": self.occurrence_count,
            "success_rate": self.success_rate,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    def from_dict(cls, pattern_dict: dict) -> 'IssuePattern':
        """
        Create IssuePattern from dictionary representation.
        
        Args:
            pattern_dict: Dictionary representation of an issue pattern
            
        Returns:
            IssuePattern instance
        """
        # Convert timestamp strings to datetime objects if provided
        last_seen = pattern_dict.get("last_seen")
        if last_seen and isinstance(last_seen, str):
            last_seen = datetime.datetime.fromisoformat(last_seen)
        
        created_at = pattern_dict.get("created_at")
        if created_at and isinstance(created_at, str):
            created_at = datetime.datetime.fromisoformat(created_at)
        
        updated_at = pattern_dict.get("updated_at")
        if updated_at and isinstance(updated_at, str):
            updated_at = datetime.datetime.fromisoformat(updated_at)
        
        # Create and return new issue pattern
        return cls(
            pattern_id=pattern_dict["pattern_id"],
            name=pattern_dict["name"],
            pattern_type=pattern_dict["pattern_type"],
            description=pattern_dict.get("description", ""),
            features=pattern_dict["features"],
            confidence_threshold=pattern_dict.get("confidence_threshold", DEFAULT_CONFIDENCE_THRESHOLD),
            occurrence_count=pattern_dict.get("occurrence_count", 0),
            success_rate=pattern_dict.get("success_rate", 0.0),
            last_seen=last_seen,
            created_at=created_at,
            updated_at=updated_at
        )
    
    @classmethod
    def from_bigquery_row(cls, row: dict) -> 'IssuePattern':
        """
        Create IssuePattern from a BigQuery row.
        
        Args:
            row: BigQuery result row
            
        Returns:
            IssuePattern instance
        """
        # Parse features from JSON if needed
        features = row["features"]
        if isinstance(features, str):
            features = json.loads(features)
        
        # Create and return new issue pattern
        return cls(
            pattern_id=row["pattern_id"],
            name=row["name"],
            pattern_type=row["pattern_type"],
            description=row.get("description", ""),
            features=features,
            confidence_threshold=row.get("confidence_threshold", DEFAULT_CONFIDENCE_THRESHOLD),
            occurrence_count=row.get("occurrence_count", 0),
            success_rate=row.get("success_rate", 0.0),
            last_seen=row.get("last_seen"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at")
        )
    
    def to_bigquery_row(self) -> dict:
        """
        Convert the issue pattern to a format suitable for BigQuery insertion.
        
        Returns:
            Dictionary formatted for BigQuery insertion
        """
        row = {
            "pattern_id": self.pattern_id,
            "name": self.name,
            "pattern_type": self.pattern_type,
            "description": self.description,
            "features": json.dumps(self.features) if isinstance(self.features, dict) else self.features,
            "confidence_threshold": self.confidence_threshold,
            "occurrence_count": self.occurrence_count,
            "success_rate": self.success_rate,
            "last_seen": self.last_seen,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
        
        return row
    
    def update_stats(self, healing_success: bool) -> None:
        """
        Update pattern statistics based on detection and healing results.
        
        Args:
            healing_success: Whether the healing action was successful
        """
        # Increment occurrence count
        self.occurrence_count += 1
        
        # Update success rate
        if self.occurrence_count == 1:
            # First occurrence, set success rate directly
            self.success_rate = 1.0 if healing_success else 0.0
        else:
            # Calculate new success rate
            previous_successes = self.success_rate * (self.occurrence_count - 1)
            current_successes = previous_successes + (1 if healing_success else 0)
            self.success_rate = current_successes / self.occurrence_count
        
        # Update timestamps
        now = datetime.datetime.now()
        self.last_seen = now
        self.updated_at = now
        
        logger.info(f"Updated stats for pattern {self.pattern_id}: occurrence_count={self.occurrence_count}, success_rate={self.success_rate:.2f}")
    
    def matches_issue(self, issue_data: dict, min_confidence: float = None) -> typing.Tuple[bool, float]:
        """
        Check if an issue matches this pattern.
        
        Args:
            issue_data: Issue data to match against the pattern
            min_confidence: Minimum confidence threshold, defaults to pattern's threshold
            
        Returns:
            Tuple of (matches, confidence_score)
        """
        # Calculate a simple similarity score based on matching keys and values
        # In a production system, this would use more sophisticated pattern matching algorithms
        pattern_keys = set(self.features.keys())
        issue_keys = set(issue_data.keys())
        
        # Find intersection and union of keys
        common_keys = pattern_keys.intersection(issue_keys)
        all_keys = pattern_keys.union(issue_keys)
        
        # If no keys match or no keys at all, return no match
        if not common_keys or not all_keys:
            return False, 0.0
        
        # Count matching values for common keys
        matching_values = 0
        for key in common_keys:
            if key in issue_data and self.features.get(key) == issue_data.get(key):
                matching_values += 1
        
        # Calculate similarity score: combination of key match and value match
        key_similarity = len(common_keys) / len(all_keys)
        value_similarity = matching_values / len(common_keys) if common_keys else 0.0
        similarity_score = (key_similarity + value_similarity) / 2.0
        
        # Get the threshold to use
        threshold = min_confidence if min_confidence is not None else self.confidence_threshold
        
        # Determine if the issue matches the pattern
        matches = similarity_score >= threshold
        
        logger.debug(f"Pattern {self.pattern_id} match result: {matches} with confidence {similarity_score:.2f}")
        
        return matches, similarity_score
    
    def update_features(self, new_features: dict) -> None:
        """
        Update the pattern features.
        
        Args:
            new_features: New pattern detection features
        """
        # Validate new features
        if not isinstance(new_features, dict) or not new_features:
            logger.error("Invalid features: must be a non-empty dictionary")
            raise ValueError("Invalid features: must be a non-empty dictionary")
        
        # Update features
        self.features = new_features
        
        # Update timestamp
        self.updated_at = datetime.datetime.now()
        
        logger.info(f"Updated features for pattern {self.pattern_id}")
    
    def set_confidence_threshold(self, threshold: float) -> None:
        """
        Set the confidence threshold for pattern matching.
        
        Args:
            threshold: New confidence threshold (between 0.0 and 1.0)
        """
        # Validate threshold
        if not 0.0 <= threshold <= 1.0:
            logger.error(f"Invalid confidence threshold: {threshold}. Must be between 0.0 and 1.0")
            raise ValueError(f"Invalid confidence threshold: {threshold}. Must be between 0.0 and 1.0")
        
        # Update threshold
        self.confidence_threshold = threshold
        
        # Update timestamp
        self.updated_at = datetime.datetime.now()
        
        logger.info(f"Updated confidence threshold for pattern {self.pattern_id}: {threshold:.2f}")