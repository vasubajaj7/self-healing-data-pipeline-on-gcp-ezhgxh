"""Manages dependencies between data extraction tasks and pipeline components,
ensuring proper execution order and dependency resolution. Provides
functionality to define, validate, and track dependencies between data
sources, enabling the orchestration layer to execute tasks in the correct
sequence and implement self-healing for dependency-related issues.
"""

import typing  # standard library
import datetime  # standard library
import uuid  # standard library
import enum  # standard library

# 3rd party libraries
import networkx  # version 2.8.0+

# Internal imports
from ...constants import DataSourceType, HealingActionType  # src/backend/constants.py
from ...config import get_config  # src/backend/config.py
from ...utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from ..metadata.metadata_tracker import MetadataTracker  # src/backend/ingestion/metadata/metadata_tracker.py
from ..errors.error_handler import with_error_handling  # src/backend/ingestion/errors/error_handler.py

# Initialize logger
logger = get_logger(__name__)


def create_dependency_id() -> str:
    """Generates a unique identifier for a dependency relationship

    Returns:
        str: Unique dependency ID
    """
    dependency_id = uuid.uuid4()  # Generate a UUID
    dependency_id = f"dep_{str(dependency_id)}"  # Format as a string with 'dep_' prefix
    return dependency_id  # Return the formatted dependency ID


class DependencyType(enum.Enum):
    """Enumeration of possible dependency types between data sources"""
    DATA_DEPENDENCY = "DATA_DEPENDENCY"
    EXECUTION_DEPENDENCY = "EXECUTION_DEPENDENCY"
    RESOURCE_DEPENDENCY = "RESOURCE_DEPENDENCY"
    SCHEMA_DEPENDENCY = "SCHEMA_DEPENDENCY"

    def __init__(self):
        """Default enum constructor"""
        pass  # Initialize enum values


class Dependency:
    """Represents a dependency relationship between data sources or pipeline components"""

    def __init__(
        self,
        source_id: str,
        target_id: str,
        dependency_type: DependencyType,
        dependency_params: dict = None,
        is_required: bool = True,
    ):
        """Initialize a new dependency relationship

        Args:
            source_id: The ID of the source that depends on the target
            target_id: The ID of the target that the source depends on
            dependency_type: The type of dependency (e.g., DATA_DEPENDENCY)
            dependency_params: Specific parameters for this dependency
            is_required: Whether the dependency is mandatory
        """
        self.dependency_id = create_dependency_id()  # Generate dependency_id if not provided
        self.source_id = source_id  # Store source_id (dependent entity)
        self.target_id = target_id  # Store target_id (entity being depended on)
        self.dependency_type = dependency_type  # Set dependency_type (DATA_DEPENDENCY, EXECUTION_DEPENDENCY, etc.)
        self.dependency_params = dependency_params or {}  # Store dependency_params (specific parameters for this dependency)
        self.is_required = is_required  # Set is_required flag (whether dependency is mandatory)
        self.created_at = datetime.datetime.utcnow()  # Initialize timestamps
        self.updated_at = datetime.datetime.utcnow()

    def to_dict(self) -> dict:
        """Convert the dependency to a dictionary

        Returns:
            dict: Dictionary representation of the dependency
        """
        dependency_dict = {  # Create dictionary with all dependency properties
            "dependency_id": self.dependency_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "dependency_type": self.dependency_type.value if isinstance(self.dependency_type, enum.Enum) else self.dependency_type,  # Convert enum values to strings
            "dependency_params": self.dependency_params,
            "is_required": self.is_required,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime.datetime) else self.created_at,  # Convert datetime objects to ISO format strings
            "updated_at": self.updated_at.isoformat() if isinstance(self.updated_at, datetime.datetime) else self.updated_at,  # Convert datetime objects to ISO format strings
        }
        return dependency_dict  # Return the dictionary representation

    @classmethod
    def from_dict(cls, dependency_dict: dict) -> "Dependency":
        """Create a Dependency from a dictionary

        Args:
            dependency_dict: Dictionary containing dependency properties

        Returns:
            Dependency: New Dependency object
        """
        source_id = dependency_dict["source_id"]  # Extract dependency properties from dictionary
        target_id = dependency_dict["target_id"]  # Extract dependency properties from dictionary
        dependency_type_str = dependency_dict["dependency_type"]  # Extract dependency properties from dictionary
        dependency_type = DependencyType(dependency_type_str) if dependency_type_str else None  # Convert string enum values to enum types
        dependency_params = dependency_dict.get("dependency_params", {})  # Extract dependency properties from dictionary
        is_required = dependency_dict.get("is_required", True)  # Extract dependency properties from dictionary

        dependency = cls(  # Create new Dependency object
            source_id=source_id,
            target_id=target_id,
            dependency_type=dependency_type,
            dependency_params=dependency_params,
            is_required=is_required,
        )

        dependency.dependency_id = dependency_dict.get("dependency_id")  # Set additional properties from dictionary
        created_at_str = dependency_dict.get("created_at")  # Set additional properties from dictionary
        dependency.created_at = datetime.datetime.fromisoformat(created_at_str) if created_at_str else None  # Convert string timestamps to datetime objects
        updated_at_str = dependency_dict.get("updated_at")  # Set additional properties from dictionary
        dependency.updated_at = datetime.datetime.fromisoformat(updated_at_str) if updated_at_str else None  # Convert string timestamps to datetime objects

        return dependency  # Return the new Dependency object

    def is_satisfied(self, context: dict) -> bool:
        """Check if the dependency is satisfied

        Args:
            context: Dictionary containing information about available data,
                     execution status, and resource availability

        Returns:
            bool: True if dependency is satisfied, False otherwise
        """
        if self.target_id not in context:  # Check if target_id exists in the context
            logger.debug(f"Target ID {self.target_id} not found in context")
            return False

        if self.dependency_type == DependencyType.DATA_DEPENDENCY:  # For DATA_DEPENDENCY, check if data is available
            data_available = context[self.target_id].get("data_available", False)
            logger.debug(f"Data dependency {self.dependency_id} - Data Available: {data_available}")
            return data_available

        elif self.dependency_type == DependencyType.EXECUTION_DEPENDENCY:  # For EXECUTION_DEPENDENCY, check if execution is complete
            execution_complete = context[self.target_id].get("execution_complete", False)
            logger.debug(f"Execution dependency {self.dependency_id} - Execution Complete: {execution_complete}")
            return execution_complete

        elif self.dependency_type == DependencyType.RESOURCE_DEPENDENCY:  # For RESOURCE_DEPENDENCY, check if resource is available
            resource_available = context[self.target_id].get("resource_available", False)
            logger.debug(f"Resource dependency {self.dependency_id} - Resource Available: {resource_available}")
            return resource_available

        elif self.dependency_type == DependencyType.SCHEMA_DEPENDENCY:  # For SCHEMA_DEPENDENCY, check if schema is compatible
            schema_compatible = context[self.target_id].get("schema_compatible", False)
            logger.debug(f"Schema dependency {self.dependency_id} - Schema Compatible: {schema_compatible}")
            return schema_compatible

        return False  # Return True if satisfied, False otherwise


class DependencyManager:
    """Manages dependencies between data sources and pipeline components"""

    def __init__(self, metadata_tracker: MetadataTracker):
        """Initialize the dependency manager with required services

        Args:
            metadata_tracker: Service for tracking metadata
        """
        self._metadata_tracker = metadata_tracker  # Store metadata_tracker reference
        self._dependencies = {}  # Initialize dependencies dictionary
        self._dependency_graph = networkx.DiGraph()  # Initialize dependency graph using networkx
        self._load_dependencies_from_metadata()  # Load existing dependencies from metadata
        self._build_dependency_graph()  # Build initial dependency graph

    @with_error_handling(context={'component': 'DependencyManager', 'operation': 'register_dependency'})
    def register_dependency(
        self,
        source_id: str,
        target_id: str,
        dependency_type: DependencyType,
        dependency_params: dict = None,
        is_required: bool = True,
    ) -> str:
        """Register a new dependency between sources

        Args:
            source_id: The ID of the source that depends on the target
            target_id: The ID of the target that the source depends on
            dependency_type: The type of dependency (e.g., DATA_DEPENDENCY)
            dependency_params: Specific parameters for this dependency
            is_required: Whether the dependency is mandatory

        Returns:
            str: The dependency ID
        """
        # Validate source_id and target_id exist
        # Check for circular dependencies
        dependency = Dependency(
            source_id=source_id,
            target_id=target_id,
            dependency_type=dependency_type,
            dependency_params=dependency_params,
            is_required=is_required,
        )  # Create Dependency object

        self._dependencies[dependency.dependency_id] = dependency  # Add to dependencies dictionary
        self._dependency_graph.add_edge(source_id, target_id, dependency_id=dependency.dependency_id)  # Update dependency graph
        self._track_dependency_metadata(dependency)  # Track in metadata
        return dependency.dependency_id  # Return dependency_id

    @with_error_handling(context={'component': 'DependencyManager', 'operation': 'remove_dependency'})
    def remove_dependency(self, dependency_id: str) -> bool:
        """Remove an existing dependency

        Args:
            dependency_id: The ID of the dependency to remove

        Returns:
            bool: True if removal successful
        """
        if dependency_id not in self._dependencies:  # Check if dependency_id exists
            logger.warning(f"Dependency with ID {dependency_id} not found")
            return False

        dependency = self._dependencies[dependency_id]
        source_id = dependency.source_id
        target_id = dependency.target_id

        del self._dependencies[dependency_id]  # Remove from dependencies dictionary
        if self._dependency_graph.has_edge(source_id, target_id):
            self._dependency_graph.remove_edge(source_id, target_id)  # Update dependency graph
        self._update_dependency_metadata(dependency_id, {'is_active': False})  # Update metadata
        return True  # Return success status

    def get_dependency(self, dependency_id: str) -> typing.Optional[Dependency]:
        """Get details of a specific dependency

        Args:
            dependency_id: The ID of the dependency to retrieve

        Returns:
            Dependency: The dependency object or None if not found
        """
        if dependency_id in self._dependencies:  # Check if dependency_id exists in dependencies dictionary
            return self._dependencies[dependency_id]  # Return the dependency if found
        else:
            return None  # Return None otherwise

    def get_dependencies_for_source(self, source_id: str, dependency_type: typing.Optional[DependencyType] = None) -> typing.List[Dependency]:
        """Get all dependencies for a specific source

        Args:
            source_id: The ID of the source to retrieve dependencies for
            dependency_type: Optional dependency type to filter by

        Returns:
            list: List of dependencies for the source
        """
        dependencies = [  # Filter dependencies where source_id matches
            dep
            for dep in self._dependencies.values()
            if dep.source_id == source_id
        ]
        if dependency_type:  # Filter by dependency_type if provided
            dependencies = [
                dep for dep in dependencies if dep.dependency_type == dependency_type
            ]
        return dependencies  # Return list of matching dependencies

    def get_dependent_sources(self, target_id: str, dependency_type: typing.Optional[DependencyType] = None) -> typing.List[str]:
        """Get sources that depend on a specific target

        Args:
            target_id: The ID of the target to retrieve dependent sources for
            dependency_type: Optional dependency type to filter by

        Returns:
            list: List of source IDs that depend on the target
        """
        dependencies = [  # Filter dependencies where target_id matches
            dep
            for dep in self._dependencies.values()
            if dep.target_id == target_id
        ]
        if dependency_type:  # Filter by dependency_type if provided
            dependencies = [
                dep for dep in dependencies if dep.dependency_type == dependency_type
            ]
        source_ids = [dep.source_id for dep in dependencies]  # Extract source_id from matching dependencies
        return source_ids  # Return list of dependent source IDs

    @with_error_handling(context={'component': 'DependencyManager', 'operation': 'check_dependencies_satisfied'})
    def check_dependencies_satisfied(self, source_id: str, context: dict) -> typing.Tuple[bool, typing.List[Dependency]]:
        """Check if all dependencies for a source are satisfied

        Args:
            source_id: The ID of the source to check dependencies for
            context: Dictionary containing information about available data,
                     execution status, and resource availability

        Returns:
            tuple: (bool, list) - Success status and list of unsatisfied dependencies
        """
        dependencies = self.get_dependencies_for_source(source_id)  # Get all dependencies for the source
        unsatisfied_dependencies = []
        for dependency in dependencies:  # Check if each dependency is satisfied using context
            if not dependency.is_satisfied(context):
                unsatisfied_dependencies.append(dependency)  # Collect unsatisfied dependencies

        if not unsatisfied_dependencies:  # Return True if all satisfied, False otherwise, along with unsatisfied list
            return True, []
        else:
            return False, unsatisfied_dependencies

    @with_error_handling(context={'component': 'DependencyManager', 'operation': 'get_execution_order'})
    def get_execution_order(self, source_ids: typing.List[str]) -> typing.List[str]:
        """Determine optimal execution order based on dependencies

        Args:
            source_ids: List of source IDs to determine execution order for

        Returns:
            list: Ordered list of source IDs for execution
        """
        subgraph = self._dependency_graph.subgraph(source_ids)  # Create subgraph with specified source IDs
        try:
            ordered_sources = list(networkx.topological_sort(subgraph))  # Perform topological sort on the subgraph
            return ordered_sources
        except networkx.NetworkXUnfeasible:
            # Handle cycles by breaking dependencies if needed
            logger.warning("Circular dependency detected. Attempting to break cycle.")
            # TODO: Implement cycle breaking logic (e.g., removing the least critical dependency)
            return source_ids  # Return original list if cycle breaking fails

    def analyze_dependency_impact(self, source_id: str) -> dict:
        """Analyze impact of changes to a specific source

        Args:
            source_id: The ID of the source to analyze impact for

        Returns:
            dict: Impact analysis including affected sources
        """
        dependent_sources = self.get_dependent_sources(source_id)  # Find all sources that depend on the specified source
        # TODO: Calculate direct and indirect dependencies
        # TODO: Determine criticality based on dependency types
        return {  # Return impact analysis dictionary
            "source_id": source_id,
            "dependent_sources": dependent_sources,
        }

    def suggest_dependency_resolution(self, source_id: str, unsatisfied_dependencies: typing.List[Dependency]) -> dict:
        """Suggest actions to resolve unsatisfied dependencies

        Args:
            source_id: The ID of the source with unsatisfied dependencies
            unsatisfied_dependencies: List of unsatisfied dependencies

        Returns:
            dict: Resolution suggestions for each unsatisfied dependency
        """
        suggestions = {}
        for dependency in unsatisfied_dependencies:  # Analyze each unsatisfied dependency
            if dependency.dependency_type == DependencyType.DATA_DEPENDENCY:  # For DATA_DEPENDENCY, suggest data extraction
                suggestions[dependency.dependency_id] = "Ensure data extraction for target is complete"
            elif dependency.dependency_type == DependencyType.EXECUTION_DEPENDENCY:  # For EXECUTION_DEPENDENCY, suggest task execution
                suggestions[dependency.dependency_id] = "Ensure task execution for target is complete"
            elif dependency.dependency_type == DependencyType.RESOURCE_DEPENDENCY:  # For RESOURCE_DEPENDENCY, suggest resource allocation
                suggestions[dependency.dependency_id] = "Ensure resource allocation for target is sufficient"
            elif dependency.dependency_type == DependencyType.SCHEMA_DEPENDENCY:  # For SCHEMA_DEPENDENCY, suggest schema updates
                suggestions[dependency.dependency_id] = "Ensure schema compatibility for target"
        return suggestions  # Return dictionary of resolution suggestions

    def detect_circular_dependencies(self) -> typing.List[typing.List[str]]:
        """Detect circular dependencies in the dependency graph

        Returns:
            list: List of circular dependency chains
        """
        try:
            cycles = networkx.find_cycles(self._dependency_graph)  # Use networkx to find cycles in the dependency graph
            dependency_chains = []
            for cycle in cycles:
                chain = " -> ".join(cycle)
                dependency_chains.append(chain)
            return dependency_chains  # Return list of circular dependency chains
        except networkx.NetworkXNoCycle:
            return []

    def visualize_dependencies(self, source_ids: typing.List[str] = None, output_format: str = "png") -> str:
        """Generate a visualization of the dependency graph

        Args:
            source_ids: List of source IDs to include in the visualization
            output_format: Format for the visualization (e.g., "png", "pdf")

        Returns:
            str: Path to generated visualization file
        """
        # TODO: Implement dependency graph visualization using networkx and matplotlib
        # Create subgraph with specified source IDs or full graph
        # Use networkx visualization capabilities
        # Generate graph in specified format (PNG, DOT, etc.)
        # Save to temporary file
        # Return path to visualization file
        return "path/to/visualization.png"

    def _build_dependency_graph(self) -> networkx.DiGraph:
        """Internal method to build the dependency graph from dependencies

        Returns:
            networkx.DiGraph: Directed graph of dependencies
        """
        graph = networkx.DiGraph()  # Create empty directed graph
        for dependency in self._dependencies.values():
            source_id = dependency.source_id
            target_id = dependency.target_id
            graph.add_node(source_id)  # Add nodes for all sources and targets
            graph.add_node(target_id)
            graph.add_edge(source_id, target_id, dependency_id=dependency.dependency_id)  # Add edges for each dependency
        self._dependency_graph = graph  # Add edge attributes based on dependency properties
        return self._dependency_graph  # Return the constructed graph

    def _track_dependency_metadata(self, dependency: Dependency) -> str:
        """Internal method to track dependency metadata

        Args:
            dependency: The dependency object to track

        Returns:
            str: Metadata record ID
        """
        metadata = dependency.to_dict()  # Create metadata record for dependency
        record_id = self._metadata_tracker.track_source_system(  # Store dependency details in metadata
            source_id=dependency.source_id,
            source_name=dependency.source_id,  # Assuming source_id can be used as source_name
            source_type=DataSourceType.CUSTOM,  # Assuming a custom type for dependencies
            connection_details={},  # No connection details for dependencies
            schema_version="1.0",  # Default schema version
        )
        return record_id  # Return metadata record ID

    def _update_dependency_metadata(self, dependency_id: str, updates: dict) -> bool:
        """Internal method to update dependency metadata

        Args:
            dependency_id: The ID of the dependency to update
            updates: Dictionary of updates to apply

        Returns:
            bool: True if update successful
        """
        # TODO: Implement metadata update logic
        return True  # Return success status

    def _load_dependencies_from_metadata(self) -> dict:
        """Internal method to load dependencies from metadata

        Returns:
            dict: Dictionary of dependencies
        """
        # TODO: Implement metadata loading logic
        return {}  # Return the dependencies dictionary