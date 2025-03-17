"""
Data lineage tracking module for the self-healing data pipeline.

This module provides functionality to track, store, and analyze data lineage information
throughout the pipeline. It captures relationships between data sources, transformations,
and targets to enable visualization, impact analysis, and support for self-healing.

The lineage tracking system maintains detailed metadata about data movements and transformations,
which helps with troubleshooting, compliance, and understanding data dependencies.
"""

import uuid
import datetime
from typing import Dict, Any, List, Optional, Union, Tuple
import json
import networkx as nx

from ...constants import DataSourceType
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.firestore_client import FirestoreClient
from ...utils.storage.bigquery_client import BigQueryClient

# Configure module logger
logger = get_logger(__name__)

# Default collection and table names for lineage storage
DEFAULT_LINEAGE_COLLECTION = "data_lineage"
DEFAULT_LINEAGE_TABLE = "data_lineage"


def create_lineage_record(record_type: str, lineage_data: Dict[str, Any]) -> str:
    """Creates a new lineage record with a unique identifier.
    
    Args:
        record_type: The type of lineage record (e.g., 'extraction', 'transformation')
        lineage_data: Dictionary containing lineage information
        
    Returns:
        The unique identifier for the created lineage record
    """
    # Generate unique ID for lineage record
    lineage_id = str(uuid.uuid4())
    
    # Create lineage record with standard fields
    lineage_record = {
        "lineage_id": lineage_id,
        "record_type": record_type,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "lineage_data": lineage_data
    }
    
    # Add system information
    lineage_record["system_info"] = {
        "environment": get_config().get_environment(),
        "project_id": get_config().get_gcp_project_id()
    }
    
    # Store the lineage record
    lineage_tracker = LineageTracker()
    lineage_tracker._store_lineage_record(lineage_record)
    
    return lineage_id


class LineageTracker:
    """Tracks and manages data lineage information throughout the pipeline.
    
    This class provides methods to record lineage information at various stages
    of the data pipeline, including extraction, transformation, loading, and
    self-healing actions. It stores lineage data in Firestore for operational use
    and optionally in BigQuery for long-term analysis.
    
    The lineage information is represented as a directed graph, where nodes are
    data entities (sources, datasets, tables) and edges represent operations or
    transformations between them.
    """
    
    def __init__(self):
        """Initialize the LineageTracker with storage clients."""
        # Initialize Firestore client for lineage storage
        self._firestore_client = FirestoreClient()
        
        # Initialize BigQuery client for long-term lineage storage
        self._bigquery_client = BigQueryClient()
        
        # Get configuration settings
        config = get_config()
        
        # Configure storage collection and table names
        self._lineage_collection = config.get("lineage.collection", DEFAULT_LINEAGE_COLLECTION)
        self._lineage_table = config.get("lineage.table", DEFAULT_LINEAGE_TABLE)
        
        # Determine if BigQuery storage is enabled
        self._enable_bigquery_storage = config.get("lineage.enable_bigquery_storage", False)
        
        # Initialize empty lineage graph
        self._lineage_graph = nx.DiGraph()
    
    def track_source_extraction(self, source_id: str, execution_id: str, 
                               dataset: str, table: str, 
                               extraction_details: Dict[str, Any]) -> str:
        """Records lineage for data extracted from a source system.
        
        Args:
            source_id: Identifier for the source system
            execution_id: Pipeline execution identifier
            dataset: Target dataset name
            table: Target table name
            extraction_details: Details about the extraction operation
            
        Returns:
            The lineage record ID
        """
        # Create source extraction lineage record
        lineage_data = {
            "source_id": source_id,
            "execution_id": execution_id,
            "dataset": dataset,
            "table": table,
            "extraction_details": extraction_details
        }
        
        # Add source and target information
        source_node = f"source:{source_id}"
        target_node = f"dataset:{dataset}.{table}"
        
        # Add nodes and edge to lineage graph
        if not self._lineage_graph.has_node(source_node):
            self._lineage_graph.add_node(source_node, type="source")
        
        if not self._lineage_graph.has_node(target_node):
            self._lineage_graph.add_node(target_node, type="dataset")
        
        # Add edge with extraction details
        self._lineage_graph.add_edge(
            source_node, 
            target_node, 
            operation="extraction",
            timestamp=datetime.datetime.utcnow().isoformat(),
            details=extraction_details
        )
        
        # Create and store lineage record
        record_id = str(uuid.uuid4())
        lineage_record = self._create_lineage_record(
            record_id, 
            "source_extraction", 
            lineage_data
        )
        
        self._store_lineage_record(lineage_record)
        
        return record_id
    
    def track_transformation(self, execution_id: str, task_id: str,
                            input_datasets: List[Dict[str, str]],
                            output_datasets: List[Dict[str, str]],
                            transformation_type: str,
                            transformation_details: Dict[str, Any]) -> str:
        """Records lineage for a data transformation operation.
        
        Args:
            execution_id: Pipeline execution identifier
            task_id: Task identifier
            input_datasets: List of input datasets [{dataset, table}, ...]
            output_datasets: List of output datasets [{dataset, table}, ...]
            transformation_type: Type of transformation
            transformation_details: Details about the transformation
            
        Returns:
            The lineage record ID
        """
        # Create transformation lineage record
        lineage_data = {
            "execution_id": execution_id,
            "task_id": task_id,
            "input_datasets": input_datasets,
            "output_datasets": output_datasets,
            "transformation_type": transformation_type,
            "transformation_details": transformation_details
        }
        
        # Add nodes for inputs and outputs
        input_nodes = []
        for dataset_info in input_datasets:
            node_id = f"dataset:{dataset_info['dataset']}.{dataset_info['table']}"
            if not self._lineage_graph.has_node(node_id):
                self._lineage_graph.add_node(node_id, type="dataset")
            input_nodes.append(node_id)
        
        output_nodes = []
        for dataset_info in output_datasets:
            node_id = f"dataset:{dataset_info['dataset']}.{dataset_info['table']}"
            if not self._lineage_graph.has_node(node_id):
                self._lineage_graph.add_node(node_id, type="dataset")
            output_nodes.append(node_id)
        
        # Add edges from each input to each output
        for input_node in input_nodes:
            for output_node in output_nodes:
                self._lineage_graph.add_edge(
                    input_node, 
                    output_node, 
                    operation="transformation",
                    transformation_type=transformation_type,
                    task_id=task_id,
                    timestamp=datetime.datetime.utcnow().isoformat(),
                    details=transformation_details
                )
        
        # Create and store lineage record
        record_id = str(uuid.uuid4())
        lineage_record = self._create_lineage_record(
            record_id, 
            "transformation", 
            lineage_data
        )
        
        self._store_lineage_record(lineage_record)
        
        return record_id
    
    def track_data_loading(self, execution_id: str, 
                          source_dataset: str, source_table: str,
                          target_dataset: str, target_table: str,
                          load_details: Dict[str, Any]) -> str:
        """Records lineage for data loaded into a target system.
        
        Args:
            execution_id: Pipeline execution identifier
            source_dataset: Source dataset name
            source_table: Source table name
            target_dataset: Target dataset name
            target_table: Target table name
            load_details: Details about the load operation
            
        Returns:
            The lineage record ID
        """
        # Create data loading lineage record
        lineage_data = {
            "execution_id": execution_id,
            "source_dataset": source_dataset,
            "source_table": source_table,
            "target_dataset": target_dataset,
            "target_table": target_table,
            "load_details": load_details
        }
        
        # Add source and target information
        source_node = f"dataset:{source_dataset}.{source_table}"
        target_node = f"dataset:{target_dataset}.{target_table}"
        
        # Add nodes to lineage graph
        if not self._lineage_graph.has_node(source_node):
            self._lineage_graph.add_node(source_node, type="dataset")
        
        if not self._lineage_graph.has_node(target_node):
            self._lineage_graph.add_node(target_node, type="dataset")
        
        # Add edge with load details
        self._lineage_graph.add_edge(
            source_node, 
            target_node, 
            operation="load",
            timestamp=datetime.datetime.utcnow().isoformat(),
            details=load_details
        )
        
        # Create and store lineage record
        record_id = str(uuid.uuid4())
        lineage_record = self._create_lineage_record(
            record_id, 
            "data_loading", 
            lineage_data
        )
        
        self._store_lineage_record(lineage_record)
        
        return record_id
    
    def track_data_quality_validation(self, execution_id: str,
                                     validation_id: str,
                                     dataset: str, table: str,
                                     validation_results: Dict[str, Any]) -> str:
        """Records lineage for data quality validation.
        
        Args:
            execution_id: Pipeline execution identifier
            validation_id: Validation identifier
            dataset: Dataset name
            table: Table name
            validation_results: Results of the validation
            
        Returns:
            The lineage record ID
        """
        # Create data quality validation lineage record
        lineage_data = {
            "execution_id": execution_id,
            "validation_id": validation_id,
            "dataset": dataset,
            "table": table,
            "validation_results": validation_results
        }
        
        # Add dataset and validation nodes
        dataset_node = f"dataset:{dataset}.{table}"
        validation_node = f"validation:{validation_id}"
        
        # Add nodes to lineage graph
        if not self._lineage_graph.has_node(dataset_node):
            self._lineage_graph.add_node(dataset_node, type="dataset")
        
        if not self._lineage_graph.has_node(validation_node):
            self._lineage_graph.add_node(validation_node, type="validation")
        
        # Add edge with validation details
        self._lineage_graph.add_edge(
            dataset_node, 
            validation_node, 
            operation="validation",
            timestamp=datetime.datetime.utcnow().isoformat(),
            details=validation_results
        )
        
        # Create and store lineage record
        record_id = str(uuid.uuid4())
        lineage_record = self._create_lineage_record(
            record_id, 
            "data_quality_validation", 
            lineage_data
        )
        
        self._store_lineage_record(lineage_record)
        
        return record_id
    
    def track_self_healing_action(self, execution_id: str,
                                 healing_id: str,
                                 dataset: str, table: str,
                                 action_type: str,
                                 action_details: Dict[str, Any]) -> str:
        """Records lineage for a self-healing action.
        
        Args:
            execution_id: Pipeline execution identifier
            healing_id: Self-healing action identifier
            dataset: Dataset name that was healed
            table: Table name that was healed
            action_type: Type of healing action
            action_details: Details about the healing action
            
        Returns:
            The lineage record ID
        """
        # Create self-healing action lineage record
        lineage_data = {
            "execution_id": execution_id,
            "healing_id": healing_id,
            "dataset": dataset,
            "table": table,
            "action_type": action_type,
            "action_details": action_details
        }
        
        # Add dataset and healing nodes
        dataset_node = f"dataset:{dataset}.{table}"
        healing_node = f"healing:{healing_id}"
        result_node = f"dataset:{dataset}.{table}:healed"
        
        # Add nodes to lineage graph
        if not self._lineage_graph.has_node(dataset_node):
            self._lineage_graph.add_node(dataset_node, type="dataset")
        
        if not self._lineage_graph.has_node(healing_node):
            self._lineage_graph.add_node(healing_node, type="healing")
        
        if not self._lineage_graph.has_node(result_node):
            self._lineage_graph.add_node(result_node, type="dataset")
        
        # Add edges to represent healing flow
        self._lineage_graph.add_edge(
            dataset_node, 
            healing_node, 
            operation="healing_input",
            timestamp=datetime.datetime.utcnow().isoformat()
        )
        
        self._lineage_graph.add_edge(
            healing_node, 
            result_node, 
            operation="healing_output",
            action_type=action_type,
            timestamp=datetime.datetime.utcnow().isoformat(),
            details=action_details
        )
        
        # Create and store lineage record
        record_id = str(uuid.uuid4())
        lineage_record = self._create_lineage_record(
            record_id, 
            "self_healing_action", 
            lineage_data
        )
        
        self._store_lineage_record(lineage_record)
        
        return record_id
    
    def get_lineage_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a specific lineage record by ID.
        
        Args:
            record_id: The lineage record ID
            
        Returns:
            The lineage record or None if not found
        """
        try:
            # Query Firestore for the record
            record = self._firestore_client.get_document(
                self._lineage_collection, 
                record_id
            )
            return record
        except Exception as e:
            logger.error(f"Error retrieving lineage record {record_id}: {e}")
            return None
    
    def get_dataset_lineage(self, dataset: str, table: str, 
                           upstream: bool = True, downstream: bool = True,
                           depth: int = None) -> Dict[str, Any]:
        """Retrieves the complete lineage for a specific dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            upstream: Whether to include upstream (source) lineage
            downstream: Whether to include downstream (target) lineage
            depth: Maximum depth for lineage traversal
            
        Returns:
            Complete lineage information including upstream and downstream dependencies
        """
        # Build lineage graph if not already built
        if not self._lineage_graph.nodes():
            self._build_lineage_graph()
        
        # Identify the node representing the dataset
        dataset_node = f"dataset:{dataset}.{table}"
        
        # Check if dataset exists in graph
        if not self._lineage_graph.has_node(dataset_node):
            logger.warning(f"Dataset {dataset}.{table} not found in lineage graph")
            return {
                "dataset": dataset,
                "table": table,
                "upstream": [],
                "downstream": [],
                "found": False
            }
        
        result = {
            "dataset": dataset,
            "table": table,
            "found": True,
            "upstream": [],
            "downstream": []
        }
        
        # Get upstream lineage
        if upstream:
            # Use networkx to find predecessors
            predecessors = set()
            current_level = {dataset_node}
            current_depth = 0
            
            while current_level and (depth is None or current_depth < depth):
                next_level = set()
                for node in current_level:
                    for pred in self._lineage_graph.predecessors(node):
                        if pred not in predecessors and pred != dataset_node:
                            next_level.add(pred)
                            predecessors.add(pred)
                
                current_level = next_level
                current_depth += 1
            
            # Add upstream nodes to result
            for node in predecessors:
                node_type, node_id = node.split(':', 1)
                
                if node_type == 'dataset':
                    ds, tbl = node_id.split('.')
                    edge_data = self._lineage_graph.get_edge_data(node, dataset_node) or {}
                    result["upstream"].append({
                        "type": node_type,
                        "dataset": ds,
                        "table": tbl,
                        "operation": edge_data.get("operation", "unknown"),
                        "timestamp": edge_data.get("timestamp"),
                        "details": edge_data.get("details", {})
                    })
                elif node_type == 'source':
                    edge_data = self._lineage_graph.get_edge_data(node, dataset_node) or {}
                    result["upstream"].append({
                        "type": node_type,
                        "source_id": node_id,
                        "operation": edge_data.get("operation", "unknown"),
                        "timestamp": edge_data.get("timestamp"),
                        "details": edge_data.get("details", {})
                    })
        
        # Get downstream lineage
        if downstream:
            # Use networkx to find successors
            successors = set()
            current_level = {dataset_node}
            current_depth = 0
            
            while current_level and (depth is None or current_depth < depth):
                next_level = set()
                for node in current_level:
                    for succ in self._lineage_graph.successors(node):
                        if succ not in successors and succ != dataset_node:
                            next_level.add(succ)
                            successors.add(succ)
                
                current_level = next_level
                current_depth += 1
            
            # Add downstream nodes to result
            for node in successors:
                node_type, node_id = node.split(':', 1)
                
                if node_type == 'dataset':
                    ds, tbl = node_id.split('.')
                    edge_data = self._lineage_graph.get_edge_data(dataset_node, node) or {}
                    result["downstream"].append({
                        "type": node_type,
                        "dataset": ds,
                        "table": tbl,
                        "operation": edge_data.get("operation", "unknown"),
                        "timestamp": edge_data.get("timestamp"),
                        "details": edge_data.get("details", {})
                    })
                elif node_type in ('validation', 'healing'):
                    edge_data = self._lineage_graph.get_edge_data(dataset_node, node) or {}
                    result["downstream"].append({
                        "type": node_type,
                        "id": node_id,
                        "operation": edge_data.get("operation", "unknown"),
                        "timestamp": edge_data.get("timestamp"),
                        "details": edge_data.get("details", {})
                    })
        
        return result
    
    def get_execution_lineage(self, execution_id: str) -> Dict[str, Any]:
        """Retrieves lineage information for a specific pipeline execution.
        
        Args:
            execution_id: The execution ID
            
        Returns:
            Lineage information for the execution
        """
        try:
            # Query lineage records for this execution
            query = {
                "lineage_data.execution_id": execution_id
            }
            
            records = self._firestore_client.query_collection(
                self._lineage_collection, 
                query
            )
            
            if not records:
                logger.warning(f"No lineage records found for execution {execution_id}")
                return {
                    "execution_id": execution_id,
                    "found": False,
                    "stages": []
                }
            
            # Build execution subgraph
            execution_graph = nx.DiGraph()
            
            # Process each record
            stages = {}
            for record in records:
                record_type = record.get("record_type")
                lineage_data = record.get("lineage_data", {})
                timestamp = record.get("timestamp")
                
                # Group by record type
                if record_type not in stages:
                    stages[record_type] = []
                
                stages[record_type].append({
                    "lineage_id": record.get("lineage_id"),
                    "timestamp": timestamp,
                    "data": lineage_data
                })
            
            return {
                "execution_id": execution_id,
                "found": True,
                "stages": stages
            }
        except Exception as e:
            logger.error(f"Error retrieving execution lineage for {execution_id}: {e}")
            return {
                "execution_id": execution_id,
                "found": False,
                "error": str(e)
            }
    
    def analyze_impact(self, dataset: str, table: str) -> Dict[str, Any]:
        """Analyzes the impact of changes to a dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            
        Returns:
            Impact analysis including affected downstream datasets
        """
        # Build lineage graph if not already built
        if not self._lineage_graph.nodes():
            self._build_lineage_graph()
        
        # Identify the node representing the dataset
        dataset_node = f"dataset:{dataset}.{table}"
        
        # Check if dataset exists in graph
        if not self._lineage_graph.has_node(dataset_node):
            logger.warning(f"Dataset {dataset}.{table} not found in lineage graph")
            return {
                "dataset": dataset,
                "table": table,
                "found": False,
                "impact": []
            }
        
        # Find all descendants in the graph (downstream datasets)
        descendants = nx.descendants(self._lineage_graph, dataset_node)
        
        # Filter to only include dataset nodes
        affected_datasets = []
        for node in descendants:
            if node.startswith("dataset:"):
                node_type, node_id = node.split(':', 1)
                
                # Skip healed datasets (they have a :healed suffix)
                if ':healed' in node_id:
                    continue
                    
                ds, tbl = node_id.split('.')
                
                # Calculate path from source to this dataset
                try:
                    path = nx.shortest_path(self._lineage_graph, dataset_node, node)
                    path_length = len(path) - 1  # Number of edges
                except (nx.NetworkXNoPath, nx.NetworkXError):
                    path_length = None
                
                affected_datasets.append({
                    "dataset": ds,
                    "table": tbl,
                    "distance": path_length
                })
        
        return {
            "dataset": dataset,
            "table": table,
            "found": True,
            "impact": {
                "affected_dataset_count": len(affected_datasets),
                "affected_datasets": affected_datasets
            }
        }
    
    def trace_data_element(self, dataset: str, table: str, 
                          column: str, value: str) -> Dict[str, Any]:
        """Traces the lineage of a specific data element through transformations.
        
        Args:
            dataset: Dataset name
            table: Table name
            column: Column name
            value: Value to trace
            
        Returns:
            Element-level lineage trace
        """
        # Build lineage graph if not already built
        if not self._lineage_graph.nodes():
            self._build_lineage_graph()
        
        # Identify the node representing the dataset
        dataset_node = f"dataset:{dataset}.{table}"
        
        # Check if dataset exists in graph
        if not self._lineage_graph.has_node(dataset_node):
            logger.warning(f"Dataset {dataset}.{table} not found in lineage graph")
            return {
                "dataset": dataset,
                "table": table,
                "column": column,
                "value": value,
                "found": False,
                "trace": []
            }
        
        # This feature requires detailed transformation metadata that tracks column-level
        # transformations, which may not be available in all cases. We'll implement a
        # basic version that identifies the transformations that affect this column.
        
        # Get upstream lineage to identify relevant transformations
        upstream_lineage = self.get_dataset_lineage(dataset, table, upstream=True, downstream=False)
        
        # Look for transformations that mention this column
        column_transformations = []
        for upstream in upstream_lineage.get("upstream", []):
            if upstream.get("operation") == "transformation":
                details = upstream.get("details", {})
                
                # Check if transformation affects this column
                if "columns" in details and column in details["columns"]:
                    column_transformations.append({
                        "source": upstream.get("dataset", "") + "." + upstream.get("table", ""),
                        "transformation_type": details.get("transformation_type", "unknown"),
                        "column_transformation": details.get("column_transformations", {}).get(column, "unknown")
                    })
                # Or if transformation mentions this column in any way
                elif column in str(details):
                    column_transformations.append({
                        "source": upstream.get("dataset", "") + "." + upstream.get("table", ""),
                        "transformation_type": details.get("transformation_type", "unknown"),
                        "details": details
                    })
        
        return {
            "dataset": dataset,
            "table": table,
            "column": column,
            "value": value,
            "found": True,
            "trace": {
                "column_transformations": column_transformations
            }
        }
    
    def visualize_lineage(self, dataset: str, table: str, 
                         depth: int = None, format: str = "dot") -> str:
        """Generates a visualization of the lineage graph.
        
        Args:
            dataset: Dataset name
            table: Table name
            depth: Maximum depth for lineage traversal
            format: Output format (dot, json, html)
            
        Returns:
            Visualization in the specified format
        """
        # Build lineage graph if not already built
        if not self._lineage_graph.nodes():
            self._build_lineage_graph()
        
        # Identify the node representing the dataset
        dataset_node = f"dataset:{dataset}.{table}"
        
        # Check if dataset exists in graph
        if not self._lineage_graph.has_node(dataset_node):
            logger.warning(f"Dataset {dataset}.{table} not found in lineage graph")
            return ""
        
        # Extract relevant subgraph based on dataset and depth
        relevant_nodes = {dataset_node}
        
        # Add upstream nodes
        current_level = {dataset_node}
        current_depth = 0
        
        while current_level and (depth is None or current_depth < depth):
            next_level = set()
            for node in current_level:
                for pred in self._lineage_graph.predecessors(node):
                    if pred not in relevant_nodes:
                        next_level.add(pred)
                        relevant_nodes.add(pred)
            
            current_level = next_level
            current_depth += 1
        
        # Add downstream nodes
        current_level = {dataset_node}
        current_depth = 0
        
        while current_level and (depth is None or current_depth < depth):
            next_level = set()
            for node in current_level:
                for succ in self._lineage_graph.successors(node):
                    if succ not in relevant_nodes:
                        next_level.add(succ)
                        relevant_nodes.add(succ)
            
            current_level = next_level
            current_depth += 1
        
        # Create subgraph with relevant nodes
        subgraph = self._lineage_graph.subgraph(relevant_nodes)
        
        # Generate visualization in requested format
        if format == "dot":
            return nx.nx_pydot.to_pydot(subgraph).to_string()
        elif format == "json":
            return json.dumps(nx.node_link_data(subgraph))
        elif format == "html":
            # Create simple HTML representation
            nodes_html = "\n".join([f'<div class="node">{node}</div>' for node in subgraph.nodes()])
            edges_html = "\n".join([f'<div class="edge">{u} -> {v}</div>' for u, v in subgraph.edges()])
            
            html = f"""
            <html>
            <head>
                <style>
                    .node {{ padding: 5px; margin: 5px; background-color: #f0f0f0; }}
                    .edge {{ padding: 5px; margin: 5px; }}
                </style>
            </head>
            <body>
                <h1>Lineage for {dataset}.{table}</h1>
                <h2>Nodes:</h2>
                {nodes_html}
                <h2>Edges:</h2>
                {edges_html}
            </body>
            </html>
            """
            return html
        else:
            logger.warning(f"Unsupported visualization format: {format}")
            return ""
    
    def search_lineage(self, search_criteria: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Searches lineage records based on criteria.
        
        Args:
            search_criteria: Dictionary of search criteria
            limit: Maximum number of records to return
            
        Returns:
            List of matching lineage records
        """
        try:
            # Build query based on search criteria
            query = {}
            
            # Process search criteria
            for key, value in search_criteria.items():
                if key.startswith("lineage_data."):
                    # For nested fields in lineage_data
                    query[key] = value
                elif key in ["record_type", "timestamp", "lineage_id"]:
                    # For top-level fields
                    query[key] = value
                else:
                    # Default to searching in lineage_data
                    query[f"lineage_data.{key}"] = value
            
            # Execute query
            records = self._firestore_client.query_collection(
                self._lineage_collection, 
                query, 
                limit=limit
            )
            
            return records or []
        
        except Exception as e:
            logger.error(f"Error searching lineage records: {e}")
            return []
    
    def _build_lineage_graph(self) -> nx.DiGraph:
        """Internal method to build the lineage graph from stored records.
        
        Returns:
            Directed graph representing data lineage
        """
        logger.info("Building lineage graph from stored records")
        
        # Initialize empty directed graph
        graph = nx.DiGraph()
        
        try:
            # Query all lineage records
            records = self._firestore_client.query_collection(
                self._lineage_collection, 
                {}, 
                limit=10000  # Set a reasonable limit
            )
            
            if not records:
                logger.warning("No lineage records found to build graph")
                return graph
            
            # Process each record to add nodes and edges
            for record in records:
                record_type = record.get("record_type")
                lineage_data = record.get("lineage_data", {})
                
                if record_type == "source_extraction":
                    # Add source and dataset nodes
                    source_id = lineage_data.get("source_id")
                    dataset = lineage_data.get("dataset")
                    table = lineage_data.get("table")
                    
                    if source_id and dataset and table:
                        source_node = f"source:{source_id}"
                        dataset_node = f"dataset:{dataset}.{table}"
                        
                        graph.add_node(source_node, type="source")
                        graph.add_node(dataset_node, type="dataset")
                        
                        # Add edge with extraction details
                        graph.add_edge(
                            source_node, 
                            dataset_node, 
                            operation="extraction",
                            timestamp=record.get("timestamp"),
                            details=lineage_data.get("extraction_details", {})
                        )
                
                elif record_type == "transformation":
                    # Add input and output dataset nodes
                    input_datasets = lineage_data.get("input_datasets", [])
                    output_datasets = lineage_data.get("output_datasets", [])
                    
                    input_nodes = []
                    for dataset_info in input_datasets:
                        ds = dataset_info.get("dataset")
                        tbl = dataset_info.get("table")
                        if ds and tbl:
                            node_id = f"dataset:{ds}.{tbl}"
                            graph.add_node(node_id, type="dataset")
                            input_nodes.append(node_id)
                    
                    output_nodes = []
                    for dataset_info in output_datasets:
                        ds = dataset_info.get("dataset")
                        tbl = dataset_info.get("table")
                        if ds and tbl:
                            node_id = f"dataset:{ds}.{tbl}"
                            graph.add_node(node_id, type="dataset")
                            output_nodes.append(node_id)
                    
                    # Add edges from each input to each output
                    for input_node in input_nodes:
                        for output_node in output_nodes:
                            graph.add_edge(
                                input_node, 
                                output_node, 
                                operation="transformation",
                                transformation_type=lineage_data.get("transformation_type"),
                                task_id=lineage_data.get("task_id"),
                                timestamp=record.get("timestamp"),
                                details=lineage_data.get("transformation_details", {})
                            )
                
                elif record_type == "data_loading":
                    # Add source and target dataset nodes
                    source_dataset = lineage_data.get("source_dataset")
                    source_table = lineage_data.get("source_table")
                    target_dataset = lineage_data.get("target_dataset")
                    target_table = lineage_data.get("target_table")
                    
                    if source_dataset and source_table and target_dataset and target_table:
                        source_node = f"dataset:{source_dataset}.{source_table}"
                        target_node = f"dataset:{target_dataset}.{target_table}"
                        
                        graph.add_node(source_node, type="dataset")
                        graph.add_node(target_node, type="dataset")
                        
                        # Add edge with load details
                        graph.add_edge(
                            source_node, 
                            target_node, 
                            operation="load",
                            timestamp=record.get("timestamp"),
                            details=lineage_data.get("load_details", {})
                        )
                
                elif record_type == "data_quality_validation":
                    # Add dataset and validation nodes
                    dataset = lineage_data.get("dataset")
                    table = lineage_data.get("table")
                    validation_id = lineage_data.get("validation_id")
                    
                    if dataset and table and validation_id:
                        dataset_node = f"dataset:{dataset}.{table}"
                        validation_node = f"validation:{validation_id}"
                        
                        graph.add_node(dataset_node, type="dataset")
                        graph.add_node(validation_node, type="validation")
                        
                        # Add edge with validation details
                        graph.add_edge(
                            dataset_node, 
                            validation_node, 
                            operation="validation",
                            timestamp=record.get("timestamp"),
                            details=lineage_data.get("validation_results", {})
                        )
                
                elif record_type == "self_healing_action":
                    # Add dataset and healing nodes
                    dataset = lineage_data.get("dataset")
                    table = lineage_data.get("table")
                    healing_id = lineage_data.get("healing_id")
                    
                    if dataset and table and healing_id:
                        dataset_node = f"dataset:{dataset}.{table}"
                        healing_node = f"healing:{healing_id}"
                        result_node = f"dataset:{dataset}.{table}:healed"
                        
                        graph.add_node(dataset_node, type="dataset")
                        graph.add_node(healing_node, type="healing")
                        graph.add_node(result_node, type="dataset")
                        
                        # Add edges to represent healing flow
                        graph.add_edge(
                            dataset_node, 
                            healing_node, 
                            operation="healing_input",
                            timestamp=record.get("timestamp")
                        )
                        
                        graph.add_edge(
                            healing_node, 
                            result_node, 
                            operation="healing_output",
                            action_type=lineage_data.get("action_type"),
                            timestamp=record.get("timestamp"),
                            details=lineage_data.get("action_details", {})
                        )
            
            # Set the graph as instance variable
            self._lineage_graph = graph
            
            logger.info(f"Built lineage graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges")
            return graph
        
        except Exception as e:
            logger.error(f"Error building lineage graph: {e}")
            return graph
    
    def _store_lineage_record(self, lineage_record: Dict[str, Any]) -> bool:
        """Internal method to store a lineage record in the appropriate storage.
        
        Args:
            lineage_record: The lineage record to store
            
        Returns:
            True if storage successful, False otherwise
        """
        try:
            # Store in Firestore
            record_id = lineage_record.get("lineage_id")
            result = self._firestore_client.set_document(
                self._lineage_collection, 
                record_id, 
                lineage_record
            )
            
            # If BigQuery storage is enabled, also store there
            if self._enable_bigquery_storage and self._bigquery_client:
                # Convert to BigQuery format if needed
                bq_record = {
                    "lineage_id": record_id,
                    "record_type": lineage_record.get("record_type"),
                    "timestamp": lineage_record.get("timestamp"),
                    "lineage_data": json.dumps(lineage_record.get("lineage_data", {})),
                    "system_info": json.dumps(lineage_record.get("system_info", {}))
                }
                
                dataset_id = get_config().get_bigquery_dataset()
                bq_result = self._bigquery_client.insert_rows(
                    dataset_id,
                    self._lineage_table,
                    [bq_record]
                )
                
                # Log if BigQuery insertion failed
                if not bq_result:
                    logger.warning(f"Failed to store lineage record {record_id} in BigQuery")
            
            logger.debug(f"Stored lineage record: {record_id}")
            return result
        
        except Exception as e:
            logger.error(f"Error storing lineage record: {e}")
            return False
    
    def _create_lineage_record(self, record_id: str, record_type: str, 
                              lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to create a standardized lineage record.
        
        Args:
            record_id: Unique identifier for the record
            record_type: Type of lineage record
            lineage_data: Lineage information
            
        Returns:
            Formatted lineage record
        """
        # Create base record
        record = {
            "lineage_id": record_id,
            "record_type": record_type,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "lineage_data": lineage_data
        }
        
        # Add system information
        record["system_info"] = {
            "environment": get_config().get_environment(),
            "project_id": get_config().get_gcp_project_id(),
            "created_by": "lineage_tracker",
            "version": "1.0"
        }
        
        return record


class LineageQuery:
    """Utility class for querying and analyzing data lineage.
    
    This class provides higher-level querying capabilities on top of the LineageTracker,
    enabling complex lineage analysis, reporting, and visualization.
    """
    
    def __init__(self, lineage_tracker: LineageTracker):
        """Initialize the LineageQuery with a LineageTracker instance.
        
        Args:
            lineage_tracker: The LineageTracker instance to use for queries
        """
        self._lineage_tracker = lineage_tracker
        self._bigquery_client = BigQueryClient()
    
    def find_data_sources(self, dataset: str, table: str) -> List[Dict[str, Any]]:
        """Finds all original data sources for a dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            
        Returns:
            List of source systems and datasets
        """
        # Get upstream lineage for the dataset
        lineage = self._lineage_tracker.get_dataset_lineage(
            dataset, 
            table, 
            upstream=True, 
            downstream=False
        )
        
        # Extract source systems
        sources = []
        for upstream in lineage.get("upstream", []):
            if upstream.get("type") == "source":
                sources.append({
                    "source_id": upstream.get("source_id"),
                    "source_type": upstream.get("details", {}).get("source_type"),
                    "extraction_time": upstream.get("timestamp")
                })
        
        return sources
    
    def find_dependent_datasets(self, dataset: str, table: str) -> List[Dict[str, Any]]:
        """Finds all datasets dependent on a source dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            
        Returns:
            List of dependent datasets
        """
        # Get impact analysis
        impact = self._lineage_tracker.analyze_impact(dataset, table)
        
        return impact.get("impact", {}).get("affected_datasets", [])
    
    def analyze_transformation_chain(self, dataset: str, table: str) -> List[Dict[str, Any]]:
        """Analyzes the chain of transformations for a dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            
        Returns:
            Ordered list of transformations
        """
        # Get complete lineage
        lineage = self._lineage_tracker.get_dataset_lineage(
            dataset, 
            table, 
            upstream=True, 
            downstream=False
        )
        
        # Extract transformations
        transformations = []
        for upstream in lineage.get("upstream", []):
            if upstream.get("operation") == "transformation":
                transformations.append({
                    "source": upstream.get("dataset", "") + "." + upstream.get("table", ""),
                    "transformation_type": upstream.get("details", {}).get("transformation_type", "unknown"),
                    "timestamp": upstream.get("timestamp"),
                    "details": upstream.get("details", {})
                })
        
        # Sort by timestamp
        transformations.sort(key=lambda x: x.get("timestamp", ""))
        
        return transformations
    
    def find_common_ancestor(self, dataset1: str, table1: str, 
                            dataset2: str, table2: str) -> Dict[str, Any]:
        """Finds the common ancestor dataset for two datasets.
        
        Args:
            dataset1: First dataset name
            table1: First table name
            dataset2: Second dataset name
            table2: Second table name
            
        Returns:
            Common ancestor dataset information
        """
        # Get upstream lineage for both datasets
        lineage1 = self._lineage_tracker.get_dataset_lineage(
            dataset1, 
            table1, 
            upstream=True, 
            downstream=False
        )
        
        lineage2 = self._lineage_tracker.get_dataset_lineage(
            dataset2, 
            table2, 
            upstream=True, 
            downstream=False
        )
        
        # Extract upstream datasets
        upstream_datasets1 = {}
        for upstream in lineage1.get("upstream", []):
            if upstream.get("type") == "dataset":
                key = upstream.get("dataset", "") + "." + upstream.get("table", "")
                upstream_datasets1[key] = upstream
        
        upstream_datasets2 = {}
        for upstream in lineage2.get("upstream", []):
            if upstream.get("type") == "dataset":
                key = upstream.get("dataset", "") + "." + upstream.get("table", "")
                upstream_datasets2[key] = upstream
        
        # Find common datasets
        common_datasets = set(upstream_datasets1.keys()) & set(upstream_datasets2.keys())
        
        if not common_datasets:
            return {
                "found": False,
                "common_ancestor": None
            }
        
        # Find the most recent common ancestor based on timestamp
        most_recent = None
        most_recent_time = None
        
        for dataset_key in common_datasets:
            upstream1 = upstream_datasets1[dataset_key]
            timestamp = upstream1.get("timestamp")
            
            if most_recent_time is None or timestamp > most_recent_time:
                most_recent = upstream1
                most_recent_time = timestamp
        
        if most_recent:
            return {
                "found": True,
                "common_ancestor": {
                    "dataset": most_recent.get("dataset"),
                    "table": most_recent.get("table"),
                    "timestamp": most_recent.get("timestamp")
                }
            }
        
        return {
            "found": False,
            "common_ancestor": None
        }
    
    def generate_lineage_report(self, dataset: str, table: str, 
                               report_type: str = "full") -> Dict[str, Any]:
        """Generates a comprehensive lineage report for analysis.
        
        Args:
            dataset: Dataset name
            table: Table name
            report_type: Type of report (full, upstream, downstream, impact)
            
        Returns:
            Lineage report data structure
        """
        report = {
            "dataset": dataset,
            "table": table,
            "report_type": report_type,
            "generated_at": datetime.datetime.utcnow().isoformat()
        }
        
        if report_type == "upstream" or report_type == "full":
            # Get upstream lineage
            upstream_lineage = self._lineage_tracker.get_dataset_lineage(
                dataset, 
                table, 
                upstream=True, 
                downstream=False
            )
            report["upstream_lineage"] = upstream_lineage
            
            # Find original sources
            sources = self.find_data_sources(dataset, table)
            report["data_sources"] = sources
            
            # Analyze transformation chain
            transformations = self.analyze_transformation_chain(dataset, table)
            report["transformation_chain"] = transformations
        
        if report_type == "downstream" or report_type == "full" or report_type == "impact":
            # Get impact analysis
            impact = self._lineage_tracker.analyze_impact(dataset, table)
            report["impact_analysis"] = impact
            
            if report_type != "impact":
                # Get downstream lineage
                downstream_lineage = self._lineage_tracker.get_dataset_lineage(
                    dataset, 
                    table, 
                    upstream=False, 
                    downstream=True
                )
                report["downstream_lineage"] = downstream_lineage
        
        # Add visualization
        if report_type == "full":
            report["visualization"] = self._lineage_tracker.visualize_lineage(
                dataset, 
                table, 
                format="json"
            )
        
        return report
    
    def analyze_column_lineage(self, dataset: str, table: str, 
                              columns: List[str]) -> Dict[str, Any]:
        """Analyzes lineage at the column level for a dataset.
        
        Args:
            dataset: Dataset name
            table: Table name
            columns: List of column names to analyze
            
        Returns:
            Column-level lineage information
        """
        result = {
            "dataset": dataset,
            "table": table,
            "columns": {},
            "found": False
        }
        
        # Get complete lineage
        lineage = self._lineage_tracker.get_dataset_lineage(
            dataset, 
            table, 
            upstream=True, 
            downstream=False
        )
        
        if not lineage.get("found", False):
            return result
        
        result["found"] = True
        
        # Analyze each column
        for column in columns:
            column_info = {
                "transformations": [],
                "sources": []
            }
            
            # Look for transformations affecting this column
            for upstream in lineage.get("upstream", []):
                if upstream.get("operation") == "transformation":
                    details = upstream.get("details", {})
                    
                    # Check if transformation mentions this column
                    if "columns" in details and column in details["columns"]:
                        column_info["transformations"].append({
                            "source": upstream.get("dataset", "") + "." + upstream.get("table", ""),
                            "transformation_type": details.get("transformation_type", "unknown"),
                            "column_transformation": details.get("column_transformations", {}).get(column, "unknown")
                        })
                    # Or if transformation mentions this column in any way
                    elif column in str(details):
                        column_info["transformations"].append({
                            "source": upstream.get("dataset", "") + "." + upstream.get("table", ""),
                            "transformation_type": details.get("transformation_type", "unknown"),
                            "details": details
                        })
            
            # Try to identify source columns
            if column_info["transformations"]:
                # If we have transformation info, we might be able to trace back to source columns
                for transform in column_info["transformations"]:
                    source_dataset, source_table = transform.get("source", ".").split(".")
                    
                    if source_dataset and source_table:
                        # This is a simplification as proper column-level lineage
                        # would require detailed metadata about column mappings
                        column_info["sources"].append({
                            "dataset": source_dataset,
                            "table": source_table,
                            "possible_source_column": column  # Assume same name as simplification
                        })
            
            result["columns"][column] = column_info
        
        return result