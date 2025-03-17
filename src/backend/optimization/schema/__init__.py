"""
Initialization module for the schema optimization package that exports key classes and functions for BigQuery table optimization, including partitioning, clustering, and schema structure improvements to enhance query performance and reduce costs.
"""

__version__ = "0.1.0"

from .clustering_optimizer import ClusteringOptimizer
from .partitioning_optimizer import PartitioningOptimizer, PartitioningStrategy
from .schema_analyzer import SchemaAnalyzer, SchemaOptimizationRecommendation
from .table_designer import TableDesigner, TableDesignRecommendation
from .table_designer import analyze_column_usage
from .table_designer import analyze_partition_distribution
from .table_designer import estimate_combined_optimization_impact
from .table_designer import generate_clustering_ddl
from .table_designer import generate_partition_ddl
from .table_designer import generate_schema_optimization_ddl

__all__ = [
    'ClusteringOptimizer',
    'PartitioningOptimizer',
    'PartitioningStrategy',
    'SchemaAnalyzer',
    'SchemaOptimizationRecommendation',
    'TableDesigner',
    'TableDesignRecommendation',
    'analyze_column_usage',
    'analyze_partition_distribution',
    'estimate_combined_optimization_impact',
    'generate_clustering_ddl',
    'generate_partition_ddl',
    'generate_schema_optimization_ddl'
]