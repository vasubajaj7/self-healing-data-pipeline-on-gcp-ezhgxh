"""
Analyzes BigQuery SQL queries to identify optimization opportunities, extract query characteristics, 
and provide recommendations for performance improvements. This component is a core part of the 
query optimization framework that enables cost reduction and performance enhancement.
"""

import re
import json
import datetime
import hashlib
import sqlparse
from typing import Dict, List, Tuple, Set, Optional, Any, Union

# Internal imports
from ...constants import METRIC_TYPE_HISTOGRAM
from ...settings import BIGQUERY_DATASET
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.logging.logger import Logger
from .pattern_identifier import PatternIdentifier

# Set up module logger
logger = Logger(__name__)

# Define constants for query history table
QUERY_HISTORY_TABLE = f"{BIGQUERY_DATASET}.query_history"

# Define metrics for query complexity calculation
QUERY_COMPLEXITY_METRICS = [
    "table_count", 
    "join_count", 
    "subquery_count",
    "filter_count", 
    "aggregation_count",
    "bytes_processed"
]

# Define optimization categories with descriptions
OPTIMIZATION_CATEGORIES = {
    "PREDICATE_PUSHDOWN": "Move predicates closer to data sources",
    "JOIN_REORDERING": "Reorder joins for optimal performance",
    "SUBQUERY_FLATTENING": "Flatten unnecessary subqueries",
    "COLUMN_PRUNING": "Remove unused columns",
    "AGGREGATION_OPTIMIZATION": "Optimize aggregation operations",
    "CTE_CONVERSION": "Convert subqueries to CTEs"
}


def parse_query(query: str) -> sqlparse.sql.Statement:
    """
    Parses a SQL query using sqlparse to extract its structure.
    
    Args:
        query: SQL query to parse
        
    Returns:
        Parsed SQL statement
    """
    try:
        # Format the query to normalize whitespace and case for better parsing
        formatted = sqlparse.format(
            query,
            keyword_case='upper',
            identifier_case='lower',
            strip_comments=True,
            reindent=True
        )
        
        # Parse the query
        parsed = sqlparse.parse(formatted)
        if not parsed:
            logger.warning("Failed to parse query: empty result")
            return None
            
        # Return the first statement (most queries have a single statement)
        return parsed[0]
    except Exception as e:
        logger.error(f"Error parsing query: {str(e)}")
        return None


def extract_tables_from_query(query: str) -> List[str]:
    """
    Extracts table references from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of table references
    """
    tables = []
    
    try:
        # Parse the query
        parsed = parse_query(query)
        if not parsed:
            return []
            
        # Traverse the parse tree to find FROM and JOIN clauses
        from_seen = False
        table_tokens = []
        
        for token in parsed.tokens:
            # Identify FROM clauses
            if token.is_keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
                
            # Collect identifiers after FROM
            if from_seen and not token.is_whitespace:
                if hasattr(token, 'tokens'):
                    # This is a token group, extract table references
                    for subtoken in token.tokens:
                        if hasattr(subtoken, 'get_real_name'):
                            tables.append(subtoken.get_real_name())
                        elif not subtoken.is_whitespace and not subtoken.is_keyword:
                            # Try to extract table name from token value
                            token_val = subtoken.value.strip()
                            if token_val and not token_val.startswith('('):
                                # Handle schema.table format
                                parts = token_val.split('.')
                                table_name = parts[-1].split(' ')[0]  # Handle aliases
                                tables.append(table_name)
                                
                # Reset from_seen flag after processing the FROM clause
                from_seen = False
            
            # Identify JOIN clauses
            if token.is_keyword and 'JOIN' in token.value.upper():
                # Find the next token which should be the table name
                join_idx = parsed.tokens.index(token)
                if join_idx + 1 < len(parsed.tokens):
                    join_token = parsed.tokens[join_idx + 1]
                    if hasattr(join_token, 'get_real_name'):
                        tables.append(join_token.get_real_name())
                    elif not join_token.is_whitespace:
                        # Extract table name from token value
                        token_val = join_token.value.strip()
                        if token_val:
                            # Handle schema.table format and aliases
                            parts = token_val.split('.')
                            table_name = parts[-1].split(' ')[0]
                            tables.append(table_name)
        
        # Remove duplicates and return
        return list(set(tables))
    except Exception as e:
        logger.error(f"Error extracting tables: {str(e)}")
        return []


def extract_join_conditions(query: str) -> List[Dict]:
    """
    Extracts join conditions from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of join conditions
    """
    join_conditions = []
    
    try:
        # Parse the query
        parsed = parse_query(query)
        if not parsed:
            return []
            
        # Traverse the parse tree to find JOIN...ON clauses
        in_join = False
        current_join = None
        
        for token in parsed.tokens:
            # Identify JOIN statements
            if token.is_keyword and 'JOIN' in token.value.upper():
                in_join = True
                current_join = {
                    'type': token.value.upper(),
                    'table': None,
                    'condition': None
                }
                continue
                
            # Extract table being joined
            if in_join and current_join['table'] is None and not token.is_whitespace:
                if hasattr(token, 'get_real_name'):
                    current_join['table'] = token.get_real_name()
                else:
                    # Extract table from token value
                    token_val = token.value.strip()
                    if token_val:
                        parts = token_val.split('.')
                        table_name = parts[-1].split(' ')[0]
                        current_join['table'] = table_name
                continue
                
            # Extract ON condition
            if in_join and token.is_keyword and token.value.upper() == 'ON':
                # Find the expression that follows the ON keyword
                on_idx = parsed.tokens.index(token)
                if on_idx + 1 < len(parsed.tokens):
                    on_condition = parsed.tokens[on_idx + 1]
                    if hasattr(on_condition, 'value'):
                        current_join['condition'] = on_condition.value.strip()
                        
                # Add the complete join condition to our list
                if current_join['table'] and current_join['condition']:
                    join_conditions.append(current_join)
                    
                # Reset for next join
                in_join = False
                current_join = None
                
        return join_conditions
    except Exception as e:
        logger.error(f"Error extracting join conditions: {str(e)}")
        return []


def extract_where_conditions(query: str) -> List[str]:
    """
    Extracts WHERE clause conditions from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of WHERE conditions
    """
    conditions = []
    
    try:
        # Parse the query
        parsed = parse_query(query)
        if not parsed:
            return []
            
        # Find WHERE clause
        where_clause = None
        for token in parsed.tokens:
            if isinstance(token, sqlparse.sql.Where):
                where_clause = token
                break
                
        if not where_clause:
            return []
            
        # Extract conditions from WHERE clause
        where_conditions = []
        for token in where_clause.tokens:
            if not token.is_whitespace and token.value.upper() != 'WHERE':
                where_conditions.append(token.value)
                
        # Join and split conditions by AND/OR operators
        condition_text = ' '.join(where_conditions)
        
        # Simple split by AND (not ideal for complex conditions, but a starting point)
        if ' AND ' in condition_text.upper():
            raw_conditions = condition_text.split(' AND ')
            for cond in raw_conditions:
                conditions.append(cond.strip())
        elif ' OR ' in condition_text.upper():
            raw_conditions = condition_text.split(' OR ')
            for cond in raw_conditions:
                conditions.append(cond.strip())
        else:
            conditions.append(condition_text.strip())
            
        return conditions
    except Exception as e:
        logger.error(f"Error extracting WHERE conditions: {str(e)}")
        return []


def extract_aggregations(query: str) -> Dict[str, Any]:
    """
    Extracts aggregation functions and GROUP BY clauses from a query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        Dictionary of aggregation details
    """
    result = {
        'aggregation_functions': [],
        'group_by_columns': [],
        'having_conditions': []
    }
    
    try:
        # Parse the query
        parsed = parse_query(query)
        if not parsed:
            return result
            
        # Extract aggregation functions from SELECT clause
        select_seen = False
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() == 'SELECT':
                select_seen = True
                continue
                
            if select_seen and token.ttype is None:  # This should be the columns list
                # Look for aggregation functions
                agg_functions = re.findall(r'(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE)\s*\(', 
                                           token.value.upper())
                result['aggregation_functions'] = list(set(agg_functions))
                break
                
        # Find GROUP BY clause
        group_seen = False
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() == 'GROUP BY':
                group_seen = True
                continue
                
            if group_seen and token.ttype is None:  # This should be the group by columns
                # Extract column names from GROUP BY
                columns = [col.strip() for col in token.value.split(',')]
                result['group_by_columns'] = columns
                break
                
        # Find HAVING clause
        having_seen = False
        for token in parsed.tokens:
            if token.is_keyword and token.value.upper() == 'HAVING':
                having_seen = True
                continue
                
            if having_seen and token.ttype is None:  # This should be the having conditions
                result['having_conditions'] = [token.value.strip()]
                break
                
        return result
    except Exception as e:
        logger.error(f"Error extracting aggregations: {str(e)}")
        return result


def extract_subqueries(query: str) -> List[Dict[str, Any]]:
    """
    Extracts subqueries from a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        List of subqueries with context
    """
    subqueries = []
    
    try:
        # Parse the query using regular expressions for initial detection
        # This is a simplified approach; a full parser would be more accurate
        # but more complex
        
        # Look for SELECT statements inside parentheses
        subquery_pattern = r'\(\s*(SELECT\s+.+?)\s*\)'
        for match in re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL):
            # Extract the subquery text
            subquery_text = match.group(1)
            
            # Determine context by looking at tokens before the subquery
            context = "UNKNOWN"
            pre_text = query[:match.start()].strip().upper()
            
            if re.search(r'FROM\s*$', pre_text):
                context = "FROM"
            elif re.search(r'JOIN\s*$', pre_text):
                context = "JOIN"
            elif re.search(r'WHERE\s*$', pre_text) or re.search(r'AND\s*$', pre_text) or re.search(r'OR\s*$', pre_text):
                context = "WHERE"
            elif re.search(r'SELECT\s*$', pre_text) or ',' in pre_text[-5:]:
                context = "SELECT"
                
            # Add to results
            subqueries.append({
                'text': subquery_text,
                'context': context,
                'position': match.start()
            })
            
        return subqueries
    except Exception as e:
        logger.error(f"Error extracting subqueries: {str(e)}")
        return []


def analyze_query_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyzes a BigQuery query execution plan to identify optimization opportunities.
    
    Args:
        plan: Query execution plan from BigQuery
        
    Returns:
        Analysis of the query plan with optimization suggestions
    """
    analysis = {
        'stages': [],
        'bottlenecks': [],
        'optimization_opportunities': [],
        'metrics': {
            'total_slots': 0,
            'max_stage_duration': 0,
            'total_bytes_processed': 0,
            'shuffle_bytes': 0
        }
    }
    
    if not plan or not isinstance(plan, dict):
        logger.warning("Invalid query plan provided for analysis")
        return analysis
        
    try:
        # Extract plan stages
        stages = plan.get('queryPlan', [])
        if not stages:
            # Try alternative field name
            stages = plan.get('executionPlan', [])
            if not stages:
                logger.warning("No query plan stages found in the plan")
                return analysis
                
        # Analyze each stage
        total_slots = 0
        max_duration = 0
        total_bytes = 0
        shuffle_bytes = 0
        
        for stage in stages:
            stage_id = stage.get('name', 'unknown')
            stage_info = {
                'id': stage_id,
                'operation': stage.get('steps', [{}])[0].get('kind', 'unknown'),
                'duration_ms': stage.get('executionStats', {}).get('endTime', 0) - 
                               stage.get('executionStats', {}).get('startTime', 0),
                'input_rows': stage.get('inputRows', 0),
                'output_rows': stage.get('outputRows', 0),
                'input_bytes': stage.get('inputBytes', 0),
                'output_bytes': stage.get('outputBytes', 0),
                'shuffle_bytes': stage.get('shuffleOutputBytes', 0)
            }
            
            # Update metrics
            total_slots += stage.get('slotMs', 0)
            max_duration = max(max_duration, stage_info['duration_ms'])
            total_bytes += stage_info['input_bytes']
            shuffle_bytes += stage_info['shuffle_bytes']
            
            # Check for bottlenecks
            input_output_ratio = (stage_info['input_rows'] / stage_info['output_rows'] 
                                 if stage_info['output_rows'] > 0 else 0)
            
            if input_output_ratio > 10:
                # High input/output ratio suggests inefficient filtering
                analysis['bottlenecks'].append({
                    'stage_id': stage_id,
                    'type': 'HIGH_FILTERING_RATIO',
                    'description': f"Stage {stage_id} processes {stage_info['input_rows']} rows but outputs only {stage_info['output_rows']} rows",
                    'suggestion': "Consider pushing filters earlier in the query or reviewing join conditions"
                })
                
            if stage_info['shuffle_bytes'] > 100 * 1024 * 1024:  # 100 MB
                # Large shuffle operations are expensive
                analysis['bottlenecks'].append({
                    'stage_id': stage_id,
                    'type': 'EXPENSIVE_SHUFFLE',
                    'description': f"Stage {stage_id} has large shuffle operation ({stage_info['shuffle_bytes'] / (1024*1024):.2f} MB)",
                    'suggestion': "Review join order and consider partitioning strategies"
                })
                
            # Add stage to analysis
            analysis['stages'].append(stage_info)
            
        # Update aggregate metrics
        analysis['metrics']['total_slots'] = total_slots
        analysis['metrics']['max_stage_duration'] = max_duration
        analysis['metrics']['total_bytes_processed'] = total_bytes
        analysis['metrics']['shuffle_bytes'] = shuffle_bytes
        
        # Generate optimization opportunities
        if shuffle_bytes > 1024 * 1024 * 1024:  # 1 GB
            analysis['optimization_opportunities'].append({
                'type': 'JOIN_REORDERING',
                'description': "Large shuffle operations detected",
                'suggestion': "Reorder joins to process smaller tables first and reduce data shuffling"
            })
            
        if total_bytes > 10 * 1024 * 1024 * 1024:  # 10 GB
            analysis['optimization_opportunities'].append({
                'type': 'PREDICATE_PUSHDOWN',
                'description': "Large amount of data processed",
                'suggestion': "Add or improve filters to reduce data scanned"
            })
            
        # Look for full table scans
        for stage in analysis['stages']:
            if 'SCAN' in stage['operation'] and stage['input_rows'] > 1000000:
                analysis['optimization_opportunities'].append({
                    'type': 'TABLE_PARTITIONING',
                    'description': f"Large table scan in stage {stage['id']}",
                    'suggestion': "Consider partitioning tables and adding partition filters"
                })
                break
                
        return analysis
    except Exception as e:
        logger.error(f"Error analyzing query plan: {str(e)}")
        return analysis


def calculate_query_complexity(query: str) -> Dict[str, Any]:
    """
    Calculates complexity metrics for a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        Dictionary of complexity metrics
    """
    metrics = {
        'table_count': 0,
        'join_count': 0,
        'subquery_count': 0,
        'filter_count': 0,
        'aggregation_count': 0,
        'complexity_score': 0
    }
    
    try:
        # Count tables
        tables = extract_tables_from_query(query)
        metrics['table_count'] = len(tables)
        
        # Count joins
        join_conditions = extract_join_conditions(query)
        metrics['join_count'] = len(join_conditions)
        
        # Count subqueries
        subqueries = extract_subqueries(query)
        metrics['subquery_count'] = len(subqueries)
        
        # Count filters
        where_conditions = extract_where_conditions(query)
        metrics['filter_count'] = len(where_conditions)
        
        # Count aggregations
        aggregations = extract_aggregations(query)
        metrics['aggregation_count'] = len(aggregations.get('aggregation_functions', []))
        
        # Calculate complexity score
        # Simple weighted sum of metrics
        metrics['complexity_score'] = (
            metrics['table_count'] * 1 +
            metrics['join_count'] * 2 +
            metrics['subquery_count'] * 3 +
            metrics['filter_count'] * 1 +
            metrics['aggregation_count'] * 2
        )
        
        return metrics
    except Exception as e:
        logger.error(f"Error calculating query complexity: {str(e)}")
        return metrics


def get_query_history(query_hash: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Retrieves historical execution data for similar queries.
    
    Args:
        query_hash: Hash of the query structure
        limit: Maximum number of historical records to retrieve
        
    Returns:
        List of historical query executions
    """
    try:
        # Construct SQL query to retrieve history
        sql = f"""
        SELECT
            execution_id,
            query,
            execution_timestamp,
            duration_ms,
            bytes_processed,
            slot_ms,
            execution_status,
            error_message
        FROM
            `{QUERY_HISTORY_TABLE}`
        WHERE
            query_hash = @query_hash
        ORDER BY
            execution_timestamp DESC
        LIMIT
            @limit
        """
        
        # Define query parameters
        query_params = [
            {'name': 'query_hash', 'parameterType': {'type': 'STRING'}, 'parameterValue': {'value': query_hash}},
            {'name': 'limit', 'parameterType': {'type': 'INT64'}, 'parameterValue': {'value': limit}}
        ]
        
        # Execute query to retrieve history
        # This would be a call to BigQueryClient.execute_query
        # but for now we'll return an empty list since we can't execute without the client
        logger.debug(f"Retrieving history for query hash: {query_hash}")
        return []
    except Exception as e:
        logger.error(f"Error retrieving query history: {str(e)}")
        return []


def generate_query_hash(query: str) -> str:
    """
    Generates a hash value for a query to identify similar queries.
    
    Args:
        query: SQL query to hash
        
    Returns:
        Hash value representing the query structure
    """
    try:
        # Normalize query to focus on structure
        normalized_query = re.sub(r'\s+', ' ', query.strip())
        
        # Remove literal values to focus on query structure
        normalized_query = re.sub(r"'[^']*'", "''", normalized_query)  # Replace string literals
        normalized_query = re.sub(r'\b\d+\b', '0', normalized_query)   # Replace numeric literals
        
        # Generate SHA-256 hash
        query_hash = hashlib.sha256(normalized_query.encode('utf-8')).hexdigest()
        return query_hash
    except Exception as e:
        logger.error(f"Error generating query hash: {str(e)}")
        return hashlib.sha256(query.encode('utf-8')).hexdigest()  # Fallback


def store_query_analysis(query: str, analysis: Dict[str, Any]) -> bool:
    """
    Stores query analysis results for future reference.
    
    Args:
        query: SQL query that was analyzed
        analysis: Analysis results
        
    Returns:
        Success status
    """
    try:
        # Generate query hash
        query_hash = generate_query_hash(query)
        
        # Prepare record for storage
        record = {
            'query_hash': query_hash,
            'query': query,
            'analysis_timestamp': datetime.datetime.now().isoformat(),
            'analysis_results': json.dumps(analysis),
            'complexity_score': analysis.get('complexity_metrics', {}).get('complexity_score', 0)
        }
        
        # This would store the record in a database/BigQuery table
        # For now we'll just log it
        logger.info(f"Stored analysis for query hash: {query_hash}")
        return True
    except Exception as e:
        logger.error(f"Error storing query analysis: {str(e)}")
        return False


def determine_query_type(query: str) -> str:
    """
    Determines the type of a SQL query.
    
    Args:
        query: SQL query to analyze
        
    Returns:
        Query type (SELECT, INSERT, UPDATE, etc.)
    """
    # Normalize whitespace and case
    normalized = re.sub(r'\s+', ' ', query.strip()).upper()
    
    # Determine query type based on first keyword
    if normalized.startswith('SELECT'):
        return 'SELECT'
    elif normalized.startswith('INSERT'):
        return 'INSERT'
    elif normalized.startswith('UPDATE'):
        return 'UPDATE'
    elif normalized.startswith('DELETE'):
        return 'DELETE'
    elif normalized.startswith('CREATE'):
        return 'CREATE'
    elif normalized.startswith('DROP'):
        return 'DROP'
    elif normalized.startswith('ALTER'):
        return 'ALTER'
    elif normalized.startswith('MERGE'):
        return 'MERGE'
    else:
        return 'UNKNOWN'


def extract_relevant_snippet(query: str, pattern: Dict[str, Any]) -> str:
    """
    Extracts a relevant snippet from a query based on a pattern.
    
    Args:
        query: The SQL query
        pattern: Pattern information
        
    Returns:
        Relevant query snippet
    """
    # This is a simplified implementation
    # A real implementation would use pattern info to extract relevant parts
    
    # Default to returning a portion of the query
    if not query:
        return ""
        
    # If pattern has position information, use it
    if 'position' in pattern:
        pos = pattern['position']
        # Extract a window around the position
        start = max(0, pos - 100)
        end = min(len(query), pos + 100)
        return query[start:end]
        
    # Fall back to returning the first part of the query
    max_len = min(200, len(query))
    return query[:max_len]


def generate_optimized_snippet(query: str, pattern: Dict[str, Any]) -> str:
    """
    Generates an optimized version of a query snippet based on a pattern.
    
    Args:
        query: The SQL query
        pattern: Pattern information
        
    Returns:
        Optimized query snippet
    """
    # This would implement actual query rewriting based on the pattern
    # For now, we'll return a placeholder
    
    pattern_id = pattern.get('pattern_id', '')
    optimization_type = pattern.get('optimization_type', '')
    
    # This is a simplified implementation
    # In a real system, we would apply actual transformations
    if not query:
        return ""
        
    if pattern_id == 'CARTESIAN_JOIN':
        # Example of adding a join condition
        return "-- Optimized version would add explicit join conditions\n" + extract_relevant_snippet(query, pattern)
        
    if pattern_id == 'UNNECESSARY_COLUMNS':
        # Example of replacing SELECT * with specific columns
        return "-- Optimized version would replace SELECT * with specific columns\n" + extract_relevant_snippet(query, pattern)
        
    if optimization_type == 'PREDICATE_PUSHDOWN':
        # Example of pushing down predicates
        return "-- Optimized version would move filter conditions to appropriate location\n" + extract_relevant_snippet(query, pattern)
        
    # Default case
    return "-- Optimized version would apply " + optimization_type.replace('_', ' ').lower() + "\n" + extract_relevant_snippet(query, pattern)


def estimate_improvement(pattern: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estimates the improvement from applying an optimization.
    
    Args:
        pattern: Pattern information
        
    Returns:
        Estimated improvement details
    """
    # Map pattern types to estimated improvements
    # These are rough estimates based on typical improvements
    impact_estimates = {
        'CARTESIAN_JOIN': {'percentage': 70, 'confidence': 'high'},
        'MISSING_JOIN_PREDICATE': {'percentage': 60, 'confidence': 'high'},
        'SUBOPTIMAL_PREDICATE_PLACEMENT': {'percentage': 40, 'confidence': 'medium'},
        'UNNECESSARY_COLUMNS': {'percentage': 30, 'confidence': 'medium'},
        'NESTED_SUBQUERIES': {'percentage': 35, 'confidence': 'medium'},
        'INEFFICIENT_AGGREGATION': {'percentage': 25, 'confidence': 'medium'},
        'REPEATED_SUBQUERIES': {'percentage': 20, 'confidence': 'medium'},
        'UNPARTITIONED_TABLE_SCAN': {'percentage': 80, 'confidence': 'high'}
    }
    
    pattern_id = pattern.get('pattern_id', '')
    estimate = impact_estimates.get(pattern_id, {'percentage': 10, 'confidence': 'low'})
    
    # Adjust based on impact score if available
    if 'impact_score' in pattern:
        score = pattern['impact_score']
        estimate['percentage'] = min(90, estimate['percentage'] * score / 5.0)
    
    # Add metrics that would be improved
    improved_metrics = ['execution_time', 'bytes_processed']
    if pattern.get('optimization_type') == 'PREDICATE_PUSHDOWN':
        improved_metrics.append('bytes_read')
    if pattern.get('optimization_type') == 'JOIN_REORDERING':
        improved_metrics.append('shuffle_bytes')
        
    estimate['metrics'] = improved_metrics
    
    return estimate


class QueryAnalyzer:
    """
    Analyzes BigQuery SQL queries to identify optimization opportunities.
    """
    
    def __init__(self, bq_client: BigQueryClient):
        """
        Initializes the QueryAnalyzer with BigQuery client.
        
        Args:
            bq_client: BigQuery client for executing queries
        """
        self._bq_client = bq_client
        self._pattern_identifier = PatternIdentifier(bq_client)
        self._analysis_cache = {}  # Cache analysis results
        
        logger.info("QueryAnalyzer initialized")
        
    def analyze_query(self, query: str, use_cache: bool = True, get_plan: bool = True) -> Dict[str, Any]:
        """
        Performs comprehensive analysis of a SQL query.
        
        Args:
            query: SQL query to analyze
            use_cache: Whether to use cached results if available
            get_plan: Whether to retrieve and analyze execution plan
            
        Returns:
            Comprehensive query analysis
        """
        if not query:
            logger.warning("Empty query provided for analysis")
            return {}
            
        # Generate query hash for cache lookup
        query_hash = generate_query_hash(query)
        
        # Check cache if enabled
        if use_cache and query_hash in self._analysis_cache:
            logger.debug(f"Using cached analysis for query hash: {query_hash}")
            return self._analysis_cache[query_hash]
            
        logger.info(f"Analyzing query with hash: {query_hash}")
        
        # Analyze query structure
        structure_analysis = self.analyze_query_structure(query)
        
        # Calculate complexity metrics
        complexity_metrics = calculate_query_complexity(query)
        
        # Get and analyze query plan if requested
        plan_analysis = {}
        if get_plan:
            plan = self.get_query_plan(query)
            if plan:
                plan_analysis = analyze_query_plan(plan)
                
        # Identify patterns using PatternIdentifier
        patterns = self._pattern_identifier.identify_patterns(query, plan if get_plan else None)
        
        # Generate optimization recommendations
        recommendations = self.get_optimization_recommendations(query, {
            'structure_analysis': structure_analysis,
            'complexity_metrics': complexity_metrics,
            'plan_analysis': plan_analysis,
            'patterns': patterns
        })
        
        # Analyze historical performance
        historical_performance = self.analyze_historical_performance(query)
        
        # Combine all analysis components
        analysis = {
            'query_hash': query_hash,
            'structure_analysis': structure_analysis,
            'complexity_metrics': complexity_metrics,
            'plan_analysis': plan_analysis,
            'patterns': patterns,
            'recommendations': recommendations,
            'historical_performance': historical_performance
        }
        
        # Cache analysis if caching is enabled
        if use_cache:
            self._analysis_cache[query_hash] = analysis
            
        # Store analysis for future reference
        store_query_analysis(query, analysis)
        
        logger.info(f"Completed analysis for query hash: {query_hash}")
        return analysis
        
    def get_optimization_recommendations(self, query: str, analysis: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Generates optimization recommendations based on query analysis.
        
        Args:
            query: SQL query to analyze
            analysis: Existing analysis results, or None to generate new analysis
            
        Returns:
            List of optimization recommendations
        """
        # Generate analysis if not provided
        if not analysis:
            analysis = self.analyze_query(query)
            
        recommendations = []
        
        # Extract patterns from analysis
        patterns = analysis.get('patterns', {}).get('patterns', [])
        
        # Convert patterns to optimization recommendations
        for pattern in patterns:
            pattern_id = pattern.get('pattern_id')
            optimization_type = pattern.get('optimization_type')
            
            if not optimization_type:
                continue
                
            # Get description for this optimization type
            description = OPTIMIZATION_CATEGORIES.get(
                optimization_type, 
                f"Apply {optimization_type.replace('_', ' ').title()}"
            )
            
            # Create recommendation
            recommendation = OptimizationRecommendation(
                optimization_type=optimization_type,
                description=description,
                rationale=pattern.get('description', ''),
                impact_score=pattern.get('impact_score', 5.0),
                original_snippet=extract_relevant_snippet(query, pattern),
                optimized_snippet=generate_optimized_snippet(query, pattern),
                estimated_improvement=estimate_improvement(pattern)
            )
            
            recommendations.append(recommendation.to_dict())
            
        # Sort recommendations by impact score (highest first)
        recommendations.sort(key=lambda r: r.get('impact_score', 0), reverse=True)
        
        return recommendations
        
    def analyze_query_structure(self, query: str) -> Dict[str, Any]:
        """
        Analyzes the structure of a SQL query.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            Structural analysis of the query
        """
        # Extract various components of the query
        tables = extract_tables_from_query(query)
        join_conditions = extract_join_conditions(query)
        where_conditions = extract_where_conditions(query)
        aggregations = extract_aggregations(query)
        subqueries = extract_subqueries(query)
        
        # Combine into structural analysis
        structure_analysis = {
            'tables': tables,
            'join_conditions': join_conditions,
            'where_conditions': where_conditions,
            'aggregations': aggregations,
            'subqueries': subqueries,
            'query_type': determine_query_type(query)
        }
        
        return structure_analysis
        
    def get_query_plan(self, query: str) -> Dict[str, Any]:
        """
        Retrieves and analyzes the execution plan for a query.
        
        Args:
            query: SQL query to get execution plan for
            
        Returns:
            Query plan with analysis
        """
        try:
            # Get query plan using EXPLAIN statement
            explain_query = f"EXPLAIN {query}"
            plan_result = self._bq_client.execute_query(explain_query)
            
            if not plan_result or 'queryPlan' not in plan_result[0]:
                # Try alternative method for older BigQuery versions
                plan = self._bq_client.get_query_plan(query)
            else:
                plan = plan_result[0]
                
            logger.debug(f"Retrieved query plan for query")
            return plan
        except Exception as e:
            logger.error(f"Error retrieving query plan: {str(e)}")
            return {}
            
    def analyze_historical_performance(self, query: str, limit: int = 10) -> Dict[str, Any]:
        """
        Analyzes historical performance of similar queries.
        
        Args:
            query: SQL query to analyze
            limit: Maximum number of historical records to analyze
            
        Returns:
            Historical performance analysis
        """
        query_hash = generate_query_hash(query)
        history = get_query_history(query_hash, limit)
        
        if not history:
            return {
                'available': False,
                'message': 'No historical data available for this query'
            }
            
        # Calculate performance statistics
        durations = [entry.get('duration_ms', 0) for entry in history]
        bytes_processed = [entry.get('bytes_processed', 0) for entry in history]
        slot_ms = [entry.get('slot_ms', 0) for entry in history]
        
        # Calculate statistics
        avg_duration = sum(durations) / len(durations) if durations else 0
        min_duration = min(durations) if durations else 0
        max_duration = max(durations) if durations else 0
        
        avg_bytes = sum(bytes_processed) / len(bytes_processed) if bytes_processed else 0
        avg_slot_ms = sum(slot_ms) / len(slot_ms) if slot_ms else 0
        
        # Check for performance trends
        trend = "stable"
        if len(durations) > 3:
            # Simple trend detection - compare recent vs. older executions
            recent_avg = sum(durations[:3]) / 3
            older_avg = sum(durations[3:]) / (len(durations) - 3)
            
            if recent_avg > older_avg * 1.2:
                trend = "degrading"
            elif recent_avg < older_avg * 0.8:
                trend = "improving"
                
        return {
            'available': True,
            'execution_count': len(history),
            'statistics': {
                'avg_duration_ms': avg_duration,
                'min_duration_ms': min_duration,
                'max_duration_ms': max_duration,
                'avg_bytes_processed': avg_bytes,
                'avg_slot_ms': avg_slot_ms
            },
            'trend': trend,
            'executions': history[:5]  # Return the 5 most recent executions
        }
        
    def estimate_query_cost(self, query: str, analysis: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Estimates the cost of a query based on its characteristics.
        
        Args:
            query: SQL query to estimate cost for
            analysis: Existing analysis results, or None to generate new analysis
            
        Returns:
            Cost estimation details
        """
        # Generate analysis if not provided
        if not analysis:
            analysis = self.analyze_query(query)
            
        # Extract relevant metrics
        complexity = analysis.get('complexity_metrics', {})
        plan = analysis.get('plan_analysis', {})
        
        # Base cost factors
        table_count = complexity.get('table_count', 0)
        join_count = complexity.get('join_count', 0)
        bytes_processed = plan.get('metrics', {}).get('total_bytes_processed', 0)
        
        # If we have historical data, use that for better estimation
        historical = analysis.get('historical_performance', {})
        if historical.get('available', False):
            avg_bytes = historical.get('statistics', {}).get('avg_bytes_processed', 0)
            if avg_bytes > 0:
                bytes_processed = avg_bytes
                
        # Calculate estimated cost (simplified - in real world would use actual pricing)
        # BigQuery pricing is approximately $5 per TB processed
        cost_per_byte = 5.0 / (1024 * 1024 * 1024 * 1024)  # $5 per TB
        estimated_cost = bytes_processed * cost_per_byte
        
        # If we don't have bytes_processed, estimate based on complexity
        if bytes_processed == 0:
            # Very rough estimation based on query complexity
            base_bytes = 100 * 1024 * 1024  # 100 MB base
            estimated_bytes = base_bytes * (1 + table_count + join_count * 2)
            estimated_cost = estimated_bytes * cost_per_byte
            confidence = "low"
        else:
            confidence = "high" if historical.get('available', False) else "medium"
            
        return {
            'estimated_cost_usd': estimated_cost,
            'confidence': confidence,
            'estimated_bytes_processed': bytes_processed if bytes_processed > 0 else estimated_bytes,
            'cost_factors': {
                'table_count': table_count,
                'join_count': join_count,
                'complexity_score': complexity.get('complexity_score', 0)
            }
        }
        
    def clear_analysis_cache(self) -> None:
        """
        Clears the query analysis cache.
        """
        self._analysis_cache = {}
        logger.info("Analysis cache cleared")


class QueryAnalysisResult:
    """
    Represents the result of a query analysis.
    """
    
    def __init__(
        self, 
        query: str, 
        query_hash: str, 
        structure_analysis: Dict[str, Any], 
        complexity_metrics: Dict[str, Any], 
        plan_analysis: Dict[str, Any] = None, 
        patterns: List[Dict[str, Any]] = None, 
        recommendations: List[Dict[str, Any]] = None, 
        historical_performance: Dict[str, Any] = None
    ):
        """
        Initializes a QueryAnalysisResult with analysis data.
        
        Args:
            query: Original SQL query
            query_hash: Hash of the query
            structure_analysis: Analysis of query structure
            complexity_metrics: Query complexity metrics
            plan_analysis: Analysis of query execution plan
            patterns: Identified patterns in the query
            recommendations: Optimization recommendations
            historical_performance: Historical performance analysis
        """
        self.query = query
        self.query_hash = query_hash
        self.structure_analysis = structure_analysis
        self.complexity_metrics = complexity_metrics
        self.plan_analysis = plan_analysis or {}
        self.patterns = patterns or []
        self.recommendations = recommendations or []
        self.historical_performance = historical_performance or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the analysis result to a dictionary.
        
        Returns:
            Dictionary representation of the analysis result
        """
        return {
            'query': self.query,
            'query_hash': self.query_hash,
            'structure_analysis': self.structure_analysis,
            'complexity_metrics': self.complexity_metrics,
            'plan_analysis': self.plan_analysis,
            'patterns': self.patterns,
            'recommendations': self.recommendations,
            'historical_performance': self.historical_performance
        }
        
    @classmethod
    def from_dict(cls, analysis_dict: Dict[str, Any]) -> 'QueryAnalysisResult':
        """
        Creates QueryAnalysisResult from a dictionary.
        
        Args:
            analysis_dict: Dictionary with analysis data
            
        Returns:
            QueryAnalysisResult instance
        """
        return cls(
            query=analysis_dict.get('query', ''),
            query_hash=analysis_dict.get('query_hash', ''),
            structure_analysis=analysis_dict.get('structure_analysis', {}),
            complexity_metrics=analysis_dict.get('complexity_metrics', {}),
            plan_analysis=analysis_dict.get('plan_analysis', {}),
            patterns=analysis_dict.get('patterns', []),
            recommendations=analysis_dict.get('recommendations', []),
            historical_performance=analysis_dict.get('historical_performance', {})
        )
        
    def get_summary(self) -> Dict[str, Any]:
        """
        Generates a summary of the query analysis.
        
        Returns:
            Summary of key analysis findings
        """
        # Extract key metrics
        table_count = self.complexity_metrics.get('table_count', 0)
        join_count = self.complexity_metrics.get('join_count', 0)
        subquery_count = self.complexity_metrics.get('subquery_count', 0)
        complexity_score = self.complexity_metrics.get('complexity_score', 0)
        
        # Determine complexity level
        if complexity_score < 5:
            complexity_level = "Simple"
        elif complexity_score < 15:
            complexity_level = "Moderate"
        else:
            complexity_level = "Complex"
            
        # Extract top recommendations
        top_recommendations = sorted(
            self.recommendations, 
            key=lambda r: r.get('impact_score', 0),
            reverse=True
        )[:3]
        
        # Get estimated improvement
        total_improvement = sum(
            r.get('estimated_improvement', {}).get('percentage', 0) 
            for r in self.recommendations
        )
        
        return {
            'query_hash': self.query_hash,
            'complexity_level': complexity_level,
            'query_structure': {
                'tables': table_count,
                'joins': join_count,
                'subqueries': subquery_count
            },
            'optimization_potential': {
                'recommendation_count': len(self.recommendations),
                'top_recommendations': top_recommendations,
                'estimated_improvement': f"{total_improvement:.1f}%"
            },
            'historical_data': {
                'available': self.historical_performance.get('available', False),
                'trend': self.historical_performance.get('trend', 'unknown')
            }
        }


class OptimizationRecommendation:
    """
    Represents a specific optimization recommendation for a query.
    """
    
    def __init__(
        self, 
        optimization_type: str, 
        description: str, 
        rationale: str, 
        impact_score: float, 
        original_snippet: str = None, 
        optimized_snippet: str = None, 
        estimated_improvement: Dict[str, Any] = None
    ):
        """
        Initializes an OptimizationRecommendation.
        
        Args:
            optimization_type: Type of optimization
            description: Description of the recommendation
            rationale: Explanation of why this optimization helps
            impact_score: Score indicating the impact (0-10)
            original_snippet: Original query snippet
            optimized_snippet: Optimized query snippet
            estimated_improvement: Estimated performance improvement
        """
        self.optimization_type = optimization_type
        self.description = description
        self.rationale = rationale
        self.impact_score = impact_score
        self.original_snippet = original_snippet
        self.optimized_snippet = optimized_snippet
        self.estimated_improvement = estimated_improvement or {
            'percentage': 0,
            'confidence': 'low',
            'metrics': {}
        }
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the recommendation to a dictionary.
        
        Returns:
            Dictionary representation of the recommendation
        """
        return {
            'optimization_type': self.optimization_type,
            'description': self.description,
            'rationale': self.rationale,
            'impact_score': self.impact_score,
            'original_snippet': self.original_snippet,
            'optimized_snippet': self.optimized_snippet,
            'estimated_improvement': self.estimated_improvement
        }
        
    @classmethod
    def from_dict(cls, recommendation_dict: Dict[str, Any]) -> 'OptimizationRecommendation':
        """
        Creates OptimizationRecommendation from a dictionary.
        
        Args:
            recommendation_dict: Dictionary with recommendation data
            
        Returns:
            OptimizationRecommendation instance
        """
        return cls(
            optimization_type=recommendation_dict.get('optimization_type', ''),
            description=recommendation_dict.get('description', ''),
            rationale=recommendation_dict.get('rationale', ''),
            impact_score=recommendation_dict.get('impact_score', 0.0),
            original_snippet=recommendation_dict.get('original_snippet'),
            optimized_snippet=recommendation_dict.get('optimized_snippet'),
            estimated_improvement=recommendation_dict.get('estimated_improvement', {})
        )