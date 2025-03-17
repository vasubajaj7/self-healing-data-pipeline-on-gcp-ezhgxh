"""
Pattern identification module for BigQuery SQL queries.

This module analyzes SQL queries to identify patterns and anti-patterns that
can impact query performance. It provides recommendations for optimization
based on detected patterns and query execution plans.
"""

import re
import hashlib
import sqlparse
from typing import Dict, List, Tuple, Set, Optional, Any, Union

# Internal imports
from ...constants import OptimizationType
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.logging.logger import Logger

# Set up module logger
logger = Logger(__name__)

# Define global query pattern definitions
QUERY_PATTERN_DEFINITIONS = {
    "CARTESIAN_JOIN": {
        "pattern": r"SELECT.*FROM.*JOIN.*ON\s+1\s*=\s*1",
        "severity": "HIGH",
        "optimization_type": "JOIN_REORDERING"
    },
    "MISSING_JOIN_PREDICATE": {
        "pattern": r"SELECT.*FROM.*JOIN.*(?!ON)",
        "severity": "HIGH",
        "optimization_type": "JOIN_REORDERING"
    },
    "SUBOPTIMAL_PREDICATE_PLACEMENT": {
        "pattern": r"SELECT.*FROM.*JOIN.*WHERE",
        "severity": "MEDIUM",
        "optimization_type": "PREDICATE_PUSHDOWN"
    },
    "UNNECESSARY_COLUMNS": {
        "pattern": r"SELECT\s+\*\s+FROM",
        "severity": "MEDIUM",
        "optimization_type": "COLUMN_PRUNING"
    },
    "NESTED_SUBQUERIES": {
        "pattern": r"SELECT.*\(\s*SELECT.*\(\s*SELECT",
        "severity": "MEDIUM",
        "optimization_type": "SUBQUERY_FLATTENING"
    },
    "INEFFICIENT_AGGREGATION": {
        "pattern": r"GROUP\s+BY.*ORDER\s+BY.*LIMIT",
        "severity": "MEDIUM",
        "optimization_type": "AGGREGATION_OPTIMIZATION"
    },
    "REPEATED_SUBQUERIES": {
        "pattern": r"(SELECT.*FROM.*WHERE.*)(\\1)",
        "severity": "MEDIUM",
        "optimization_type": "CTE_CONVERSION"
    },
    "UNPARTITIONED_TABLE_SCAN": {
        "pattern": r"FROM\s+[^\(]*\s+WHERE\s+(?!.*partition)",
        "severity": "HIGH",
        "optimization_type": "TABLE_PARTITIONING"
    }
}

# Define anti-pattern definitions with descriptions and recommendations
ANTI_PATTERN_DEFINITIONS = {
    "CROSS_JOIN": {
        "description": "Cross join detected which may cause Cartesian product",
        "impact": "HIGH",
        "recommendation": "Add join conditions to filter the result set"
    },
    "SELECT_STAR": {
        "description": "SELECT * retrieves unnecessary columns",
        "impact": "MEDIUM",
        "recommendation": "Explicitly list only required columns"
    },
    "INEFFICIENT_JOIN_ORDER": {
        "description": "Join order may not be optimal",
        "impact": "MEDIUM",
        "recommendation": "Reorder joins to process smaller tables first"
    },
    "MISSING_WHERE_CLAUSE": {
        "description": "Query without WHERE clause scans entire table",
        "impact": "HIGH",
        "recommendation": "Add appropriate filters to reduce data scanned"
    },
    "MISSING_PARTITIONING_FILTER": {
        "description": "Query does not filter on partitioning column",
        "impact": "HIGH",
        "recommendation": "Add filter on partitioning column to reduce data scanned"
    },
    "REDUNDANT_JOIN": {
        "description": "Join that doesn't add value to the result",
        "impact": "MEDIUM",
        "recommendation": "Remove unnecessary joins"
    },
    "COMPLEX_SUBQUERY": {
        "description": "Complex nested subquery that could be simplified",
        "impact": "MEDIUM",
        "recommendation": "Flatten subqueries or use CTEs"
    },
    "INEFFICIENT_FUNCTION_USAGE": {
        "description": "Inefficient use of functions in WHERE clause",
        "impact": "MEDIUM",
        "recommendation": "Avoid functions on filtered columns"
    }
}


def normalize_query(query: str) -> str:
    """
    Normalizes a SQL query for consistent pattern matching.
    
    Args:
        query: The SQL query to normalize
        
    Returns:
        Normalized query string
    """
    if not query or not isinstance(query, str):
        return ""
    
    # Remove comments
    parsed = sqlparse.parse(query)
    if not parsed:
        return ""
    
    # Use sqlparse to format consistently
    formatted = sqlparse.format(
        query,
        keyword_case='lower',
        identifier_case='lower',
        strip_comments=True,
        reindent=True
    )
    
    # Further normalize whitespace
    normalized = re.sub(r'\s+', ' ', formatted)
    
    # Remove literal values to focus on query structure
    # Replace string literals
    normalized = re.sub(r"'[^']*'", "'?'", normalized)
    # Replace numeric literals
    normalized = re.sub(r'\b\d+\b', '?', normalized)
    
    return normalized.strip()


def generate_query_fingerprint(query: str) -> str:
    """
    Generates a fingerprint hash for a query to identify similar queries.
    
    Args:
        query: The SQL query to fingerprint
        
    Returns:
        Query fingerprint hash
    """
    # Normalize the query first
    normalized_query = normalize_query(query)
    
    # Extract key structural elements
    # - Extract tables referenced
    tables = extract_tables_from_query(normalized_query)
    table_list = sorted([t.lower() for t in tables])
    
    # - Extract join conditions
    join_conditions = extract_join_conditions(normalized_query)
    join_list = sorted([str(j).lower() for j in join_conditions])
    
    # - Extract where conditions
    where_conditions = extract_where_conditions(normalized_query)
    where_list = sorted([str(w).lower() for w in where_conditions])
    
    # Create a canonical representation
    canonical = f"TABLES:{','.join(table_list)}|JOINS:{','.join(join_list)}|WHERE:{','.join(where_list)}"
    
    # Generate a hash of the canonical representation
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def match_pattern(query: str, pattern: str) -> bool:
    """
    Checks if a query matches a specific pattern using regex.
    
    Args:
        query: The SQL query to check
        pattern: Regex pattern to match
        
    Returns:
        True if pattern matches, False otherwise
    """
    # Normalize query if not already normalized
    normalized_query = query if query.strip() == normalize_query(query) else normalize_query(query)
    
    # Compile the regex pattern for efficiency
    try:
        compiled_pattern = re.compile(pattern, re.IGNORECASE | re.DOTALL)
        return bool(compiled_pattern.search(normalized_query))
    except re.error as e:
        logger.error(f"Invalid regex pattern: {pattern}. Error: {str(e)}")
        return False


def extract_tables_from_query(query: str) -> List[str]:
    """
    Extracts table references from a SQL query.
    
    Args:
        query: The SQL query to analyze
        
    Returns:
        List of table references
    """
    tables = []
    
    try:
        # Parse the query
        parsed = sqlparse.parse(query)
        if not parsed:
            return []
        
        stmt = parsed[0]
        
        # Extract FROM clauses and JOINs
        from_seen = False
        table_tokens = []
        
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'FROM':
                from_seen = True
            elif from_seen and token.ttype is None:  # Identifier or identifier list
                table_tokens.append(token)
            elif token.is_keyword and token.value.upper() in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 
                                                             'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN'):
                # Handle join tables
                # Find the next identifier after JOIN
                join_idx = stmt.tokens.index(token)
                for t in stmt.tokens[join_idx+1:]:
                    if t.ttype is None and not t.is_whitespace:
                        table_tokens.append(t)
                        break
        
        # Process table tokens
        for token in table_tokens:
            if hasattr(token, 'get_real_name'):
                tables.append(token.get_real_name())
            elif hasattr(token, 'value'):
                # Handle cases like "table_name alias" or "schema.table_name alias"
                table_str = token.value.strip()
                # Handle alias
                if ' ' in table_str:
                    table_str = table_str.split(' ')[0]
                tables.append(table_str)
                
        # Remove duplicates and return
        return list(set(tables))
        
    except Exception as e:
        logger.error(f"Error extracting tables from query: {str(e)}")
        return []


def extract_join_conditions(query: str) -> List[Dict[str, Any]]:
    """
    Extracts join conditions from a SQL query.
    
    Args:
        query: The SQL query to analyze
        
    Returns:
        List of join conditions with metadata
    """
    join_conditions = []
    
    try:
        # Parse the query
        parsed = sqlparse.parse(query)
        if not parsed:
            return []
        
        stmt = parsed[0]
        
        # Process tokens to find JOIN...ON clauses
        current_join = None
        in_on_clause = False
        
        for i, token in enumerate(stmt.tokens):
            # Identify JOIN statements
            if token.is_keyword and token.value.upper() in ('JOIN', 'INNER JOIN', 'LEFT JOIN', 
                                                           'RIGHT JOIN', 'FULL JOIN', 'CROSS JOIN'):
                current_join = {
                    'type': token.value.upper(),
                    'table': None,
                    'conditions': []
                }
                
                # Find the table being joined
                for j in range(i+1, len(stmt.tokens)):
                    if stmt.tokens[j].ttype is None and not stmt.tokens[j].is_whitespace:
                        if hasattr(stmt.tokens[j], 'get_real_name'):
                            current_join['table'] = stmt.tokens[j].get_real_name()
                        else:
                            table_str = stmt.tokens[j].value.strip()
                            if ' ' in table_str:  # Handle alias
                                table_str = table_str.split(' ')[0]
                            current_join['table'] = table_str
                        break
            
            # Identify ON clauses
            elif token.is_keyword and token.value.upper() == 'ON' and current_join:
                in_on_clause = True
                
            # Capture conditions in ON clause
            elif in_on_clause and token.ttype is None and not token.is_whitespace:
                current_join['conditions'].append(token.value)
                in_on_clause = False
                join_conditions.append(current_join)
                current_join = None
        
        return join_conditions
        
    except Exception as e:
        logger.error(f"Error extracting join conditions from query: {str(e)}")
        return []


def extract_where_conditions(query: str) -> List[str]:
    """
    Extracts WHERE clause conditions from a SQL query.
    
    Args:
        query: The SQL query to analyze
        
    Returns:
        List of WHERE conditions
    """
    conditions = []
    
    try:
        # Parse the query
        parsed = sqlparse.parse(query)
        if not parsed:
            return []
        
        stmt = parsed[0]
        
        # Find the WHERE clause
        where_clause = None
        for token in stmt.tokens:
            if isinstance(token, sqlparse.sql.Where):
                where_clause = token
                break
        
        if not where_clause:
            return []
            
        # Extract individual conditions
        # Split on AND/OR operators
        condition_text = where_clause.value.upper().replace('WHERE', '', 1).strip()
        
        # Simple split for basic analysis - in a real implementation this would be more sophisticated
        # to handle nested conditions and parentheses
        if ' AND ' in condition_text:
            conditions.extend([c.strip() for c in condition_text.split(' AND ')])
        elif ' OR ' in condition_text:
            conditions.extend([c.strip() for c in condition_text.split(' OR ')])
        else:
            conditions.append(condition_text)
            
        return conditions
        
    except Exception as e:
        logger.error(f"Error extracting WHERE conditions from query: {str(e)}")
        return []


def analyze_plan_for_patterns(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Analyzes a query execution plan to identify performance patterns.
    
    Args:
        plan: BigQuery query execution plan
        
    Returns:
        List of identified patterns from the plan
    """
    patterns = []
    
    if not plan or not isinstance(plan, dict):
        return patterns
        
    try:
        # Check if there's a queryPlan or executionPlan key
        query_plan = plan.get('queryPlan') or plan.get('executionPlan') or []
        
        # Analyze each stage in the execution plan
        for stage in query_plan:
            stage_id = stage.get('name', '')
            
            # Look for table scans without filters
            if 'steps' in stage:
                for step in stage['steps']:
                    kind = step.get('kind', '')
                    
                    # Check for table scans
                    if 'READ' in kind:
                        # Check if there are no filters
                        if 'substeps' in step:
                            has_filter = any('filter' in substep.lower() for substep in step['substeps'])
                            if not has_filter:
                                patterns.append({
                                    'pattern_id': 'FULL_TABLE_SCAN',
                                    'pattern_type': 'PLAN',
                                    'description': f'Full table scan detected in stage {stage_id}',
                                    'severity': 'HIGH',
                                    'optimization_type': 'PREDICATE_PUSHDOWN',
                                    'details': {'stage': stage_id, 'step': step}
                                })
                    
                    # Check for expensive shuffle operations
                    if 'SHUFFLE' in kind:
                        patterns.append({
                            'pattern_id': 'EXPENSIVE_SHUFFLE',
                            'pattern_type': 'PLAN',
                            'description': f'Expensive shuffle operation in stage {stage_id}',
                            'severity': 'MEDIUM',
                            'optimization_type': 'JOIN_REORDERING',
                            'details': {'stage': stage_id, 'step': step}
                        })
                    
                    # Check for repeated operations
                    if 'AGGREGATE' in kind and 'GROUP_BY' in kind:
                        patterns.append({
                            'pattern_id': 'MULTI_STAGE_AGGREGATION',
                            'pattern_type': 'PLAN',
                            'description': f'Multi-stage aggregation in stage {stage_id}',
                            'severity': 'MEDIUM',
                            'optimization_type': 'AGGREGATION_OPTIMIZATION',
                            'details': {'stage': stage_id, 'step': step}
                        })
            
            # Check for stages with high input/output ratio
            input_records = stage.get('inputStage', {}).get('recordsRead', 0)
            output_records = stage.get('recordsWritten', 0)
            
            if input_records and output_records and input_records > 10 * output_records:
                patterns.append({
                    'pattern_id': 'HIGH_INPUT_OUTPUT_RATIO',
                    'pattern_type': 'PLAN',
                    'description': f'High input/output ratio in stage {stage_id}',
                    'severity': 'MEDIUM',
                    'optimization_type': 'FILTER_PUSHDOWN',
                    'details': {
                        'stage': stage_id,
                        'input_records': input_records,
                        'output_records': output_records,
                        'ratio': input_records / output_records if output_records else 0
                    }
                })
        
        return patterns
        
    except Exception as e:
        logger.error(f"Error analyzing query plan: {str(e)}")
        return []


class QueryPattern:
    """
    Represents a pattern identified in a SQL query.
    """
    
    def __init__(self, pattern_id: str, pattern_type: str, description: str, 
                 severity: str, optimization_type: str, details: Dict[str, Any] = None):
        """
        Initializes a QueryPattern with pattern details.
        
        Args:
            pattern_id: Unique identifier for the pattern
            pattern_type: Type of pattern (SYNTAX, STRUCTURE, PLAN)
            description: Description of the pattern
            severity: Severity level (HIGH, MEDIUM, LOW)
            optimization_type: Type of optimization needed
            details: Additional pattern-specific details
        """
        self.pattern_id = pattern_id
        self.pattern_type = pattern_type
        self.description = description
        self.severity = severity
        self.optimization_type = optimization_type
        self.details = details or {}
        
        # Validate severity
        valid_severities = ['HIGH', 'MEDIUM', 'LOW']
        if self.severity not in valid_severities:
            self.severity = 'MEDIUM'
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the pattern to a dictionary representation.
        
        Returns:
            Dictionary representation of the pattern
        """
        return {
            'pattern_id': self.pattern_id,
            'pattern_type': self.pattern_type,
            'description': self.description,
            'severity': self.severity,
            'optimization_type': self.optimization_type,
            'details': self.details,
            'impact_score': self.get_impact_score(),
            'recommendation': self.get_recommendation()
        }
    
    def get_impact_score(self) -> float:
        """
        Calculates the impact score of this pattern.
        
        Returns:
            Impact score from 0.0 to 10.0
        """
        # Base score based on severity
        base_scores = {
            'HIGH': 8.0,
            'MEDIUM': 5.0,
            'LOW': 2.0
        }
        
        score = base_scores.get(self.severity, 5.0)
        
        # Adjust based on pattern type
        type_modifiers = {
            'SYNTAX': 1.0,
            'STRUCTURE': 1.2,
            'PLAN': 1.5
        }
        
        score *= type_modifiers.get(self.pattern_type, 1.0)
        
        # Consider additional factors from details
        if 'records_scanned' in self.details:
            # Adjust based on data volume - higher volume = higher impact
            records = self.details['records_scanned']
            if records > 1000000000:  # 1 billion
                score += 1.5
            elif records > 100000000:  # 100 million
                score += 1.0
            elif records > 10000000:  # 10 million
                score += 0.5
        
        # Ensure score is within 0-10 range
        return min(10.0, max(0.0, score))
    
    def get_recommendation(self) -> str:
        """
        Generates a recommendation to address this pattern.
        
        Returns:
            Recommendation text
        """
        # Base recommendations by pattern type
        recommendations = {
            'CARTESIAN_JOIN': "Add explicit join conditions to prevent Cartesian product.",
            'MISSING_JOIN_PREDICATE': "Add explicit ON conditions to your JOIN clauses.",
            'SUBOPTIMAL_PREDICATE_PLACEMENT': "Move filter conditions from WHERE to JOIN ON clauses when filtering joined tables.",
            'UNNECESSARY_COLUMNS': "Explicitly list required columns instead of using SELECT *.",
            'NESTED_SUBQUERIES': "Simplify nested subqueries using CTEs or by flattening the query.",
            'INEFFICIENT_AGGREGATION': "Consider pre-aggregating data or refactoring the query to avoid expensive GROUP BY operations.",
            'REPEATED_SUBQUERIES': "Use Common Table Expressions (CTEs) to avoid repeating the same subquery multiple times.",
            'UNPARTITIONED_TABLE_SCAN': "Add filters on partitioning columns to limit data scanned.",
            'FULL_TABLE_SCAN': "Add appropriate WHERE clauses to filter data early in the query.",
            'EXPENSIVE_SHUFFLE': "Review join order and consider denormalizing data to reduce shuffling.",
            'MULTI_STAGE_AGGREGATION': "Simplify aggregations or consider pre-aggregating data in an intermediate table.",
            'HIGH_INPUT_OUTPUT_RATIO': "Push filters earlier in the query to reduce data processed."
        }
        
        # Get base recommendation for this pattern
        recommendation = recommendations.get(self.pattern_id, "Review query for optimization opportunities.")
        
        # Enhance with pattern-specific details
        if self.pattern_id == 'UNPARTITIONED_TABLE_SCAN' and 'table' in self.details:
            recommendation += f" Table '{self.details['table']}' should be queried with partition filters."
            
        if self.pattern_id == 'UNNECESSARY_COLUMNS' and 'columns_needed' in self.details:
            cols = ", ".join(self.details['columns_needed'])
            recommendation += f" Consider selecting only needed columns: {cols}."
            
        return recommendation


class PatternIdentifier:
    """
    Identifies patterns and anti-patterns in BigQuery SQL queries to enable optimization.
    """
    
    def __init__(self, bq_client: BigQueryClient):
        """
        Initializes the PatternIdentifier with BigQuery client.
        
        Args:
            bq_client: BigQuery client for executing queries and retrieving plans
        """
        self._bq_client = bq_client
        self._pattern_cache = {}  # Cache for pattern identification results
        self._query_history = {}  # History of analyzed queries
        
        logger.info("PatternIdentifier initialized")
    
    def identify_patterns(self, query: str, plan: Dict[str, Any] = None, 
                         use_cache: bool = True) -> Dict[str, Any]:
        """
        Identifies patterns and anti-patterns in a SQL query.
        
        Args:
            query: The SQL query to analyze
            plan: Optional query execution plan for deeper analysis
            use_cache: Whether to use cached results if available
        
        Returns:
            Dictionary of identified patterns and anti-patterns
        """
        if not query:
            logger.warning("Empty query provided for pattern identification")
            return {"patterns": [], "anti_patterns": [], "query_fingerprint": ""}
        
        # Generate query fingerprint for cache lookup
        query_fingerprint = generate_query_fingerprint(query)
        
        # Check cache if enabled
        if use_cache and query_fingerprint in self._pattern_cache:
            logger.debug(f"Using cached patterns for query fingerprint: {query_fingerprint}")
            return self._pattern_cache[query_fingerprint]
        
        logger.info(f"Identifying patterns for query fingerprint: {query_fingerprint}")
        
        # Normalize query for pattern matching
        normalized_query = normalize_query(query)
        
        # Identify syntax patterns
        syntax_patterns = self.identify_syntax_patterns(normalized_query)
        
        # Identify structural patterns
        structural_patterns = self.identify_structural_patterns(query)
        
        # Identify patterns from execution plan if provided
        plan_patterns = []
        if plan:
            plan_patterns = analyze_plan_for_patterns(plan)
        
        # Combine all identified patterns
        all_patterns = syntax_patterns + structural_patterns + plan_patterns
        
        # Convert to QueryPattern objects if not already
        patterns = []
        for p in all_patterns:
            if isinstance(p, QueryPattern):
                patterns.append(p)
            else:
                patterns.append(QueryPattern(
                    pattern_id=p.get('pattern_id', 'UNKNOWN'),
                    pattern_type=p.get('pattern_type', 'UNKNOWN'),
                    description=p.get('description', 'Unknown pattern'),
                    severity=p.get('severity', 'MEDIUM'),
                    optimization_type=p.get('optimization_type', 'UNKNOWN'),
                    details=p.get('details', {})
                ))
        
        # Identify anti-patterns
        anti_patterns = self.identify_anti_patterns(query, patterns)
        
        # Prepare result dictionary
        result = {
            "query_fingerprint": query_fingerprint,
            "patterns": [p.to_dict() for p in patterns],
            "anti_patterns": anti_patterns,
            "optimization_suggestions": self.get_optimization_suggestions(patterns)
        }
        
        # Cache the result if caching is enabled
        if use_cache:
            self._pattern_cache[query_fingerprint] = result
            
        # Store in query history
        self._query_history[query_fingerprint] = {
            "timestamp": None,  # Would normally set to current time
            "query": query,
            "patterns": [p.pattern_id for p in patterns]
        }
        
        logger.info(f"Identified {len(patterns)} patterns and {len(anti_patterns)} anti-patterns")
        return result
    
    def identify_patterns_from_history(self, queries: List[str], 
                                      min_occurrence: int = 2) -> Dict[str, Any]:
        """
        Identifies common patterns across historical queries.
        
        Args:
            queries: List of queries to analyze
            min_occurrence: Minimum occurrences threshold for reporting
            
        Returns:
            Dictionary of common patterns with frequency
        """
        if not queries:
            return {"common_patterns": []}
            
        logger.info(f"Analyzing {len(queries)} queries for common patterns")
        
        # Process each query
        pattern_counts = {}
        processed_queries = 0
        
        for query in queries:
            # Skip empty queries
            if not query:
                continue
                
            # Identify patterns in this query
            patterns = self.identify_patterns(query)
            
            # Count occurrences of each pattern
            for pattern in patterns.get("patterns", []):
                pattern_id = pattern.get("pattern_id")
                if pattern_id:
                    if pattern_id not in pattern_counts:
                        pattern_counts[pattern_id] = {
                            "count": 0,
                            "details": pattern
                        }
                    pattern_counts[pattern_id]["count"] += 1
            
            processed_queries += 1
        
        # Filter patterns by minimum occurrence
        common_patterns = []
        for pattern_id, data in pattern_counts.items():
            if data["count"] >= min_occurrence:
                pattern_info = data["details"].copy()
                pattern_info["frequency"] = data["count"]
                pattern_info["frequency_percentage"] = (data["count"] / processed_queries) * 100 if processed_queries > 0 else 0
                common_patterns.append(pattern_info)
        
        # Sort by frequency and impact
        common_patterns.sort(key=lambda x: (x["frequency"], x.get("impact_score", 0)), reverse=True)
        
        logger.info(f"Identified {len(common_patterns)} common patterns across queries")
        return {"common_patterns": common_patterns}
    
    def identify_syntax_patterns(self, normalized_query: str) -> List[Dict[str, Any]]:
        """
        Identifies syntax-based patterns using regex matching.
        
        Args:
            normalized_query: Normalized SQL query
            
        Returns:
            List of identified syntax patterns
        """
        patterns = []
        
        # Check each pattern definition against the query
        for pattern_id, definition in QUERY_PATTERN_DEFINITIONS.items():
            if match_pattern(normalized_query, definition["pattern"]):
                patterns.append({
                    "pattern_id": pattern_id,
                    "pattern_type": "SYNTAX",
                    "description": f"Detected {pattern_id.replace('_', ' ').title()}",
                    "severity": definition["severity"],
                    "optimization_type": definition["optimization_type"],
                    "details": {}
                })
        
        return patterns
    
    def identify_structural_patterns(self, query: str) -> List[Dict[str, Any]]:
        """
        Identifies structural patterns based on query components.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            List of identified structural patterns
        """
        patterns = []
        
        # Extract query components
        tables = extract_tables_from_query(query)
        join_conditions = extract_join_conditions(query)
        where_conditions = extract_where_conditions(query)
        
        # Analyze join structure
        if join_conditions:
            join_analysis = self.analyze_join_efficiency(tables, join_conditions)
            
            # Check for cross joins
            if join_analysis.get("has_cartesian_join", False):
                patterns.append({
                    "pattern_id": "CARTESIAN_JOIN",
                    "pattern_type": "STRUCTURE",
                    "description": "Cartesian join detected which may cause excessive data processing",
                    "severity": "HIGH",
                    "optimization_type": "JOIN_REORDERING",
                    "details": join_analysis
                })
            
            # Check for missing join conditions
            if join_analysis.get("missing_join_conditions", False):
                patterns.append({
                    "pattern_id": "MISSING_JOIN_PREDICATE",
                    "pattern_type": "STRUCTURE",
                    "description": "Join without explicit join condition detected",
                    "severity": "HIGH",
                    "optimization_type": "JOIN_REORDERING",
                    "details": join_analysis
                })
        
        # Analyze predicate placement
        if join_conditions and where_conditions:
            predicate_analysis = self.analyze_predicate_efficiency(where_conditions, join_conditions)
            
            if predicate_analysis.get("suboptimal_predicate_placement", False):
                patterns.append({
                    "pattern_id": "SUBOPTIMAL_PREDICATE_PLACEMENT",
                    "pattern_type": "STRUCTURE",
                    "description": "Filter conditions could be moved from WHERE to JOIN for better performance",
                    "severity": "MEDIUM",
                    "optimization_type": "PREDICATE_PUSHDOWN",
                    "details": predicate_analysis
                })
        
        # Check for SELECT *
        if "select * from" in normalize_query(query).lower():
            patterns.append({
                "pattern_id": "UNNECESSARY_COLUMNS",
                "pattern_type": "STRUCTURE",
                "description": "SELECT * retrieves unnecessary columns",
                "severity": "MEDIUM",
                "optimization_type": "COLUMN_PRUNING",
                "details": {"tables": tables}
            })
        
        # Check for nested subqueries
        subquery_count = query.lower().count("select")
        if subquery_count > 2:
            patterns.append({
                "pattern_id": "NESTED_SUBQUERIES",
                "pattern_type": "STRUCTURE",
                "description": f"Query contains {subquery_count-1} nested subqueries",
                "severity": "MEDIUM",
                "optimization_type": "SUBQUERY_FLATTENING",
                "details": {"subquery_count": subquery_count-1}
            })
            
        # Detect missing WHERE clause for large tables
        if not where_conditions and tables:
            patterns.append({
                "pattern_id": "MISSING_WHERE_CLAUSE",
                "pattern_type": "STRUCTURE", 
                "description": "Query does not have a WHERE clause, potentially scanning entire tables",
                "severity": "HIGH",
                "optimization_type": "PREDICATE_PUSHDOWN",
                "details": {"tables": tables}
            })
        
        return patterns
    
    def identify_anti_patterns(self, query: str, patterns: List[QueryPattern]) -> List[Dict[str, Any]]:
        """
        Identifies anti-patterns in a SQL query.
        
        Args:
            query: SQL query to analyze
            patterns: List of identified patterns
            
        Returns:
            List of identified anti-patterns
        """
        anti_patterns = []
        
        # Map from pattern IDs to anti-pattern definitions
        pattern_to_anti_pattern = {
            "CARTESIAN_JOIN": "CROSS_JOIN",
            "UNNECESSARY_COLUMNS": "SELECT_STAR",
            "NESTED_SUBQUERIES": "COMPLEX_SUBQUERY",
            "MISSING_WHERE_CLAUSE": "MISSING_WHERE_CLAUSE",
            "UNPARTITIONED_TABLE_SCAN": "MISSING_PARTITIONING_FILTER",
            "SUBOPTIMAL_PREDICATE_PLACEMENT": "INEFFICIENT_FUNCTION_USAGE"
        }
        
        # Process each pattern and map to anti-patterns
        for pattern in patterns:
            anti_pattern_id = pattern_to_anti_pattern.get(pattern.pattern_id)
            
            if anti_pattern_id and anti_pattern_id in ANTI_PATTERN_DEFINITIONS:
                definition = ANTI_PATTERN_DEFINITIONS[anti_pattern_id]
                
                anti_patterns.append({
                    "anti_pattern_id": anti_pattern_id,
                    "description": definition["description"],
                    "impact": definition["impact"],
                    "recommendation": definition["recommendation"],
                    "details": pattern.details
                })
        
        # Additional analysis for anti-patterns not directly mapped from patterns
        normalized_query = normalize_query(query)
        
        # Check for inefficient function usage in WHERE clause
        if re.search(r"where\s+\w+\s*\(", normalized_query, re.IGNORECASE):
            anti_patterns.append({
                "anti_pattern_id": "INEFFICIENT_FUNCTION_USAGE",
                "description": ANTI_PATTERN_DEFINITIONS["INEFFICIENT_FUNCTION_USAGE"]["description"],
                "impact": ANTI_PATTERN_DEFINITIONS["INEFFICIENT_FUNCTION_USAGE"]["impact"],
                "recommendation": ANTI_PATTERN_DEFINITIONS["INEFFICIENT_FUNCTION_USAGE"]["recommendation"],
                "details": {}
            })
        
        # De-duplicate anti-patterns
        unique_anti_patterns = []
        seen_ids = set()
        
        for ap in anti_patterns:
            if ap["anti_pattern_id"] not in seen_ids:
                unique_anti_patterns.append(ap)
                seen_ids.add(ap["anti_pattern_id"])
        
        return unique_anti_patterns
    
    def get_optimization_suggestions(self, patterns: List[QueryPattern]) -> List[Dict[str, Any]]:
        """
        Generates optimization suggestions based on identified patterns.
        
        Args:
            patterns: List of identified patterns
            
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        
        # Group patterns by optimization type
        optimization_groups = {}
        
        for pattern in patterns:
            opt_type = pattern.optimization_type
            if opt_type not in optimization_groups:
                optimization_groups[opt_type] = []
            optimization_groups[opt_type].append(pattern)
        
        # Generate suggestions for each optimization type
        for opt_type, patterns_group in optimization_groups.items():
            # Sort patterns by impact score
            patterns_group.sort(key=lambda p: p.get_impact_score(), reverse=True)
            
            # Generate suggestion based on optimization type
            suggestion = {
                "optimization_type": opt_type,
                "patterns": [p.pattern_id for p in patterns_group],
                "impact_score": max([p.get_impact_score() for p in patterns_group]),
                "description": f"Apply {opt_type.replace('_', ' ').title()} optimization",
                "recommendations": []
            }
            
            # Add pattern-specific recommendations
            for pattern in patterns_group:
                suggestion["recommendations"].append({
                    "pattern_id": pattern.pattern_id,
                    "recommendation": pattern.get_recommendation()
                })
            
            suggestions.append(suggestion)
        
        # Sort suggestions by impact score
        suggestions.sort(key=lambda s: s["impact_score"], reverse=True)
        
        return suggestions
    
    def analyze_join_efficiency(self, tables: List[str], 
                               join_conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyzes the efficiency of join operations in a query.
        
        Args:
            tables: List of tables in the query
            join_conditions: List of join conditions
            
        Returns:
            Join efficiency analysis
        """
        analysis = {
            "has_cartesian_join": False,
            "missing_join_conditions": False,
            "join_types": {},
            "recommendations": []
        }
        
        # Count join types
        for join in join_conditions:
            join_type = join.get("type", "JOIN")
            if join_type not in analysis["join_types"]:
                analysis["join_types"][join_type] = 0
            analysis["join_types"][join_type] += 1
            
            # Check if this join has conditions
            if not join.get("conditions"):
                analysis["missing_join_conditions"] = True
                analysis["recommendations"].append(f"Add explicit join conditions for {join.get('table')}")
        
        # Check for CROSS JOIN
        if "CROSS JOIN" in analysis["join_types"]:
            analysis["has_cartesian_join"] = True
            analysis["recommendations"].append("Replace CROSS JOIN with explicit join conditions")
        
        # Check for implicit cartesian join (JOIN without ON)
        if analysis["missing_join_conditions"]:
            analysis["has_cartesian_join"] = True
        
        # Basic join order analysis
        if len(tables) > 2 and len(join_conditions) > 1:
            analysis["recommendations"].append("Consider optimizing join order to process smaller tables first")
        
        return analysis
    
    def analyze_predicate_efficiency(self, where_conditions: List[str], 
                                    join_conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyzes the efficiency of predicates in a query.
        
        Args:
            where_conditions: List of WHERE conditions
            join_conditions: List of join conditions
            
        Returns:
            Predicate efficiency analysis
        """
        analysis = {
            "suboptimal_predicate_placement": False,
            "sargable_issues": False,
            "function_in_predicate": False,
            "recommendations": []
        }
        
        # Extract tables from join conditions
        join_tables = set()
        for join in join_conditions:
            if join.get("table"):
                join_tables.add(join.get("table"))
        
        # Check each WHERE condition to see if it should be in JOIN
        for condition in where_conditions:
            # Check if condition references joined tables
            for table in join_tables:
                if table.lower() in condition.lower():
                    analysis["suboptimal_predicate_placement"] = True
                    analysis["recommendations"].append(
                        f"Consider moving condition on {table} from WHERE to JOIN ON clause"
                    )
            
            # Check for functions in predicates
            if re.search(r"\w+\s*\(", condition):
                analysis["function_in_predicate"] = True
                analysis["recommendations"].append(
                    "Avoid using functions in WHERE clause predicates when possible"
                )
            
            # Check for non-sargable predicates
            if re.search(r"like\s+[^%]", condition, re.IGNORECASE):
                analysis["sargable_issues"] = True
                analysis["recommendations"].append(
                    "Consider adding a % wildcard at the beginning of LIKE patterns"
                )
        
        return analysis
    
    def clear_pattern_cache(self) -> None:
        """
        Clears the pattern identification cache.
        """
        self._pattern_cache = {}
        logger.info("Pattern cache cleared")


# Export key components
__all__ = [
    'PatternIdentifier',
    'QueryPattern',
    'normalize_query',
    'generate_query_fingerprint'
]