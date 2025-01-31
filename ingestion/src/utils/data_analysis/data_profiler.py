"""Data Profiling Module

This module provides utilities for generating detailed profiles of data structures.
It analyzes the content, structure, and characteristics of data to provide insights
about its composition and quality.

Example:
    >>> data = {"users": [{"name": "John", "age": "30"}]}
    >>> profile = profile_data(data)
    >>> print(profile['field_types'])
    {'users.name': 'string', 'users.age': 'integer'}
"""

from typing import Any, Dict, List
import logging
from collections import defaultdict
from .structure_analyzer import analyze_structure
from .type_inference import infer_type, get_field_stats

logger = logging.getLogger(__name__)

def analyze_field(values: List[Any], path: str) -> Dict[str, Any]:
    """Analyze a list of field values to generate statistics.
    
    This function examines a collection of values for a specific field,
    generating statistics and insights about the data.
    
    Args:
        values: List of values to analyze
        path: Dot-notation path to the field
        
    Returns:
        Dictionary containing field analysis results
        
    Example:
        >>> values = ["John", "Jane", None, "John"]
        >>> analysis = analyze_field(values, "users.name")
        >>> print(analysis)
        {
            'path': 'users.name',
            'stats': {
                'count': 4,
                'null_count': 1,
                'types': ['string', 'null'],
                'unique_count': 2
            }
        }
        
    Notes:
        - Handles mixed types
        - Provides null value analysis
        - Calculates uniqueness statistics
        - Identifies patterns in the data
    """
    stats = get_field_stats(values)
    
    return {
        'path': path,
        'stats': stats
    }

def profile_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a comprehensive profile of a data structure.
    
    This function analyzes the entire data structure, providing insights about
    its composition, field types, value distributions, and potential quality issues.
    
    Args:
        data: The data structure to profile
        
    Returns:
        Dictionary containing the complete data profile
        
    Example:
        >>> data = {
        ...     "users": [
        ...         {"name": "John", "age": "30"},
        ...         {"name": "Jane", "age": "25"}
        ...     ]
        ... }
        >>> profile = profile_data(data)
        >>> print(profile['field_types'])
        {'users.name': 'string', 'users.age': 'integer'}
        >>> print(profile['null_counts'])
        {'users.name': 0, 'users.age': 0}
        
    Notes:
        - Analyzes structure and content
        - Identifies data types and patterns
        - Reports quality metrics
        - Provides detailed field-level statistics
    """
    paths = analyze_structure(data)
    field_values = defaultdict(list)
    
    # Collect values for each path
    def collect_values(obj: Any, prefix: str = ""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    collect_values(value, new_prefix)
                else:
                    field_values[new_prefix].append(value)
        elif isinstance(obj, list):
            for item in obj:
                collect_values(item, prefix)
                
    collect_values(data)
    
    # Analyze each field
    field_analyses = {}
    field_types = {}
    null_counts = {}
    
    for path, values in field_values.items():
        analysis = analyze_field(values, path)
        field_analyses[path] = analysis
        
        # Extract common type (excluding null)
        types = [t for t in analysis['stats']['types'] if t != 'null']
        field_types[path] = types[0] if types else 'null'
        
        null_counts[path] = analysis['stats']['null_count']
    
    return {
        'field_types': field_types,
        'null_counts': null_counts,
        'field_analyses': field_analyses,
        'total_fields': len(field_values),
        'total_paths': len(paths)
    }
