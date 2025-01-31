"""Type Inference Module

This module provides utilities for inferring data types and collecting statistics
about fields in nested data structures. It's particularly useful for understanding
the shape and characteristics of unknown data.

Example:
    >>> data = {"id": "123", "active": "true", "score": "45.6"}
    >>> print(infer_type(data["id"]))
    'integer'
    >>> print(infer_type(data["active"]))
    'boolean'
    >>> print(infer_type(data["score"]))
    'float'
"""

from typing import Any, Dict, Optional, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def infer_type(value: Any) -> str:
    """Infer the data type of a value.
    
    This function attempts to determine the most specific type that accurately
    represents the value. It handles common string representations of various
    types (e.g., "true" as boolean, "123" as integer).
    
    Args:
        value: The value to analyze
        
    Returns:
        String representing the inferred type
        
    Example:
        >>> print(infer_type("123"))
        'integer'
        >>> print(infer_type("true"))
        'boolean'
        >>> print(infer_type("2023-01-01"))
        'date'
        >>> print(infer_type("hello"))
        'string'
        
    Notes:
        - Handles null/None values
        - Recognizes common date formats
        - Detects numeric strings
        - Identifies boolean strings
    """
    if value is None:
        return 'null'
        
    if isinstance(value, bool):
        return 'boolean'
        
    if isinstance(value, int):
        return 'integer'
        
    if isinstance(value, float):
        return 'float'
        
    if isinstance(value, str):
        # Try boolean
        value_lower = value.lower()
        if value_lower in ('true', 'false'):
            return 'boolean'
            
        # Try integer
        try:
            int(value)
            return 'integer'
        except ValueError:
            pass
            
        # Try float
        try:
            float(value)
            return 'float'
        except ValueError:
            pass
            
        # Try date
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return 'date'
        except ValueError:
            pass
            
        # Try datetime
        try:
            datetime.fromisoformat(value)
            return 'datetime'
        except ValueError:
            pass
            
    return 'string'

def get_field_stats(values: list[Any]) -> Dict[str, Union[int, float, list[str]]]:
    """Calculate statistics for a list of field values.
    
    This function analyzes a list of values and returns various statistics
    about them, including type distribution, null count, and unique values.
    
    Args:
        values: List of values to analyze
        
    Returns:
        Dictionary containing field statistics
        
    Example:
        >>> values = ["123", "456", None, "789"]
        >>> stats = get_field_stats(values)
        >>> print(stats)
        {
            'count': 4,
            'null_count': 1,
            'types': ['integer', 'null'],
            'unique_count': 3
        }
        
    Notes:
        - Handles mixed types
        - Counts null values
        - Identifies unique values
        - Determines type distribution
    """
    stats = {
        'count': len(values),
        'null_count': sum(1 for v in values if v is None),
        'types': sorted(set(infer_type(v) for v in values)),
        'unique_count': len(set(values))
    }
    
    return stats
