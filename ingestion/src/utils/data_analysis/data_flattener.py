"""Data Flattening Module

This module provides utilities for flattening nested data structures into
simpler formats like lists of dictionaries. It's particularly useful for
preparing hierarchical data for analysis or export.

Example:
    >>> data = {"users": [{"name": "John", "address": {"city": "NY"}}]}
    >>> flattened, metadata = flatten_data(data)
    >>> print(flattened[0])
    {'name': 'John', 'address_city': 'NY'}
"""

from typing import Any, Dict, List, Tuple
import logging
from .structure_analyzer import analyze_structure, find_main_list

logger = logging.getLogger(__name__)

def flatten_data(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Flatten nested data into a list of dictionaries.
    
    This function converts a nested data structure into a flat list of records,
    where each record is a dictionary with dot-notation keys. It's particularly
    useful for preparing data for export to CSV or similar formats.
    
    Args:
        data: The nested data structure to flatten
        
    Returns:
        Tuple of (list of flattened records, metadata about the flattening)
        
    Example:
        >>> data = {
        ...     "users": [
        ...         {
        ...             "name": "John",
        ...             "address": {"city": "NY"}
        ...         }
        ...     ]
        ... }
        >>> records, metadata = flatten_data(data)
        >>> print(records[0])
        {'name': 'John', 'address_city': 'NY'}
        >>> print(metadata['columns'])
        ['name', 'address_city']
        
    Notes:
        - Nested objects are flattened using underscore notation
        - Lists are handled by finding the main data list
        - Empty or invalid data returns ([], {'error': 'message'})
    """
    paths = analyze_structure(data)
    if not paths:
        logger.warning("No paths found in data to flatten")
        return [], {"error": "No paths found"}
        
    main_data, main_path = find_main_list(data, paths)
    if not main_data:
        logger.warning("No list data found to flatten")
        return [], {"error": "No list data found"}
        
    flattened_records = []
    for record in main_data:
        flat_record = {}
        
        for path in paths:
            if path.startswith(main_path):
                relative_path = path[len(main_path):].lstrip('.')
                
                value = record
                for key in relative_path.split('.'):
                    if isinstance(value, dict):
                        value = value.get(key)
                    else:
                        value = None
                        break
                        
                field_name = relative_path.replace('.', '_')
                flat_record[field_name] = value
                
        flattened_records.append(flat_record)
        
    # Collect metadata about the flattening process
    metadata = {
        "record_count": len(flattened_records),
        "column_count": len(paths),
        "columns": sorted(flattened_records[0].keys()) if flattened_records else []
    }
    
    return flattened_records, metadata
