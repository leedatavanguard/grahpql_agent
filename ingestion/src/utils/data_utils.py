"""Utilities for handling nested data structures and GraphQL responses.

This module provides utilities for:
1. Analyzing and flattening nested data structures
2. Profiling data for quality assessment
3. Exporting data to various formats
"""

from typing import Any, Dict, List, Set, Tuple, Optional
import logging
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
from tabulate import tabulate

logger = logging.getLogger(__name__)

class NestedDataAnalyzer:
    """Analyzes and processes nested data structures.
    
    This class provides utilities for working with complex nested data, such as GraphQL responses.
    It can analyze structure, flatten nested data, and generate data profiles.
    
    Example:
        analyzer = NestedDataAnalyzer()
        paths = analyzer.analyze_structure(data)
        records, metadata = analyzer.flatten_data(data)
        profile = analyzer.profile_data(data)
    """
    
    def analyze_structure(self, obj: Any, prefix: str = "", visited: Set[int] = None) -> List[str]:
        """Recursively analyze structure and generate paths.
        
        Args:
            obj: Object to analyze
            prefix: Current path prefix
            visited: Set of object ids to prevent circular references
            
        Returns:
            List of flattened paths
            
        Example:
            >>> data = {"user": {"name": "John", "addresses": [{"city": "NY"}]}}
            >>> analyzer = NestedDataAnalyzer()
            >>> paths = analyzer.analyze_structure(data)
            >>> print(paths)
            ['user.name', 'user.addresses.city']
        """
        if visited is None:
            visited = set()
            
        obj_id = id(obj)
        if obj_id in visited:
            logger.debug(f"Circular reference detected at {prefix}")
            return []
        visited.add(obj_id)
        
        paths = []
        try:
            if isinstance(obj, dict):
                for key, value in sorted(obj.items()):
                    new_prefix = f"{prefix}.{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        paths.extend(self.analyze_structure(value, new_prefix, visited))
                    else:
                        paths.append(new_prefix)
            elif isinstance(obj, list) and obj:
                all_paths = set()
                for i, item in enumerate(obj[:10]):  # Sample first 10 items
                    try:
                        item_paths = self.analyze_structure(item, prefix, visited)
                        all_paths.update(item_paths)
                    except Exception as e:
                        logger.warning(f"Error analyzing list item {i} at {prefix}: {str(e)}")
                paths.extend(sorted(all_paths))
        except Exception as e:
            logger.warning(f"Error analyzing structure at {prefix}: {str(e)}")
        finally:
            visited.remove(obj_id)
            
        return paths
        
    def find_main_list(self, data: Dict[str, Any], paths: List[str]) -> Tuple[Optional[List], Optional[str]]:
        """Find the main data list in the structure.
        
        Args:
            data: The nested data structure
            paths: List of paths from analyze_structure
            
        Returns:
            Tuple of (main list data, path to list)
        """
        for path in paths:
            parts = path.split('.')
            current = data
            is_list_found = False
            
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part)
                if isinstance(current, list):
                    return current, path[:path.index(part) + len(part)]
                    
        return None, None
        
    def flatten_data(self, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Flatten nested data into a list of dictionaries.
        
        Args:
            data: The nested data structure to flatten
            
        Returns:
            Tuple of (list of flattened records, metadata about the flattening)
            
        Example:
            >>> data = {"users": [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]}
            >>> analyzer = NestedDataAnalyzer()
            >>> records, metadata = analyzer.flatten_data(data)
            >>> print(records[0].keys())
            ['name', 'age']
        """
        paths = self.analyze_structure(data)
        if not paths:
            logger.warning("No paths found in data to flatten")
            return [], {"error": "No paths found"}
            
        main_data, main_path = self.find_main_list(data, paths)
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
            
        # Collect metadata
        metadata = {
            "record_count": len(flattened_records),
            "column_count": len(paths),
            "columns": sorted(flattened_records[0].keys()) if flattened_records else []
        }
        
        return flattened_records, metadata
        
    def profile_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a profile of the data structure.
        
        Args:
            data: The data structure to profile
            
        Returns:
            Dictionary containing profile information
            
        Example:
            >>> data = {"users": [{"name": "John"}, {"name": "Jane"}]}
            >>> analyzer = NestedDataAnalyzer()
            >>> profile = analyzer.profile_data(data)
            >>> print(profile['field_types'])
            {'users.name': 'string'}
        """
        profile = {
            'field_types': {},
            'list_lengths': {},
            'null_counts': {},
            'unique_values': {}
        }
        
        def analyze_field(obj: Any, path: List[str]):
            current_path = '.'.join(path) if path else 'root'
            
            if obj is None:
                profile['null_counts'][current_path] = profile['null_counts'].get(current_path, 0) + 1
                return
                
            if isinstance(obj, dict):
                for key, value in obj.items():
                    analyze_field(value, path + [key])
            elif isinstance(obj, list):
                profile['list_lengths'][current_path] = len(obj)
                for item in obj:
                    analyze_field(item, path)
            else:
                field_type = type(obj).__name__
                profile['field_types'][current_path] = field_type
                
                if isinstance(obj, (str, int, float, bool)):
                    if current_path not in profile['unique_values']:
                        profile['unique_values'][current_path] = set()
                    profile['unique_values'][current_path].add(str(obj))
        
        analyze_field(data, [])
        
        # Convert sets to lists for JSON serialization
        for key in profile['unique_values']:
            profile['unique_values'][key] = list(profile['unique_values'][key])
            
        return profile
