"""GraphQL data flattening utilities.

This module provides robust data flattening capabilities for GraphQL responses,
with circular reference protection and schema information collection.
"""

from typing import Any, Dict, List, Set, Tuple, Optional
from collections import defaultdict
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class CircularReferenceError(Exception):
    """Raised when a circular reference is detected during flattening."""
    pass

class DataFlattener:
    """Handles flattening of nested data structures with schema collection."""
    
    def __init__(self, max_depth: int = 10):
        """Initialize the flattener.
        
        Args:
            max_depth: Maximum nesting depth to process
        """
        self.max_depth = max_depth
        self.path_tracker: Set[str] = set()
        self.schema_info: Dict[str, Dict[str, Any]] = {}
        self.quality_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
    def _track_schema(self, path: str, value: Any) -> None:
        """Track schema information for a field.
        
        Args:
            path: Field path
            value: Field value
        """
        if path not in self.schema_info:
            self.schema_info[path] = {
                'type': type(value).__name__,
                'sample': str(value)[:100] if value is not None else None,
                'null_count': 1 if value is None else 0,
                'total_count': 1
            }
        else:
            self.schema_info[path]['total_count'] += 1
            if value is None:
                self.schema_info[path]['null_count'] += 1
                
    def _track_quality(self, path: str, value: Any) -> None:
        """Track data quality metrics for a field.
        
        Args:
            path: Field path
            value: Field value
        """
        metrics = self.quality_metrics[path]
        
        # Track null values
        if 'null_count' not in metrics:
            metrics['null_count'] = 0
        if value is None:
            metrics['null_count'] += 1
            
        # Track unique values
        if value is not None:
            if 'unique_values' not in metrics:
                metrics['unique_values'] = set()
            if len(metrics['unique_values']) < 1000:  # Limit unique value tracking
                metrics['unique_values'].add(str(value))
                
        # Track value length for strings
        if isinstance(value, str):
            if 'max_length' not in metrics:
                metrics['max_length'] = 0
            metrics['max_length'] = max(metrics['max_length'], len(value))
            
    def _flatten_dict(
        self,
        d: Dict[str, Any],
        parent_key: str = '',
        sep: str = '.',
        depth: int = 0
    ) -> List[Dict[str, Any]]:
        """Recursively flatten a dictionary with circular reference protection.
        
        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested fields
            sep: Separator for nested keys
            depth: Current recursion depth
            
        Returns:
            List of flattened dictionaries
        """
        if depth > self.max_depth:
            logger.warning(f"Max depth {self.max_depth} exceeded at {parent_key}")
            return [{}]
            
        items: List[Dict[str, Any]] = []
        
        # Handle dictionary
        if isinstance(d, dict):
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                # Track schema and quality
                self._track_schema(new_key, v)
                self._track_quality(new_key, v)
                
                # Handle nested structures
                if isinstance(v, (dict, list)):
                    path_key = f"{new_key}:{id(v)}"
                    if path_key in self.path_tracker:
                        raise CircularReferenceError(f"Circular reference detected at {new_key}")
                    self.path_tracker.add(path_key)
                    
                    try:
                        if isinstance(v, dict):
                            nested_items = self._flatten_dict(v, new_key, sep, depth + 1)
                        else:  # list
                            nested_items = self._flatten_list(v, new_key, sep, depth + 1)
                        items.extend(nested_items)
                    finally:
                        self.path_tracker.remove(path_key)
                else:
                    items.append({new_key: v})
                    
        return items if items else [{}]
        
    def _flatten_list(
        self,
        lst: List[Any],
        parent_key: str = '',
        sep: str = '.',
        depth: int = 0
    ) -> List[Dict[str, Any]]:
        """Flatten a list of items.
        
        Args:
            lst: List to flatten
            parent_key: Parent key for nested fields
            sep: Separator for nested keys
            depth: Current recursion depth
            
        Returns:
            List of flattened dictionaries
        """
        items: List[Dict[str, Any]] = []
        
        for i, item in enumerate(lst):
            if isinstance(item, dict):
                items.extend(self._flatten_dict(item, parent_key, sep, depth))
            elif isinstance(item, list):
                items.extend(self._flatten_list(item, parent_key, sep, depth + 1))
            else:
                key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                self._track_schema(key, item)
                self._track_quality(key, item)
                items.append({key: item})
                
        return items if items else [{}]
        
    def flatten(self, data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Flatten nested data structure and collect schema information.
        
        Args:
            data: Nested data structure to flatten
            
        Returns:
            Tuple of (flattened_data, metadata)
        """
        try:
            # Reset trackers
            self.path_tracker.clear()
            self.schema_info.clear()
            self.quality_metrics.clear()
            
            # Flatten the data
            flattened = self._flatten_dict(data)
            
            # Combine all dictionaries
            result = []
            for item in flattened:
                if item:  # Skip empty dictionaries
                    combined = {}
                    for d in flattened:
                        combined.update(d)
                    result.append(combined)
                    
            # Prepare metadata
            metadata = {
                'schema': self.schema_info,
                'quality': {
                    k: {
                        'null_count': v['null_count'],
                        'unique_count': len(v['unique_values']) if 'unique_values' in v else 0,
                        'max_length': v.get('max_length')
                    }
                    for k, v in self.quality_metrics.items()
                },
                'timestamp': datetime.now().isoformat(),
                'record_count': len(result)
            }
            
            return result, metadata
            
        except CircularReferenceError as e:
            logger.error(f"Circular reference detected: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Flattening failed: {str(e)}")
            raise
