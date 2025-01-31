"""Data processor for analyzing and flattening nested data structures."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from .errors import CircularReferenceError, EmptyDataError, InvalidDataStructureError

logger = logging.getLogger(__name__)

class DataProcessor:
    """Processor for analyzing and flattening nested data structures."""

    def __init__(self, max_depth: int = 10):
        """Initialize the processor.
        
        Args:
            max_depth: Maximum depth to traverse when flattening
        """
        self.max_depth = max_depth
        self.field_stats = {}
        self.field_paths = set()
        
    def analyze_structure(
            self,
            obj: Any,
            prefix: str = "",
            depth: int = 0,
            visited: Optional[Set[int]] = None
        ) -> None:
        """Analyze the structure of a data object.
        
        Args:
            obj: Object to analyze
            prefix: Current path prefix
            depth: Current recursion depth
            visited: Set of visited object ids
            
        Raises:
            CircularReferenceError: If circular reference detected
            InvalidDataStructureError: If structure is invalid
        """
        if visited is None:
            visited = set()
            
        if depth > self.max_depth:
            logger.warning(f"Max depth {self.max_depth} exceeded at {prefix}")
            return
            
        # Handle null values
        if obj is None:
            self._update_field_stats(prefix, None)
            return
            
        # Check for circular references
        obj_id = id(obj)
        if obj_id in visited:
            raise CircularReferenceError(f"Circular reference detected at {prefix}")
        visited.add(obj_id)
        
        try:
            if isinstance(obj, (str, int, float, bool)):
                self._update_field_stats(prefix, obj)
                
            elif isinstance(obj, dict):
                for key, value in obj.items():
                    new_prefix = f"{prefix}.{key}" if prefix else key
                    self.analyze_structure(value, new_prefix, depth + 1, visited)
                    
            elif isinstance(obj, (list, tuple)):
                if obj:  # Only analyze non-empty sequences
                    self.analyze_structure(obj[0], prefix, depth + 1, visited)
                    
            else:
                raise InvalidDataStructureError(
                    f"Unsupported type {type(obj)} at {prefix}"
                )
                
        except Exception as e:
            logger.warning(f"Failed to process field: {str(e)}")
            
        finally:
            visited.remove(obj_id)
            
    def _update_field_stats(self, path: str, value: Any) -> None:
        """Update statistics for a field.
        
        Args:
            path: Field path
            value: Field value
        """
        if not path:
            return
            
        self.field_paths.add(path)
        
        if path not in self.field_stats:
            self.field_stats[path] = {
                'count': 0,
                'null_count': 0,
                'type': None,
                'unique_values': set(),
            }
            
        try:
            stats = self.field_stats[path]
            stats['count'] += 1
            
            if value is None:
                stats['null_count'] += 1
            else:
                # Infer type
                if stats['type'] is None:
                    if isinstance(value, bool):
                        stats['type'] = 'bool'
                    elif isinstance(value, int):
                        stats['type'] = 'int'
                    elif isinstance(value, float):
                        stats['type'] = 'float'
                    else:
                        stats['type'] = 'str'
                        stats['max_length'] = len(str(value))
                elif stats['type'] != 'str':
                    if not isinstance(value, eval(stats['type'])):
                        stats['type'] = 'str'
                        stats['max_length'] = len(str(value))
                elif stats['type'] == 'str':
                    current_len = len(str(value))
                    if 'max_length' not in stats or current_len > stats['max_length']:
                        stats['max_length'] = current_len
                        
                stats['unique_values'].add(str(value))
                
        except Exception as e:
            logger.warning(f"Failed to process field: {str(e)}")
            
    def get_field_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all fields.
        
        Returns:
            Dictionary mapping field paths to their statistics
        """
        stats = {}
        for path, field_stats in self.field_stats.items():
            stats[path] = {
                'count': field_stats['count'],
                'null_count': field_stats['null_count'],
                'type': field_stats['type'],
                'unique_count': len(field_stats['unique_values']),
            }
            if 'max_length' in field_stats:
                stats[path]['max_length'] = field_stats['max_length']
        return stats
        
    def get_field_paths(self) -> Set[str]:
        """Get all field paths found during analysis.
        
        Returns:
            Set of field paths
        """
        return self.field_paths.copy()
        
    def flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten nested data structures.
        
        Args:
            data: List of records to flatten
            
        Returns:
            List of flattened records
            
        Raises:
            EmptyDataError: If input data is empty
            CircularReferenceError: If circular reference detected
        """
        if not data:
            raise EmptyDataError("No data to flatten")
            
        try:
            # Reset stats for new analysis
            self.field_stats = {}
            self.field_paths = set()
            
            # Analyze structure using first record
            self.analyze_structure(data[0])
            
            # Flatten all records
            flattened = []
            for record in data:
                flat_record = {}
                for path in self.field_paths:
                    value = self._get_nested_value(record, path.split('.'))
                    flat_record[path] = value
                flattened.append(flat_record)
                
            return flattened
            
        except Exception as e:
            logger.error(f"Flattening failed: {str(e)}")
            raise
            
    def _get_nested_value(self, obj: Any, path: List[str]) -> Any:
        """Get value from nested object using path.
        
        Args:
            obj: Object to traverse
            path: List of path components
            
        Returns:
            Value at path
        """
        for key in path:
            if obj is None:
                return None
            if isinstance(obj, (list, tuple)):
                obj = obj[0] if obj else None
            if isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
        return obj
