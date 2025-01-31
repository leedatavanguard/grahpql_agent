"""Structure Analysis Module

This module provides utilities for analyzing the structure of nested data.
It focuses on extracting paths, identifying patterns, and understanding
the hierarchical organization of data.

Example:
    >>> data = {"users": [{"name": "John", "addresses": [{"city": "NY"}]}]}
    >>> paths = analyze_structure(data)
    >>> print(paths)
    ['users.name', 'users.addresses.city']
"""

from typing import Any, Dict, List, Set, Optional
import logging
from collections import deque

from .constants import MAX_LIST_SAMPLE_SIZE
from .errors import CircularReferenceError, InvalidDataStructureError, EmptyDataError

logger = logging.getLogger(__name__)

def analyze_structure(obj: Any, prefix: str = "", visited: Optional[Set[int]] = None) -> List[str]:
    """Recursively analyze structure and generate dot-notation paths.
    
    This function traverses a nested data structure and generates a list of paths
    that can be used to access each leaf node. It handles circular references and
    provides detailed logging for debugging.
    
    Args:
        obj: Object to analyze (dict, list, or primitive type)
        prefix: Current path prefix in dot notation
        visited: Set of object ids to prevent circular references
        
    Returns:
        List of dot-notation paths to all leaf nodes
        
    Raises:
        CircularReferenceError: If a circular reference is detected
        InvalidDataStructureError: If the data structure is malformed
        EmptyDataError: If the input data is empty
        
    Example:
        >>> data = {
        ...     "user": {
        ...         "name": "John",
        ...         "addresses": [{"city": "NY"}]
        ...     }
        ... }
        >>> paths = analyze_structure(data)
        >>> print(paths)
        ['user.name', 'user.addresses.city']
        
    Notes:
        - For lists, only the first MAX_LIST_SAMPLE_SIZE items are sampled
        - Circular references are detected and logged
        - All exceptions during traversal are caught and logged
    """
    if obj is None:
        raise EmptyDataError("Cannot analyze None value")
        
    if visited is None:
        visited = set()
        
    obj_id = id(obj)
    if obj_id in visited:
        raise CircularReferenceError(prefix)
    visited.add(obj_id)
    
    paths = []
    try:
        if isinstance(obj, dict):
            for key, value in sorted(obj.items()):
                new_prefix = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    paths.extend(analyze_structure(value, new_prefix, visited))
                else:
                    paths.append(new_prefix)
        elif isinstance(obj, list) and obj:
            # Sample first MAX_LIST_SAMPLE_SIZE items to infer structure
            all_paths = set()
            for i, item in enumerate(obj[:MAX_LIST_SAMPLE_SIZE]):
                try:
                    item_paths = analyze_structure(item, prefix, visited)
                    all_paths.update(item_paths)
                except Exception as e:
                    logger.warning(f"Error analyzing list item {i} at {prefix}: {str(e)}")
            paths.extend(sorted(all_paths))
        elif not isinstance(obj, (str, int, float, bool)):
            raise InvalidDataStructureError(
                f"Unsupported type at {prefix}: {type(obj)}",
                {"type": str(type(obj)), "path": prefix}
            )
    except (CircularReferenceError, InvalidDataStructureError, EmptyDataError):
        raise
    except Exception as e:
        logger.error(f"Error analyzing structure at {prefix}: {str(e)}")
        raise InvalidDataStructureError(
            f"Failed to analyze structure at {prefix}: {str(e)}",
            {"error": str(e), "path": prefix}
        )
    finally:
        visited.remove(obj_id)
        
    return paths

def find_main_list(data: Dict[str, Any], paths: List[str]) -> tuple[Optional[List], Optional[str]]:
    """Find the primary data list in a nested structure.
    
    This function attempts to identify the main list of records in a data structure,
    which is typically the core data being returned (e.g., list of users, products, etc.).
    
    Args:
        data: The nested data structure to analyze
        paths: List of paths from analyze_structure()
        
    Returns:
        Tuple of (main list data, path to list)
        
    Raises:
        InvalidDataStructureError: If the data structure is malformed
        EmptyDataError: If the input data is empty
        
    Example:
        >>> data = {
        ...     "data": {
        ...         "users": [
        ...             {"id": 1, "name": "John"},
        ...             {"id": 2, "name": "Jane"}
        ...         ]
        ...     }
        ... }
        >>> paths = analyze_structure(data)
        >>> main_list, path = find_main_list(data, paths)
        >>> print(path)
        'data.users'
        >>> print(len(main_list))
        2
        
    Notes:
        - Returns (None, None) if no suitable list is found
        - Prioritizes longer lists over shorter ones
        - Handles nested dictionaries and lists
    """
    if not data:
        raise EmptyDataError("Input data is empty")
        
    if not paths:
        raise InvalidDataStructureError("No paths found in data structure")
        
    candidates = []
    
    for path in paths:
        parts = path.split('.')
        current = data
        
        try:
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part)
                    if isinstance(current, list):
                        candidates.append((current, path[:path.index(part) + len(part)]))
                        break
                else:
                    break
        except Exception as e:
            logger.warning(f"Error traversing path {path}: {str(e)}")
                
    if not candidates:
        return None, None
        
    # Choose the longest list as the main data
    main_list, path = max(candidates, key=lambda x: len(x[0]))
    return main_list, path
