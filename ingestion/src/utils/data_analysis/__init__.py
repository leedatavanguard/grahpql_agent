"""Data Analysis Package

This package provides utilities for analyzing and transforming nested data structures.
It includes tools for structure analysis, data flattening, type inference, and profiling.
"""

from .structure_analyzer import analyze_structure, find_main_list
from .data_flattener import flatten_data
from .type_inference import infer_type, get_field_stats
from .data_profiler import profile_data
from .errors import (
    DataAnalysisError,
    CircularReferenceError,
    InvalidDataStructureError,
    DataTypeError,
    EmptyDataError
)

__all__ = [
    'analyze_structure',
    'find_main_list',
    'flatten_data',
    'infer_type',
    'get_field_stats',
    'profile_data',
    'DataAnalysisError',
    'CircularReferenceError',
    'InvalidDataStructureError',
    'DataTypeError',
    'EmptyDataError'
]
