"""Error handling utilities for data analysis.

This module defines custom exceptions and error handling utilities
to provide clear and actionable error messages.
"""

class DataAnalysisError(Exception):
    """Base exception for all data analysis errors."""
    pass

class CircularReferenceError(DataAnalysisError):
    """Raised when a circular reference is detected in the data structure."""
    def __init__(self, path: str):
        self.path = path
        super().__init__(f"Circular reference detected at path: {path}")

class InvalidDataStructureError(DataAnalysisError):
    """Raised when the data structure is not in the expected format."""
    def __init__(self, message: str, details: dict = None):
        self.details = details or {}
        super().__init__(message)

class DataTypeError(DataAnalysisError):
    """Raised when data type inference or conversion fails."""
    def __init__(self, value: str, expected_type: str):
        self.value = value
        self.expected_type = expected_type
        super().__init__(f"Could not convert '{value}' to {expected_type}")

class EmptyDataError(DataAnalysisError):
    """Raised when the input data is empty or null."""
    pass
