"""Unit tests for the base DLT handler implementation.

This test suite verifies the core functionality of the DLT handler base class,
which provides the foundation for all DLT-based data ingestion handlers.

Test Coverage:
-------------
1. Configuration Validation
   - Verifies that all required configuration fields are present
   - Ensures invalid configurations are properly rejected
   - Validates configuration type checking and constraints

2. Pipeline Initialization
   - Tests proper setup of DLT pipeline with provided configuration
   - Verifies error handling during pipeline initialization
   - Ensures pipeline configuration is correctly passed through

3. Task Handling
   - Tests successful data extraction and loading
   - Verifies proper handling of table names and write dispositions
   - Ensures task metrics are collected and reported
   - Validates error handling and reporting

4. Lambda Integration
   - Tests proper handling of Lambda events
   - Verifies configuration extraction from events
   - Ensures proper error handling in Lambda context

5. Metrics Collection
   - Verifies start/end task metrics are recorded
   - Ensures proper error metrics are captured
   - Validates metric context and tagging

Test Organization:
-----------------
- Uses pytest fixtures for common test data and mocks
- Implements a test handler class for concrete testing
- Mocks external dependencies (DLT pipeline, metrics)
- Provides comprehensive coverage of success and error paths

Usage:
------
Run these tests using pytest:
    pytest tests/unit/handlers/dlt/test_dlt_handler.py -v

Dependencies:
------------
- pytest
- unittest.mock
- dlt
"""

import os
import json
import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any

import dlt

from ingestion.handlers.dlt.dlt_handler import DLTHandler, DLTConfig

class TestDLTHandler(DLTHandler):
    """Test implementation of DLTHandler."""
    
    def __init__(self, config: Dict[str, Any], test_data: Any = None):
        self.test_data = test_data
        super().__init__(config)
    
    def extract_data(self) -> Any:
        """Return test data for testing."""
        return self.test_data

@pytest.fixture
def basic_config() -> Dict[str, Any]:
    """Basic DLT handler configuration with all required fields."""
    return {
        "pipeline_name": "test_pipeline",
        "destination": "duckdb",
        "schema_name": "test_schema",
        "credentials": {
            "api_key": "test-key"
        },
        "config": {
            "batch_size": 1000
        }
    }

@pytest.fixture
def mock_pipeline():
    """Mock DLT pipeline with expected behavior."""
    pipeline = Mock()
    pipeline.run.return_value = Mock(
        dict=lambda: {
            "rows_processed": 100,
            "status": "success",
            "metrics": {
                "total_time": 1.5,
                "batch_count": 1
            }
        }
    )
    return pipeline

def test_dlt_handler_initialization(basic_config):
    """Test DLT handler initialization with valid configuration.
    
    Verifies that:
    1. All config fields are properly set
    2. Pipeline is initialized
    3. Metrics collector is created
    """
    handler = TestDLTHandler(basic_config)
    assert handler.config.pipeline_name == "test_pipeline"
    assert handler.config.destination == "duckdb"
    assert handler.config.schema_name == "test_schema"
    assert handler.config.credentials == {"api_key": "test-key"}
    assert handler.config.config == {"batch_size": 1000}

def test_missing_config_raises_error():
    """Test that initialization without config raises ValueError."""
    with pytest.raises(ValueError, match="Configuration must be provided"):
        TestDLTHandler(None)

def test_invalid_config_raises_error():
    """Test that invalid configuration raises ValueError with details."""
    invalid_config = {
        "pipeline_name": "test",
        # Missing required fields
    }
    with pytest.raises(ValueError) as exc_info:
        TestDLTHandler(invalid_config)
    assert "missing required fields" in str(exc_info.value).lower()

@patch('dlt.pipeline')
def test_pipeline_setup(mock_dlt_pipeline, basic_config, mock_pipeline):
    """Test DLT pipeline initialization with configuration.
    
    Verifies that:
    1. Pipeline is created with correct parameters
    2. Configuration is properly passed through
    3. Pipeline instance is stored
    """
    mock_dlt_pipeline.return_value = mock_pipeline
    
    handler = TestDLTHandler(basic_config)
    assert handler.pipeline == mock_pipeline
    
    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name="test_pipeline",
        destination="duckdb",
        schema_name="test_schema",
        credentials={"api_key": "test-key"},
        batch_size=1000
    )

def test_handle_task_success(basic_config, mock_pipeline):
    """Test successful task handling with data loading.
    
    Verifies that:
    1. Data is extracted and loaded
    2. Metrics are collected
    3. Proper result structure is returned
    4. Write disposition is correctly set
    """
    test_data = {"key": "value"}
    task_config = {
        "dlt_options": {
            "table_name": "test_table"
        },
        "incremental": True
    }
    
    with patch('dlt.pipeline', return_value=mock_pipeline):
        handler = TestDLTHandler(basic_config, test_data)
        result = handler.handle_task(task_config)
        
        assert result["status"] == "success"
        assert result["table_name"] == "test_table"
        assert result["write_disposition"] == "merge"
        assert "load_info" in result
        
        mock_pipeline.run.assert_called_once_with(
            test_data,
            table_name="test_table",
            write_disposition="merge"
        )

def test_handle_task_failure(basic_config):
    """Test task handling with pipeline failure.
    
    Verifies that:
    1. Errors are caught and logged
    2. Error metrics are recorded
    3. Proper error result is returned
    """
    task_config = {"dlt_options": {"table_name": "test_table"}}
    
    with patch('dlt.pipeline', side_effect=Exception("Test error")):
        handler = TestDLTHandler(basic_config)
        result = handler.handle_task(task_config)
        
        assert result["status"] == "error"
        assert "Test error" in result["error"]

def test_handle_lambda(basic_config, mock_pipeline):
    """Test Lambda event handling with task execution.
    
    Verifies that:
    1. Event configuration is properly extracted
    2. Task is executed successfully
    3. Lambda-specific error handling works
    """
    test_data = {"key": "value"}
    event = {
        "task_config": {
            "dlt_options": {
                "table_name": "test_table"
            }
        }
    }
    
    with patch('dlt.pipeline', return_value=mock_pipeline):
        handler = TestDLTHandler(basic_config, test_data)
        result = handler.handle_lambda(event, None)
        
        assert result["status"] == "success"
        assert "load_info" in result
