"""DLT handler for data ingestion with DLT pipeline support."""

import os
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime

import dlt
from pydantic import BaseModel, Field

from ingestion.base.base_handler import BaseHandler
from ingestion.utils.graphql_client import GraphQLOAuthClient
from observability.clickhouse.src.models.job_metrics import JobMetrics, JobLog, JobType, JobLogType
from ingestion.utils.metrics import TaskMetrics

# Set up structured logging
logger = logging.getLogger(__name__)

class DLTConfig(BaseModel):
    """Configuration for DLT pipeline.
    
    All fields are required to ensure proper pipeline setup and execution.
    No default values are provided to enforce explicit configuration.
    """
    pipeline_name: str = Field(..., description="Name of the DLT pipeline")
    destination: str = Field(..., description="Destination for the pipeline (e.g., 'duckdb', 'athena')")
    schema_name: str = Field(..., description="Schema name in the destination")
    credentials: Dict[str, Any] = Field(..., description="Credentials for the destination")
    config: Dict[str, Any] = Field(..., description="Additional pipeline configuration")

class DLTHandler(BaseHandler):
    """Base handler for DLT-enabled data ingestion.
    
    This handler provides a foundation for building data pipelines using DLT.
    It handles pipeline setup, basic configuration, and metric collection while
    maintaining compatibility with existing ingestion infrastructure.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the DLT handler.
        
        Args:
            config: Configuration dictionary containing all required fields.
                   Must include pipeline_name, destination, schema_name,
                   credentials, and config.
                   
        Raises:
            ValueError: If config is None or missing required fields
        """
        if config is None:
            raise ValueError("Configuration must be provided")
            
        self.handler_name = os.environ.get("HANDLER_NAME", self.__class__.__name__)
        self.task_name = os.environ.get("TASK_NAME", "UnknownTask")
        self.metrics = TaskMetrics(self.handler_name, self.task_name)
        
        logger.info("DLT handler configuration received", extra={
            'handler': self.handler_name,
            'task': self.task_name,
            'pipeline_name': config.get('pipeline_name'),
            'destination': config.get('destination')
        })
        
        try:
            self.config = DLTConfig(**config)
        except Exception as e:
            logger.error(f"Invalid configuration: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            raise ValueError(f"Invalid configuration: {e}")
            
        self._setup_pipeline()
    
    def _setup_pipeline(self) -> None:
        """Initialize the DLT pipeline with configuration."""
        try:
            self.pipeline = dlt.pipeline(
                pipeline_name=self.config.pipeline_name,
                destination=self.config.destination,
                schema_name=self.config.schema_name,
                credentials=self.config.credentials,
                **self.config.config
            )
            logger.info("DLT pipeline initialized successfully", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'pipeline_name': self.config.pipeline_name
            })
        except Exception as e:
            logger.error(f"Failed to initialize DLT pipeline: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            raise
    
    @abstractmethod
    def extract_data(self) -> Any:
        """Extract data for the pipeline. Must be implemented by subclasses."""
        pass
    
    def handle_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a task with the given configuration.
        
        Args:
            task_config: Task configuration from platform config
            
        Returns:
            Dict containing task results
        """
        try:
            self.metrics.start_task()
            
            # Get DLT-specific options
            dlt_options = task_config.get('dlt_options', {})
            table_name = dlt_options.get('table_name', 'default')
            write_disposition = 'merge' if task_config.get('incremental', False) else 'replace'
            
            # Extract data and load into pipeline
            data = self.extract_data()
            load_info = self.pipeline.run(
                data,
                table_name=table_name,
                write_disposition=write_disposition
            )
            
            result = {
                "status": "success",
                "pipeline_name": self.config.pipeline_name,
                "destination": self.config.destination,
                "table_name": table_name,
                "write_disposition": write_disposition,
                "load_info": load_info.dict()
            }
            
            self.metrics.end_task("success")
            return result
            
        except Exception as e:
            logger.error(f"Task failed: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            self.metrics.end_task("error", str(e))
            return {
                "status": "error",
                "error": str(e)
            }
    
    def handle_lambda(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Handle a Lambda invocation.
        
        Args:
            event: Lambda event
            context: Lambda context
            
        Returns:
            Dict containing Lambda response
        """
        try:
            task_config = event.get('task_config', {})
            return self.handle_task(task_config)
        except Exception as e:
            logger.error(f"Lambda handler failed: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            return {
                "status": "error",
                "error": str(e)
            }
