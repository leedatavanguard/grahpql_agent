"""Data sink implementations."""

import os
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSink(ABC):
    """Abstract base class for data sinks."""
    
    @abstractmethod
    async def write(self, data: Any, key: str):
        """Write data to the sink.
        
        Args:
            data: Data to write
            key: Key/path to write the data to
        """
        pass
        
    @classmethod
    def create(cls, config: Dict[str, Any]) -> 'DataSink':
        """Create a sink instance based on configuration.
        
        Args:
            config: Sink configuration
            
        Returns:
            DataSink instance
        """
        sink_type = config["type"]
        
        if sink_type == "s3":
            return S3Sink(config)
        elif sink_type == "local":
            return LocalSink(config)
        else:
            raise ValueError(f"Unsupported sink type: {sink_type}")

class S3Sink(DataSink):
    """S3 data sink implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize S3 sink.
        
        Args:
            config: Sink configuration with bucket_url and optional key_prefix
        """
        self.bucket = config["bucket_url"]
        self.key_prefix = config.get("key_prefix", "")
        
    async def write(self, data: Any, key: str):
        """Write data to S3.
        
        Args:
            data: Data to write
            key: S3 key suffix
        """
        import boto3
        s3 = boto3.client("s3")
        
        # Construct full key
        full_key = f"{self.key_prefix}/{key}" if self.key_prefix else key
        
        # Convert data to JSON if needed
        if not isinstance(data, (str, bytes)):
            data = json.dumps(data, indent=2)
            
        logger.info(f"Writing to S3: {self.bucket}/{full_key}")
        s3.put_object(
            Bucket=self.bucket,
            Key=full_key.lstrip("/"),
            Body=data
        )

class LocalSink(DataSink):
    """Local filesystem data sink for testing."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize local sink.
        
        Args:
            config: Sink configuration with base_path and optional key_prefix
        """
        self.base_path = Path(config["base_path"])
        self.key_prefix = config.get("key_prefix", "")
        
        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    async def write(self, data: Any, key: str):
        """Write data to local filesystem.
        
        Args:
            data: Data to write
            key: File path suffix
        """
        # Construct full path
        full_path = self.base_path
        if self.key_prefix:
            full_path = full_path / self.key_prefix
        full_path = full_path / key.lstrip("/")
        
        # Create parent directories
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert data to JSON if needed
        if not isinstance(data, (str, bytes)):
            data = json.dumps(data, indent=2)
            
        logger.info(f"Writing to local file: {full_path}")
        full_path.write_text(data)
