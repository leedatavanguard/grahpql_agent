"""GraphQL-specific DLT handler for data ingestion."""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

import dlt
from pydantic import BaseModel, Field
from aws_xray_sdk.core import xray_recorder

from ingestion.handlers.dlt.dlt_handler import DLTHandler, DLTConfig
from ingestion.utils.graphql_client import GraphQLOAuthClient

# Set up structured logging
logger = logging.getLogger(__name__)

class GraphQLDLTConfig(DLTConfig):
    """Configuration for GraphQL DLT handler."""
    query: str = Field(..., description="GraphQL query to execute")
    variables: Dict[str, Any] = Field(default_factory=dict, description="Query variables")
    endpoint: str = Field(..., description="GraphQL endpoint URL")
    auth: Dict[str, Any] = Field(..., description="Authentication configuration")

class GraphQLDLTHandler(DLTHandler):
    """Handler for loading GraphQL data using DLT.
    
    This handler extends the base DLT handler to support loading data from
    GraphQL endpoints directly into data warehouses using DLT pipelines.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the GraphQL DLT handler.
        
        Args:
            config: Optional configuration dictionary. If not provided,
                   will attempt to load from environment variables.
        """
        # Load GraphQL-specific config from environment if not provided
        if config is None:
            config_str = os.environ.get("TASK_CONFIG")
            if not config_str:
                config = {
                    # DLT base config
                    "pipeline_name": os.environ.get("DLT_PIPELINE_NAME", "default"),
                    "destination": os.environ.get("DLT_DESTINATION", "duckdb"),
                    "schema_name": os.environ.get("DLT_SCHEMA", "public"),
                    "credentials": json.loads(os.environ.get("DLT_CREDENTIALS", "{}")),
                    "config": json.loads(os.environ.get("DLT_CONFIG", "{}")),
                    # GraphQL specific config
                    "query": os.environ.get("GRAPHQL_QUERY", ""),
                    "variables": json.loads(os.environ.get("GRAPHQL_VARIABLES", "{}")),
                    "endpoint": os.environ.get("GRAPHQL_ENDPOINT", ""),
                    "auth": json.loads(os.environ.get("GRAPHQL_AUTH", "{}"))
                }
            else:
                config = json.loads(config_str)
        
        super().__init__(config)
        self.graphql_config = GraphQLDLTConfig(**config)
        self.client = None
        
        logger.info("GraphQL DLT handler initialized", extra={
            'handler': self.handler_name,
            'task': self.task_name,
            'endpoint': self.graphql_config.endpoint,
            'query_length': len(self.graphql_config.query)
        })
    
    def _get_secrets(self) -> Dict[str, str]:
        """Get API credentials from Secrets Manager or environment variables.
        
        Returns:
            Dict containing API credentials
            
        Raises:
            ValueError: If required secrets are missing
        """
        try:
            # Try environment variables first
            secrets = {
                "CLIENT_ID": os.environ.get("GRAPHQL_CLIENT_ID"),
                "CLIENT_SECRET": os.environ.get("GRAPHQL_CLIENT_SECRET"),
                "SCOPE": os.environ.get("GRAPHQL_SCOPE")
            }
            
            # If any required secret is missing, try Secrets Manager
            if not all(secrets.values()):
                secret_name = self.graphql_config.auth.get("secret_name")
                if not secret_name:
                    raise ValueError("No secret name provided in auth config")
                
                import boto3
                client = boto3.client('secretsmanager')
                response = client.get_secret_value(SecretId=secret_name)
                secrets = json.loads(response['SecretString'])
            
            # Validate all required secrets are present
            missing = [k for k, v in secrets.items() if not v]
            if missing:
                raise ValueError(f"Missing required secrets: {', '.join(missing)}")
            
            return secrets
            
        except Exception as e:
            logger.error(f"Failed to get secrets: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            raise
    
    def extract_data(self) -> Any:
        """Extract data from GraphQL endpoint.
        
        Returns:
            The query result data that will be loaded into the pipeline
            
        Raises:
            Exception: If query execution fails
        """
        try:
            # Initialize client if not already done
            if not self.client:
                with xray_recorder.capture('init_client'):
                    secrets = self._get_secrets()
                    self.client = GraphQLOAuthClient(
                        endpoint=self.graphql_config.endpoint,
                        client_id=secrets["CLIENT_ID"],
                        client_secret=secrets["CLIENT_SECRET"],
                        scope=secrets["SCOPE"]
                    )
            
            # Execute query with tracing
            with xray_recorder.capture('execute_query'):
                result = self.client.execute_query(
                    query=self.graphql_config.query,
                    variables=self.graphql_config.variables
                )
                
                logger.info("GraphQL query executed successfully", extra={
                    'handler': self.handler_name,
                    'task': self.task_name,
                    'result_size': len(str(result))
                })
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to extract GraphQL data: {e}", extra={
                'handler': self.handler_name,
                'task': self.task_name,
                'error': str(e)
            })
            raise
