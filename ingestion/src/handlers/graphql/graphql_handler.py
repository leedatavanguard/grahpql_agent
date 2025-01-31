"""GraphQL handler for data ingestion."""

import os
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, Union, Literal, List, Generic, TypeVar
from datetime import datetime
from pydantic import BaseModel, Field, root_validator
from botocore.exceptions import ClientError
import boto3
import tempfile
import yaml

from ingestion.base.base_handler import BaseHandler
from ingestion.utils.graphql_client import GraphQLOAuthClient, GraphQLQueryLoader
from observability.tracking.job_metrics import JobMetricsTracker
from observability.models.job_metrics import JobType


# Set up structured logging
logger = logging.getLogger(__name__)


class GraphQLError(Exception):
    """Exception raised for GraphQL-specific errors."""
    pass


class NetworkError(Exception):
    """Exception raised for network-related errors."""
    pass


class ValidationError(Exception):
    """Exception raised for configuration validation errors."""
    pass


# Configuration type for type hints
ConfigT = TypeVar('ConfigT', bound=BaseModel)

class SinkConfig(BaseModel):
    """Configuration for data sink."""
    type: Literal['s3'] = Field(description="Type of sink (currently only s3 supported)")
    key_prefix: str = Field(description="Prefix for output files")
    format: Literal['json'] = Field(description="Output format (currently only json supported)")
    compression: Optional[str] = Field(None, description="Compression format (if any)")
    base_path: str = Field(description="Base path for output files")

class RateLimitConfig(BaseModel):
    """Configuration for rate limiting."""
    requests_per_second: int = Field(description="Number of requests allowed per second")
    burst: int = Field(description="Number of requests allowed to burst")

class HandlerConfig(BaseModel):
    """Additional handler configuration."""
    rate_limit: RateLimitConfig = Field(description="Rate limiting configuration")

class GraphQLConfig(BaseModel):
    """Configuration for GraphQL handler."""
    
    query_config: Dict[str, Dict[str, Any]] = Field(
        description="Map of query configurations containing query and default variables"
    )
    query_name: str = Field(
        description="Name of the query to execute from query_config"
    )
    variables: Dict[str, Any] = Field(
        default={},
        description="Variables to merge with default query variables"
    )
    sink: SinkConfig = Field(
        description="Configuration for data sink"
    )
    config: HandlerConfig = Field(
        description="Additional configuration options"
    )
    
    @root_validator(pre=True)
    def validate_query_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate query configuration.
        
        Args:
            values: Configuration values to validate
            
        Returns:
            Validated configuration values
            
        Raises:
            ValueError: If query configuration is invalid
        """
        query_config = values.get('query_config')
        query_name = values.get('query_name')
        
        if not query_config:
            raise ValueError("query_config must be provided")
            
        if not query_name:
            raise ValueError("query_name must be provided")
            
        if query_name not in query_config:
            raise ValueError(f"Query {query_name} not found in query_config")
            
        if 'query' not in query_config[query_name]:
            raise ValueError(f"Query {query_name} must contain a 'query' field")
            
        return values
        
class GraphQLHandler(BaseHandler[GraphQLConfig]):
    """Handler for ingesting data from GraphQL APIs."""
    
    REQUIRED_ENV_VARS = [
        'GRAPHQL_SECRET_ARN',  # Secret ARN containing GraphQL credentials
        'CLICKHOUSE_SECRET_ARN'  # Secret ARN containing ClickHouse credentials
    ]
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize GraphQL handler.
        
        Args:
            config: Handler configuration
            
        Raises:
            ValueError: If configuration is missing or invalid
            EnvironmentError: If required environment variables are missing
        """
        super().__init__()
        
        if not config:
            raise ValueError("Configuration must be provided")
            
        # Initialize base configuration
        self.initialize(GraphQLConfig(**config))
    
    def validate_config(self, config: GraphQLConfig) -> None:
        """Validate handler configuration.
        
        Args:
            config: Handler configuration to validate
            
        Raises:
            ValidationError: If configuration is invalid
        """
        # Validate environment variables
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.environ.get(var)]
        if missing_vars:
            error_msg = (
                f"Missing required environment variables: {', '.join(missing_vars)}. "
                "Please ensure these are set in the task environment"
            )
            logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def initialize(self, config: GraphQLConfig) -> None:
        """Initialize handler with configuration.
        
        Args:
            config: Handler configuration
            
        Raises:
            ValidationError: If configuration is invalid
            Exception: If initialization fails
        """
        try:
            # Store and validate configuration
            self.config = config
            self.validate_config(config)
            
            secrets_client = boto3.client('secretsmanager')
            
            # Load GraphQL credentials from Secrets Manager
            graphql_secrets = self._validate_and_load_secrets(
                secrets_client,
                os.environ['GRAPHQL_SECRET_ARN'],
                ['GRAPHQL_URL', 'AUTH_URL', 'CLIENT_ID', 'CLIENT_SECRET', 'SCOPE'],
                'GraphQL'
            )
            os.environ.update(graphql_secrets)
            
            # Load ClickHouse credentials from Secrets Manager
            clickhouse_secrets = self._validate_and_load_secrets(
                secrets_client,
                os.environ['CLICKHOUSE_SECRET_ARN'],
                ['CH_URL', 'CH_PORT', 'CH_USERNAME', 'CH_PASSWORD', 'CH_DATABASE_NAME'],
                'ClickHouse'
            )
            os.environ.update(clickhouse_secrets)
            
            # Initialize clients
            self.client = GraphQLOAuthClient(
                graphql_url=os.environ['GRAPHQL_URL'],
                token_url=os.environ['AUTH_URL'],
                client_id=os.environ['CLIENT_ID'],
                client_secret=os.environ['CLIENT_SECRET'],
                scope=os.environ['SCOPE']
            )
            
            # Handle query config - can be either a file path or dictionary
            self.temp_config_file = None
            if isinstance(self.config.query_config, str):
                # It's a file path
                self.query_loader = GraphQLQueryLoader(self.config.query_config)
            else:
                # It's a dictionary, write it to a temporary file
                self.temp_config_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
                yaml.dump(self.config.query_config, self.temp_config_file)
                self.query_loader = GraphQLQueryLoader(self.temp_config_file.name)
            
            # Initialize metrics
            self.job_metrics = JobMetricsTracker(
                job_id=os.environ.get('HANDLER_NAME', 'unknown'),
                dataset_id=os.environ.get('TASK_NAME', 'unknown'),
                job_type=JobType.INGESTION
            )
            
            # Initialize rate limiter
            self.rate_limiter = None
            
            # Ensure data directory exists
            self.data_dir = Path(os.environ.get('DATA_DIR', '/tmp/data'))
            self.data_dir.mkdir(parents=True, exist_ok=True)
            
        except Exception as e:
            error_msg = f"Failed to initialize handler: {str(e)}"
            logger.error(error_msg)
            raise
    
    def cleanup(self) -> None:
        """Clean up handler resources."""
        try:
            # Clean up temporary config file if it exists
            if self.temp_config_file:
                try:
                    os.unlink(self.temp_config_file.name)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temporary config file: {str(e)}")
            
            # Clean up data directory
            if self.data_dir.exists():
                try:
                    for file in self.data_dir.glob("*"):
                        file.unlink()
                    self.data_dir.rmdir()
                except Exception as e:
                    logger.warning(f"Failed to cleanup data directory: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
    
    def __del__(self):
        """Destructor to ensure cleanup is called."""
        self.cleanup()

    def handle_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a task with the given configuration.
        
        Args:
            task_config: Task configuration from platform config
            
        Returns:
            Dict containing task results
            
        Raises:
            ValueError: If task configuration is invalid
            Exception: If task execution fails
        """
        try:
            self.initialize(GraphQLConfig(**task_config))
            result = self.execute()
            return self.format_success_response(result)
        except ValidationError as e:
            return self.format_error_response(e)
        except Exception as e:
            return self.format_error_response(e)

    def handle_lambda(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Handle a Lambda invocation.
        
        Args:
            event: Lambda event
            context: Lambda context
            
        Returns:
            Dict containing Lambda response
        """
        try:
            self.initialize(GraphQLConfig(**event))
            result = self.execute()
            return self.format_success_response(result, status_code=200)
        except ValidationError as e:
            return self.format_error_response(e, status_code=400)
        except Exception as e:
            return self.format_error_response(e, status_code=500)

    def process_data(self) -> Dict[str, Any]:
        """Process data from GraphQL API.
        
        Returns:
            API response data
            
        Raises:
            GraphQLError: If there's an error in the GraphQL query
            NetworkError: If there's a network-related error
            Exception: For other unexpected errors
        """
        try:
            self.job_metrics.start()
            
            # Get query configuration
            query_config = self.config.query_config[self.config.query_name]
            query = query_config['query']
            
            # Merge default variables with provided variables
            variables = {**query_config.get('variables', {}), **self.config.variables}
            
            # Execute query
            logger.info(f"Executing GraphQL query: {self.config.query_name}")
            result = self.client.execute_query(query, variables)
            
            if result.get('errors'):
                error_msg = f"GraphQL query returned errors: {result['errors']}"
                logger.error(error_msg)
                self.job_metrics.end(status="error", error=error_msg)
                raise GraphQLError(error_msg)
                
            self.job_metrics.end(status="completed")
            return result
            
        except GraphQLError as e:
            error_msg = f"GraphQL query error: {str(e)}"
            logger.error(error_msg)
            self.job_metrics.end(status="error", error=error_msg)
            raise
        except NetworkError as e:
            error_msg = f"Network error during query: {str(e)}"
            logger.error(error_msg)
            self.job_metrics.end(status="error", error=error_msg)
            raise
        except Exception as e:
            error_msg = f"Unexpected error during query: {str(e)}"
            logger.error(error_msg)
            self.job_metrics.end(status="error", error=error_msg)
            raise

    def execute(self) -> Dict[str, Any]:
        """Execute the GraphQL query and return results.
        
        Returns:
            Dict containing query results
            
        Raises:
            Exception: If query execution fails
        """
        try:
            return self.process_data()
        except Exception as e:
            logger.error(f"Failed to execute GraphQL query: {str(e)}")
            raise

    def run(self) -> None:
        """Run the handler."""
        try:
            # Process data
            data = self.process_data()
            
            # Write to sink
            if self.config.sink.type == 's3':
                output_path = self.data_dir / f"{self.config.sink.key_prefix}.json"
                with open(output_path, 'w') as f:
                    json.dump(data, f)
                    
            else:
                raise ValueError(f"Unsupported sink type: {self.config.sink.type}")
                
        except Exception as e:
            logger.error(f"Error running GraphQL handler: {str(e)}")
            raise

    def _validate_and_load_secrets(self, secrets_client: Any, secret_arn: str, required_secrets: List[str], secret_type: str) -> Dict[str, str]:
        """Validate and load secrets from AWS Secrets Manager.
        
        Args:
            secrets_client: AWS Secrets Manager client
            secret_arn: ARN of the secret to load
            required_secrets: List of required secret keys
            secret_type: Type of secrets (e.g., 'GraphQL' or 'ClickHouse')
            
        Returns:
            Dict containing the secret values
            
        Raises:
            EnvironmentError: If there are issues loading or validating secrets
        """
        try:
            secret_value = secrets_client.get_secret_value(SecretId=secret_arn)
            secret_data = json.loads(secret_value['SecretString'])
            
            logger.info(f"Loaded {secret_type} secret data: {json.dumps(secret_data, indent=2)}")
            
            # Validate all required secrets are present and not None
            missing_secrets = []
            none_secrets = []
            for secret in required_secrets:
                if secret not in secret_data:
                    missing_secrets.append(secret)
                elif secret_data[secret] is None:
                    none_secrets.append(secret)
                    logger.error(f"{secret_type} secret {secret} has None value")
            
            error_messages = []
            if missing_secrets:
                error_messages.append(f"Missing required {secret_type} secrets: {', '.join(missing_secrets)}")
            if none_secrets:
                error_messages.append(f"Required {secret_type} secrets have None values: {', '.join(none_secrets)}")
            
            if error_messages:
                raise ValueError('\n'.join(error_messages))
            
            return secret_data
            
        except (ClientError, json.JSONDecodeError, KeyError) as e:
            error_msg = f"Error loading {secret_type} credentials from Secrets Manager: {str(e)}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)
