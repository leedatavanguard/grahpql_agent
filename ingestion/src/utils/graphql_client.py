"""GraphQL client with OAuth2 authentication."""

import os
import json
import gzip
import boto3
import logging
import yaml
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union, BinaryIO, Tuple

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

logger = logging.getLogger(__name__)

class GraphQLQueryLoader:
    """Loads and manages GraphQL queries from a YAML configuration file."""
    
    def __init__(self, config_path: Union[str, Path]):
        """Initialize the query loader.
        
        Args:
            config_path: Path to the YAML configuration file containing queries
        """
        self.config_path = Path(config_path)
        self.queries = {}
        self._load_queries()
        
    def _load_queries(self):
        """Load queries from the configuration file."""
        with open(self.config_path) as f:
            self.queries = yaml.safe_load(f)
            
        # Validate query structure
        for name, query_data in self.queries.items():
            if not isinstance(query_data, dict):
                raise ValueError(f"Query {name} must be a dictionary")
            if 'query' not in query_data:
                raise ValueError(f"Query {name} missing 'query' field")
                
    def get_query(self, name: str) -> Tuple[str, Dict[str, Any]]:
        """Get a query by name.
        
        Args:
            name: Name of the query to retrieve
            
        Returns:
            Tuple of (query string, variables dict)
            
        Raises:
            KeyError: If query name not found
        """
        if name not in self.queries:
            raise KeyError(f"Query {name} not found in config")
            
        query_data = self.queries[name]
        return query_data['query'], query_data.get('variables', {})

class GraphQLOAuthClient:
    def __init__(
        self,
        graphql_url: str,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
        data_dir: Optional[Path] = None,
        sink_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize GraphQL client with OAuth2 authentication.
        
        Args:
            graphql_url: The GraphQL endpoint URL
            token_url: OAuth2 token endpoint URL
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            scope: OAuth2 scope (if required)
            data_dir: Directory for storing data (if using local sink)
            sink_config: Configuration for data sink (type, key_prefix, etc.)
        """
        self.graphql_url = graphql_url
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token = None
        self.client = None
        self.data_dir = data_dir or Path("/tmp/data")
        self.sink_config = sink_config or {"type": "local", "key_prefix": "data"}
        
        # Initialize S3 client if needed
        self._s3_client = None
        if self.sink_config.get("type") == "s3":
            self._s3_client = boto3.client("s3")
        
        # Allow insecure transport for testing
        if os.environ.get('TESTING'):
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

    def _get_oauth_token(self) -> Dict[str, Any]:
        """Get OAuth2 token using client credentials flow.
        
        Returns:
            OAuth2 token information
            
        Raises:
            Exception: If token retrieval fails
        """
        try:
            oauth2_client = BackendApplicationClient(client_id=self.client_id)
            oauth = OAuth2Session(
                client=oauth2_client,
                scope=self.scope
            )
            
            self.token = oauth.fetch_token(
                token_url=self.token_url,
                client_id=self.client_id,
                client_secret=self.client_secret,
                include_client_id=True,
                scope=self.scope
            )
            
            return self.token
            
        except Exception as e:
            logger.error(f"Failed to get OAuth token: {str(e)}")
            raise

    def _setup_client(self) -> None:
        """Setup GQL client with OAuth2 authentication.
        
        Raises:
            Exception: If client setup fails
        """
        try:
            if not self.token:
                self._get_oauth_token()
            
            transport = RequestsHTTPTransport(
                url=self.graphql_url,
                headers={
                    'Authorization': f'Bearer {self.token["access_token"]}',
                    'Content-Type': 'application/json',
                },
                verify=not os.environ.get('TESTING', False),
                retries=3,
            )
            
            self.client = Client(
                transport=transport,
                fetch_schema_from_transport=True
            )
            
        except Exception as e:
            logger.error(f"Failed to setup GraphQL client: {str(e)}")
            raise

    def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute GraphQL query with authentication.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            Query response
            
        Raises:
            Exception: If query execution fails
        """
        try:
            if not self.client:
                self._setup_client()
            
            parsed_query = gql(query)
            result = self.client.execute(parsed_query, variable_values=variables)
            
            # Wrap result in data field to match GraphQL convention
            return {"data": result}
            
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise

    def _get_output_key(self) -> str:
        """Get output key for storing query results.
        
        Returns:
            S3 key or local path for output file
        """
        # Get path components
        base_path = self.sink_config.get("base_path", "data")
        key_prefix = self.sink_config.get("key_prefix", "")
        format = self.sink_config.get("format", "json")
        compression = self.sink_config.get("compression")
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Build filename with format and optional compression
        filename = f"{key_prefix}_{timestamp}.{format}"
        if compression == "gzip":
            filename += ".gz"
            
        # Combine parts for full path/key
        parts = [base_path]
        if key_prefix:
            parts.append(key_prefix)
        parts.append(filename)
        
        return "/".join(str(p) for p in parts if p)

    def _get_output_path(self) -> Path:
        """Get output path for storing query results locally.
        
        Returns:
            Path to output file
            
        Raises:
            ValueError: If sink type is not supported
        """
        sink_type = self.sink_config.get("type", "local")
        if sink_type not in ["local", "s3"]:
            raise ValueError(f"Unsupported sink type: {sink_type}")
        
        output_key = self._get_output_key()
        return self.data_dir / output_key

    def _write_data(self, file: Union[BinaryIO, Path], data: Dict[str, Any]) -> None:
        """Write data to file with appropriate format and compression.
        
        Args:
            file: File object or path to write to
            data: Data to write
            
        Raises:
            Exception: If writing fails
        """
        format = self.sink_config.get("format", "json")
        compression = self.sink_config.get("compression")
        
        try:
            # Convert data to appropriate format
            if format == "json":
                content = json.dumps(data, indent=2)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Write with appropriate compression
            if compression == "gzip":
                if isinstance(file, (str, Path)):
                    with gzip.open(file, 'wt') as f:
                        f.write(content)
                else:
                    with gzip.GzipFile(fileobj=file, mode='w') as gz:
                        gz.write(content.encode('utf-8'))
            else:
                if isinstance(file, (str, Path)):
                    with open(file, 'w') as f:
                        f.write(content)
                else:
                    file.write(content.encode('utf-8'))
                    
        except Exception as e:
            logger.error(f"Failed to write data: {str(e)}")
            raise

    def save_results(self, data: Dict[str, Any]) -> Union[Path, str]:
        """Save query results to configured sink.
        
        Args:
            data: Query results to save
            
        Returns:
            Path where data was saved (local path or S3 URI)
            
        Raises:
            Exception: If saving fails
        """
        try:
            sink_type = self.sink_config.get("type", "local")
            output_key = self._get_output_key()
            
            if sink_type == "s3":
                # Get S3 bucket and ensure it exists
                bucket = self.sink_config.get("bucket")
                if not bucket:
                    raise ValueError("S3 bucket not specified in sink config")
                
                # Write to memory buffer first
                buffer = BytesIO()
                self._write_data(buffer, data)
                buffer.seek(0)
                
                # Upload to S3
                self._s3_client.upload_fileobj(
                    buffer,
                    bucket,
                    output_key,
                    ExtraArgs={
                        'ContentType': 'application/json',
                        'ACL': 'bucket-owner-full-control'
                    }
                )
                
                s3_uri = f"s3://{bucket}/{output_key}"
                logger.info(f"Successfully stored results in {s3_uri}")
                return s3_uri
                
            else:  # Local file
                output_path = self._get_output_path()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Set file permissions to 644
                if not output_path.parent.exists():
                    output_path.parent.mkdir(parents=True, mode=0o755)
                
                self._write_data(output_path, data)
                output_path.chmod(0o644)
                
                logger.info(f"Successfully stored results in {output_path}")
                return output_path
            
        except Exception as e:
            logger.error(f"Failed to save results: {str(e)}")
            raise
