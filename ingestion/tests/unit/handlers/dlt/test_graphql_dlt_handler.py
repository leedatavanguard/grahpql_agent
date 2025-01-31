"""Unit tests for the GraphQL DLT handler."""

import os
import json
import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any

import dlt
import boto3
from botocore.stub import Stubber

from ingestion.handlers.dlt.graphql_dlt_handler import GraphQLDLTHandler, GraphQLDLTConfig

@pytest.fixture
def graphql_config() -> Dict[str, Any]:
    """GraphQL DLT handler configuration."""
    return {
        # DLT base config
        "pipeline_name": "test_pipeline",
        "destination": "duckdb",
        "schema_name": "public",
        "credentials": {},
        "config": {},
        # GraphQL specific config
        "query": "query { test { id name } }",
        "variables": {"limit": 100},
        "endpoint": "https://api.example.com/graphql",
        "auth": {
            "secret_name": "test/graphql/auth"
        }
    }

@pytest.fixture
def mock_secrets():
    """Mock secrets for testing."""
    return {
        "CLIENT_ID": "test-client-id",
        "CLIENT_SECRET": "test-client-secret",
        "SCOPE": "test-scope"
    }

@pytest.fixture
def mock_graphql_client():
    """Mock GraphQL client."""
    client = Mock()
    client.execute_query.return_value = {
        "data": {
            "test": [
                {"id": 1, "name": "Test 1"},
                {"id": 2, "name": "Test 2"}
            ]
        }
    }
    return client

def test_graphql_handler_initialization(graphql_config):
    """Test GraphQL DLT handler initialization with config."""
    handler = GraphQLDLTHandler(graphql_config)
    assert handler.graphql_config.query == "query { test { id name } }"
    assert handler.graphql_config.endpoint == "https://api.example.com/graphql"
    assert handler.graphql_config.variables == {"limit": 100}

def test_graphql_handler_env_config():
    """Test GraphQL DLT handler initialization from environment variables."""
    with patch.dict(os.environ, {
        "DLT_PIPELINE_NAME": "env_pipeline",
        "DLT_DESTINATION": "athena",
        "DLT_SCHEMA": "test",
        "DLT_CREDENTIALS": "{}",
        "DLT_CONFIG": "{}",
        "GRAPHQL_QUERY": "query { env { id } }",
        "GRAPHQL_VARIABLES": '{"test": true}',
        "GRAPHQL_ENDPOINT": "https://env.example.com/graphql",
        "GRAPHQL_AUTH": '{"type": "oauth"}'
    }):
        handler = GraphQLDLTHandler(None)
        assert handler.graphql_config.pipeline_name == "env_pipeline"
        assert handler.graphql_config.query == "query { env { id } }"
        assert handler.graphql_config.endpoint == "https://env.example.com/graphql"

def test_get_secrets_from_env(graphql_config, mock_secrets):
    """Test getting secrets from environment variables."""
    with patch.dict(os.environ, {
        "GRAPHQL_CLIENT_ID": mock_secrets["CLIENT_ID"],
        "GRAPHQL_CLIENT_SECRET": mock_secrets["CLIENT_SECRET"],
        "GRAPHQL_SCOPE": mock_secrets["SCOPE"]
    }):
        handler = GraphQLDLTHandler(graphql_config)
        secrets = handler._get_secrets()
        assert secrets == mock_secrets

def test_get_secrets_from_secrets_manager(graphql_config, mock_secrets):
    """Test getting secrets from AWS Secrets Manager."""
    secrets_client = boto3.client('secretsmanager')
    with Stubber(secrets_client) as stubber:
        stubber.add_response(
            'get_secret_value',
            {
                'SecretString': json.dumps(mock_secrets)
            },
            {'SecretId': 'test/graphql/auth'}
        )
        
        with patch('boto3.client', return_value=secrets_client):
            handler = GraphQLDLTHandler(graphql_config)
            secrets = handler._get_secrets()
            assert secrets == mock_secrets

@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
def test_extract_data(mock_client_class, graphql_config, mock_graphql_client, mock_secrets):
    """Test GraphQL data extraction."""
    mock_client_class.return_value = mock_graphql_client
    
    with patch.dict(os.environ, {
        "GRAPHQL_CLIENT_ID": mock_secrets["CLIENT_ID"],
        "GRAPHQL_CLIENT_SECRET": mock_secrets["CLIENT_SECRET"],
        "GRAPHQL_SCOPE": mock_secrets["SCOPE"]
    }):
        handler = GraphQLDLTHandler(graphql_config)
        result = handler.extract_data()
        
        assert result == mock_graphql_client.execute_query.return_value
        mock_client_class.assert_called_once_with(
            endpoint=graphql_config["endpoint"],
            client_id=mock_secrets["CLIENT_ID"],
            client_secret=mock_secrets["CLIENT_SECRET"],
            scope=mock_secrets["SCOPE"]
        )

@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
def test_handle_task_with_graphql(mock_client_class, graphql_config, mock_graphql_client):
    """Test end-to-end task handling with GraphQL data."""
    mock_client_class.return_value = mock_graphql_client
    mock_pipeline = Mock()
    mock_pipeline.run.return_value = Mock(
        dict=lambda: {
            "rows_processed": 2,
            "status": "success"
        }
    )
    
    task_config = {
        "dlt_options": {
            "table_name": "test_graphql"
        },
        "incremental": True
    }
    
    with patch('dlt.pipeline', return_value=mock_pipeline), \
         patch.dict(os.environ, {
             "GRAPHQL_CLIENT_ID": "test-id",
             "GRAPHQL_CLIENT_SECRET": "test-secret",
             "GRAPHQL_SCOPE": "test-scope"
         }):
        handler = GraphQLDLTHandler(graphql_config)
        result = handler.handle_task(task_config)
        
        assert result["status"] == "success"
        assert result["table_name"] == "test_graphql"
        assert result["write_disposition"] == "merge"
        
        mock_pipeline.run.assert_called_once_with(
            mock_graphql_client.execute_query.return_value,
            table_name="test_graphql",
            write_disposition="merge"
        )
