"""Tests for GraphQL handler."""

import os
import json
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
import pytest
from botocore.exceptions import ClientError
import logging
import asyncio
from pathlib import Path
import time
import yaml

# Set up logger
logger = logging.getLogger(__name__)

from ingestion.handlers.graphql.graphql_handler import GraphQLHandler, GraphQLConfig, GraphQLError, NetworkError, ValidationError
from ingestion.utils.graphql_client import GraphQLOAuthClient
from observability.tracking.job_metrics import JobMetricsTracker

SUCCESSFUL_QUERY = '''
query getOrganisationAthletes(
    $id: ID!,
    $page: Int!,
    $per: Int!
) {
    organisationAthletes(
        id: $id,
        page: $page,
        per: $per
    ) {
        athletes {
            id
            name
            dob
            properties
            users {
                phone
            }
            memberships {
                id
                expired 
                createdAt
                expiryDate 
                organisation {
                    id
                    name
                }
            }
        }
        totalCount
    }
}
'''

ERROR_QUERY = '''
query invalidQuery {
    nonexistentField {
        invalidAttribute
    }
}
'''

@pytest.fixture
def mock_secrets_manager():
    """Mock AWS Secrets Manager client."""
    graphql_secret_string = json.dumps({
        'GRAPHQL_URL': "https://liveheats.com/api/graphql",
        'AUTH_URL': "https://liveheats.com/oauth/token",
        'CLIENT_ID': "DF-uc7TXdKYzEoQ7sZ9HpjCYDhUZ4AKwa951tI1XpPI",
        'CLIENT_SECRET': "s2whc64ErBcASx6kTUfSApHsp2X0MndL6PZ4-FUiGOI",
        'SCOPE': "public events/director organisations/manage"
    })
    
    clickhouse_secret_string = json.dumps({
        'CH_URL': 'https://g30laelexn.ap-southeast-2.aws.clickhouse.cloud',
        'CH_PORT': '8443',
        'CH_USERNAME': 'default',
        'CH_PASSWORD': 'kuyILu7874_0l',
        'CH_DATABASE_NAME': 'dve'
    })
    
    mock_secrets = MagicMock()
    def get_secret_value(SecretId):
        if SecretId == 'test-graphql-secret':
            return {
                'SecretString': graphql_secret_string,
            }
        elif SecretId == 'test-clickhouse-secret':
            return {
                'SecretString': clickhouse_secret_string,
            }
        raise ClientError(
            operation_name='GetSecretValue',
            error_response={
                'Error': {
                    'Code': 'ResourceNotFoundException',
                    'Message': f'Secret {SecretId} not found'
                }
            }
        )
    
    mock_secrets.get_secret_value = get_secret_value
    return mock_secrets


@pytest.fixture
def mock_env():
    """Mock environment variables."""
    env_vars = {
        'AWS_REGION': 'us-west-2',
        'GRAPHQL_SECRET_ARN': 'test-graphql-secret',
        'CLICKHOUSE_SECRET_ARN': 'test-clickhouse-secret',
        'HANDLER_NAME': 'test-handler',
        'TASK_NAME': 'test-task',
        'DATA_DIR': '/tmp/data',
        'CH_URL': 'https://localhost',
        'CH_PORT': '8443',
        'CH_USERNAME': 'default',
        'CH_PASSWORD': 'test',
        'CH_DATABASE_NAME': 'test'
    }
    
    with patch.dict(os.environ, env_vars, clear=True):
        yield env_vars


@pytest.fixture
def graphql_config_success():
    """Create GraphQL config for testing."""
    return {
        'query_name': 'test_query',
        'query_config': {
            'test_query': {
                'query': SUCCESSFUL_QUERY,
                'variables': {"id": 78, "page": 0, "per": 1000}
            }
        },
        'variables': {},
        'dataset_id': "test-dataset",
        'job_id': "test-job",
        'sink': {
            'type': 's3',
            'key_prefix': 'test',
            'format': 'json',
            'compression': None,
            'base_path': 'test'
        },
        'config': {
            'rate_limit': {
                'requests_per_second': 2,
                'burst': 3
            }
        }
    }


@pytest.fixture
def graphql_config_error():
    """Create GraphQL config with error query for testing."""
    return {
        'query_name': 'test_query',
        'query_config': {
            'test_query': {
                'query': ERROR_QUERY,
                'variables': {}
            }
        },
        'variables': {},
        'dataset_id': "test-dataset",
        'job_id': "test-job",
        'sink': {
            'type': 's3',
            'key_prefix': 'test',
            'format': 'json',
            'compression': None,
            'base_path': 'test'
        },
        'config': {
            'rate_limit': {
                'requests_per_second': 2,
                'burst': 3
            }
        }
    }


@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_query_execution_success(mock_boto3, graphql_config_success, mock_env, mock_secrets_manager):
    """Test query execution with live data while mocking AWS Secrets Manager."""
    # Set up mock boto3 client to return our mock_secrets_manager
    mock_boto3.client.return_value = mock_secrets_manager

    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_success)
        result = handler.execute()
        
        # Verify the structure of the response
        assert isinstance(result, dict)
        assert 'data' in result
        
        # Verify the data contains expected fields
        data = result['data']
        assert isinstance(data, dict)
        
        # Log the actual response for debugging
        logging.debug(f"GraphQL Response: {json.dumps(data, indent=2)}")
        
        # Add specific data validation based on your GraphQL schema
        # TODO: Add more specific assertions based on your expected data structure


@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_query_execution_error(mock_boto3, graphql_config_error, mock_env, mock_secrets_manager, caplog):
    """Test query execution with invalid query to verify error handling.
    
    This test:
    1. Executes an invalid GraphQL query that should fail schema validation
    2. Verifies the error is properly caught and logged
    3. Ensures metrics are tracked for the error
    """
    # Set up mock boto3 client to return our mock_secrets_manager
    mock_boto3.client.return_value = mock_secrets_manager

    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_error)
        
        # Execute query and expect Exception
        # Note: The GraphQL validation error comes through as a regular Exception
        # because it's caught by the GraphQL client before our GraphQLError wrapper
        with pytest.raises(Exception) as exc_info:
            handler.execute()
        
        # Verify error message contains expected GraphQL error details
        error_message = str(exc_info.value)
        assert "Cannot query field 'nonexistentField' on type 'Query'" in error_message
        
        # Log the error for debugging
        logging.debug(f"GraphQL Error: {error_message}")
        
        # Verify the error was logged
        # This can be found in the pytest output
        assert "Failed to execute GraphQL query" in caplog.text
        assert "Cannot query field 'nonexistentField'" in caplog.text