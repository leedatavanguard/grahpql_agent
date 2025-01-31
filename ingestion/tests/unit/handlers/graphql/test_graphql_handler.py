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


@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_graphql_error_handling(mock_boto3, mock_client_class, graphql_config_success, mock_env, mock_secrets_manager):
    """Test GraphQL error handling."""
    mock_boto3.client.return_value = mock_secrets_manager
    mock_client = mock_client_class.return_value
    mock_client.execute_query.side_effect = GraphQLError("Invalid query")
    mock_metrics = mock_metrics_class.return_valu
    
    with patch.dict(os.environ, mock_env), \
         pytest.raises(GraphQLError, match="Invalid query"):
        handler = GraphQLHandler(graphql_config_success)
        handler.execute()
        
    mock_metrics.end.assert_called_once_with(
        status="error",
        error="GraphQL query error: Invalid query"
    )


@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_network_error_handling(mock_boto3, mock_client_class, graphql_config_success, mock_env, mock_secrets_manager):
    """Test network error handling."""
    mock_boto3.client.return_value = mock_secrets_manager
    mock_client = mock_client_class.return_value
    mock_client.execute_query.side_effect = NetworkError("Connection failed")
    mock_metrics = mock_metrics_class.return_value
    
    with patch.dict(os.environ, mock_env), \
         pytest.raises(NetworkError, match="Connection failed"):
        handler = GraphQLHandler(graphql_config_success)
        handler.execute()
        
    mock_metrics.end.assert_called_once_with(
        status="error",
        error="Network error during query: Connection failed"
    )


@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_task_handling(mock_boto3, mock_client_class, graphql_config_success, mock_env, mock_secrets_manager):
    """Test task handling."""
    mock_boto3.client.return_value = mock_secrets_manager
    mock_client = mock_client_class.return_value
    mock_client.execute_query.return_value = {'data': {'test': 'data'}}
    
    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_success)
        result = handler.handle_task(graphql_config_success)
        
        assert result == {
            'status': 'success',
            'data': {'data': {'test': 'data'}}
        }


@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_lambda_handling(mock_boto3, mock_client_class, graphql_config_success, mock_env, mock_secrets_manager):
    """Test Lambda handling."""
    mock_boto3.client.return_value = mock_secrets_manager
    mock_client = mock_client_class.return_value
    mock_client.execute_query.return_value = {'data': {'test': 'data'}}
    
    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_success)
        result = handler.handle_lambda(graphql_config_success, None)
        
        assert result == {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'data': {'data': {'test': 'data'}}
            })
        }


@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_invalid_config(mock_boto3, mock_client_class, mock_env, mock_secrets_manager):
    """Test invalid configuration handling."""
    mock_boto3.client.return_value = mock_secrets_manager
    
    with patch.dict(os.environ, mock_env), \
         pytest.raises(ValueError, match="Configuration must be provided"):
        GraphQLHandler(None)


@patch('observability.tracking.job_metrics.JobMetricsTracker')
@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_missing_env_vars(mock_boto3, mock_client_class, mock_metrics_class, graphql_config_success):
    """Test missing environment variables handling."""
    with pytest.raises(EnvironmentError, match="Missing required environment variables"):
        GraphQLHandler(graphql_config_success)


@patch('observability.tracking.job_metrics.JobMetricsTracker')
@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_rate_limiting(mock_boto3, mock_client_class, mock_metrics_class, graphql_config_success, mock_env, mock_secrets_manager):
    """Test rate limiting functionality."""
    mock_boto3.client.return_value = mock_secrets_manager
    mock_client = mock_client_class.return_value
    mock_client.execute_query.return_value = {'data': {'test': 'data'}}
    
    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_success)
        
        # Execute multiple requests quickly
        start_time = time.time()
        results = []
        for _ in range(3):  # More than requests_per_second
            results.append(handler.execute())
        end_time = time.time()
        
        # Verify rate limiting worked
        duration = end_time - start_time
        assert duration >= 1.0  # Should take at least 1 second due to rate limiting
        
        # Verify all requests succeeded
        for result in results:
            assert result == {'data': {'test': 'data'}}


@patch('observability.tracking.job_metrics.JobMetricsTracker')
@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_cleanup(mock_boto3, mock_client_class, mock_metrics_class, graphql_config_success, mock_env, mock_secrets_manager, tmp_path):
    """Test resource cleanup."""
    mock_boto3.client.return_value = MagicMock()
    
    with patch.dict(os.environ, mock_env):
        handler = GraphQLHandler(graphql_config_success)
        temp_file = handler.temp_config_file.name
        
        # Verify temp file exists
        assert os.path.exists(temp_file)
        
        # Delete handler
        del handler
        
        # Verify temp file was cleaned up
        assert not os.path.exists(temp_file)


@patch('observability.tracking.job_metrics.JobMetricsTracker')
@patch('ingestion.utils.graphql_client.GraphQLOAuthClient')
@patch('ingestion.handlers.graphql.graphql_handler.boto3')
def test_graphql_config_validation(mock_boto3, mock_client_class, mock_metrics_class, mock_env):
    """Test GraphQL configuration validation.
    
    Tests:
    1. Initial config validation (None/empty)
    2. Required fields validation
    3. Query config validation
    4. Rate limit validation
    """
    mock_boto3.client.return_value = MagicMock()
    
    # Test 1: Initial config validation
    for invalid_config in [None, {}, {'some': 'value'}]:
        with pytest.raises(ValueError, match="Configuration must be provided"):
            GraphQLHandler(invalid_config)

    # Test 2: Required fields validation
    minimal_config = {
        'dataset_id': 'test',
        'job_id': 'test',
        'variables': {}  # Optional field with default
    }
    
    with pytest.raises(ValidationError) as exc_info:
        with patch.dict(os.environ, mock_env):
            GraphQLHandler(minimal_config)
    
    errors = {error['loc'][0] for error in exc_info.value.errors()}
    expected_missing = {'query_config', 'query_name', 'sink', 'config'}
    assert errors == expected_missing, f"Expected missing fields {expected_missing}, got {errors}"

    # Test 3: Query config validation
    invalid_query_configs = [
        # Missing query in query_config
        {
            'query_name': 'test_query',
            'query_config': {
                'test_query': {
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
        },
        # Query name not in query_config
        {
            'query_name': 'nonexistent_query',
            'query_config': {
                'test_query': {
                    'query': SUCCESSFUL_QUERY,
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
    ]
    
    for invalid_config in invalid_query_configs:
        with pytest.raises(ValueError):
            with patch.dict(os.environ, mock_env):
                GraphQLHandler(invalid_config)

    # Test 4: Rate limit validation
    invalid_rate_config = {
        'query_name': 'test_query',
        'query_config': {
            'test_query': {
                'query': SUCCESSFUL_QUERY,
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
                'requests_per_second': -1,  # Invalid value
                'burst': 3
            }
        }
    }
    
    with pytest.raises(ValidationError) as exc_info:
        with patch.dict(os.environ, mock_env):
            GraphQLHandler(invalid_rate_config)
    assert any("requests_per_second" in str(error['msg']) for error in exc_info.value.errors())
