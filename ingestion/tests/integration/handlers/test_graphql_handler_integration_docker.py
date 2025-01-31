"""Integration tests for the GraphQL handler.

These tests verify the GraphQL handler's functionality against the liveheats
GraphQL service using real AWS services in the test account.
"""

import os
import json
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv

import boto3
from ingestion.handlers.graphql.graphql_handler import GraphQLHandler

def load_env_files():
    """Load environment variables from platform config files."""
    platform_config = Path(__file__).parents[4] / ".platform_config/dev_platform"
    
    # Load AWS credentials
    load_dotenv(platform_config / ".env")
    
    # Load Liveheats specific config
    load_dotenv(platform_config / ".env.liveheats")

def check_aws_credentials():
    """Verify AWS credentials are properly configured."""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"Using AWS account: {identity['Account']}")
        return True
    except Exception as e:
        pytest.skip(f"AWS credentials not configured: {e}")
        return False

def create_graphql_query() -> str:
    """Create the GraphQL query for testing."""
    return """
        query getOrganisationAthletes($id: ID!, $page: Int!, $per: Int!) {
            organisationAthletes(id: $id, page: $page, per: $per) {
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
    """

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Set up test environment and verify credentials."""
    load_env_files()
    check_aws_credentials()

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Test configuration for GraphQL handler."""
    return {
        "query": create_graphql_query(),
        "variables": {
            "id": os.environ.get("TEST_ORGANISATION_ID", "1"),  # Default test org
            "page": 1,
            "per": 10  # Small limit for testing
        },
        "endpoint": os.environ["LIVEHEATS_GRAPHQL_ENDPOINT"],
        "auth": {
            "client_id": os.environ["LIVEHEATS_CLIENT_ID"],
            "client_secret": os.environ["LIVEHEATS_CLIENT_SECRET"],
            "scope": os.environ["LIVEHEATS_SCOPE"]
        },
        "rate_limit": {
            "requests_per_second": 2,
            "burst": 3
        }
    }

def test_graphql_handler_integration(test_config):
    """Test GraphQL handler with liveheats service.
    
    This test:
    1. Initializes the handler with liveheats configuration
    2. Executes the organisation athletes query
    3. Verifies the response format
    4. Checks CloudWatch metrics were published
    """
    # Initialize handler
    handler = GraphQLHandler(test_config)
    
    # Execute query
    result = handler.execute_query()
    
    # Verify successful execution
    assert result["status"] == "success", f"Query failed: {result.get('error')}"
    assert "data" in result
    assert "organisationAthletes" in result["data"]
    
    # Verify data format
    athletes_data = result["data"]["organisationAthletes"]
    assert "athletes" in athletes_data
    assert "totalCount" in athletes_data
    
    # Verify athlete data structure
    if athletes_data["athletes"]:
        athlete = athletes_data["athletes"][0]
        assert "id" in athlete
        assert "name" in athlete
        assert "dob" in athlete
        assert "properties" in athlete
        assert "memberships" in athlete
        
        if athlete["memberships"]:
            membership = athlete["memberships"][0]
            assert "id" in membership
            assert "expired" in membership
            assert "createdAt" in membership
            assert "organisation" in membership
    
    # Check CloudWatch metrics
    cloudwatch = boto3.client("cloudwatch")
    
    # Get metrics for the last 5 minutes
    metrics = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "GraphQLHandler",
                        "MetricName": "QueryDuration",
                        "Dimensions": [
                            {
                                "Name": "HandlerName",
                                "Value": handler.handler_name
                            }
                        ]
                    },
                    "Period": 60,
                    "Stat": "Average"
                }
            }
        ],
        StartTime=datetime.now() - timedelta(minutes=5),
        EndTime=datetime.now()
    )
    
    # Verify metrics were published
    assert len(metrics["MetricDataResults"]) > 0, "No metrics found"
    
    # Log resource usage
    resource_metrics = handler.get_resource_metrics()
    print(f"\nResource Usage:")
    print(f"Memory: {resource_metrics['memory_mb']:.2f} MB")
    print(f"CPU: {resource_metrics['cpu_percent']:.2f}%")
    print(f"Total Athletes: {athletes_data['totalCount']}")
