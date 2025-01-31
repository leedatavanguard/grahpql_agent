"""Integration tests for LiveHeats GraphQL handler."""

import os
import json
import pytest
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
from dotenv import load_dotenv

from ingestion.handlers.graphql.graphql_handler import GraphQLHandler
from ingestion.runner import get_handler_class, main as runner_main

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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

def create_query_variables() -> Dict[str, Any]:
    """Create query variables for testing."""
    return {
        "id": 78,  # Known working organization ID
        "page": 1,
        "per": 10  # Smaller page size for testing
    }

def create_local_sink_config(test_data_dir: Path) -> Dict[str, Any]:
    """Create local sink configuration."""
    return {
        "type": "local",
        "key_prefix": "athletes",  # Just the final path component
        "format": "json",  # Specify output format
        "compression": None,  # No compression for testing
        "base_path": str(test_data_dir)  # Base path for local
    }

def create_s3_sink_config() -> Dict[str, Any]:
    """Create S3 sink configuration."""
    return {
        "type": "s3",
        "bucket": "your-s3-bucket",  # Replace with actual bucket name
        "key_prefix": "athletes",  # Just the final path component
        "format": "json",  # Specify output format
        "compression": None,  # No compression for testing
        "base_path": "liveheats/organisations"  # Base path for both local and S3
    }

@pytest.fixture(autouse=True)
def load_env():
    """Load environment variables from .env.liveheats file."""
    env_path = Path(__file__).parents[4] / ".platform_config" / "dev_platform" / ".env.liveheats"
    load_dotenv(env_path)
    
    # Enable testing mode for OAuth
    os.environ["TESTING"] = "1"

@pytest.fixture
def test_data_dir() -> Path:
    """Create a directory for test data."""
    # Use a fixed output directory in the project root
    data_dir = Path(__file__).parents[3] / "output_test_data" / "raw-data"
    # Clean up any existing test data
    if data_dir.exists():
        import shutil
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)
    return data_dir

@pytest.fixture
def task_config(test_data_dir: Path) -> Dict[str, Any]:
    """Get task configuration for testing the GraphQL handler."""
    return {
        "query": create_graphql_query(),
        "variables": create_query_variables(),
        "sink": create_local_sink_config(test_data_dir),
        "config": {}
    }

@pytest.fixture
def s3_task_config() -> Dict[str, Any]:
    """Get task configuration for testing the GraphQL handler with S3 sink."""
    return {
        "query": create_graphql_query(),
        "variables": create_query_variables(),
        "sink": create_s3_sink_config(),
        "config": {}
    }

@pytest.fixture
def mock_secrets() -> Dict[str, str]:
    """Get API credentials from environment variables."""
    required_secrets = [
        "CLIENT_ID",
        "CLIENT_SECRET",
        "GRAPHQL_URL",
        "AUTH_URL",
        "SCOPE"
    ]
    
    # Check all required secrets are present
    missing = [s for s in required_secrets if s not in os.environ]
    if missing:
        pytest.fail(f"Missing required environment variables: {missing}")
        
    return {
        "CLIENT_ID": os.environ["CLIENT_ID"],
        "CLIENT_SECRET": os.environ["CLIENT_SECRET"],
        "GRAPHQL_URL": os.environ["GRAPHQL_URL"],
        "AUTH_URL": os.environ["AUTH_URL"],
        "SCOPE": os.environ["SCOPE"]
    }

def create_task_env_config(task_config: Dict[str, Any]) -> Dict[str, str]:
    """Create environment variables for task configuration."""
    return {
        "QUERY": task_config["query"],
        "VARIABLES": json.dumps(task_config["variables"]),
        "SINK_CONFIG": json.dumps(task_config["sink"]),
        "TASK_CONFIG": json.dumps({
            "query": task_config["query"],
            "variables": task_config["variables"],
            "sink": task_config["sink"],
            "config": task_config.get("config", {})
        })
    }

@pytest.mark.asyncio
async def test_handler_processes_data(
    test_data_dir: Path,
    task_config: Dict[str, Any],
    mock_secrets: Dict[str, str]
):
    """Test that the handler processes and stores data correctly."""
    try:
        # Set environment variables
        os.environ["DATA_DIR"] = str(test_data_dir)
        env_config = create_task_env_config(task_config)
        for key, value in env_config.items():
            os.environ[key] = value
        
        # Set up secrets in environment
        for key, value in mock_secrets.items():
            os.environ[key] = value
        
        # Initialize and run handler
        handler = GraphQLHandler()
        await handler.process_data()
        
        # Find output file
        output_files = list(test_data_dir.glob("**/*.json"))
        assert len(output_files) == 1, "Expected exactly one output file"
        output_file = output_files[0]
        
        # Verify file path structure
        expected_prefix = task_config["sink"]["base_path"] + "/" + task_config["sink"]["key_prefix"]
        assert str(output_file).startswith(str(test_data_dir / expected_prefix)), \
            f"Output file should be in {expected_prefix} directory"
            
        # Verify file name format
        file_name = output_file.name
        assert file_name.endswith(".json"), "File should have .json extension"
        timestamp_str = file_name.replace(".json", "").split("_")[-1]
        try:
            datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        except ValueError:
            pytest.fail("Filename should end with timestamp in format YYYYMMDDHHMMSS")
        
        # Verify file permissions
        assert output_file.stat().st_mode & 0o777 == 0o644, \
            "File should have 644 permissions"
            
        # Verify file is not empty
        assert output_file.stat().st_size > 0, "File should not be empty"
        
        # Load and validate data
        with open(output_file) as f:
            data = json.load(f)
            logger.info(f"Loaded data from file: {data}")
        
        # Basic structure validation
        assert "data" in data, "Response should have data field"
        assert "organisationAthletes" in data["data"], "Data should have organisationAthletes"
        
        org_athletes = data["data"]["organisationAthletes"]
        assert "athletes" in org_athletes, "Should have athletes list"
        assert "totalCount" in org_athletes, "Should have totalCount"
        assert isinstance(org_athletes["athletes"], list), "Athletes should be a list"
        
        if len(org_athletes["athletes"]) > 0:
            athlete = org_athletes["athletes"][0]
            assert "id" in athlete, "Athlete should have id"
            assert "name" in athlete, "Athlete should have name"
            assert "dob" in athlete, "Athlete should have dob"
            assert "properties" in athlete, "Athlete should have properties"
            assert "users" in athlete, "Athlete should have users"
            assert "memberships" in athlete, "Athlete should have memberships"
            
            if len(athlete["memberships"]) > 0:
                membership = athlete["memberships"][0]
                assert "id" in membership, "Membership should have id"
                assert "expired" in membership, "Membership should have expired"
                assert "createdAt" in membership, "Membership should have createdAt"
                assert "expiryDate" in membership, "Membership should have expiryDate"
                assert "organisation" in membership, "Membership should have organisation"
                
                org = membership["organisation"]
                assert "id" in org, "Organisation should have id"
                assert "name" in org, "Organisation should have name"
        
        logger.info("All assertions passed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

@pytest.mark.asyncio
async def test_handler_with_runner(
    test_data_dir: Path,
    task_config: Dict[str, Any],
    mock_secrets: Dict[str, str]
):
    """Test that the handler works correctly when loaded through the runner module."""
    try:
        # Set environment variables
        os.environ["DATA_DIR"] = str(test_data_dir)
        os.environ["HANDLER_MODULE"] = "ingestion.handlers.graphql.graphql_handler"
        os.environ["HANDLER_CLASS"] = "GraphQLHandler"
        
        # Set task configuration
        env_config = create_task_env_config(task_config)
        for key, value in env_config.items():
            os.environ[key] = value
        
        # Set up secrets in environment
        for key, value in mock_secrets.items():
            os.environ[key] = value
        
        # Get handler class using runner module
        handler_class = get_handler_class()
        assert handler_class == GraphQLHandler, "Handler class should be GraphQLHandler"
        
        # Run the handler using the runner's main function
        await runner_main()
        
        # Find output file
        output_files = list(test_data_dir.glob("**/*.json"))
        assert len(output_files) == 1, "Expected exactly one output file"
        output_file = output_files[0]
        
        # Verify file exists and is not empty
        assert output_file.exists(), "Output file should exist"
        assert output_file.stat().st_size > 0, "File should not be empty"
        
        # Load and validate data
        with open(output_file) as f:
            data = json.load(f)
            
        assert "data" in data, "Response should contain data field"
        assert "organisationAthletes" in data["data"], \
            "Response should contain organisationAthletes field"
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

@pytest.mark.skipif(not os.environ.get("AWS_ACCESS_KEY_ID"), reason="AWS credentials not configured")
async def test_handler_processes_data_to_s3(
    test_data_dir: Path,
    s3_task_config: Dict[str, Any],
    mock_secrets: Dict[str, str]
):
    """Test that the handler processes and stores data correctly to S3."""
    try:
        # Set environment variables
        os.environ["DATA_DIR"] = str(test_data_dir)
        env_config = create_task_env_config(s3_task_config)
        for key, value in env_config.items():
            os.environ[key] = value
        
        # Set up secrets in environment
        for key, value in mock_secrets.items():
            os.environ[key] = value
        
        # Initialize and run handler
        handler = GraphQLHandler()
        result = await handler.process_data()
        
        # Verify S3 path format
        assert result.startswith("s3://"), "Result should be an S3 URI"
        assert s3_task_config["sink"]["bucket"] in result, "Result should contain bucket name"
        assert s3_task_config["sink"]["base_path"] in result, "Result should contain base path"
        assert s3_task_config["sink"]["key_prefix"] in result, "Result should contain key prefix"
        assert result.endswith(".json"), "Result should end with .json extension"
        
        # Extract timestamp from path
        path_parts = result.split("/")
        filename = path_parts[-1]
        timestamp_str = filename.replace(".json", "").split("_")[-1]
        try:
            datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
        except ValueError:
            pytest.fail("Filename should end with timestamp in format YYYYMMDDHHMMSS")
        
        logger.info("All assertions passed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
