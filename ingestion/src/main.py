"""Main entry point for ingestion containers."""

import os
import json
import logging
from ingestion.utils.graphql_client import GraphQLOAuthClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for ingestion containers."""
    try:
        # Get task configuration from environment
        handler_name = os.environ.get("HANDLER_NAME")
        handler_type = os.environ.get("HANDLER_TYPE")
        task_config = json.loads(os.environ.get("TASK_CONFIG", "{}"))
        
        logger.info(f"Starting {handler_name} with type {handler_type}")
        
        if handler_type == "graphql":
            # Get secrets
            secrets_arn = os.environ.get("LIVEHEATS_SECRET_ARN")
            if not secrets_arn:
                raise ValueError("LIVEHEATS_SECRET_ARN not found in environment")
            
            # Initialize GraphQL client
            client = GraphQLOAuthClient(secrets_arn)
            
            # Execute query
            result = client.execute(
                query=task_config["query"],
                variables=task_config.get("variables", {}),
                sink=task_config.get("sink", {})
            )
            
            logger.info(f"Successfully executed {handler_name}")
            return result
        else:
            raise ValueError(f"Unknown handler type: {handler_type}")
            
    except Exception as e:
        logger.error(f"Failed to execute {handler_name}: {str(e)}")
        raise

if __name__ == "__main__":
    main()
