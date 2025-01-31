"""NSW Surfing GraphQL API exploration script."""

import os
from pathlib import Path
import logging
from typing import Dict, Any, Optional
import sys
import json
from datetime import datetime

from ingestion.exploration.graphql_explorer import GraphQLExplorer, ExplorationConfig
from ingestion.utils.query_loader import QueryLoader
from ingestion.utils.logging_utils import setup_logging, print_json, print_table, print_query
from ..constants import (
    RAW_DATA_FILE,
    FLAT_DATA_FILE,
    DATA_REPORT_FILE,
    OUTPUT_DIR,
    SAMPLE_SIZE
)

# Set up rich logging
setup_logging()
logger = logging.getLogger(__name__)

def explore_nsw_surfing(query_name: Optional[str] = None):
    """Run exploration queries against the NSW Surfing GraphQL API."""
    # Load configuration
    config = load_config()
    
    # Initialize explorer
    explorer_config = ExplorationConfig(
        graphql_url=config.graphql_url,
        token_url=config.token_url,
        client_id=config.client_id,
        client_secret=config.client_secret,
        scope=config.scope,
        output_dir=config.output_dir,
        query_name=query_name or "all_queries"
    )
    explorer = GraphQLExplorer(explorer_config)
    
    if not all([config.client_id, config.client_secret]):
        logger.error("Missing required environment variables. Please check .env file")
        sys.exit(1)
        
    # Initialize query loader
    query_dir = Path(__file__).parent / "queries"
    query_loader = QueryLoader(query_dir)
    available_queries = query_loader.list_queries()
    
    if not available_queries:
        logger.error(f"No queries found in {query_dir}")
        sys.exit(1)
        
    if query_name and query_name not in available_queries:
        logger.error(f"Query {query_name} not found. Available queries: {list(available_queries.keys())}")
        return
        
    queries_to_run = [query_name] if query_name else available_queries.keys()

    for q_name in queries_to_run:
        logger.info(f"\n[bold green]Analyzing query: {q_name}[/bold green]")
        query = query_loader.load_query(q_name)
        
        if not query:
            logger.error(f"Failed to load query: {q_name}")
            continue

        # Print the query
        print_query(query, "GraphQL Query")

        # Run the query and analyze results
        try:
            result = explorer.analyze_query(query, query_name=q_name)
            
            if not result['success']:
                if 'errors' in result:
                    logger.error("Query returned errors:")
                    print_json(result['errors'])
                else:
                    logger.error(f"Analysis failed: {result.get('error', 'Unknown error')}")
                continue
                
            # Log output locations
            logger.info("\n[bold green]Analysis complete![/bold green]")
            logger.info(f"Output directory: [bold]{result['output_dir']}[/bold]")
            logger.info("\nGenerated files:")
            logger.info("1. Raw response: raw_data.json")
            logger.info("2. Flattened data: flat_data.json")
            logger.info("3. Data analysis report: data_report.md")
                
        except Exception as e:
            logger.error(f"Error analyzing {q_name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            continue

def test_structure_analysis():
    """Test the structure analysis with a sample response."""
    sample_data = {
        "data": {
            "series": {
                "name": "Test Series",
                "paginatedMemberships": {
                    "totalCount": 2,
                    "nodes": [
                        {
                            "id": "1",
                            "member": {
                                "name": "John Doe",
                                "email": "john@example.com"
                            }
                        },
                        {
                            "id": "2",
                            "member": {
                                "name": "Jane Doe",
                                "email": "jane@example.com"
                            }
                        }
                    ]
                }
            }
        }
    }
    
    # Create test output directory
    output_dir = Path("test_outputs")
    output_dir.mkdir(exist_ok=True)
    
    # Initialize explorer with test config
    config = ExplorationConfig(
        graphql_url="http://test.example.com/graphql",
        token_url="http://test.example.com/token",
        client_id="test_id",
        client_secret="test_secret",
        output_dir=str(output_dir)
    )
    explorer = GraphQLExplorer(config)
    
    # Test analysis
    result = explorer.analyze_query("", query_name="test")
    
    if result['success']:
        logger.info("Test successful!")
        logger.info(f"Output files in: {result['output_dir']}")
    else:
        logger.error("Test failed!")
        logger.error(result.get('error', 'Unknown error'))

def load_config():
    # Load configuration from environment variables
    base_dir = OUTPUT_DIR
    return ExplorationConfig(
        graphql_url=os.getenv("GRAPHQL_URL", "https://liveheats.com/api/graphql"),
        token_url=os.getenv("AUTH_URL", "https://liveheats.com/oauth/token"),
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        scope=os.getenv("SCOPE"),
        output_dir=str(base_dir)
    )

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_structure_analysis()
    else:
        query_name = sys.argv[1] if len(sys.argv) > 1 else None
        explore_nsw_surfing(query_name)
