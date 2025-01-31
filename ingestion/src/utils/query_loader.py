"""Utility for loading GraphQL queries from files."""

from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class QueryLoader:
    """Loads GraphQL queries from a directory.
    
    This class helps manage a catalog of GraphQL queries stored in .graphql files.
    It supports loading queries by name and listing available queries.
    
    Example:
        loader = QueryLoader("/path/to/queries")
        query = loader.load_query("my_query")
        available_queries = loader.list_queries()
    """
    
    def __init__(self, query_dir: str):
        """Initialize the query loader.
        
        Args:
            query_dir: Path to directory containing .graphql query files
        """
        self.query_dir = Path(query_dir)
        if not self.query_dir.exists():
            raise ValueError(f"Query directory not found: {query_dir}")
        logger.info(f"\nInitialized query loader with directory: {query_dir}")
            
    def load_query(self, query_name: str) -> Optional[str]:
        """Load a query by name.
        
        Args:
            query_name: Name of the query file (without .graphql extension)
            
        Returns:
            Query string if found, None otherwise
        """
        query_file = self.query_dir / f"{query_name}.graphql"
        if not query_file.exists():
            logger.error(f"Query file not found: {query_file}")
            return None
            
        try:
            query = query_file.read_text().strip()
            logger.info(f"Successfully loaded query: {query_name}")
            return query
        except Exception as e:
            logger.error(f"Error reading query file {query_file}: {str(e)}")
            return None
            
    def list_queries(self) -> Dict[str, str]:
        """List all available queries.
        
        Returns:
            Dictionary of query_name: query_content
        """
        queries = {}
        logger.info("\nAvailable queries:")
        for query_file in self.query_dir.glob("*.graphql"):
            name = query_file.stem
            content = self.load_query(name)
            if content:
                queries[name] = content
                logger.info(f"- {name}")
        return queries
