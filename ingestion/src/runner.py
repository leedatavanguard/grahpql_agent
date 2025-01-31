"""Runner module for GraphQL handlers."""

import os
import sys
import logging
import asyncio
import importlib
from typing import Type, Optional

from .base.base_handler import BaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_handler_class() -> Type[BaseHandler]:
    """Get the handler class from environment variables.
    
    Returns:
        Handler class to instantiate
    
    Raises:
        ImportError: If handler module or class cannot be imported
        ValueError: If handler class is not a subclass of BaseHandler
    """
    module_name = os.environ.get("HANDLER_MODULE")
    class_name = os.environ.get("HANDLER_CLASS")
    
    if not module_name or not class_name:
        raise ValueError(
            "HANDLER_MODULE and HANDLER_CLASS environment variables must be set"
        )
    
    try:
        module = importlib.import_module(module_name)
        handler_class = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Failed to import handler: {e}")
    
    if not issubclass(handler_class, BaseHandler):
        raise ValueError(
            f"Handler class {class_name} must be a subclass of BaseHandler"
        )
    
    return handler_class

async def main():
    """Main entry point for running handlers."""
    try:
        # Get handler class
        handler_class = get_handler_class()
        logger.info(f"Starting handler: {handler_class.__name__}")
        
        # Initialize and run handler
        handler = handler_class()
        await handler.run()
        
    except Exception as e:
        logger.error(f"Error running handler: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
