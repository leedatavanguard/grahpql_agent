"""Base handler for all ingestion handlers."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, TypeVar, Generic
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

# Generic type for configuration
ConfigT = TypeVar('ConfigT')

class BaseHandler(ABC, Generic[ConfigT]):
    """Base handler class that all handlers must inherit from.
    
    Generic type parameters:
        ConfigT: The type of configuration object used by the handler
    """
    
    def __init__(self) -> None:
        """Initialize base handler."""
        self.initialized_at = datetime.utcnow()
        
    @abstractmethod
    def handle_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a task with the given configuration.
        
        Args:
            task_config: Task configuration from platform config
            
        Returns:
            Dict containing task results
            
        Raises:
            ValueError: If configuration is invalid
            Exception: If task execution fails
        """
        pass
    
    @abstractmethod
    def handle_lambda(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Handle a Lambda invocation.
        
        Args:
            event: Lambda event
            context: Lambda context
            
        Returns:
            Dict containing Lambda response
            
        Raises:
            ValueError: If event is invalid
            Exception: If Lambda execution fails
        """
        pass
    
    @abstractmethod
    def validate_config(self, config: ConfigT) -> None:
        """Validate handler configuration.
        
        Args:
            config: Handler configuration to validate
            
        Raises:
            ValueError: If configuration is invalid
        """
        pass
    
    @abstractmethod
    def initialize(self, config: ConfigT) -> None:
        """Initialize handler with configuration.
        
        Args:
            config: Handler configuration
            
        Raises:
            ValueError: If configuration is invalid
            Exception: If initialization fails
        """
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up handler resources.
        
        This method should be called when the handler is no longer needed
        to clean up any resources it has allocated.
        """
        pass
    
    def format_error_response(self, error: Exception, status_code: Optional[int] = None) -> Dict[str, Any]:
        """Format an error response.
        
        Args:
            error: The error that occurred
            status_code: Optional HTTP status code for Lambda responses
            
        Returns:
            Dict containing formatted error response
        """
        error_msg = f"{error.__class__.__name__}: {str(error)}"
        logger.error(error_msg)
        
        if status_code:
            return {
                "statusCode": status_code,
                "body": {
                    "status": "error",
                    "error": error_msg
                }
            }
        
        return {
            "status": "error",
            "error": error_msg
        }
    
    def format_success_response(self, data: Any, status_code: Optional[int] = None) -> Dict[str, Any]:
        """Format a success response.
        
        Args:
            data: The data to return
            status_code: Optional HTTP status code for Lambda responses
            
        Returns:
            Dict containing formatted success response
        """
        if status_code:
            return {
                "statusCode": status_code,
                "body": {
                    "status": "success",
                    "data": data
                }
            }
        
        return {
            "status": "success",
            "data": data
        }
