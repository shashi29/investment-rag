from typing import Dict, Any, Optional, Callable
import traceback
from datetime import datetime
import asyncio
from ..core.exceptions import (
    DataCollectionError,
    APIError,
    ValidationError,
    RateLimitError
)
from .logger import get_logger

logger = get_logger(__name__)

class ErrorHandler:
    """Handle and process errors in the data collection system."""
    
    def __init__(self):
        self.error_counts: Dict[str, int] = {}
        self.error_callbacks: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    async def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Handle an error and return error information."""
        try:
            error_type = type(error).__name__
            error_info = {
                "error_type": error_type,
                "error_message": str(error),
                "timestamp": datetime.utcnow(),
                "traceback": traceback.format_exc(),
                "context": context or {}
            }
            
            # Update error counts
            async with self._lock:
                self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
            
            # Log error based on type
            if isinstance(error, APIError):
                await self._handle_api_error(error, error_info)
            elif isinstance(error, ValidationError):
                await self._handle_validation_error(error, error_info)
            elif isinstance(error, RateLimitError):
                await self._handle_rate_limit_error(error, error_info)
            else:
                await self._handle_generic_error(error, error_info)
            
            # Execute error callbacks
            await self._execute_callbacks(error_type, error_info)
            
            return error_info
            
        except Exception as e:
            logger.error(f"Error in error handler: {str(e)}")
            return {
                "error_type": "ErrorHandlerError",
                "error_message": f"Error handling error: {str(e)}",
                "timestamp": datetime.utcnow(),
                "original_error": str(error)
            }

    async def _handle_api_error(
        self,
        error: APIError,
        error_info: Dict[str, Any]
    ) -> None:
        """Handle API-specific errors."""
        error_info.update({
            "status_code": error.status_code,
            "response": error.response,
            "category": "api_error"
        })
        logger.error(f"API Error: {error_info}")

    async def _handle_validation_error(
        self,
        error: ValidationError,
        error_info: Dict[str, Any]
    ) -> None:
        """Handle validation errors."""
        error_info.update({
            "validation_errors": error.validation_errors,
            "category": "validation_error"
        })
        logger.error(f"Validation Error: {error_info}")

    async def _handle_rate_limit_error(
        self,
        error: RateLimitError,
        error_info: Dict[str, Any]
    ) -> None:
        """Handle rate limit errors."""
        error_info.update({
            "retry_after": error.retry_after,
            "category": "rate_limit_error"
        })
        logger.warning(f"Rate Limit Error: {error_info}")

    async def _handle_generic_error(
        self,
        error: Exception,
        error_info: Dict[str, Any]
    ) -> None:
        """Handle generic errors."""
        error_info.update({
            "category": "generic_error"
        })
        logger.error(f"Generic Error: {error_info}")

    def register_callback(
        self,
        error_type: str,
        callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """Register a callback for specific error type."""
        self.error_callbacks[error_type] = callback

    async def _execute_callbacks(
        self,
        error_type: str,
        error_info: Dict[str, Any]
    ) -> None:
        """Execute registered callbacks for error type."""
        callback = self.error_callbacks.get(error_type)
        if callback:
            try:
                await callback(error_info)
            except Exception as e:
                logger.error(f"Error in callback for {error_type}: {str(e)}")

    def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics."""
        return {
            "error_counts": self.error_counts,
            "total_errors": sum(self.error_counts.values()),
            "last_error_timestamp": datetime.utcnow()
        }

    async def clear_error_counts(self) -> None:
        """Clear error count statistics."""
        async with self._lock:
            self.error_counts.clear()

    def create_error_context(
        self,
        operation: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Create context information for error handling."""
        return {
            "operation": operation,
            "timestamp": datetime.utcnow(),
            **kwargs
        }

    async def handle_batch_errors(
        self,
        errors: Dict[str, Exception],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Handle multiple errors from batch operations."""
        error_results = {}
        for key, error in errors.items():
            error_results[key] = await self.handle_error(error, context)
        return error_results