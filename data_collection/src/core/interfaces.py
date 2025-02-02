from typing import Protocol, Dict, Any, List, Optional
from datetime import datetime

class DataProviderProtocol(Protocol):
    """Protocol defining the interface for data providers."""
    
    async def fetch_data(self, symbol: str, start_date: datetime, 
                        end_date: datetime) -> Dict[str, Any]:
        """Fetch data for given symbol and date range."""
        ...

    async def get_real_time_data(self, symbol: str) -> Dict[str, Any]:
        """Get real-time data for given symbol."""
        ...

    async def get_historical_data(self, symbol: str, 
                                lookback_days: int) -> Dict[str, Any]:
        """Get historical data for given symbol."""
        ...

class DataValidatorProtocol(Protocol):
    """Protocol defining the interface for data validation."""
    
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """Validate data structure and content."""
        ...

    async def sanitize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize and clean data."""
        ...

    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors."""
        ...

class CacheProtocol(Protocol):
    """Protocol defining the interface for caching."""
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...

    async def set(self, key: str, value: Any, 
                 expiry: Optional[int] = None) -> None:
        """Set value in cache."""
        ...

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        ...

    async def clear(self) -> None:
        """Clear all cache."""
        ...

class DataProcessorProtocol(Protocol):
    """Protocol defining the interface for data processing."""
    
    async def process_raw_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw data into standardized format."""
        ...

    async def aggregate_data(self, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate multiple data points."""
        ...

    async def transform_data(self, data: Dict[str, Any], 
                           format_type: str) -> Dict[str, Any]:
        """Transform data into specified format."""
        ...

class StorageProtocol(Protocol):
    """Protocol defining the interface for data storage."""
    
    async def save(self, data: Dict[str, Any]) -> str:
        """Save data and return identifier."""
        ...

    async def get(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Retrieve data by identifier."""
        ...

    async def update(self, identifier: str, data: Dict[str, Any]) -> bool:
        """Update existing data."""
        ...

    async def delete(self, identifier: str) -> bool:
        """Delete data by identifier."""
        ...

class RateLimiterProtocol(Protocol):
    """Protocol defining the interface for rate limiting."""
    
    async def acquire(self) -> bool:
        """Attempt to acquire permission for API call."""
        ...

    async def release(self) -> None:
        """Release rate limiter."""
        ...

    def get_remaining_calls(self) -> int:
        """Get number of remaining calls allowed."""
        ...

class ErrorHandlerProtocol(Protocol):
    """Protocol defining the interface for error handling."""
    
    async def handle_error(self, error: Exception, 
                         context: Optional[Dict[str, Any]] = None) -> None:
        """Handle and process error."""
        ...

    async def log_error(self, error: Exception, 
                       severity: str = "ERROR") -> None:
        """Log error with specified severity."""
        ...

    def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics."""
        ...

class MetricsProtocol(Protocol):
    """Protocol defining the interface for metrics collection."""
    
    def record_latency(self, operation: str, latency: float) -> None:
        """Record operation latency."""
        ...

    def record_error(self, error_type: str) -> None:
        """Record error occurrence."""
        ...

    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics."""
        ...