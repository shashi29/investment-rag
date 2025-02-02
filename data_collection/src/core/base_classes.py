from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime

class DataProvider(ABC):
    """Base class for all data providers."""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self._headers = {"Content-Type": "application/json"}

    @abstractmethod
    async def fetch_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch data for a given symbol and date range."""
        pass

    @abstractmethod
    async def validate_response(self, response: Dict[str, Any]) -> bool:
        """Validate the API response."""
        pass

    @abstractmethod
    async def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the data into a standardized format."""
        pass

class DataValidator(ABC):
    """Base class for data validation."""
    
    @abstractmethod
    async def validate(self, data: Dict[str, Any]) -> bool:
        """Validate the data."""
        pass

    @abstractmethod
    async def clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and sanitize the data."""
        pass

class RateLimiter(ABC):
    """Base class for rate limiting implementation."""
    
    def __init__(self, calls_per_minute: int, calls_per_day: Optional[int] = None):
        self.calls_per_minute = calls_per_minute
        self.calls_per_day = calls_per_day
        self.calls: List[datetime] = []

    @abstractmethod
    async def acquire(self) -> bool:
        """Acquire permission to make an API call."""
        pass

    @abstractmethod
    async def release(self) -> None:
        """Release the rate limiter."""
        pass

class DataProcessor(ABC):
    """Base class for data processing."""
    
    @abstractmethod
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process the data."""
        pass

    @abstractmethod
    async def aggregate(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate multiple data points."""
        pass

class CacheManager(ABC):
    """Base class for cache management."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get data from cache."""
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, expiry: Optional[int] = None) -> None:
        """Set data in cache."""
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete data from cache."""
        pass

class ErrorHandler(ABC):
    """Base class for error handling."""
    
    @abstractmethod
    async def handle_error(self, error: Exception) -> None:
        """Handle an error."""
        pass

    @abstractmethod
    async def log_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Log an error with context."""
        pass

class DataStore(ABC):
    """Base class for data storage."""
    
    @abstractmethod
    async def save(self, data: Dict[str, Any]) -> bool:
        """Save data to storage."""
        pass

    @abstractmethod
    async def get(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Retrieve data from storage."""
        pass

    @abstractmethod
    async def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Update existing data."""
        pass

class DataCollector(ABC):
    """Base class for data collection orchestration."""
    
    def __init__(self, provider: DataProvider, validator: DataValidator, 
                 processor: DataProcessor, cache: CacheManager):
        self.provider = provider
        self.validator = validator
        self.processor = processor
        self.cache = cache

    @abstractmethod
    async def collect(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Collect data for a given symbol and date range."""
        pass

    @abstractmethod
    async def validate_and_process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and process collected data."""
        pass