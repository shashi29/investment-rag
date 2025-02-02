from typing import Any, Dict, Optional

class DataCollectionError(Exception):
    """Base exception for data collection errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

class APIError(DataCollectionError):
    """Exception for API-related errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response: Optional[Dict[str, Any]] = None):
        super().__init__(message, {
            'status_code': status_code,
            'response': response
        })
        self.status_code = status_code
        self.response = response

class RateLimitError(APIError):
    """Exception for rate limit violations."""
    
    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message, 429, {'retry_after': retry_after})
        self.retry_after = retry_after

class ValidationError(DataCollectionError):
    """Exception for data validation errors."""
    
    def __init__(self, message: str, validation_errors: Dict[str, Any]):
        super().__init__(message, {'validation_errors': validation_errors})
        self.validation_errors = validation_errors

class DataProcessingError(DataCollectionError):
    """Exception for data processing errors."""
    
    def __init__(self, message: str, processing_step: str, 
                 input_data: Optional[Dict[str, Any]] = None):
        super().__init__(message, {
            'processing_step': processing_step,
            'input_data': input_data
        })
        self.processing_step = processing_step
        self.input_data = input_data

class CacheError(DataCollectionError):
    """Exception for cache-related errors."""
    
    def __init__(self, message: str, cache_operation: str):
        super().__init__(message, {'cache_operation': cache_operation})
        self.cache_operation = cache_operation

class ConfigurationError(DataCollectionError):
    """Exception for configuration-related errors."""
    
    def __init__(self, message: str, config_key: Optional[str] = None):
        super().__init__(message, {'config_key': config_key})
        self.config_key = config_key

class ProviderError(DataCollectionError):
    """Exception for data provider-specific errors."""
    
    def __init__(self, message: str, provider: str, 
                 provider_error: Optional[Dict[str, Any]] = None):
        super().__init__(message, {
            'provider': provider,
            'provider_error': provider_error
        })
        self.provider = provider
        self.provider_error = provider_error

class AuthenticationError(APIError):
    """Exception for authentication-related errors."""
    
    def __init__(self, message: str, provider: str):
        super().__init__(message, 401, {'provider': provider})
        self.provider = provider

class NetworkError(DataCollectionError):
    """Exception for network-related errors."""
    
    def __init__(self, message: str, retry_count: int = 0):
        super().__init__(message, {'retry_count': retry_count})
        self.retry_count = retry_count

class DataNotFoundError(DataCollectionError):
    """Exception for when requested data is not found."""
    
    def __init__(self, message: str, query: Dict[str, Any]):
        super().__init__(message, {'query': query})
        self.query = query
        
        
class DataStorageError(DataCollectionError):
    """Exception for data storage-related errors."""
    
    def __init__(self, message: str):
        super().__init__(message)