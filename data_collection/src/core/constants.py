from enum import Enum
from typing import Dict, Any

# API Configuration
DEFAULT_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Rate Limiting
DEFAULT_RATE_LIMIT = {
    'calls_per_minute': 5,
    'calls_per_day': 500
}

# Cache Configuration
DEFAULT_CACHE_EXPIRY = 300  # seconds
MAX_CACHE_SIZE = 1000  # items

# Data Validation
REQUIRED_FIELDS = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
MAX_PRICE_DIGITS = 4
MAX_VOLUME_DIGITS = 0

class TimeFrame(Enum):
    """Enum for different timeframes."""
    MINUTE_1 = '1m'
    MINUTE_5 = '5m'
    MINUTE_15 = '15m'
    MINUTE_30 = '30m'
    HOUR_1 = '1h'
    HOUR_4 = '4h'
    DAY_1 = '1d'
    WEEK_1 = '1w'
    MONTH_1 = '1M'

class DataProvider(Enum):
    """Enum for different data providers."""
    ALPHA_VANTAGE = 'alpha_vantage'
    YAHOO_FINANCE = 'yahoo_finance'

class DataType(Enum):
    """Enum for different types of data."""
    REAL_TIME = 'real_time'
    HISTORICAL = 'historical'
    TECHNICAL = 'technical'
    FUNDAMENTAL = 'fundamental'

class ErrorCode(Enum):
    """Enum for error codes."""
    API_ERROR = 'api_error'
    RATE_LIMIT = 'rate_limit'
    CACHE_ERROR = 'cache_error'
    DATA_ERROR = 'data_error'