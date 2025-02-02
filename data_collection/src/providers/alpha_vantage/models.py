from typing import Any, Optional, List, Dict
from datetime import datetime
from pydantic import BaseModel, Field, validator
from decimal import Decimal

class AlphaVantageMetadata(BaseModel):
    """Model for Alpha Vantage metadata."""
    information: str = Field(..., description="Information about the data")
    symbol: str = Field(..., description="Trading symbol")
    last_refreshed: datetime = Field(..., description="Last data refresh time")
    time_zone: str = Field(..., description="Timezone of the data")

class GlobalQuote(BaseModel):
    """Model for global quote data."""
    symbol: str = Field(..., description="Trading symbol")
    open: Decimal = Field(..., ge=0, description="Opening price")
    high: Decimal = Field(..., ge=0, description="Highest price")
    low: Decimal = Field(..., ge=0, description="Lowest price")
    price: Decimal = Field(..., ge=0, description="Current price")
    volume: int = Field(..., ge=0, description="Trading volume")
    latest_trading_day: datetime = Field(..., description="Latest trading day")
    previous_close: Decimal = Field(..., ge=0, description="Previous closing price")
    change: Decimal = Field(..., description="Price change")
    change_percent: float = Field(..., description="Price change percentage")

    @validator('change_percent')
    def validate_change_percent(cls, v):
        """Validate change percentage."""
        if v < -100 or v > 100:
            raise ValueError("Change percentage must be between -100 and 100")
        return v

class TechnicalIndicator(BaseModel):
    """Model for technical indicator data."""
    timestamp: datetime = Field(..., description="Timestamp of the indicator")
    indicator_type: str = Field(..., description="Type of technical indicator")
    values: Dict[str, Decimal] = Field(..., description="Indicator values")
    
    @validator('values')
    def validate_values(cls, v):
        """Validate indicator values."""
        if not v:
            raise ValueError("Values dictionary cannot be empty")
        return v

class AlphaVantageResponse(BaseModel):
    """Model for complete Alpha Vantage response."""
    metadata: Optional[AlphaVantageMetadata] = Field(None, description="Response metadata")
    time_series: Optional[List[TimeSeriesEntry]] = Field(None, description="Time series data")
    technical_indicators: Optional[List[TechnicalIndicator]] = Field(None, description="Technical indicators")
    global_quote: Optional[GlobalQuote] = Field(None, description="Global quote data")
    error_message: Optional[str] = Field(None, description="Error message if any")
    
    @validator('time_series', 'technical_indicators')
    def validate_lists(cls, v):
        """Validate list fields."""
        if v is not None and not v:
            raise ValueError("List cannot be empty if provided")
        return v

class SearchResult(BaseModel):
    """Model for symbol search results."""
    symbol: str = Field(..., description="Symbol")
    name: str = Field(..., description="Company name")
    type: str = Field(..., description="Security type")
    region: str = Field(..., description="Region")
    market_open: str = Field(..., description="Market open time")
    market_close: str = Field(..., description="Market close time")
    timezone: str = Field(..., description="Timezone")
    currency: str = Field(..., description="Currency")
    match_score: float = Field(..., ge=0, le=1, description="Search match score")

class CompanyOverview(BaseModel):
    """Model for company overview data."""
    symbol: str = Field(..., description="Symbol")
    asset_type: str = Field(..., description="Asset type")
    name: str = Field(..., description="Company name")
    description: str = Field(..., description="Company description")
    exchange: str = Field(..., description="Exchange")
    currency: str = Field(..., description="Currency")
    country: str = Field(..., description="Country")
    sector: str = Field(..., description="Sector")
    industry: str = Field(..., description="Industry")
    market_cap: Optional[int] = Field(None, description="Market capitalization")
    pe_ratio: Optional[float] = Field(None, description="P/E ratio")
    dividend_yield: Optional[float] = Field(None, description="Dividend yield")
    fiscal_year_end: str = Field(..., description="Fiscal year end")
    latest_quarter: datetime = Field(..., description="Latest quarter")
    
    @validator('pe_ratio', 'dividend_yield')
    def validate_ratios(cls, v):
        """Validate financial ratios."""
        if v is not None and v < 0:
            raise ValueError("Financial ratios cannot be negative")
        return v

class ErrorResponse(BaseModel):
    """Model for error responses."""
    error_code: str = Field(..., description="Error code")
    error_message: str = Field(..., description="Error message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    additional_info: Optional[Dict[str, Any]] = Field(None, description="Additional error information")

class RateLimitInfo(BaseModel):
    """Model for rate limit information."""
    minute_limit: int = Field(..., description="Calls allowed per minute")
    minute_usage: int = Field(..., description="Calls used in current minute")
    daily_limit: int = Field(..., description="Calls allowed per day")
    daily_usage: int = Field(..., description="Calls used today")
    reset_timestamp: datetime = Field(..., description="Rate limit reset timestamp")