from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from decimal import Decimal

class MarketData(BaseModel):
    """Model for market data."""
    symbol: str = Field(..., description="Trading symbol")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    open: Decimal = Field(..., ge=0)
    high: Decimal = Field(..., ge=0)
    low: Decimal = Field(..., ge=0)
    close: Decimal = Field(..., ge=0)
    adj_close: Decimal = Field(..., ge=0)
    volume: int = Field(..., ge=0)
    dividends: Decimal = Field(default=Decimal('0'))
    stock_splits: Decimal = Field(default=Decimal('0'))

    @validator('volume')
    def validate_volume(cls, v):
        if v < 0:
            raise ValueError("Volume cannot be negative")
        return v

class CompanyProfile(BaseModel):
    """Model for company profile information."""
    symbol: str = Field(..., description="Trading symbol")
    name: str = Field(..., description="Company name")
    industry: Optional[str] = None
    sector: Optional[str] = None
    country: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    full_time_employees: Optional[int] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    beta: Optional[float] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class FinancialStatement(BaseModel):
    """Model for financial statement data."""
    period_end_date: datetime = Field(..., description="End date of financial period")
    items: Dict[str, Any] = Field(..., description="Financial statement items")
    currency: str = Field(default="USD", description="Currency of financial data")
    statement_type: str = Field(..., description="Type of financial statement")

class FinancialData(BaseModel):
    """Model for complete financial data."""
    income_statement: Optional[List[FinancialStatement]] = None
    balance_sheet: Optional[List[FinancialStatement]] = None
    cash_flow: Optional[List[FinancialStatement]] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class Option(BaseModel):
    """Model for options data."""
    strike: float = Field(..., description="Strike price")
    contract_name: str = Field(..., description="Option contract symbol")
    last_trade_date: datetime = Field(..., description="Last trade date")
    bid: float = Field(..., ge=0)
    ask: float = Field(..., ge=0)
    change: float
    percent_change: float
    volume: int = Field(..., ge=0)
    open_interest: int = Field(..., ge=0)
    implied_volatility: float = Field(..., ge=0)
    in_the_money: bool

class OptionsChain(BaseModel):
    """Model for complete options chain."""
    symbol: str = Field(..., description="Trading symbol")
    expiration_date: datetime = Field(..., description="Options expiration date")
    calls: List[Option] = Field(default_factory=list)
    puts: List[Option] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class EarningsEntry(BaseModel):
    """Model for earnings data."""
    date: datetime
    eps_estimate: Optional[float] = None
    eps_actual: Optional[float] = None
    surprise: Optional[float] = None
    number_of_analysts: Optional[int] = None

class EarningsData(BaseModel):
    """Model for complete earnings data."""
    symbol: str = Field(..., description="Trading symbol")
    history: List[EarningsEntry] = Field(default_factory=list)
    estimates: List[EarningsEntry] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class MarketNews(BaseModel):
    """Model for market news."""
    title: str = Field(..., description="News title")
    link: str = Field(..., description="News link")
    publisher: str = Field(..., description="News publisher")
    publish_date: datetime = Field(..., description="Publication date")
    summary: Optional[str] = None
    related_symbols: List[str] = Field(default_factory=list)

class YahooFinanceData(BaseModel):
    """Model for complete Yahoo Finance data."""
    market_data: Optional[List[MarketData]] = None
    company_profile: Optional[CompanyProfile] = None
    financial_data: Optional[FinancialData] = None
    options_chain: Optional[List[OptionsChain]] = None
    earnings_data: Optional[EarningsData] = None
    news: Optional[List[MarketNews]] = None
    error_message: Optional[str] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        arbitrary_types_allowed = True