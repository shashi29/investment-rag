import yfinance as yf
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import asyncio
from ...core.base_classes import DataProvider
from ...core.exceptions import APIError, DataProcessingError
from ...utils.logger import get_logger
from .models import YahooFinanceData

logger = get_logger(__name__)

class YahooFinanceClient(DataProvider):
    """Client for Yahoo Finance API."""
    
    def __init__(self):
        """Initialize Yahoo Finance client."""
        super().__init__(api_key=None, base_url=None)  # Yahoo Finance doesn't require API key
        self.session = None

    async def fetch_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Fetch historical data for a given symbol and date range."""
        try:
            # Run yfinance operations in a thread pool as they are blocking
            data = await asyncio.to_thread(
                self._fetch_historical_data,
                symbol,
                start_date,
                end_date
            )
            return await self.transform_data(data)
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise APIError(f"Yahoo Finance API error: {str(e)}")

    def _fetch_historical_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch historical data using yfinance."""
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=start_date,
            end=end_date,
            interval="1d"
        )
        
        if df.empty:
            raise DataProcessingError(f"No data found for {symbol} between {start_date} and {end_date}")
        
        df.reset_index(inplace=True)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    async def get_real_time_data(self, symbol: str) -> Dict[str, Any]:
        """Get real-time data for a symbol."""
        try:
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            info = await asyncio.to_thread(lambda: ticker.info)
            
            return {
                "symbol": symbol,
                "timestamp": datetime.now(),
                "price": info.get("regularMarketPrice"),
                "volume": info.get("regularMarketVolume"),
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("forwardPE"),
                "dividend_yield": info.get("dividendYield")
            }
            
        except Exception as e:
            logger.error(f"Error fetching real-time data for {symbol}: {str(e)}")
            raise APIError(f"Yahoo Finance API error: {str(e)}")

    async def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """Get company information."""
        try:
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            info = await asyncio.to_thread(lambda: ticker.info)
            
            return {
                "symbol": symbol,
                "name": info.get("longName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "country": info.get("country"),
                "website": info.get("website"),
                "description": info.get("longBusinessSummary"),
                "employees": info.get("fullTimeEmployees")
            }
            
        except Exception as e:
            logger.error(f"Error fetching company info for {symbol}: {str(e)}")
            raise APIError(f"Yahoo Finance API error: {str(e)}")

    async def get_financial_data(
        self, 
        symbol: str, 
        statements: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get financial statements data."""
        try:
            ticker = await asyncio.to_thread(yf.Ticker, symbol)
            financials = {}
            
            if not statements or "income" in statements:
                financials["income_statement"] = await asyncio.to_thread(
                    lambda: ticker.financials.to_dict()
                )
            
            if not statements or "balance" in statements:
                financials["balance_sheet"] = await asyncio.to_thread(
                    lambda: ticker.balance_sheet.to_dict()
                )
            
            if not statements or "cash" in statements:
                financials["cash_flow"] = await asyncio.to_thread(
                    lambda: ticker.cashflow.to_dict()
                )
            
            return financials
            
        except Exception as e:
            logger.error(f"Error fetching financial data for {symbol}: {str(e)}")
            raise APIError(f"Yahoo Finance API error: {str(e)}")

    async def validate_response(self, response: Dict[str, Any]) -> bool:
        """Validate the API response."""
        if not response:
            return False
        if "error" in response:
            return False
        return True

    async def transform_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Transform pandas DataFrame to standardized format."""
        try:
            if type(data) is dict and 'metadata' in data:
                return data
            
            transformed_data = {
                "metadata": {
                    "provider": "yahoo_finance",
                    "timestamp": datetime.now()
                },
                "data": []
            }
            
            for index, row in data.iterrows():
                entry = {
                    "timestamp": row.get("Date"),
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": float(row.get("Close", 0)),
                    "volume": int(row.get("Volume", 0)),
                    "dividends": float(row.get("Dividends", 0)),
                    "stock_splits": float(row.get("Stock Splits", 0))
                }
                transformed_data["data"].append(entry)
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise DataProcessingError(
                f"Error transforming Yahoo Finance data: {str(e)}",
                "transform_data",
                data
            )