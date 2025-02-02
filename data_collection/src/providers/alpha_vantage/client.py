from typing import Dict, Any, Optional
import aiohttp
import asyncio
from datetime import datetime
from ...core.base_classes import DataProvider
from ...core.exceptions import APIError, RateLimitError, NetworkError
from ...utils.rate_limiter import RateLimiter
from ...utils.logger import get_logger

logger = get_logger(__name__)

class AlphaVantageClient(DataProvider):
    """Client for Alpha Vantage API."""
    
    def __init__(self, api_key: str, base_url: str = "https://www.alphavantage.co/query"):
        super().__init__(api_key, base_url)
        self.rate_limiter = RateLimiter(calls_per_minute=5, calls_per_day=500)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Set up async context."""
        self.session = aiohttp.ClientSession(headers=self._get_headers())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up async context."""
        if self.session:
            await self.session.close()

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        return {
            "Content-Type": "application/json",
            "User-Agent": "TradingDataCollector/1.0"
        }

    async def fetch_data(
        self, 
        symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Fetch data for a given symbol and date range."""
        try:
            await self.rate_limiter.acquire()
            
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "apikey": self.api_key,
                "outputsize": "full"
            }
            
            if not self.session:
                raise RuntimeError("Client session not initialized")
                
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    raise RateLimitError("Alpha Vantage rate limit exceeded")
                    
                if response.status != 200:
                    raise APIError(
                        f"Alpha Vantage API error: {response.status}",
                        status_code=response.status,
                        response=await response.json()
                    )
                    
                data = await response.json()
                return await self.transform_data(data)
                
        except aiohttp.ClientError as e:
            raise NetworkError(f"Network error during API call: {str(e)}")
        finally:
            await self.rate_limiter.release()

    async def get_intraday_data(
        self, 
        symbol: str, 
        interval: str = "5min"
    ) -> Dict[str, Any]:
        """Get intraday data for a symbol."""
        try:
            await self.rate_limiter.acquire()
            
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": symbol,
                "interval": interval,
                "apikey": self.api_key,
                "outputsize": "compact"
            }
            
            if not self.session:
                raise RuntimeError("Client session not initialized")
                
            async with self.session.get(self.base_url, params=params) as response:
                return await self._handle_response(response)
                
        except Exception as e:
            logger.error(f"Error fetching intraday data: {str(e)}")
            raise
        finally:
            await self.rate_limiter.release()

    async def get_technical_indicators(
        self,
        symbol: str,
        indicator: str,
        interval: str = "daily",
        time_period: int = 14
    ) -> Dict[str, Any]:
        """Get technical indicators for a symbol."""
        try:
            await self.rate_limiter.acquire()
            
            params = {
                "function": indicator,
                "symbol": symbol,
                "interval": interval,
                "time_period": time_period,
                "apikey": self.api_key
            }
            
            if not self.session:
                raise RuntimeError("Client session not initialized")
                
            async with self.session.get(self.base_url, params=params) as response:
                return await self._handle_response(response)
                
        except Exception as e:
            logger.error(f"Error fetching technical indicators: {str(e)}")
            raise
        finally:
            await self.rate_limiter.release()

    async def _handle_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """Handle API response and common error cases."""
        if response.status == 429:
            retry_after = response.headers.get("Retry-After", 60)
            raise RateLimitError("Rate limit exceeded", retry_after=int(retry_after))
            
        if response.status != 200:
            raise APIError(
                f"API request failed with status {response.status}",
                status_code=response.status,
                response=await response.json()
            )
            
        data = await response.json()
        
        if "Error Message" in data:
            raise APIError(f"API Error: {data['Error Message']}")
            
        return data

    async def validate_response(self, response: Dict[str, Any]) -> bool:
        """Validate the API response."""
        if "Error Message" in response:
            return False
        if "Note" in response:  # Alpha Vantage standard rate limit message
            return False
        return True

    async def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the data into a standardized format."""
        transformed_data = {
            "metadata": {
                "provider": "alpha_vantage",
                "timestamp": datetime.now()
            },
            "data": []
        }
        
        # Extract time series data
        time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
        if time_series_key and isinstance(data[time_series_key], dict):
            time_series = data[time_series_key]
            
            for date, values in time_series.items():
                entry = {
                    "timestamp": date,
                    "open": float(values.get("1. open", 0)),
                    "high": float(values.get("2. high", 0)),
                    "low": float(values.get("3. low", 0)),
                    "close": float(values.get("4. close", 0)),
                    "volume": int(float(values.get("5. volume", 0)))
                }
                transformed_data["data"].append(entry)
                
        return transformed_data