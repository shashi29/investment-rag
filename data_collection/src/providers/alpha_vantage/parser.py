from typing import Dict, Any, List, Optional
from datetime import datetime
from ...core.exceptions import DataProcessingError
from ...utils.logger import get_logger

logger = get_logger(__name__)

class AlphaVantageParser:
    """Parser for Alpha Vantage API responses."""
    
    @staticmethod
    def parse_time_series(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse time series data from Alpha Vantage response."""
        try:
            parsed_data = []
            time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
            
            if not time_series_key:
                raise DataProcessingError(
                    "No time series data found in response",
                    "parse_time_series",
                    data
                )
                
            time_series = data[time_series_key]
            
            for timestamp, values in time_series.items():
                try:
                    parsed_entry = {
                        "timestamp": datetime.strptime(timestamp, "%Y-%m-%d"),
                        "open": float(values.get("1. open", 0)),
                        "high": float(values.get("2. high", 0)),
                        "low": float(values.get("3. low", 0)),
                        "close": float(values.get("4. close", 0)),
                        "volume": int(float(values.get("5. volume", 0))),
                        "adjusted_close": float(values.get("5. adjusted close", 0))
                    }
                    parsed_data.append(parsed_entry)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing entry for {timestamp}: {str(e)}")
                    continue
                    
            return parsed_data
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing time series data: {str(e)}",
                "parse_time_series",
                data
            )

    @staticmethod
    def parse_technical_indicators(data: Dict[str, Any], indicator_type: str) -> List[Dict[str, Any]]:
        """Parse technical indicators data from Alpha Vantage response."""
        try:
            parsed_data = []
            technical_key = f"Technical Analysis: {indicator_type}"
            
            if technical_key not in data:
                raise DataProcessingError(
                    f"No {indicator_type} data found in response",
                    "parse_technical_indicators",
                    data
                )
                
            indicators = data[technical_key]
            
            for timestamp, values in indicators.items():
                try:
                    parsed_entry = {
                        "timestamp": datetime.strptime(timestamp, "%Y-%m-%d"),
                        "values": {k: float(v) for k, v in values.items()}
                    }
                    parsed_data.append(parsed_entry)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing indicator for {timestamp}: {str(e)}")
                    continue
                    
            return parsed_data
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing technical indicators: {str(e)}",
                "parse_technical_indicators",
                data
            )

    @staticmethod
    def parse_quote(data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse global quote data from Alpha Vantage response."""
        try:
            quote_data = data.get("Global Quote", {})
            
            if not quote_data:
                raise DataProcessingError(
                    "No quote data found in response",
                    "parse_quote",
                    data
                )
                
            return {
                "symbol": quote_data.get("01. symbol", ""),
                "price": float(quote_data.get("05. price", 0)),
                "change": float(quote_data.get("09. change", 0)),
                "change_percent": float(quote_data.get("10. change percent", "0").strip("%")),
                "volume": int(float(quote_data.get("06. volume", 0))),
                "latest_trading_day": quote_data.get("07. latest trading day", ""),
                "previous_close": float(quote_data.get("08. previous close", 0)),
                "timestamp": datetime.now()
            }
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing quote data: {str(e)}",
                "parse_quote",
                data
            )

    @staticmethod
    def parse_metadata(data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse metadata from Alpha Vantage response."""
        try:
            metadata_key = next((k for k in data.keys() if "Meta Data" in k), None)
            
            if not metadata_key:
                return {}
                
            metadata = data[metadata_key]
            return {
                "symbol": metadata.get("2. Symbol", ""),
                "last_refreshed": metadata.get("3. Last Refreshed", ""),
                "interval": metadata.get("4. Interval", ""),
                "output_size": metadata.get("5. Output Size", ""),
                "timezone": metadata.get("6. Time Zone", "")
            }
            
        except Exception as e:
            logger.warning(f"Error parsing metadata: {str(e)}")
            return {}