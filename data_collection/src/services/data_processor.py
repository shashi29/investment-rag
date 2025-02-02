from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..core.exceptions import DataProcessingError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataProcessor:
    """Service for processing and analyzing collected data."""
    
    def __init__(self):
        self.technical_indicators = {
            'sma': self._calculate_sma,
            'ema': self._calculate_ema,
            'rsi': self._calculate_rsi,
            'macd': self._calculate_macd,
            'bollinger_bands': self._calculate_bollinger_bands
        }

    def process_market_data(
        self,
        data: Dict[str, Any],
        indicators: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Process market data and calculate technical indicators."""
        try:
            # Convert to pandas DataFrame
            df = self._convert_to_dataframe(data)
            if df.empty:
                raise DataProcessingError("Empty dataset provided")

            # Calculate requested indicators
            indicators_data = {}
            if indicators:
                for indicator in indicators:
                    if indicator in self.technical_indicators:
                        indicators_data[indicator] = self.technical_indicators[indicator](df)

            # Prepare processed data
            processed_data = {
                "symbol": data.get("symbol", "Unknown"),
                "timestamp": datetime.now(),
                "market_data": df.to_dict(orient='records'),
                "technical_indicators": indicators_data
            }

            return processed_data

        except Exception as e:
            logger.error(f"Error processing market data: {str(e)}")
            raise DataProcessingError(f"Data processing error: {str(e)}")

    def _convert_to_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Convert market data to pandas DataFrame."""
        try:
            # Extract time series data
            time_series = data.get("data", [])
            if not time_series:
                raise DataProcessingError("No time series data found")

            # Create DataFrame
            df = pd.DataFrame(time_series)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Sort by timestamp
            df.sort_index(inplace=True)
            
            return df

        except Exception as e:
            logger.error(f"Error converting to DataFrame: {str(e)}")
            raise DataProcessingError(f"DataFrame conversion error: {str(e)}")

    def _calculate_sma(
        self,
        df: pd.DataFrame,
        window: int = 20
    ) -> Dict[str, List[float]]:
        """Calculate Simple Moving Average."""
        try:
            sma = df['close'].rolling(window=window).mean()
            return {
                'periods': window,
                'values': sma.tolist()
            }
        except Exception as e:
            logger.error(f"Error calculating SMA: {str(e)}")
            raise DataProcessingError(f"SMA calculation error: {str(e)}")

    def _calculate_ema(
        self,
        df: pd.DataFrame,
        window: int = 20
    ) -> Dict[str, List[float]]:
        """Calculate Exponential Moving Average."""
        try:
            ema = df['close'].ewm(span=window, adjust=False).mean()
            return {
                'periods': window,
                'values': ema.tolist()
            }
        except Exception as e:
            logger.error(f"Error calculating EMA: {str(e)}")
            raise DataProcessingError(f"EMA calculation error: {str(e)}")

    def _calculate_rsi(
        self,
        df: pd.DataFrame,
        window: int = 14
    ) -> Dict[str, List[float]]:
        """Calculate Relative Strength Index."""
        try:
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return {
                'periods': window,
                'values': rsi.tolist()
            }
        except Exception as e:
            logger.error(f"Error calculating RSI: {str(e)}")
            raise DataProcessingError(f"RSI calculation error: {str(e)}")

    def _calculate_macd(
        self,
        df: pd.DataFrame,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9
    ) -> Dict[str, Dict[str, List[float]]]:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        try:
            # Calculate MACD line
            fast_ema = df['close'].ewm(span=fast_period, adjust=False).mean()
            slow_ema = df['close'].ewm(span=slow_period, adjust=False).mean()
            macd_line = fast_ema - slow_ema
            
            # Calculate signal line
            signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
            
            # Calculate histogram
            histogram = macd_line - signal_line
            
            return {
                'parameters': {
                    'fast_period': fast_period,
                    'slow_period': slow_period,
                    'signal_period': signal_period
                },
                'macd_line': macd_line.tolist(),
                'signal_line': signal_line.tolist(),
                'histogram': histogram.tolist()
            }
        except Exception as e:
            logger.error(f"Error calculating MACD: {str(e)}")
            raise DataProcessingError(f"MACD calculation error: {str(e)}")

    def _calculate_bollinger_bands(
        self,
        df: pd.DataFrame,
        window: int = 20,
        num_std: float = 2.0
    ) -> Dict[str, Dict[str, List[float]]]:
        """Calculate Bollinger Bands."""
        try:
            sma = df['close'].rolling(window=window).mean()
            std = df['close'].rolling(window=window).std()
            
            upper_band = sma + (std * num_std)
            lower_band = sma - (std * num_std)
            
            return {
                'parameters': {
                    'window': window,
                    'num_std': num_std
                },
                'middle_band': sma.tolist(),
                'upper_band': upper_band.tolist(),
                'lower_band': lower_band.tolist()
            }
        except Exception as e:
            logger.error(f"Error calculating Bollinger Bands: {str(e)}")
            raise DataProcessingError(f"Bollinger Bands calculation error: {str(e)}")