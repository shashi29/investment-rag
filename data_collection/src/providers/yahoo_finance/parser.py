from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
from ...core.exceptions import DataProcessingError
from ...utils.logger import get_logger

logger = get_logger(__name__)

class YahooFinanceParser:
    """Parser for Yahoo Finance data."""
    
    @staticmethod
    def parse_historical_data(data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parse historical price data from Yahoo Finance."""
        try:
            parsed_data = []
            
            for index, row in data.iterrows():
                entry = {
                    "timestamp": index,
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": float(row.get("Close", 0)),
                    "adj_close": float(row.get("Adj Close", 0)),
                    "volume": int(row.get("Volume", 0)),
                    "dividends": float(row.get("Dividends", 0)),
                    "stock_splits": float(row.get("Stock Splits", 0))
                }
                parsed_data.append(entry)
            
            return parsed_data
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing historical data: {str(e)}",
                "parse_historical_data",
                data.to_dict()
            )

    @staticmethod
    def parse_financial_statements(data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Parse financial statements data."""
        try:
            parsed_statements = {}
            
            for statement_type, df in data.items():
                if df is not None and not df.empty:
                    # Convert DataFrame to dictionary format
                    statement_data = []
                    for col in df.columns:
                        period_data = {
                            "period_end_date": col if isinstance(col, pd.Timestamp) else str(col),
                            "items": {}
                        }
                        
                        for idx in df.index:
                            value = df[col][idx]
                            # Handle different data types
                            if pd.isna(value):
                                processed_value = None
                            elif isinstance(value, (int, float)):
                                processed_value = float(value)
                            else:
                                processed_value = str(value)
                                
                            period_data["items"][idx] = processed_value
                            
                        statement_data.append(period_data)
                        
                    parsed_statements[statement_type] = statement_data
                    
            return parsed_statements
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing financial statements: {str(e)}",
                "parse_financial_statements",
                str(data)
            )

    @staticmethod
    def parse_company_info(info: Dict[str, Any]) -> Dict[str, Any]:
        """Parse company information."""
        try:
            return {
                "symbol": info.get("symbol"),
                "name": info.get("longName"),
                "industry": info.get("industry"),
                "sector": info.get("sector"),
                "country": info.get("country"),
                "website": info.get("website"),
                "description": info.get("longBusinessSummary"),
                "full_time_employees": info.get("fullTimeEmployees"),
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "pe_ratio": info.get("forwardPE"),
                "peg_ratio": info.get("pegRatio"),
                "dividend_yield": info.get("dividendYield"),
                "beta": info.get("beta"),
                "52_week_high": info.get("fiftyTwoWeekHigh"),
                "52_week_low": info.get("fiftyTwoWeekLow"),
                "50_day_average": info.get("fiftyDayAverage"),
                "200_day_average": info.get("twoHundredDayAverage"),
                "last_updated": datetime.now()
            }
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing company info: {str(e)}",
                "parse_company_info",
                info
            )

    @staticmethod
    def parse_options_data(data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parse options chain data."""
        try:
            options_data = []
            
            for index, row in data.iterrows():
                option = {
                    "strike": float(row.get("strike", 0)),
                    "contract_name": str(row.get("contractSymbol", "")),
                    "last_trade_date": row.get("lastTradeDate") if pd.notnull(row.get("lastTradeDate")) else None,
                    "bid": float(row.get("bid", 0)),
                    "ask": float(row.get("ask", 0)),
                    "change": float(row.get("change", 0)),
                    "percent_change": float(row.get("percentChange", 0)),
                    "volume": int(row.get("volume", 0)) if pd.notnull(row.get("volume")) else 0,
                    "open_interest": int(row.get("openInterest", 0)) if pd.notnull(row.get("openInterest")) else 0,
                    "implied_volatility": float(row.get("impliedVolatility", 0)),
                    "in_the_money": bool(row.get("inTheMoney", False))
                }
                options_data.append(option)
                
            return options_data
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing options data: {str(e)}",
                "parse_options_data",
                data.to_dict()
            )

    @staticmethod
    def parse_earnings(data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse earnings data."""
        try:
            earnings = {
                "history": [],
                "estimates": []
            }
            
            # Parse earnings history
            if "history" in data and isinstance(data["history"], pd.DataFrame):
                for index, row in data["history"].iterrows():
                    entry = {
                        "date": index if isinstance(index, pd.Timestamp) else str(index),
                        "eps_estimate": float(row.get("epsEstimate", 0)) if pd.notnull(row.get("epsEstimate")) else None,
                        "eps_actual": float(row.get("epsActual", 0)) if pd.notnull(row.get("epsActual")) else None,
                        "surprise": float(row.get("surprise", 0)) if pd.notnull(row.get("surprise")) else None
                    }
                    earnings["history"].append(entry)
                    
            # Parse earnings estimates
            if "estimates" in data and isinstance(data["estimates"], pd.DataFrame):
                for index, row in data["estimates"].iterrows():
                    estimate = {
                        "date": index if isinstance(index, pd.Timestamp) else str(index),
                        "eps_estimate": float(row.get("epsEstimate", 0)) if pd.notnull(row.get("epsEstimate")) else None,
                        "number_of_analysts": int(row.get("numberOfAnalysts", 0)) if pd.notnull(row.get("numberOfAnalysts")) else None
                    }
                    earnings["estimates"].append(estimate)
                    
            return earnings
            
        except Exception as e:
            raise DataProcessingError(
                f"Error parsing earnings data: {str(e)}",
                "parse_earnings",
                str(data)
            )