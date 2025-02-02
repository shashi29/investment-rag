from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from pydantic import ValidationError
from ..core.exceptions import ValidationError as DataValidationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
    """Service for validating market data."""
    
    def __init__(self):
        self.validation_rules = {
            'price': self._validate_price,
            'volume': self._validate_volume,
            'timestamp': self._validate_timestamp,
            'sequence': self._validate_sequence
        }

    async def validate_market_data(
        self,
        data: Dict[str, Any],
        rules: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Validate market data against specified rules."""
        try:
            validation_results = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'timestamp': datetime.now()
            }

            # Convert to DataFrame for validation
            df = self._prepare_dataframe(data)
            
            # Apply validation rules
            rules_to_apply = rules if rules else self.validation_rules.keys()
            
            for rule in rules_to_apply:
                if rule in self.validation_rules:
                    rule_result = await self.validation_rules[rule](df)
                    if not rule_result['is_valid']:
                        validation_results['is_valid'] = False
                        validation_results['errors'].extend(rule_result['errors'])
                        validation_results['warnings'].extend(rule_result.get('warnings', []))

            return validation_results

        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            raise DataValidationError(f"Validation error: {str(e)}")

    def _prepare_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Prepare data for validation by converting to DataFrame."""
        try:
            time_series = data.get('data', [])
            if not time_series:
                raise DataValidationError("No time series data found")

            df = pd.DataFrame(time_series)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df

        except Exception as e:
            logger.error(f"Error preparing DataFrame: {str(e)}")
            raise DataValidationError(f"Data preparation error: {str(e)}")

    async def _validate_price(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate price data."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Check for negative prices
            negative_prices = df[df[['open', 'high', 'low', 'close']].lt(0).any(axis=1)]
            if not negative_prices.empty:
                result['is_valid'] = False
                result['errors'].append("Negative prices found")

            # Check for high-low relationship
            invalid_hl = df[df['high'] < df['low']]
            if not invalid_hl.empty:
                result['is_valid'] = False
                result['errors'].append("High price less than low price")

            # Check for extreme price changes (warning)
            pct_change = df['close'].pct_change().abs()
            extreme_changes = pct_change[pct_change > 0.2]  # 20% change
            if not extreme_changes.empty:
                result['warnings'].append("Extreme price changes detected")

            return result

        except Exception as e:
            logger.error(f"Error validating prices: {str(e)}")
            raise DataValidationError(f"Price validation error: {str(e)}")

    async def _validate_volume(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate volume data."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Check for negative volume
            negative_volume = df[df['volume'] < 0]
            if not negative_volume.empty:
                result['is_valid'] = False
                result['errors'].append("Negative volume found")

            # Check for zero volume (warning)
            zero_volume = df[df['volume'] == 0]
            if not zero_volume.empty:
                result['warnings'].append("Zero volume periods found")

            # Check for extreme volume spikes (warning)
            avg_volume = df['volume'].mean()
            extreme_volume = df[df['volume'] > avg_volume * 10]
            if not extreme_volume.empty:
                result['warnings'].append("Extreme volume spikes detected")

            return result

        except Exception as e:
            logger.error(f"Error validating volume: {str(e)}")
            raise DataValidationError(f"Volume validation error: {str(e)}")

    async def _validate_timestamp(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate timestamp data."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Check for future timestamps
            future_data = df[df['timestamp'] > datetime.now()]
            if not future_data.empty:
                result['is_valid'] = False
                result['errors'].append("Future timestamps found")

            # Check for duplicate timestamps
            duplicates = df[df['timestamp'].duplicated()]
            if not duplicates.empty:
                result['is_valid'] = False
                result['errors'].append("Duplicate timestamps found")

            # Check for gaps in timestamps (warning)
            time_diff = df['timestamp'].diff()
            expected_diff = pd.Timedelta('1d')  # Assuming daily data
            gaps = time_diff[time_diff > expected_diff * 1.5]
            if not gaps.empty:
                result['warnings'].append("Gaps in timestamp sequence detected")

            return result

        except Exception as e:
            logger.error(f"Error validating timestamps: {str(e)}")
            raise DataValidationError(f"Timestamp validation error: {str(e)}")

    async def _validate_sequence(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate data sequence and consistency."""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }

        try:
            # Check OHLC sequence
            invalid_ohlc = df[
                (df['open'] > df['high']) |
                (df['close'] > df['high']) |
                (df['open'] < df['low']) |
                (df['close'] < df['low'])
            ]
            if not invalid_ohlc.empty:
                result['is_valid'] = False
                result['errors'].append("Invalid OHLC sequence found")

            # Check for missing required fields
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                result['is_valid'] = False
                result['errors'].append(f"Missing required fields: {missing_fields}")

            return result

        except Exception as e:
            logger.error(f"Error validating sequence: {str(e)}")
            raise DataValidationError(f"Sequence validation error: {str(e)}")

    def add_validation_rule(
        self,
        rule_name: str,
        rule_func: callable
    ) -> None:
        """Add a custom validation rule."""
        try:
            if rule_name in self.validation_rules:
                raise DataValidationError(f"Validation rule {rule_name} already exists")
            
            self.validation_rules[rule_name] = rule_func
            logger.info(f"Added new validation rule: {rule_name}")

        except Exception as e:
            logger.error(f"Error adding validation rule: {str(e)}")
            raise DataValidationError(f"Error adding validation rule: {str(e)}")

    async def validate_multiple(
        self,
        data_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Validate multiple datasets."""
        try:
            results = []
            for data in data_list:
                validation_result = await self.validate_market_data(data)
                results.append(validation_result)
            return results

        except Exception as e:
            logger.error(f"Error validating multiple datasets: {str(e)}")
            raise DataValidationError(f"Multiple validation error: {str(e)}")