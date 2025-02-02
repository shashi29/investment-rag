from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import pandas as pd
import numpy as np
from ..core.exceptions import ValidationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class ValidationRule:
    """Base class for validation rules."""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    async def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data according to the rule."""
        raise NotImplementedError("Validate method must be implemented")

class PriceValidationRule(ValidationRule):
    """Validation rules for price data."""
    
    def __init__(self):
        super().__init__(
            name="price_validation",
            description="Validates price data for consistency and validity"
        )

    async def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data.get("data", []))
            result = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }

            # Check for negative prices
            for col in ['open', 'high', 'low', 'close']:
                if (df[col] < 0).any():
                    result["is_valid"] = False
                    result["errors"].append(f"Negative values found in {col}")

            # Check price relationships
            invalid_hl = df[df['high'] < df['low']]
            if not invalid_hl.empty:
                result["is_valid"] = False
                result["errors"].append("High price lower than low price")

            # Check for extreme price movements
            price_change = df['close'].pct_change()
            extreme_moves = price_change[abs(price_change) > 0.2]
            if not extreme_moves.empty:
                result["warnings"].append("Extreme price movements detected")

            return result

        except Exception as e:
            logger.error(f"Error in price validation: {str(e)}")
            raise ValidationError(f"Price validation error: {str(e)}")

class VolumeValidationRule(ValidationRule):
    """Validation rules for volume data."""
    
    def __init__(self):
        super().__init__(
            name="volume_validation",
            description="Validates trading volume data"
        )

    async def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data.get("data", []))
            result = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }

            # Check for negative volume
            if (df['volume'] < 0).any():
                result["is_valid"] = False
                result["errors"].append("Negative volume values found")

            # Check for zero volume
            zero_volume = (df['volume'] == 0).sum()
            if zero_volume > 0:
                result["warnings"].append(f"{zero_volume} periods with zero volume")

            # Check for volume spikes
            mean_volume = df['volume'].mean()
            std_volume = df['volume'].std()
            volume_spikes = df[df['volume'] > mean_volume + 3 * std_volume]
            if not volume_spikes.empty:
                result["warnings"].append("Unusual volume spikes detected")

            return result

        except Exception as e:
            logger.error(f"Error in volume validation: {str(e)}")
            raise ValidationError(f"Volume validation error: {str(e)}")

class TimestampValidationRule(ValidationRule):
    """Validation rules for timestamp data."""
    
    def __init__(self):
        super().__init__(
            name="timestamp_validation",
            description="Validates timestamp consistency and sequence"
        )

    async def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data.get("data", []))
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            result = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }

            # Check for duplicates
            duplicates = df[df['timestamp'].duplicated()]
            if not duplicates.empty:
                result["is_valid"] = False
                result["errors"].append("Duplicate timestamps found")

            # Check for gaps
            time_diff = df['timestamp'].diff()
            expected_diff = pd.Timedelta('1d')  # Assuming daily data
            gaps = time_diff[time_diff > expected_diff * 1.5]
            if not gaps.empty:
                result["warnings"].append(f"Found {len(gaps)} gaps in data")

            # Check for future dates
            future_dates = df[df['timestamp'] > datetime.now()]
            if not future_dates.empty:
                result["is_valid"] = False
                result["errors"].append("Future dates found in data")

            return result

        except Exception as e:
            logger.error(f"Error in timestamp validation: {str(e)}")
            raise ValidationError(f"Timestamp validation error: {str(e)}")

class DataConsistencyValidationRule(ValidationRule):
    """Validation rules for overall data consistency."""
    
    def __init__(self):
        super().__init__(
            name="data_consistency",
            description="Validates overall data consistency and completeness"
        )

    async def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data.get("data", []))
            result = {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }

            # Check required fields
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                result["is_valid"] = False
                result["errors"].append(f"Missing required fields: {missing_fields}")

            # Check for null values
            null_counts = df[required_fields].isnull().sum()
            fields_with_nulls = null_counts[null_counts > 0]
            if not fields_with_nulls.empty:
                result["is_valid"] = False
                for field, count in fields_with_nulls.items():
                    result["errors"].append(f"{count} null values in {field}")

            # Check data types
            expected_types = {
                'timestamp': ['datetime64[ns]'],
                'open': ['float64', 'float32', 'int64'],
                'high': ['float64', 'float32', 'int64'],
                'low': ['float64', 'float32', 'int64'],
                'close': ['float64', 'float32', 'int64'],
                'volume': ['int64', 'float64']
            }
            
            for field, expected in expected_types.items():
                if df[field].dtype.name not in expected:
                    result["warnings"].append(
                        f"Unexpected type for {field}: {df[field].dtype.name}"
                    )

            return result

        except Exception as e:
            logger.error(f"Error in data consistency validation: {str(e)}")
            raise ValidationError(f"Data consistency validation error: {str(e)}")

# Registry of available validation rules
VALIDATION_RULES = {
    'price': PriceValidationRule(),
    'volume': VolumeValidationRule(),
    'timestamp': TimestampValidationRule(),
    'consistency': DataConsistencyValidationRule()
}