from typing import Dict, Any, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
from ..core.exceptions import ValidationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataSanitizer:
    """Sanitize and clean market data."""
    
    def __init__(self):
        self.sanitizers = {
            'remove_duplicates': self._remove_duplicates,
            'fill_missing_values': self._fill_missing_values,
            'handle_outliers': self._handle_outliers,
            'normalize_timestamps': self._normalize_timestamps,
            'adjust_prices': self._adjust_prices,
            'validate_ohlc': self._validate_ohlc
        }

    async def sanitize(
        self,
        data: Dict[str, Any],
        steps: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Sanitize data using specified steps."""
        try:
            # Convert to DataFrame for processing
            df = pd.DataFrame(data.get('data', []))
            if df.empty:
                raise ValidationError("Empty dataset provided")

            # Track changes made during sanitization
            changes = {
                'timestamp': datetime.now(),
                'steps_applied': [],
                'modifications': {}
            }

            # Apply sanitization steps
            steps_to_apply = steps if steps else self.sanitizers.keys()
            
            for step in steps_to_apply:
                if step in self.sanitizers:
                    df, step_changes = await self.sanitizers[step](df)
                    changes['steps_applied'].append(step)
                    changes['modifications'][step] = step_changes

            # Convert back to dictionary format
            sanitized_data = {
                'data': df.to_dict('records'),
                'metadata': {
                    'sanitization': changes,
                    'original_length': len(data.get('data', [])),
                    'sanitized_length': len(df)
                }
            }

            return sanitized_data

        except Exception as e:
            logger.error(f"Error during data sanitization: {str(e)}")
            raise ValidationError(f"Sanitization error: {str(e)}")

    async def _remove_duplicates(
        self,
        df: pd.DataFrame
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Remove duplicate entries."""
        try:
            original_length = len(df)
            df = df.drop_duplicates(subset=['timestamp'])
            
            changes = {
                'rows_removed': original_length - len(df),
                'duplicate_timestamps_removed': True
            }
            
            return df, changes

        except Exception as e:
            logger.error(f"Error removing duplicates: {str(e)}")
            raise ValidationError(f"Duplicate removal error: {str(e)}")

    async def _fill_missing_values(
        self,
        df: pd.DataFrame
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Fill missing values in the dataset."""
        try:
            changes = {
                'filled_values': {}
            }

            # Handle missing OHLC values
            for col in ['open', 'high', 'low', 'close']:
                missing_count = df[col].isnull().sum()
                if missing_count > 0:
                    df[col] = df[col].fillna(method='ffill')
                    changes['filled_values'][col] = missing_count

            # Handle missing volume
            volume_missing = df['volume'].isnull().sum()
            if volume_missing > 0:
                df['volume'] = df['volume'].fillna(0)
                changes['filled_values']['volume'] = volume_missing

            return df, changes

        except Exception as e:
            logger.error(f"Error filling missing values: {str(e)}")
            raise ValidationError(f"Missing value fill error: {str(e)}")

    async def _handle_outliers(
        self,
        df: pd.DataFrame,
        std_dev: float = 3.0
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Handle outliers in the dataset."""
        try:
            changes = {
                'outliers_detected': {},
                'outliers_handled': {}
            }

            # Handle price outliers
            for col in ['open', 'high', 'low', 'close']:
                mean = df[col].mean()
                std = df[col].std()
                outliers = df[
                    (df[col] > mean + std_dev * std) |
                    (df[col] < mean - std_dev * std)
                ]
                
                changes['outliers_detected'][col] = len(outliers)
                
                if not outliers.empty:
                    df.loc[outliers.index, col] = df[col].clip(
                        lower=mean - std_dev * std,
                        upper=mean + std_dev * std
                    )
                    changes['outliers_handled'][col] = len(outliers)

            # Handle volume outliers
            volume_mean = df['volume'].mean()
            volume_std = df['volume'].std()
            volume_outliers = df[df['volume'] > volume_mean + std_dev * volume_std]
            
            changes['outliers_detected']['volume'] = len(volume_outliers)
            
            if not volume_outliers.empty:
                df.loc[volume_outliers.index, 'volume'] = volume_mean
                changes['outliers_handled']['volume'] = len(volume_outliers)

            return df, changes

        except Exception as e:
            logger.error(f"Error handling outliers: {str(e)}")
            raise ValidationError(f"Outlier handling error: {str(e)}")

    async def _normalize_timestamps(
        self,
        df: pd.DataFrame
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Normalize timestamps in the dataset."""
        try:
            changes = {
                'timestamp_modifications': 0,
                'gaps_filled': 0
            }

            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                changes['timestamp_modifications'] += len(df)

            # Sort by timestamp
            df = df.sort_values('timestamp')

            # Reindex to fill gaps
            min_date = df['timestamp'].min()
            max_date = df['timestamp'].max()
            ideal_index = pd.date_range(start=min_date, end=max_date, freq='D')
            
            if len(ideal_index) > len(df):
                df = df.set_index('timestamp').reindex(ideal_index).reset_index()
                changes['gaps_filled'] = len(ideal_index) - len(df)

            return df, changes

        except Exception as e:
            logger.error(f"Error normalizing timestamps: {str(e)}")
            raise ValidationError(f"Timestamp normalization error: {str(e)}")

    async def _adjust_prices(
        self,
        df: pd.DataFrame
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Adjust prices for consistency."""
        try:
            changes = {
                'price_adjustments': 0,
                'adjusted_columns': []
            }

            # Ensure high >= low
            invalid_hl = df['high'] < df['low']
            if invalid_hl.any():
                temp = df.loc[invalid_hl, 'high'].copy()
                df.loc[invalid_hl, 'high'] = df.loc[invalid_hl, 'low']
                df.loc[invalid_hl, 'low'] = temp
                changes['price_adjustments'] += len(invalid_hl)
                changes['adjusted_columns'].extend(['high', 'low'])

            # Ensure open and close are within high-low range
            for col in ['open', 'close']:
                above_high = df[col] > df['high']
                below_low = df[col] < df['low']
                
                if above_high.any() or below_low.any():
                    df.loc[above_high, col] = df.loc[above_high, 'high']
                    df.loc[below_low, col] = df.loc[below_low, 'low']
                    changes['price_adjustments'] += len(above_high) + len(below_low)
                    changes['adjusted_columns'].append(col)

            return df, changes

        except Exception as e:
            logger.error(f"Error adjusting prices: {str(e)}")
            raise ValidationError(f"Price adjustment error: {str(e)}")

    async def _validate_ohlc(
        self,
        df: pd.DataFrame
    ) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Validate OHLC relationships."""
        try:
            changes = {
                'validations_performed': 0,
                'corrections_made': 0
            }

            # Check for negative values
            negative_mask = df[['open', 'high', 'low', 'close']] < 0
            if negative_mask.any().any():
                df[['open', 'high', 'low', 'close']] = df[
                    ['open', 'high', 'low', 'close']
                ].abs()
                changes['corrections_made'] += negative_mask.sum().sum()

            # Ensure price relationships
            corrections = (
                (df['high'] < df['low']) |
                (df['open'] > df['high']) |
                (df['open'] < df['low']) |
                (df['close'] > df['high']) |
                (df['close'] < df['low'])
            )

            if corrections.any():
                affected_rows = df[corrections]
                for idx in affected_rows.index:
                    prices = [
                        df.loc[idx, 'open'],
                        df.loc[idx, 'high'],
                        df.loc[idx, 'low'],
                        df.loc[idx, 'close']
                    ]
                    df.loc[idx, 'high'] = max(prices)
                    df.loc[idx, 'low'] = min(prices)
                    changes['corrections_made'] += 1

            changes['validations_performed'] = len(df)
            return df, changes

        except Exception as e:
            logger.error(f"Error validating OHLC: {str(e)}")
            raise ValidationError(f"OHLC validation error: {str(e)}")

    def add_sanitizer(
        self,
        name: str,
        sanitizer_func: callable
    ) -> None:
        """Add a custom sanitizer function."""
        if name in self.sanitizers:
            raise ValidationError(f"Sanitizer {name} already exists")
            
        self.sanitizers[name] = sanitizer_func
        logger.info(f"Added new sanitizer: {name}")

    def get_available_sanitizers(self) -> List[str]:
        """Get list of available sanitizers."""
        return list(self.sanitizers.keys())