from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from .validation_rules import VALIDATION_RULES
from ..core.exceptions import ValidationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class MarketDataValidator:
    """Validator for market data using configured validation rules."""
    
    def __init__(self, custom_rules: Optional[Dict] = None):
        self.rules = {**VALIDATION_RULES}
        if custom_rules:
            self.rules.update(custom_rules)

    async def validate(
        self,
        data: Dict[str, Any],
        rules: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Validate market data using specified rules."""
        try:
            validation_results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "rule_results": {},
                "timestamp": datetime.now()
            }

            # Determine which rules to apply
            rules_to_apply = rules if rules else self.rules.keys()
            
            # Apply each validation rule
            for rule_name in rules_to_apply:
                if rule_name not in self.rules:
                    logger.warning(f"Unknown validation rule: {rule_name}")
                    continue

                rule = self.rules[rule_name]
                result = await rule.validate(data)
                
                validation_results["rule_results"][rule_name] = result
                
                if not result["is_valid"]:
                    validation_results["is_valid"] = False
                
                validation_results["errors"].extend(result.get("errors", []))
                validation_results["warnings"].extend(result.get("warnings", []))

            return validation_results

        except Exception as e:
            logger.error(f"Error during validation: {str(e)}")
            raise ValidationError(f"Validation error: {str(e)}")

    async def validate_batch(
        self,
        data_list: List[Dict[str, Any]],
        rules: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Validate multiple datasets concurrently."""
        try:
            tasks = [
                self.validate(data, rules)
                for data in data_list
            ]
            return await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"Error during batch validation: {str(e)}")
            raise ValidationError(f"Batch validation error: {str(e)}")

    def add_rule(self, rule_name: str, rule: Any) -> None:
        """Add a new validation rule."""
        if rule_name in self.rules:
            raise ValidationError(f"Validation rule {rule_name} already exists")
            
        self.rules[rule_name] = rule
        logger.info(f"Added validation rule: {rule_name}")

    def remove_rule(self, rule_name: str) -> None:
        """Remove a validation rule."""
        if rule_name not in self.rules:
            raise ValidationError(f"Validation rule {rule_name} not found")
            
        del self.rules[rule_name]
        logger.info(f"Removed validation rule: {rule_name}")

    def get_available_rules(self) -> List[str]:
        """Get list of available validation rules."""
        return list(self.rules.keys())

class TimeSeriesValidator:
    """Specialized validator for time series data."""
    
    def __init__(self, market_validator: MarketDataValidator):
        self.market_validator = market_validator

    async def validate_time_series(
        self,
        data: Dict[str, Any],
        frequency: str = "1d"
    ) -> Dict[str, Any]:
        """Validate time series data with frequency-specific checks."""
        try:
            # First apply standard market data validation
            base_validation = await self.market_validator.validate(data)
            
            # Add time series specific validation
            ts_validation = await self._validate_time_series_specific(data, frequency)
            
            # Combine results
            combined_results = {
                "is_valid": base_validation["is_valid"] and ts_validation["is_valid"],
                "errors": base_validation["errors"] + ts_validation["errors"],
                "warnings": base_validation["warnings"] + ts_validation["warnings"],
                "rule_results": {
                    **base_validation["rule_results"],
                    "time_series_specific": ts_validation
                },
                "timestamp": datetime.now()
            }

            return combined_results

        except Exception as e:
            logger.error(f"Error during time series validation: {str(e)}")
            raise ValidationError(f"Time series validation error: {str(e)}")

    async def _validate_time_series_specific(
        self,
        data: Dict[str, Any],
        frequency: str
    ) -> Dict[str, Any]:
        """Perform time series specific validation."""
        result = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }

        try:
            # Validate frequency consistency
            freq_validation = await self._validate_frequency(data, frequency)
            if not freq_validation["is_valid"]:
                result["is_valid"] = False
                result["errors"].extend(freq_validation["errors"])

            # Validate continuity
            continuity_validation = await self._validate_continuity(data)
            if not continuity_validation["is_valid"]:
                result["warnings"].extend(continuity_validation["warnings"])

            # Add more time series specific validations as needed

            return result

        except Exception as e:
            logger.error(f"Error in time series specific validation: {str(e)}")
            raise ValidationError(f"Time series specific validation error: {str(e)}")

    async def _validate_frequency(
        self,
        data: Dict[str, Any],
        expected_frequency: str
    ) -> Dict[str, Any]:
        """Validate time series frequency."""
        # Implementation for frequency validation
        pass

    async def _validate_continuity(
        self,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate time series continuity."""
        # Implementation for continuity validation
        pass