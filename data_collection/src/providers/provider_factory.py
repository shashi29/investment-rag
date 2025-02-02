from typing import List, Optional, Dict, Any
from ..core.base_classes import DataProvider
from .alpha_vantage.client import AlphaVantageClient
from .yahoo_finance.client import YahooFinanceClient
from ..core.exceptions import ConfigurationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataProviderFactory:
    """Factory for creating data provider instances."""
    
    _providers = {
        "alpha_vantage": AlphaVantageClient,
        "yahoo_finance": YahooFinanceClient
    }
    
    @classmethod
    def create_provider(
        cls,
        provider_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> DataProvider:
        """Create a data provider instance."""
        try:
            if provider_name not in cls._providers:
                raise ConfigurationError(f"Unknown provider: {provider_name}")
            
            provider_class = cls._providers[provider_name]
            config = config or {}
            
            if provider_name == "alpha_vantage":
                api_key = config.get("api_key")
                if not api_key:
                    raise ConfigurationError("API key required for Alpha Vantage")
                return provider_class(api_key=api_key)
                
            elif provider_name == "yahoo_finance":
                return provider_class()
            
            logger.info(f"Created provider instance: {provider_name}")
            return provider_class(**config)
            
        except Exception as e:
            logger.error(f"Error creating provider {provider_name}: {str(e)}")
            raise

    @classmethod
    def register_provider(
        cls,
        provider_name: str,
        provider_class: type
    ) -> None:
        """Register a new data provider."""
        if not issubclass(provider_class, DataProvider):
            raise ConfigurationError(
                f"Provider class must inherit from DataProvider"
            )
        
        cls._providers[provider_name] = provider_class
        logger.info(f"Registered new provider: {provider_name}")

    @classmethod
    def get_available_providers(cls) -> List[str]:
        """Get list of available providers."""
        return list(cls._providers.keys())

    @classmethod
    def validate_config(
        cls,
        provider_name: str,
        config: Dict[str, Any]
    ) -> bool:
        """Validate provider configuration."""
        try:
            if provider_name not in cls._providers:
                return False
                
            if provider_name == "alpha_vantage":
                return bool(config.get("api_key"))
                
            elif provider_name == "yahoo_finance":
                return True
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating config: {str(e)}")
            return False