import asyncio
from src.providers.provider_factory import DataProviderFactory
from src.services.data_collector import DataCollector
from src.services.data_processor import DataProcessor
from src.services.data_validator import DataValidator
from src.services.data_store import DataStore
from src.utils.cache_manager import CacheManager
from src.utils.error_handler import ErrorHandler
from src.utils.logger import get_logger
Logger = get_logger(__name__)


async def main():
    # Initialize components
    cache_manager = CacheManager()
    error_handler = ErrorHandler()
    
    # Create providers
    providers = {
        'alpha_vantage': DataProviderFactory.create_provider(
            'alpha_vantage',
            {'api_key': 'ELH4JHL4BBJ5GK94'}
        ),
        'yahoo_finance': DataProviderFactory.create_provider('yahoo_finance')
    }
    
    # Initialize services
    collector = DataCollector(providers, cache_manager, error_handler)
    processor = DataProcessor()
    validator = DataValidator()
    store = DataStore()
    
    # Example usage
    symbol = "AAPL"
    start_date = "2024-12-01"
    end_date = "2025-01-31"
    
    Logger.info(f"Collecting data for {symbol} from Yahoo Finance")
    try:
        # Collect data
        data = await collector.collect_data(
            symbol=symbol,
            provider_name='yahoo_finance',
            start_date=start_date,
            end_date=end_date
        )
        
        # Process data
        # processed_data = processor.process_market_data(
        #     data,
        #     indicators=['sma', 'ema', 'rsi']
        # )
        
        # Validate data
        validation_result = await validator.validate_market_data(data)
        
        # Store data if valid
        if validation_result['is_valid']:
            await store.store_market_data(
                data,
                symbol,
                'yahoo_finance'
            )
            
        print(f"Data collection complete for {symbol}")
        print(f"Validation result: {validation_result['is_valid']}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())