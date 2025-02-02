from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio
from ..core.base_classes import DataProvider
from ..core.exceptions import DataCollectionError
from ..utils.logger import get_logger
from ..utils.cache_manager import CacheManager
from ..utils.error_handler import ErrorHandler

logger = get_logger(__name__)

class DataCollector:
    """Service for collecting data from different providers."""
    
    def __init__(
        self,
        providers: Dict[str, DataProvider],
        cache_manager: CacheManager,
        error_handler: ErrorHandler
    ):
        self.providers = providers
        self.cache_manager = cache_manager
        self.error_handler = error_handler
        self.collection_tasks: Dict[str, asyncio.Task] = {}

    async def collect_data(
        self,
        symbol: str,
        provider_name: str,
        start_date: datetime,
        end_date: datetime,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """Collect data for a symbol from specified provider."""
        try:
            # Generate cache key
            cache_key = f"{provider_name}_{symbol}_{start_date}_{end_date}"
            
            # Check cache if enabled
            if use_cache:
                cached_data = await self.cache_manager.get(cache_key)
                if cached_data:
                    logger.info(f"Cache hit for {cache_key}")
                    return cached_data

            # Get provider instance
            provider = self.providers.get(provider_name)
            if not provider:
                raise DataCollectionError(f"Unknown provider: {provider_name}")

            # Fetch data
            data = await provider.fetch_data(symbol, start_date, end_date)
            
            # Validate response
            if not await provider.validate_response(data):
                raise DataCollectionError("Invalid response from provider")

            # Transform data
            transformed_data = await provider.transform_data(data)
            
            # Cache the result if enabled
            if use_cache:
                await self.cache_manager.set(cache_key, transformed_data)

            return transformed_data

        except Exception as e:
            context = {
                "symbol": symbol,
                "provider": provider_name,
                "start_date": start_date,
                "end_date": end_date
            }
            await self.error_handler.handle_error(e, context)
            raise

    async def collect_batch_data(
        self,
        symbols: List[str],
        provider_name: str,
        start_date: datetime,
        end_date: datetime,
        max_concurrent: int = 5
    ) -> Dict[str, Any]:
        """Collect data for multiple symbols concurrently."""
        try:
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def collect_with_semaphore(symbol: str) -> Dict[str, Any]:
                async with semaphore:
                    return await self.collect_data(
                        symbol,
                        provider_name,
                        start_date,
                        end_date
                    )

            # Create tasks for each symbol
            tasks = [
                asyncio.create_task(collect_with_semaphore(symbol))
                for symbol in symbols
            ]

            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            processed_results = {}
            for symbol, result in zip(symbols, results):
                if isinstance(result, Exception):
                    processed_results[symbol] = {
                        "success": False,
                        "error": str(result)
                    }
                else:
                    processed_results[symbol] = {
                        "success": True,
                        "data": result
                    }

            return processed_results

        except Exception as e:
            context = {
                "symbols": symbols,
                "provider": provider_name,
                "start_date": start_date,
                "end_date": end_date
            }
            await self.error_handler.handle_error(e, context)
            raise

    async def start_real_time_collection(
        self,
        symbol: str,
        provider_name: str,
        interval: int = 60  # seconds
    ) -> None:
        """Start real-time data collection for a symbol."""
        try:
            task_key = f"{provider_name}_{symbol}"
            
            if task_key in self.collection_tasks:
                raise DataCollectionError(f"Collection already running for {task_key}")

            async def collection_loop():
                while True:
                    try:
                        provider = self.providers.get(provider_name)
                        if not provider:
                            raise DataCollectionError(f"Unknown provider: {provider_name}")

                        data = await provider.get_real_time_data(symbol)
                        
                        # Cache with short TTL for real-time data
                        cache_key = f"realtime_{task_key}_{datetime.now().timestamp()}"
                        await self.cache_manager.set(cache_key, data, ttl=interval * 2)
                        
                        await asyncio.sleep(interval)
                        
                    except Exception as e:
                        logger.error(f"Error in real-time collection: {str(e)}")
                        await asyncio.sleep(interval)

            # Start collection task
            self.collection_tasks[task_key] = asyncio.create_task(collection_loop())
            logger.info(f"Started real-time collection for {task_key}")

        except Exception as e:
            context = {
                "symbol": symbol,
                "provider": provider_name,
                "interval": interval
            }
            await self.error_handler.handle_error(e, context)
            raise

    async def stop_real_time_collection(
        self,
        symbol: str,
        provider_name: str
    ) -> None:
        """Stop real-time data collection for a symbol."""
        try:
            task_key = f"{provider_name}_{symbol}"
            
            if task_key in self.collection_tasks:
                self.collection_tasks[task_key].cancel()
                del self.collection_tasks[task_key]
                logger.info(f"Stopped real-time collection for {task_key}")
            else:
                logger.warning(f"No collection running for {task_key}")

        except Exception as e:
            context = {
                "symbol": symbol,
                "provider": provider_name
            }
            await self.error_handler.handle_error(e, context)
            raise

    def get_active_collections(self) -> List[str]:
        """Get list of active real-time collections."""
        return list(self.collection_tasks.keys())

    async def cleanup(self) -> None:
        """Clean up all active collections and resources."""
        try:
            # Cancel all active collection tasks
            for task_key, task in self.collection_tasks.items():
                task.cancel()
                logger.info(f"Cancelled collection for {task_key}")
                
            self.collection_tasks.clear()
            
            # Clear caches
            await self.cache_manager.clear()
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            raise