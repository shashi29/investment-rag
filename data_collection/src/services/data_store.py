from typing import Dict, Any, List, Optional, Union
import json
import time
from datetime import datetime
import aiofiles
import asyncio
from pathlib import Path
from ..core.exceptions import DataStorageError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DataStore:
    """Service for storing and retrieving market data."""
    
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._locks: Dict[str, asyncio.Lock] = {}

    async def store_market_data(
        self,
        data: Dict[str, Any],
        symbol: str,
        provider: str
    ) -> str:
        """Store market data with metadata."""
        try:
            # Generate storage key
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            storage_key = f"{symbol}_{provider}_{timestamp}"
            
            # Add metadata
            storage_data = {
                "metadata": {
                    "symbol": symbol,
                    "provider": provider,
                    "timestamp": datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S"),
                    "storage_key": storage_key
                },
                "data": data
            }
            
            # Get or create lock for symbol
            lock = self._get_lock(symbol)
            
            async with lock:
                # Create directory structure
                symbol_dir = self.base_path / symbol / provider
                symbol_dir.mkdir(parents=True, exist_ok=True)
                
                # Store data
                file_path = symbol_dir / f"{timestamp}.json"
                async with aiofiles.open(file_path, 'w') as f:
                    await f.write(json.dumps(storage_data, indent=2))
                
                logger.info(f"Stored data for {symbol} from {provider} with key {storage_key}")
                return storage_key

        except Exception as e:
            logger.error(f"Error storing market data: {str(e)}")
            raise DataStorageError(f"Storage error: {str(e)}")

    async def retrieve_market_data(
        self,
        symbol: str,
        provider: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve market data within date range."""
        try:
            symbol_dir = self.base_path / symbol / provider
            if not symbol_dir.exists():
                return []
                
            data_files = sorted(symbol_dir.glob("*.json"))
            results = []
            
            for file_path in data_files:
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        content = await f.read()
                        data = json.loads(content)
                        
                    # Check date range if specified
                    timestamp = datetime.fromisoformat(
                        data['metadata']['timestamp'].replace('Z', '+00:00')
                    )
                    
                    if start_date and timestamp < start_date:
                        continue
                    if end_date and timestamp > end_date:
                        continue
                        
                    results.append(data)
                    
                except Exception as e:
                    logger.warning(f"Error reading file {file_path}: {str(e)}")
                    continue
                    
            return results

        except Exception as e:
            logger.error(f"Error retrieving market data: {str(e)}")
            raise DataStorageError(f"Retrieval error: {str(e)}")

    async def update_market_data(
        self,
        storage_key: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update stored market data."""
        try:
            # Parse storage key
            symbol, provider, timestamp = storage_key.split('_')
            file_path = self.base_path / symbol / provider / f"{timestamp}.json"
            
            if not file_path.exists():
                raise DataStorageError(f"Data not found for key: {storage_key}")
                
            lock = self._get_lock(symbol)
            
            async with lock:
                # Read existing data
                async with aiofiles.open(file_path, 'r') as f:
                    content = await f.read()
                    data = json.loads(content)
                
                # Apply updates
                data['data'].update(updates)
                data['metadata']['last_updated'] = datetime.now()
                
                # Write back
                async with aiofiles.open(file_path, 'w') as f:
                    await f.write(json.dumps(data, indent=2))
                    
            logger.info(f"Updated data for storage key: {storage_key}")
            return True

        except Exception as e:
            logger.error(f"Error updating market data: {str(e)}")
            raise DataStorageError(f"Update error: {str(e)}")

    async def delete_market_data(
        self,
        storage_key: str
    ) -> bool:
        """Delete stored market data."""
        try:
            # Parse storage key
            symbol, provider, timestamp = storage_key.split('_')
            file_path = self.base_path / symbol / provider / f"{timestamp}.json"
            
            if not file_path.exists():
                return False
                
            lock = self._get_lock(symbol)
            
            async with lock:
                file_path.unlink()
                logger.info(f"Deleted data for storage key: {storage_key}")
                
                # Clean up empty directories
                symbol_dir = file_path.parent
                if not any(symbol_dir.iterdir()):
                    symbol_dir.rmdir()
                    
            return True

        except Exception as e:
            logger.error(f"Error deleting market data: {str(e)}")
            raise DataStorageError(f"Deletion error: {str(e)}")

    def _get_lock(self, symbol: str) -> asyncio.Lock:
        """Get or create lock for symbol."""
        if symbol not in self._locks:
            self._locks[symbol] = asyncio.Lock()
        return self._locks[symbol]

    async def list_symbols(self) -> List[str]:
        """List all stored symbols."""
        try:
            return [d.name for d in self.base_path.iterdir() if d.is_dir()]
        except Exception as e:
            logger.error(f"Error listing symbols: {str(e)}")
            raise DataStorageError(f"List error: {str(e)}")
        
    async def list_symbols(self) -> List[str]:
        """List all stored symbols."""
        try:
            return [d.name for d in self.base_path.iterdir() if d.is_dir()]
        except Exception as e:
            logger.error(f"Error listing symbols: {str(e)}")
            raise DataStorageError(f"List error: {str(e)}")

    async def get_symbol_stats(
        self,
        symbol: str
    ) -> Dict[str, Any]:
        """Get statistics for a symbol's stored data."""
        try:
            symbol_dir = self.base_path / symbol
            if not symbol_dir.exists():
                return {}

            stats = {
                "symbol": symbol,
                "providers": {},
                "total_records": 0,
                "first_record": None,
                "last_record": None
            }

            for provider_dir in symbol_dir.iterdir():
                if provider_dir.is_dir():
                    provider_name = provider_dir.name
                    data_files = sorted(provider_dir.glob("*.json"))
                    
                    provider_stats = {
                        "record_count": len(data_files),
                        "size_bytes": sum(f.stat().st_size for f in data_files)
                    }

                    if data_files:
                        # Get first and last record timestamps
                        async with aiofiles.open(data_files[0], 'r') as f:
                            first_data = json.loads(await f.read())
                        async with aiofiles.open(data_files[-1], 'r') as f:
                            last_data = json.loads(await f.read())

                        provider_stats.update({
                            "first_record": first_data['metadata']['timestamp'],
                            "last_record": last_data['metadata']['timestamp']
                        })

                    stats["providers"][provider_name] = provider_stats
                    stats["total_records"] += provider_stats["record_count"]

            return stats

        except Exception as e:
            logger.error(f"Error getting symbol stats: {str(e)}")
            raise DataStorageError(f"Stats error: {str(e)}")

    async def cleanup_old_data(
        self,
        max_age_days: int = 30
    ) -> Dict[str, int]:
        """Clean up data older than specified age."""
        try:
            cleanup_stats = {
                "files_removed": 0,
                "bytes_freed": 0
            }

            cutoff_date = datetime.now() - timedelta(days=max_age_days)

            for symbol_dir in self.base_path.iterdir():
                if not symbol_dir.is_dir():
                    continue

                lock = self._get_lock(symbol_dir.name)
                async with lock:
                    for provider_dir in symbol_dir.iterdir():
                        if not provider_dir.is_dir():
                            continue

                        for file_path in provider_dir.glob("*.json"):
                            try:
                                async with aiofiles.open(file_path, 'r') as f:
                                    data = json.loads(await f.read())
                                timestamp = datetime.fromisoformat(
                                    data['metadata']['timestamp'].replace('Z', '+00:00')
                                )

                                if timestamp < cutoff_date:
                                    file_size = file_path.stat().st_size
                                    file_path.unlink()
                                    cleanup_stats["files_removed"] += 1
                                    cleanup_stats["bytes_freed"] += file_size

                            except Exception as e:
                                logger.warning(f"Error processing file {file_path}: {str(e)}")
                                continue

                    # Clean up empty directories
                    self._cleanup_empty_dirs(symbol_dir)

            return cleanup_stats

        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            raise DataStorageError(f"Cleanup error: {str(e)}")

    def _cleanup_empty_dirs(self, directory: Path) -> None:
        """Remove empty directories recursively."""
        for path in sorted(directory.glob('**/*'), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()

    async def verify_data_integrity(
        self,
        symbol: str,
        provider: str
    ) -> Dict[str, Any]:
        """Verify integrity of stored data."""
        try:
            integrity_report = {
                "symbol": symbol,
                "provider": provider,
                "files_checked": 0,
                "files_corrupted": 0,
                "errors": []
            }

            symbol_dir = self.base_path / symbol / provider
            if not symbol_dir.exists():
                return integrity_report

            for file_path in symbol_dir.glob("*.json"):
                integrity_report["files_checked"] += 1
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        content = await f.read()
                        json.loads(content)  # Validate JSON structure
                except Exception as e:
                    integrity_report["files_corrupted"] += 1
                    integrity_report["errors"].append({
                        "file": str(file_path),
                        "error": str(e)
                    })

            return integrity_report

        except Exception as e:
            logger.error(f"Error verifying data integrity: {str(e)}")
            raise DataStorageError(f"Verification error: {str(e)}")

    async def export_data(
        self,
        symbol: str,
        provider: str,
        format: str = "json",
        output_path: Optional[str] = None
    ) -> Union[str, Dict[str, Any]]:
        """Export stored data in specified format."""
        try:
            data = await self.retrieve_market_data(symbol, provider)
            if not data:
                raise DataStorageError(f"No data found for {symbol} from {provider}")

            if format.lower() == "json":
                if output_path:
                    output_file = Path(output_path)
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(output_file, 'w') as f:
                        await f.write(json.dumps(data, indent=2))
                    return str(output_file)
                return data

            elif format.lower() == "csv":
                # Convert to CSV format
                csv_data = []
                header = ["timestamp", "open", "high", "low", "close", "volume"]
                csv_data.append(",".join(header))
                
                for record in data:
                    time_series = record["data"]
                    for entry in time_series:
                        row = [
                            entry["timestamp"],
                            entry["open"],
                            entry["high"],
                            entry["low"],
                            entry["close"],
                            entry["volume"]
                        ]
                        csv_data.append(",".join(map(str, row)))

                if output_path:
                    output_file = Path(output_path)
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(output_file, 'w') as f:
                        await f.write("\n".join(csv_data))
                    return str(output_file)
                return "\n".join(csv_data)

            else:
                raise DataStorageError(f"Unsupported export format: {format}")

        except Exception as e:
            logger.error(f"Error exporting data: {str(e)}")
            raise DataStorageError(f"Export error: {str(e)}")