from typing import Any, Optional, Dict
import json
import time
from pathlib import Path
import asyncio
from cachetools import TTLCache, LRUCache
from ..core.exceptions import CacheError
from .logger import get_logger
from datetime import datetime

logger = get_logger(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    
class CacheManager:
    """Manage caching of data with multiple cache levels."""
    
    def __init__(
        self,
        memory_cache_size: int = 1000,
        memory_ttl: int = 300,  # 5 minutes
        disk_cache_dir: Optional[str] = "cache",
        disk_cache_size: int = 10000
    ):
        # Memory cache with TTL
        self.memory_cache = TTLCache(
            maxsize=memory_cache_size,
            ttl=memory_ttl
        )
        
        # Disk cache configuration
        self.disk_cache_dir = Path(disk_cache_dir) if disk_cache_dir else None
        if self.disk_cache_dir:
            self.disk_cache_dir.mkdir(parents=True, exist_ok=True)
            
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Cache statistics
        self.stats = {
            "memory_hits": 0,
            "memory_misses": 0,
            "disk_hits": 0,
            "disk_misses": 0
        }

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        try:
            # Try memory cache first
            value = self.memory_cache.get(key)
            if value is not None:
                self.stats["memory_hits"] += 1
                return value
                
            self.stats["memory_misses"] += 1
            
            # Try disk cache if configured
            if self.disk_cache_dir:
                disk_value = await self._read_from_disk(key)
                if disk_value is not None:
                    self.stats["disk_hits"] += 1
                    # Update memory cache
                    self.memory_cache[key] = disk_value
                    return disk_value
                    
                self.stats["disk_misses"] += 1
                
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving from cache: {str(e)}")
            raise CacheError(f"Cache retrieval error: {str(e)}", "get")

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> None:
        """Set value in cache."""
        try:
            async with self._lock:
                # Set in memory cache
                self.memory_cache[key] = value
                
                # Set in disk cache if configured
                if self.disk_cache_dir:
                    await self._write_to_disk(key, value, ttl)
                    
        except Exception as e:
            logger.error(f"Error setting cache: {str(e)}")
            raise CacheError(f"Cache set error: {str(e)}", "set")

    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        try:
            async with self._lock:
                # Delete from memory cache
                self.memory_cache.pop(key, None)
                
                # Delete from disk cache if configured
                if self.disk_cache_dir:
                    await self._delete_from_disk(key)
                    
        except Exception as e:
            logger.error(f"Error deleting from cache: {str(e)}")
            raise CacheError(f"Cache deletion error: {str(e)}", "delete")

    async def clear(self) -> None:
        """Clear all cache entries."""
        try:
            async with self._lock:
                # Clear memory cache
                self.memory_cache.clear()
                
                # Clear disk cache if configured
                if self.disk_cache_dir:
                    for cache_file in self.disk_cache_dir.glob("*.cache"):
                        cache_file.unlink()
                        
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
            raise CacheError(f"Cache clear error: {str(e)}", "clear")

    async def _write_to_disk(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> None:
        """Write value to disk cache."""
        if not self.disk_cache_dir:
            return
            
        try:
            cache_data = {
                "value": value,
                "timestamp": datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S"),
                "ttl": ttl
            }
            
            cache_file = self.disk_cache_dir / f"{key}.cache"
            cache_file.write_text(json.dumps(cache_data, cls=CustomJSONEncoder))
            
        except Exception as e:
            logger.error(f"Error writing to disk cache: {str(e)}")
            raise CacheError(f"Disk cache write error: {str(e)}", "write")

    async def _read_from_disk(self, key: str) -> Optional[Any]:
        """Read value from disk cache."""
        if not self.disk_cache_dir:
            return None
            
        try:
            cache_file = self.disk_cache_dir / f"{key}.cache"
            if not cache_file.exists():
                return None
                
            cache_data = json.loads(cache_file.read_text())
            
            # Check TTL if specified
            if cache_data.get("ttl"):
                age = time.time() - cache_data["timestamp"]
                if age > cache_data["ttl"]:
                    await self._delete_from_disk(key)
                    return None
                    
            return cache_data["value"]
            
        except Exception as e:
            logger.error(f"Error reading from disk cache: {str(e)}")
            return None

    async def _delete_from_disk(self, key: str) -> None:
        """Delete value from disk cache."""
        if not self.disk_cache_dir:
            return
            
        try:
            cache_file = self.disk_cache_dir / f"{key}.cache"
            if cache_file.exists():
                cache_file.unlink()
                
        except Exception as e:
            logger.error(f"Error deleting from disk cache: {str(e)}")
            raise CacheError(f"Disk cache delete error: {str(e)}", "delete")

    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            **self.stats,
            "memory_cache_size": len(self.memory_cache),
            "memory_cache_maxsize": self.memory_cache.maxsize
        }

    async def cleanup_expired(self) -> None:
        """Clean up expired cache entries."""
        try:
            # Memory cache auto-cleans with TTLCache
            
            # Clean disk cache
            if self.disk_cache_dir:
                for cache_file in self.disk_cache_dir.glob("*.cache"):
                    try:
                        cache_data = json.loads(cache_file.read_text())
                        if cache_data.get("ttl"):
                            age = time.time() - cache_data["timestamp"]
                            if age > cache_data["ttl"]:
                                cache_file.unlink()
                    except Exception as e:
                        logger.warning(f"Error cleaning up cache file {cache_file}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Error during cache cleanup: {str(e)}")
            raise CacheError(f"Cache cleanup error: {str(e)}", "cleanup")