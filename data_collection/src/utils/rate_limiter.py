import asyncio
import time
from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import deque
from ..core.exceptions import RateLimitError
from ..utils.logger import get_logger

logger = get_logger(__name__)

class RateLimiter:
    """Rate limiter implementation using token bucket algorithm."""
    
    def __init__(
        self,
        calls_per_minute: int,
        calls_per_day: Optional[int] = None,
        burst_size: Optional[int] = None
    ):
        self.calls_per_minute = calls_per_minute
        self.calls_per_day = calls_per_day
        self.burst_size = burst_size or calls_per_minute
        
        # Initialize token buckets
        self.minute_bucket = self.burst_size
        self.day_bucket = calls_per_day if calls_per_day else float('inf')
        
        # Track last refill times
        self.last_minute_refill = time.time()
        self.last_day_refill = time.time()
        
        # Track API calls
        self.minute_calls = deque()
        self.day_calls = deque()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire permission to make an API call."""
        async with self._lock:
            await self._refill()
            
            if self.minute_bucket <= 0:
                wait_time = self._get_minute_wait_time()
                logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                raise RateLimitError(
                    "Rate limit exceeded",
                    retry_after=int(wait_time)
                )
                
            if self.day_bucket <= 0:
                wait_time = self._get_day_wait_time()
                logger.warning(f"Daily rate limit reached. Waiting {wait_time:.2f} seconds")
                raise RateLimitError(
                    "Daily rate limit exceeded",
                    retry_after=int(wait_time)
                )
                
            # Consume tokens
            self.minute_bucket -= 1
            if self.calls_per_day:
                self.day_bucket -= 1
                
            # Record call time
            current_time = time.time()
            self.minute_calls.append(current_time)
            if self.calls_per_day:
                self.day_calls.append(current_time)

    async def _refill(self) -> None:
        """Refill token buckets."""
        current_time = time.time()
        
        # Refill minute bucket
        elapsed_minutes = (current_time - self.last_minute_refill) / 60
        new_tokens = int(elapsed_minutes * self.calls_per_minute)
        if new_tokens > 0:
            self.minute_bucket = min(
                self.burst_size,
                self.minute_bucket + new_tokens
            )
            self.last_minute_refill = current_time
            
        # Refill day bucket
        if self.calls_per_day:
            elapsed_days = (current_time - self.last_day_refill) / (24 * 3600)
            if elapsed_days >= 1:
                self.day_bucket = self.calls_per_day
                self.last_day_refill = current_time
                
        # Clean up old calls
        self._cleanup_calls()

    def _cleanup_calls(self) -> None:
        """Remove expired calls from tracking."""
        current_time = time.time()
        minute_ago = current_time - 60
        day_ago = current_time - (24 * 3600)
        
        # Clean minute calls
        while self.minute_calls and self.minute_calls[0] < minute_ago:
            self.minute_calls.popleft()
            
        # Clean day calls
        if self.calls_per_day:
            while self.day_calls and self.day_calls[0] < day_ago:
                self.day_calls.popleft()

    def _get_minute_wait_time(self) -> float:
        """Get wait time for minute bucket refill."""
        if not self.minute_calls:
            return 0
        return max(0, 60 - (time.time() - self.minute_calls[0]))

    def _get_day_wait_time(self) -> float:
        """Get wait time for day bucket refill."""
        if not self.calls_per_day or not self.day_calls:
            return 0
        return max(0, (24 * 3600) - (time.time() - self.day_calls[0]))

    def get_remaining_calls(self) -> Dict[str, int]:
        """Get remaining API calls."""
        return {
            "minute": self.minute_bucket,
            "day": self.day_bucket if self.calls_per_day else float('inf')
        }

    async def wait_if_needed(self) -> None:
        """Wait if rate limit is reached."""
        try:
            await self.acquire()
        except RateLimitError as e:
            await asyncio.sleep(e.retry_after)
            await self.acquire()