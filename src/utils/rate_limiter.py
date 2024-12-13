"""Redis-based rate limiter for API calls."""

import asyncio
import os
import time
from typing import Optional
from urllib.parse import urlparse

import redis
from loguru import logger


class RedisRateLimiter:
    """Redis-based rate limiter for API calls using a simple key expiration approach.
    
    This implementation uses a single key with expiration time to enforce rate limits.
    For FMP's 740 calls/minute limit, we set key expiration to 60/740 seconds.
    
    Attributes
    ----------
    redis_client : redis.Redis
        Redis client instance
    max_calls : int
        Maximum number of calls allowed per minute
    key_prefix : str
        Prefix for Redis keys
    window_size : float
        Time window in seconds for each request (60/max_calls)
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        max_calls: int = 740,
        key_prefix: str = "fmp_api:",
        redis_password: Optional[str] = None
    ):
        """Initialize Redis rate limiter.
        
        Parameters
        ----------
        redis_url : str
            Redis connection URL
        max_calls : int
            Maximum calls allowed per minute
        key_prefix : str
            Prefix for Redis keys
        redis_password : Optional[str]
            Redis password. If not provided, will try to get from environment variable
        """
        redis_password = redis_password or os.getenv("REDIS_PASSWORD")
        if not redis_password:
            raise ValueError("Redis password not provided and REDIS_PASSWORD not found in environment variables")
            
        # Parse URL and add password
        parsed = urlparse(redis_url)
        redis_url = f"redis://:{redis_password}@{parsed.hostname}:{parsed.port}"
        
        self.redis_client = redis.from_url(redis_url)
        self.max_calls = max_calls
        self.key_prefix = key_prefix
        self.window_size = 60 / max_calls  # Time window per request in seconds
        self._rate_limit_hits = 0
        
    async def acquire(self) -> bool:
        """Acquire a rate limit token.
        
        Returns
        -------
        bool
            True if token acquired, False if rate limit exceeded
        """
        key = f"{self.key_prefix}last_call"
        
        # Ensure minimum window size of 1ms to prevent potential issues
        expiry_ms = max(int(self.window_size * 1000), 1)
        
        try:
            # Try to set key with NX (only if not exists) using pexpire for millisecond precision
            if self.redis_client.set(key, '1', px=expiry_ms, nx=True):
                return True
                
            self._rate_limit_hits += 1
            if self._rate_limit_hits == 1:  # Only log on first hit
                logger.warning(f"Rate limit exceeded: waiting {self.window_size:.3f} seconds between requests")
            return False
            
        except redis.RedisError as e:
            logger.error(f"Redis error in rate limiter: {e}")
            # On Redis errors, allow the request to prevent complete service disruption
            return True
        
    async def wait_if_needed(self) -> None:
        """Wait until a rate limit token is available."""
        if not await self.acquire():
            if self._rate_limit_hits == 1:  # Only log on first hit
                logger.warning("Rate limit hit, waiting for token...")
            t0 = time.time()
            while not await self.acquire():
                await asyncio.sleep(self.window_size)
            t1 = time.time()
            logger.info(f"Rate limit cleared after {self._rate_limit_hits} hits, took {t1 - t0:.2f} seconds")
            self._rate_limit_hits = 0

    @property
    def total_rate_limit_hits(self) -> int:
        """Get total number of rate limit hits."""
        return self._rate_limit_hits 