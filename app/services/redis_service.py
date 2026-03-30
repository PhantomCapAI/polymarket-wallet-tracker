import json
import logging
from typing import Any, Optional

import redis.asyncio as redis

from config.settings import settings

logger = logging.getLogger(__name__)


class RedisService:
    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self._connected: bool = False

    async def initialize(self):
        """Create async Redis connection from settings"""
        try:
            self.client = redis.from_url(
                settings.REDIS_URL,
                password=settings.REDIS_PASSWORD,
                db=settings.REDIS_DB,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            await self.client.ping()
            self._connected = True
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis unavailable, running without cache: {e}")
            self.client = None
            self._connected = False

    async def get(self, key: str) -> Optional[str]:
        """Get a cached value by key"""
        if not self._connected:
            return None
        try:
            return await self.client.get(key)
        except Exception as e:
            logger.warning(f"Redis GET failed for {key}: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a value with optional TTL in seconds"""
        if not self._connected:
            return False
        try:
            serialized = value if isinstance(value, str) else json.dumps(value)
            if ttl:
                await self.client.setex(key, ttl, serialized)
            else:
                await self.client.set(key, serialized)
            return True
        except Exception as e:
            logger.warning(f"Redis SET failed for {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete a key from cache"""
        if not self._connected:
            return False
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Redis DELETE failed for {key}: {e}")
            return False

    async def get_wallet_score(self, wallet: str) -> Optional[float]:
        """Get cached wallet signal score"""
        raw = await self.get(f"wallet_score:{wallet}")
        if raw is None:
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None

    async def set_wallet_score(self, wallet: str, score: float, ttl: int = 900) -> bool:
        """Cache a wallet signal score (default 15 min TTL)"""
        return await self.set(f"wallet_score:{wallet}", str(score), ttl=ttl)

    async def ping(self) -> bool:
        """Health check for Redis connection"""
        if not self._connected:
            return False
        try:
            return await self.client.ping()
        except Exception:
            self._connected = False
            return False

    async def close(self):
        """Close Redis connection and cleanup"""
        if self.client:
            try:
                await self.client.aclose()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
            finally:
                self.client = None
                self._connected = False


redis_service = RedisService()
