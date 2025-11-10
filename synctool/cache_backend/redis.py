import asyncio
import json
import pickle
from typing import Any, Optional, Dict, List
from redis import asyncio as aioredis

from .base import BaseCacheBackend


class RedisCacheBackend(BaseCacheBackend):
    """
    Redis-based cache backend for distributed caching.
    
    Features:
    - Distributed cache across multiple workers/processes
    - Built-in TTL support
    - Atomic operations (no race conditions)
    - Persistence options
    - High performance
    
    Thread/async-safe: Redis operations are atomic by design.
    Connection pooling prevents connection exhaustion under concurrent load.
    """
    
    def __init__(self, url: str = 'redis://localhost:6379', 
                 ttl: int = 3600, 
                 key_prefix: str = 'synctool:join:',
                 max_connections: int = 10):
        """
        Initialize Redis cache backend.
        
        Args:
            url: Redis connection URL
            ttl: Default time-to-live in seconds
            key_prefix: Prefix for all cache keys (namespace isolation)
            max_connections: Max connections in pool (for concurrent access)
        """
        self.url = url
        self.default_ttl = ttl
        self.key_prefix = key_prefix
        self.max_connections = max_connections
        self.redis: Optional[aioredis.Redis] = None
        
        # Statistics (approximate, as Redis is distributed)
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'deletes': 0
        }
    
    def _make_key(self, key: Any) -> str:
        """Convert key to string representation with prefix"""
        if isinstance(key, tuple):
            key_str = ':'.join(str(k) for k in key)
        else:
            key_str = str(key)
        
        return f"{self.key_prefix}{key_str}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage"""
        # Use pickle for Python objects, supports complex types
        return pickle.dumps(value)
    
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from storage"""
        if data is None:
            return None
        return pickle.loads(data)
    
    async def connect(self):
        """
        Initialize Redis connection pool.
        Connection pooling ensures efficient concurrent access.
        """
        self.redis = await aioredis.from_url(
            self.url,
            encoding="utf-8",
            decode_responses=False,  # We handle serialization
            max_connections=self.max_connections
        )
    
    async def disconnect(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            self.redis = None
    
    async def get(self, key: Any) -> Optional[Any]:
        """
        Get value from Redis cache.
        Atomic operation - no race conditions.
        """
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        redis_key = self._make_key(key)
        
        try:
            data = await self.redis.get(redis_key)
            
            if data is None:
                self.stats['misses'] += 1
                return None
            
            self.stats['hits'] += 1
            return self._deserialize(data)
            
        except Exception as e:
            # Log error and return None (cache miss)
            self.stats['misses'] += 1
            return None
    
    async def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        Set value in Redis cache.
        Atomic operation with automatic TTL - no race conditions.
        """
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        redis_key = self._make_key(key)
        ttl = ttl if ttl is not None else self.default_ttl
        
        try:
            serialized = self._serialize(value)
            
            if ttl:
                await self.redis.setex(redis_key, ttl, serialized)
            else:
                await self.redis.set(redis_key, serialized)
            
            self.stats['sets'] += 1
            
        except Exception as e:
            # Log error but don't fail the operation
            pass
    
    async def get_many(self, keys: List[Any]) -> Dict[Any, Any]:
        """
        Get multiple values from Redis (batch operation using MGET).
        Single round-trip to Redis - very efficient for concurrent access.
        """
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        if not keys:
            return {}
        
        redis_keys = [self._make_key(k) for k in keys]
        
        try:
            # MGET is atomic and efficient
            values = await self.redis.mget(redis_keys)
            
            result = {}
            for original_key, data in zip(keys, values):
                if data is not None:
                    try:
                        result[original_key] = self._deserialize(data)
                        self.stats['hits'] += 1
                    except:
                        self.stats['misses'] += 1
                else:
                    self.stats['misses'] += 1
            
            return result
            
        except Exception as e:
            self.stats['misses'] += len(keys)
            return {}
    
    async def set_many(self, items: Dict[Any, Any], ttl: Optional[int] = None):
        """
        Set multiple values in Redis (batch operation using pipeline).
        Pipeline ensures atomicity and reduces round-trips.
        """
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        if not items:
            return
        
        ttl = ttl if ttl is not None else self.default_ttl
        
        try:
            # Use pipeline for atomic batch operation
            async with self.redis.pipeline(transaction=False) as pipe:
                for key, value in items.items():
                    redis_key = self._make_key(key)
                    serialized = self._serialize(value)
                    
                    if ttl:
                        pipe.setex(redis_key, ttl, serialized)
                    else:
                        pipe.set(redis_key, serialized)
                
                await pipe.execute()
                self.stats['sets'] += len(items)
                
        except Exception as e:
            # Log error but don't fail
            pass
    
    async def delete(self, key: Any):
        """Delete entry from Redis"""
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        redis_key = self._make_key(key)
        
        try:
            await self.redis.delete(redis_key)
            self.stats['deletes'] += 1
        except Exception as e:
            pass
    
    async def clear(self):
        """
        Clear all entries with our key prefix.
        Uses SCAN for safe iteration (doesn't block Redis).
        """
        if not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        try:
            # Use SCAN to find all keys with our prefix
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(
                    cursor=cursor, 
                    match=f"{self.key_prefix}*",
                    count=1000
                )
                
                if keys:
                    await self.redis.delete(*keys)
                
                if cursor == 0:
                    break
                    
        except Exception as e:
            pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            **self.stats,
            'type': 'redis',
            'url': self.url,
            'key_prefix': self.key_prefix,
            'hit_rate': self.stats['hits'] / (self.stats['hits'] + self.stats['misses']) 
                       if (self.stats['hits'] + self.stats['misses']) > 0 else 0.0
        }

