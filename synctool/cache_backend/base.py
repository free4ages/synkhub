from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0
    ttl: Optional[int] = None  # Time to live in seconds
    
    def is_expired(self) -> bool:
        """Check if entry has expired based on TTL"""
        if self.ttl is None:
            return False
        
        elapsed = (datetime.now() - self.created_at).total_seconds()
        return elapsed > self.ttl


class BaseCacheBackend(ABC):
    """
    Base class for cache backends.
    
    All cache backends should implement this interface to support
    different storage mechanisms (in-memory, Redis, etc.) and
    eviction policies (LRU, LFU, FIFO).
    
    Thread/async-safe: All implementations must handle concurrent access.
    """
    
    @abstractmethod
    async def get(self, key: Any) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key (will be converted to string internally)
            
        Returns:
            Cached value or None if not found/expired
        """
        pass
    
    @abstractmethod
    async def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Optional time-to-live in seconds (overrides default)
        """
        pass
    
    @abstractmethod
    async def get_many(self, keys: List[Any]) -> Dict[Any, Any]:
        """
        Get multiple values from cache (batch operation).
        
        Args:
            keys: List of cache keys
            
        Returns:
            Dict mapping keys to values (missing keys not included)
        """
        pass
    
    @abstractmethod
    async def set_many(self, items: Dict[Any, Any], ttl: Optional[int] = None):
        """
        Set multiple values in cache (batch operation).
        
        Args:
            items: Dict mapping keys to values
            ttl: Optional time-to-live in seconds
        """
        pass
    
    @abstractmethod
    async def delete(self, key: Any):
        """Delete entry from cache"""
        pass
    
    @abstractmethod
    async def clear(self):
        """Clear all entries from cache"""
        pass
    
    @abstractmethod
    async def connect(self):
        """Initialize connection (for external caches like Redis)"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close connection and cleanup resources"""
        pass
    
    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dict with stats like: hits, misses, size, evictions, etc.
        """
        pass
