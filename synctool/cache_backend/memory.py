import asyncio
import pickle
from typing import Any, Optional, Dict, List
from collections import OrderedDict
from datetime import datetime
from cachetools import LRUCache, LFUCache

from .base import BaseCacheBackend, CacheEntry


class InMemoryCacheBackend(BaseCacheBackend):
    """
    In-memory cache backend with multiple eviction policies.
    
    Supports:
    - LRU (Least Recently Used): Evicts least recently accessed items
    - LFU (Least Frequently Used): Evicts least frequently accessed items
    - FIFO (First In First Out): Evicts oldest items
    
    Thread/async-safe with asyncio.Lock for concurrent access.
    Uses asyncio.Lock to prevent race conditions during concurrent operations.
    """
    
    def __init__(self, max_size: int = 10000, policy: str = 'lru', ttl: Optional[int] = None):
        """
        Initialize in-memory cache.
        
        Args:
            max_size: Maximum number of entries
            policy: Eviction policy ('lru', 'lfu', 'fifo')
            ttl: Default time-to-live in seconds (None = no expiration)
        """
        self.max_size = max_size
        self.policy = policy.lower()
        self.default_ttl = ttl
        
        # Async lock for preventing race conditions
        self.lock = asyncio.Lock()
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'sets': 0,
            'deletes': 0,
            'expired': 0
        }
        
        # Initialize storage based on policy
        if self.policy == 'lru':
            # Use cachetools LRU for efficient implementation
            self._cache: Dict[str, CacheEntry] = LRUCache(maxsize=max_size)
        elif self.policy == 'lfu':
            # Use cachetools LFU
            self._cache: Dict[str, CacheEntry] = LFUCache(maxsize=max_size)
        elif self.policy == 'fifo':
            # Use OrderedDict for FIFO
            self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        else:
            raise ValueError(f"Unsupported eviction policy: {policy}. Use 'lru', 'lfu', or 'fifo'")
    
    def _make_key(self, key: Any) -> str:
        """Convert key to string representation"""
        if isinstance(key, tuple):
            return ':'.join(str(k) for k in key)
        return str(key)
    
    def _evict_if_needed(self):
        """Evict entries if cache is full (for FIFO policy only)"""
        if self.policy == 'fifo' and len(self._cache) >= self.max_size:
            # Remove oldest entry
            self._cache.popitem(last=False)
            self.stats['evictions'] += 1
    
    async def get(self, key: Any) -> Optional[Any]:
        """
        Get value from cache with expiration check.
        Thread-safe with async lock.
        """
        str_key = self._make_key(key)
        
        async with self.lock:
            entry = self._cache.get(str_key)
            
            if entry is None:
                self.stats['misses'] += 1
                return None
            
            # Check expiration
            if entry.is_expired():
                del self._cache[str_key]
                self.stats['misses'] += 1
                self.stats['expired'] += 1
                return None
            
            # Update access metadata
            entry.last_accessed = datetime.now()
            entry.access_count += 1
            
            self.stats['hits'] += 1
            return entry.value
    
    async def set(self, key: Any, value: Any, ttl: Optional[int] = None):
        """
        Set value in cache.
        Thread-safe with async lock to prevent race conditions.
        """
        str_key = self._make_key(key)
        ttl = ttl if ttl is not None else self.default_ttl
        
        async with self.lock:
            # For FIFO, manually handle eviction
            if self.policy == 'fifo':
                self._evict_if_needed()
            
            # Create cache entry
            now = datetime.now()
            entry = CacheEntry(
                value=value,
                created_at=now,
                last_accessed=now,
                access_count=0,
                ttl=ttl
            )
            
            # Set in cache (LRU/LFU handle eviction automatically)
            self._cache[str_key] = entry
            self.stats['sets'] += 1
    
    async def get_many(self, keys: List[Any]) -> Dict[Any, Any]:
        """
        Get multiple values from cache (batch operation).
        More efficient than multiple get() calls due to single lock acquisition.
        """
        result = {}
        
        async with self.lock:
            for key in keys:
                str_key = self._make_key(key)
                entry = self._cache.get(str_key)
                
                if entry is not None and not entry.is_expired():
                    # Update access metadata
                    entry.last_accessed = datetime.now()
                    entry.access_count += 1
                    result[key] = entry.value
                    self.stats['hits'] += 1
                else:
                    if entry is not None and entry.is_expired():
                        del self._cache[str_key]
                        self.stats['expired'] += 1
                    self.stats['misses'] += 1
        
        return result
    
    async def set_many(self, items: Dict[Any, Any], ttl: Optional[int] = None):
        """
        Set multiple values in cache (batch operation).
        More efficient than multiple set() calls due to single lock acquisition.
        """
        ttl = ttl if ttl is not None else self.default_ttl
        now = datetime.now()
        
        async with self.lock:
            for key, value in items.items():
                str_key = self._make_key(key)
                
                # For FIFO, manually handle eviction
                if self.policy == 'fifo':
                    self._evict_if_needed()
                
                # Create cache entry
                entry = CacheEntry(
                    value=value,
                    created_at=now,
                    last_accessed=now,
                    access_count=0,
                    ttl=ttl
                )
                
                self._cache[str_key] = entry
                self.stats['sets'] += 1
    
    async def delete(self, key: Any):
        """Delete entry from cache"""
        str_key = self._make_key(key)
        
        async with self.lock:
            if str_key in self._cache:
                del self._cache[str_key]
                self.stats['deletes'] += 1
    
    async def clear(self):
        """Clear all entries from cache"""
        async with self.lock:
            self._cache.clear()
            # Reset stats except configuration
            self.stats = {
                'hits': 0,
                'misses': 0,
                'evictions': 0,
                'sets': 0,
                'deletes': 0,
                'expired': 0
            }
    
    async def connect(self):
        """No connection needed for in-memory cache"""
        pass
    
    async def disconnect(self):
        """Cleanup resources"""
        await self.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            **self.stats,
            'size': len(self._cache),
            'max_size': self.max_size,
            'policy': self.policy,
            'hit_rate': self.stats['hits'] / (self.stats['hits'] + self.stats['misses']) 
                       if (self.stats['hits'] + self.stats['misses']) > 0 else 0.0
        }

