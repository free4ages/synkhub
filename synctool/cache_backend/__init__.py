from .base import BaseCacheBackend
from .memory import InMemoryCacheBackend
from .redis import RedisCacheBackend

def get_cache_backend(cache_type: str, config: dict) -> BaseCacheBackend:
    """
    Factory function to create cache backend instances.
    
    Args:
        cache_type: Type of cache ('memory', 'redis')
        config: Configuration dict with cache-specific settings
        
    Returns:
        Cache backend instance
        
    Example config:
        {
            'type': 'memory',
            'policy': 'lru',  # or 'lfu', 'fifo'
            'max_size': 10000,
            'ttl': 3600  # seconds
        }
    """
    cache_type = cache_type.lower()
    
    if cache_type == 'memory':
        return InMemoryCacheBackend(
            max_size=config.get('max_size', 10000),
            policy=config.get('policy', 'lru'),
            ttl=config.get('ttl')
        )
    elif cache_type == 'redis':
        return RedisCacheBackend(
            url=config.get('url', 'redis://localhost:6379'),
            ttl=config.get('ttl', 3600),
            key_prefix=config.get('key_prefix', 'synctool:join:')
        )
    else:
        raise ValueError(f"Unsupported cache type: {cache_type}. Supported: 'memory', 'redis'")

__all__ = ['BaseCacheBackend', 'InMemoryCacheBackend', 'RedisCacheBackend', 'get_cache_backend']
