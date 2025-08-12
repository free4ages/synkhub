from .memory_cache import InMemoryCache
from .redis_cache import RedisCache

def get_cache_backend(type_: str, config: dict):
    if type_ == "memory":
        return InMemoryCache(maxsize=config.get("maxsize", 10000))
    elif type_ == "redis":
        return RedisCache(**config)
    else:
        raise ValueError(f"Unsupported cache backend: {type_}")