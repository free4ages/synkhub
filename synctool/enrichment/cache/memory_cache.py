from .base import BaseCache
from cachetools import LRUCache
from threading import Lock

class InMemoryCache(BaseCache):
    def __init__(self, maxsize=10000):
        self.cache = LRUCache(maxsize=maxsize)
        self.lock = Lock()

    async def get(self, key):
        with self.lock:
            return self.cache.get(key)

    async def set(self, key, value, ttl=None):
        with self.lock:
            self.cache[key] = value