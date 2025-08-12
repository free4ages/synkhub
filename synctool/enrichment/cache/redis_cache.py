from redis import asyncio as aioredis
from .base import BaseCache

class RedisCache(BaseCache):
    def __init__(self):
        self.redis = None

    async def connect(self, config: dict):
        self.redis = await aioredis.from_url(config["url"])

    async def disconnect(self):
        if self.redis:
            await self.redis.close()

    def get(self, key):
        return self.redis.get(key)

    def set(self, key, value):
        return self.redis.set(key, value)