import redis
import time
import logging
from typing import Optional
from contextlib import contextmanager


class RedisLockManager:
    """Redis-based distributed lock manager"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", lock_timeout: int = 3600):
        self.redis_client = redis.from_url(redis_url)
        self.lock_timeout = lock_timeout
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def acquire_lock(self, job_name: str, strategy_name: str):
        """Acquire a distributed lock for a job strategy"""
        lock_key = f"synctool:lock:{job_name}:{strategy_name}"
        lock_value = f"{time.time()}"
        
        try:
            # Try to acquire lock
            if self.redis_client.set(lock_key, lock_value, nx=True, ex=self.lock_timeout):
                self.logger.info(f"Acquired lock for {job_name}:{strategy_name}")
                yield True
            else:
                self.logger.warning(f"Could not acquire lock for {job_name}:{strategy_name} - job may already be running")
                yield False
        except Exception as e:
            self.logger.error(f"Error with Redis lock for {job_name}:{strategy_name}: {e}")
            yield False
        finally:
            # Release lock
            try:
                # Only release if we still own the lock
                lua_script = """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("DEL", KEYS[1])
                else
                    return 0
                end
                """
                self.redis_client.eval(lua_script, 1, lock_key, lock_value)
                self.logger.info(f"Released lock for {job_name}:{strategy_name}")
            except Exception as e:
                self.logger.error(f"Error releasing lock for {job_name}:{strategy_name}: {e}")
    
    def is_locked(self, job_name: str, strategy_name: str) -> bool:
        """Check if a job strategy is currently locked"""
        lock_key = f"synctool:lock:{job_name}:{strategy_name}"
        try:
            return self.redis_client.exists(lock_key) > 0
        except Exception as e:
            self.logger.error(f"Error checking lock status for {job_name}:{strategy_name}: {e}")
            return False
    
    def get_all_locks(self) -> list[str]:
        """Get all current locks"""
        try:
            keys = self.redis_client.keys("synctool:lock:*")
            return [key.decode() for key in keys]
        except Exception as e:
            self.logger.error(f"Error getting all locks: {e}")
            return []
