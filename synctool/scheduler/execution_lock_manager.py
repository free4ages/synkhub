"""
Execution lock manager for pipeline and table DDL locks.
Ensures only one strategy per pipeline runs at a time and handles table DDL change locks.
"""

import asyncio
import redis
import time
import logging
from typing import Optional, AsyncContextManager
from contextlib import asynccontextmanager


class ExecutionLockManager:
    """
    Manages distributed execution locks for pipelines and table DDL changes.
    
    - Pipeline-level lock: Only one strategy of a pipeline can run at a time
    - Table DDL lock: If table schema (DDL) change is in progress, other operations on that table wait or skip
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        pipeline_lock_timeout: int = 3600,  # 1 hour default
        table_ddl_lock_timeout: int = 7200  # 2 hours default
    ):
        self.redis_client = redis.from_url(redis_url)
        self.pipeline_lock_timeout = pipeline_lock_timeout
        self.table_ddl_lock_timeout = table_ddl_lock_timeout
        self.logger = logging.getLogger(__name__)
    
    @asynccontextmanager
    async def acquire_pipeline_lock(
        self,
        pipeline_id: str,
        strategy_name: str,
        timeout: Optional[float] = None,
        wait_for_lock: bool = False,
        wait_interval: float = 1.0
    ) -> AsyncContextManager[bool]:
        """
        Acquire pipeline-level execution lock.
        
        Args:
            pipeline_id: Pipeline identifier
            strategy_name: Strategy name (for logging)
            timeout: Lock expiry timeout in seconds (defaults to pipeline_lock_timeout)
            wait_for_lock: If True, wait for lock to become available
            wait_interval: Polling interval when waiting (seconds)
        
        Yields:
            bool: True if lock acquired, False otherwise
        """
        lock_key = f"synctool:exec:pipeline:{pipeline_id}"
        lock_value = f"{strategy_name}:{time.time()}"
        timeout_val = timeout or self.pipeline_lock_timeout
        
        acquired = False
        start_time = time.time()
        max_wait = timeout_val if wait_for_lock else 0
        
        try:
            while True:
                # Try to acquire lock
                if self.redis_client.set(lock_key, lock_value, nx=True, ex=int(timeout_val)):
                    acquired = True
                    self.logger.info(
                        f"Acquired pipeline execution lock for {pipeline_id}:{strategy_name}"
                    )
                    yield True
                    break
                
                # Check if we should wait
                if not wait_for_lock or (time.time() - start_time) >= max_wait:
                    self.logger.warning(
                        f"Could not acquire pipeline lock for {pipeline_id}:{strategy_name}"
                    )
                    yield False
                    break
                
                # Wait and retry
                await asyncio.sleep(wait_interval)
        
        except Exception as e:
            self.logger.error(
                f"Error acquiring pipeline lock for {pipeline_id}:{strategy_name}: {e}"
            )
            yield False
        
        finally:
            # Release lock if we acquired it
            if acquired:
                try:
                    lua_script = """
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("DEL", KEYS[1])
                    else
                        return 0
                    end
                    """
                    self.redis_client.eval(lua_script, 1, lock_key, lock_value)
                    self.logger.info(
                        f"Released pipeline execution lock for {pipeline_id}:{strategy_name}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error releasing pipeline lock for {pipeline_id}:{strategy_name}: {e}"
                    )
    
    @asynccontextmanager
    async def acquire_pipeline_enqueue_lock(
        self,
        pipeline_id: str,
        strategy_name: str,
        timeout: Optional[float] = None,
        wait_for_lock: bool = False,
        wait_interval: float = 1.0
    ) -> AsyncContextManager[bool]:
        """
        Acquire pipeline-level enqueue lock (separate from execution lock).
        This prevents multiple strategies from being enqueued for the same pipeline.
        
        Args:
            pipeline_id: Pipeline identifier
            strategy_name: Strategy name (for logging)
            timeout: Lock expiry timeout in seconds (defaults to 300s for enqueue)
            wait_for_lock: If True, wait for lock to become available
            wait_interval: Polling interval when waiting (seconds)
        
        Yields:
            bool: True if lock acquired, False otherwise
        """
        lock_key = f"synctool:enqueue:pipeline:{pipeline_id}"  # Different key prefix
        lock_value = f"{strategy_name}:{time.time()}"
        timeout_val = timeout or 300  # 5 minutes default for enqueue
        
        acquired = False
        start_time = time.time()
        max_wait = timeout_val if wait_for_lock else 0
        
        try:
            while True:
                # Try to acquire lock
                if self.redis_client.set(lock_key, lock_value, nx=True, ex=int(timeout_val)):
                    acquired = True
                    self.logger.info(
                        f"Acquired pipeline enqueue lock for {pipeline_id}:{strategy_name}"
                    )
                    yield True
                    break
                
                # Check if we should wait
                if not wait_for_lock or (time.time() - start_time) >= max_wait:
                    self.logger.warning(
                        f"Could not acquire pipeline enqueue lock for {pipeline_id}:{strategy_name}"
                    )
                    yield False
                    break
                
                await asyncio.sleep(wait_interval)
        
        except Exception as e:
            self.logger.error(
                f"Error acquiring pipeline enqueue lock for {pipeline_id}:{strategy_name}: {e}"
            )
            yield False
        
        finally:
            # Release lock if we acquired it
            if acquired:
                try:
                    lua_script = """
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("DEL", KEYS[1])
                    else
                        return 0
                    end
                    """
                    self.redis_client.eval(lua_script, 1, lock_key, lock_value)
                    self.logger.info(
                        f"Released pipeline enqueue lock for {pipeline_id}:{strategy_name}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error releasing pipeline enqueue lock for {pipeline_id}:{strategy_name}: {e}"
                    )
    
    def try_acquire_pipeline_enqueue_lock(
        self,
        pipeline_id: str,
        strategy_name: str,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Try to acquire pipeline-level enqueue lock WITHOUT automatic release.
        This lock must be manually released by the worker.
        
        Args:
            pipeline_id: Pipeline identifier
            strategy_name: Strategy name (for logging)
            timeout: Lock expiry timeout in seconds (defaults to 300s)
        
        Returns:
            bool: True if lock acquired, False otherwise
        """
        lock_key = f"synctool:enqueue:pipeline:{pipeline_id}"
        lock_value = f"{strategy_name}:{time.time()}"
        timeout_val = timeout or 300  # 5 minutes default
        
        try:
            if self.redis_client.set(lock_key, lock_value, nx=True, ex=int(timeout_val)):
                self.logger.info(
                    f"Acquired pipeline enqueue lock for {pipeline_id}:{strategy_name} "
                    f"(will be released by worker)"
                )
                return True
            else:
                self.logger.warning(
                    f"Could not acquire pipeline enqueue lock for {pipeline_id}:{strategy_name}"
                )
                return False
        except Exception as e:
            self.logger.error(
                f"Error acquiring pipeline enqueue lock for {pipeline_id}:{strategy_name}: {e}"
            )
            return False
    
    @asynccontextmanager
    async def acquire_table_ddl_lock(
        self,
        datastore_name: str,
        schema_name: str,
        table_name: str,
        operation: str = "read",
        timeout: Optional[float] = None,
        wait_for_lock: bool = False,
        wait_interval: float = 1.0
    ) -> AsyncContextManager[bool]:
        """
        Acquire table DDL lock for schema changes.
        
        Args:
            datastore_name: Datastore identifier (e.g., "postgres_prod")
            schema_name: Database schema name (e.g., "public")
            table_name: Table name (e.g., "users")
            operation: "read" or "ddl_change" (ddl_change blocks all, read blocks only DDL changes)
            timeout: Lock expiry timeout
            wait_for_lock: If True, wait for lock
            wait_interval: Polling interval
        
        Yields:
            bool: True if lock acquired, False otherwise
        """
        lock_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:{operation}"
        lock_value = f"{time.time()}"
        timeout_val = timeout or self.table_ddl_lock_timeout
        
        acquired = False
        start_time = time.time()
        max_wait = timeout_val if wait_for_lock else 0
        
        try:
            while True:
                # Check for conflicting locks
                if operation == "ddl_change":
                    # DDL change operations need exclusive access
                    read_lock_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:read"
                    ddl_lock_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:ddl_change"
                    
                    if self.redis_client.exists(read_lock_key) or self.redis_client.exists(ddl_lock_key):
                        if not wait_for_lock or (time.time() - start_time) >= max_wait:
                            self.logger.warning(
                                f"Table {datastore_name}.{schema_name}.{table_name} locked for DDL change"
                            )
                            yield False
                            return
                        await asyncio.sleep(wait_interval)
                        continue
                
                # Try to acquire lock
                if self.redis_client.set(lock_key, lock_value, nx=True, ex=int(timeout_val)):
                    acquired = True
                    self.logger.info(
                        f"Acquired table {operation} lock for {datastore_name}.{schema_name}.{table_name}"
                    )
                    yield True
                    break
                
                # Check if we should wait
                if not wait_for_lock or (time.time() - start_time) >= max_wait:
                    self.logger.warning(
                        f"Could not acquire table DDL lock for {datastore_name}.{schema_name}.{table_name}"
                    )
                    yield False
                    break
                
                await asyncio.sleep(wait_interval)
        
        except Exception as e:
            self.logger.error(
                f"Error acquiring table DDL lock for {datastore_name}.{schema_name}.{table_name}: {e}"
            )
            yield False
        
        finally:
            if acquired:
                try:
                    lua_script = """
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("DEL", KEYS[1])
                    else
                        return 0
                    end
                    """
                    self.redis_client.eval(lua_script, 1, lock_key, lock_value)
                    self.logger.info(
                        f"Released table {operation} lock for {datastore_name}.{schema_name}.{table_name}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error releasing table DDL lock for {datastore_name}.{schema_name}.{table_name}: {e}"
                    )
    
    def is_pipeline_locked(self, pipeline_id: str) -> bool:
        """Check if pipeline is currently locked"""
        lock_key = f"synctool:exec:pipeline:{pipeline_id}"
        try:
            return self.redis_client.exists(lock_key) > 0
        except Exception as e:
            self.logger.error(f"Error checking pipeline lock for {pipeline_id}: {e}")
            return False
    
    def is_table_locked(self, datastore_name: str, schema_name: str, table_name: str) -> bool:
        """Check if table has any active DDL locks"""
        try:
            read_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:read"
            ddl_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:ddl_change"
            return self.redis_client.exists(read_key) > 0 or self.redis_client.exists(ddl_key) > 0
        except Exception as e:
            self.logger.error(f"Error checking table lock for {datastore_name}.{schema_name}.{table_name}: {e}")
            return False
    
    def get_all_pipeline_locks(self) -> list[str]:
        """Get all active pipeline execution locks"""
        try:
            keys = self.redis_client.keys("synctool:exec:pipeline:*")
            return [key.decode().replace("synctool:exec:pipeline:", "") for key in keys]
        except Exception as e:
            self.logger.error(f"Error getting pipeline locks: {e}")
            return []
    
    def get_all_table_locks(self) -> list[str]:
        """Get all active table DDL locks"""
        try:
            keys = self.redis_client.keys("synctool:exec:table:*")
            return [key.decode() for key in keys]
        except Exception as e:
            self.logger.error(f"Error getting table locks: {e}")
            return []

