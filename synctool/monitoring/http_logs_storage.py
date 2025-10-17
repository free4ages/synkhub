import json
import logging
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import deque
from threading import RLock
import time

from .logs_storage import LogsStorage


class BufferedHttpLogsStorage(LogsStorage):
    """
    Extends LogsStorage to send logs to scheduler via HTTP in batches.
    Falls back to local file storage if scheduler is unreachable.
    NO background thread - flushes on buffer full or explicit flush.
    """
    
    def __init__(
        self,
        scheduler_url: str,
        logs_dir: str = "./data/logs",
        max_runs_per_strategy: int = 50,
        batch_size: int = 50,
        max_retries: int = 2,
        local_fallback: bool = True,
        http_timeout: float = 3.0
    ):
        super().__init__(logs_dir=logs_dir, max_runs_per_strategy=max_runs_per_strategy)
        
        self.scheduler_url = scheduler_url.rstrip('/')
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.local_fallback = local_fallback
        self.http_timeout = http_timeout
        
        # Buffer for pending log entries
        self._buffer: deque = deque()
        self._buffer_lock = RLock()
        
        self.logger = logging.getLogger(f"{__name__}.BufferedHttpLogsStorage")
    
    def append_log(
        self,
        job_name: str,
        strategy_name: str,
        run_id: str,
        timestamp: datetime,
        level: str,
        message: str,
        logger_name: Optional[str] = None
    ) -> None:
        """
        Append log entry to buffer (and optionally local file).
        Will be sent to scheduler in batches.
        """
        entry = {
            "job_name": job_name,
            "strategy_name": strategy_name,
            "run_id": run_id,
            "timestamp": timestamp.isoformat(),
            "level": level,
            "message": message,
            "logger": logger_name,
        }
        
        # Always write to local file first (guaranteed to work)
        if self.local_fallback:
            try:
                super().append_log(job_name, strategy_name, run_id, timestamp, level, message, logger_name)
            except Exception as e:
                self.logger.error(f"Failed to write local log: {e}")
        
        # Add to buffer and check if flush needed
        should_flush = False
        with self._buffer_lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self.batch_size:
                should_flush = True
        # Lock released here
        
        # Flush OUTSIDE the lock to avoid nested lock acquisition
        if should_flush:
            try:
                self._flush_buffer_non_blocking()
            except Exception as e:
                self.logger.warning(f"Non-blocking flush failed: {e}")
    
    def _flush_buffer_non_blocking(self):
        """Flush buffer without blocking (fire and forget)"""
        # Now we can safely acquire the lock
        with self._buffer_lock:
            if not self._buffer:
                return
            
            # Take up to batch_size entries
            batch = []
            while self._buffer and len(batch) < self.batch_size:
                batch.append(self._buffer.popleft())
        # Lock released here
        
        if not batch:
            return
        
        # Send outside the lock (no lock held here)
        try:
            try:
                loop = asyncio.get_running_loop()
                asyncio.create_task(self._send_batch_async_safe(batch))
            except RuntimeError:
                with self._buffer_lock:
                    self._buffer.extendleft(reversed(batch))
        except Exception as e:
            self.logger.debug(f"Could not schedule HTTP send: {e}")
    
    async def _send_batch_async_safe(self, batch: List[Dict[str, Any]]) -> bool:
        """Send batch with proper error handling and short timeout"""
        try:
            timeout = aiohttp.ClientTimeout(total=self.http_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.scheduler_url}/api/worker/logs/batch",
                    json={"logs": batch}
                ) as response:
                    if response.status == 200:
                        return True
                    else:
                        self.logger.warning(f"HTTP log send failed with status {response.status}")
                        return False
        except asyncio.TimeoutError:
            self.logger.debug(f"HTTP log send timed out after {self.http_timeout}s")
            return False
        except Exception as e:
            self.logger.debug(f"HTTP log send failed: {e}")
            return False
    
    def flush(self):
        """Force flush all buffered logs - BLOCKING version for final cleanup"""
        with self._buffer_lock:
            if not self._buffer:
                return
            
            batch = list(self._buffer)
            self._buffer.clear()
        
        if not batch:
            return
        
        # Try to send synchronously for final flush
        try:
            # Check if in async context
            try:
                loop = asyncio.get_running_loop()
                # Can't do blocking send in async context
                self.logger.debug("Cannot do blocking flush in async context, logs saved locally")
            except RuntimeError:
                # Not in async context - can send synchronously
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._send_batch_async_safe(batch))
                except Exception as e:
                    self.logger.warning(f"Flush failed: {e}")
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)
        except Exception as e:
            self.logger.warning(f"Failed to flush logs: {e}")
    
    def close(self):
        """Close and flush - but don't wait too long"""
        try:
            self.flush()
        except Exception as e:
            self.logger.debug(f"Close/flush failed: {e}")

