import json
import logging
import asyncio
import aiohttp
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import deque
from threading import Lock, Thread
import time

from .logs_storage import LogsStorage


class BufferedHttpLogsStorage(LogsStorage):
    """
    Extends LogsStorage to send logs to scheduler via HTTP in batches.
    Falls back to local file storage if scheduler is unreachable.
    """
    
    def __init__(
        self,
        scheduler_url: str,
        logs_dir: str = "./data/logs",
        max_runs_per_job: int = 50,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_retries: int = 3,
        local_fallback: bool = True
    ):
        # Initialize parent (local file storage)
        super().__init__(logs_dir=logs_dir, max_runs_per_job=max_runs_per_job)
        
        self.scheduler_url = scheduler_url.rstrip('/')
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        self.local_fallback = local_fallback
        
        # Buffer for pending log entries
        self._buffer: deque = deque()
        self._buffer_lock = Lock()
        
        # Track failed batches for retry
        self._failed_batches: deque = deque()
        
        # Background thread for flushing
        self._flush_thread: Optional[Thread] = None
        self._stop_flag = False
        self._start_flush_thread()
        
        self.logger = logging.getLogger(f"{__name__}.BufferedHttpLogsStorage")
    
    def _start_flush_thread(self):
        """Start background thread for periodic flushing"""
        self._stop_flag = False
        self._flush_thread = Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
    
    def _flush_loop(self):
        """Background loop that flushes buffer periodically"""
        while not self._stop_flag:
            try:
                time.sleep(self.flush_interval)
                self._flush_buffer()
                self._retry_failed_batches()
            except Exception as e:
                self.logger.error(f"Error in flush loop: {e}")
    
    def append_log(
        self,
        job_name: str,
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
            "run_id": run_id,
            "timestamp": timestamp.isoformat(),
            "level": level,
            "message": message,
            "logger": logger_name,
        }
        
        # Add to buffer
        with self._buffer_lock:
            self._buffer.append(entry)
            
            # Flush if buffer is full
            if len(self._buffer) >= self.batch_size:
                self._flush_buffer()
        
        # Also write to local file as backup
        if self.local_fallback:
            super().append_log(job_name, run_id, timestamp, level, message, logger_name)
    
    def _flush_buffer(self):
        """Send buffered logs to scheduler"""
        with self._buffer_lock:
            if not self._buffer:
                return
            
            # Take up to batch_size entries
            batch = []
            while self._buffer and len(batch) < self.batch_size:
                batch.append(self._buffer.popleft())
        
        if not batch:
            return
        
        # Send batch to scheduler
        success = self._send_batch_sync(batch)
        
        if not success:
            # Add to failed batches for retry
            with self._buffer_lock:
                self._failed_batches.append(batch)
                
                # Limit failed batch queue size
                if len(self._failed_batches) > 10:
                    self._failed_batches.popleft()
    
    def _send_batch_sync(self, batch: List[Dict[str, Any]]) -> bool:
        """Send a batch of logs to scheduler (runs async in sync context)"""
        try:
            # Run async HTTP call in the event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self._send_batch_async(batch))
                return result
            finally:
                loop.close()
        except Exception as e:
            self.logger.warning(f"Failed to send log batch to scheduler: {e}")
            return False
    
    async def _send_batch_async(self, batch: List[Dict[str, Any]]) -> bool:
        """Send a batch of logs to scheduler via HTTP (async)"""
        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.scheduler_url}/api/worker/logs/batch",
                    json={"logs": batch}
                ) as response:
                    response.raise_for_status()
                    return True
        except Exception as e:
            self.logger.warning(f"Failed to send log batch to scheduler: {e}")
            return False
    
    def _retry_failed_batches(self):
        """Retry sending previously failed batches"""
        with self._buffer_lock:
            retry_count = min(3, len(self._failed_batches))
            
            for _ in range(retry_count):
                if not self._failed_batches:
                    break
                
                batch = self._failed_batches.popleft()
                success = self._send_batch_sync(batch)
                
                if not success:
                    # Put back at end of queue
                    self._failed_batches.append(batch)
                    break
    
    def flush(self):
        """Force flush all buffered logs immediately"""
        self._flush_buffer()
        self._retry_failed_batches()
    
    def close(self):
        """Stop background thread and flush remaining logs"""
        self._stop_flag = True
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)
        self.flush()
    
    def __del__(self):
        """Ensure cleanup on deletion"""
        try:
            self.close()
        except:
            pass

