import logging
import asyncio
import aiohttp
from typing import Optional
from pathlib import Path

from ..core.models import JobRunMetrics
from .metrics_storage import MetricsStorage


class HttpMetricsStorage(MetricsStorage):
    """
    Extends MetricsStorage to send metrics to scheduler via HTTP.
    Falls back to local file storage.
    """
    
    def __init__(
        self,
        scheduler_url: str,
        metrics_dir: str = "./data/metrics",
        max_runs_per_job: int = 50,
        max_retries: int = 3,
        local_fallback: bool = True
    ):
        # Initialize parent (local file storage)
        super().__init__(metrics_dir=metrics_dir, max_runs_per_job=max_runs_per_job)
        
        self.scheduler_url = scheduler_url.rstrip('/')
        self.max_retries = max_retries
        self.local_fallback = local_fallback
        self.logger = logging.getLogger(f"{__name__}.HttpMetricsStorage")
    
    def save_metrics(self, metrics: JobRunMetrics) -> None:
        """
        Save metrics locally AND send to scheduler via HTTP.
        """
        # Always save locally first (as fallback)
        if self.local_fallback:
            super().save_metrics(metrics)
        
        # Try to send to scheduler
        self._send_metrics_to_scheduler(metrics)
    
    def _send_metrics_to_scheduler(self, metrics: JobRunMetrics) -> bool:
        """Send metrics to scheduler via HTTP (runs async in sync context)"""
        try:
            # Run async HTTP call in the event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self._send_metrics_async(metrics))
                return result
            finally:
                loop.close()
        except Exception as e:
            self.logger.error(f"Failed to send metrics to scheduler: {e}")
            return False
    
    async def _send_metrics_async(self, metrics: JobRunMetrics) -> bool:
        """Send metrics to scheduler via HTTP (async)"""
        metrics_data = {
            "job_name": metrics.job_name,
            "strategy_name": metrics.strategy_name,
            "run_id": metrics.run_id,
            "start_time": metrics.start_time.isoformat(),
            "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
            "status": metrics.status,
            "rows_fetched": metrics.rows_fetched,
            "rows_detected": metrics.rows_detected,
            "rows_inserted": metrics.rows_inserted,
            "rows_updated": metrics.rows_updated,
            "rows_deleted": metrics.rows_deleted,
            "error_message": metrics.error_message,
            "partition_count": metrics.partition_count,
            "successful_partitions": metrics.successful_partitions,
            "failed_partitions": metrics.failed_partitions,
            "duration_seconds": metrics.duration_seconds
        }
        
        for attempt in range(self.max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=5.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        f"{self.scheduler_url}/api/worker/metrics",
                        json=metrics_data
                    ) as response:
                        response.raise_for_status()
                        self.logger.debug(f"Sent metrics to scheduler for run {metrics.run_id}")
                        return True
            except Exception as e:
                self.logger.warning(
                    f"Failed to send metrics to scheduler (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1.0 * (attempt + 1))  # Exponential backoff
        
        self.logger.error(f"Failed to send metrics to scheduler after {self.max_retries} attempts")
        return False

