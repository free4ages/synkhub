import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..monitoring.metrics_storage import MetricsStorage
    from ..scheduler.redis_lock_manager import RedisLockManager

from ..core.models import PipelineJobConfig, SyncProgress, DataStorage
from ..monitoring.metrics_collector import MetricsCollector
from ..monitoring.logging_collector import LoggingCollector
from ..monitoring.logs_storage import LogsStorage
from .sync_engine import SyncEngine


class SyncJobManager:
    """Manage multiple sync jobs with scheduling, locking, and monitoring"""
    
    def __init__(self, max_concurrent_jobs: int = 2, 
                 metrics_storage: Optional['MetricsStorage'] = None,
                 lock_manager: Optional['RedisLockManager'] = None,
                 logs_storage: Optional[LogsStorage] = None,
                 data_storage: Optional[DataStorage] = None):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.metrics_storage = metrics_storage
        self.lock_manager = lock_manager
        self.logs_storage = logs_storage
        self.data_storage = data_storage
        self.active_jobs: Dict[str, SyncEngine] = {}
        self.job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.logger = logging.getLogger(f"{__name__}.SyncJobManager")
    
    async def run_sync_job(self, config: PipelineJobConfig, 
                          strategy_name: Optional[str] = None,
                          start: Any = None, 
                          end: Any = None,
                          progress_callback: Optional[Callable[[str, SyncProgress], None]] = None,
                          use_locking: bool = True) -> Dict[str, Any]:
        """Run a single sync job with resource management, locking, and metrics collection"""
        job_name = config.name
        strategy_name = strategy_name or 'default'
        
        # Handle Redis locking if lock_manager is provided and use_locking is True
        if self.lock_manager and use_locking:
            with self.lock_manager.acquire_lock(job_name, strategy_name) as lock_acquired:
                if not lock_acquired:
                    self.logger.warning(f"Could not acquire lock for {job_name}:{strategy_name}")
                    return {
                        'status': 'skipped',
                        'reason': 'could_not_acquire_lock',
                        'job_name': job_name,
                        'strategy': strategy_name
                    }
                
                return await self._execute_job(config, strategy_name, start, end, progress_callback)
        else:
            # Run without locking
            return await self._execute_job(config, strategy_name, start, end, progress_callback)
    
    async def _execute_job(self, config: PipelineJobConfig, 
                          strategy_name: str,
                          start: Any = None, 
                          end: Any = None,
                          progress_callback: Optional[Callable[[str, SyncProgress], None]] = None) -> Dict[str, Any]:
        """Execute the actual sync job with metrics collection"""
        job_name = config.name
        
        # Initialize metrics and logging collectors if storages are provided
        metrics_collector = None
        logging_collector = None
        run_id = None
        if self.metrics_storage:
            metrics_collector = MetricsCollector(self.metrics_storage)
            run_id = metrics_collector.start_job_run(job_name, strategy_name)
            self.logger.info(f"Started metrics collection for job {job_name}, run {run_id}")
        if self.logs_storage and run_id:
            logging_collector = LoggingCollector(self.logs_storage)
            logging_collector.start_job_run(job_name, run_id)
            self.logger.info(f"Started logging collection for job {job_name}, run {run_id}")
        
        result: Dict[str, Any] = {}
        try:
            self.logger.info(f"Starting sync job: {job_name} with strategy: {strategy_name}")
            
            # Create sync engine with metrics collector and data storage
            sync_engine = SyncEngine(config, metrics_collector, logger=self.logger, data_storage=self.data_storage)
            
            # Create progress callback wrapper for job-specific callback
            def job_progress_callback(progress: SyncProgress):
                if progress_callback:
                    progress_callback(job_name, progress)
            
            # Run the sync job - metrics collection is now handled internally by SyncEngine
            result = await sync_engine.sync(
                strategy_name=strategy_name,
                start=start,
                end=end,
                progress_callback=job_progress_callback
            )
            
            # Finish job run - metrics updates are handled by ProgressManager
            if metrics_collector:
                metrics_collector.finish_job_run("completed")
                self.logger.info(f"Completed metrics collection for run {run_id}")
            
            self.logger.info(f"Completed sync job: {job_name}:{strategy_name}")
            
        except Exception as e:
            # Finish job run with error
            if metrics_collector:
                metrics_collector.finish_job_run("failed", str(e))
                self.logger.error(f"Failed metrics collection for run {run_id}: {e}")
            
            self.logger.error(f"Failed sync job {job_name}:{strategy_name}: {e}")
            raise e
        finally:
            # Ensure logging handler is removed
            if logging_collector:
                logging_collector.stop_job_run()
        return result
    
    async def run_multiple_jobs(self, configs: List[PipelineJobConfig],
                               progress_callback: Optional[Callable[[str, SyncProgress], None]] = None) -> Dict[str, Dict[str, Any]]:
        """Run multiple sync jobs concurrently"""
        
        tasks = []
        for config in configs:
            task = self.run_sync_job(
                config=config,
                progress_callback=progress_callback
            )
            tasks.append((config.name, task))
        
        results = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        for (job_name, _), result in zip(tasks, completed_tasks):
            if isinstance(result, Exception):
                results[job_name] = {
                    'status': 'failed',
                    'error': str(result)
                }
            else:
                results[job_name] = result
        
        return results
    
    def get_active_jobs(self) -> List[str]:
        """Get list of currently active job names"""
        return list(self.active_jobs.keys())
    
    async def cancel_job(self, job_name: str) -> bool:
        """Cancel a running job"""
        if job_name in self.active_jobs:
            try:
                await self.active_jobs[job_name].cleanup()
                del self.active_jobs[job_name]
                self.logger.info(f"Cancelled job: {job_name}")
                return True
            except Exception as e:
                self.logger.error(f"Error cancelling job {job_name}: {str(e)}")
                return False
        return False