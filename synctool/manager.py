import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any

from .core import SyncJobConfig, SyncProgress
from .engine import SyncEngine


class SyncJobManager:
    """Manage multiple sync jobs with scheduling and monitoring"""
    
    def __init__(self, max_concurrent_jobs: int = 2):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.active_jobs: Dict[str, SyncEngine] = {}
        self.job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.logger = logging.getLogger(f"{__name__}.SyncJobManager")
    
    async def run_sync_job(self, config: SyncJobConfig, 
                          strategy: Optional[str] = None,
                          start: Any = None, 
                          end: Any = None,
                          progress_callback: Optional[Callable[[str, SyncProgress], None]] = None) -> Dict[str, Any]:
        """Run a single sync job with resource management"""
        
        async with self.job_semaphore:
            job_name = config.name
            self.logger.info(f"Starting sync job: {job_name}")
            
            try:
                # Create and register sync engine
                sync_engine = SyncEngine(config)
                self.active_jobs[job_name] = sync_engine
                
                # Create progress callback wrapper
                def job_progress_callback(progress: SyncProgress):
                    if progress_callback:
                        progress_callback(job_name, progress)
                
                # Run the sync job
                result = await sync_engine.sync(
                    strategy=strategy,
                    start=start,
                    end=end,
                    progress_callback=job_progress_callback
                )
                
                self.logger.info(f"Sync job {job_name} completed successfully")
                return result
                
            except Exception as e:
                self.logger.error(f"Sync job {job_name} failed: {str(e)}")
                raise
            finally:
                # Clean up job registration
                if job_name in self.active_jobs:
                    del self.active_jobs[job_name]
    
    async def run_multiple_jobs(self, configs: List[SyncJobConfig],
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
