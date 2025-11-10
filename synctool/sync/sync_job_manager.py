import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING, Awaitable, Union

if TYPE_CHECKING:
    from ..monitoring.metrics_storage import MetricsStorage
    from ..scheduler.execution_lock_manager import ExecutionLockManager

from ..core.models import PipelineJobConfig, SyncProgress, DataStorage
from ..monitoring.metrics_collector import MetricsCollector
from ..monitoring.logging_collector import LoggingCollector
from ..monitoring.logs_storage import LogsStorage
from .sync_engine import SyncEngine


class SyncJobManager:
    """Manage multiple sync jobs with scheduling, locking, and monitoring"""
    
    def __init__(self, max_concurrent_jobs: int = 2, 
                 metrics_storage: Optional['MetricsStorage'] = None,
                 logs_storage: Optional[LogsStorage] = None,
                 data_storage: Optional[DataStorage] = None,
                 execution_lock_manager: Optional['ExecutionLockManager'] = None):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.metrics_storage = metrics_storage
        self.logs_storage = logs_storage
        self.data_storage = data_storage
        self.execution_lock_manager = execution_lock_manager
        self.active_jobs: Dict[str, SyncEngine] = {}
        self.job_semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.logger = logging.getLogger(f"{__name__}.SyncJobManager")
    
    async def run_sync_job(self, 
                          config: PipelineJobConfig, 
                          strategy_name: Optional[str] = None,
                          bounds: Optional[List[Dict[str, Any]]] = None,
                          progress_callback: Optional[Callable[[str, SyncProgress], None]] = None,
                          run_id: Optional[str] = None,  # ADD THIS PARAMETER
                          # Lock parameters
                          wait_for_pipeline_lock: bool = False,
                          pipeline_lock_wait_timeout: int = 60,
                          wait_for_table_lock: bool = False,
                          table_lock_wait_timeout: int = 30,
                          # Callback for enqueue lock release (worker context only)
                          on_locks_acquired: Optional[Union[Callable[[], None], Callable[[], Awaitable[None]]]] = None,
                          on_skip: Optional[Union[Callable[[], None], Callable[[], Awaitable[None]]]] = None) -> Dict[str, Any]:
        """
        Run a single sync job with pipeline and table lock management.
        
        Args:
            config: Pipeline configuration
            strategy_name: Strategy to execute
            bounds: Optional bounds for filtering
            progress_callback: Progress callback function
            run_id: Optional run ID to use (generates new one if not provided)  # ADD THIS
            wait_for_pipeline_lock: Whether to wait for pipeline lock
            pipeline_lock_wait_timeout: Max time to wait for pipeline lock (seconds)
            wait_for_table_lock: Whether to wait for table DDL lock
            table_lock_wait_timeout: Max time to wait for table DDL lock (seconds)
            on_locks_acquired: Callback (sync or async) when all locks are acquired
            on_skip: Callback (sync or async) when job is skipped
        
        Returns:
            Dict with status and result data
        """
        job_name = config.name
        strategy_name = strategy_name or 'default'
        
        # If no lock manager provided, run without locking
        if not self.execution_lock_manager:
            # Call on_locks_acquired if provided (no actual locks but ready to start)
            if on_locks_acquired:
                if asyncio.iscoroutinefunction(on_locks_acquired):
                    await on_locks_acquired()
                else:
                    on_locks_acquired()
            return await self._execute_job(config, strategy_name, bounds, progress_callback, run_id)  # PASS run_id
        
        # Get strategy config for identifying pipeline and table info
        strategy_config = self._get_strategy_config(config, strategy_name)
        if not strategy_config:
            error_msg = f"Strategy '{strategy_name}' not found in config"
            self.logger.error(error_msg)
            if on_skip:
                if asyncio.iscoroutinefunction(on_skip):
                    await on_skip()
                else:
                    on_skip()
            return {"status": "failed", "error": error_msg}
        
        # Try to acquire pipeline-level execution lock
        async with self.execution_lock_manager.acquire_pipeline_lock(
            pipeline_id=job_name,
            strategy_name=strategy_name,
            timeout=self.execution_lock_manager.pipeline_lock_timeout,
            wait_for_lock=wait_for_pipeline_lock,
            wait_interval=1.0
        ) as pipeline_lock_acquired:
            
            if not pipeline_lock_acquired:
                # Could not acquire pipeline lock - RELEASE ENQUEUE LOCK
                if on_skip:
                    if asyncio.iscoroutinefunction(on_skip):
                        await on_skip()
                    else:
                        on_skip()
                
                if wait_for_pipeline_lock:
                    skip_msg = f"Pipeline lock timeout after {pipeline_lock_wait_timeout}s, skipping execution"
                else:
                    skip_msg = "Pipeline lock not available, skipping execution"
                
                self.logger.warning(f"{job_name}:{strategy_name} - {skip_msg}")
                
                return {
                    "status": "skipped",
                    "reason": "pipeline_lock_unavailable",
                    "message": skip_msg
                }
            
            # Pipeline lock acquired - now check table DDL locks
            dest_info = self._get_destination_table_info(config)
            
            if dest_info and dest_info.get('table'):
                datastore_name = dest_info['datastore']
                schema_name = dest_info.get('schema', 'public')
                table_name = dest_info['table']
                
                # Check if table has active DDL locks
                table_locked = self.execution_lock_manager.is_table_locked(
                    datastore_name, schema_name, table_name
                )
                
                if table_locked:
                    self.logger.warning(
                        f"Table {datastore_name}.{schema_name}.{table_name} has active DDL lock"
                    )
                    
                    if wait_for_table_lock:
                        # Wait for table lock to become available
                        self.logger.info(f"Waiting for table DDL lock to be released...")
                        wait_time = 0
                        
                        while wait_time < table_lock_wait_timeout:
                            await asyncio.sleep(1)
                            wait_time += 1
                            
                            if not self.execution_lock_manager.is_table_locked(
                                datastore_name, schema_name, table_name
                            ):
                                self.logger.info(f"Table DDL lock released after {wait_time}s")
                                table_locked = False
                                break
                        
                        if table_locked:
                            # Timeout waiting for table lock - RELEASE ENQUEUE LOCK
                            if on_skip:
                                if asyncio.iscoroutinefunction(on_skip):
                                    await on_skip()
                                else:
                                    on_skip()
                            
                            skip_msg = f"Table DDL lock timeout after {table_lock_wait_timeout}s, skipping execution"
                            self.logger.warning(skip_msg)
                            
                            return {
                                "status": "skipped",
                                "reason": "table_ddl_lock_timeout",
                                "message": skip_msg
                            }
                    else:
                        # Skip execution if table is locked - RELEASE ENQUEUE LOCK
                        if on_skip:
                            if asyncio.iscoroutinefunction(on_skip):
                                await on_skip()
                            else:
                                on_skip()
                        
                        skip_msg = "Table has active DDL lock, skipping execution"
                        self.logger.warning(skip_msg)
                        
                        return {
                            "status": "skipped",
                            "reason": "table_ddl_lock_active",
                            "message": skip_msg
                        }
            
            # All locks acquired successfully - RELEASE ENQUEUE LOCK and start execution
            if on_locks_acquired:
                if asyncio.iscoroutinefunction(on_locks_acquired):
                    await on_locks_acquired()
                else:
                    on_locks_acquired()
        
        # Execute the job (at the end)
        return await self._execute_job(config, strategy_name, bounds, progress_callback, run_id)  # PASS run_id
    
    async def _execute_job(self, config: PipelineJobConfig, 
                          strategy_name: str,
                          bounds: Optional[List[Dict[str, Any]]] = None,
                          progress_callback: Optional[Callable[[str, SyncProgress], None]] = None,
                          run_id: Optional[str] = None) -> Dict[str, Any]:  # ADD THIS PARAMETER
        """Execute the actual sync job with metrics collection"""
        job_name = config.name
        
        # Initialize metrics and logging collectors if storages are provided
        metrics_collector = None
        logging_collector = None
        final_run_id = run_id  # Use provided run_id or will be generated
        
        if self.metrics_storage:
            metrics_collector = MetricsCollector(self.metrics_storage)
            # Pass run_id to metrics collector (it will generate new one if None)
            final_run_id = metrics_collector.start_job_run(job_name, strategy_name, run_id)
            self.logger.info(f"Started metrics collection for job {job_name}, run {final_run_id}")
        
        if self.logs_storage and final_run_id:
            logging_collector = LoggingCollector(self.logs_storage)
            logging_collector.start_job_run(job_name, strategy_name,final_run_id)
            self.logger.info(f"Started logging collection for job {job_name}, run {final_run_id}")
        
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
                bounds=bounds,
                progress_callback=job_progress_callback
            )
            
            # Finish job run - metrics updates are handled by ProgressManager
            if metrics_collector:
                metrics_collector.finish_job_run("completed")
                self.logger.info(f"Completed metrics collection for run {final_run_id}")
            
            self.logger.info(f"Completed sync job: {job_name}:{strategy_name}")
            
        except Exception as e:
            # Finish job run with error
            if metrics_collector:
                metrics_collector.finish_job_run("failed", str(e))
                self.logger.error(f"Failed metrics collection for run {final_run_id}: {e}")
            
            self.logger.error(f"Failed sync job {job_name}:{strategy_name}: {e}")
            raise e
        finally:
            # Ensure logging handler is removed
            if logging_collector:
                logging_collector.stop_job_run()
        return result
    
    def _get_strategy_config(self, config: PipelineJobConfig, strategy_name: str):
        """Get strategy config from pipeline config"""
        for stage in config.stages:
            for strategy in stage.strategies:
                if strategy.name == strategy_name:
                    return strategy
        return None
    
    def _get_destination_table_info(self, config: PipelineJobConfig) -> Optional[Dict[str, str]]:
        """Extract destination table information from config"""
        try:
            if hasattr(config, 'destination') and config.destination:
                dest = config.destination
                return {
                    'datastore': dest.datastore if hasattr(dest, 'datastore') else None,
                    'schema': dest.schema if hasattr(dest, 'schema') else 'public',
                    'table': dest.table if hasattr(dest, 'table') else None
                }
        except Exception as e:
            self.logger.warning(f"Could not extract destination table info: {e}")
        
        return None
    
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