"""
Enhanced ARQ worker tasks with strategy-level execution and locking.
Enqueue lock is held until either skipped or all locks acquired.
Retry logic is handled by the scheduler's slot-aware retry mechanism.
"""

import asyncio
import logging
import aiohttp
import socket
import os
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from ..config.config_serializer import ConfigSerializer
from ..config.global_config_loader import get_global_config
from ..sync.sync_job_manager import SyncJobManager
from ..monitoring.http_metrics_storage import HttpMetricsStorage
from ..monitoring.http_logs_storage import BufferedHttpLogsStorage
from ..scheduler.execution_lock_manager import ExecutionLockManager

# Get worker identifier
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

logger = logging.getLogger(__name__)


async def execute_pipeline_strategy(
    ctx: dict,
    config_json: Dict[str, Any],
    strategy_name: str,
    run_id: str,
    pipeline_id: str,
    scheduler_url: str
) -> Dict[str, Any]:
    """
    Enhanced ARQ worker function to execute a pipeline strategy.
    
    Enqueue lock lifecycle:
    - Acquired by scheduler before enqueuing
    - Held throughout lock acquisition phase
    - Released when:
      a) All locks acquired and execution starts, OR
      b) Job is skipped due to lock unavailability
    
    Args:
        ctx: ARQ context
        config_json: JSON-serialized config and datastores
        strategy_name: Strategy to execute
        run_id: Unique run ID
        pipeline_id: Pipeline identifier
        scheduler_url: URL of scheduler's HTTP endpoint
    
    Returns:
        Result dictionary with status and metadata
    """
    
    global_config = get_global_config()
    logs_storage = None
    execution_lock_manager = None
    enqueue_lock_released = False
    
    try:
        # Deserialize config from JSON
        config, data_storage = ConfigSerializer.json_dict_to_config(config_json)
        
        logger.info(
            f"Worker {WORKER_ID} starting {pipeline_id}:{strategy_name} (run_id: {run_id})"
        )
        
        # Get strategy config for wait settings
        strategy_config = _get_strategy_config(config, strategy_name)
        if not strategy_config:
            error_msg = f"Strategy '{strategy_name}' not found in config"
            logger.error(error_msg)
            await _send_status(
                scheduler_url, pipeline_id, strategy_name, run_id,
                "failed", error_msg, error=error_msg
            )
            return {"status": "failed", "error": error_msg, "worker": WORKER_ID}
        
        # Extract strategy settings for lock behavior
        wait_for_pipeline_lock = getattr(strategy_config, 'wait_for_pipeline_lock', False)
        pipeline_lock_wait_timeout = getattr(strategy_config, 'pipeline_lock_wait_timeout', 60)
        wait_for_table_lock = getattr(strategy_config, 'wait_for_table_lock', False)
        table_lock_wait_timeout = getattr(strategy_config, 'table_lock_wait_timeout', 30)
        
        # Initialize execution lock manager
        execution_lock_manager = ExecutionLockManager(
            redis_url=global_config.redis.url,
            pipeline_lock_timeout=global_config.scheduler.lock_timeout
        )
        
        # Define callback to release enqueue lock
        def release_enqueue_lock():
            nonlocal enqueue_lock_released
            if not enqueue_lock_released:
                _release_enqueue_lock(execution_lock_manager, pipeline_id)
                enqueue_lock_released = True

        # Define async callback for when locks are acquired
        async def on_locks_acquired_callback():
            release_enqueue_lock()
            await _send_status(
                scheduler_url, pipeline_id, strategy_name, run_id,
                "running", "Execution in progress"
            )
            logger.info(f"All locks acquired for {pipeline_id}:{strategy_name}, starting execution")

        # Define async callback for when job is skipped
        async def on_skip_callback():
            release_enqueue_lock()
            logger.info(f"Job skipped for {pipeline_id}:{strategy_name}, enqueue lock released")
        
        # Initialize storage instances
        metrics_storage = HttpMetricsStorage(
            scheduler_url=scheduler_url,
            metrics_dir=global_config.storage.metrics_dir,
            max_runs_per_strategy=global_config.storage.max_runs_per_strategy,
            max_retries=global_config.http.max_retries,
            local_fallback=global_config.log_batching.local_fallback
        )
        
        logs_storage = BufferedHttpLogsStorage(
            scheduler_url=scheduler_url,
            logs_dir=global_config.storage.logs_dir,
            max_runs_per_strategy=global_config.storage.max_runs_per_strategy,
            batch_size=global_config.log_batching.batch_size,
            flush_interval=global_config.log_batching.flush_interval,
            max_retries=global_config.http.max_retries,
            local_fallback=global_config.log_batching.local_fallback
        )
        
        # Create job manager with lock manager
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=metrics_storage,
            logs_storage=logs_storage,
            data_storage=data_storage,
            execution_lock_manager=execution_lock_manager
        )
        
        # Execute job - enqueue lock will be released by callbacks
        result = await job_manager.run_sync_job(
            config=config,
            strategy_name=strategy_name,
            run_id=run_id,
            wait_for_pipeline_lock=wait_for_pipeline_lock,
            pipeline_lock_wait_timeout=pipeline_lock_wait_timeout,
            wait_for_table_lock=wait_for_table_lock,
            table_lock_wait_timeout=table_lock_wait_timeout,
            on_locks_acquired=on_locks_acquired_callback,
            on_skip=on_skip_callback
        )
        
        # Check if job was skipped due to locking
        if result.get('status') == 'skipped':
            reason = result.get('reason', 'unknown')
            
            # Send skipped status - scheduler will retry via slot-aware logic
            await _send_status(
                scheduler_url, pipeline_id, strategy_name, run_id,
                "skipped", result.get('message', 'Job skipped')
            )
            
            logger.info(
                f"Job skipped for {pipeline_id}:{strategy_name} due to {reason}. "
                f"Scheduler will retry within current schedule slot."
            )
            
            return {
                "status": "skipped",
                "reason": reason,
                "message": result.get('message'),
                "worker": WORKER_ID
            }
        
        # Flush remaining logs
        if logs_storage:
            logs_storage.flush()
        
        # Send success status
        await _send_status(
            scheduler_url, pipeline_id, strategy_name, run_id,
            "success", "Completed successfully"
        )
        
        logger.info(
            f"Worker {WORKER_ID} completed {pipeline_id}:{strategy_name}"
        )
        
        return {
            "status": "success",
            "result": result,
            "worker": WORKER_ID
        }
    
    except Exception as e:
        error_msg = f"Pipeline execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # Try to flush any buffered logs
        try:
            if logs_storage:
                logs_storage.flush()
        except:
            pass
        
        # Release enqueue lock if not already released
        if execution_lock_manager and not enqueue_lock_released:
            _release_enqueue_lock(execution_lock_manager, pipeline_id)
            enqueue_lock_released = True
        
        # Send failure status
        await _send_status(
            scheduler_url, pipeline_id, strategy_name, run_id,
            "failed", error_msg, error=str(e)
        )
        
        return {
            "status": "failed",
            "error": str(e),
            "worker": WORKER_ID
        }


def _release_enqueue_lock(execution_lock_manager: ExecutionLockManager, pipeline_id: str):
    """Release the pipeline enqueue lock acquired by scheduler"""
    try:
        lock_key = f"synctool:enqueue:pipeline:{pipeline_id}"
        lua_script = "return redis.call('DEL', KEYS[1])"
        execution_lock_manager.redis_client.eval(lua_script, 1, lock_key)
        logger.info(f"Released enqueue lock for pipeline {pipeline_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to release enqueue lock for {pipeline_id}: {e}")
        return False


def _get_strategy_config(config, strategy_name: str):
    """Get strategy config from pipeline config"""
    for stage in config.stages:
        for strategy in stage.strategies:
            if strategy.name == strategy_name:
                return strategy
    return None


async def _send_status(
    scheduler_url: str,
    pipeline_id: str,
    strategy_name: str,
    run_id: str,
    status: str,
    message: str,
    error: Optional[str] = None
):
    """Send status update to scheduler"""
    try:
        timeout = aiohttp.ClientTimeout(total=5.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            payload = {
                "pipeline_id": pipeline_id,
                "strategy_name": strategy_name,
                "run_id": run_id,
                "worker": WORKER_ID,
                "status": status,
                "message": message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            if error:
                payload["error"] = error
            
            async with session.post(
                f"{scheduler_url}/api/worker/status",
                json=payload
            ) as response:
                response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send status to scheduler: {e}")


# ARQ Worker class configuration
class WorkerSettings:
    """Enhanced ARQ worker settings"""
    functions = [execute_pipeline_strategy]
    redis_settings = {
        'host': 'localhost',
        'port': 6379,
        'database': 0
    }
    max_jobs = 4
    job_timeout = 3600
    keep_result = 3600
    max_tries = 3

