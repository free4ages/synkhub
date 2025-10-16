"""
Enhanced ARQ worker tasks with strategy-level execution, locking, and retry management.
Handles pipeline-level locks, table DDL locks, and re-enqueue logic.
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
    Enhanced ARQ worker function to execute a pipeline strategy with robust locking and retry logic.
    
    Features:
    - Pipeline-level execution lock (only one strategy per pipeline at a time)
    - Table DDL locks (prevent conflicts during schema changes)
    - Automatic retry with re-enqueue on lock timeout
    - Strategy-level state updates
    - Dropped/skipped job tracking
    
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
        
        # Extract strategy settings
        wait_for_pipeline_lock = getattr(strategy_config, 'wait_for_pipeline_lock', False)
        pipeline_lock_wait_timeout = getattr(strategy_config, 'pipeline_lock_wait_timeout', 60)
        max_retry_on_lock_failure = getattr(strategy_config, 'max_retry_on_lock_failure', 2)
        wait_for_table_lock = getattr(strategy_config, 'wait_for_table_lock', False)
        table_lock_wait_timeout = getattr(strategy_config, 'table_lock_wait_timeout', 30)
        
        # Initialize execution lock manager
        execution_lock_manager = ExecutionLockManager(
            redis_url=global_config.redis.url,
            pipeline_lock_timeout=global_config.scheduler.lock_timeout
        )
        
        # Helper function to release enqueue lock
        def release_enqueue_lock():
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
        
        # Helper function to check if table has active DDL locks (not acquire, just check)
        def check_table_ddl_lock_available(datastore_name: str, schema_name: str, table_name: str) -> bool:
            """Check if table is free from DDL locks"""
            try:
                ddl_lock_key = f"synctool:exec:table:{datastore_name}:{schema_name}:{table_name}:ddl_change"
                has_ddl_lock = execution_lock_manager.redis_client.exists(ddl_lock_key) > 0
                return not has_ddl_lock
            except Exception as e:
                logger.warning(f"Failed to check table DDL lock: {e}")
                return True  # Assume available on error
        
        # Try to acquire pipeline-level execution lock
        async with execution_lock_manager.acquire_pipeline_lock(
            pipeline_id=pipeline_id,
            strategy_name=strategy_name,
            timeout=global_config.scheduler.lock_timeout,
            wait_for_lock=wait_for_pipeline_lock,
            wait_interval=1.0
        ) as pipeline_lock_acquired:
            
            if not pipeline_lock_acquired:
                # Could not acquire pipeline lock
                logger.warning(
                    f"Could not acquire pipeline lock for {pipeline_id}:{strategy_name}"
                )
                
                # Check if we should retry or skip
                retry_count = ctx.get('job_try', 1)
                
                if wait_for_pipeline_lock and retry_count < max_retry_on_lock_failure:
                    # Re-enqueue for retry
                    await _send_status(
                        scheduler_url, pipeline_id, strategy_name, run_id,
                        "pending", f"Re-enqueueing due to lock timeout (retry {retry_count})"
                    )
                    
                    # Notify scheduler to re-enqueue
                    await _request_reenqueue(
                        scheduler_url, pipeline_id, strategy_name,
                        run_id, retry_count, "pipeline_lock_timeout"
                    )
                    
                    return {
                        "status": "requeued",
                        "reason": "pipeline_lock_timeout",
                        "retry_count": retry_count,
                        "worker": WORKER_ID
                    }
                else:
                    # Skip this execution and release enqueue lock
                    skip_msg = "Pipeline lock not available, skipping execution"
                    logger.info(f"{pipeline_id}:{strategy_name} - {skip_msg}")
                    
                    # Release enqueue lock BEFORE sending status
                    release_enqueue_lock()
                    
                    await _send_status(
                        scheduler_url, pipeline_id, strategy_name, run_id,
                        "skipped", skip_msg
                    )
                    
                    return {
                        "status": "skipped",
                        "reason": "pipeline_lock_unavailable",
                        "worker": WORKER_ID
                    }
            
            # Pipeline lock acquired - release enqueue lock and send running status
            release_enqueue_lock()
            
            await _send_status(
                scheduler_url, pipeline_id, strategy_name, run_id,
                "running", "Execution started"
            )
            
            # Initialize storage instances
            metrics_storage = HttpMetricsStorage(
                scheduler_url=scheduler_url,
                metrics_dir=global_config.storage.metrics_dir,
                max_runs_per_job=global_config.storage.max_runs_per_job,
                max_retries=global_config.http.max_retries,
                local_fallback=global_config.log_batching.local_fallback
            )
            
            logs_storage = BufferedHttpLogsStorage(
                scheduler_url=scheduler_url,
                logs_dir=global_config.storage.logs_dir,
                max_runs_per_job=global_config.storage.max_runs_per_job,
                batch_size=global_config.log_batching.batch_size,
                flush_interval=global_config.log_batching.flush_interval,
                max_retries=global_config.http.max_retries,
                local_fallback=global_config.log_batching.local_fallback
            )
            
            # Get destination table info for DDL lock checking
            dest_info = _get_destination_table_info(config)
            
            # Check if table has active DDL locks (don't acquire, just check)
            if dest_info and dest_info.get('table'):
                datastore_name = dest_info['datastore']
                schema_name = dest_info.get('schema', 'public')
                table_name = dest_info['table']
                
                # Check if table DDL lock is available
                table_available = check_table_ddl_lock_available(
                    datastore_name, schema_name, table_name
                )
                
                if not table_available:
                    logger.warning(
                        f"Table {datastore_name}.{schema_name}.{table_name} has active DDL lock"
                    )
                    
                    if wait_for_table_lock:
                        # Wait for table lock to become available
                        logger.info(f"Waiting for table DDL lock to be released...")
                        max_wait_time = table_lock_wait_timeout
                        wait_time = 0
                        
                        while wait_time < max_wait_time:
                            await asyncio.sleep(1)
                            wait_time += 1
                            
                            if check_table_ddl_lock_available(datastore_name, schema_name, table_name):
                                logger.info(f"Table DDL lock released after {wait_time}s")
                                table_available = True
                                break
                        
                        if not table_available:
                            # Timeout waiting for table lock
                            skip_msg = f"Table DDL lock timeout after {max_wait_time}s, skipping execution"
                            await _send_status(
                                scheduler_url, pipeline_id, strategy_name, run_id,
                                "skipped", skip_msg
                            )
                            
                            return {
                                "status": "skipped",
                                "reason": "table_ddl_lock_timeout",
                                "worker": WORKER_ID
                            }
                    else:
                        # Skip execution if table is locked
                        skip_msg = "Table has active DDL lock, skipping execution"
                        await _send_status(
                            scheduler_url, pipeline_id, strategy_name, run_id,
                            "skipped", skip_msg
                        )
                        
                        return {
                            "status": "skipped",
                            "reason": "table_ddl_lock_active",
                            "worker": WORKER_ID
                        }
            
            # Table is available or no table info - execute the job
            result = await _execute_strategy(
                config, data_storage, strategy_name,
                metrics_storage, logs_storage
            )
            
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
        try:
            lock_key = f"synctool:enqueue:pipeline:{pipeline_id}"
            lua_script = "return redis.call('DEL', KEYS[1])"
            exec_lock_mgr = ExecutionLockManager(redis_url=get_global_config().redis.url)
            exec_lock_mgr.redis_client.eval(lua_script, 1, lock_key)
            logger.info(f"Released enqueue lock in error handler for {pipeline_id}")
        except:
            pass
        
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


async def _execute_strategy(
    config, data_storage, strategy_name,
    metrics_storage, logs_storage
) -> Dict[str, Any]:
    """Execute the actual sync job"""
    job_manager = SyncJobManager(
        max_concurrent_jobs=1,
        metrics_storage=metrics_storage,
        logs_storage=logs_storage,
        data_storage=data_storage
    )
    
    result = await job_manager.run_sync_job(
        config=config,
        strategy_name=strategy_name,
        use_locking=False  # We handle locking at worker level
    )
    
    return result


def _get_strategy_config(config, strategy_name: str):
    """Get strategy config from pipeline config"""
    for stage in config.stages:
        for strategy in stage.strategies:
            if strategy.name == strategy_name:
                return strategy
    return None


def _get_destination_table_info(config) -> Optional[Dict[str, str]]:
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
        logger.warning(f"Could not extract destination table info: {e}")
    
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
    """
    Send status update to scheduler.
    
    The state manager will automatically determine whether to update pipeline state
    by comparing run_ids.
    """
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


async def _request_reenqueue(
    scheduler_url: str,
    pipeline_id: str,
    strategy_name: str,
    run_id: str,
    retry_count: int,
    reason: str
):
    """Request scheduler to re-enqueue this job"""
    try:
        timeout = aiohttp.ClientTimeout(total=5.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{scheduler_url}/api/worker/reenqueue",
                json={
                    "pipeline_id": pipeline_id,
                    "strategy_name": strategy_name,
                    "run_id": run_id,
                    "retry_count": retry_count,
                    "reason": reason,
                    "worker": WORKER_ID,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            ) as response:
                response.raise_for_status()
                logger.info(f"Requested re-enqueue for {pipeline_id}:{strategy_name}")
    except Exception as e:
        logger.error(f"Failed to request re-enqueue: {e}")


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

