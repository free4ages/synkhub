import asyncio
import logging
import aiohttp
from typing import Dict, Any, Optional
import os
import socket
from datetime import datetime

from ..config.config_serializer import ConfigSerializer
from ..config.global_config_loader import get_global_config
from ..sync.sync_job_manager import SyncJobManager
from ..monitoring.http_metrics_storage import HttpMetricsStorage
from ..monitoring.http_logs_storage import BufferedHttpLogsStorage

# Get worker identifier
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

logger = logging.getLogger(__name__)


async def execute_pipeline_job(
    ctx: dict,
    config_json: Dict[str, Any],
    strategy_name: str,
    run_id: str,
    pipeline_id: str,
    scheduler_url: str
) -> Dict[str, Any]:
    """
    ARQ worker function to execute a pipeline job.
    Uses HTTP-backed storage to seamlessly send logs and metrics to scheduler.
    
    Args:
        ctx: ARQ context
        config_json: JSON-serialized config and datastores
        strategy_name: Strategy to execute
        run_id: Unique run ID
        pipeline_id: Pipeline identifier
        scheduler_url: URL of scheduler's HTTP endpoint
    
    Returns:
        Result dictionary
    """
    
    # Load global config
    global_config = get_global_config()
    
    # Send initial status update
    await _send_status(scheduler_url, pipeline_id, strategy_name, run_id, "running", "Starting pipeline execution")
    
    # Initialize storage instances that will be used
    logs_storage = None
    
    try:
        # Deserialize config from JSON
        config, data_storage = ConfigSerializer.json_dict_to_config(config_json)
        
        logger.info(f"Worker {WORKER_ID} executing {pipeline_id}:{strategy_name} (run_id: {run_id})")
        
        # Create HTTP-backed storage that sends data to scheduler (using global config)
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
        
        # Create job manager with HTTP-backed storage
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=metrics_storage,
            logs_storage=logs_storage,
            data_storage=data_storage
        )
        
        # Execute the job
        result = await job_manager.run_sync_job(
            config=config,
            strategy_name=strategy_name,
            use_locking=False
        )
        
        # Force flush any remaining logs
        if logs_storage:
            logs_storage.flush()
        
        # Send final status update
        await _send_status(scheduler_url, pipeline_id, strategy_name, run_id, "success", "Completed successfully")
        
        logger.info(f"Worker {WORKER_ID} completed {pipeline_id}:{strategy_name}")
        
        return {
            "status": "success",
            "result": result,
            "worker": WORKER_ID
        }
        
    except Exception as e:
        error_msg = f"Pipeline execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # Try to flush any buffered logs before failing
        try:
            if logs_storage:
                logs_storage.flush()
        except:
            pass
        
        # Send failure status
        await _send_status(scheduler_url, pipeline_id, strategy_name, run_id, "failed", error_msg)
        
        return {
            "status": "failed",
            "error": str(e),
            "worker": WORKER_ID
        }


async def _send_status(
    scheduler_url: str,
    pipeline_id: str,
    strategy: str,
    run_id: str,
    status: str,
    message: str
):
    """Send status update to scheduler"""
    try:
        timeout = aiohttp.ClientTimeout(total=5.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{scheduler_url}/api/worker/status",
                json={
                    "pipeline_id": pipeline_id,
                    "strategy": strategy,
                    "run_id": run_id,
                    "worker": WORKER_ID,
                    "status": status,
                    "message": message,
                    "timestamp": datetime.now().isoformat()
                }
            ) as response:
                response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send status to scheduler: {e}")


# ARQ Worker class configuration
class WorkerSettings:
    """ARQ worker settings"""
    functions = [execute_pipeline_job]
    redis_settings = {
        'host': 'localhost',
        'port': 6379,
        'database': 0
    }
    max_jobs = 4
    job_timeout = 3600
    keep_result = 3600

