#!/usr/bin/env python3
"""
Synctool Worker CLI

ARQ worker for executing pipeline jobs from the scheduler.
"""

import asyncio
import click
import logging
import sys
from arq import Worker
from arq.connections import RedisSettings
from urllib.parse import urlparse


@click.group()
def worker_cli():
    """ARQ Worker CLI - Execute pipeline jobs from scheduler"""
    pass


@worker_cli.command()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set the logging level')
def start(global_config: str, log_level: str):
    """Start an ARQ worker"""
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    logger = logging.getLogger(__name__)
    
    # Load global config
    from ..config.global_config_loader import load_global_config
    
    if global_config:
        global_cfg = load_global_config(global_config)
        logger.info(f"Loaded global config from: {global_config}")
    else:
        global_cfg = load_global_config()
        logger.info("Using default global config")
    
    # Display configuration
    logger.info("=" * 80)
    logger.info("ARQ Worker Configuration")
    logger.info("=" * 80)
    logger.info(f"Max Jobs: {global_cfg.worker.max_jobs}")
    logger.info(f"Job Timeout: {global_cfg.worker.job_timeout}s")
    logger.info(f"Redis URL: {global_cfg.redis.url}")
    logger.info(f"Redis Database: {global_cfg.redis.database}")
    logger.info(f"Storage - Metrics: {global_cfg.storage.metrics_dir}")
    logger.info(f"Storage - Logs: {global_cfg.storage.logs_dir}")
    logger.info(f"Storage - State: {global_cfg.storage.state_dir}")
    logger.info(f"HTTP Timeout: {global_cfg.http.timeout}s")
    logger.info(f"HTTP Max Retries: {global_cfg.http.max_retries}")
    logger.info(f"Log Batch Size: {global_cfg.log_batching.batch_size}")
    logger.info(f"Log Flush Interval: {global_cfg.log_batching.flush_interval}s")
    logger.info(f"Log Local Fallback: {global_cfg.log_batching.local_fallback}")
    logger.info("=" * 80)
    
    # Parse Redis URL
    parsed = urlparse(global_cfg.redis.url)
    
    redis_settings = RedisSettings(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 6379,
        database=global_cfg.redis.database
    )
    


    from ..worker.enhanced_tasks import WorkerSettings
    logger.info("Using enhanced worker tasks (with execution locking)")

    
    # Update worker settings from global config
    WorkerSettings.redis_settings = redis_settings
    WorkerSettings.max_jobs = global_cfg.worker.max_jobs
    WorkerSettings.job_timeout = global_cfg.worker.job_timeout
    
    # Run worker - ARQ manages its own event loop
    async def async_main():
        """Async wrapper for worker initialization and run"""
        worker = Worker(
            WorkerSettings.functions,
            redis_settings=redis_settings,
            max_jobs=global_cfg.worker.max_jobs,
            job_timeout=global_cfg.worker.job_timeout,
            keep_result=getattr(WorkerSettings, 'keep_result', 3600)
        )
        logger.info("=" * 80)
        logger.info("Worker started successfully!")
        logger.info(f"Listening for jobs on Redis: {global_cfg.redis.url}")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Use async_run() instead of run() to avoid event loop conflicts
        await worker.async_run()
    
    try:
        # Run the async worker
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info("Worker stopped by user")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"Worker error: {e}", exc_info=True)
        sys.exit(1)


@worker_cli.command()
@click.option('--global-config', default=None, help='Path to global config YAML')
def info(global_config: str):
    """Display worker configuration information"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    logger = logging.getLogger(__name__)
    
    # Load global config
    from ..config.global_config_loader import load_global_config
    
    if global_config:
        global_cfg = load_global_config(global_config)
    else:
        global_cfg = load_global_config()
    
    # Display full configuration
    logger.info("\n" + "=" * 80)
    logger.info("ARQ Worker Configuration")
    logger.info("=" * 80)
    
    logger.info("\nRedis Configuration:")
    logger.info(f"  URL: {global_cfg.redis.url}")
    logger.info(f"  Database: {global_cfg.redis.database}")
    
    logger.info("\nWorker Configuration:")
    logger.info(f"  Max Jobs: {global_cfg.worker.max_jobs}")
    logger.info(f"  Job Timeout: {global_cfg.worker.job_timeout}s")
    
    logger.info("\nStorage Configuration:")
    logger.info(f"  Metrics Directory: {global_cfg.storage.metrics_dir}")
    logger.info(f"  Logs Directory: {global_cfg.storage.logs_dir}")
    logger.info(f"  State Directory: {global_cfg.storage.state_dir}")
    logger.info(f"  Max Runs Per Strategy: {global_cfg.storage.max_runs_per_strategy}")
    
    logger.info("\nHTTP Configuration:")
    logger.info(f"  Timeout: {global_cfg.http.timeout}s")
    logger.info(f"  Max Retries: {global_cfg.http.max_retries}")
    
    logger.info("\nLog Batching Configuration:")
    logger.info(f"  Batch Size: {global_cfg.log_batching.batch_size}")
    logger.info(f"  Flush Interval: {global_cfg.log_batching.flush_interval}s")
    logger.info(f"  Local Fallback: {global_cfg.log_batching.local_fallback}")
    
    logger.info("\nConfig Stores:")
    if global_cfg.config_manager.stores:
        for i, store_cfg in enumerate(global_cfg.config_manager.stores, 1):
            primary_marker = " (PRIMARY)" if store_cfg.is_primary else ""
            if store_cfg.type == "file":
                logger.info(f"  {i}. {store_cfg.name}: file @ {store_cfg.base_path}{primary_marker}")
            elif store_cfg.type == "database":
                logger.info(f"  {i}. {store_cfg.name}: {store_cfg.db_type} @ {store_cfg.db_host}:{store_cfg.db_port}/{store_cfg.db_name}{primary_marker}")
    else:
        logger.info("  No config stores configured")
    
    logger.info("\n" + "=" * 80 + "\n")


if __name__ == '__main__':
    worker_cli()
