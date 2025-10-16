import asyncio
import logging
import click
from arq import Worker
from arq.connections import RedisSettings
from urllib.parse import urlparse
from ..config.global_config_loader import load_global_config


@click.group()
def worker_cli():
    """Worker CLI for running ARQ workers"""
    pass


@worker_cli.command()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', help='Log level')
def start(global_config: str, log_level: str):
    """Start an ARQ worker"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
        logger.info(f"Loaded global config from: {global_config}")
    else:
        global_cfg = load_global_config()
        logger.info("Using default global config")
    
    logger.info(f"Starting ARQ worker with max_jobs={global_cfg.worker.max_jobs}")
    logger.info(f"Redis: {global_cfg.redis.url}")
    
    # Parse Redis URL
    parsed = urlparse(global_cfg.redis.url)
    
    redis_settings = RedisSettings(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 6379,
        database=global_cfg.redis.database
    )
    
    # Import worker settings
    from .tasks import WorkerSettings
    
    # Update worker settings from global config
    WorkerSettings.redis_settings = redis_settings
    WorkerSettings.max_jobs = global_cfg.worker.max_jobs
    WorkerSettings.job_timeout = global_cfg.worker.job_timeout
    
    # Run worker
    async def run_worker():
        worker = Worker(
            WorkerSettings.functions,
            redis_settings=redis_settings,
            max_jobs=global_cfg.worker.max_jobs,
            job_timeout=global_cfg.worker.job_timeout,
            keep_result=WorkerSettings.keep_result
        )
        logger.info(f"Worker started, listening for jobs...")
        await worker.run()
    
    asyncio.run(run_worker())


if __name__ == '__main__':
    worker_cli()

