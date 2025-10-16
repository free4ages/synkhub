import asyncio
import click
import logging
from ..scheduler.enhanced_arq_scheduler import EnhancedARQScheduler
from ..config.config_manager import ConfigManager
from ..config.global_config_loader import load_global_config


@click.group()
def scheduler():
    """ARQ-based scheduler commands"""
    pass


@scheduler.command()
@click.option('--config-dir', required=True, help='Pipeline config directory')
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', help='Log level')
def start(
    config_dir: str,
    global_config: str,
    log_level: str
):
    """Start the ARQ scheduler"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting ARQ-based scheduler...")
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
        logger.info(f"Loaded global config from: {global_config}")
    else:
        global_cfg = load_global_config()
        logger.info("Using default global config")
    
    logger.info(f"Redis: {global_cfg.redis.url}")
    logger.info(f"HTTP Port: {global_cfg.scheduler.http_port}")
    logger.info(f"Schedule Interval: {global_cfg.scheduler.schedule_interval}s")
    logger.info(f"Storage Dirs - Metrics: {global_cfg.storage.metrics_dir}, Logs: {global_cfg.storage.logs_dir}, State: {global_cfg.storage.state_dir}")
    
    # Create config manager
    config_manager = ConfigManager()
    config_manager.add_file_store('default', config_dir, is_primary=True)
    
    # Create and start scheduler
    scheduler_instance = EnhancedARQScheduler(config_manager, global_cfg)
    
    try:
        asyncio.run(scheduler_instance.start())
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        asyncio.run(scheduler_instance.stop())


@scheduler.command()
@click.option('--config-dir', required=True, help='Config directory')
def list_jobs(config_dir: str):
    """List all configured jobs"""
    logging.basicConfig(level=logging.INFO)
    
    from ..config.config_manager import ConfigManager
    
    config_manager = ConfigManager()
    config_manager.add_file_store('default', config_dir, is_primary=True)
    
    async def list_configs():
        await config_manager.initialize_database_stores()
        configs_by_store = await config_manager.list_pipeline_configs()
        
        total = 0
        for store_name, configs in configs_by_store.items():
            print(f"\n{store_name}:")
            for config_meta in configs:
                print(f"  - {config_meta.name}")
                total += 1
        
        print(f"\nTotal: {total} pipeline(s)")
        
        await config_manager.close()
    
    asyncio.run(list_configs())


@scheduler.command()
@click.option('--global-config', default=None, help='Path to global config YAML')
def status(global_config: str):
    """Show status of all pipelines"""
    from ..scheduler.pipeline_state_manager import PipelineStateManager
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
    else:
        global_cfg = load_global_config()
    
    state_manager = PipelineStateManager(state_dir=global_cfg.storage.state_dir)
    states = state_manager.list_all_states()
    
    if not states:
        print("No pipeline states found")
        return
    
    print(f"\n{'Pipeline':<30} {'Strategy':<20} {'Status':<15} {'Last Run':<25}")
    print("-" * 90)
    
    for pipeline_id, state in states.items():
        last_run = state.last_run if state.last_run else "Never"
        print(f"{pipeline_id:<30} {state.strategy:<20} {state.status:<15} {last_run:<25}")
    
    print(f"\nTotal: {len(states)} pipeline(s)")


@scheduler.command()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.argument('pipeline_id')
def history(global_config: str, pipeline_id: str):
    """Show run history for a pipeline"""
    from ..scheduler.pipeline_state_manager import PipelineStateManager
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
    else:
        global_cfg = load_global_config()
    
    state_manager = PipelineStateManager(state_dir=global_cfg.storage.state_dir)
    history = state_manager.get_run_history(pipeline_id, limit=20)
    
    if not history:
        print(f"No history found for pipeline: {pipeline_id}")
        return
    
    print(f"\nRun history for: {pipeline_id}")
    print(f"\n{'Run ID':<40} {'Strategy':<20} {'Status':<15} {'Time':<25}")
    print("-" * 100)
    
    for run in history:
        time_str = run.updated_at if run.updated_at else "N/A"
        print(f"{run.run_id:<40} {run.strategy:<20} {run.status:<15} {time_str:<25}")


if __name__ == '__main__':
    scheduler()

