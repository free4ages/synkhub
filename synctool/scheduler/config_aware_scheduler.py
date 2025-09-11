import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from croniter import croniter

from ..config.config_manager import ConfigManager, ConfigMetadata
from ..config.config_loader import ConfigLoader
from ..core.models import PipelineJobConfig, SchedulerConfig, DataStorage
from ..sync.sync_job_manager import SyncJobManager
from ..monitoring.metrics_storage import MetricsStorage
from ..monitoring.logs_storage import LogsStorage
from .redis_lock_manager import RedisLockManager


class ConfigAwareScheduler:
    """Enhanced scheduler that works with ConfigManager for flexible config storage"""
    
    def __init__(self, scheduler_config: SchedulerConfig, config_manager: ConfigManager):
        self.config = scheduler_config
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.job_configs: Dict[str, PipelineJobConfig] = {}
        self.data_storage: Optional[DataStorage] = None
        
        # Initialize components
        self.metrics_storage = MetricsStorage(
            metrics_dir=scheduler_config.metrics_dir,
            max_runs_per_job=scheduler_config.max_runs_per_job
        )
        self.logs_storage = LogsStorage(
            logs_dir=scheduler_config.logs_dir,
            max_runs_per_job=scheduler_config.max_runs_per_job
        )
        self.lock_manager = RedisLockManager(
            redis_url=scheduler_config.redis_url,
            lock_timeout=scheduler_config.lock_timeout
        )
        
        self.job_manager: Optional[SyncJobManager] = None
        self.last_run_times: Dict[str, datetime] = {}
    
    async def start(self):
        """Start the scheduler"""
        self.running = True
        self.logger.info("Starting config-aware scheduler")
        
        # Initialize database stores if any
        await self.config_manager.initialize_database_stores()
        
        # Load datastores from ConfigManager
        await self.load_datastores()
        
        # Create job manager
        self.job_manager = SyncJobManager(
            max_concurrent_jobs=4,
            metrics_storage=self.metrics_storage,
            lock_manager=self.lock_manager,
            logs_storage=self.logs_storage,
            data_storage=self.data_storage
        )
        
        # Load initial configs
        await self.load_configs()
        
        # Start scheduling loop
        while self.running:
            try:
                await self._schedule_jobs()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)
    
    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        self.logger.info("Stopping config-aware scheduler")
        
        # Close config manager connections
        await self.config_manager.close()
    
    async def load_datastores(self):
        """Load datastores configuration from ConfigManager"""
        try:
            self.data_storage = await self.config_manager.load_datastores_config()
            
            if self.data_storage:
                self.logger.info(f"Loaded {len(self.data_storage.datastores)} datastores: {list(self.data_storage.datastores.keys())}")
            else:
                self.logger.warning("No datastores configuration found - creating empty DataStorage")
                self.data_storage = DataStorage()
                
        except Exception as e:
            self.logger.error(f"Failed to load datastores: {e}")
            self.data_storage = DataStorage()
    
    async def load_configs(self):
        """Load all pipeline configurations from ConfigManager"""
        try:
            self.job_configs.clear()
            
            # Get configs from all stores
            configs_by_store = await self.config_manager.list_pipeline_configs()
            
            # Track unique configs (in case same config exists in multiple stores)
            loaded_configs = set()
            
            for store_name, config_metas in configs_by_store.items():
                self.logger.info(f"Loading {len(config_metas)} configs from store: {store_name}")
                
                for config_meta in config_metas:
                    # Skip if already loaded from another store
                    if config_meta.name in loaded_configs:
                        self.logger.debug(f"Config '{config_meta.name}' already loaded from another store, skipping")
                        continue
                    
                    try:
                        config = await self.config_manager.load_pipeline_config(
                            config_meta.name, store_name
                        )
                        
                        if config:
                            # Validate datastore references
                            issues = ConfigLoader.validate_datastore_references(config, self.data_storage)
                            if issues:
                                self.logger.error(f"Datastore validation failed for {config.name}: {issues}")
                                continue
                            
                            self.job_configs[config.name] = config
                            loaded_configs.add(config.name)
                            self.logger.info(f"Loaded config: {config.name} from {store_name}")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to load config {config_meta.name} from {store_name}: {e}")
            
            self.logger.info(f"Total loaded configs: {len(self.job_configs)}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configs: {e}")
    
    async def reload_configs(self):
        """Reload all configurations"""
        self.logger.info("Reloading configurations...")
        await self.load_datastores()
        await self.load_configs()
        self.logger.info("Configuration reload completed")
    
    async def add_config_to_store(self, config: PipelineJobConfig, store_name: str = None,
                                 metadata: ConfigMetadata = None) -> bool:
        """Add a new configuration to a store"""
        try:
            result = await self.config_manager.save_pipeline_config(config, metadata, store_name)
            if result:
                # Reload configs to include the new one
                await self.load_configs()
                self.logger.info(f"Added config '{config.name}' to scheduler")
            return result
        except Exception as e:
            self.logger.error(f"Failed to add config '{config.name}': {e}")
            return False
    
    async def remove_config_from_stores(self, name: str, store_name: str = None) -> Dict[str, bool]:
        """Remove a configuration from stores"""
        try:
            results = await self.config_manager.delete_pipeline_config(name, store_name)
            
            # Remove from active configs if successful
            if any(results.values()) and name in self.job_configs:
                del self.job_configs[name]
                self.logger.info(f"Removed config '{name}' from scheduler")
            
            return results
        except Exception as e:
            self.logger.error(f"Failed to remove config '{name}': {e}")
            return {}
    

    
    async def _schedule_jobs(self):
        """Check and schedule jobs based on cron expressions"""
        current_time = datetime.now()
        
        for job_name, job_config in self.job_configs.items():
            for stage in job_config.stages:
                for strategy_config in stage.strategies:
                    if not strategy_config.enabled:
                        continue
                    
                    cron_expr = strategy_config.cron
                    if not cron_expr:
                        continue
                    
                    strategy_name = strategy_config.name
                    strategy_key = f"{job_name}:{strategy_name}"
                    
                    if self._should_run_strategy(strategy_key, cron_expr, current_time):
                        if self.job_manager:
                            asyncio.create_task(self._run_job(job_config, strategy_name))
    
    def _should_run_strategy(self, strategy_key: str, cron_expr: str, current_time: datetime) -> bool:
        """Check if a strategy should run based on its cron expression"""
        try:
            cron = croniter(cron_expr, current_time)
            next_run = cron.get_prev(datetime)
            
            # Check if we've already run since the last scheduled time
            last_run = self.last_run_times.get(strategy_key)
            if last_run and last_run >= next_run:
                return False
            
            # Check if the next run time is within the last minute
            return (current_time - next_run).total_seconds() < 60
            
        except Exception as e:
            self.logger.error(f"Invalid cron expression '{cron_expr}' for {strategy_key}: {e}")
            return False
    
    async def _run_job(self, job_config: PipelineJobConfig, strategy_name: str):
        """Run a single job with the specified strategy"""
        job_name = job_config.name
        strategy_key = f"{job_name}:{strategy_name}"
        
        try:
            # Run the job - job_manager handles locking and metrics
            if not self.job_manager:
                self.logger.error(f"Job manager not initialized for {job_name}:{strategy_name}")
                return
                
            result = await self.job_manager.run_sync_job(
                config=job_config,
                strategy_name=strategy_name,
                use_locking=True  # Enable locking for scheduled jobs
            )
            
            # Update last run time only if job actually ran (wasn't skipped due to lock)
            if result.get('status') != 'skipped':
                self.last_run_times[strategy_key] = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Failed to run scheduled job {job_name}:{strategy_name}: {e}")
    
    def get_job_configs(self) -> Dict[str, PipelineJobConfig]:
        """Get all loaded job configurations"""
        return self.job_configs.copy()
    
    def get_job_config(self, job_name: str) -> Optional[PipelineJobConfig]:
        """Get a specific job configuration"""
        return self.job_configs.get(job_name)
    
    def get_data_storage(self) -> Optional[DataStorage]:
        """Get the loaded DataStorage configuration"""
        return self.data_storage
    
    def get_datastore(self, datastore_name: str) -> Optional:
        """Get a specific datastore by name"""
        if self.data_storage:
            return self.data_storage.get_datastore(datastore_name)
        return None
    
    def get_config_manager(self) -> ConfigManager:
        """Get the config manager instance"""
        return self.config_manager
    
    async def backup_configs_to_store(self, target_store: str, source_store: str = None, 
                                     tags: List[str] = None) -> Dict[str, bool]:
        """Backup configurations to another store"""
        if source_store:
            return await self.config_manager.backup_configs(source_store, target_store, tags)
        else:
            # Backup from primary store
            primary_store = self.config_manager.get_primary_store_name()
            if primary_store:
                return await self.config_manager.backup_configs(primary_store, target_store, tags)
            else:
                self.logger.error("No primary store configured for backup")
                return {}
    
    async def get_config_status(self) -> Dict[str, Any]:
        """Get status information about loaded configurations"""
        store_configs = await self.config_manager.list_pipeline_configs()
        
        status = {
            'loaded_configs': len(self.job_configs),
            'active_configs': len([c for c in self.job_configs.values() 
                                 if any(s.enabled for stage in c.stages for s in stage.strategies)]),
            'datastores': len(self.data_storage.datastores) if self.data_storage else 0,
            'stores': {
                'names': self.config_manager.get_store_names(),
                'primary': self.config_manager.get_primary_store_name(),
                'configs_per_store': {name: len(configs) for name, configs in store_configs.items()}
            },
            'last_reload': datetime.now().isoformat()
        }
        
        return status
