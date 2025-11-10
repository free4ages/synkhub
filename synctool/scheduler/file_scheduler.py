import asyncio
import logging
import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional
from croniter import croniter
from datetime import datetime, timedelta

from ..config.config_loader import ConfigLoader
from ..core.models import PipelineJobConfig, SchedulerConfig, DataStorage, DataStore
from ..sync.sync_job_manager import SyncJobManager
from ..monitoring.metrics_storage import MetricsStorage
from ..monitoring.logs_storage import LogsStorage
from .redis_lock_manager import RedisLockManager


class FileBasedScheduler:
    """File-based scheduler that reads YAML configs and schedules sync jobs with cron expressions"""
    
    def __init__(self, scheduler_config: SchedulerConfig):
        self.config = scheduler_config
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
        
        # Job manager will be created after datastores are loaded
        self.job_manager: Optional[SyncJobManager] = None
        
        # Track last run times for each strategy
        self.last_run_times: Dict[str, datetime] = {}
    
    async def start(self):
        """Start the scheduler"""
        self.running = True
        self.logger.info(f"Starting file-based scheduler, reading configs from: {self.config.config_dir}")
        
        # Load datastores first
        await self.load_datastores()
        
        # Create job manager now that datastores are loaded
        self.logger.info(f"Creating SyncJobManager with data_storage: {'Available' if self.data_storage else 'None'}")
        if self.data_storage:
            self.logger.info(f"DataStorage contains {len(self.data_storage.datastores)} datastores: {list(self.data_storage.datastores.keys())}")
        
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
        self.logger.info("Stopping file-based scheduler")
    
    async def load_datastores(self):
        """Load datastores configuration from datastores.yaml"""
        config_dir = Path(self.config.config_dir)
        datastores_file = config_dir / "datastores.yaml"
        
        if not datastores_file.exists():
            self.logger.warning(f"Datastores config file does not exist: {datastores_file}")
            self.logger.warning("Creating empty DataStorage - sync jobs may fail without datastore references")
            self.data_storage = DataStorage()
            return
        
        try:
            self.data_storage = ConfigLoader.load_datastores_from_yaml(str(datastores_file))
            
            # Validate datastores config
            issues = ConfigLoader.validate_datastores_config(self.data_storage)
            if issues:
                self.logger.error(f"Datastores validation failed: {issues}")
                return
            
            self.logger.info(f"Loaded {len(self.data_storage.datastores)} datastores: {list(self.data_storage.datastores.keys())}")
            
        except Exception as e:
            self.logger.error(f"Failed to load datastores from {datastores_file}: {e}")
            self.data_storage = DataStorage()
    
    async def load_configs(self):
        """Load all YAML config files from the config directory"""

        await self.load_datastores()
        
        config_dir = Path(self.config.config_dir)
        if not config_dir.exists():
            self.logger.warning(f"Config directory does not exist: {config_dir}")
            return
        
        self.job_configs.clear()
        
        for config_file in config_dir.glob("*.yaml"):
            # Skip datastores.yaml as it's handled separately
            if config_file.name == "datastores.yaml":
                continue
                
            try:
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                # Load config using existing ConfigLoader
                if config_data is None:
                    self.logger.error(f"Empty or invalid YAML file: {config_file}")
                    continue
                job_config = ConfigLoader.load_from_dict(config_data)
                
                # Validate config
                issues = ConfigLoader.validate_config(job_config)
                if issues:
                    self.logger.error(f"Config validation failed for {config_file}: {issues}")
                    continue
                
                # Validate that referenced datastores exist
                datastore_issues = ConfigLoader.validate_datastore_references(job_config, self.data_storage)
                if datastore_issues:
                    self.logger.error(f"Datastore validation failed for {config_file}: {datastore_issues}")
                    continue
                
                self.job_configs[job_config.name] = job_config
                self.logger.info(f"Loaded config: {job_config.name}")
                
            except Exception as e:
                self.logger.error(f"Failed to load config from {config_file}: {e}")
    

    
    async def _schedule_jobs(self):
        """Check and schedule jobs based on cron expressions"""
        current_time = datetime.now()
        
        for job_name, job_config in self.job_configs.items():
            # Iterate through stages to find strategies
            for stage in job_config.stages:
                for strategy_config in stage.strategies:
                    if not strategy_config.enabled:
                        continue
                    
                    cron_expr = strategy_config.cron
                    if not cron_expr:
                        continue
                    
                    strategy_name = strategy_config.name
                    strategy_key = f"{job_name}:{strategy_name}"
                    
                    # Check if it's time to run this strategy
                    if self._should_run_strategy(strategy_key, cron_expr, current_time):
                        # Schedule the job (job_manager handles locking)
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
    
    def get_datastore(self, datastore_name: str) -> Optional[DataStore]:
        """Get a specific datastore by name"""
        if self.data_storage:
            return self.data_storage.get_datastore(datastore_name)
        return None
