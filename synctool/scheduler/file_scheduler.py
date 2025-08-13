import asyncio
import logging
import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional
from croniter import croniter
from datetime import datetime, timedelta

from ..config.config_loader import ConfigLoader
from ..core.models import SyncJobConfig, SchedulerConfig
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
        self.job_configs: Dict[str, SyncJobConfig] = {}
        
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
        
        # Create job manager with metrics, logging and locking
        self.job_manager = SyncJobManager(
            max_concurrent_jobs=4,
            metrics_storage=self.metrics_storage,
            lock_manager=self.lock_manager,
            logs_storage=self.logs_storage
        )
        
        # Track last run times for each strategy
        self.last_run_times: Dict[str, datetime] = {}
    
    async def start(self):
        """Start the scheduler"""
        self.running = True
        self.logger.info(f"Starting file-based scheduler, reading configs from: {self.config.config_dir}")
        
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
    
    async def load_configs(self):
        """Load all YAML config files from the config directory"""
        config_dir = Path(self.config.config_dir)
        if not config_dir.exists():
            self.logger.warning(f"Config directory does not exist: {config_dir}")
            return
        
        self.job_configs.clear()
        
        for config_file in config_dir.glob("*.yaml"):
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
                
                self.job_configs[job_config.name] = job_config
                self.logger.info(f"Loaded config: {job_config.name}")
                
            except Exception as e:
                self.logger.error(f"Failed to load config from {config_file}: {e}")
    
    async def _schedule_jobs(self):
        """Check and schedule jobs based on cron expressions"""
        current_time = datetime.now()
        
        for job_name, job_config in self.job_configs.items():
            for strategy_config in job_config.strategies:
                if not strategy_config.get('enabled', True):
                    continue
                
                cron_expr = strategy_config.get('cron')
                if not cron_expr:
                    continue
                
                strategy_name = strategy_config['name']
                strategy_key = f"{job_name}:{strategy_name}"
                
                # Check if it's time to run this strategy
                if self._should_run_strategy(strategy_key, cron_expr, current_time):
                    # Schedule the job (job_manager handles locking)
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
    
    async def _run_job(self, job_config: SyncJobConfig, strategy_name: str):
        """Run a single job with the specified strategy"""
        job_name = job_config.name
        strategy_key = f"{job_name}:{strategy_name}"
        
        try:
            # Run the job - job_manager handles locking and metrics
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

    
    def get_job_configs(self) -> Dict[str, SyncJobConfig]:
        """Get all loaded job configurations"""
        return self.job_configs.copy()
    
    def get_job_config(self, job_name: str) -> Optional[SyncJobConfig]:
        """Get a specific job configuration"""
        return self.job_configs.get(job_name)
