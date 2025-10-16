"""
Enhanced ARQ scheduler with strategy-level state management and robust locking.
Handles multiple strategies per pipeline independently with proper enqueue locks.
"""

import asyncio
import logging
import json
import uuid
from typing import Dict, Optional
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI, Request
from pydantic import BaseModel
import uvicorn

from ..config.config_manager import ConfigManager
from ..config.global_config_loader import get_global_config, GlobalConfig
from ..core.models import DataStorage, PipelineJobConfig
from ..config.config_serializer import ConfigSerializer
from .pipeline_state_manager import PipelineStateManager, PipelineRunState, StrategyRunState
from .enhanced_strategy_selector import EnhancedStrategySelector
from .execution_lock_manager import ExecutionLockManager


class StatusUpdate(BaseModel):
    """Status update from worker"""
    pipeline_id: str
    strategy_name: str
    run_id: str
    worker: str
    status: str
    message: str
    timestamp: str
    error: Optional[str] = None


class EnhancedARQScheduler:
    """
    Enhanced scheduler with strategy-level state management.
    Handles multiple strategies per pipeline with proper locking.
    """
    
    def __init__(
        self,
        config_manager: ConfigManager,
        global_config: Optional[GlobalConfig] = None
    ):
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
        self.running = False
        
        # Load global config
        if global_config is None:
            global_config = get_global_config()
        self.global_config = global_config
        
        # Initialize pipeline state manager with strategy-level support
        self.state_manager = PipelineStateManager(
            state_dir=global_config.storage.state_dir,
            max_runs_per_pipeline=global_config.storage.max_runs_per_job
        )
        
        # Initialize enhanced strategy selector
        self.strategy_selector = EnhancedStrategySelector(
            state_manager=self.state_manager,
            safe_fallback_window_minutes=30,
            max_retry_count=3,
            schedule_buffer_seconds=60
        )
        
        # # Initialize enqueue lock manager
        # self.enqueue_lock_manager = RedisLockManager(
        #     redis_url=global_config.redis.url,
        #     lock_timeout=300  # 5 minutes for enqueue lock
        # )
        self.enqueue_lock_manager = ExecutionLockManager(
            redis_url=global_config.redis.url,
            pipeline_lock_timeout=300  # 5 minutes for enqueue lock
        )
        
        # ARQ connection
        self.redis_pool = None
        self.job_configs: Dict[str, PipelineJobConfig] = {}
        self.data_storage: Optional[DataStorage] = None
        
        # HTTP server for worker updates
        self.app = FastAPI(title="Enhanced Scheduler API")
        self._setup_http_endpoints()
        self.http_port = global_config.scheduler.http_port
    
    def _setup_http_endpoints(self):
        """Setup HTTP endpoints for worker communication"""
        
        @self.app.post("/api/worker/status")
        async def receive_status_update(update: StatusUpdate):
            """
            Receive status update from worker.
            
            State manager automatically determines whether to update pipeline state
            by comparing run_ids. If worker's run_id matches current pipeline run_id,
            both pipeline and strategy state are updated. Otherwise, only strategy state.
            """
            self.logger.info(
                f"Status update from {update.worker}: "
                f"{update.pipeline_id}:{update.strategy_name} -> {update.status}"
            )
            
            # Always update strategy state
            state = self.state_manager.get_current_state(
                update.pipeline_id,
                update.strategy_name
            )
            
            if not state:
                state = StrategyRunState(
                    strategy=update.strategy_name,
                    status=update.status
                )
            
            # Update state fields
            state.status = update.status
            state.run_id = update.run_id
            state.worker = update.worker
            state.message = update.message
            
            if update.error:
                state.error = update.error
            
            # Update timestamps based on status
            if update.status == "running":
                state.last_attempted_at = update.timestamp
            elif update.status == "success":
                state.last_run = update.timestamp
                state.retry_count = 0
                state.error = None
            elif update.status == "failed":
                state.retry_count = state.retry_count + 1
            
            # State manager will auto-detect if pipeline state should be updated
            # by comparing run_ids
            self.state_manager.update_state(state, update.pipeline_id)
            
            return {"status": "ok"}
        
        @self.app.post("/api/worker/logs/batch")
        async def receive_log_batch(request: Request):
            """Receive batch of log entries from worker"""
            try:
                data = await request.json()
                logs = data.get("logs", [])
                
                for log_entry in logs:
                    job_name = log_entry.get("job_name")
                    run_id = log_entry.get("run_id")
                    timestamp = log_entry.get("timestamp")
                    level = log_entry.get("level")
                    message = log_entry.get("message")
                    logger_name = log_entry.get("logger")
                    
                    if not all([job_name, run_id, timestamp, level, message]):
                        continue
                    
                    # Write to log file
                    log_dir = Path(self.global_config.storage.logs_dir) / job_name
                    log_dir.mkdir(parents=True, exist_ok=True)
                    log_file = log_dir / f"{run_id}.log"
                    
                    log_line = {
                        "timestamp": timestamp,
                        "level": level,
                        "message": message,
                        "run_id": run_id
                    }
                    if logger_name:
                        log_line["logger"] = logger_name
                    
                    with open(log_file, 'a') as f:
                        f.write(json.dumps(log_line) + "\n")
                
                return {"status": "ok", "count": len(logs)}
            
            except Exception as e:
                self.logger.error(f"Failed to process log batch: {e}")
                return {"status": "error", "message": str(e)}
        
        @self.app.post("/api/worker/metrics")
        async def receive_metrics(request: Request):
            """Receive metrics from worker"""
            try:
                metrics_data = await request.json()
                job_name = metrics_data.get("job_name")
                run_id = metrics_data.get("run_id")
                
                if not job_name or not run_id:
                    return {"status": "error", "message": "Missing job_name or run_id"}
                
                metrics_dir = Path(self.global_config.storage.metrics_dir) / job_name
                metrics_dir.mkdir(parents=True, exist_ok=True)
                metrics_file = metrics_dir / f"{run_id}.json"
                
                with open(metrics_file, 'w') as f:
                    json.dump(metrics_data, f, indent=2)
                
                return {"status": "ok"}
            
            except Exception as e:
                self.logger.error(f"Failed to process metrics: {e}")
                return {"status": "error", "message": str(e)}
        
        @self.app.get("/api/pipelines/{pipeline_id}/strategies")
        async def list_pipeline_strategies(pipeline_id: str):
            """List all strategies and their states for a pipeline"""
            strategies = self.state_manager.get_all_strategies_for_pipeline(pipeline_id)
            return {
                "pipeline_id": pipeline_id,
                "strategies": {
                    name: state.to_dict()
                    for name, state in strategies.items()
                },
                "count": len(strategies)
            }
        
        @self.app.get("/api/pipelines/{pipeline_id}/strategies/{strategy_name}/history")
        async def get_strategy_history(pipeline_id: str, strategy_name: str, limit: int = 20):
            """Get run history for a specific strategy"""
            history = self.state_manager.get_run_history(pipeline_id, strategy_name, limit)
            return {
                "pipeline_id": pipeline_id,
                "strategy_name": strategy_name,
                "history": [h.to_dict() for h in history],
                "count": len(history)
            }
        
        @self.app.get("/api/strategies/all")
        async def list_all_strategies():
            """List all strategies across all pipelines"""
            all_strategies = self.state_manager.list_all_states()
            return {
                "pipelines": {
                    pipeline_id: {
                        name: state.to_dict()
                        for name, state in strategies.items()
                    }
                    for pipeline_id, strategies in all_strategies.items()
                },
                "pipeline_count": len(all_strategies)
            }
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "scheduler": "running" if self.running else "stopped",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def start(self):
        """Start the enhanced scheduler"""
        self.running = True
        self.logger.info("Starting Enhanced ARQ Scheduler with strategy-level state")
        
        # Initialize Redis pool for ARQ
        parsed = urlparse(self.global_config.redis.url)
        redis_settings = RedisSettings(
            host=parsed.hostname or 'localhost',
            port=parsed.port or 6379,
            database=self.global_config.redis.database
        )
        self.redis_pool = await create_pool(redis_settings)
        
        # Load datastores and configs
        await self.config_manager.initialize_database_stores()
        await self.load_datastores()
        await self.load_configs()
        
        # Start HTTP server in background
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.http_port,
            log_level="info"
        )
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())
        
        self.logger.info(f"HTTP server started on port {self.http_port}")
        
        # Start scheduling loop
        while self.running:
            try:
                await self._schedule_jobs()
                await asyncio.sleep(self.global_config.scheduler.schedule_interval)
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                await asyncio.sleep(self.global_config.scheduler.schedule_interval)
    
    async def load_datastores(self):
        """Load datastores from config manager"""
        try:
            self.data_storage = await self.config_manager.load_datastores_config()
            if self.data_storage:
                self.logger.info(f"Loaded {len(self.data_storage.datastores)} datastores")
            else:
                self.logger.warning("No datastores found")
                self.data_storage = DataStorage()
        except Exception as e:
            self.logger.error(f"Failed to load datastores: {e}")
            self.data_storage = DataStorage()
    
    async def load_configs(self):
        """Load pipeline configs"""
        try:
            self.job_configs.clear()
            configs_by_store = await self.config_manager.list_pipeline_configs()
            
            for store_name, config_metas in configs_by_store.items():
                self.logger.info(f"Loading {len(config_metas)} configs from store: {store_name}")
                
                for config_meta in config_metas:
                    config = await self.config_manager.load_pipeline_config(
                        config_meta.name, store_name
                    )
                    if config:
                        self.job_configs[config.name] = config
            
            self.logger.info(f"Total loaded configs: {len(self.job_configs)}")
        
        except Exception as e:
            self.logger.error(f"Failed to load configs: {e}")
    
    async def _schedule_jobs(self):
        """
        Check and schedule jobs to ARQ workers.
        Uses strategy-level state and enqueue locks.
        Selects ONE strategy per pipeline (highest priority).
        """
        current_time = datetime.now(timezone.utc)
        
        for job_name, job_config in self.job_configs.items():
            try:
                # Select strategy to run (picks ONE with highest priority)
                strategy_selection = self.strategy_selector.select_strategy(job_config)
                
                if not strategy_selection:
                    continue
                
                strategy_config, strategy_name = strategy_selection
                
                # Try to enqueue the selected strategy
                await self._try_enqueue_strategy(
                    job_name,
                    job_config,
                    strategy_name,
                    strategy_config,
                    current_time
                )
            
            except Exception as e:
                self.logger.error(f"Failed to process pipeline {job_name}: {e}", exc_info=True)
    
    async def _try_enqueue_strategy(
        self,
        pipeline_id: str,
        job_config: PipelineJobConfig,
        strategy_name: str,
        strategy_config,
        current_time: datetime
    ):
        """
        Try to enqueue a strategy with proper pipeline-level locking and state management.
        Lock is at pipeline level to prevent multiple strategies from same pipeline being enqueued.
        
        The enqueue lock is NOT released here - it will be released by the worker
        when the job starts running or is skipped.
        """
        lock_key = f"{pipeline_id}:{strategy_name}"
        
        # Try to acquire pipeline-level ENQUEUE lock (will be released by worker)
        lock_acquired = self.enqueue_lock_manager.try_acquire_pipeline_enqueue_lock(
            pipeline_id=pipeline_id,
            strategy_name=strategy_name,
            timeout=300  # 5 minutes
        )
        
        if not lock_acquired:
            self.logger.debug(f"Could not acquire enqueue lock for pipeline {pipeline_id}")
            return
        
        # Double-check if we can still enqueue (state may have changed)
        # can_enqueue, reason = self.strategy_selector.can_enqueue_strategy(
        #     pipeline_id,
        #     strategy_name,
        #     current_time
        # )
        
        # if not can_enqueue:
        #     self.logger.info(f"Cannot enqueue {lock_key}: {reason}")
        #     # Release lock since we're not enqueueing
        #     self._release_enqueue_lock(pipeline_id)
        #     return
        
        try:
            # Serialize config to JSON
            config_json = ConfigSerializer.config_to_json_dict(
                config=job_config,
                data_storage=self.data_storage
            )
            
            # Generate run ID
            run_id = str(uuid.uuid4())
            
            # Update state to pending BEFORE enqueuing
            state = self.state_manager.get_current_state(pipeline_id, strategy_name)
            if not state:
                state = StrategyRunState(
                    strategy=strategy_name,
                    status="inactive"
                )
            
            state.status = "pending"
            state.run_id = run_id
            state.last_scheduled_at = current_time.isoformat()
            state.message = "Enqueued to worker"
            
            self.state_manager.update_state(state, pipeline_id)
            
            # Enqueue job to ARQ (lock will be released by worker)
            job = await self.redis_pool.enqueue_job(
                'execute_pipeline_strategy',
                config_json,
                strategy_name,
                run_id,
                pipeline_id,
                f"http://localhost:{self.http_port}"
            )
            
            self.logger.info(
                f"Successfully enqueued {pipeline_id}:{strategy_name} "
                f"with run_id {run_id} (enqueue lock will be released by worker)"
            )
        
        except Exception as e:
            self.logger.error(
                f"Failed to enqueue {lock_key}: {e}",
                exc_info=True
            )
            # Release lock on error
            self._release_enqueue_lock(pipeline_id)
            
            # Update state to reflect failure
            state = self.state_manager.get_current_state(pipeline_id, strategy_name)
            if state:
                state.status = "failed"
                state.message = f"Enqueue failed: {str(e)}"
                self.state_manager.update_state(state, pipeline_id)

    def _release_enqueue_lock(self, pipeline_id: str):
        """Helper to release enqueue lock (used only on errors before worker picks up)"""
        try:
            lock_key = f"synctool:enqueue:pipeline:{pipeline_id}"
            lua_script = "return redis.call('DEL', KEYS[1])"
            self.enqueue_lock_manager.redis_client.eval(lua_script, 1, lock_key)
            self.logger.info(f"Released enqueue lock for pipeline {pipeline_id}")
        except Exception as e:
            self.logger.error(f"Failed to release enqueue lock for {pipeline_id}: {e}")
    
    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.redis_pool:
            await self.redis_pool.close()
        await self.config_manager.close()
        self.logger.info("Enhanced scheduler stopped")

