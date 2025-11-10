import asyncio
import logging
import json
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path
from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI, Request
from pydantic import BaseModel
import uvicorn

from ..config.config_manager import ConfigManager
from ..config.global_config_loader import get_global_config, GlobalConfig
from ..core.models import DataStorage, PipelineJobConfig
from .pipeline_state_manager import PipelineStateManager, PipelineRunState
from ..config.config_serializer import ConfigSerializer
from .strategy_selector import StrategySelector
from .execution_lock_manager import ExecutionLockManager


class StatusUpdate(BaseModel):
    pipeline_id: str
    strategy: str
    run_id: str
    worker: str
    status: str
    message: str
    timestamp: str


class LogEntry(BaseModel):
    pipeline_id: str
    run_id: str
    level: str
    message: str
    timestamp: str


class ARQScheduler:
    """Enhanced scheduler that uses ARQ for distributed job execution"""
    
    def __init__(self, config_manager: ConfigManager, global_config: Optional[GlobalConfig] = None):
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
        self.running = False
        
        # Load global config
        if global_config is None:
            global_config = get_global_config()
        self.global_config = global_config
        
        # Initialize components using global config
        self.state_manager = PipelineStateManager(
            state_dir=global_config.storage.state_dir,
            max_runs_per_pipeline=global_config.storage.max_runs_per_job
        )
        self.strategy_selector = StrategySelector(self.state_manager)
        # self.lock_manager = RedisLockManager(
        #     redis_url=global_config.redis.url,
        #     lock_timeout=global_config.scheduler.lock_timeout
        # )
        self.lock_manager = ExecutionLockManager(
            redis_url=global_config.redis.url,
            pipeline_lock_timeout=global_config.scheduler.lock_timeout
        )
        
        # ARQ connection
        self.redis_pool = None
        self.job_configs: Dict[str, PipelineJobConfig] = {}
        self.data_storage: Optional[DataStorage] = None
        
        # HTTP server for worker updates
        self.app = FastAPI(title="Scheduler API")
        self._setup_http_endpoints()
        self.http_port = global_config.scheduler.http_port
    
    def _setup_http_endpoints(self):
        """Setup HTTP endpoints for worker communication"""
        
        @self.app.post("/api/worker/status")
        async def receive_status_update(update: StatusUpdate):
            """Receive status update from worker"""
            self.logger.info(
                f"Status update from {update.worker}: "
                f"{update.pipeline_id}:{update.strategy} -> {update.status}"
            )
            
            # Update pipeline state
            state = PipelineRunState(
                pipeline_id=update.pipeline_id,
                strategy=update.strategy,
                status=update.status,
                last_run=update.timestamp if update.status == "success" else None,
                run_id=update.run_id,
                worker=update.worker,
                message=update.message,
                updated_at=update.timestamp
            )
            self.state_manager.update_state(state)
            
            return {"status": "ok"}
        
        @self.app.post("/api/worker/logs/batch")
        async def receive_log_batch(request: Request):
            """Receive batch of log entries from worker"""
            try:
                data = await request.json()
                logs = data.get("logs", [])
                
                # Write all logs to appropriate files
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
                    
                    # Convert timestamp string to datetime for formatting
                    try:
                        ts = datetime.fromisoformat(timestamp)
                        timestamp_str = ts.isoformat()
                    except:
                        timestamp_str = timestamp
                    
                    # Write log entry as JSON line
                    log_line = {
                        "timestamp": timestamp_str,
                        "level": level,
                        "message": message,
                        "run_id": run_id
                    }
                    if logger_name:
                        log_line["logger"] = logger_name
                    
                    with open(log_file, 'a') as f:
                        f.write(json.dumps(log_line) + "\n")
                
                self.logger.debug(f"Received and stored {len(logs)} log entries")
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
                
                # Save metrics to scheduler's storage
                metrics_dir = Path(self.global_config.storage.metrics_dir) / job_name
                metrics_dir.mkdir(parents=True, exist_ok=True)
                metrics_file = metrics_dir / f"{run_id}.json"
                
                with open(metrics_file, 'w') as f:
                    json.dump(metrics_data, f, indent=2)
                
                self.logger.info(f"Received metrics for {job_name}:{run_id}")
                return {"status": "ok"}
                
            except Exception as e:
                self.logger.error(f"Failed to process metrics: {e}")
                return {"status": "error", "message": str(e)}
        
        @self.app.get("/api/pipelines/states")
        async def list_pipeline_states():
            """List all pipeline states"""
            states = self.state_manager.list_all_states()
            return {
                "states": [state.to_dict() for state in states.values()],
                "total": len(states)
            }
        
        @self.app.get("/api/pipelines/{pipeline_id}/history")
        async def get_pipeline_history(pipeline_id: str, limit: int = 10):
            """Get run history for a pipeline"""
            history = self.state_manager.get_run_history(pipeline_id, limit)
            return {
                "pipeline_id": pipeline_id,
                "history": [h.to_dict() for h in history]
            }
        
        @self.app.get("/api/pipelines/{pipeline_id}/runs/{run_id}/logs")
        async def get_run_logs(pipeline_id: str, run_id: str):
            """Get logs for a specific run"""
            log_file = Path(self.global_config.storage.logs_dir) / pipeline_id / f"{run_id}.log"
            
            if not log_file.exists():
                return {"error": "Log file not found"}
            
            logs = []
            with open(log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            logs.append(json.loads(line))
                        except:
                            pass
            
            return {
                "pipeline_id": pipeline_id,
                "run_id": run_id,
                "logs": logs,
                "count": len(logs)
            }
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "scheduler": "running" if self.running else "stopped",
                "timestamp": datetime.now().isoformat()
            }
    
    async def start(self):
        """Start the scheduler"""
        self.running = True
        self.logger.info("Starting ARQ-based scheduler")
        
        # Initialize Redis pool for ARQ
        from urllib.parse import urlparse
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
                self.logger.warning("No datastores found, creating empty DataStorage")
                from ..core.models import DataStorage
                self.data_storage = DataStorage()
        except Exception as e:
            self.logger.error(f"Failed to load datastores: {e}")
            from ..core.models import DataStorage
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
                        self.logger.info(f"Loaded config: {config.name}")
            
            self.logger.info(f"Total loaded configs: {len(self.job_configs)}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configs: {e}")
    
    async def _schedule_jobs(self):
        """Check and schedule jobs to ARQ workers"""
        current_time = datetime.now()
        
        for job_name, job_config in self.job_configs.items():
            try:
                # Check current state
                # current_state = self.state_manager.get_current_state(job_name)
                
                # # Skip if already running
                # if current_state and current_state.status == "running":
                #     self.logger.debug(f"Pipeline {job_name} already running, skipping")
                #     continue
                
                # Select strategy to run
                strategy_selection = self.strategy_selector.select_strategy(job_config)
                
                if not strategy_selection:
                    continue
                
                strategy_config, strategy_name = strategy_selection

                # try to acquire enqueue lock for the pipeline enqueue_key = f"pipeline:{pipeline_id}:enqueued"
                # if lock is acquired, continue
                # check if the strategy is configured to wait 
                
                # Try to acquire lock
                lock_key = f"{job_name}:{strategy_name}"
                with self.lock_manager.acquire_lock(job_name, strategy_name) as lock_acquired:
                    if not lock_acquired:
                        self.logger.info(f"Could not acquire lock for {lock_key}")
                        continue
                    
                    # Serialize config to JSON (no file needed - passed directly)
                    config_json = ConfigSerializer.config_to_json_dict(
                        config=job_config,
                        data_storage=self.data_storage
                    )
                    
                    # Generate run ID
                    import uuid
                    run_id = str(uuid.uuid4())
                    
                    # Enqueue job to ARQ with JSON config (no pickle file path)
                    job = await self.redis_pool.enqueue_job(
                        'execute_pipeline_job',
                        config_json,  # Pass JSON directly
                        strategy_name,
                        run_id,
                        job_name,
                        f"http://localhost:{self.http_port}"
                    )
                    
                    self.logger.info(f"Enqueued job {job_name}:{strategy_name} with run_id {run_id}")
                    
                    # Update state to waiting
                    state = PipelineRunState(
                        pipeline_id=job_name,
                        strategy=strategy_name,
                        status="waiting",
                        run_id=run_id,
                        message="Enqueued to worker",
                        updated_at=datetime.now().isoformat()
                    )
                    self.state_manager.update_state(state)
                
            except Exception as e:
                self.logger.error(f"Failed to schedule {job_name}: {e}", exc_info=True)
    
    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.redis_pool:
            await self.redis_pool.close()
        await self.config_manager.close()
        self.logger.info("Scheduler stopped")

