#!/usr/bin/env python3
"""
Synctool Sync CLI

This CLI provides commands to run individual sync jobs without the scheduler.
"""

import asyncio
import argparse
import json
import logging
import traceback
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import yaml
from datetime import datetime, timezone

from ..config.config_loader import ConfigLoader
from ..config.config_manager import ConfigManager
from ..monitoring.metrics_storage import MetricsStorage
from ..monitoring.logs_storage import LogsStorage
from ..sync.sync_job_manager import SyncJobManager
from ..utils.schema_manager import SchemaManager
from ..scheduler.pipeline_state_manager import PipelineStateManager, StrategyRunState
from ..config.global_config_loader import get_global_config
import uuid
import socket


class SyncCLI:
    """Command-line interface for running individual sync jobs"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Initialize state manager
        try:
            global_config = get_global_config()
            self.state_manager = PipelineStateManager(
                state_dir=global_config.storage.state_dir,
                max_runs_per_strategy=global_config.storage.max_runs_per_strategy
            )
            self.worker_id = f"{socket.gethostname()}-manual-cli"
        except Exception as e:
            self.logger.warning(f"Could not initialize state manager: {e}")
            self.state_manager = None
            self.worker_id = "manual-cli"
    
    def _parse_bounds(self, bounds_str: Optional[str]) -> Optional[List[Dict[str, Any]]]:
        """Parse and validate bounds JSON string"""
        if not bounds_str:
            return None
        
        try:
            bounds = json.loads(bounds_str)
            
            # Ensure bounds is a list
            if not isinstance(bounds, list):
                raise ValueError("Bounds must be a list of dictionaries")
            
            # Validate each bound entry
            for i, bound in enumerate(bounds):
                if not isinstance(bound, dict):
                    raise ValueError(f"Bound entry {i} must be a dictionary")
                
                # Check required 'column' field
                if 'column' not in bound:
                    raise ValueError(f"Bound entry {i} must have a 'column' field")
                
                # Check that at least one of the valid keys is present
                valid_keys = {'start', 'end', 'value', 'column'}
                bound_keys = set(bound.keys())
                
                if not bound_keys.issubset(valid_keys):
                    invalid_keys = bound_keys - valid_keys
                    raise ValueError(f"Bound entry {i} contains invalid keys: {invalid_keys}. Valid keys are: {valid_keys}")
                
                # Check that we have either range bounds (start/end) or value bounds, not both
                has_range = 'start' in bound or 'end' in bound
                has_value = 'value' in bound
                
                if has_range and has_value:
                    raise ValueError(f"Bound entry {i} cannot have both range bounds (start/end) and value bounds (value)")
                
                if not has_range and not has_value:
                    raise ValueError(f"Bound entry {i} must have either range bounds (start/end) or value bounds (value)")
            
            self.logger.info(f"Successfully parsed {len(bounds)} bound entries")
            return bounds
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in bounds parameter: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing bounds: {e}")
        
    async def run_job(self, args):
        """Run a specific job manually"""
        self.logger.info(f"Starting sync job: {args.job_name}")
        
        # Generate run_id for this execution
        run_id = str(uuid.uuid4())
        
        # Parse bounds if provided
        bounds = None
        try:
            bounds = self._parse_bounds(getattr(args, 'bounds', None))
        except ValueError as e:
            self.logger.error(f"Invalid bounds parameter: {e}")
            sys.exit(1)
        
        # Load configuration using ConfigManager if available, otherwise fall back to file loading
        job_config = None
        data_storage = None
        
        if args.config_store_type == 'file' or not args.config_store_type:
            # Use file-based loading
            job_config, data_storage = await self._load_config_from_file(args)
        elif args.config_store_type == 'manager':
            # Use ConfigManager
            job_config, data_storage = await self._load_config_from_manager(args)
        else:
            self.logger.error(f"Unsupported config store type: {args.config_store_type}")
            sys.exit(1)
        
        if not job_config:
            self.logger.error(f"Job '{args.job_name}' not found")
            sys.exit(1)
        
        # Determine which strategy we're running
        strategy_name = args.strategy
        if not strategy_name:
            # If no strategy specified, use the first enabled strategy
            for stage in job_config.stages:
                for s in stage.strategies:
                    if s.enabled:
                        strategy_name = s.name
                        break
                if strategy_name:
                    break
        
        # Validate strategy if provided
        if strategy_name:
            strategy_names = []
            for stage in job_config.stages:
                strategy_names.extend([s.name for s in stage.strategies])
            
            if strategy_name not in strategy_names:
                strategy_list = ', '.join(str(name) for name in strategy_names)
                self.logger.error(f"Strategy '{strategy_name}' not found. Available strategies: {strategy_list}")
                sys.exit(1)
        
        self.logger.info(f"Running job: {args.job_name}")
        if strategy_name:
            self.logger.info(f"Strategy: {strategy_name}")
            self.logger.info(f"Run ID: {run_id}")
        
        # Update state to "running" before execution
        if self.state_manager and strategy_name:
            try:
                state = StrategyRunState(
                    strategy=strategy_name,
                    status="running",
                    run_id=run_id,
                    worker=self.worker_id,
                    message="Manual execution started",
                    last_attempted_at=datetime.now(timezone.utc).isoformat()
                )
                self.state_manager.update_state(state, args.job_name)
                self.logger.info(f"Updated state: {args.job_name}:{strategy_name} -> running")
            except Exception as e:
                self.logger.warning(f"Failed to update initial state: {e}")
        
        # Create storage components
        metrics_storage = MetricsStorage(args.metrics_dir, args.max_runs_per_job)
        logs_storage = LogsStorage(args.logs_dir, args.max_runs_per_job)
        
        # Initialize execution lock manager if Redis is available
        execution_lock_manager = None
        try:
            from ..config.global_config_loader import get_global_config
            from ..scheduler.execution_lock_manager import ExecutionLockManager
            
            global_config = get_global_config()
            execution_lock_manager = ExecutionLockManager(
                redis_url=global_config.redis.url,
                pipeline_lock_timeout=global_config.scheduler.lock_timeout
            )
            self.logger.info("Lock manager initialized for manual execution with locking enabled")
        except Exception as e:
            self.logger.warning(f"Could not initialize lock manager, running without locks: {e}")
            self.logger.warning("Manual execution will proceed without distributed locking")
        
        # Get strategy-specific lock settings if strategy is specified
        wait_for_pipeline_lock = True  # Always wait in manual mode
        pipeline_lock_wait_timeout = 300  # Default 5 minutes
        wait_for_table_lock = True  # Always wait in manual mode
        table_lock_wait_timeout = 300  # Default 5 minutes
        
        if args.strategy and job_config:
            strategy_config = None
            for stage in job_config.stages:
                for s in stage.strategies:
                    if s.name == args.strategy:
                        strategy_config = s
                        break
                if strategy_config:
                    break
            
            if strategy_config:
                # Use strategy-specific settings if available
                wait_for_pipeline_lock = getattr(strategy_config, 'wait_for_pipeline_lock', True)
                pipeline_lock_wait_timeout = getattr(strategy_config, 'pipeline_lock_wait_timeout', 300)
                wait_for_table_lock = getattr(strategy_config, 'wait_for_table_lock', True)
                table_lock_wait_timeout = getattr(strategy_config, 'table_lock_wait_timeout', 300)
                
                self.logger.info(
                    f"Lock settings from strategy config: "
                    f"wait_for_pipeline_lock={wait_for_pipeline_lock}, "
                    f"pipeline_lock_wait_timeout={pipeline_lock_wait_timeout}s, "
                    f"wait_for_table_lock={wait_for_table_lock}, "
                    f"table_lock_wait_timeout={table_lock_wait_timeout}s"
                )
        
        # Allow command-line overrides for lock behavior
        if hasattr(args, 'no_wait_lock') and args.no_wait_lock:
            wait_for_pipeline_lock = False
            wait_for_table_lock = False
            self.logger.info("Lock waiting disabled by --no-wait-lock flag")
        
        if hasattr(args, 'lock_timeout') and args.lock_timeout:
            pipeline_lock_wait_timeout = args.lock_timeout
            table_lock_wait_timeout = args.lock_timeout
            self.logger.info(f"Lock timeout overridden to {args.lock_timeout}s by --lock-timeout flag")
        
        # Create job manager with lock manager
        job_manager = SyncJobManager(
            max_concurrent_jobs=1, 
            metrics_storage=metrics_storage,
            logs_storage=logs_storage,
            data_storage=data_storage,
            execution_lock_manager=execution_lock_manager
        )
        
        # Progress callback
        def progress_callback(job_name: str, progress):
            self.logger.info(
                f"Progress: {progress.processed_partitions+progress.skipped_partitions}/"
                f"{progress.total_partitions} partitions completed"
            )
        
        try:
            self.logger.info("Attempting to acquire locks and execute job...")
            
            # Run the job with lock management
            result = await job_manager.run_sync_job(
                config=job_config,
                strategy_name=strategy_name,
                bounds=bounds,
                progress_callback=progress_callback,
                run_id=run_id,
                wait_for_pipeline_lock=wait_for_pipeline_lock,
                pipeline_lock_wait_timeout=pipeline_lock_wait_timeout,
                wait_for_table_lock=wait_for_table_lock,
                table_lock_wait_timeout=table_lock_wait_timeout
            )
            
            # Check if job was skipped
            if result.get('status') == 'skipped':
                # Update state to "skipped"
                if self.state_manager and strategy_name:
                    try:
                        state = StrategyRunState(
                            strategy=strategy_name,
                            status="skipped",
                            run_id=run_id,
                            worker=self.worker_id,
                            message=result.get('message', 'Job was skipped'),
                            last_attempted_at=datetime.now(timezone.utc).isoformat()
                        )
                        self.state_manager.update_state(state, args.job_name)
                        self.logger.info(f"Updated state: {args.job_name}:{strategy_name} -> skipped")
                    except Exception as e:
                        self.logger.warning(f"Failed to update skipped state: {e}")
                
                self.logger.error(f"\n{'='*80}")
                self.logger.error(f"JOB SKIPPED")
                self.logger.error(f"{'='*80}")
                self.logger.error(f"Reason: {result.get('reason', 'unknown')}")
                self.logger.error(f"Message: {result.get('message', 'Job was skipped')}")
                self.logger.error(f"{'='*80}\n")
                
                if result.get('reason') == 'pipeline_lock_unavailable':
                    self.logger.error(
                        "Another instance of this pipeline is currently running.\n"
                        "Options:\n"
                        "  1. Wait for the other instance to complete and try again\n"
                        "  2. Use --no-wait-lock flag to skip immediately if locked\n"
                        "  3. Increase --lock-timeout if you want to wait longer"
                    )
                elif 'table' in result.get('reason', ''):
                    self.logger.error(
                        "The destination table is currently locked for DDL changes.\n"
                        "Options:\n"
                        "  1. Wait for the DDL operation to complete and try again\n"
                        "  2. Use --no-wait-lock flag to skip immediately if locked"
                    )
                
                sys.exit(1)
            
            # Check if job failed
            if result.get('status') == 'failed':
                # Update state to "failed"
                if self.state_manager and strategy_name:
                    try:
                        current_state = self.state_manager.get_current_state(args.job_name, strategy_name)
                        retry_count = current_state.retry_count + 1 if current_state else 1
                        
                        state = StrategyRunState(
                            strategy=strategy_name,
                            status="failed",
                            run_id=run_id,
                            worker=self.worker_id,
                            message="Manual execution failed",
                            error=result.get('error', 'Unknown error'),
                            retry_count=retry_count,
                            last_attempted_at=datetime.now(timezone.utc).isoformat()
                        )
                        self.state_manager.update_state(state, args.job_name)
                        self.logger.info(f"Updated state: {args.job_name}:{strategy_name} -> failed")
                    except Exception as e:
                        self.logger.warning(f"Failed to update failed state: {e}")
                
                self.logger.error(f"\nJob failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)
            # Success - update state to "success"
            if self.state_manager and strategy_name:
                try:
                    state = StrategyRunState(
                        strategy=strategy_name,
                        status="success",
                        run_id=run_id,
                        worker=self.worker_id,
                        message="Manual execution completed successfully",
                        last_run=datetime.now(timezone.utc).isoformat(),
                        last_attempted_at=datetime.now(timezone.utc).isoformat(),
                        retry_count=0,
                        error=None
                    )
                    self.state_manager.update_state(state, args.job_name)
                    self.logger.info(f"Updated state: {args.job_name}:{strategy_name} -> success")
                except Exception as e:
                    self.logger.warning(f"Failed to update success state: {e}")
            
            # Success - display results
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"JOB COMPLETED SUCCESSFULLY")
            self.logger.info(f"{'='*80}")
            self.logger.info(f"Total primary partitions: {result.get('total_primary_partitions', 0)}")
            self.logger.info(f"Total partitions: {result.get('total_partitions', 0)}")
            self.logger.info(f"Processed partitions: {result.get('processed_partitions', 0)}")
            self.logger.info(f"Skipped partitions: {result.get('skipped_partitions', 0)}")
            self.logger.info(f"Failed partitions: {result.get('failed_partitions', 0)}")
            self.logger.info(f"Rows detected: {result.get('total_rows_detected', 0)}")
            self.logger.info(f"Rows fetched: {result.get('total_rows_fetched', 0)}")
            self.logger.info(f"Rows inserted: {result.get('total_rows_inserted', 0)}")
            self.logger.info(f"Rows updated: {result.get('total_rows_updated', 0)}")
            self.logger.info(f"Rows deleted: {result.get('total_rows_deleted', 0)}")
            self.logger.info(f"Rows failed: {result.get('total_rows_failed', 0)}")
            self.logger.info(f"Detection query count: {result.get('detection_query_count', 0)}")
            self.logger.info(f"Hash query count: {result.get('hash_query_count', 0)}")
            self.logger.info(f"Data query count: {result.get('data_query_count', 0)}")
            self.logger.info(f"Duration: {result.get('duration', 'unknown')}")
            self.logger.info(f"{'='*80}\n")
        
        except Exception as e:
            # Update state to "failed" on exception
            if self.state_manager and strategy_name:
                try:
                    current_state = self.state_manager.get_current_state(args.job_name, strategy_name)
                    retry_count = current_state.retry_count + 1 if current_state else 1
                    
                    state = StrategyRunState(
                        strategy=strategy_name,
                        status="failed",
                        run_id=run_id,
                        worker=self.worker_id,
                        message="Manual execution failed with exception",
                        error=str(e),
                        retry_count=retry_count,
                        last_attempted_at=datetime.now(timezone.utc).isoformat()
                    )
                    self.state_manager.update_state(state, args.job_name)
                    self.logger.info(f"Updated state: {args.job_name}:{strategy_name} -> failed")
                except Exception as state_error:
                    self.logger.warning(f"Failed to update exception state: {state_error}")
            
            traceback.print_exc()
            self.logger.error(f"Error running job: {e}")
            sys.exit(1)
    
    async def _load_config_from_file(self, args) -> tuple[Optional[object], Optional[object]]:
        """Load configuration from file system"""
        config_dir = Path(args.config_dir)
        
        if not config_dir.exists():
            self.logger.error(f"Config directory does not exist: {config_dir}")
            return None, None
        
        # Look for the specific job config file
        job_config_file = None
        for config_file in config_dir.glob("*.yaml"):
            if config_file.name == "datastores.yaml":
                continue
                
            try:
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                if config_data and config_data.get('name') == args.job_name:
                    job_config_file = config_file
                    break
            except Exception as e:
                self.logger.warning(f"Could not read config file {config_file}: {e}")
        
        if not job_config_file:
            self.logger.error(f"No config file found for job '{args.job_name}' in {config_dir}")
            return None, None
        
        try:
            # Load the specific job configuration
            job_config = ConfigLoader.load_from_yaml(str(job_config_file))
            
            # Load datastores configuration
            datastores_file = config_dir / "datastores.yaml"
            if not datastores_file.exists():
                self.logger.error(f"Datastores config file not found: {datastores_file}")
                return None, None
            
            data_storage = ConfigLoader.load_datastores_from_yaml(str(datastores_file))
            
            # Validate the configuration
            issues = ConfigLoader.validate_pipeline_with_datastores(job_config, data_storage)
            if issues:
                self.logger.error(f"Configuration validation failed: {issues}")
                return None, None
            
            self.logger.info(f"Successfully loaded job config from: {job_config_file}")
            return job_config, data_storage
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return None, None
    
    async def _load_config_from_manager(self, args) -> tuple[Optional[object], Optional[object]]:
        """Load configuration using ConfigManager"""
        config_manager = ConfigManager()
        
        # Add file store if config_dir is provided
        if args.config_dir:
            config_manager.add_file_store(
                name="file_store",
                base_path=args.config_dir,
                is_primary=True
            )
        
        # Add database store if database config is provided
        if args.db_type:
            db_config = {
                'type': args.db_type,
                'host': args.db_host,
                'port': args.db_port,
                'user': args.db_user,
                'password': args.db_password,
                'database': args.db_name
            }
            config_manager.add_database_store(
                name="db_store",
                connection_config=db_config,
                is_primary=not args.config_dir  # Primary if no file store
            )
            
            # Initialize database tables
            await config_manager.initialize_database_stores()
        
        try:
            # Load the specific job configuration
            job_config = await config_manager.load_pipeline_config(args.job_name)
            if not job_config:
                return None, None
            
            # Load datastores configuration
            data_storage = await config_manager.load_datastores_config()
            if not data_storage:
                self.logger.error("No datastores configuration found")
                return None, None
            
            # Validate the configuration
            issues = ConfigLoader.validate_pipeline_with_datastores(job_config, data_storage)
            if issues:
                self.logger.error(f"Configuration validation failed: {issues}")
                return None, None
            
            self.logger.info(f"Successfully loaded job config from ConfigManager")
            return job_config, data_storage
            
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Failed to load configuration from ConfigManager: {e}")
            return None, None
        finally:
            await config_manager.close()
    
    async def list_jobs(self, args):
        """List all available jobs"""
        if args.config_store_type == 'manager' and args.db_type:
            await self._list_jobs_from_manager(args)
        else:
            await self._list_jobs_from_file(args)
    
    async def _list_jobs_from_file(self, args):
        """List jobs from file system"""
        config_dir = Path(args.config_dir)
        
        if not config_dir.exists():
            self.logger.error(f"Config directory does not exist: {config_dir}")
            return
        
        jobs = []
        for config_file in config_dir.glob("*.yaml"):
            if config_file.name == "datastores.yaml":
                continue
                
            try:
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                if config_data and config_data.get('name'):
                    job_config = ConfigLoader.load_from_dict(config_data)
                    jobs.append((job_config.name, job_config.description, job_config))
                    
            except Exception as e:
                self.logger.warning(f"Could not read config file {config_file}: {e}")
        
        if not jobs:
            print("No jobs found in config directory")
            return
        
        print(f"\nFound {len(jobs)} job(s):")
        print("-" * 80)
        
        for job_name, description, job_config in jobs:
            print(f"Job: {job_name}")
            print(f"  Description: {description}")
            print(f"  Stages: {len(job_config.stages)}")
            
            # Show strategies from all stages
            all_strategies = []
            for stage in job_config.stages:
                for strategy in stage.strategies:
                    status = "✓" if strategy.enabled else "✗"
                    cron = strategy.cron or 'No schedule'
                    all_strategies.append(f"    {status} {strategy.name} ({strategy.type.value if hasattr(strategy.type, 'value') else strategy.type}) - {cron}")
            
            if all_strategies:
                print(f"  Strategies:")
                for strategy_info in all_strategies:
                    print(strategy_info)
            print()
    
    async def _list_jobs_from_manager(self, args):
        """List jobs from ConfigManager"""
        config_manager = ConfigManager()
        
        # Add database store
        db_config = {
            'type': args.db_type,
            'host': args.db_host,
            'port': args.db_port,
            'user': args.db_user,
            'password': args.db_password,
            'database': args.db_name
        }
        config_manager.add_database_store(
            name="db_store",
            connection_config=db_config,
            is_primary=True
        )
        
        try:
            await config_manager.initialize_database_stores()
            
            # List configurations
            configs = await config_manager.list_pipeline_configs()
            
            if not configs:
                print("No jobs found in configuration store")
                return
            
            print(f"\nFound {len(configs)} job(s):")
            print("-" * 80)
            
            for config_name in configs:
                job_config = await config_manager.load_pipeline_config(config_name)
                if job_config:
                    print(f"Job: {job_config.name}")
                    print(f"  Description: {job_config.description}")
                    print(f"  Stages: {len(job_config.stages)}")
                    print()
                    
        except Exception as e:
            self.logger.error(f"Error listing jobs from ConfigManager: {e}")
        finally:
            await config_manager.close()
    
    async def generate_ddl(self, args):
        """Generate DDL for populate stage destination table"""
        self.logger.info(f"Generating DDL for job: {args.job_name}")
        
        # Load configuration
        if args.config_store_type == 'file' or not args.config_store_type:
            job_config, data_storage = await self._load_config_from_file(args)
        else:
            job_config, data_storage = await self._load_config_from_manager(args)
        
        if not job_config or not data_storage:
            self.logger.error("Failed to load configuration")
            sys.exit(1)
        
        # Find the populate stage (auto-detect)
        populate_stages = [stage for stage in job_config.stages if stage.type == 'populate']
        
        if not populate_stages:
            self.logger.error(f"No populate stage found in job '{args.job_name}'")
            sys.exit(1)
        
        if len(populate_stages) > 1:
            stage_names = [s.name for s in populate_stages]
            self.logger.error(f"Multiple populate stages found in job '{args.job_name}': {stage_names}")
            self.logger.error("Please ensure only one populate stage per pipeline")
            sys.exit(1)
        
        stage_config = populate_stages[0]
        self.logger.info(f"Found populate stage: {stage_config.name}")
        
        if not stage_config.destination:
            self.logger.error(f"No destination configured for stage '{stage_config.name}'")
            sys.exit(1)
        
        if not stage_config.destination.datastore_name:
            self.logger.error(f"No datastore configured for destination in stage '{stage_config.name}'")
            sys.exit(1)
        # Get datastore directly
        datastore = data_storage.get_datastore(stage_config.destination.datastore_name)
        if not datastore:
            self.logger.error(f"Datastore '{stage_config.destination.datastore_name}' not found")
            sys.exit(1)
        
        await datastore.connect(self.logger)
        
        try:
            # Use SchemaManager with datastore
            schema_manager = SchemaManager(self.logger)
            
            # Check if we should auto-apply (--yes or --apply flags)
            auto_apply = args.yes or args.apply
            
            # First, always generate DDL without applying to show the user
            result = await schema_manager.ensure_table_schema(
                datastore=datastore,
                columns=stage_config.destination.columns,
                table_name=stage_config.destination.table,
                schema_name=stage_config.destination.schema,
                apply=False,  # Always preview first
                if_not_exists=args.if_not_exists
            )
            
            # Output results
            print(f"\n{'='*80}")
            print(f"Job: {args.job_name}")
            print(f"Stage: {stage_config.name}")
            print(f"Table: {result['table_name']}")
            print(f"Action: {result['action']}")
            print(f"Table Exists: {result['table_exists']}")
            print(f"{'='*80}\n")
            
            if result['changes']:
                print(f"Changes required:")
                for change in result['changes']:
                    print(f"  - {change.get('description', change['type'])}")
                print()
            
            if result['ddl_statements']:
                print(f"DDL Statements:\n")
                for ddl in result['ddl_statements']:
                    print(ddl)
                    print()
                
                # Determine whether to apply changes
                should_apply = False
                
                if auto_apply:
                    # Auto-apply mode (--yes or --apply flag)
                    should_apply = True
                    if args.yes:
                        print("Auto-applying changes (--yes flag)...")
                    else:
                        print("Auto-applying changes (--apply flag, consider using --yes)...")
                else:
                    # Interactive mode - prompt user
                    try:
                        response = input("\nDo you want to apply these changes? (yes/no): ").strip().lower()
                        should_apply = response in ['yes', 'y']
                    except (KeyboardInterrupt, EOFError):
                        print(f"\n\nOperation cancelled by user\n")
                        should_apply = False
                
                # Apply changes if approved
                if should_apply:
                    self.logger.info("Applying DDL changes...")
                    await schema_manager.ensure_table_schema(
                        datastore=datastore,
                        columns=stage_config.destination.columns,
                        table_name=stage_config.destination.table,
                        schema_name=stage_config.destination.schema,
                        apply=True,  # Apply changes
                        if_not_exists=args.if_not_exists
                    )
                    print(f"\n✓ DDL applied successfully\n")
                else:
                    print(f"\nNo changes applied\n")
            else:
                print("No DDL changes required - table schema matches config\n")
            
        except Exception as e:
            self.logger.error(f"Error generating DDL: {e}")
            traceback.print_exc()
            sys.exit(1)
        finally:
            await datastore.disconnect(self.logger)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Synctool Sync CLI - Run individual sync jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a job from file-based config
  python -m synctool.cli.sync_cli run --job-name my_job --config-dir ./configs

  # Run a job with specific strategy
  python -m synctool.cli.sync_cli run --job-name my_job --strategy delta --config-dir ./configs

  # Run a job without waiting for locks (fail fast if locked)
  python -m synctool.cli.sync_cli run --job-name my_job --no-wait-lock --config-dir ./configs

  # Run a job with custom lock timeout (wait up to 10 minutes)
  python -m synctool.cli.sync_cli run --job-name my_job --lock-timeout 600 --config-dir ./configs

  # Run a job with bounds filtering (date range)
  python -m synctool.cli.sync_cli run --job-name my_job --bounds '[{"column":"order_date","start":"2025-01-01","end":"2025-06-01"}]' --config-dir ./configs

  # Run a job with multiple bounds (range and value)
  python -m synctool.cli.sync_cli run --job-name my_job --bounds '[{"column":"order_date","start":"2025-01-01","end":"2025-06-01"},{"column":"category_id","value":[1,2,3]}]' --config-dir ./configs

  # List all available jobs
  python -m synctool.cli.sync_cli list --config-dir ./configs

  # Generate DDL (interactive - prompts for confirmation)
  python -m synctool.cli.sync_cli generate-ddl --job-name my_job --config-dir ./configs

  # Generate DDL with auto-apply (no prompt - for scripts/CI)
  python -m synctool.cli.sync_cli generate-ddl --job-name my_job --config-dir ./configs --yes

  # Generate DDL with IF NOT EXISTS
  python -m synctool.cli.sync_cli generate-ddl --job-name my_job --config-dir ./configs --if-not-exists

  # Run a job from database config store
  python -m synctool.cli.sync_cli run --job-name my_job --config-store-type manager --db-type postgres --db-host localhost --db-user synctool --db-password password --db-name synctool_configs
        """
    )
    
    # Add global arguments
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run a specific sync job')
    run_parser.add_argument('--job-name', required=True, help='Name of the job to run')
    run_parser.add_argument('--strategy', help='Strategy to use (optional)')
    run_parser.add_argument('--bounds', help='JSON string with bounds data. Format: [{"column":"col1","start":"val1","end":"val2"}, {"column":"col2","value":[1,2,3]}]')
    run_parser.add_argument('--config-dir', help='Directory containing job configs (for file-based loading)')
    run_parser.add_argument('--config-store-type', choices=['file', 'manager'], default='file',
                           help='Type of configuration store to use')
    run_parser.add_argument('--metrics-dir', default='./data/metrics', help='Directory for metrics storage')
    run_parser.add_argument('--logs-dir', default='./data/logs', help='Directory for logs')
    run_parser.add_argument('--max-runs-per-job', type=int, default=50, help='Maximum runs to keep per job')
    
    # Lock-related options
    run_parser.add_argument('--no-wait-lock', action='store_true',
                           help='Do not wait for locks (fail immediately if locked)')
    run_parser.add_argument('--lock-timeout', type=int,
                           help='Maximum time to wait for locks in seconds (overrides config)')
    
    # Database configuration for ConfigManager
    run_parser.add_argument('--db-type', choices=['postgres', 'mysql'], help='Database type for config store')
    run_parser.add_argument('--db-host', help='Database host')
    run_parser.add_argument('--db-port', type=int, help='Database port')
    run_parser.add_argument('--db-user', help='Database user')
    run_parser.add_argument('--db-password', help='Database password')
    run_parser.add_argument('--db-name', help='Database name')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all available jobs')
    list_parser.add_argument('--config-dir', help='Directory containing job configs (for file-based listing)')
    list_parser.add_argument('--config-store-type', choices=['file', 'manager'], default='file',
                            help='Type of configuration store to use')
    
    # Database configuration for listing
    list_parser.add_argument('--db-type', choices=['postgres', 'mysql'], help='Database type for config store')
    list_parser.add_argument('--db-host', help='Database host')
    list_parser.add_argument('--db-port', type=int, help='Database port')
    list_parser.add_argument('--db-user', help='Database user')
    list_parser.add_argument('--db-password', help='Database password')
    list_parser.add_argument('--db-name', help='Database name')
    
    # Generate DDL command
    ddl_parser = subparsers.add_parser('generate-ddl', help='Generate DDL for populate stage destination table (auto-detects populate stage)')
    ddl_parser.add_argument('--job-name', required=True, help='Name of the job')
    ddl_parser.add_argument('--apply', action='store_true', help='DEPRECATED: Apply without prompting. Use --yes instead for non-interactive mode')
    ddl_parser.add_argument('--yes', '-y', action='store_true', help='Automatically approve and apply DDL changes without prompting')
    ddl_parser.add_argument('--if-not-exists', action='store_true', help='Add IF NOT EXISTS clause for CREATE TABLE')
    ddl_parser.add_argument('--config-dir', help='Directory containing job configs (for file-based loading)')
    ddl_parser.add_argument('--config-store-type', choices=['file', 'manager'], default='file',
                           help='Type of configuration store to use')
    
    # Database configuration for DDL generation (config store)
    ddl_parser.add_argument('--db-type', choices=['postgres', 'mysql'], help='Database type for config store')
    ddl_parser.add_argument('--db-host', help='Database host for config store')
    ddl_parser.add_argument('--db-port', type=int, help='Database port for config store')
    ddl_parser.add_argument('--db-user', help='Database user for config store')
    ddl_parser.add_argument('--db-password', help='Database password for config store')
    ddl_parser.add_argument('--db-name', help='Database name for config store')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Create CLI instance and run command
    cli = SyncCLI()
    
    try:
        if args.command == 'run':
            # Validate required arguments
            if args.config_store_type == 'file' and not args.config_dir:
                print("Error: --config-dir is required for file-based configuration")
                sys.exit(1)
            
            asyncio.run(cli.run_job(args))
        elif args.command == 'list':
            if args.config_store_type == 'file' and not args.config_dir:
                print("Error: --config-dir is required for file-based configuration")
                sys.exit(1)
            
            asyncio.run(cli.list_jobs(args))
        elif args.command == 'generate-ddl':
            if args.config_store_type == 'file' and not args.config_dir:
                print("Error: --config-dir is required for file-based configuration")
                sys.exit(1)
            
            asyncio.run(cli.generate_ddl(args))
        else:
            parser.print_help()
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
