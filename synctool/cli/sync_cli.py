#!/usr/bin/env python3
"""
Synctool Sync CLI

This CLI provides commands to run individual sync jobs without the scheduler.
"""

import asyncio
import click
import json
import logging
import traceback
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import yaml
from datetime import datetime, timezone

from ..config.config_loader import ConfigLoader
from ..config.config_manager_factory import create_config_manager_from_global
from ..monitoring.metrics_storage import MetricsStorage
from ..monitoring.logs_storage import LogsStorage
from ..sync.sync_job_manager import SyncJobManager
from ..utils.schema_manager import SchemaManager
from ..scheduler.pipeline_state_manager import PipelineStateManager, StrategyRunState
from ..config.global_config_loader import load_global_config
import uuid
import socket


class SyncCLI:
    """Command-line interface for running individual sync jobs"""
    
    def __init__(self, global_config):
        self.logger = logging.getLogger(__name__)
        self.global_config = global_config
        self.config_manager = None
        
        # Initialize state manager
        try:
            self.state_manager = PipelineStateManager(
                state_dir=global_config.storage.state_dir,
                max_runs_per_strategy=global_config.storage.max_runs_per_strategy
            )
            self.worker_id = f"{socket.gethostname()}-manual-cli"
        except Exception as e:
            self.logger.warning(f"Could not initialize state manager: {e}")
            self.state_manager = None
            self.worker_id = "manual-cli"
    
    async def _ensure_config_manager(self):
        """Ensure ConfigManager is initialized"""
        if self.config_manager is None:
            self.config_manager = await create_config_manager_from_global(self.global_config)
            self.logger.info("Initialized ConfigManager from global config")
        return self.config_manager
    
    async def _cleanup_config_manager(self):
        """Cleanup ConfigManager resources"""
        if self.config_manager:
            await self.config_manager.close()
            self.config_manager = None
    
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
    
    async def run_job(self, job_name, strategy, bounds, no_wait_lock, lock_timeout):
        """Run a specific job manually"""
        self.logger.info(f"Starting sync job: {job_name}")
        
        # Generate run_id for this execution
        run_id = str(uuid.uuid4())
        
        # Parse bounds if provided
        parsed_bounds = None
        try:
            parsed_bounds = self._parse_bounds(bounds)
        except ValueError as e:
            self.logger.error(f"Invalid bounds parameter: {e}")
            sys.exit(1)
        
        # Initialize ConfigManager
        config_manager = await self._ensure_config_manager()
        
        try:
            # Load configuration using ConfigManager
            job_config = await config_manager.load_pipeline_config(job_name)
            if not job_config:
                self.logger.error(f"Job '{job_name}' not found in any configured store")
                sys.exit(1)
            
            data_storage = await config_manager.load_datastores_config()
            if not data_storage:
                self.logger.error("No datastores configuration found")
                sys.exit(1)
            
            # Validate the configuration
            issues = ConfigLoader.validate_pipeline_with_datastores(job_config, data_storage)
            if issues:
                self.logger.error(f"Configuration validation failed: {issues}")
                sys.exit(1)
            
            self.logger.info(f"Successfully loaded job config for: {job_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            traceback.print_exc()
            sys.exit(1)
        
        # Determine which strategy we're running
        strategy_name = strategy
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
        
        self.logger.info(f"Running job: {job_name}")
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
                self.state_manager.update_state(state, job_name)
                self.logger.info(f"Updated state: {job_name}:{strategy_name} -> running")
            except Exception as e:
                self.logger.warning(f"Failed to update initial state: {e}")
        
        # Create storage components using global config
        metrics_storage = MetricsStorage(
            self.global_config.storage.metrics_dir, 
            self.global_config.storage.max_runs_per_strategy
        )
        logs_storage = LogsStorage(
            self.global_config.storage.logs_dir, 
            self.global_config.storage.max_runs_per_strategy
        )
        
        # Initialize execution lock manager if Redis is available
        execution_lock_manager = None
        try:
            from ..scheduler.execution_lock_manager import ExecutionLockManager
            
            execution_lock_manager = ExecutionLockManager(
                redis_url=self.global_config.redis.url,
                pipeline_lock_timeout=self.global_config.scheduler.lock_timeout
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
        
        if strategy and job_config:
            strategy_config = None
            for stage in job_config.stages:
                for s in stage.strategies:
                    if s.name == strategy:
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
        if no_wait_lock:
            wait_for_pipeline_lock = False
            wait_for_table_lock = False
            self.logger.info("Lock waiting disabled by --no-wait-lock flag")
        
        if lock_timeout:
            pipeline_lock_wait_timeout = lock_timeout
            table_lock_wait_timeout = lock_timeout
            self.logger.info(f"Lock timeout overridden to {lock_timeout}s by --lock-timeout flag")
        
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
                bounds=parsed_bounds,
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
                        self.state_manager.update_state(state, job_name)
                        self.logger.info(f"Updated state: {job_name}:{strategy_name} -> skipped")
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
                        current_state = self.state_manager.get_current_state(job_name, strategy_name)
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
                        self.state_manager.update_state(state, job_name)
                        self.logger.info(f"Updated state: {job_name}:{strategy_name} -> failed")
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
                    self.state_manager.update_state(state, job_name)
                    self.logger.info(f"Updated state: {job_name}:{strategy_name} -> success")
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
                    current_state = self.state_manager.get_current_state(job_name, strategy_name)
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
                    self.state_manager.update_state(state, job_name)
                    self.logger.info(f"Updated state: {job_name}:{strategy_name} -> failed")
                except Exception as state_error:
                    self.logger.warning(f"Failed to update exception state: {state_error}")
            
            traceback.print_exc()
            self.logger.error(f"Error running job: {e}")
            sys.exit(1)
    
    async def list_jobs(self):
        """List all available jobs"""
        config_manager = await self._ensure_config_manager()
        
        try:
            # List configurations from all stores
            all_configs = await config_manager.list_pipeline_configs()
            
            if not all_configs:
                click.echo("No jobs found in any configured store")
                return
            
            # Display jobs grouped by store
            for store_name, configs in all_configs.items():
                if not configs:
                    continue
                    
                click.echo(f"\n=== Store: {store_name} ===")
                click.echo(f"Found {len(configs)} job(s):")
                click.echo("-" * 80)
                
                for config_meta in configs:
                    # Load the full config
                    job_config = await config_manager.load_pipeline_config(
                        config_meta.name, store_name=store_name
                    )
                    
                    if job_config:
                        click.echo(f"Job: {job_config.name}")
                        click.echo(f"  Description: {job_config.description}")
                        click.echo(f"  Stages: {len(job_config.stages)}")
                        
                        # Show strategies from all stages
                        all_strategies = []
                        for stage in job_config.stages:
                            for strategy in stage.strategies:
                                status = "✓" if strategy.enabled else "✗"
                                cron = strategy.cron or 'No schedule'
                                all_strategies.append(
                                    f"    {status} {strategy.name} "
                                    f"({strategy.type.value if hasattr(strategy.type, 'value') else strategy.type}) "
                                    f"- {cron}"
                                )
                        
                        if all_strategies:
                            click.echo(f"  Strategies:")
                            for strategy_info in all_strategies:
                                click.echo(strategy_info)
                        click.echo()
                        
        except Exception as e:
            self.logger.error(f"Error listing jobs: {e}")
            traceback.print_exc()
    
    async def generate_ddl(self, job_name, yes, apply, if_not_exists):
        """Generate DDL for populate stage destination table"""
        self.logger.info(f"Generating DDL for job: {job_name}")
        
        # Initialize ConfigManager
        config_manager = await self._ensure_config_manager()
        
        try:
            # Load configuration using ConfigManager
            job_config = await config_manager.load_pipeline_config(job_name)
            if not job_config:
                self.logger.error(f"Job '{job_name}' not found in any configured store")
                sys.exit(1)
            
            data_storage = await config_manager.load_datastores_config()
            if not data_storage:
                self.logger.error("No datastores configuration found")
                sys.exit(1)
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            traceback.print_exc()
            sys.exit(1)
        
        # Find the populate stage (auto-detect)
        populate_stages = [stage for stage in job_config.stages if stage.type == 'populate']
        
        if not populate_stages:
            self.logger.error(f"No populate stage found in job '{job_name}'")
            sys.exit(1)
        
        if len(populate_stages) > 1:
            stage_names = [s.name for s in populate_stages]
            self.logger.error(f"Multiple populate stages found in job '{job_name}': {stage_names}")
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
            auto_apply = yes or apply
            
            # First, always generate DDL without applying to show the user
            result = await schema_manager.ensure_table_schema(
                datastore=datastore,
                columns=stage_config.destination.columns,
                table_name=stage_config.destination.table,
                schema_name=stage_config.destination.schema,
                apply=False,  # Always preview first
                if_not_exists=if_not_exists
            )
            
            # Output results
            click.echo(f"\n{'='*80}")
            click.echo(f"Job: {job_name}")
            click.echo(f"Stage: {stage_config.name}")
            click.echo(f"Table: {result['table_name']}")
            click.echo(f"Action: {result['action']}")
            click.echo(f"Table Exists: {result['table_exists']}")
            click.echo(f"{'='*80}\n")
            
            if result['changes']:
                click.echo(f"Changes required:")
                for change in result['changes']:
                    click.echo(f"  - {change.get('description', change['type'])}")
                click.echo()
            
            if result['ddl_statements']:
                click.echo(f"DDL Statements:\n")
                for ddl in result['ddl_statements']:
                    click.echo(ddl)
                    click.echo()
                
                # Determine whether to apply changes
                should_apply = False
                
                if auto_apply:
                    # Auto-apply mode (--yes or --apply flag)
                    should_apply = True
                    if yes:
                        click.echo("Auto-applying changes (--yes flag)...")
                    else:
                        click.echo("Auto-applying changes (--apply flag, consider using --yes)...")
                else:
                    # Interactive mode - prompt user
                    should_apply = click.confirm("\nDo you want to apply these changes?")
                
                # Apply changes if approved
                if should_apply:
                    self.logger.info("Applying DDL changes...")
                    await schema_manager.ensure_table_schema(
                        datastore=datastore,
                        columns=stage_config.destination.columns,
                        table_name=stage_config.destination.table,
                        schema_name=stage_config.destination.schema,
                        apply=True,  # Apply changes
                        if_not_exists=if_not_exists
                    )
                    click.echo(f"\n✓ DDL applied successfully\n")
                else:
                    click.echo(f"\nNo changes applied\n")
            else:
                click.echo("No DDL changes required - table schema matches config\n")
            
        except Exception as e:
            self.logger.error(f"Error generating DDL: {e}")
            traceback.print_exc()
            sys.exit(1)
        finally:
            await datastore.disconnect(self.logger)


@click.group()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set the logging level')
@click.pass_context
def cli(ctx, global_config, log_level):
    """Synctool Sync CLI - Run individual sync jobs"""
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
    else:
        global_cfg = load_global_config()
    
    ctx.ensure_object(dict)
    ctx.obj['global_config'] = global_cfg
    ctx.obj['cli'] = SyncCLI(global_cfg)


@cli.command()
@click.option('--job-name', required=True, help='Name of the job to run')
@click.option('--strategy', help='Strategy to use (optional)')
@click.option('--bounds', help='JSON string with bounds data. Format: [{"column":"col1","start":"val1","end":"val2"}, {"column":"col2","value":[1,2,3]}]')
@click.option('--no-wait-lock', is_flag=True, help='Do not wait for locks (fail immediately if locked)')
@click.option('--lock-timeout', type=int, help='Maximum time to wait for locks in seconds (overrides config)')
@click.pass_context
def run(ctx, job_name, strategy, bounds, no_wait_lock, lock_timeout):
    """Run a specific sync job"""
    cli_instance = ctx.obj['cli']
    try:
        asyncio.run(cli_instance.run_job(
            job_name, strategy, bounds, no_wait_lock, lock_timeout
        ))
    finally:
        # Cleanup config manager
        asyncio.run(cli_instance._cleanup_config_manager())


@cli.command()
@click.pass_context
def list(ctx):
    """List all available jobs from all configured stores"""
    cli_instance = ctx.obj['cli']
    try:
        asyncio.run(cli_instance.list_jobs())
    finally:
        # Cleanup config manager
        asyncio.run(cli_instance._cleanup_config_manager())


@cli.command('generate-ddl')
@click.option('--job-name', required=True, help='Name of the job')
@click.option('--apply', is_flag=True, help='DEPRECATED: Apply without prompting. Use --yes instead')
@click.option('--yes', '-y', is_flag=True, help='Automatically approve and apply DDL changes without prompting')
@click.option('--if-not-exists', is_flag=True, help='Add IF NOT EXISTS clause for CREATE TABLE')
@click.pass_context
def generate_ddl(ctx, job_name, apply, yes, if_not_exists):
    """Generate DDL for populate stage destination table (auto-detects populate stage)"""
    cli_instance = ctx.obj['cli']
    try:
        asyncio.run(cli_instance.generate_ddl(job_name, yes, apply, if_not_exists))
    finally:
        # Cleanup config manager
        asyncio.run(cli_instance._cleanup_config_manager())


if __name__ == "__main__":
    cli()
