#!/usr/bin/env python3
"""
Synctool Scheduler CLI

This CLI provides commands to manage the Synctool scheduler daemon and jobs.
"""

import asyncio
import argparse
import logging
import traceback
import signal
import sys
from pathlib import Path
from typing import Optional
import yaml

from ..core.models import SchedulerConfig
from ..scheduler.file_scheduler import FileBasedScheduler
from ..monitoring.metrics_storage import MetricsStorage
from ..config.config_loader import ConfigLoader
from ..sync.sync_job_manager import SyncJobManager


class SchedulerCLI:
    """Command-line interface for Synctool scheduler"""
    
    def __init__(self):
        self.scheduler: Optional[FileBasedScheduler] = None
        self.running = False
        
    async def start_scheduler(self, args):
        """Start the scheduler daemon"""
        config = SchedulerConfig(
            enabled=True,
            redis_url=args.redis_url,
            lock_timeout=args.lock_timeout,
            config_dir=args.config_dir,
            metrics_dir=args.metrics_dir,
            logs_dir=args.logs_dir,
            max_runs_per_job=args.max_runs_per_job
        )
        
        # Setup file logging for daemon mode (console logging is already configured globally)
        file_handler = logging.FileHandler(Path(config.logs_dir) / "scheduler.log")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(file_handler)
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting Synctool scheduler with config directory: {config.config_dir}")
        
        # Create scheduler
        self.scheduler = FileBasedScheduler(config)
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Start scheduler
            await self.scheduler.start()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        finally:
            if self.scheduler:
                await self.scheduler.stop()
            logger.info("Scheduler stopped")
    
    async def list_jobs(self, args):
        """List all available jobs"""
        config = SchedulerConfig(config_dir=args.config_dir)
        scheduler = FileBasedScheduler(config)
        
        try:
            await scheduler.load_configs()
            job_configs = scheduler.get_job_configs()
            
            if not job_configs:
                print("No jobs found in config directory")
                return
            
            print(f"\nFound {len(job_configs)} job(s):")
            print("-" * 80)
            
            for job_name, job_config in job_configs.items():
                print(f"Job: {job_name}")
                print(f"  Description: {job_config.description}")
                print(f"  Partition Key: {job_config.partition_column}")
                print(f"  Strategies:")
                
                for strategy in job_config.strategies:
                    status = "✓" if strategy.get('enabled', True) else "✗"
                    cron = strategy.get('cron', 'No schedule')
                    print(f"    {status} {strategy.get('name', 'Unknown')} ({strategy.get('type', 'unknown')}) - {cron}")
                
                print()
        
        except Exception as e:
            print(f"Error listing jobs: {e}")
            sys.exit(1)
    

    
    async def show_status(self, args):
        """Show scheduler status and recent runs"""
        config = SchedulerConfig(
            config_dir=args.config_dir,
            metrics_dir=args.metrics_dir
        )
        
        try:
            # Load job configs
            scheduler = FileBasedScheduler(config)
            await scheduler.load_configs()
            job_configs = scheduler.get_job_configs()
            
            # Load metrics
            metrics_storage = MetricsStorage(config.metrics_dir, config.max_runs_per_job)
            
            print("Synctool Scheduler Status")
            print("=" * 50)
            print(f"Config Directory: {config.config_dir}")
            print(f"Metrics Directory: {config.metrics_dir}")
            print(f"Total Jobs: {len(job_configs)}")
            
            # Show job status
            print(f"\nJobs:")
            print("-" * 30)
            
            for job_name in job_configs.keys():
                recent_runs = metrics_storage.get_job_runs(job_name, limit=1)
                last_run = recent_runs[0] if recent_runs else None
                total_runs = len(metrics_storage.get_job_runs(job_name))
                
                status_icon = "🟢" if last_run and last_run.status == "completed" else "🔴" if last_run and last_run.status == "failed" else "⚪"
                
                print(f"{status_icon} {job_name}")
                print(f"   Total runs: {total_runs}")
                if last_run:
                    print(f"   Last run: {last_run.start_time.strftime('%Y-%m-%d %H:%M:%S')} ({last_run.status})")
                    if last_run.duration_seconds:
                        print(f"   Duration: {last_run.duration_seconds:.2f}s")
                else:
                    print(f"   Last run: Never")
                print()
        
        except Exception as e:
            print(f"Error getting status: {e}")
            sys.exit(1)
    
    async def validate_configs(self, args):
        """Validate all job configurations"""
        config_dir = Path(args.config_dir)
        
        if not config_dir.exists():
            print(f"Config directory does not exist: {config_dir}")
            sys.exit(1)
        
        print(f"Validating configurations in: {config_dir}")
        print("-" * 50)
        
        valid_count = 0
        invalid_count = 0
        
        for config_file in config_dir.glob("*.yaml"):
            try:
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
                
                # Load and validate config
                if config_data is None:
                    print(f"❌ {config_file.name}: ERROR - Empty or invalid YAML file")
                    invalid_count += 1
                    continue
                job_config = ConfigLoader.load_from_dict(config_data)
                issues = ConfigLoader.validate_config(job_config)
                
                if issues:
                    print(f"❌ {config_file.name}: INVALID")
                    for issue in issues:
                        print(f"   - {issue}")
                    invalid_count += 1
                else:
                    print(f"✅ {config_file.name}: VALID")
                    valid_count += 1
                
            except Exception as e:
                print(f"❌ {config_file.name}: ERROR - {e}")
                invalid_count += 1
        
        print(f"\nValidation Summary:")
        print(f"Valid configs: {valid_count}")
        print(f"Invalid configs: {invalid_count}")
        
        if invalid_count > 0:
            sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Synctool Scheduler CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the scheduler daemon
  python -m synctool.cli.scheduler_cli start --config-dir ./configs

  # List all available jobs
  python -m synctool.cli.scheduler_cli list --config-dir ./configs

  # Show scheduler status
  python -m synctool.cli.scheduler_cli status --config-dir ./configs

  # Validate all configurations
  python -m synctool.cli.scheduler_cli validate --config-dir ./configs

  # To run a specific job manually, use the sync CLI:
  python -m synctool.cli.sync_cli run --job-name my_job --strategy delta --config-dir ./configs
        """
    )
    
    # Add global arguments
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start the scheduler daemon')
    start_parser.add_argument('--config-dir', required=True, help='Directory containing job configs')
    start_parser.add_argument('--redis-url', default='redis://localhost:6379', help='Redis URL for locking')
    start_parser.add_argument('--lock-timeout', type=int, default=3600, help='Lock timeout in seconds')
    start_parser.add_argument('--metrics-dir', default='./data/metrics', help='Directory for metrics storage')
    start_parser.add_argument('--logs-dir', default='./data/logs', help='Directory for logs')
    start_parser.add_argument('--max-runs-per-job', type=int, default=50, help='Maximum runs to keep per job')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all available jobs')
    list_parser.add_argument('--config-dir', required=True, help='Directory containing job configs')
    

    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show scheduler status')
    status_parser.add_argument('--config-dir', default='./configs', help='Directory containing job configs')
    status_parser.add_argument('--metrics-dir', default='./data/metrics', help='Directory for metrics storage')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate job configurations')
    validate_parser.add_argument('--config-dir', required=True, help='Directory containing job configs')
    
    args = parser.parse_args()
    
    # Setup global logging configuration for all CLI commands
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
    cli = SchedulerCLI()
    
    try:
        if args.command == 'start':
            asyncio.run(cli.start_scheduler(args))
        elif args.command == 'list':
            asyncio.run(cli.list_jobs(args))

        elif args.command == 'status':
            asyncio.run(cli.show_status(args))
        elif args.command == 'validate':
            asyncio.run(cli.validate_configs(args))
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
