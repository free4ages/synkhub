#!/usr/bin/env python3
"""
Main entry point for the SyncTool application.
"""
import argparse
import asyncio
import logging
import sys
from typing import Dict, Any

from .config import ConfigLoader
from .manager import SyncJobManager
from .core import SyncProgress


def setup_logging(level: int = logging.INFO):
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def progress_callback(job_name: str, progress: SyncProgress):
    """Callback function to track sync progress."""
    print(f"Job {job_name}: {progress.completed_partitions}/{progress.total_partitions} partitions completed")
    print(f"Rows processed: {progress.rows_processed}")


async def run_sync_job(config_path: str, strategy: str = None, start: Any = None, end: Any = None):
    """Run a sync job from a configuration file."""
    try:
        # Load configuration
        config = ConfigLoader.load_from_yaml(config_path)
        
        # Validate configuration
        issues = ConfigLoader.validate_config(config)
        if issues:
            print("Configuration validation issues:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        # Create job manager
        job_manager = SyncJobManager(max_concurrent_jobs=2)
        
        # Run sync job
        result = await job_manager.run_sync_job(
            config=config,
            strategy=strategy,
            start=start,
            end=end,
            progress_callback=progress_callback
        )
        
        print(f"Sync completed: {result}")
        return True
        
    except Exception as e:
        print(f"Sync failed: {str(e)}")
        logging.exception("Sync job failed")
        return False


def main():
    """Main function to parse arguments and run the application."""
    parser = argparse.ArgumentParser(description="SyncTool - Data synchronization utility")
    parser.add_argument("config", help="Path to the YAML configuration file")
    parser.add_argument("--strategy", help="Sync strategy to use (full, hash, delta)")
    parser.add_argument("--start", help="Start boundary for sync")
    parser.add_argument("--end", help="End boundary for sync")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(log_level)
    
    # Run the sync job
    success = asyncio.run(run_sync_job(args.config, args.strategy, args.start, args.end))
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
