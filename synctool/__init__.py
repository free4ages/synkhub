"""
Data Sync Tool - A comprehensive async data synchronization framework

Main modules:
- core: Core data models and enums
- providers: Database provider implementations  
- utils: Utility classes for partitioning, hashing, and comparison
- enrichment: Data enrichment and transformation engine
- sync: Main sync engine and job management
- config: Configuration loading and validation
"""

import asyncio
import logging
from .core.models import PipelineJobConfig, SyncProgress
from .sync.sync_engine import SyncEngine
from .sync.sync_job_manager import SyncJobManager
from .config.config_loader import ConfigLoader
from .config.config_manager import ConfigManager
from .config.config_store import ConfigStore, ConfigMetadata
from .config.file_config_store import FileConfigStore
from .config.database_config_store import DatabaseConfigStore
from .scheduler.config_aware_scheduler import ConfigAwareScheduler

__version__ = "1.0.0"
__all__ = [
    'PipelineJobConfig',
    'SyncProgress', 
    'SyncEngine',
    'SyncJobManager',
    'ConfigLoader',
    'ConfigManager',
    'ConfigStore',
    'ConfigMetadata',
    'FileConfigStore',
    'DatabaseConfigStore',
    'ConfigAwareScheduler'
]


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run example
    from ..examples.example_usage import run_example_sync
    asyncio.run(run_example_sync())