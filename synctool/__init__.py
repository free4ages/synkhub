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
from .core.models import SyncJobConfig, SyncProgress
from .sync.sync_engine import SyncEngine
from .sync.sync_job_manager import SyncJobManager
from .config.config_loader import ConfigLoader

__version__ = "1.0.0"
__all__ = [
    'SyncJobConfig',
    'SyncProgress', 
    'SyncEngine',
    'SyncJobManager',
    'ConfigLoader'
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