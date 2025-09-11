#!/usr/bin/env python3
"""
Example usage of the ConfigManager system for storing pipeline configs
in multiple locations including folders and OLTP databases.
"""

import asyncio
import logging
from pathlib import Path

from synctool.config.config_manager import ConfigManager, ConfigMetadata
from synctool.config.config_loader import ConfigLoader
from synctool.scheduler.config_aware_scheduler import ConfigAwareScheduler
from synctool.core.models import SchedulerConfig


async def basic_file_store_example():
    """Basic example using file-based config store"""
    print("\n=== Basic File Store Example ===")
    
    # Create ConfigManager
    config_manager = ConfigManager()
    
    # Add file-based store
    config_manager.add_file_store(
        name="local_files",
        base_path="./config_store_data/files",
        is_primary=True,
        use_metadata_files=True
    )
    
    # Load an existing config and save it to the store
    existing_config_path = Path("./examples/configs/pipeline_postgres_sync.yaml")
    if existing_config_path.exists():
        config = ConfigLoader.load_from_yaml(str(existing_config_path))
        
        # Create metadata
        metadata = ConfigMetadata(
            name=config.name,
            version="1.0",
            tags=["postgres", "sync", "example"],
            description="Example pipeline sync configuration"
        )
        
        # Save to store
        success = await config_manager.save_pipeline_config(config, metadata)
        print(f"Saved config to file store: {success}")
        
        # List configs
        configs = await config_manager.list_pipeline_configs()
        print(f"Configs in stores: {configs}")
        
        # Load config back
        loaded_config = await config_manager.load_pipeline_config(config.name)
        print(f"Loaded config: {loaded_config.name if loaded_config else 'None'}")
    
    else:
        print("Example config file not found, skipping file store demo")


async def database_store_example():
    """Example using database-based config store"""
    print("\n=== Database Store Example ===")
    
    # Create ConfigManager
    config_manager = ConfigManager()
    
    # Add database store (PostgreSQL)
    # Note: This requires a running PostgreSQL instance
    try:
        config_manager.add_database_store(
            name="postgres_store",
            connection_config={
                'type': 'postgres',
                'host': 'localhost',
                'port': 5432,
                'user': 'synctool',
                'password': 'password',
                'database': 'synctool_configs'
            },
            is_primary=True
        )
        
        # Initialize database tables
        await config_manager.initialize_database_stores()
        
        # Create a simple test config
        from synctool.core.models import PipelineJobConfig, Column, GlobalStageConfig
        from synctool.core.schema_models import UniversalDataType
        
        test_config = PipelineJobConfig(
            name="test_db_config",
            description="Test configuration stored in database",
            columns=[
                Column(name="id", expr="id", dtype=UniversalDataType.INTEGER, unique_column=True),
                Column(name="name", expr="name", dtype=UniversalDataType.VARCHAR)
            ],
            stages=[
                GlobalStageConfig(
                    name="test_stage",
                    type="change_detection",
                    enabled=True
                )
            ]
        )
        
        # Create metadata
        metadata = ConfigMetadata(
            name=test_config.name,
            version="1.0",
            tags=["test", "database"],
            description="Test configuration for database store"
        )
        
        # Save to database store
        success = await config_manager.save_pipeline_config(test_config, metadata)
        print(f"Saved config to database store: {success}")
        
        # List configs
        configs = await config_manager.list_pipeline_configs()
        print(f"Configs in stores: {configs}")
        
        # Load config back
        loaded_config = await config_manager.load_pipeline_config(test_config.name)
        print(f"Loaded config from database: {loaded_config.name if loaded_config else 'None'}")
        
    except Exception as e:
        print(f"Database store example failed (this is expected if PostgreSQL is not running): {e}")


async def multi_store_example():
    """Example using multiple stores with backup/sync functionality"""
    print("\n=== Multi-Store Example ===")
    
    # Create ConfigManager
    config_manager = ConfigManager()
    
    # Add multiple stores
    config_manager.add_file_store(
        name="primary_files",
        base_path="./config_store_data/primary",
        is_primary=True
    )
    
    config_manager.add_file_store(
        name="backup_files",
        base_path="./config_store_data/backup",
        is_primary=False
    )
    
    # Create test configurations
    from synctool.core.models import PipelineJobConfig, Column, GlobalStageConfig
    from synctool.core.schema_models import UniversalDataType
    
    configs_to_create = [
        {
            "name": "config_1",
            "description": "First test configuration",
            "tags": ["test", "config1"]
        },
        {
            "name": "config_2", 
            "description": "Second test configuration",
            "tags": ["test", "config2"]
        }
    ]
    
    # Save configs to primary store
    for config_data in configs_to_create:
        config = PipelineJobConfig(
            name=config_data["name"],
            description=config_data["description"],
            columns=[
                Column(name="id", expr="id", dtype=UniversalDataType.INTEGER, unique_column=True)
            ],
            stages=[
                GlobalStageConfig(name="test_stage", type="test", enabled=True)
            ]
        )
        
        metadata = ConfigMetadata(
            name=config.name,
            tags=config_data["tags"],
            description=config_data["description"]
        )
        
        success = await config_manager.save_pipeline_config(config, metadata, "primary_files")
        print(f"Saved {config.name} to primary store: {success}")
    
    # List configs in primary store
    primary_configs = await config_manager.list_pipeline_configs(store_name="primary_files")
    print(f"Primary store configs: {primary_configs}")
    
    # Backup configs from primary to backup store
    backup_results = await config_manager.backup_configs("primary_files", "backup_files")
    print(f"Backup results: {backup_results}")
    
    # List configs in backup store
    backup_configs = await config_manager.list_pipeline_configs(store_name="backup_files")
    print(f"Backup store configs: {backup_configs}")
    
    # Sync a specific config
    sync_result = await config_manager.sync_config("config_1", "primary_files", "backup_files")
    print(f"Sync result: {sync_result}")


async def scheduler_integration_example():
    """Example of using ConfigManager with the enhanced scheduler"""
    print("\n=== Scheduler Integration Example ===")
    
    # Create ConfigManager
    config_manager = ConfigManager()
    
    # Add file store with existing configs
    config_manager.add_file_store(
        name="scheduler_configs",
        base_path="./examples/configs",  # Use existing examples directory
        is_primary=True
    )
    
    # Create scheduler configuration
    scheduler_config = SchedulerConfig(
        enabled=True,
        redis_url="redis://localhost:6379",
        config_dir="./examples/configs",  # Not used by ConfigAwareScheduler
        metrics_dir="./data/metrics",
        logs_dir="./data/logs"
    )
    
    # Create config-aware scheduler
    scheduler = ConfigAwareScheduler(scheduler_config, config_manager)
    
    # Load configurations (without starting the scheduler loop)
    await scheduler.load_datastores()
    await scheduler.load_configs()
    
    # Get status
    status = await scheduler.get_config_status()
    print(f"Scheduler status: {status}")
    
    # List loaded job configs
    job_configs = scheduler.get_job_configs()
    print(f"Loaded job configurations: {list(job_configs.keys())}")
    
    # Don't actually start the scheduler in this example
    print("Scheduler integration example completed (scheduler not started)")


async def datastores_management_example():
    """Example of managing datastores configuration"""
    print("\n=== Datastores Management Example ===")
    
    # Create ConfigManager
    config_manager = ConfigManager()
    
    # Add file store
    config_manager.add_file_store(
        name="datastores_store",
        base_path="./config_store_data/datastores",
        is_primary=True
    )
    
    # Load existing datastores config if available
    existing_datastores_path = Path("./examples/configs/datastores.yaml")
    if existing_datastores_path.exists():
        datastores = ConfigLoader.load_datastores_from_yaml(str(existing_datastores_path))
        
        # Create metadata
        metadata = ConfigMetadata(
            name="datastores",
            version="1.0",
            description="Main datastores configuration"
        )
        
        # Save to store
        success = await config_manager.save_datastores_config(datastores, metadata)
        print(f"Saved datastores config: {success}")
        
        # Load back
        loaded_datastores = await config_manager.load_datastores_config()
        if loaded_datastores:
            print(f"Loaded datastores: {loaded_datastores.list_datastores()}")
        else:
            print("No datastores configuration found")
    
    else:
        print("Example datastores config file not found, skipping datastores demo")


async def main():
    """Run all examples"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("ConfigManager Usage Examples")
    print("=" * 50)
    
    try:
        # Run examples
        await basic_file_store_example()
        await database_store_example()
        await multi_store_example()
        await scheduler_integration_example()
        await datastores_management_example()
        
        print("\n=== All Examples Completed ===")
        
    except Exception as e:
        print(f"Example failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
