"""Factory for creating ConfigManager from GlobalConfig"""
import logging
from typing import Optional
from .config_manager import ConfigManager
from .global_config_loader import GlobalConfig


async def create_config_manager_from_global(global_config: GlobalConfig) -> ConfigManager:
    """
    Create and initialize a ConfigManager from GlobalConfig
    
    Args:
        global_config: The global configuration
        
    Returns:
        Initialized ConfigManager instance
    """
    logger = logging.getLogger(__name__)
    config_manager = ConfigManager()
    
    for store_config in global_config.config_manager.stores:
        if store_config.type == "file":
            config_manager.add_file_store(
                name=store_config.name,
                base_path=store_config.base_path,
                is_primary=store_config.is_primary,
                use_metadata_files=store_config.use_metadata_files
            )
            logger.info(f"Added file store: {store_config.name} at {store_config.base_path}")
            
        elif store_config.type == "database":
            db_connection_config = {
                'type': store_config.db_type,
                'host': store_config.db_host,
                'port': store_config.db_port,
                'user': store_config.db_user,
                'password': store_config.db_password,
                'database': store_config.db_name
            }
            config_manager.add_database_store(
                name=store_config.name,
                connection_config=db_connection_config,
                is_primary=store_config.is_primary,
                table_prefix=store_config.table_prefix
            )
            logger.info(f"Added database store: {store_config.name}")
        else:
            logger.warning(f"Unknown store type: {store_config.type}, skipping store: {store_config.name}")
    
    # Initialize database stores
    await config_manager.initialize_database_stores()
    
    return config_manager
