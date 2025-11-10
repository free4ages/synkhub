from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import logging

from .config_store import ConfigStore, ConfigMetadata
from .file_config_store import FileConfigStore
from .database_config_store import DatabaseConfigStore
from ..core.models import PipelineJobConfig, DataStorage


class ConfigManager:
    """Centralized configuration manager supporting multiple storage backends"""
    
    def __init__(self):
        self._stores: Dict[str, ConfigStore] = {}
        self._primary_store: Optional[str] = None
        self.logger = logging.getLogger(__name__)
    
    def add_store(self, name: str, store: ConfigStore, is_primary: bool = False):
        """Add a configuration store"""
        self._stores[name] = store
        if is_primary or self._primary_store is None:
            self._primary_store = name
        self.logger.info(f"Added config store: {name} (primary: {is_primary or self._primary_store == name})")
    
    def add_file_store(self, name: str, base_path: str, is_primary: bool = False, 
                      use_metadata_files: bool = True):
        """Add a file-based configuration store"""
        store = FileConfigStore(base_path, use_metadata_files)
        self.add_store(name, store, is_primary)
    
    def add_database_store(self, name: str, connection_config: dict, is_primary: bool = False,
                          table_prefix: str = "synctool_config"):
        """Add a database-based configuration store"""
        store = DatabaseConfigStore(connection_config, table_prefix)
        self.add_store(name, store, is_primary)
    
    async def initialize_database_stores(self):
        """Initialize database tables for all database stores"""
        for name, store in self._stores.items():
            if isinstance(store, DatabaseConfigStore):
                try:
                    await store.initialize_tables()
                    self.logger.info(f"Initialized database tables for store: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to initialize database tables for store {name}: {e}")
    
    async def save_pipeline_config(self, config: PipelineJobConfig, 
                                  metadata: ConfigMetadata = None,
                                  store_name: str = None) -> bool:
        """Save a pipeline configuration"""
        store = self._get_store(store_name)
        if not store:
            self.logger.error(f"Store not found: {store_name or self._primary_store}")
            return False
        
        if not metadata:
            metadata = ConfigMetadata(name=config.name)
        
        try:
            result = await store.save_pipeline_config(config, metadata)
            if result:
                self.logger.info(f"Saved pipeline config '{config.name}' to store: {store_name or self._primary_store}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to save pipeline config '{config.name}': {e}")
            return False
    
    async def load_pipeline_config(self, name: str, 
                                  store_name: str = None) -> Optional[PipelineJobConfig]:
        """Load a pipeline configuration"""
        if store_name:
            store = self._get_store(store_name)
            if store:
                try:
                    config = await store.load_pipeline_config(name)
                    if config:
                        self.logger.debug(f"Loaded pipeline config '{name}' from store: {store_name}")
                    return config
                except Exception as e:
                    self.logger.error(f"Failed to load pipeline config '{name}' from store {store_name}: {e}")
        else:
            # Try primary store first, then others
            primary_store = self._get_store(self._primary_store)
            if primary_store:
                try:
                    config = await primary_store.load_pipeline_config(name)
                    if config:
                        self.logger.debug(f"Loaded pipeline config '{name}' from primary store: {self._primary_store}")
                        return config
                except Exception as e:
                    self.logger.error(f"Failed to load pipeline config '{name}' from primary store: {e}")
            
            # Try other stores
            for store_name, store in self._stores.items():
                if store_name != self._primary_store:
                    try:
                        config = await store.load_pipeline_config(name)
                        if config:
                            self.logger.debug(f"Loaded pipeline config '{name}' from store: {store_name}")
                            return config
                    except Exception as e:
                        self.logger.error(f"Failed to load pipeline config '{name}' from store {store_name}: {e}")
        
        return None
    
    async def list_pipeline_configs(self, tags: List[str] = None,
                                   store_name: str = None) -> Dict[str, List[ConfigMetadata]]:
        """List pipeline configurations from all or specific stores"""
        results = {}
        
        if store_name:
            store = self._get_store(store_name)
            if store:
                try:
                    configs = await store.list_pipeline_configs(tags)
                    if configs:
                        results[store_name] = configs
                except Exception as e:
                    self.logger.error(f"Failed to list configs from store {store_name}: {e}")
        else:
            # List from all stores
            for name, store in self._stores.items():
                try:
                    configs = await store.list_pipeline_configs(tags)
                    if configs:
                        results[name] = configs
                except Exception as e:
                    self.logger.error(f"Failed to list configs from store {name}: {e}")
        
        return results
    
    async def delete_pipeline_config(self, name: str, 
                                    store_name: str = None) -> Dict[str, bool]:
        """Delete a pipeline configuration from stores"""
        results = {}
        
        if store_name:
            store = self._get_store(store_name)
            if store:
                try:
                    results[store_name] = await store.delete_pipeline_config(name)
                    if results[store_name]:
                        self.logger.info(f"Deleted pipeline config '{name}' from store: {store_name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete pipeline config '{name}' from store {store_name}: {e}")
                    results[store_name] = False
        else:
            # Delete from all stores that have the config
            for store_name, store in self._stores.items():
                try:
                    if await store.config_exists(name):
                        results[store_name] = await store.delete_pipeline_config(name)
                        if results[store_name]:
                            self.logger.info(f"Deleted pipeline config '{name}' from store: {store_name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete pipeline config '{name}' from store {store_name}: {e}")
                    results[store_name] = False
        
        return results
    
    async def save_datastores_config(self, datastores: DataStorage,
                                    metadata: ConfigMetadata = None,
                                    store_name: str = None) -> bool:
        """Save datastores configuration"""
        store = self._get_store(store_name)
        if not store:
            self.logger.error(f"Store not found: {store_name or self._primary_store}")
            return False
        
        if not metadata:
            metadata = ConfigMetadata(name="datastores")
        
        try:
            result = await store.save_datastores_config(datastores, metadata)
            if result:
                self.logger.info(f"Saved datastores config to store: {store_name or self._primary_store}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to save datastores config: {e}")
            return False
    
    async def load_datastores_config(self, store_name: str = None) -> Optional[DataStorage]:
        """Load datastores configuration"""
        if store_name:
            store = self._get_store(store_name)
            if store:
                try:
                    config = await store.load_datastores_config()
                    if config:
                        self.logger.debug(f"Loaded datastores config from store: {store_name}")
                    return config
                except Exception as e:
                    self.logger.error(f"Failed to load datastores config from store {store_name}: {e}")
        else:
            # Try primary store first, then others
            primary_store = self._get_store(self._primary_store)
            if primary_store:
                try:
                    config = await primary_store.load_datastores_config()
                    if config:
                        self.logger.debug(f"Loaded datastores config from primary store: {self._primary_store}")
                        return config
                except Exception as e:
                    self.logger.error(f"Failed to load datastores config from primary store: {e}")
            
            # Try other stores
            for store_name, store in self._stores.items():
                if store_name != self._primary_store:
                    try:
                        config = await store.load_datastores_config()
                        if config:
                            self.logger.debug(f"Loaded datastores config from store: {store_name}")
                            return config
                    except Exception as e:
                        self.logger.error(f"Failed to load datastores config from store {store_name}: {e}")
        
        return None
    
    async def sync_config(self, name: str, from_store: str, to_store: str) -> bool:
        """Sync a configuration between stores"""
        source_store = self._get_store(from_store)
        dest_store = self._get_store(to_store)
        
        if not source_store or not dest_store:
            self.logger.error(f"Store not found - from: {from_store}, to: {to_store}")
            return False
        
        try:
            # Load config and metadata from source
            config = await source_store.load_pipeline_config(name)
            metadata = await source_store.get_config_metadata(name)
            
            if not config:
                self.logger.warning(f"Config '{name}' not found in source store: {from_store}")
                return False
            
            # Save to destination
            result = await dest_store.save_pipeline_config(config, metadata)
            if result:
                self.logger.info(f"Synced config '{name}' from {from_store} to {to_store}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to sync config '{name}' from {from_store} to {to_store}: {e}")
            return False
    
    async def backup_configs(self, from_store: str, to_store: str, 
                           tags: List[str] = None) -> Dict[str, bool]:
        """Backup configurations from one store to another"""
        source_store = self._get_store(from_store)
        dest_store = self._get_store(to_store)
        
        if not source_store or not dest_store:
            self.logger.error(f"Store not found - from: {from_store}, to: {to_store}")
            return {}
        
        try:
            # List configs from source
            configs = await source_store.list_pipeline_configs(tags)
            results = {}
            
            # Sync each config
            for config_meta in configs:
                results[config_meta.name] = await self.sync_config(
                    config_meta.name, from_store, to_store
                )
            
            successful_count = sum(1 for success in results.values() if success)
            self.logger.info(f"Backup completed: {successful_count}/{len(results)} configs synced from {from_store} to {to_store}")
            
            return results
        except Exception as e:
            self.logger.error(f"Failed to backup configs from {from_store} to {to_store}: {e}")
            return {}
    
    async def sync_datastores_config(self, from_store: str, to_store: str) -> bool:
        """Sync datastores configuration between stores"""
        source_store = self._get_store(from_store)
        dest_store = self._get_store(to_store)
        
        if not source_store or not dest_store:
            self.logger.error(f"Store not found - from: {from_store}, to: {to_store}")
            return False
        
        try:
            # Load datastores config from source
            datastores = await source_store.load_datastores_config()
            
            if not datastores:
                self.logger.warning(f"Datastores config not found in source store: {from_store}")
                return False
            
            # Save to destination
            metadata = ConfigMetadata(name="datastores")
            result = await dest_store.save_datastores_config(datastores, metadata)
            if result:
                self.logger.info(f"Synced datastores config from {from_store} to {to_store}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to sync datastores config from {from_store} to {to_store}: {e}")
            return False
    
    async def close(self):
        """Close all database connections"""
        for name, store in self._stores.items():
            if isinstance(store, DatabaseConfigStore):
                try:
                    await store.close()
                    self.logger.info(f"Closed database store: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to close database store {name}: {e}")
    
    def _get_store(self, store_name: str = None) -> Optional[ConfigStore]:
        """Get a store by name, or primary store if name is None"""
        if store_name:
            return self._stores.get(store_name)
        elif self._primary_store:
            return self._stores.get(self._primary_store)
        else:
            return None
    
    def get_store_names(self) -> List[str]:
        """Get list of configured store names"""
        return list(self._stores.keys())
    
    def get_primary_store_name(self) -> Optional[str]:
        """Get the name of the primary store"""
        return self._primary_store
    
    def set_primary_store(self, store_name: str) -> bool:
        """Set the primary store"""
        if store_name in self._stores:
            self._primary_store = store_name
            self.logger.info(f"Set primary store to: {store_name}")
            return True
        else:
            self.logger.error(f"Cannot set primary store - store not found: {store_name}")
            return False
