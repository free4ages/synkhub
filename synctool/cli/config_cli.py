#!/usr/bin/env python3
"""
CLI tool for managing pipeline configurations using ConfigManager
"""

import asyncio
import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from ..config.config_manager import ConfigManager, ConfigMetadata
from ..config.config_loader import ConfigLoader


class ConfigCLI:
    """Command-line interface for configuration management"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
    
    async def setup_stores(self, file_store_path: Optional[str] = None, 
                          db_config: Optional[dict] = None):
        """Setup configuration stores"""
        if file_store_path:
            self.config_manager.add_file_store(
                name="file_store",
                base_path=file_store_path,
                is_primary=True
            )
            print(f"Added file store: {file_store_path}")
        
        if db_config:
            self.config_manager.add_database_store(
                name="db_store",
                connection_config=db_config,
                is_primary=not file_store_path  # Primary if no file store
            )
            await self.config_manager.initialize_database_stores()
            print(f"Added database store: {db_config['type']}://{db_config['host']}:{db_config.get('port')}/{db_config['database']}")
    
    async def list_configs(self, store_name: Optional[str] = None, tags: Optional[list] = None):
        """List configurations"""
        configs_by_store = await self.config_manager.list_pipeline_configs(tags, store_name)
        
        if not configs_by_store:
            print("No configurations found")
            return
        
        for store, configs in configs_by_store.items():
            print(f"\nStore: {store}")
            print("-" * (len(store) + 7))
            
            if not configs:
                print("  No configurations")
                continue
            
            for config in configs:
                print(f"  Name: {config.name}")
                print(f"    Version: {config.version}")
                print(f"    Created: {config.created_at}")
                print(f"    Updated: {config.updated_at}")
                if config.tags:
                    print(f"    Tags: {', '.join(config.tags)}")
                if config.description:
                    print(f"    Description: {config.description}")
                print()
    
    async def save_config(self, config_path: str, store_name: Optional[str] = None,
                         tags: Optional[list] = None, description: Optional[str] = None):
        """Save a configuration from YAML file"""
        try:
            config = ConfigLoader.load_from_yaml(config_path)
            
            metadata = ConfigMetadata(
                name=config.name,
                tags=tags or [],
                description=description or f"Loaded from {config_path}"
            )
            
            success = await self.config_manager.save_pipeline_config(config, metadata, store_name)
            if success:
                print(f"Successfully saved configuration '{config.name}'")
            else:
                print(f"Failed to save configuration '{config.name}'")
                return 1
        
        except Exception as e:
            print(f"Error saving configuration: {e}")
            return 1
        
        return 0
    
    async def load_config(self, config_name: str, output_path: Optional[str] = None,
                         store_name: Optional[str] = None):
        """Load and optionally export a configuration"""
        try:
            config = await self.config_manager.load_pipeline_config(config_name, store_name)
            
            if not config:
                print(f"Configuration '{config_name}' not found")
                return 1
            
            if output_path:
                # Export to YAML file
                from ..config.config_serializer import ConfigSerializer
                import yaml
                
                config_dict = ConfigSerializer.config_to_dict(config)
                
                with open(output_path, 'w') as f:
                    yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
                
                print(f"Exported configuration '{config_name}' to {output_path}")
            else:
                # Print to stdout
                print(f"Configuration: {config.name}")
                print(f"Description: {config.description}")
                print(f"Columns: {len(config.columns)}")
                print(f"Stages: {len(config.stages)}")
        
        except Exception as e:
            print(f"Error loading configuration: {e}")
            return 1
        
        return 0
    
    async def delete_config(self, config_name: str, store_name: Optional[str] = None):
        """Delete a configuration"""
        try:
            results = await self.config_manager.delete_pipeline_config(config_name, store_name)
            
            if not results:
                print(f"Configuration '{config_name}' not found")
                return 1
            
            for store, success in results.items():
                if success:
                    print(f"Deleted '{config_name}' from store: {store}")
                else:
                    print(f"Failed to delete '{config_name}' from store: {store}")
        
        except Exception as e:
            print(f"Error deleting configuration: {e}")
            return 1
        
        return 0
    
    async def backup_configs(self, from_store: str, to_store: str, tags: Optional[list] = None):
        """Backup configurations between stores"""
        try:
            results = await self.config_manager.backup_configs(from_store, to_store, tags)
            
            if not results:
                print(f"No configurations found in store: {from_store}")
                return 1
            
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            
            print(f"Backup completed: {successful}/{total} configurations synced")
            
            for config_name, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {config_name}")
        
        except Exception as e:
            print(f"Error backing up configurations: {e}")
            return 1
        
        return 0
    
    async def sync_config(self, config_name: str, from_store: str, to_store: str):
        """Sync a specific configuration between stores"""
        try:
            success = await self.config_manager.sync_config(config_name, from_store, to_store)
            
            if success:
                print(f"Successfully synced '{config_name}' from {from_store} to {to_store}")
            else:
                print(f"Failed to sync '{config_name}' from {from_store} to {to_store}")
                return 1
        
        except Exception as e:
            print(f"Error syncing configuration: {e}")
            return 1
        
        return 0
    
    async def close(self):
        """Close connections"""
        await self.config_manager.close()


async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Pipeline Configuration Manager")
    
    # Global options
    parser.add_argument("--file-store", help="Path to file-based config store")
    parser.add_argument("--db-type", choices=["postgres", "mysql"], help="Database type")
    parser.add_argument("--db-host", help="Database host")
    parser.add_argument("--db-port", type=int, help="Database port")
    parser.add_argument("--db-user", help="Database user")
    parser.add_argument("--db-password", help="Database password")
    parser.add_argument("--db-name", help="Database name")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List configurations")
    list_parser.add_argument("--store", help="Specific store to list from")
    list_parser.add_argument("--tags", nargs="*", help="Filter by tags")
    
    # Save command
    save_parser = subparsers.add_parser("save", help="Save configuration from YAML file")
    save_parser.add_argument("config_path", help="Path to YAML configuration file")
    save_parser.add_argument("--store", help="Store to save to")
    save_parser.add_argument("--tags", nargs="*", help="Tags to add")
    save_parser.add_argument("--description", help="Description")
    
    # Load command
    load_parser = subparsers.add_parser("load", help="Load configuration")
    load_parser.add_argument("config_name", help="Name of configuration to load")
    load_parser.add_argument("--output", help="Output YAML file path")
    load_parser.add_argument("--store", help="Store to load from")
    
    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete configuration")
    delete_parser.add_argument("config_name", help="Name of configuration to delete")
    delete_parser.add_argument("--store", help="Store to delete from")
    
    # Backup command
    backup_parser = subparsers.add_parser("backup", help="Backup configurations between stores")
    backup_parser.add_argument("from_store", help="Source store")
    backup_parser.add_argument("to_store", help="Destination store")
    backup_parser.add_argument("--tags", nargs="*", help="Filter by tags")
    
    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync specific configuration between stores")
    sync_parser.add_argument("config_name", help="Name of configuration to sync")
    sync_parser.add_argument("from_store", help="Source store")
    sync_parser.add_argument("to_store", help="Destination store")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Create CLI instance
    cli = ConfigCLI()
    
    try:
        # Setup stores
        db_config = None
        if args.db_type:
            db_config = {
                'type': args.db_type,
                'host': args.db_host or 'localhost',
                'port': args.db_port or (5432 if args.db_type == 'postgres' else 3306),
                'user': args.db_user or 'synctool',
                'password': args.db_password or 'password',
                'database': args.db_name or 'synctool_configs'
            }
        
        await cli.setup_stores(args.file_store, db_config)
        
        # Execute command
        if args.command == "list":
            return await cli.list_configs(args.store, args.tags)
        elif args.command == "save":
            return await cli.save_config(args.config_path, args.store, args.tags, args.description)
        elif args.command == "load":
            return await cli.load_config(args.config_name, args.output, args.store)
        elif args.command == "delete":
            return await cli.delete_config(args.config_name, args.store)
        elif args.command == "backup":
            return await cli.backup_configs(args.from_store, args.to_store, args.tags)
        elif args.command == "sync":
            return await cli.sync_config(args.config_name, args.from_store, args.to_store)
    
    except KeyboardInterrupt:
        print("\nOperation cancelled")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        await cli.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
