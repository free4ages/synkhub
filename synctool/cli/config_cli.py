#!/usr/bin/env python3
"""
CLI tool for managing pipeline configurations using ConfigManager
"""

import asyncio
import click
import logging
import sys
from pathlib import Path
from typing import Optional

from ..config.config_manager_factory import create_config_manager_from_global
from ..config.config_manager import ConfigManager, ConfigMetadata
from ..config.config_loader import ConfigLoader
from ..config.global_config_loader import load_global_config


class ConfigCLI:
    """Command-line interface for configuration management"""
    
    def __init__(self, global_config):
        self.global_config = global_config
        self.config_manager = None
        self.logger = logging.getLogger(__name__)
    
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
    
    async def list_configs(self, store_name: Optional[str] = None, tags: Optional[list] = None):
        """List configurations"""
        config_manager = await self._ensure_config_manager()
        configs_by_store = await config_manager.list_pipeline_configs(tags, store_name)
        
        if not configs_by_store:
            click.echo("No configurations found")
            return
        
        for store, configs in configs_by_store.items():
            click.echo(f"\nStore: {store}")
            click.echo("-" * (len(store) + 7))
            
            if not configs:
                click.echo("  No configurations")
                continue
            
            for config in configs:
                click.echo(f"  Name: {config.name}")
                click.echo(f"    Version: {config.version}")
                click.echo(f"    Created: {config.created_at}")
                click.echo(f"    Updated: {config.updated_at}")
                if config.tags:
                    click.echo(f"    Tags: {', '.join(config.tags)}")
                if config.description:
                    click.echo(f"    Description: {config.description}")
                click.echo()
    
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
            
            config_manager = await self._ensure_config_manager()
            success = await config_manager.save_pipeline_config(config, metadata, store_name)
            
            if success:
                click.echo(f"Successfully saved configuration '{config.name}'")
                return 0
            else:
                click.echo(f"Failed to save configuration '{config.name}'", err=True)
                return 1
        
        except Exception as e:
            click.echo(f"Error saving configuration: {e}", err=True)
            import traceback
            traceback.print_exc()
            return 1
    
    async def load_config(self, config_name: str, output_path: Optional[str] = None,
                         store_name: Optional[str] = None):
        """Load and optionally export a configuration"""
        try:
            config_manager = await self._ensure_config_manager()
            config = await config_manager.load_pipeline_config(config_name, store_name)
            
            if not config:
                click.echo(f"Configuration '{config_name}' not found", err=True)
                return 1
            
            if output_path:
                # Export to YAML file
                from ..config.config_serializer import ConfigSerializer
                import yaml
                
                config_dict = ConfigSerializer.config_to_dict(config)
                
                with open(output_path, 'w') as f:
                    yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
                
                click.echo(f"Exported configuration '{config_name}' to {output_path}")
            else:
                # Print to stdout
                click.echo(f"Configuration: {config.name}")
                click.echo(f"Description: {config.description}")
                click.echo(f"Columns: {len(config.columns)}")
                click.echo(f"Stages: {len(config.stages)}")
            
            return 0
        
        except Exception as e:
            click.echo(f"Error loading configuration: {e}", err=True)
            import traceback
            traceback.print_exc()
            return 1
    
    async def delete_config(self, config_name: str, store_name: Optional[str] = None):
        """Delete a configuration"""
        try:
            config_manager = await self._ensure_config_manager()
            results = await config_manager.delete_pipeline_config(config_name, store_name)
            
            if not results:
                click.echo(f"Configuration '{config_name}' not found", err=True)
                return 1
            
            for store, success in results.items():
                if success:
                    click.echo(f"Deleted '{config_name}' from store: {store}")
                else:
                    click.echo(f"Failed to delete '{config_name}' from store: {store}", err=True)
            
            return 0
        
        except Exception as e:
            click.echo(f"Error deleting configuration: {e}", err=True)
            import traceback
            traceback.print_exc()
            return 1
    
    async def backup_configs(self, from_store: str, to_store: str, tags: Optional[list] = None):
        """Backup configurations between stores"""
        try:
            config_manager = await self._ensure_config_manager()
            results = await config_manager.backup_configs(from_store, to_store, tags)
            
            if not results:
                click.echo(f"No configurations found in store: {from_store}", err=True)
                return 1
            
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            
            click.echo(f"Backup completed: {successful}/{total} configurations synced")
            
            for config_name, success in results.items():
                status = "✓" if success else "✗"
                click.echo(f"  {status} {config_name}")
            
            return 0
        
        except Exception as e:
            click.echo(f"Error backing up configurations: {e}", err=True)
            import traceback
            traceback.print_exc()
            return 1
    
    async def sync_config(self, config_name: str, from_store: str, to_store: str):
        """Sync a specific configuration between stores"""
        try:
            config_manager = await self._ensure_config_manager()
            success = await config_manager.sync_config(config_name, from_store, to_store)
            
            if success:
                click.echo(f"Successfully synced '{config_name}' from {from_store} to {to_store}")
                return 0
            else:
                click.echo(f"Failed to sync '{config_name}' from {from_store} to {to_store}", err=True)
                return 1
        
        except Exception as e:
            click.echo(f"Error syncing configuration: {e}", err=True)
            import traceback
            traceback.print_exc()
            return 1
    
    async def list_stores(self):
        """List all configured stores"""
        config_manager = await self._ensure_config_manager()
        
        store_names = config_manager.get_store_names()
        primary_store = config_manager.get_primary_store_name()
        
        if not store_names:
            click.echo("No stores configured")
            return
        
        click.echo(f"\nConfigured stores:")
        for store_name in store_names:
            primary_marker = " (PRIMARY)" if store_name == primary_store else ""
            click.echo(f"  - {store_name}{primary_marker}")
        
        click.echo(f"\nTotal: {len(store_names)} store(s)")


@click.group()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set the logging level')
@click.pass_context
def cli(ctx, global_config, log_level):
    """Pipeline Configuration Manager - All stores configured in global_config.yaml"""
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
    ctx.obj['cli'] = ConfigCLI(global_cfg)


@cli.command()
@click.option('--store', help='Specific store to list from')
@click.option('--tags', multiple=True, help='Filter by tags')
@click.pass_context
def list(ctx, store, tags):
    """List configurations"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.list_configs(store, list(tags) if tags else None))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


@cli.command()
@click.pass_context
def stores(ctx):
    """List all configured stores"""
    cli_instance = ctx.obj['cli']
    try:
        asyncio.run(cli_instance.list_stores())
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())


@cli.command()
@click.argument('config_path')
@click.option('--store', help='Store to save to (uses primary if not specified)')
@click.option('--tags', multiple=True, help='Tags to add')
@click.option('--description', help='Description')
@click.pass_context
def save(ctx, config_path, store, tags, description):
    """Save configuration from YAML file"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.save_config(
            config_path, store, list(tags) if tags else None, description
        ))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


@cli.command()
@click.argument('config_name')
@click.option('--output', help='Output YAML file path')
@click.option('--store', help='Store to load from')
@click.pass_context
def load(ctx, config_name, output, store):
    """Load configuration"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.load_config(config_name, output, store))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


@cli.command()
@click.argument('config_name')
@click.option('--store', help='Store to delete from (deletes from all stores if not specified)')
@click.pass_context
def delete(ctx, config_name, store):
    """Delete configuration"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.delete_config(config_name, store))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


@cli.command()
@click.argument('from_store')
@click.argument('to_store')
@click.option('--tags', multiple=True, help='Filter by tags')
@click.pass_context
def backup(ctx, from_store, to_store, tags):
    """Backup configurations between stores"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.backup_configs(
            from_store, to_store, list(tags) if tags else None
        ))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


@cli.command()
@click.argument('config_name')
@click.argument('from_store')
@click.argument('to_store')
@click.pass_context
def sync(ctx, config_name, from_store, to_store):
    """Sync specific configuration between stores"""
    cli_instance = ctx.obj['cli']
    try:
        return_code = asyncio.run(cli_instance.sync_config(config_name, from_store, to_store))
    finally:
        asyncio.run(cli_instance._cleanup_config_manager())
    sys.exit(return_code or 0)


if __name__ == "__main__":
    cli()
