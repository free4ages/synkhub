"""
Main build manager that orchestrates config building.
"""
import uuid
import os
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from ...core.models import PipelineJobConfig, DataStorage
from ...utils.schema_manager import SchemaManager
from ..config_loader import ConfigLoader
from ..config_serializer import ConfigSerializer
from .models import (
    BuildStatus,
    ConfigMetadata,
    ValidationResult,
    DDLCheckResult,
    BuildResult,
    BuildReport
)
from .hasher import ConfigHasher
from .scanner import ConfigScanner
from .change_detector import ChangeDetector
from .metadata import MetadataManager
from .lock import BuildLockManager


class ConfigBuildManager:
    """
    Main orchestrator for config building and validation.
    """
    
    def __init__(
        self,
        user_config_dir: Path,
        built_config_dir: Path,
        datastores_path: Path,
        ddl_check_on_build: str = "required",  # "required" | "optional" | "skip"
        on_build_failure: str = "keep_old",  # "keep_old" | "remove"
        remove_orphaned_configs: bool = True,
        allow_empty_source_dir: bool = True,
        build_lock_timeout: int = 30
    ):
        """
        Initialize config build manager.
        
        Args:
            user_config_dir: Directory containing user pipeline configs
            built_config_dir: Directory where built configs are saved
            datastores_path: Path to datastores.yaml
            ddl_check_on_build: How to handle DDL checks during build
            on_build_failure: What to do when build fails
            remove_orphaned_configs: Whether to remove orphaned configs
            allow_empty_source_dir: Whether to allow empty source directory
            build_lock_timeout: Timeout for build lock acquisition
        """
        self.user_config_dir = Path(user_config_dir)
        self.built_config_dir = Path(built_config_dir)
        self.datastores_path = Path(datastores_path)
        self.ddl_check_on_build = ddl_check_on_build
        self.on_build_failure = on_build_failure
        self.remove_orphaned_configs = remove_orphaned_configs
        self.allow_empty_source_dir = allow_empty_source_dir
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.scanner = ConfigScanner(user_config_dir, datastores_path)
        self.hasher = ConfigHasher()
        self.metadata_manager = MetadataManager(built_config_dir)
        self.lock_manager = BuildLockManager(built_config_dir, build_lock_timeout)
        self.change_detector = ChangeDetector(
            self.scanner,
            self.metadata_manager,
            self.hasher
        )
        self.schema_manager = SchemaManager(self.logger)
    
    async def build_all_changed_configs(self) -> BuildReport:
        """
        Main entry point for scheduler startup.
        Detects changes, validates, and builds configs.
        
        Returns:
            BuildReport with results
        """
        self.logger.info("Starting build process...")
        
        async with self.lock_manager:
            # Bootstrap check
            if not self.scanner.has_configs():
                if self.allow_empty_source_dir:
                    self.logger.warning("No user configs found, skipping build process")
                    return BuildReport()
                else:
                    raise ValueError("No user configs found in source directory")
            
            # Load datastores
            datastores = self.scanner.load_datastores()
            if not datastores:
                raise ValueError("Failed to load datastores configuration")
            
            # Detect changes
            self.logger.info("Detecting config changes...")
            changes = self.change_detector.detect_changes(datastores)
            
            # Remove disabled/orphaned configs
            removed = await self._remove_configs(
                changes.get_configs_to_remove()
            )
            
            # Process configs that need building
            configs_to_build = changes.get_configs_to_build()
            
            if not configs_to_build:
                self.logger.info("No configs require building")
                return BuildReport(
                    removed=removed,
                    unchanged=changes.unchanged
                )
            
            self.logger.info(f"Building {len(configs_to_build)} configs...")
            
            # Validate configs
            validation_results = await self._validate_configs(
                configs_to_build,
                datastores
            )
            
            # Check DDL requirements
            ddl_results = await self._check_ddl_requirements(
                validation_results['valid'],
                datastores
            )
            
            # Build auto-buildable configs
            build_results = await self._build_configs_atomic(
                ddl_results['auto_build'],
                datastores
            )
            
            # Track configs requiring manual deployment
            await self._track_manual_deploy_configs(
                ddl_results['requires_manual_deploy']
            )
            
            # Generate report
            report = BuildReport(
                successful=build_results['successful'],
                requires_manual_deploy=ddl_results['requires_manual_deploy'],
                failed_validation=validation_results['invalid'],
                failed_build=build_results['failed'],
                removed=removed,
                unchanged=changes.unchanged
            )
            
            self.logger.info("Build process complete")
            return report
    
    async def _remove_configs(self, config_names: List[str]) -> List[str]:
        """Remove configs from built directory"""
        removed = []
        
        for config_name in config_names:
            if self.metadata_manager.delete_config(config_name):
                removed.append(config_name)
                self.logger.info(f"Removed config: {config_name}")
            else:
                self.logger.warning(f"Failed to remove config: {config_name}")
        
        return removed
    
    async def _validate_configs(
        self,
        config_names: List[str],
        datastores: DataStorage
    ) -> Dict[str, List]:
        """
        Validate configs.
        
        Returns:
            Dict with 'valid' and 'invalid' lists
        """
        valid = []
        invalid = []
        
        user_configs = self.scanner.scan_pipeline_configs()
        
        for config_name in config_names:
            config_path = user_configs.get(config_name)
            if not config_path:
                invalid.append({
                    'name': config_name,
                    'error': 'Config file not found'
                })
                continue
            
            # Load config
            config = self.scanner.load_config(config_path)
            if not config:
                invalid.append({
                    'name': config_name,
                    'error': 'Failed to parse config'
                })
                continue
            
            # Validate config
            try:
                issues = ConfigLoader.validate_pipeline_with_datastores(config, datastores)
                if issues:
                    invalid.append({
                        'name': config_name,
                        'error': ', '.join(issues)
                    })
                    self.logger.error(f"Validation failed for {config_name}: {issues}")
                else:
                    valid.append({
                        'name': config_name,
                        'config': config,
                        'path': config_path
                    })
                    self.logger.debug(f"Validation passed for {config_name}")
                    
            except Exception as e:
                invalid.append({
                    'name': config_name,
                    'error': str(e)
                })
                self.logger.error(f"Validation error for {config_name}: {e}")
        
        return {
            'valid': valid,
            'invalid': invalid
        }
    
    async def _check_ddl_requirements(
        self,
        valid_configs: List[Dict],
        datastores: DataStorage
    ) -> Dict[str, List]:
        """
        Check DDL requirements for valid configs.
        
        Returns:
            Dict with 'auto_build' and 'requires_manual_deploy' lists
        """
        auto_build = []
        requires_manual_deploy = []
        
        for config_info in valid_configs:
            config_name = config_info['name']
            config = config_info['config']
            
            # Check if config has populate stage
            if not self.scanner.has_populate_stage(config):
                # No populate stage, safe to auto-build
                auto_build.append(config_info)
                continue
            
            # Check DDL based on configuration
            if self.ddl_check_on_build == "skip":
                # Skip DDL check, auto-build
                auto_build.append(config_info)
                self.logger.debug(f"Skipping DDL check for {config_name} (configured)")
                continue
            
            # Perform DDL check
            ddl_result = await self._check_config_ddl(config, datastores)
            
            if ddl_result['connection_error']:
                # Connection failed
                if self.ddl_check_on_build == "optional":
                    auto_build.append(config_info)
                    self.logger.warning(
                        f"DDL check failed for {config_name} (connection error), "
                        f"building anyway (optional mode)"
                    )
                else:
                    requires_manual_deploy.append({
                        'name': config_name,
                        'reason': f"Connection error: {ddl_result['connection_error']}",
                        'config_info': config_info,
                        'ddl_statements': []
                    })
                    self.logger.warning(
                        f"DDL check failed for {config_name} (connection error), "
                        f"requires manual deploy"
                    )
                continue
            
            if ddl_result['requires_ddl']:
                # DDL changes required
                requires_manual_deploy.append({
                    'name': config_name,
                    'reason': f"DDL changes required: {len(ddl_result['changes'])} change(s)",
                    'config_info': config_info,
                    'ddl_statements': ddl_result['ddl_statements']
                })
                self.logger.info(
                    f"Config {config_name} requires DDL changes, skipping auto-build"
                )
            else:
                # No DDL changes needed
                auto_build.append(config_info)
                self.logger.debug(f"No DDL changes for {config_name}")
        
        return {
            'auto_build': auto_build,
            'requires_manual_deploy': requires_manual_deploy
        }
    
    async def _check_config_ddl(
        self,
        config: PipelineJobConfig,
        datastores: DataStorage
    ) -> Dict:
        """Check DDL requirements for a config"""
        result = {
            'requires_ddl': False,
            'table_exists': False,
            'ddl_statements': [],
            'changes': [],
            'connection_error': None
        }
        
        # Find populate stage
        populate_stage = None
        for stage in config.stages:
            if stage.type == 'populate':
                populate_stage = stage
                break
        
        if not populate_stage:
            return result
        
        if not populate_stage.destination:
            return result
        
        # Get datastore
        datastore = datastores.get_datastore(populate_stage.destination.datastore_name)
        if not datastore:
            result['connection_error'] = f"Datastore not found: {populate_stage.destination.datastore_name}"
            return result
        
        # Connect to datastore
        try:
            await datastore.connect(self.logger)
            
            # Use SchemaManager to check DDL
            schema_result = await self.schema_manager.ensure_table_schema(
                datastore=datastore,
                columns=populate_stage.destination.columns,
                table_name=populate_stage.destination.table,
                schema_name=populate_stage.destination.schema,
                apply=False,  # Just check, don't apply
                if_not_exists=False
            )
            
            result['table_exists'] = schema_result.get('table_exists', False)
            result['ddl_statements'] = schema_result.get('ddl_statements', [])
            result['changes'] = schema_result.get('changes', [])
            result['requires_ddl'] = bool(result['ddl_statements'])
            
        except Exception as e:
            result['connection_error'] = str(e)
            self.logger.error(f"DDL check failed: {e}")
        
        finally:
            try:
                await datastore.disconnect(self.logger)
            except:
                pass
        
        return result
    
    async def _build_configs_atomic(
        self,
        configs_to_build: List[Dict],
        datastores: DataStorage
    ) -> Dict[str, List]:
        """
        Atomically build configs.
        
        Returns:
            Dict with 'successful' and 'failed' lists
        """
        successful = []
        failed = []
        
        user_configs = self.scanner.scan_pipeline_configs()
        
        for config_info in configs_to_build:
            config_name = config_info['name']
            config = config_info['config']
            config_path = config_info['path']
            
            try:
                # Build atomically
                success = await self._build_config_atomic(
                    config_name,
                    config,
                    config_path,
                    datastores
                )
                
                if success:
                    successful.append(config_name)
                else:
                    failed.append({
                        'name': config_name,
                        'error': 'Build failed (see logs)'
                    })
                    
            except Exception as e:
                failed.append({
                    'name': config_name,
                    'error': str(e)
                })
                self.logger.error(f"Failed to build {config_name}: {e}")
        
        return {
            'successful': successful,
            'failed': failed
        }
    
    async def _build_config_atomic(
        self,
        config_name: str,
        config: PipelineJobConfig,
        config_path: Path,
        datastores: DataStorage
    ) -> bool:
        """
        Atomically build and save a single config.
        Uses temp files + atomic rename.
        """
        temp_id = str(uuid.uuid4())[:8]
        temp_yaml = self.built_config_dir / f".temp_{temp_id}.yaml"
        temp_meta = self.built_config_dir / f".temp_{temp_id}.meta.json"
        
        final_yaml = self.metadata_manager.get_config_path(config_name)
        final_meta = self.metadata_manager.get_metadata_path(config_name)
        
        try:
            # Get previous version
            prev_metadata = self.metadata_manager.load_metadata(config_name)
            prev_version = prev_metadata.version if prev_metadata else 0
            
            # Extract info
            datastore_refs = self.scanner.extract_datastore_refs(config)
            datastore_hashes = {
                ref: self.hasher.compute_datastore_hash(datastores.get_datastore(ref))
                for ref in datastore_refs
                if datastores.get_datastore(ref)
            }
            
            # Serialize config
            config_dict = ConfigSerializer.config_to_dict(config)
            
            # Create metadata
            metadata = ConfigMetadata(
                name=config_name,
                version=prev_version + 1,
                source_path=self.scanner.get_relative_path(config_path),
                config_hash=self.hasher.compute_config_hash(config),
                datastore_refs=datastore_refs,
                datastore_hashes=datastore_hashes,
                built_at=datetime.now(timezone.utc).isoformat(),
                build_status=BuildStatus.SUCCESS.value,
                requires_manual_deploy=False,
                enabled=getattr(config, 'enabled', True),
                strategies_enabled=self.scanner.get_enabled_strategies(config),
                has_populate_stage=self.scanner.has_populate_stage(config)
            )
            
            # Write to temp files
            with open(temp_yaml, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False)
            
            with open(temp_meta, 'w') as f:
                import json
                json.dump(metadata.to_dict(), f, indent=2)
            
            # Validate both are readable
            with open(temp_yaml, 'r') as f:
                yaml.safe_load(f)
            with open(temp_meta, 'r') as f:
                import json
                json.load(f)
            
            # Atomic rename (POSIX guarantees atomicity)
            os.rename(str(temp_yaml), str(final_yaml))
            os.rename(str(temp_meta), str(final_meta))
            
            # Remove any pending metadata
            self.metadata_manager.remove_pending_metadata(config_name)
            
            self.logger.info(
                f"✅ Built config: {config_name} (version {metadata.version})"
            )
            return True
            
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            self.logger.error(f"Failed to build {config_name}: {e}")
            
            # Handle failure based on configuration
            if self.on_build_failure == "keep_old":
                if final_yaml.exists() and final_meta.exists():
                    self.logger.info(
                        f"   Keeping previous built config for {config_name}"
                    )
            elif self.on_build_failure == "remove":
                self.metadata_manager.delete_config(config_name)
                self.logger.info(f"   Removed failed config: {config_name}")
            
            return False
            
        finally:
            # Cleanup temp files
            for temp_file in [temp_yaml, temp_meta]:
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                    except:
                        pass
    
    async def _track_manual_deploy_configs(
        self,
        requires_manual_deploy: List[Dict]
    ):
        """Track configs that require manual deployment"""
        for item in requires_manual_deploy:
            config_name = item['name']
            reason = item['reason']
            config_info = item['config_info']
            ddl_statements = item.get('ddl_statements', [])
            
            config = config_info['config']
            config_path = config_info['path']
            
            # Extract info
            datastore_refs = self.scanner.extract_datastore_refs(config)
            
            # Save pending metadata
            self.metadata_manager.save_pending_metadata(
                config_name=config_name,
                reason=reason,
                source_path=self.scanner.get_relative_path(config_path),
                config_hash=self.hasher.compute_config_hash(config),
                datastore_refs=datastore_refs,
                ddl_statements=ddl_statements
            )
            
            self.logger.info(
                f"Tracked config requiring manual deploy: {config_name}"
            )

