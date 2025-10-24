"""
Detects changes in pipeline configurations.
"""
from pathlib import Path
from typing import Dict, Set
import logging

from ...core.models import PipelineJobConfig, DataStorage
from .models import ConfigChanges, ChangeType
from .hasher import ConfigHasher
from .scanner import ConfigScanner
from .metadata import MetadataManager


class ChangeDetector:
    """Detects changes between user configs and built configs"""
    
    def __init__(
        self,
        scanner: ConfigScanner,
        metadata_manager: MetadataManager,
        hasher: ConfigHasher
    ):
        """
        Initialize change detector.
        
        Args:
            scanner: ConfigScanner instance
            metadata_manager: MetadataManager instance
            hasher: ConfigHasher instance
        """
        self.scanner = scanner
        self.metadata_manager = metadata_manager
        self.hasher = hasher
        self.logger = logging.getLogger(__name__)
    
    def detect_changes(
        self,
        datastores: DataStorage
    ) -> ConfigChanges:
        """
        Detect changes between user configs and built configs.
        
        Args:
            datastores: Current datastores configuration
            
        Returns:
            ConfigChanges object with categorized changes
        """
        changes = ConfigChanges()
        
        # Scan user configs
        user_configs = self.scanner.scan_pipeline_configs()
        user_config_names = set(user_configs.keys())
        
        # Get built configs
        built_config_names = set(self.metadata_manager.list_built_configs())
        
        # Compute datastore hashes
        datastore_hashes = self._compute_datastore_hashes(datastores)
        
        # Process each user config
        for config_name, config_path in user_configs.items():
            # Load config
            config = self.scanner.load_config(config_path)
            if not config:
                self.logger.warning(f"Failed to load config: {config_name}")
                continue
            
            # Check if enabled
            if not self.scanner.is_config_enabled(config):
                changes.disabled.append(config_name)
                self.logger.debug(f"Config is disabled: {config_name}")
                continue
            
            # Load existing metadata
            metadata = self.metadata_manager.load_metadata(config_name)
            
            if not metadata:
                # New config
                changes.new.append(config_name)
                self.logger.info(f"New config detected: {config_name}")
                continue
            
            # Compute current hash
            current_hash = self.hasher.compute_config_hash(config)
            
            # Check if config hash changed
            if current_hash != metadata.config_hash:
                changes.modified.append(config_name)
                self.logger.info(f"Modified config detected: {config_name}")
                continue
            
            # Check if any referenced datastore changed
            current_datastore_refs = self.scanner.extract_datastore_refs(config)
            datastore_changed = self._check_datastore_changes(
                current_datastore_refs,
                metadata.datastore_hashes,
                datastore_hashes
            )
            
            if datastore_changed:
                changes.datastore_changed.append(config_name)
                self.logger.info(
                    f"Datastore dependency changed for config: {config_name}"
                )
                continue
            
            # No changes
            changes.unchanged.append(config_name)
            self.logger.debug(f"Config unchanged: {config_name}")
        
        # Find orphaned configs (in built/ but not in user configs)
        orphaned = built_config_names - user_config_names
        changes.orphaned.extend(sorted(orphaned))
        
        if orphaned:
            self.logger.info(f"Orphaned configs detected: {len(orphaned)}")
        
        self.logger.info(
            f"Change detection complete: "
            f"new={len(changes.new)}, modified={len(changes.modified)}, "
            f"datastore_changed={len(changes.datastore_changed)}, "
            f"disabled={len(changes.disabled)}, orphaned={len(changes.orphaned)}, "
            f"unchanged={len(changes.unchanged)}"
        )
        
        return changes
    
    def _compute_datastore_hashes(
        self,
        datastores: DataStorage
    ) -> Dict[str, str]:
        """
        Compute hashes for all datastores.
        
        Args:
            datastores: DataStorage object
            
        Returns:
            Dict mapping datastore name to hash
        """
        datastore_hashes = {}
        
        for name, datastore in datastores.datastores.items():
            try:
                hash_value = self.hasher.compute_datastore_hash(datastore)
                datastore_hashes[name] = hash_value
            except Exception as e:
                self.logger.error(f"Failed to hash datastore {name}: {e}")
                # Use empty string as fallback
                datastore_hashes[name] = ""
        
        return datastore_hashes
    
    def _check_datastore_changes(
        self,
        current_refs: list,
        old_hashes: Dict[str, str],
        new_hashes: Dict[str, str]
    ) -> bool:
        """
        Check if any referenced datastores have changed.
        
        Args:
            current_refs: List of current datastore references
            old_hashes: Old datastore hashes from metadata
            new_hashes: Current datastore hashes
            
        Returns:
            True if any referenced datastore changed
        """
        for ref in current_refs:
            old_hash = old_hashes.get(ref)
            new_hash = new_hashes.get(ref)
            
            if old_hash != new_hash:
                self.logger.debug(
                    f"Datastore {ref} changed: {old_hash} -> {new_hash}"
                )
                return True
        
        return False

