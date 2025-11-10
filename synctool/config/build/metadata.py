"""
Manages metadata for built configs.
"""
import json
from pathlib import Path
from typing import Optional, Dict, List
import logging

from .models import ConfigMetadata, BuildStatus


class MetadataManager:
    """Manages reading/writing of config metadata"""
    
    def __init__(self, built_config_dir: Path):
        """
        Initialize metadata manager.
        
        Args:
            built_config_dir: Directory where built configs are stored
        """
        self.built_config_dir = Path(built_config_dir)
        self.logger = logging.getLogger(__name__)
        
        # Ensure directory exists
        self.built_config_dir.mkdir(parents=True, exist_ok=True)
    
    def get_metadata_path(self, config_name: str) -> Path:
        """Get path to metadata file for a config"""
        return self.built_config_dir / f"{config_name}.meta.json"
    
    def get_config_path(self, config_name: str) -> Path:
        """Get path to built config file"""
        return self.built_config_dir / f"{config_name}.yaml"
    
    def get_pending_metadata_path(self, config_name: str) -> Path:
        """Get path to pending metadata file"""
        return self.built_config_dir / f".pending_{config_name}.meta.json"
    
    def load_metadata(self, config_name: str) -> Optional[ConfigMetadata]:
        """
        Load metadata for a config.
        
        Args:
            config_name: Name of config
            
        Returns:
            ConfigMetadata or None if not found
        """
        metadata_path = self.get_metadata_path(config_name)
        
        if not metadata_path.exists():
            return None
        
        try:
            with open(metadata_path, 'r') as f:
                data = json.load(f)
            
            return ConfigMetadata.from_dict(data)
            
        except Exception as e:
            self.logger.error(f"Failed to load metadata for {config_name}: {e}")
            return None
    
    def save_metadata(self, metadata: ConfigMetadata) -> bool:
        """
        Save metadata for a config.
        
        Args:
            metadata: ConfigMetadata to save
            
        Returns:
            True if successful
        """
        metadata_path = self.get_metadata_path(metadata.name)
        
        try:
            with open(metadata_path, 'w') as f:
                json.dump(metadata.to_dict(), f, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save metadata for {metadata.name}: {e}")
            return False
    
    def save_pending_metadata(
        self,
        config_name: str,
        reason: str,
        source_path: str,
        config_hash: str,
        datastore_refs: List[str],
        ddl_statements: List[str] = None
    ) -> bool:
        """
        Save pending deployment metadata.
        
        Args:
            config_name: Name of config
            reason: Reason for pending deployment
            source_path: Relative path to source config
            config_hash: Hash of config
            datastore_refs: List of datastore references
            ddl_statements: Optional list of DDL statements
            
        Returns:
            True if successful
        """
        from datetime import datetime, timezone
        
        pending_path = self.get_pending_metadata_path(config_name)
        
        pending_data = {
            "name": config_name,
            "source_path": source_path,
            "config_hash": config_hash,
            "build_status": BuildStatus.REQUIRES_DEPLOY.value,
            "requires_manual_deploy": True,
            "manual_deploy_reason": reason,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "datastore_refs": datastore_refs,
            "ddl_required": bool(ddl_statements),
            "ddl_statements": ddl_statements or []
        }
        
        try:
            with open(pending_path, 'w') as f:
                json.dump(pending_data, f, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save pending metadata for {config_name}: {e}")
            return False
    
    def remove_pending_metadata(self, config_name: str) -> bool:
        """
        Remove pending deployment metadata.
        
        Args:
            config_name: Name of config
            
        Returns:
            True if successful or file doesn't exist
        """
        pending_path = self.get_pending_metadata_path(config_name)
        
        if not pending_path.exists():
            return True
        
        try:
            pending_path.unlink()
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove pending metadata for {config_name}: {e}")
            return False
    
    def list_built_configs(self) -> List[str]:
        """
        List all configs that have been built.
        
        Returns:
            List of config names
        """
        configs = []
        
        if not self.built_config_dir.exists():
            return configs
        
        # Find all .meta.json files (excluding .pending_)
        for metadata_file in self.built_config_dir.glob("*.meta.json"):
            if metadata_file.name.startswith(".pending_"):
                continue
            
            # Extract config name
            config_name = metadata_file.stem.replace(".meta", "")
            
            # Verify corresponding .yaml exists
            config_file = self.get_config_path(config_name)
            if config_file.exists():
                configs.append(config_name)
            else:
                self.logger.warning(
                    f"Found metadata without config file: {config_name}"
                )
        
        return configs
    
    def list_pending_configs(self) -> List[Dict[str, any]]:
        """
        List all configs pending manual deployment.
        
        Returns:
            List of pending config info dicts
        """
        pending = []
        
        if not self.built_config_dir.exists():
            return pending
        
        # Find all .pending_*.meta.json files
        for pending_file in self.built_config_dir.glob(".pending_*.meta.json"):
            try:
                with open(pending_file, 'r') as f:
                    data = json.load(f)
                pending.append(data)
            except Exception as e:
                self.logger.error(f"Failed to read pending file {pending_file}: {e}")
                continue
        
        return pending
    
    def config_exists(self, config_name: str) -> bool:
        """
        Check if a config has been built.
        
        Args:
            config_name: Name of config
            
        Returns:
            True if both .yaml and .meta.json exist
        """
        config_path = self.get_config_path(config_name)
        metadata_path = self.get_metadata_path(config_name)
        
        return config_path.exists() and metadata_path.exists()
    
    def delete_config(self, config_name: str) -> bool:
        """
        Delete a built config and its metadata.
        
        Args:
            config_name: Name of config
            
        Returns:
            True if successful
        """
        config_path = self.get_config_path(config_name)
        metadata_path = self.get_metadata_path(config_name)
        
        success = True
        
        try:
            if config_path.exists():
                config_path.unlink()
        except Exception as e:
            self.logger.error(f"Failed to delete config file for {config_name}: {e}")
            success = False
        
        try:
            if metadata_path.exists():
                metadata_path.unlink()
        except Exception as e:
            self.logger.error(f"Failed to delete metadata for {config_name}: {e}")
            success = False
        
        # Also remove pending metadata if exists
        self.remove_pending_metadata(config_name)
        
        return success

