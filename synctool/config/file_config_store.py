import os
import yaml
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .config_store import ConfigStore, ConfigMetadata
from .config_loader import ConfigLoader
from .config_serializer import ConfigSerializer
from ..core.models import PipelineJobConfig, DataStorage


class FileConfigStore(ConfigStore):
    """File-based configuration store using YAML files"""
    
    def __init__(self, base_path: str, use_metadata_files: bool = True):
        self.base_path = Path(base_path)
        self.use_metadata_files = use_metadata_files
        self.pipelines_dir = self.base_path / "pipelines"
        self.datastores_file = self.base_path / "datastores.yaml"
        self.metadata_dir = self.base_path / "metadata"
        
        # Ensure directories exist
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        if self.use_metadata_files:
            self.metadata_dir.mkdir(parents=True, exist_ok=True)
    
    async def save_pipeline_config(self, config: PipelineJobConfig, metadata: ConfigMetadata = None) -> bool:
        """Save a pipeline configuration to YAML file"""
        try:
            config_file = self.pipelines_dir / f"{config.name}.yaml"
            
            # Convert config to dict for YAML serialization
            config_dict = ConfigSerializer.config_to_dict(config)
            
            with open(config_file, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
            
            # Save metadata if enabled
            if self.use_metadata_files and metadata:
                await self._save_metadata(config.name, metadata)
            
            return True
        except Exception as e:
            print(f"Failed to save pipeline config {config.name}: {e}")
            return False
    
    async def load_pipeline_config(self, name: str) -> Optional[PipelineJobConfig]:
        """Load a pipeline configuration from YAML file"""
        try:
            config_file = self.pipelines_dir / f"{name}.yaml"
            if not config_file.exists():
                return None
            
            return ConfigLoader.load_from_yaml(str(config_file))
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Failed to load pipeline config {name}: {e}")
            return None
    
    async def list_pipeline_configs(self, tags: List[str] = None) -> List[ConfigMetadata]:
        """List all pipeline configurations"""
        configs = []
        
        for config_file in self.pipelines_dir.glob("*.yaml"):
            name = config_file.stem
            metadata = await self.get_config_metadata(name)
            
            if metadata:
                # Filter by tags if specified
                if tags and not any(tag in metadata.tags for tag in tags):
                    continue
                configs.append(metadata)
        
        return configs
    
    async def delete_pipeline_config(self, name: str) -> bool:
        """Delete a pipeline configuration"""
        try:
            config_file = self.pipelines_dir / f"{name}.yaml"
            if config_file.exists():
                config_file.unlink()
            
            # Delete metadata if exists
            if self.use_metadata_files:
                metadata_file = self.metadata_dir / f"{name}.json"
                if metadata_file.exists():
                    metadata_file.unlink()
            
            return True
        except Exception as e:
            print(f"Failed to delete pipeline config {name}: {e}")
            return False
    
    async def save_datastores_config(self, datastores: DataStorage, metadata: ConfigMetadata = None) -> bool:
        """Save datastores configuration"""
        try:
            datastores_dict = ConfigSerializer.datastores_to_dict(datastores)
            
            with open(self.datastores_file, 'w') as f:
                yaml.dump(datastores_dict, f, default_flow_style=False, sort_keys=False)
            
            # Save metadata if enabled
            if self.use_metadata_files and metadata:
                await self._save_metadata("datastores", metadata)
            
            return True
        except Exception as e:
            print(f"Failed to save datastores config: {e}")
            return False
    
    async def load_datastores_config(self) -> Optional[DataStorage]:
        """Load datastores configuration"""
        try:
            if not self.datastores_file.exists():
                return None
            
            return ConfigLoader.load_datastores_from_yaml(str(self.datastores_file))
        except Exception as e:
            print(f"Failed to load datastores config: {e}")
            return None
    
    async def config_exists(self, name: str) -> bool:
        """Check if a configuration exists"""
        config_file = self.pipelines_dir / f"{name}.yaml"
        return config_file.exists()
    
    async def get_config_metadata(self, name: str) -> Optional[ConfigMetadata]:
        """Get metadata for a configuration"""
        if self.use_metadata_files:
            metadata_file = self.metadata_dir / f"{name}.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        data = json.load(f)
                    return ConfigMetadata(
                        name=data['name'],
                        version=data.get('version', '1.0'),
                        created_at=datetime.fromisoformat(data['created_at']),
                        updated_at=datetime.fromisoformat(data['updated_at']),
                        tags=data.get('tags', []),
                        description=data.get('description')
                    )
                except Exception:
                    pass
        
        # Fallback to file stats
        config_file = self.pipelines_dir / f"{name}.yaml"
        if config_file.exists():
            stat = config_file.stat()
            return ConfigMetadata(
                name=name,
                created_at=datetime.fromtimestamp(stat.st_ctime),
                updated_at=datetime.fromtimestamp(stat.st_mtime)
            )
        
        return None
    
    async def _save_metadata(self, name: str, metadata: ConfigMetadata):
        """Save metadata to JSON file"""
        metadata_file = self.metadata_dir / f"{name}.json"
        metadata_dict = {
            'name': metadata.name,
            'version': metadata.version,
            'created_at': metadata.created_at.isoformat(),
            'updated_at': metadata.updated_at.isoformat(),
            'tags': metadata.tags,
            'description': metadata.description
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata_dict, f, indent=2)
