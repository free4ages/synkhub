from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
from ..core.models import PipelineJobConfig, DataStorage


@dataclass
class ConfigMetadata:
    """Metadata for a stored configuration"""
    name: str
    version: str = "1.0"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: List[str] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
        if self.tags is None:
            self.tags = []


class ConfigStore(ABC):
    """Abstract base class for configuration storage backends"""
    
    @abstractmethod
    async def save_pipeline_config(self, config: PipelineJobConfig, metadata: ConfigMetadata = None) -> bool:
        """Save a pipeline configuration"""
        pass
    
    @abstractmethod
    async def load_pipeline_config(self, name: str) -> Optional[PipelineJobConfig]:
        """Load a pipeline configuration by name"""
        pass
    
    @abstractmethod
    async def list_pipeline_configs(self, tags: List[str] = None) -> List[ConfigMetadata]:
        """List all pipeline configurations, optionally filtered by tags"""
        pass
    
    @abstractmethod
    async def delete_pipeline_config(self, name: str) -> bool:
        """Delete a pipeline configuration"""
        pass
    
    @abstractmethod
    async def save_datastores_config(self, datastores: DataStorage, metadata: ConfigMetadata = None) -> bool:
        """Save datastores configuration"""
        pass
    
    @abstractmethod
    async def load_datastores_config(self) -> Optional[DataStorage]:
        """Load datastores configuration"""
        pass
    
    @abstractmethod
    async def config_exists(self, name: str) -> bool:
        """Check if a configuration exists"""
        pass
    
    @abstractmethod
    async def get_config_metadata(self, name: str) -> Optional[ConfigMetadata]:
        """Get metadata for a configuration"""
        pass
