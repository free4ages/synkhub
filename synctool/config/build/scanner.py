"""
Scans user config directories for pipeline configurations.
"""
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging

from ...core.models import PipelineJobConfig, DataStorage
from ..config_loader import ConfigLoader


class ConfigScanner:
    """Scans and loads pipeline configs from user directories"""
    
    def __init__(self, user_config_dir: Path, datastores_path: Path):
        """
        Initialize scanner.
        
        Args:
            user_config_dir: Root directory containing pipeline configs
            datastores_path: Path to datastores.yaml
        """
        self.user_config_dir = Path(user_config_dir)
        self.datastores_path = Path(datastores_path)
        self.logger = logging.getLogger(__name__)
    
    def has_configs(self) -> bool:
        """Check if user config directory has any YAML files"""
        if not self.user_config_dir.exists():
            return False
        
        yaml_files = list(self.user_config_dir.rglob("*.yaml"))
        yml_files = list(self.user_config_dir.rglob("*.yml"))
        
        return len(yaml_files) + len(yml_files) > 0
    
    def get_config_path(self, config_name: Union[str, Path]) -> Path:
        """
        Get path to config file.
        
        Args:
            config_name: Name of config
            
        Returns:
            Path to config file
        """
        if not self.user_config_dir.exists():
            self.logger.warning(f"Config directory does not exist: {self.user_config_dir}")
            return None
        if isinstance(config_name, Path):
            if config_name.exists():
                return config_name
            else:
                return None
        if isinstance(config_name, str) and (config_name.endswith('.yaml') or config_name.endswith('.yml')):
            config_path = Path(config_name)
            if config_path.exists():
                return config_path
            else:
                return None
        config_name_parts = config_name.split('.')
        config_name = config_name_parts[-1]
        config_base_path = self.user_config_dir
        for part in config_name_parts[:-1]:
            config_base_path = config_base_path / part
        
        config_path = config_base_path / f"{config_name}.yaml"
        if config_path.exists():
            return config_path
        config_path = config_base_path / f"{config_name}.yml"
        if config_path.exists():
            return config_path
        return None

    def scan_pipeline_configs(self) -> Dict[str, Path]:
        """
        Recursively scan for pipeline config files.
        
        Returns:
            Dict mapping pipeline name to file path
        """
        configs = {}
        
        if not self.user_config_dir.exists():
            self.logger.warning(f"Config directory does not exist: {self.user_config_dir}")
            return configs
        
        # Find all YAML files recursively
        yaml_files = list(self.user_config_dir.rglob("*.yaml"))
        yaml_files.extend(self.user_config_dir.rglob("*.yml"))
        
        for config_file in yaml_files:
            try:
                # Try to load and parse the config
                # config = ConfigLoader.load_from_yaml(str(config_file))
                # convert path to . separated string without extension
                config_name = ".".join(config_file.relative_to(self.user_config_dir).with_suffix('').parts)
                configs[config_name] = config_file
                self.logger.debug(f"Found config: {config_name} at {config_file}")
                
            except Exception as e:
                self.logger.debug(f"Skipping {config_file}: {e}")
                continue
        
        self.logger.info(f"Scanned {len(configs)} pipeline configs from {self.user_config_dir}")
        return configs
    
    def load_config(self, config_path: Path) -> Optional[PipelineJobConfig]:
        """
        Load a single config file.
        
        Args:
            config_path: Path to config file
            
        Returns:
            Parsed config or None if load fails
        """
        try:
            config = ConfigLoader.load_from_yaml(str(config_path))
            return config
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {e}")
            return None
    
    def load_datastores(self) -> Optional[DataStorage]:
        """
        Load datastores configuration.
        
        Returns:
            DataStorage object or None if load fails
        """
        try:
            if not self.datastores_path.exists():
                self.logger.error(f"Datastores file not found: {self.datastores_path}")
                return None
            
            datastores = ConfigLoader.load_datastores_from_yaml(str(self.datastores_path))
            self.logger.info(f"Loaded {len(datastores.datastores)} datastores")
            return datastores
            
        except Exception as e:
            self.logger.error(f"Failed to load datastores from {self.datastores_path}: {e}")
            return None
    
    def get_relative_path(self, config_path: Path) -> str:
        """
        Get relative path of config from user_config_dir.
        
        Args:
            config_path: Absolute path to config
            
        Returns:
            Relative path as string
        """
        try:
            return str(config_path.relative_to(self.user_config_dir))
        except ValueError:
            # If path is not relative to user_config_dir, return as-is
            return str(config_path)
    
    def extract_datastore_refs(self, config: PipelineJobConfig) -> List[str]:
        """
        Extract list of datastore names referenced by config.
        
        Args:
            config: Pipeline config
            
        Returns:
            List of datastore names
        """
        datastore_refs = set()
        
        # Extract from stages
        for stage in config.stages:
            if stage.source and stage.source.datastore_name:
                datastore_refs.add(stage.source.datastore_name)
            
            if stage.destination and stage.destination.datastore_name:
                datastore_refs.add(stage.destination.datastore_name)
        
        return sorted(list(datastore_refs))
    
    def get_enabled_strategies(self, config: PipelineJobConfig) -> List[str]:
        """
        Get list of enabled strategy names from config.
        
        Args:
            config: Pipeline config
            
        Returns:
            List of enabled strategy names
        """
        enabled_strategies = []
        
        # Check global strategies
        if hasattr(config, 'strategies') and config.strategies:
            for strategy in config.strategies:
                if getattr(strategy, 'enabled', True):
                    enabled_strategies.append(strategy.name)
        
        # Check stage-level strategies
        for stage in config.stages:
            if hasattr(stage, 'strategies') and stage.strategies:
                for strategy in stage.strategies:
                    if getattr(strategy, 'enabled', True) and strategy.name not in enabled_strategies:
                        enabled_strategies.append(strategy.name)
        
        return enabled_strategies
    
    def is_config_enabled(self, config: PipelineJobConfig) -> bool:
        """
        Check if config is enabled.
        
        Args:
            config: Pipeline config
            
        Returns:
            True if enabled (and has at least one enabled strategy)
        """
        # Check pipeline-level enabled flag
        if not getattr(config, 'enabled', True):
            return False
        
        # Check if at least one strategy is enabled
        enabled_strategies = self.get_enabled_strategies(config)
        
        return len(enabled_strategies) > 0
    
    def has_populate_stage(self, config: PipelineJobConfig) -> bool:
        """
        Check if config has a populate stage.
        
        Args:
            config: Pipeline config
            
        Returns:
            True if has populate stage
        """
        for stage in config.stages:
            if stage.type == 'populate':
                return True
        
        return False

