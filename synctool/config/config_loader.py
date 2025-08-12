import yaml
from typing import Dict, Any, List
from ..core.models import SyncJobConfig


class ConfigLoader:
    """Load and validate sync job configurations"""
    
    @staticmethod
    def load_from_yaml(file_path: str) -> SyncJobConfig:
        """Load configuration from YAML file"""
        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)
        
        return SyncJobConfig(**config_dict)
    
    @staticmethod
    def load_from_dict(config_dict: Dict[str, Any]) -> SyncJobConfig:
        """Load configuration from dictionary"""
        return SyncJobConfig(**config_dict)
    
    @staticmethod
    def validate_config(config: SyncJobConfig) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        if not config.name:
            issues.append("Job name is required")
        
        if not config.source_provider:
            issues.append("Source provider configuration is required")
        
        if not config.destination_provider:
            issues.append("Destination provider configuration is required")
        
        if not config.strategies:
            issues.append("At least one strategy must be defined")
        
        # Validate strategy configurations
        for strategy in config.strategies:
            if 'name' not in strategy:
                issues.append("Strategy name is required")
            if 'column' not in strategy:
                issues.append(f"Strategy '{strategy.get('name', 'unknown')}' must specify a column")
        
        return issues
