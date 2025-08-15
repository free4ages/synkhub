import yaml
import os
from typing import Dict, Any, List
from ..core.models import SyncJobConfig, DataStorage, DataStore, ConnectionConfig, ProviderConfig, BackendConfig


class ConfigLoader:
    """Load and validate sync job configurations"""
    
    @staticmethod
    def load_from_yaml(file_path: str) -> SyncJobConfig:
        """Load configuration from YAML file"""
        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)
        
        if config_dict is None:
            raise ValueError(f"Empty or invalid YAML file: {file_path}")
        
        return ConfigLoader.load_from_dict(config_dict)
    
    @staticmethod
    def load_from_dict(config_dict: Dict[str, Any]) -> SyncJobConfig:
        """Load configuration from dictionary"""
        # Convert provider dictionaries to ProviderConfig objects
        processed_config = config_dict.copy()
        
        # Process source_provider
        if 'source_provider' in processed_config and isinstance(processed_config['source_provider'], dict):
            provider_config = processed_config['source_provider']
            
            # Convert data_backend dict to BackendConfig
            data_backend = None
            if 'data_backend' in provider_config:
                data_backend = BackendConfig(**provider_config['data_backend'])
            
            # Convert state_backend dict to BackendConfig if present
            state_backend = None
            if 'state_backend' in provider_config:
                state_backend = BackendConfig(**provider_config['state_backend'])
            
            processed_config['source_provider'] = ProviderConfig(
                data_backend=data_backend,
                state_backend=state_backend
            )
        
        # Process destination_provider
        if 'destination_provider' in processed_config and isinstance(processed_config['destination_provider'], dict):
            provider_config = processed_config['destination_provider']
            
            # Convert data_backend dict to BackendConfig
            data_backend = None
            if 'data_backend' in provider_config:
                data_backend = BackendConfig(**provider_config['data_backend'])
            
            # Convert state_backend dict to BackendConfig if present
            state_backend = None
            if 'state_backend' in provider_config:
                state_backend = BackendConfig(**provider_config['state_backend'])
            
            processed_config['destination_provider'] = ProviderConfig(
                data_backend=data_backend,
                state_backend=state_backend
            )
        
        return SyncJobConfig(**processed_config)
    
    @staticmethod
    def load_datastores_from_yaml(file_path: str) -> DataStorage:
        """Load DataStorage configuration from YAML file"""
        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)
        
        if config_dict is None:
            raise ValueError(f"Empty or invalid datastores YAML file: {file_path}")
        
        return ConfigLoader.load_datastores_from_dict(config_dict)
    
    @staticmethod
    def load_datastores_from_dict(config_dict: Dict[str, Any]) -> DataStorage:
        """Load DataStorage configuration from dictionary"""
        datastores = {}
        
        # Support environment variable substitution for sensitive data
        def resolve_env_vars(value: Any) -> Any:
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]  # Remove ${ and }
                default_value = ""
                if ":" in env_var:
                    env_var, default_value = env_var.split(":", 1)
                return os.getenv(env_var, default_value)
            elif isinstance(value, dict):
                return {k: resolve_env_vars(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_env_vars(item) for item in value]
            return value
        
        for store_name, store_config in config_dict.get('datastores', {}).items():
            # Resolve environment variables in connection config
            resolved_config = resolve_env_vars(store_config)
            
            # Create ConnectionConfig from connection data
            connection_data = resolved_config.get('connection', {})
            connection_config = ConnectionConfig(**connection_data)
            
            # Create DataStore
            datastore = DataStore(
                name=store_name,
                type=resolved_config.get('type', ''),
                connection=connection_config,
                description=resolved_config.get('description'),
                tags=resolved_config.get('tags', [])
            )
            
            datastores[store_name] = datastore
        
        return DataStorage(datastores=datastores)
    
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
    
    @staticmethod
    def validate_datastores_config(data_storage: DataStorage) -> List[str]:
        """Validate DataStorage configuration and return list of issues"""
        issues = []
        
        if not data_storage.datastores:
            issues.append("At least one datastore must be defined")
        
        for name, datastore in data_storage.datastores.items():
            if not datastore.name:
                issues.append(f"Datastore name is required")
            if not datastore.type:
                issues.append(f"Datastore '{name}' must specify a type")
            if not datastore.connection:
                issues.append(f"Datastore '{name}' must have connection configuration")
            
            # Validate connection based on type
            conn = datastore.connection
            if datastore.type in ['postgres', 'mysql']:
                if not conn.host:
                    issues.append(f"Datastore '{name}' must specify host")
                if not conn.user:
                    issues.append(f"Datastore '{name}' must specify user")
                if not (conn.database or conn.dbname):
                    issues.append(f"Datastore '{name}' must specify database/dbname")
            elif datastore.type == 'object_storage':
                if not conn.s3_bucket:
                    issues.append(f"Datastore '{name}' must specify s3_bucket")
        
        return issues
