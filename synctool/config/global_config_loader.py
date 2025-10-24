import yaml
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class RedisConfig:
    """Redis configuration"""
    url: str = "redis://localhost:6379"
    database: int = 0


@dataclass
class SchedulerConfig:
    """Scheduler configuration"""
    schedule_interval: int = 60
    http_port: int = 8001
    lock_timeout: int = 3600


@dataclass
class StorageConfig:
    """Storage configuration"""
    metrics_dir: str = "./data/metrics"
    logs_dir: str = "./data/logs"
    state_dir: str = "./data/pipeline_states"
    max_runs_per_strategy: int = 50  # Maximum runs to keep per strategy (was max_runs_per_job)


@dataclass
class WorkerConfig:
    """Worker configuration"""
    max_jobs: int = 4
    job_timeout: int = 3600


@dataclass
class HttpConfig:
    """HTTP client configuration"""
    timeout: float = 5.0
    max_retries: int = 3


@dataclass
class LogBatchingConfig:
    """Log batching configuration"""
    batch_size: int = 50
    flush_interval: float = 2.0
    local_fallback: bool = True


@dataclass
class ConfigStoreConfig:
    """Configuration store definition"""
    name: str
    type: str  # 'file' or 'database'
    is_primary: bool = False
    # File store specific
    base_path: Optional[str] = None
    use_metadata_files: bool = True
    # Database store specific
    db_type: Optional[str] = None  # 'postgres' or 'mysql'
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_name: Optional[str] = None
    table_prefix: str = "synctool_config"


@dataclass
class ConfigManagerConfig:
    """Configuration Manager settings"""
    stores: List[ConfigStoreConfig]
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConfigManagerConfig':
        """Create ConfigManagerConfig from dictionary"""
        stores_data = data.get('stores', [])
        stores = []
        for store_data in stores_data:
            stores.append(ConfigStoreConfig(**store_data))
        return cls(stores=stores)


@dataclass
class BuildSystemConfig:
    """Build system configuration"""
    user_config_dir: str = "./examples/configs/pipelines"
    built_config_dir: str = "./examples/configs/built"
    datastores_path: str = "./examples/configs/datastores.yaml"
    auto_build_on_startup: bool = True
    ddl_check_on_build: str = "required"  # "required" | "optional" | "skip"
    on_build_failure: str = "keep_old"  # "keep_old" | "remove"
    remove_orphaned_configs: bool = True
    allow_empty_source_dir: bool = True
    build_lock_timeout: int = 30


@dataclass
class GlobalConfig:
    """Global configuration for ARQ Scheduler and Workers"""
    redis: RedisConfig
    scheduler: SchedulerConfig
    storage: StorageConfig
    worker: WorkerConfig
    http: HttpConfig
    log_batching: LogBatchingConfig
    config_manager: ConfigManagerConfig
    build_system: BuildSystemConfig
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GlobalConfig':
        """Create GlobalConfig from dictionary"""
        return cls(
            redis=RedisConfig(**data.get('redis', {})),
            scheduler=SchedulerConfig(**data.get('scheduler', {})),
            storage=StorageConfig(**data.get('storage', {})),
            worker=WorkerConfig(**data.get('worker', {})),
            http=HttpConfig(**data.get('http', {})),
            log_batching=LogBatchingConfig(**data.get('log_batching', {})),
            config_manager=ConfigManagerConfig.from_dict(data.get('config_manager', {})),
            build_system=BuildSystemConfig(**data.get('build_system', {}))
        )
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'GlobalConfig':
        """Load GlobalConfig from YAML file"""
        path = Path(yaml_path)
        if not path.exists():
            # Return default config if file doesn't exist
            return cls.default()
        
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        
        return cls.from_dict(data or {})
    

    
    @classmethod
    def default(cls) -> 'GlobalConfig':
        """Return default configuration"""
        return cls(
            redis=RedisConfig(),
            scheduler=SchedulerConfig(),
            storage=StorageConfig(),
            worker=WorkerConfig(),
            http=HttpConfig(),
            log_batching=LogBatchingConfig(),
            config_manager=ConfigManagerConfig(stores=[
                ConfigStoreConfig(
                    name="default_file_store",
                    type="file",
                    is_primary=True,
                    base_path="./examples/configs"
                )
            ]),
            build_system=BuildSystemConfig()
        )


# Global instance - can be overridden
_global_config: Optional[GlobalConfig] = None


def load_global_config(config_path: Optional[str] = None) -> GlobalConfig:
    """
    Load global configuration from YAML file.
    If no path provided, looks for global_config.yaml in standard locations.
    """
    global _global_config

    
    if config_path:
        _global_config = GlobalConfig.from_yaml(config_path)
        return _global_config
    
    # Try standard locations
    search_paths = [
        Path("./global_config.yaml"),
        Path("./synctool/config/global_config.yaml"),
        Path("./config/global_config.yaml"),
        Path("/etc/synctool/global_config.yaml"),
    ]
    
    for path in search_paths:
        if path.exists():
            _global_config = GlobalConfig.from_yaml(str(path))
            return _global_config
    
    # Return default if no config found
    _global_config = GlobalConfig.default()
    return _global_config


def get_global_config() -> GlobalConfig:
    """Get the loaded global configuration"""
    global _global_config
    if _global_config is None:
        _global_config = load_global_config()
    return _global_config
