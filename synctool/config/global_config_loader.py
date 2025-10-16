import yaml
from pathlib import Path
from typing import Optional, Dict, Any
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
    max_runs_per_job: int = 50


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
class GlobalConfig:
    """Global configuration for ARQ Scheduler and Workers"""
    redis: RedisConfig
    scheduler: SchedulerConfig
    storage: StorageConfig
    worker: WorkerConfig
    http: HttpConfig
    log_batching: LogBatchingConfig
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GlobalConfig':
        """Create GlobalConfig from dictionary"""
        return cls(
            redis=RedisConfig(**data.get('redis', {})),
            scheduler=SchedulerConfig(**data.get('scheduler', {})),
            storage=StorageConfig(**data.get('storage', {})),
            worker=WorkerConfig(**data.get('worker', {})),
            http=HttpConfig(**data.get('http', {})),
            log_batching=LogBatchingConfig(**data.get('log_batching', {}))
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
            log_batching=LogBatchingConfig()
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
