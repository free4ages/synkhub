import asyncio
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Any, Union, Tuple, Callable
from datetime import datetime
from .enums import HashAlgo, SyncStrategy, PartitionType, BackendType
from .schema_models import UniversalDataType
from ..utils.schema_utils import cast_value

@dataclass
class ConnectionConfig:
    """Base connection configuration"""
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    dbname: Optional[str] = None
    schema: Optional[str] = None
    # For object storage
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region: Optional[str] = None
    # Connection pool settings
    max_connections: int = 10
    min_connections: int = 1


@dataclass
class ColumnConfig:
    """Column configuration for source to destination mapping"""
    name: str
    dest: Optional[str] = None
    insert: bool = True
    transform: Optional[str] = None


@dataclass
class JoinConfig:
    """Join configuration for source queries"""
    table: str
    on: str
    type: str = "inner"
    alias: Optional[str] = None

@dataclass
class FilterConfig:
    column: str
    operator: Literal['=', '!=', '<', '<=', '>', '>=']
    value: Any


# @dataclass
# class ColumnMapping:
#     """Column mapping for keys and hashes"""
#     unique_key: List[str] = field(default_factory=list)
#     partition_key: Optional[str] = None
#     row_hash_key: Optional[str] = None
#     delta_key: Optional[str] = None
#     order_key: Optional[str] = None


@dataclass
class StrategyConfig:
    """Strategy configuration with partitioning"""
    name: str
    type: SyncStrategy
    enabled: bool = True
    column: str = ""
    column_type: Optional[str] = None
    sub_partition_step: int = 100
    interval_reduction_factor: int = 2
    intervals: List[int] = field(default_factory=list)
    prevent_update_unless_changed: bool = True
    use_pagination: bool = False
    page_size: int = 1000
    cron: Optional[str] = None  # Cron expression for scheduling

@dataclass
class TransformationConfig:
    """Transformation configuration"""
    transform: str
    dest: str
    dtype: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)


@dataclass
class DimensionFieldConfig:
    """Dimension field configuration"""
    source: str
    dest: str
    transform: Optional[str] = None
    dtype: Optional[UniversalDataType] = None


@dataclass
class DimensionConfig:
    """Dimension configuration for enrichment"""
    name: str
    join_key: str
    source: Dict[str, Any]
    fields: List[DimensionFieldConfig]


@dataclass
class EnrichmentConfig:
    """Enrichment configuration"""
    enabled: bool = False
    dimensions: List[DimensionConfig] = field(default_factory=list)
    cache_backend: Optional[str] = "memory"  # 'memory' or 'redis'
    cache_config: Optional[Dict[str, Any]] = field(default_factory=dict)
    transformations: List[TransformationConfig] = field(default_factory=list)


@dataclass
class Partition:
    """Partition bounds for sync operations"""
    start: Any
    end: Any
    # step_size: int = 0
    column: str
    column_type: str
    partition_step: int = 0
    partition_id: str = field(default_factory=lambda: str(threading.current_thread().ident))
    parent_partition: Optional['Partition'] = None
    level: int = 0
    num_rows: int = 0
    hash: Union[str,int, None] = None
    intervals: List[int] = field(default_factory=list)


@dataclass
class SyncProgress:
    """Track sync progress"""
    total_partitions: int = 0
    completed_partitions: int = 0
    failed_partitions: int = 0
    rows_detected: int = 0    # Total rows detected in the partition
    rows_inserted: int = 0    # Total rows inserted in the partition
    rows_updated: int = 0    # Total rows updated in the partition
    rows_deleted: int = 0
    start_time: Optional[datetime] = None
    hash_query_count: int = 0
    data_query_count: int = 0

    def update_progress(self, 
        completed: bool = False, 
        failed: bool = False, 
        rows_detected: int = 0, 
        rows_inserted: int = 0, 
        rows_updated: int = 0, 
        rows_deleted: int = 0,
        hash_query_count: int = 0,
        data_query_count: int = 0,
    ):
        if completed:
            self.completed_partitions += 1
        if failed:
            self.failed_partitions += 1
        self.rows_detected += rows_detected
        self.rows_inserted += rows_inserted
        self.rows_updated += rows_updated
        self.rows_deleted += rows_deleted
        self.hash_query_count += hash_query_count
        self.data_query_count += data_query_count

@dataclass
class BackendConfig:
    """Provider configuration"""
    type: str
    connection: Optional[Dict[str, Any]] = None
    table: Optional[str] = None
    schema: Optional[str] = None
    alias: Optional[str] = None
    join: List[Dict[str, Any]] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    supports_update: bool = True
    backend: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

@dataclass
class ProviderConfig:
    """Provider configuration"""
    data_backend: BackendConfig
    state_backend: Optional[BackendConfig]


@dataclass
class SyncJobConfig:
    """Complete sync job configuration"""
    name: str
    description: str
    partition_step: int
    partition_key: str
    source_provider: Dict[str, ProviderConfig]
    destination_provider: Dict[str, ProviderConfig]
    # Column mapping configuration
    column_map: List[Dict[str, Any]]
    strategies: List[Dict[str, Any]] = field(default_factory=list)
    enrichment: Optional[Dict[str, Any]] = None
    hash_algo: Optional[HashAlgo] = HashAlgo.HASH_MD5_HASH
    partition_prefix_length: int = 2

    # Concurrency settings
    max_concurrent_partitions: int = 4




@dataclass
class Column:
    expr: str # Source expression (e.g. "u.id")
    name: str                  # Destination column name
    dtype: Optional[UniversalDataType] = None
    roles: List[str] = field(default_factory=list)
    insert: bool = True
    expr_map: Optional[Dict[str, str|None]] = None

    def get_order_direction(self) -> Optional[str]:
        """
        Parses order_key role to return sorting direction if any.
        Example: 'order_key[asc]' -> 'asc'
        """
        for role in self.roles:
            if role.startswith("order_key"):
                # Extract direction in brackets if exists
                if "[" in role and role.endswith("]"):
                    return role[role.index("[")+1 : -1].lower()
                return "asc"  # default if no direction specified
        return None

    def has_role(self, role_name: str) -> bool:
        """Checks if this column has a role (prefix match)."""
        return any(r.startswith(role_name) for r in self.roles)
    
    def is_enriched_column(self) -> bool:
        return self.has_role("enriched_key")
    
    def cast(self, value: Any) -> Any:
        if not self.dtype:
            return value
        return cast_value(value, self.dtype)


@dataclass
class JobRunMetrics:
    """Metrics for a single job run"""
    job_name: str
    strategy_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    error_message: Optional[str] = None
    partition_count: int = 0
    successful_partitions: int = 0
    failed_partitions: int = 0
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class SchedulerConfig:
    """Scheduler configuration"""
    enabled: bool = True
    redis_url: str = "redis://localhost:6379"
    lock_timeout: int = 3600  # 1 hour in seconds
    config_dir: str = "./configs"
    metrics_dir: str = "./data/metrics"
    logs_dir: str = "./data/logs"
    max_runs_per_job: int = 50


