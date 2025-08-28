import asyncio
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Any, Union, Tuple, Callable, Set
from datetime import datetime
from .enums import HashAlgo, SyncStrategy, PartitionType, BackendType, Capability
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
class DataStore:
    """Centralized data store configuration for database connections"""
    name: str
    type: str  # postgres, mysql, clickhouse, duckdb, object_storage, etc.
    connection: ConnectionConfig
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    # Custom capabilities that ADD to storage type defaults
    additional_capabilities: Optional[List[str]] = None
    # Capabilities to explicitly disable/remove
    disabled_capabilities: Optional[List[str]] = None
    
    def __post_init__(self):
        # Convert dict to ConnectionConfig if needed
        if isinstance(self.connection, dict):
            self.connection = ConnectionConfig(**self.connection)
        
        # Initialize capability caches
        self._computed_capabilities: Optional[Set[Capability]] = None
        self._capability_lookup: Optional[Dict[Capability, bool]] = None
    
    def get_capabilities(self) -> Set[Capability]:
        """Get capabilities with simple instance caching"""
        if self._computed_capabilities is None:
            from .capabilities import get_storage_capabilities
            
            # Start with storage type defaults
            capabilities = get_storage_capabilities(self.type)
            
            # Add any additional capabilities
            if self.additional_capabilities:
                for cap_str in self.additional_capabilities:
                    try:
                        cap = Capability(cap_str)
                        capabilities.add(cap)
                    except ValueError:
                        # Skip invalid capability strings
                        pass
            
            # Remove any disabled capabilities
            if self.disabled_capabilities:
                for cap_str in self.disabled_capabilities:
                    try:
                        cap = Capability(cap_str)
                        capabilities.discard(cap)
                    except ValueError:
                        # Skip invalid capability strings
                        pass
            
            # Cache the result
            self._computed_capabilities = capabilities
            # Pre-build lookup dictionary for O(1) has_capability checks
            self._capability_lookup = {cap: True for cap in capabilities}
        
        return self._computed_capabilities.copy()
    
    def has_capability(self, capability: Capability) -> bool:
        """Fast capability check with O(1) lookup"""
        if self._capability_lookup is None:
            self.get_capabilities()  # Populate caches
        
        return self._capability_lookup.get(capability, False)


@dataclass  
class DataStorage:
    """Collection of data stores for centralized connection management"""
    datastores: Dict[str, DataStore] = field(default_factory=dict)
    
    def add_datastore(self, datastore: DataStore):
        """Add a data store to the collection"""
        self.datastores[datastore.name] = datastore
    
    def get_datastore(self, name: str) -> Optional[DataStore]:
        """Get a data store by name"""
        return self.datastores.get(name)
    
    def list_datastores(self) -> List[str]:
        """List all available data store names"""
        return list(self.datastores.keys())
    
    def get_datastores_by_type(self, store_type: str) -> List[DataStore]:
        """Get all data stores of a specific type"""
        return [ds for ds in self.datastores.values() if ds.type == store_type]


# @dataclass
# class ColumnConfig:
#     """Column configuration for source to destination mapping"""
#     name: str
#     dest: Optional[str] = None
#     insert: bool = True
#     transform: Optional[str] = None


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
    # if true, will create sub partitions for each partition. Only available for delta and full strategies.
    use_sub_partitions: bool = True   
    # step size for sub partitions. Only available for delta and full strategies.
    sub_partition_step: int = 100 
    # minimum step size for sub partitions. Only available for hash strategies.
    # Main parition will be recursively divided into sub partitions until the step size is less than or equal to min_sub_partition_step or page_size is reached.
    min_sub_partition_step: int = 10  
    # factor by which to reduce the step size for sub partitions. Only available for hash strategies.
    # The block size will recursively reduced by this factor until it is less than or equal to min_sub_partition_step.
    interval_reduction_factor: int = 2  
    # Optionally provide a list of intervals to use for the hash strategy.
    # If not provided, will be automatically calculated based on the min_sub_partition_step and interval_reduction_factor.
    # If provided, will be used to calculate the sub partitions.
    # If provided, min_sub_partition_step and interval_reduction_factor will be ignored.
    intervals: List[int] = field(default_factory=list)
    prevent_update_unless_changed: bool = True
    # if true, will use pagination to fetch data. Only available for delta and full and hash strategies.
    use_pagination: bool = False
    # page size for pagination. Only available for delta and full and hash strategies.
    # If use_pagination is true, will fetch data in chunks of page_size.
    # For delta strategy, will fetch data in chunks of page_size.
    # Also prevents sub partitions from being created in hash strategy.
    page_size: int = 1000
    cron: Optional[str] = None  # Cron expression for scheduling
    partition_key: Optional[str] = None  # Column to use for partitioning
    partition_step: Optional[int] = None  # Step size for partitioning
    
    # New pipeline configuration
    pipeline_config: Optional[Dict[str, Any]] = None
    enable_pipeline: bool = True  # Whether to use pipeline architecture

@dataclass
class TransformationConfig:
    """Transformation configuration"""
    transform: str
    expr: str
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
    rows_fetched: int = 0   # Total rows fetched from the source
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
        rows_fetched: int = 0,
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
        self.rows_fetched += rows_fetched
        self.hash_query_count += hash_query_count
        self.data_query_count += data_query_count

@dataclass
class Column:
    expr: str # Source expression (e.g. "u.id")
    name: str                  # Destination column name
    dtype: Optional[UniversalDataType] = None
    hash_field: bool = True
    data_field: bool = True
    unique_key: bool = False
    order_key: bool = False
    direction: str = "asc"
    delta_key: bool = False
    partition_key: bool = False
    hash_key: bool = False
    
    def cast(self, value: Any) -> Any:
        if not self.dtype:
            return value
        return cast_value(value, self.dtype)

@dataclass
class BackendConfig:
    """Provider configuration"""
    type: str
    datastore_name: str  # Reference to DataStore name instead of direct connection
    table: Optional[str] = None
    schema: Optional[str] = None
    alias: Optional[str] = None
    join: List[JoinConfig] = field(default_factory=list)
    filters: List[FilterConfig] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)
    config: Optional[Dict[str, Any]] = None
    hash_cache: Optional[Dict[str, Any]] = None
    index_cache: Optional[Dict[str, Any]] = None
    supports_update: bool = False  # Whether the backend supports update operations

# @dataclass
# class ProviderConfig:
#     """Provider configuration"""
#     data_backend: BackendConfig
#     state_backend: Optional[BackendConfig]



@dataclass
class GlobalStageConfig:
    """Configuration for a single pipeline stage"""
    name: str
    type: Optional[str] = None
    enabled: bool = True
    source: Optional[BackendConfig] = None
    destination: Optional[BackendConfig] = None
    config: Optional[Dict[str, Any]] = None
    class_path: Optional[str] = None
    columns: List[Column] = field(default_factory=list)
    transformations: List[TransformationConfig] = field(default_factory=list)
    page_size: Optional[int] = None
    strategies: List[StrategyConfig] = field(default_factory=list)
    max_concurrent_partitions: int = 1
    target_batch_size: Optional[int] = None  # For batcher stages
    use_pagination: Optional[bool] = None  # For data fetch stages

# @dataclass
# class SyncJobConfig:
#     """Complete sync job configuration"""
#     name: str
#     description: str
#     columns: List[Column] = field(default_factory=list)
#     stages: List[GlobalStageConfig] = field(default_factory=list)


@dataclass
class PipelineJobConfig:
    """Complete pipeline-based sync job configuration"""
    name: str
    description: str
    columns: List[Column] = field(default_factory=list)
    stages: List[GlobalStageConfig] = field(default_factory=list)
    

@dataclass
class JobContext:
    """Context for the entire sync job - not tied to a specific partition or strategy"""
    job_name: str
    user_strategy_name: Optional[str] = None  # Strategy name requested by user
    user_start: Any = None  # Start bounds requested by user
    user_end: Any = None    # End bounds requested by user
    metadata: Dict[str, Any] = field(default_factory=dict)








@dataclass
class JobRunMetrics:
    """Metrics for a single job run"""
    job_name: str
    strategy_name: str
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    rows_fetched: int = 0
    rows_detected: int = 0
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


