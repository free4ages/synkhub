import asyncio
import threading
from dataclasses import dataclass, field
# from tkinter import W
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
#     unique_column: List[str] = field(default_factory=list)
#     partition_column: Optional[str] = None
#     row_hash_key: Optional[str] = None
#     delta_column: Optional[str] = None
#     order_column: Optional[str] = None

@dataclass
class DimensionPartitionConfig:
    """Single partition dimension configuration"""
    column: str
    step: int  # Can be numeric step or time period like 'daily', 'weekly'
    step_unit: str = ""
    data_type: str = ""
    start: Any = None
    end: Any = None
    type: str = "range"  # values: range, upper_bound, lower_bound, value
    value: List[Any] = field(default_factory=list)
    lower_bound: Any = None
    upper_bound: Any = None
    bounded: bool = False

@dataclass
class StrategyConfig:
    """Strategy configuration with partitioning"""
    name: str
    type: SyncStrategy
    enabled: bool = True
    default: bool = True
    # partition_column: str = ""
    match_row_count: bool = True
    # partition_column_type: Optional[str] = None
    max_concurrent_partitions: int = 1
    
    # Legacy single-dimension partitioning (kept for backward compatibility)
    # use_sub_partition: bool = True   
    # sub_partition_step: int = 100 
    # min_sub_partition_step: int = 10  
    # interval_reduction_factor: int = 2  
    # intervals: List[int] = field(default_factory=list)
    # partition_step: Optional[int] = None
    
    # New multi-dimensional partitioning
    primary_partitions: List[DimensionPartitionConfig] = field(default_factory=list)
    secondary_partitions: List[DimensionPartitionConfig] = field(default_factory=list)
    delta_partitions: List[DimensionPartitionConfig] = field(default_factory=list)
    
    prevent_update_unless_changed: bool = True
    use_pagination: bool = False
    page_size: int = 1000
    cron: Optional[str] = None
    
    # New pipeline configuration
    pipeline_config: Optional[Dict[str, Any]] = None
    enable_pipeline: bool = True

@dataclass
class TransformationConfig:
    """Transformation configuration"""
    transform: Union[str, Callable]
    expr: str
    data_type: Optional[str] = None
    columns: Optional[List[str]] = field(default_factory=list)


@dataclass
class DimensionFieldConfig:
    """Dimension field configuration"""
    source: str
    dest: str
    transform: Optional[str] = None
    data_type: Optional[UniversalDataType] = None


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
class SyncProgress:
    """Track sync progress"""
    total_partitions: int = 0
    completed_partitions: int = 0
    failed_partitions: int = 0
    processed_partitions: int = 0
    skipped_partitions: int = 0
    rows_fetched: int = 0   # Total rows fetched from the source
    rows_detected: int = 0    # Total rows detected in the partition
    rows_inserted: int = 0    # Total rows inserted in the partition
    rows_updated: int = 0    # Total rows updated in the partition
    rows_deleted: int = 0
    rows_failed: int = 0
    total_primary_partitions: int = 0
    detection_query_count: int = 0
    hash_query_count: int = 0
    data_query_count: int = 0
    start_time: Optional[datetime] = None

    def update_progress(self, 
        completed_partitions: int = 0,
        failed_partitions: int = 0,
        skipped_partitions: int = 0,
        processed_partitions: int = 0,
        total_partitions: int = 0,
        rows_detected: int = 0, 
        rows_fetched: int = 0,
        rows_inserted: int = 0, 
        rows_updated: int = 0, 
        rows_deleted: int = 0,
        rows_failed: int = 0,
        total_primary_partitions: int = 0,
        detection_query_count: int = 0,
        hash_query_count: int = 0,
        data_query_count: int = 0
    ):
        self.completed_partitions += completed_partitions
        self.failed_partitions += failed_partitions
        self.skipped_partitions += skipped_partitions
        self.processed_partitions += processed_partitions
        self.total_partitions += total_partitions
        self.rows_detected += rows_detected
        self.rows_inserted += rows_inserted
        self.rows_updated += rows_updated
        self.rows_deleted += rows_deleted
        self.rows_fetched += rows_fetched
        self.rows_failed += rows_failed
        self.total_primary_partitions += total_primary_partitions
        self.detection_query_count += detection_query_count
        self.hash_query_count += hash_query_count
        self.data_query_count += data_query_count
        
@dataclass
class Column:
    expr: str # Source expression (e.g. "u.id")
    name: str                  # Destination column name
    data_type: Optional[UniversalDataType] = None
    hash_column: bool = True
    # data_column: bool = True
    unique_column: bool = False
    order_column: bool = False
    direction: str = "asc"
    # delta_column: bool = False
    # partition_column: bool = False
    hash_key: bool = False
    virtual: bool = False
    
    def cast(self, value: Any) -> Any:
        if not self.data_type:
            return value
        return cast_value(value, self.data_type)

@dataclass
class BackendConfig:
    """Provider configuration"""
    type: str
    datastore_name: str  # Reference to DataStore name instead of direct connection
    name: Optional[str] = None
    table: Optional[str] = None
    schema: Optional[str] = None
    alias: Optional[str] = None
    join: List[JoinConfig] = field(default_factory=list)
    filters: List[FilterConfig] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)
    db_columns: List[Column] = field(default_factory=list)
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
    hash_algo: Optional[HashAlgo] = HashAlgo.HASH_MD5_HASH
    destination: Optional[BackendConfig] = None
    config: Optional[Dict[str, Any]] = None
    class_path: Optional[str] = None
    columns: List[Column] = field(default_factory=list)
    transformations: List[TransformationConfig] = field(default_factory=list)
    strategies: List[StrategyConfig] = field(default_factory=list)

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
    sync_type: Optional[str] = 'row-level'
    hash_algo: Optional[HashAlgo] = HashAlgo.HASH_MD5_HASH
    backends: List[BackendConfig] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)
    strategies: List[StrategyConfig] = field(default_factory=list)
    stages: List[GlobalStageConfig] = field(default_factory=list)
    max_concurrent_partitions: int = 1
    

# @dataclass
# class JobContext:
#     """Context for the entire sync job - not tied to a specific partition or strategy"""
#     job_name: str
#     user_strategy_name: Optional[str] = None  # Strategy name requested by user
#     user_start: Any = None  # Start bounds requested by user
#     user_end: Any = None    # End bounds requested by user
#     metadata: Dict[str, Any] = field(default_factory=dict)








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


@dataclass
class DimensionPartition:
    """Partition bounds for sync operations"""
    start: Any
    end: Any
    column: str
    data_type: str
    step: int = 0
    step_unit: str = ""
    offset: int = 0
    partition_id: str = "0"
    level: int = 0
    type: str = "range"
    value: Any = None

# @dataclass
# class Partition:
#     """Partition bounds for sync operations"""
#     start: Any
#     end: Any
#     column: str
#     data_type: str
#     step: int = 0
#     offset: int = 0
#     partition_id: str = "0"
#     level: int = 0

@dataclass
class PartitionBound:
    """Partition bounds for sync operations"""
    column: str
    data_type: str
    type: str = "range"
    start: Any = None
    end: Any = None
    value: Any = None
    lower_bound: Any = None
    upper_bound: Any = None
    bounded: bool = False
    exclusive: bool = True


@dataclass
class MultiDimensionPartition:
    """Multi-dimensional partition with multiple column bounds"""
    dimensions: List[DimensionPartition]
    parent_partition: Optional['MultiDimensionPartition'] = None
    level: int = 0
    num_rows: int = 0
    hash: Union[str, int, None] = None

    @property
    def partition_id(self) -> Optional[str]:
        id_parts = []
        if self.parent_partition:
            for dimension in self.parent_partition.dimensions:
                id_parts.append(dimension.partition_id or "0")
        for dimension in self.dimensions:
            id_parts.append(dimension.partition_id or "0")
        return '-'.join(id_parts) if id_parts else None
    
    def _get_dimension_partition_bound(self, dimension: DimensionPartition) -> PartitionBound:
        if dimension.type == "range":
            return PartitionBound(column=dimension.column, start=dimension.start, end=dimension.end, data_type=dimension.data_type, type=dimension.type)
        if dimension.type == "value":
            return PartitionBound(column=dimension.column, value=dimension.value, data_type=dimension.data_type, type=dimension.type)
            
    
    def get_bound_for_column(self, column: str) -> PartitionBound:
        """Get start/end bounds for a specific column"""
        for dimension in self.dimensions:
            if dimension.column == column:
                return self._get_dimension_partition_bound(dimension)
        return None
    
    def has_column(self, column: str) -> bool:
        """Check if partition has bounds for a specific column"""
        for dimension in self.dimensions:
            if dimension.column == column:
                return True
        return False
    
    def get_partition_bounds(self) -> List[PartitionBound]:
        partition_bounds = []
        handled_columns = set()
        for dimension in self.dimensions:
            partition_bounds.append(self._get_dimension_partition_bound(dimension))
            handled_columns.add(dimension.column)
        if self.parent_partition:
            for dimension in self.parent_partition.dimensions:
                if dimension.column not in handled_columns:
                    partition_bounds.append(PartitionBound(column=dimension.column, start=dimension.start, end=dimension.end, data_type=dimension.data_type, type=dimension.type))
                    handled_columns.add(dimension.column)
        return partition_bounds
    



