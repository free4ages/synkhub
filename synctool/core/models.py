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
    # StarRocks specific
    http_port: Optional[int] = None


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
        
        # Initialize the appropriate datastore implementation
        self._datastore_impl = None
        self._create_datastore_implementation()
    
    def _create_datastore_implementation(self):
        """Create the appropriate datastore implementation based on type"""
        from ..datastore import PostgresDatastore, MySQLDatastore, StarRocksDatastore
        
        if self.type == 'postgres':
            self._datastore_impl = PostgresDatastore(self.name, self.connection)
        elif self.type == 'mysql':
            self._datastore_impl = MySQLDatastore(self.name, self.connection)
        elif self.type == 'starrocks':
            self._datastore_impl = StarRocksDatastore(self.name, self.connection)
        else:
            raise ValueError(f"Unsupported datastore type: {self.type}")
    
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
    
    async def connect(self, logger=None) -> None:
        """Connect to the datastore - idempotent operation"""
        if not self._datastore_impl:
            raise RuntimeError(f"No datastore implementation available for type: {self.type}")
        await self._datastore_impl.connect(logger)
    
    async def disconnect(self, logger=None) -> None:
        """Disconnect from the datastore - idempotent operation"""
        if self._datastore_impl:
            await self._datastore_impl.disconnect(logger)
    
    async def get_connection_pool(self):
        """Get the connection pool for database operations"""
        if not self._datastore_impl:
            raise RuntimeError(f"No datastore implementation available for type: {self.type}")
        return await self._datastore_impl.get_connection_pool()
    
    async def get_http_session(self):
        """Get the HTTP session for StarRocks stream load operations"""
        if not self._datastore_impl:
            raise RuntimeError(f"No datastore implementation available for type: {self.type}")
        return await self._datastore_impl.get_http_session()
    
    async def set_session_variables_for_connection(self, conn, logger=None):
        """Set session variables for StarRocks connections - track to avoid duplicates"""
        if self._datastore_impl and hasattr(self._datastore_impl, 'set_session_variables_for_connection'):
            await self._datastore_impl.set_session_variables_for_connection(conn, logger)
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        action: str = 'select'
    ) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a raw SQL query against the datastore.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            action: Type of query ('select', 'insert', 'update', 'delete')
            
        Returns:
            For SELECT queries: List of dictionaries representing rows
            For DML queries: Number of affected rows
        """
        if not self._datastore_impl:
            raise RuntimeError(f"No datastore implementation available for type: {self.type}")
        return await self._datastore_impl.execute_query(query, params, action)
    
    @property
    def is_connected(self) -> bool:
        """Check if the datastore is currently connected"""
        if not self._datastore_impl:
            return False
        return self._datastore_impl.is_connected


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
    
    async def connect_all(self, logger=None) -> None:
        """Connect to all datastores"""
        for datastore in self.datastores.values():
            await datastore.connect(logger)
    
    async def disconnect_all(self, logger=None) -> None:
        """Disconnect from all datastores"""
        for datastore in self.datastores.values():
            await datastore.disconnect(logger)
    
    async def __aenter__(self):
        await self.connect_all()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect_all()


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
class JoinCondition:
    """Single join condition"""
    data_key: str              # Column from the data stream
    backend_key: str           # Column from the backend table
    operator: str = '='        # Comparison operator: '=', '>=', '<=', '>', '<', '!='

@dataclass
class AsofConfig:
    """Configuration for ASOF join time conditions"""
    data_key: str                           # Time column in data stream
    valid_from_column: str                  # Start of validity period in backend
    valid_to_column: Optional[str] = None   # End of validity period in backend (optional)
    operator: str = '<='                    # Operator for valid_from comparison
    
    # For valid_to: if provided, adds condition: data_key < valid_to (or IS NULL for current records)
    include_open_ended: bool = True         # If True, matches records where valid_to IS NULL

@dataclass
class JoinStageConfig:
    """Configuration for join stage"""
    join_type: str                          # 'left', 'right', 'inner', 'asof_left'
    join_on: List[JoinCondition]            # List of join conditions
    asof: Optional[AsofConfig] = None       # ASOF-specific configuration (alternative to join_on)
    fetch_columns: Optional[List[Dict[str, str]]] = None  # Columns to fetch
    cache: Optional[Dict[str, Any]] = None  # Cache configuration
    cardinality: str = 'one_to_one'         # 'one_to_one' or 'one_to_many'
    merge: bool = True                      # Whether to merge joined data into rows
    output_key: Optional[str] = None        # If merge=False, key name for storing joined data


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
    
    prevent_update_unless_changed: bool = False
    use_pagination: bool = False
    page_size: int = 1000
    cron: Optional[str] = None
    
    # New pipeline configuration
    pipeline_config: Optional[Dict[str, Any]] = None
    
    # Enhanced strategy-level execution and locking settings
    wait_for_pipeline_lock: bool = False  # If True, wait for pipeline lock instead of skipping
    pipeline_lock_wait_timeout: int = 60  # Max seconds to wait for pipeline lock
    max_retry_on_lock_failure: int = 2    # Max retries when lock acquisition fails
    wait_for_table_lock: bool = False     # If True, wait for table DDL lock
    table_lock_wait_timeout: int = 30     # Max seconds to wait for table lock
    
    # Retry and failure handling
    max_consecutive_failures: int = 3     # Max consecutive failures before marking inactive
    retry_backoff_multiplier: float = 1.5 # Backoff multiplier for retries
    initial_retry_delay: int = 60         # Initial retry delay in seconds
    enable_pipeline: bool = True

@dataclass
class TransformationConfig:
    """Transformation configuration"""
    transform: Union[str, Callable]
    name: str
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
    # Type-specific attributes
    max_length: Optional[int] = None  # For VARCHAR, CHAR
    precision: Optional[int] = None    # For DECIMAL, NUMERIC
    scale: Optional[int] = None        # For DECIMAL, NUMERIC
    
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
    applicable_on: Optional[Union[str, List[str], Dict[str, Any]]] = None
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
    # ARQ-specific fields
    state_dir: str = "./data/pipeline_states"
    schedule_interval: int = 60  # Seconds between scheduling checks
    http_port: int = 8001  # Port for HTTP server to receive worker updates
    pickle_dir: str = "./data/pickled_configs"


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
    composite_dimensions: List['CompositeDimensionPartition'] = field(default_factory=list)
    parent_partition: Optional['MultiDimensionPartition'] = None
    level: int = 0
    num_rows: int = 0
    count_diff: int = 0
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
    
    def _get_composite_dimension_partition_bound(self, composite_dim: 'CompositeDimensionPartition') -> 'CompositePartitionBound':
        """Convert composite dimension to partition bound"""
        if composite_dim.is_range_based:
            return CompositePartitionBound(
                columns=composite_dim.columns,
                data_types=composite_dim.data_types,
                dimension_types=[dim.type for dim in composite_dim.dimensions],
                start_values=composite_dim.start_values,
                end_values=composite_dim.end_values,
                type="composite_range"
            )
        elif composite_dim.is_value_based:
            return CompositePartitionBound(
                columns=composite_dim.columns,
                data_types=composite_dim.data_types,
                dimension_types=[dim.type for dim in composite_dim.dimensions],
                value_tuples=composite_dim.value_tuples,
                type="composite_values"
            )
        elif composite_dim.has_mixed_types:
            # For mixed types, we'll handle this in the generator
            # This is a placeholder for mixed composite bounds
            return CompositePartitionBound(
                columns=composite_dim.columns,
                data_types=composite_dim.data_types,
                dimension_types=[dim.type for dim in composite_dim.dimensions],
                type="composite_mixed"
            )
        else:
            raise ValueError("Invalid composite dimension configuration")

    def get_partition_bounds(self) -> List[Union[PartitionBound, 'CompositePartitionBound']]:
        partition_bounds = []
        handled_columns = set()
        
        # Handle regular dimension partitions
        for dimension in self.dimensions:
            partition_bounds.append(self._get_dimension_partition_bound(dimension))
            handled_columns.add(dimension.column)
            
        # Handle composite dimension partitions  
        for composite_dim in self.composite_dimensions:
            partition_bounds.append(self._get_composite_dimension_partition_bound(composite_dim))
            handled_columns.update(composite_dim.columns)
            
        # Handle parent partitions
        if self.parent_partition:
            for dimension in self.parent_partition.dimensions:
                if dimension.column not in handled_columns:
                    partition_bounds.append(PartitionBound(column=dimension.column, start=dimension.start, end=dimension.end, data_type=dimension.data_type, type=dimension.type))
                    handled_columns.add(dimension.column)
                    
        return partition_bounds


@dataclass
class CompositeDimensionConfig:
    """Configuration for a single dimension within a composite partition"""
    column: str
    data_type: str
    type: str = "range"  # "range", "value", "upper_bound", "lower_bound" 
    step: int = 1
    step_unit: str = ""


@dataclass
class CompositeDimensionPartition:
    """Composite partition for multiple columns supporting both ranges and discrete values"""
    dimensions: List[CompositeDimensionConfig]  # Configuration for each dimension
    partition_id: str = "0"
    level: int = 0
    type: str = "composite"
    
    # For range-based composite partitions (e.g., datetime ranges + user_id ranges)
    start_values: Optional[Tuple[Any, ...]] = None  # Start values for each dimension
    end_values: Optional[Tuple[Any, ...]] = None    # End values for each dimension
    
    # For discrete value-based composite partitions (e.g., specific user_id, product_id tuples)
    value_tuples: Optional[List[Tuple[Any, ...]]] = None  # List of discrete value tuples
    
    def __post_init__(self):
        """Validate that either ranges or discrete values are provided"""
        has_ranges = self.start_values is not None and self.end_values is not None
        has_values = self.value_tuples is not None
        
        if not (has_ranges or has_values):
            raise ValueError("CompositeDimensionPartition must have either range values or discrete value tuples")
        
        if has_ranges and has_values:
            raise ValueError("CompositeDimensionPartition cannot have both range values and discrete value tuples")
    
    @property
    def columns(self) -> List[str]:
        """Get column names in order"""
        return [dim.column for dim in self.dimensions]
    
    @property
    def data_types(self) -> List[str]:
        """Get data types in order"""
        return [dim.data_type for dim in self.dimensions]
    
    @property
    def is_range_based(self) -> bool:
        """Check if this is a range-based composite partition"""
        return self.start_values is not None and self.end_values is not None
    
    @property 
    def is_value_based(self) -> bool:
        """Check if this is a discrete value-based composite partition"""
        return self.value_tuples is not None
    
    @property
    def has_mixed_types(self) -> bool:
        """Check if partition has both range and value dimensions"""
        types = {dim.type for dim in self.dimensions}
        return len(types) > 1


@dataclass 
class CompositePartitionBound:
    """Partition bound for composite filtering"""
    columns: List[str]
    data_types: List[str]
    dimension_types: List[str]  # "range" or "value" for each dimension
    type: str = "composite"
    
    # For range-based bounds
    start_values: Optional[Tuple[Any, ...]] = None
    end_values: Optional[Tuple[Any, ...]] = None
    
    # For discrete value-based bounds  
    value_tuples: Optional[List[Tuple[Any, ...]]] = None
    
    # For mixed bounds (some dimensions are ranges, others are values)
    dimension_values: Optional[List[Any]] = None  # Mixed values corresponding to each dimension
    
    @property
    def is_range_based(self) -> bool:
        return self.start_values is not None and self.end_values is not None
    
    @property
    def is_value_based(self) -> bool:
        return self.value_tuples is not None
    
    @property
    def is_mixed(self) -> bool:
        return self.dimension_values is not None
    
    def get_range_dimensions(self) -> List[int]:
        """Get indices of range-type dimensions"""
        return [i for i, dim_type in enumerate(self.dimension_types) if dim_type == "range"]
    
    def get_value_dimensions(self) -> List[int]:
        """Get indices of value-type dimensions"""
        return [i for i, dim_type in enumerate(self.dimension_types) if dim_type == "value"]



