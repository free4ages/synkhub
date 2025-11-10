import logging
from typing import Dict, List, Optional, Any, Tuple
from .models import BackendConfig, ProviderConfig, Partition, DataStorage
from .column_mapper import ColumnSchema  # Add this import
from .enums import HashAlgo, Capability
from ..backend import PostgresBackend, ClickHouseBackend, DuckDBBackend, Backend

logger = logging.getLogger(__name__)

class Provider:
    """Unified Provider class that wraps data and state backends"""
    
    def __init__(self, config: Dict[str, Any], column_schemas: Dict[str,ColumnSchema]=None, role=None, logger= None, data_storage: Optional[DataStorage] = None):
        data_backend_config = config.get('data_backend', config)
        state_backend_config = config.get('state_backend')
        self.role = role
        self.data_storage = data_storage
        self.column_schema = column_schemas.get("common")

        logger_name = f"{logger.name}.{self.role}.provider" if logger else f"{__name__}.{self.role}.provider"
        self.logger = logging.getLogger(logger_name)
        # Pass column schemas to backends
        self.data_backend = self._create_backend(data_backend_config, column_schemas.get("data"))
        self.has_same_backend = True
        
        if state_backend_config:
            self.has_same_backend = False
            # For state backend, pass the state column schema
            self.state_backend = self._create_backend(state_backend_config, column_schemas.get("state"))
        else:
            self.state_backend = self.data_backend

    
    def _create_backend(self, config: Dict[str, Any], column_schema=None) -> Backend:
        """Create backend instance with column schema"""
        backend_config = BackendConfig(**config)
        
        if backend_config.type == "postgres":
            return PostgresBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        elif backend_config.type == "mysql":
            from ..backend.mysql import MySQLBackend
            return MySQLBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        elif backend_config.type == "clickhouse":
            return ClickHouseBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        elif backend_config.type == "starrocks":
            from ..backend.starrocks import StarRocksBackend
            return StarRocksBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        elif backend_config.type == "starrocks_mysql":
            from ..backend.starrocks_mysql import StarRocksMySQLBackend
            return StarRocksMySQLBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        elif backend_config.type == "duckdb":
            return DuckDBBackend(backend_config, column_schema, logger=self.logger, data_storage=self.data_storage)
        else:
            raise ValueError(f"Unsupported backend type: {backend_config.type}")
    
    @property
    def data_columns(self) -> List[str]:
        """Get column names from data backend schema"""
        if self.data_backend.column_schema:
            return [col.name for col in self.data_backend.column_schema.columns if col.insert]
        return []
    
    @property
    def state_columns(self) -> List[str]:
        """Get column names from state backend schema"""
        if self.state_backend.column_schema:
            return [col.name for col in self.state_backend.column_schema.columns if col.insert]
        return []
    
    @property
    def data_unique_columns(self) -> List[str]:
        if self.data_backend.column_schema:
            return [col.name for col in self.data_backend.column_schema.unique_columns]
        return []
    
    @property
    def state_unique_columns(self) -> List[str]:
        if self.state_backend.column_schema:
            return [col.name for col in self.state_backend.column_schema.unique_columns]
        return []
    
    @property
    def data_hash_key(self) -> Optional[str]:
        if self.data_backend.column_schema and self.data_backend.column_schema.hash_key:
            return self.data_backend.column_schema.hash_key[0].name
        return None
    
    @property
    def state_hash_key(self) -> Optional[str]:
        if self.state_backend.column_schema and self.state_backend.column_schema.hash_key:
            return self.state_backend.column_schema.hash_key[0].name
        return None
    
    @property
    def data_partition_column(self) -> Optional[str]:
        if self.data_backend.column_schema and self.data_backend.column_schema.partition_column:
            return self.data_backend.column_schema.partition_column.name
        return None
    
    @property
    def state_partition_column(self) -> Optional[str]:
        if self.state_backend.column_schema and self.state_backend.column_schema.partition_column:
            return self.state_backend.column_schema.partition_column.name
        return None

    @property
    def data_order_column(self) -> Optional[str]:
        if self.data_backend.column_schema and self.data_backend.column_schema.order_column:
            return self.data_backend.column_schema.order_column[0].name
        return None

    @property
    def state_order_column(self) -> Optional[str]:
        if self.state_backend.column_schema and self.state_backend.column_schema.order_column:
            return self.state_backend.column_schema.order_column[0].name
        return None
    
    @property
    def data_delta_column(self) -> Optional[str]:
        if self.data_backend.column_schema and self.data_backend.column_schema.delta_column:
            return self.data_backend.column_schema.delta_column.name
        return None
    
    @property
    def state_delta_column(self) -> Optional[str]:
        if self.state_backend.column_schema and self.state_backend.column_schema.delta_column:
            return self.state_backend.column_schema.delta_column.name
        return None
    
    
    async def connect(self):
        """Connect both backends"""
        await self.data_backend.connect()
        if self.state_backend != self.data_backend:
            await self.state_backend.connect()
    
    async def disconnect(self):
        """Disconnect both backends"""
        await self.data_backend.disconnect()
        if self.state_backend != self.data_backend:
            await self.state_backend.disconnect()
    
    # Data operations - delegate to data backend
    async def has_data(self) -> bool:
        return await self.data_backend.has_data()
    
    async def fetch_partition_data(self, partition: Partition, 
                                 hash_algo=HashAlgo.HASH_MD5_HASH,
                                 page_size: Optional[int] = None,
                                 offset: Optional[int] = None) -> List[Dict]:
        with_hash = self.has_same_backend
        # logger.debug(f"Fetching partition data for start {partition.start} and end {partition.end}")
        # @TODO implement data fetching for different backend with pagination/partition strategy
        data = await self.data_backend.fetch_partition_data(partition, with_hash, hash_algo, page_size, offset)
        logger.debug(f"Fetched {len(data)} partition data from {self.role} for start {partition.start} and end {partition.end} with page size {page_size} and offset {offset}")
        return data
    
    async def fetch_delta_data(self, partition: Partition,
                             hash_algo=HashAlgo.HASH_MD5_HASH,
                             page_size: Optional[int] = None,
                             offset: Optional[int] = None) -> List[Dict]:
        with_hash: bool = self.has_same_backend
        # @TODO implement data fetching for different backend with pagination/partition strategy
        # logger.debug(f"Fetching partition data from {self.role} for start {partition.start} and end {partition.end}")
        data = await self.data_backend.fetch_delta_data(partition, with_hash, hash_algo, page_size, offset)
        logger.debug(f"Fetched {len(data)} delta data from {self.role} for start {partition.start} and end {partition.end} with page size {page_size} and offset {offset}")
        return data
    
    async def fetch_child_partition_hashes(self, partition: Partition,
                             hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        with_hash: bool = self.has_same_backend
        # @TODO implement data fetching for different backend with pagination/partition strategy
        
        data = await self.data_backend.fetch_child_partition_hashes(partition, with_hash, hash_algo)
        logger.debug(f"Fetched {len(data)} partition hashes from {self.role} for start {partition.start} and end {partition.end}")
        return data
    
    # def _process_pre_insert_data(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    #     """Process data before inserting into destination"""
    #     if data:
    #         # populate destination hash column
    #         if self.has_same_backend:
    #             destination_hash_key = self.data_backend.column_schema.hash_key[0].name
    #         else:
    #             destination_hash_key = self.state_backend.column_schema.hash_key[0].name
    #         for d in data:
    #             d[destination_hash_key] = d['hash__']
    #     return data
    
    async def insert_partition_data(self, data: List[Dict], partition: Partition, upsert: bool = True) -> int:
        # data = self._process_pre_insert_data(data)
        return await self.data_backend.insert_partition_data(data, partition, upsert=upsert)
    
    async def insert_delta_data(self, data: List[Dict], partition: Partition, upsert: bool = True) -> int:
        return await self.data_backend.insert_delta_data(data, partition, upsert=upsert)
    
    async def delete_partition_data(self, partition: Partition) -> int:
        """Delete partition data from destination"""
        return await self.data_backend.delete_partition_data(partition)
    
    async def fetch_partition_row_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        # can_fetch_hash_with_data = self.data_backend.has_capability(Capability.FETCH_HASH_WITH_DATA)
        # if self.has_same_backend:
        #     # if data fetching is optional and backend supports it, fetch data along with hashes
        #     if with_data is True:
        #         if can_fetch_hash_with_data:
        #             hashes, has_data = await self.data_backend.fetch_partition_row_hashes(partition, hash_algo=hash_algo, with_data=with_data)
        #             return hashes, has_data
        #         else:
        #             raise ValueError("Backend does not support fetching hash with data")
        #     elif with_data is False:
        #         return await self.data_backend.fetch_partition_row_hashes(partition, hash_algo=hash_algo, with_data=with_data)
        #     else:
        #         # let backend decide if it wants to fetch data along with hashes
        #         return await self.data_backend.fetch_partition_row_hashes(partition, hash_algo=hash_algo)
        # else:
        #     hashes, has_data = await self.state_backend.fetch_partition_row_hashes(partition, hash_algo=hash_algo, with_data=False)
        #     if with_data is True:
        #         # Fetch data separately from data backend if needed
        #         data = await self.data_backend.fetch_partition_data(partition, with_hash=False, hash_algo=hash_algo)
        #         unique_columns = self.column_schema.unique_columns
        #         hash_map = {(h[x.name] for x in unique_columns): h for h in hashes}
        #         for d in data:
        #             key = tuple(d[x.name] for x in unique_columns)
        #             if key in hash_map:
        #                 d["hash__"] = hash_map[key]["hash__"]
        #         return data, True
        #     else:
        #         return hashes, has_data
        hashes= await self.state_backend.fetch_partition_row_hashes(partition, hash_algo=hash_algo)
    
    async def update_data(self, data: List[Dict], unique_columns: List[str]) -> int:
        return await self.data_backend.update_data(data, unique_columns)
    
    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        return await self.data_backend.get_partition_bounds()
    
    # State operations - delegate to state backend
    async def fetch_hashes(self, partition: Optional[Partition] = None) -> List[Dict]:
        return await self.state_backend.fetch_hashes(partition)
    
    async def insert_hashes(self, data: List[Dict]) -> int:
        return await self.state_backend.insert_hashes(data)
    
    async def get_last_sync_point(self) -> Any:
        return await self.state_backend.get_last_sync_point()
    
    async def get_max_sync_point(self) -> Any:
        return await self.state_backend.get_max_sync_point()
    
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
