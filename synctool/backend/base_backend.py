from synctool.utils.sql_builder import SqlBuilder
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from datetime import timedelta, datetime, date, time
from enum import Enum
from dataclasses import dataclass
import uuid
import json
import decimal
import logging

from synctool.core.query_models import Field, Filter, Join, Query, RowHashMeta, Table
from ..core.models import BackendConfig, FilterConfig, ConnectionConfig, JoinConfig, Partition, Column, DataStorage
from ..core.column_mapper import ColumnSchema  # Add this import
from ..core.enums import HashAlgo, BackendType, Capability
from ..core.decorators import async_retry
from ..core.schema_models import UniversalSchema, UniversalDataType
from ..utils.schema_utils import cast_value




class BaseBackend(ABC):
    """Base backend class for data and state operations"""
    
    # Class-level capabilities - these ADD to upstream capabilities, don't replace them
    _capabilities: Optional[Set[Capability]] = None
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Simple instance-level caches - no locking needed since instance is created once
        self._computed_capabilities: Optional[Set[Capability]] = None
        self._capability_lookup: Optional[Dict[Capability, bool]] = None
    
    def get_capabilities(self) -> Set[Capability]:
        """
        Get all capabilities for this backend with simple caching.
        
        Since backend instance is created once per run, we can use simple caching
        without thread safety concerns.
        """
        if self._computed_capabilities is None:
            from ..core.capabilities import get_storage_capabilities
            
            all_capabilities = set()
            
            # 1. Start with storage type default capabilities (base level)
            storage_type = getattr(self.config, 'type', 'unknown')
            all_capabilities.update(get_storage_capabilities(storage_type))
            
            # 2. Add DataStore capabilities (if available)
            if hasattr(self, '_datastore') and self._datastore:
                datastore_caps = self._datastore.get_capabilities()
                all_capabilities.update(datastore_caps)
            
            # 3. Add backend class-level capabilities (most specific)
            if self._capabilities is not None:
                all_capabilities.update(self._capabilities)
            
            # Cache the computed capabilities
            self._computed_capabilities = all_capabilities
            
            # Pre-build lookup dictionary for O(1) has_capability checks
            self._capability_lookup = {cap: True for cap in all_capabilities}
        
        return self._computed_capabilities.copy()
    
    def has_capability(self, capability: Capability) -> bool:
        """
        Ultra-fast capability check optimized for multiple calls within a single run.
        
        Uses pre-computed dictionary lookup for O(1) performance.
        No thread safety overhead since backend instance is created once per run.
        
        Usage:
            can_fetch_hash_with_data = self.data_backend.has_capability(Capability.FETCH_HASH_WITH_DATA)
        """
        # Ensure capabilities are computed and cached
        if self._capability_lookup is None:
            self.get_capabilities()  # This populates both caches
        
        # O(1) dictionary lookup - fastest possible
        return self._capability_lookup.get(capability, False)
    
    def require_capability(self, capability: Capability) -> None:
        """Raise an exception if the backend doesn't have the required capability"""
        if not self.has_capability(capability):
            backend_type = getattr(self.config, 'type', 'unknown')
            available_caps = [cap.value for cap in self.get_capabilities()]
            raise ValueError(
                f"Backend '{backend_type}' does not support capability: {capability.value}. "
                f"Available capabilities: {available_caps}"
            )
    
    def _get_full_table_name(self, tablename: Optional[str] = None)-> str:
        tablename = tablename or self.table
        table_name = ""
        if self.schema:
            table_name = f"{self.schema}.{tablename}"
        if self.alias:
            table_name = f"{table_name} {self.alias}"
        return table_name

    
    @abstractmethod
    async def connect(self):
        """Establish connection to the provider"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close connection to the provider"""
        pass
    
    @abstractmethod
    async def has_data(self) -> bool:
        """Check if the provider has any data"""
        pass

    @abstractmethod
    async def insert_partition_data(self, data: List[Dict], partition: Optional[Partition] = None, upsert: bool = False) -> int:
        """Insert data into the provider"""
        pass
    
    @abstractmethod
    async def insert_delta_data(self, data: List[Dict], partition: Optional[Partition] = None, batch_size: int = 5000, upsert: bool = False) -> int:
        """Insert data into the provider"""
        pass

    @abstractmethod
    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        pass
    
    @abstractmethod
    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        pass

    @abstractmethod
    async def fetch_partition_row_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        pass

    @abstractmethod
    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        """Get the partition bounds"""
        pass
    
    @abstractmethod
    async def get_last_sync_point(self) -> Any:
        """Get the last sync point"""
        pass   

    @abstractmethod
    async def get_max_sync_point(self) -> Any:
        """Get the maximum sync point"""
        pass

    @abstractmethod
    async def fetch_child_partition_hashes(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch child hashes for a partition"""
        pass

    @abstractmethod
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract table schema in universal format"""
        pass
    
    @abstractmethod
    def generate_create_table_ddl(self, schema: UniversalSchema, target_table_name: Optional[str] = None) -> str:
        """Generate CREATE TABLE DDL for this backend type"""
        pass
    
    @abstractmethod
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map database-specific type to universal type"""
        pass
    
    @abstractmethod
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to database-specific type"""
        pass


class Backend(BaseBackend):
    def __init__(self, config: BackendConfig, column_schema: Optional[ColumnSchema] = None, logger= None, data_storage: Optional[DataStorage] = None):
        self.config = config
        # self.provider_type = BackendType(self.config.type)
        
        # Debug logging
        if logger:
            logger.info(f"Backend init - datastore_name: {getattr(config, 'datastore_name', 'NOT_SET')}, data_storage: {'Available' if data_storage else 'None'}")
            if data_storage:
                logger.info(f"Available datastores: {list(data_storage.datastores.keys())}")
        
        # Store reference to datastore for capability checking
        self._datastore = None
        
        # Resolve datastore_name to connection config
        if hasattr(self.config, 'datastore_name') and self.config.datastore_name and data_storage:
            datastore = data_storage.get_datastore(self.config.datastore_name)
            if not datastore:
                raise ValueError(f"Datastore '{self.config.datastore_name}' not found in DataStorage")
            self.connection_config = datastore.connection
            self._datastore = datastore  # Store for capability checking
            if logger:
                logger.info(f"Using datastore '{self.config.datastore_name}' connection config")
        else:
            # Fallback for backward compatibility with direct connection config
            self.connection_config = ConnectionConfig(**config.connection) if hasattr(config, 'connection') and config.connection else ConnectionConfig()
            if logger:
                logger.info(f"Using fallback connection config: {self.connection_config}")
        
        self.table = self.config.table
        self.alias = self.config.alias
        self.schema = self.config.schema or self.connection_config.schema or self._get_default_schema()
        # Removed: self.columns = [ColumnConfig(**col) for col in config.columns] if config.columns else []
        self.joins = self.config.join if self.config.join else []
        # Fix filter parsing - config.filters should be List[Dict] not List[str]
        self.filters = self.config.filters if self.config.filters else []
        self.supports_update = config.supports_update
        # Removed: self.column_mapping = ColumnMapping(**config.column_mapping) if config.column_mapping else ColumnMapping()
        self.column_schema = column_schema  # New: ColumnSchema instance
        # self.backend = config.backend
        self.provider_config = config.config or {}
        logger_name = f"{logger.name}.backend.{self.config.type}" if logger else f"{__name__}.{self.config.type}"
        self.logger = logging.getLogger(logger_name)
        self._connection_pool = None
        self._lock = asyncio.Lock()
        
        # Initialize capability caching from BaseBackend
        super().__init__()
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class SqlBackend(Backend):
    def _get_default_schema(self) -> Optional[str]:
        return None
    # Data operations
    async def has_data(self) -> bool:
        """Check if the provider has any data"""
        schema_table = self._get_full_table_name()
        query = f"SELECT COUNT(*) as count FROM {schema_table} LIMIT 1"
        result = await self.execute_query(query)
        return result.iloc[0]['count'] > 0
    
    

    async def insert_partition_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        column_keys: Optional[List[str]] = None,
        batch_size: int = 5000,
        upsert: bool = False
    ) -> int:
        """Insert data into the provider"""
        # Implementation depends on provider type
        return len(data)
    
    async def insert_delta_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        column_keys: Optional[List[str]] = None,
        batch_size: int = 5000,
        upsert: bool = False
    ) -> int:
        """Insert data into the provider"""
        # Implementation depends on provider type
        return len(data)

    
    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        if not self.column_schema or not self.column_schema.partition_column:
            raise ValueError("No partition key configured in column schema")
        column = self.column_schema.partition_column.expr
        """Get min and max values for partition column"""
        query = f"SELECT MIN({column}) as min_val, MAX({column}) as max_val FROM {self.table}"
        result = await self.execute_query(query)
        return result[0]['min_val'], result[0]['max_val']
    
    async def get_last_sync_point(self) -> Any:
        """Get the last sync point for a specific column type"""
        if not self.column_schema or not self.column_schema.delta_column:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_column.expr
        query = f"SELECT MAX({column}) as last_sync_point FROM {self.table}"
        result = await self.execute_query(query)
        return result[0]['last_sync_point']

    async def get_max_sync_point(self) -> Any:
        """Get the maximum sync point for a specific column type"""
        if not self.column_schema or not self.column_schema.delta_column:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_column.expr
        query = f"SELECT MAX({column}) as max_sync_point FROM {self.table}"
        result = await self.execute_query(query)
        return result[0]['max_sync_point']


    
    def _build_sql(self, query: Query) -> Tuple[str, List[Any]]:
        return SqlBuilder.build(query)


    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        query = self._build_partition_data_query(partition, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)
    
    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        if partition is None:
            # Handle case where no partition is provided
            query = Query(
                select=self._build_select_query(with_hash=with_hash, hash_algo=hash_algo),
                table=self._build_table_query(),
                joins=self._build_join_query(),
                filters=self._build_filter_query(),
                limit=page_size,
                offset=offset
            )
        else:
            query = self._build_partition_data_query(partition, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)
    
    async def fetch_child_partition_hashes(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        return []
    

    
    def _process_pre_insert_data(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        final = []
        for d in data:
            final.append(self.column_schema.transform_data(d))
        return final
    
    def _build_partition_data_query(self, partition: Partition, columns:List[Column]=None, order_by=None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None):
        filters = self._build_filter_query()
        # if not self.column_schema or not self.column_schema.partition_column:
        #     raise ValueError("No partition key configured in column schema")
        partition_column = partition.column or self.column_schema.partition_column.name
        column: Column = self.column_schema.column(partition_column)

        filters.extend([     
            Filter(column=column.expr, operator=">=", value=cast_value(partition.start, column.dtype)),
            Filter(column=column.expr, operator="<", value=cast_value(partition.end, column.dtype))
        ])
        
        # Add pagination if specified
        limit = page_size if page_size is not None else None

        query = Query(
            select = self._build_select_query(with_hash=with_hash, hash_algo=hash_algo, columns=columns,partition_id=partition.partition_id),
            table= self._build_table_query(),
            joins= self._build_join_query(),
            filters= filters,
            limit=limit,
            offset=offset,
            order_by=order_by
        )
        return query
    
    
    def _build_select_query(self, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, columns:List[Column]=None, partition_id:str=None):
        select = []
        if not self.column_schema:
            raise ValueError("No column schema configured")
        
        columns_to_fetch = columns or self.column_schema.columns_to_fetch()
        
        for col in columns_to_fetch:
            select.append(Field(expr=col.expr or col.name, alias=col.name))
        
        if partition_id:
            select.append(Field(expr=f"'{partition_id}'", alias='partition__'))
        
        if with_hash:
            hash_column = self.column_schema.hash_key
            if hash_column:
                # hash_columns is a list of Column objects
                select.append(Field(expr=hash_column.expr, alias='hash__'))
            else:
                # Generate hash from all columns
                hash_columns = [Field(expr=col.expr or col.name) for col in self.column_schema.columns if col.hash_column]
                select.append(Field(expr="", alias="hash__", type="rowhash", metadata=RowHashMeta(strategy=hash_algo, fields=hash_columns)))
        return select
 

    def _build_table_query(self):
        table = Table(
            table=self.table or "",  # Ensure table is not None
            schema=self.schema,
            alias=self.alias
        )
        return table

    def _build_join_query(self):
        # Build joins
        joins = []
        if self.joins:
            for join_cfg in self.joins or []:
                joins.append(Join(
                    table=join_cfg.table,
                    alias=join_cfg.alias,
                    on=join_cfg.on,
                    type=join_cfg.type
                ))
        return joins
    
    def _build_filter_query(self):
        # Build filters
        filters = []
        if self.filters:
            for filter_cfg in self.filters or []:
                filters.append(Filter(
                    column=filter_cfg.column,
                    operator=filter_cfg.operator,
                    value=filter_cfg.value
                ))
        return filters
