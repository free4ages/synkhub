import aiohttp
import asyncio
import json
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

from synctool.core.models import StrategyConfig, BackendConfig, DataStorage
from synctool.core.enums import HashAlgo, Capability
from synctool.core.query_models import Query, Field, Filter, BlockHashMeta, BlockNameMeta, RowHashMeta, Table
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import MultiDimensionPartition, Column
from ..core.column_mapper import ColumnSchema
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType
# from ..utils.hash_cache import HashCache

logger = logging.getLogger(__name__)


class StarRocksBackend(SqlBackend):
    """StarRocks implementation of Backend"""
    
    # StarRocks-specific capabilities beyond storage type defaults
    _capabilities = {
    }
    
    def __init__(self, config: BackendConfig, column_schema: Optional[ColumnSchema] = None, logger=None, data_storage: Optional[DataStorage] = None):
        super().__init__(config, column_schema, logger=logger, data_storage=data_storage)
        
        # Extract StarRocks-specific connection details from connection_config
        self.host = getattr(self.connection_config, 'host', 'localhost')
        self.port = getattr(self.connection_config, 'port', 9030)
        self.fe_host = f"http://{self.host}:{self.port}"
        self.user = getattr(self.connection_config, 'user', 'root')
        self.password = getattr(self.connection_config, 'password', '')
        self.database = getattr(self.connection_config, 'database', None)
        self.batch_size = getattr(config, 'batch_size', 5000)
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_kwargs = {}
        self._connection = None
        
        # Scale up cache size for better performance
        cache_size = getattr(config, 'cache_size', 5000)
        # self.hash_cache = HashCache(max_rows=cache_size)
        self.hash_cache = None
    
    def _get_default_schema(self) -> str:
        return ''
    
    async def connect(self) -> None:
        """Connect to StarRocks database"""
        try:
            if self._session is None:
                # Create basic auth if credentials provided
                auth = None
                if self.user and self.password:
                    auth = aiohttp.BasicAuth(self.user, self.password)
                elif self.user:
                    auth = aiohttp.BasicAuth(self.user, '')
                
                # Create session with proper headers
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'synctool-starrocks-client/1.0'
                }
                
                connector = aiohttp.TCPConnector(
                    limit=10,
                    ttl_dns_cache=300,
                    use_dns_cache=True,
                )
                
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                
                self._session = aiohttp.ClientSession(
                    auth=auth,
                    headers=headers,
                    connector=connector,
                    timeout=timeout,
                    **self._session_kwargs
                )
                self.logger.info(f"Connecting to StarRocks: host={self.host}, port={self.port}, user={self.user}, database={self.database}")
                
                # Test connection
                await self._test_connection()
                
        except Exception as e:
            if self._session:
                await self._session.close()
                self._session = None
            raise RuntimeError(f"Failed to connect to StarRocks: {e}")

    async def _test_connection(self):
        """Test the connection to StarRocks"""
        try:
            test_query = "SELECT 1 as test"
            await self.execute_query(test_query)
            self.logger.info("StarRocks connection test successful")
        except Exception as e:
            raise RuntimeError(f"StarRocks connection test failed: {e}")

    async def disconnect(self) -> None:
        """Disconnect from StarRocks database"""
        if self._session:
            await self._session.close()
            self._session = None

    def _full_table(self) -> str:
        if self.database:
            return f"`{self.database}`.`{self.table}`"
        return f"`{self.table}`"

    def _query_url(self) -> str:
        # Query endpoint for executing SQL statements (POST /api/{db}/_sql)
        if self.database:
            return f"{self.fe_host}/api/{self.database}/_sql"
        else:
            # For queries without database context, use default endpoint
            return f"{self.fe_host}/api/_sql"

    def _stream_load_url(self) -> str:
        # Stream load endpoint: /api/{db}/{tbl}/_stream_load
        if self.database:
            return f"{self.fe_host}/api/{self.database}/{self.table}/_stream_load"
        else:
            return f"{self.fe_host}/api/{self.table}/_stream_load"

    async def execute_query(self, query: str, params: Optional[list] = None, action: Optional[str] = 'select') -> List[Dict]:
        """Execute query and return List[Dict]"""
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        self.logger.info(f"Executing query: {query}")
        if params:
            self.logger.info(f"Query params: {params}")
        
        # For parameterized queries, we need to substitute parameters manually for StarRocks
        # since it doesn't support prepared statements like PostgreSQL
        if params:
            # Simple parameter substitution - in production you'd want proper escaping
            formatted_query = query
            for i, param in enumerate(params):
                placeholder = f"${i+1}"
                if isinstance(param, str):
                    escaped_param = param.replace("'", "''")
                    value = f"'{escaped_param}'"
                elif param is None:
                    value = "NULL"
                else:
                    value = str(param)
                formatted_query = formatted_query.replace(placeholder, value)
            query = formatted_query

        url = self._query_url()
        
        # StarRocks HTTP API expects "stmt" field, not "sql"
        payload = {"stmt": query}
        
        self.logger.debug(f"Request URL: {url}")
        self.logger.debug(f"Request payload: {payload}")
        
        try:
            async with self._session.post(url, json=payload) as resp:
                text = await resp.text()
                
                self.logger.debug(f"Response status: {resp.status}")
                self.logger.debug(f"Response text: {text}")
                
                if resp.status != 200:
                    self.logger.error(f"StarRocks query failed with status {resp.status}")
                    self.logger.error(f"Response text: {text}")
                    self.logger.error(f"URL: {url}")
                    self.logger.error(f"Payload: {payload}")
                    raise RuntimeError(f"StarRocks query failed {resp.status}: {text}\nSQL: {query}")
                
                try:
                    result = json.loads(text)
                except Exception as e:
                    self.logger.error(f"Failed to parse JSON response: {text}")
                    if action == 'delete':
                        return 0  # Return success count for delete operations
                    return []

                # Handle different response formats from StarRocks HTTP API
                if action == 'select':
                    # StarRocks HTTP API returns {"data": [...], "meta": [...]} format
                    if "data" in result:
                        return result["data"]
                    elif "columns" in result and "rows" in result:
                        # Alternative format: {"columns": [...], "rows": [...]}
                        cols = result["columns"]
                        rows = result["rows"]
                        if cols and rows:
                            col_names = [c.get("name", c) if isinstance(c, dict) else str(c) for c in cols]
                            return [dict(zip(col_names, row)) for row in rows]
                    elif isinstance(result, list):
                        return result
                    else:
                        # Sometimes StarRocks returns the result directly
                        return [result] if isinstance(result, dict) else []
                        
                elif action == 'delete':
                    # For delete operations, return the affected rows count or success indicator
                    if isinstance(result, dict):
                        return result.get("affected_rows", result.get("rowsAffected", result.get("count", 1)))
                    return 1
                    
                else:
                    return result if isinstance(result, list) else [result]
                    
        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP client error: {e}")
            raise RuntimeError(f"HTTP error connecting to StarRocks: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in execute_query: {e}")
            raise

    async def has_data(self) -> bool:
        """Check if the provider has any data"""
        table = self._full_table()
        query = f"SELECT COUNT(*) as count FROM {table} LIMIT 1"
        result = await self.execute_query(query)
        return result[0]['count'] > 0 if result else False

    async def get_last_sync_point(self, column: str) -> Any:
        """Get the last sync point for a specific column type"""
        column = self.column_schema.column(column).expr
        table = self._full_table()
        query = f"SELECT MAX({column}) as last_sync_point FROM {table}"
        result = await self.execute_query(query)
        return result[0]['last_sync_point'] if result else None

    async def get_max_sync_point(self, column: str) -> Any:
        """Get the maximum sync point for a specific column type"""
        column = self.column_schema.column(column).expr
        table = self._full_table()
        query = f"SELECT MAX({column}) as max_sync_point FROM {table}"
        result = await self.execute_query(query)
        max_sync_point = result[0]['max_sync_point'] if result else None
        return max_sync_point

    async def get_partition_bounds(self, column: str) -> Tuple[Any, Any]:
        """Get min and max values for partition column"""
        partition_column = self.column_schema.column(column).expr
        table = self._full_table()
        query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table}"
        result = await self.execute_query(query)
        if not result:
            return None, None
        min_val, max_val = result[0]['min_val'], result[0]['max_val']
        return min_val, max_val

    async def fetch_partition_data(self, partition: MultiDimensionPartition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        if page_size is not None or offset is not None:
            order_by = [f"{c.expr} {c.direction}" for c in self.column_schema.order_columns] if self.column_schema.order_columns else [f"{self.column_schema.partition_column.expr} asc"]
        else:
            order_by = None
        query = self._build_partition_data_query(partition, order_by=order_by, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)

    async def fetch_delta_data(self, partition: Optional[MultiDimensionPartition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        if not self.column_schema or not self.column_schema.delta_column:
            raise ValueError("No delta key configured in column schema")
        if page_size is not None or offset is not None:
            order_by = [f"{c.expr} {d}" for c,d in self.column_schema.order_columns] if self.column_schema.order_columns else [f"{self.column_schema.partition_column.expr} asc"]
        else:
            order_by = None
        partition_column = self.column_schema.delta_column.name
        query = self._build_partition_data_query(partition, partition_column=partition_column, order_by=order_by, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)

    async def fetch_partition_row_hashes(self, partition: MultiDimensionPartition, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None, skip_cache=False) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        # Use HashCache for optimized fetching
        fallback_fn = lambda: self._fetch_partition_row_hashes_direct(partition, hash_algo, page_size, offset)
        if self.hash_cache and not skip_cache:
            return await self.hash_cache.fetch_partition_row_hashes(partition, self, fallback_fn, hash_algo, page_size=page_size, offset=offset)
        else:
            return await fallback_fn()

    async def fetch_child_partition_hashes(self, partition: Optional[MultiDimensionPartition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch child partition hashes"""
        # Use HashCache for optimized fetching
        if partition is None:
            raise ValueError("MultiDimensionPartition is required for hash cache operations")
        
        if not self.column_schema:
            raise ValueError("Column schema is required for hash cache operations")
        
        fallback_fn = lambda: self._fetch_child_partition_hashes_direct(partition, with_hash, hash_algo)
        if self.hash_cache:
            return await self.hash_cache.fetch_child_partition_hashes(
                partition, self, fallback_fn, hash_algo
            )
        else:
            return await fallback_fn()

    async def _fetch_child_partition_hashes_direct(self, partition: MultiDimensionPartition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Direct database fetch for child partition hashes - used by HashCache"""
        query: Query = self._build_partition_hash_query(partition, hash_algo)
        query = self._rewrite_query(query)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)

    async def _fetch_partition_row_hashes_direct(self, partition: MultiDimensionPartition, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Direct database fetch for partition row hashes - used by HashCache"""
        return await self.fetch_partition_data(partition, with_hash=True, hash_algo=hash_algo, page_size=page_size, offset=offset)

    def mark_partition_complete(self, partition_id: str):
        """Mark partition as complete and evict from cache"""
        if self.hash_cache:
            self.hash_cache.mark_partition_complete(partition_id)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get hash cache statistics"""
        if self.hash_cache:
            return self.hash_cache.get_cache_stats()
        else:
            return {}

    def clear_cache(self):
        """Clear the hash cache"""
        if self.hash_cache:
            self.hash_cache.clear_cache()

    async def fetch_row_hashes(self, unique_column_values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch row hashes for specific unique key values from cache if possible"""
        if not self.column_schema:
            return []
        
        unique_columns = [col.name for col in self.column_schema.unique_columns] if self.column_schema.unique_columns else []
        partition_column = self.column_schema.partition_column.name if self.column_schema.partition_column else ""
        partition_column_type = str(self.column_schema.partition_column.data_type) if self.column_schema.partition_column else ""
        
        # This would need intervals from the partition context - for now return empty
        # In practice, this would be called with proper context from partition processor
        return []

    async def delete_partition_data(self, partition: MultiDimensionPartition) -> int:
        """Delete partition data from destination"""
        filters = self._build_filter_query()
        column = self.column_schema.column(partition.column)
        filters += [
            Filter(column=column.expr, operator='>=', value=partition.start), 
            Filter(column=column.expr, operator='<', value=partition.end)
        ]
        query = Query(action='delete', table=self._build_table_query(), filters=filters)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params, action='delete')

    async def delete_rows(self, rows: List[Dict], partition: MultiDimensionPartition, strategy_config: StrategyConfig) -> int:
        """Delete rows from destination"""
        filters = self._build_filter_query()
        unique_columns = self.column_schema.unique_columns
        unique_columns_values = [tuple(row[key] for key in unique_columns) for row in rows]
        filters += [
            Filter(column=self.column_schema.column(key).expr, operator='in', value=unique_columns_values)
            for key in unique_columns
        ]
        query = Query(action='delete', table=self._build_table_query(), filters=filters)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params, action='delete')

    async def fetch_partition_hashes(self, partition: MultiDimensionPartition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        return await self.data_backend.fetch_partition_row_hashes(partition, hash_algo)

    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level # level is the current level for which we are building the group name
        intervals = metadata.intervals
        partition_column = metadata.partition_column
        partition_column_type = metadata.partition_column_type
        parent_partition_id = metadata.parent_partition_id
        ids = [int(i) for i in parent_partition_id.split('-')] if parent_partition_id else []
        parent_offset = sum([intervals[i]*ids[i] for i in range(len(ids))]) if len(ids) > 0 else 0

        if partition_column_type == UniversalDataType.INTEGER:
            expr = f"FLOOR(({partition_column} - {parent_offset}) / {intervals[level]})"
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS STRING))" if parent_partition_id else f"CAST({expr} AS STRING)"

        elif partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            expr = f"FLOOR((UNIX_TIMESTAMP({partition_column}) - {parent_offset}) / {intervals[level]})"
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS STRING))" if parent_partition_id else f"CAST({expr} AS STRING)"

        elif partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            # StarRocks UUID handling - convert hex to integer
            expr = f"FLOOR((CONV(SUBSTR({partition_column}, 1, 8), 16, 10) - {parent_offset})/{intervals[level]})"
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS STRING))" if parent_partition_id else f"CAST({expr} AS STRING)"

        else:
            raise ValueError(f"Unsupported partition type: {partition_column}")

    def _build_rowhash_expr(self, field: Field) -> str:
        """Build row hash expression for StarRocks"""
        metadata: RowHashMeta = field.metadata
        expr=""
        if metadata.hash_column:
            expr = metadata.hash_column
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            concat = ", ".join([f"CAST({x.expr} AS STRING)" for x in metadata.fields])
            expr = f"CONV(SUBSTR(MD5(CONCAT({concat})), 1, 8), 16, 10)"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            concat = ", ".join([f"CAST({x.expr} AS STRING)" for x in metadata.fields])
            expr = f"MD5(CONCAT({concat}))"
        return expr

    def _build_blockhash_expr(self, field: Field):
        metadata: BlockHashMeta = field.metadata
        inner_expr = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.MD5_SUM_HASH:
            expr = f"SUM({inner_expr})"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            expr = f"MD5(GROUP_CONCAT({inner_expr} ORDER BY {metadata.order_column} SEPARATOR ''))"
        return expr

    def _rewrite_query(self, query: Query) -> Query:
        """Rewrite query to handle StarRocks-specific expressions"""
        rewritten = []
        for f in query.select:
            if f.type == 'blockhash':
                expr = self._build_blockhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            elif f.type == "blockname":
                expr = self._build_group_name_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            elif f.type == "rowhash":
                expr = self._build_rowhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                rewritten.append(f)
        query.select = rewritten
        return query

    def _build_sql(self, query: Query) -> Tuple[str, list]:
        """Build SQL query for StarRocks"""
        q = self._rewrite_query(query)
        return SqlBuilder.build(q, dialect='starrocks')

    def _build_partition_hash_query(self, partition: MultiDimensionPartition, hash_algo: HashAlgo) -> Query:
        """Build partition hash query for StarRocks"""
        start = partition.start
        end = partition.end
        partition_column: Column = self.column_schema.column(partition.column)
        partition_column_type: UniversalDataType | None = partition_column.data_type

        hash_column: Column | None = self.column_schema.hash_key
        order_columns: list[tuple[Column, str]] | None = self.column_schema.order_columns
        order_column_expr = ",".join([f"{c.expr} {c.direction}" for c in order_columns]) if order_columns else None
        
        partition_hash_column = Field(
            expr=f"{partition_column.expr}",
            alias="partition_hash",
            metadata=BlockHashMeta(
                order_column=order_column_expr,
                partition_column = partition_column.expr,
                hash_column = hash_column.expr if hash_column else None,
                strategy = hash_algo,
                fields = [Field(expr=x.expr) for x in self.column_schema.columns_to_fetch()],
                partition_column_type=partition_column_type
            ),
            type="blockhash"
        )

        partition_id_field = Field(expr=f"partition_id", alias="partition_id", metadata=BlockNameMeta(
            level=partition.level + 1,
            intervals=partition.intervals,
            partition_column_type=partition_column_type,
            strategy=hash_algo,
            partition_column=partition_column.expr,
            parent_partition_id=partition.partition_id
        ), type="blockname")

        select = [
            Field(expr='COUNT(1)', alias='num_rows', type='column'),
            partition_hash_column,
            partition_id_field
        ]
        grp_field = Field(expr="partition_id", type="column")

        filters = []
        if partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            filters += [
                Filter(column=partition_column.expr, operator='>=', value=partition_column.cast(start)), 
                Filter(column=partition_column.expr, operator='<', value=partition_column.cast(end))
            ]
        elif partition_column_type == UniversalDataType.INTEGER:
            filters += [
                Filter(column=partition_column.expr, operator='>=', value=partition_column.cast(start)), 
                Filter(column=partition_column.expr, operator='<', value=partition_column.cast(end))
            ]
        elif partition_column_type == UniversalDataType.VARCHAR:
            filters += [
                Filter(column=partition_column.expr, operator='>=', value=partition_column.cast(start)), 
                Filter(column=partition_column.expr, operator='<=', value=partition_column.cast(end))
            ]
        elif partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            filters += [
                Filter(column=partition_column.expr, operator='>=', value=partition_column.cast(start)), 
                Filter(column=partition_column.expr, operator='<=', value=partition_column.cast(end))
            ]
        else:
            raise ValueError(f"Unsupported partition type: {partition_column_type}")
        filters += self._build_filter_query()
        query = Query(
            select=select,
            table=self._build_table_query(),
            joins=self._build_join_query(),
            filters=filters,
            group_by=[grp_field]
        )
        return query

    async def insert_data(
        self,
        data: List[Dict],
        partition: Optional[MultiDimensionPartition] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:
        """
        Insert (or upsert) data into StarRocks.
        - Uses Stream Load (JSONEachRow) for efficient bulk upserts into Primary Key tables.
        - If Stream Load fails, falls back to INSERT INTO ... VALUES via query endpoint.
        """
        data = self._process_pre_insert_data(data)
        if not data:
            return 0
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        column_keys = [col.expr for col in self.column_schema.columns_to_insert()]
        unique_columns = [col.expr for col in self.column_schema.unique_columns]
        if not unique_columns:
            raise ValueError("Unique keys must be defined in column_mapping")

        total = 0

        # Helper: stream load a batch (JSONEachRow)
        async def _stream_load_batch(batch_rows: List[Dict]) -> None:
            url = self._stream_load_url()
            # label must be unique per request; use timestamp
            label = f"stream_{int(asyncio.get_event_loop().time()*1000)}"
            headers = {
                "label": label,
                "format": "json",         # StarRocks expects format param in header
                "columns": ",".join(column_keys)
            }
            # StarRocks expects body as jsonlines
            payload = "\n".join(json.dumps({col: row.get(col) for col in column_keys}, default=str) for row in batch_rows).encode("utf-8")
            async with self._session.put(url, data=payload, headers=headers) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"Stream load failed {resp.status}: {text}\nURL: {url}\nHeaders: {headers}")
                # Stream load returns JSON with status/result; can inspect for errors if needed
                res = json.loads(text)
                if res.get("Status", "").lower() != "success" and not res.get("Status", "").lower().endswith("success"):
                    # treat non-success statuses as errors to trigger fallback
                    raise RuntimeError(f"Stream load reported failure: {res}")

        # Helper: fallback insert via SQL (batched)
        async def _fallback_insert_batch(batch_rows: List[Dict]) -> None:
            # Build VALUES clause safely
            values_parts = []
            for r in batch_rows:
                vals = [self._format_value(r.get(c)) for c in column_keys]
                values_parts.append("(" + ", ".join(vals) + ")")
            values_sql = ", ".join(values_parts)
            cols_sql = ", ".join([f"`{c}`" for c in column_keys])
            
            table_name = self._full_table()
            if upsert:
                # For StarRocks Primary Key tables, use INSERT with duplicate key update
                non_conflict_cols = [col for col in column_keys if col not in unique_columns]
                update_clause = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in non_conflict_cols)
                sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES {values_sql} ON DUPLICATE KEY UPDATE {update_clause}"
            else:
                sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES {values_sql}"
            await self.execute_query(sql, action="insert")

        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            # Primary Key upsert doesn't need explicit delete — stream load will upsert
            try:
                await _stream_load_batch(batch)
                total += len(batch)
            except Exception as e:
                self.logger.warning(f"Stream load failed; falling back to INSERT for batch {i}:{i+len(batch)}: {e}")
                try:
                    await _fallback_insert_batch(batch)
                    total += len(batch)
                except Exception as ex2:
                    # As final fallback, insert row-by-row to skip bad rows
                    self.logger.error(f"Fallback batch insert failed, trying row-by-row: {ex2}")
                    for row in batch:
                        try:
                            await _fallback_insert_batch([row])
                            total += 1
                        except Exception as row_err:
                            row['failed__']=True
                            self.logger.error(f"Failed to insert row {row}: {row_err}")

        return total

    def _format_value(self, v):
        import datetime
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            return "'" + str(v) + "'"
        return str(v)

    # Schema extraction and DDL generation methods
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract StarRocks table schema"""
        # StarRocks DESCRIBE query
        describe_query = f"DESCRIBE {self._full_table()}"
        columns_raw = await self.execute_query(describe_query)
        
        columns = []
        primary_keys = []
        
        for col in columns_raw:
            col_name = col['Field']
            col_type = col['Type']
            col_key = col.get('Key')
            
            universal_type = self.map_source_type_to_universal(col_type)
            
            # Check if it's a primary key
            is_primary_key = col_key == 'PRI'
            if is_primary_key:
                primary_keys.append(col_name)
            
            column = UniversalColumn(
                name=col_name,
                data_type=universal_type,
                nullable=col['Null'] == 'YES',
                primary_key=is_primary_key,
                unique=False,
                auto_increment=False,
                default_value=col.get('Default'),
                extra=col.get('Extra')
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            database_name=self.database,
            columns=columns,
            primary_keys=primary_keys,
            engine='Primary Key' if primary_keys else 'Duplicate Key'
        )
    
    def generate_create_table_ddl(self, schema: UniversalSchema, target_table_name: Optional[str] = None) -> str:
        """Generate StarRocks CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        if self.database:
            table_name = f"{self.database}.{table_name}"
        
        columns = []
        for col in schema.columns:
            col_type = self.map_universal_type_to_target(col.data_type)
            col_def = f"{col.name} {col_type}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            columns.append(col_def)
        
        # StarRocks Primary Key table
        if schema.primary_keys:
            pk_columns = ', '.join(schema.primary_keys)
            columns.append(f"PRIMARY KEY ({pk_columns})")
            return f"""CREATE TABLE {table_name} (
  {', '.join(columns)}
) ENGINE = Primary Key;"""
        else:
            return f"""CREATE TABLE {table_name} (
  {', '.join(columns)}
) ENGINE = Duplicate Key;"""
    
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map StarRocks type to universal type"""
        type_mapping = {
            'TINYINT': UniversalDataType.SMALLINT,
            'SMALLINT': UniversalDataType.SMALLINT,
            'INT': UniversalDataType.INTEGER,
            'BIGINT': UniversalDataType.BIGINT,
            'FLOAT': UniversalDataType.FLOAT,
            'DOUBLE': UniversalDataType.DOUBLE,
            'DECIMAL': UniversalDataType.DECIMAL,
            'VARCHAR': UniversalDataType.VARCHAR,
            'STRING': UniversalDataType.TEXT,
            'CHAR': UniversalDataType.CHAR,
            'DATE': UniversalDataType.DATE,
            'DATETIME': UniversalDataType.DATETIME,
            'TIMESTAMP': UniversalDataType.TIMESTAMP,
            'BOOLEAN': UniversalDataType.BOOLEAN,
            'JSON': UniversalDataType.JSON,
            'UUID': UniversalDataType.UUID,
        }
        
        normalized_type = source_type.upper().strip()
        # Handle types with precision/scale (e.g., DECIMAL(18,2), VARCHAR(255))
        base_type = normalized_type.split('(')[0] if '(' in normalized_type else normalized_type
        return type_mapping.get(base_type, UniversalDataType.TEXT)
    
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to StarRocks type"""
        type_mapping = {
            UniversalDataType.INTEGER: 'INT',
            UniversalDataType.BIGINT: 'BIGINT',
            UniversalDataType.SMALLINT: 'SMALLINT',
            UniversalDataType.FLOAT: 'FLOAT',
            UniversalDataType.DOUBLE: 'DOUBLE',
            UniversalDataType.DECIMAL: 'DECIMAL(18,2)',
            UniversalDataType.VARCHAR: 'VARCHAR(255)',
            UniversalDataType.TEXT: 'STRING',
            UniversalDataType.CHAR: 'CHAR(1)',
            UniversalDataType.DATE: 'DATE',
            UniversalDataType.TIME: 'DATETIME',
            UniversalDataType.TIMESTAMP: 'DATETIME',
            UniversalDataType.DATETIME: 'DATETIME',
            UniversalDataType.BOOLEAN: 'BOOLEAN',
            UniversalDataType.BLOB: 'STRING',
            UniversalDataType.JSON: 'JSON',
            UniversalDataType.UUID: 'STRING',
        }
        return type_mapping.get(universal_type, 'STRING')
