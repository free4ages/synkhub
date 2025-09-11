import asyncio
import csv
import io
# import gzip
import uuid
import logging
import json
from typing import Any, Dict, List, Optional, Tuple, Union
import aiohttp
from synctool.pipeline.base import DataBatch
from synctool.core.models import StrategyConfig, BackendConfig, DataStorage
from synctool.core.enums import HashAlgo, Capability
from synctool.core.query_models import Query, Field, Filter, BlockHashMeta, BlockNameMeta, RowHashMeta, Table
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import Partition, Column
from ..core.column_mapper import ColumnSchema
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType
from ..utils.hash_cache import HashCache


MAX_MYSQL_PARAMS = 65535  # MySQL limit for placeholders


class StarRocksMySQLBackend(SqlBackend):
    """StarRocks implementation using MySQL async client instead of HTTP"""
    
    # StarRocks-specific capabilities beyond storage type defaults
    _capabilities = {
    }
    
    def __init__(self, config: BackendConfig, column_schema: Optional[ColumnSchema] = None, logger=None, data_storage: Optional[DataStorage] = None):
        super().__init__(config, column_schema, logger=logger, data_storage=data_storage)
        
        # Extract StarRocks-specific connection details from connection_config
        self.host = getattr(self.connection_config, 'host', 'localhost')
        self.port = int(getattr(self.connection_config, 'port', 9030))  # Ensure port is integer
        self.http_port = int(getattr(self.connection_config, 'http_port', 8030))
        self.user = getattr(self.connection_config, 'user', 'root')
        self.password = getattr(self.connection_config, 'password', '')
        self.database = getattr(self.connection_config, 'database', None)
        self.fe_host = f"http://{self.host}:{self.http_port}"
        
        self._pool = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_kwargs = {}
        self._connection_session_vars_set = set()  # Track which connections have session vars set
        
        # Scale up cache size for better performance
        cache_size = int(getattr(config, 'cache_size', 5000))
        # self.hash_cache = HashCache(max_rows=cache_size)
        self.hash_cache = None
    
    def _get_default_schema(self) -> str:
        return ''
    
    async def connect(self) -> None:
        """Connect to StarRocks database via MySQL protocol"""
        try:
            import aiomysql
            self.logger.info(f"Connecting to StarRocks via MySQL: host={self.host}, port={self.port}, user={self.user}, database={self.database}")
            
            # Get connection pool settings, ensuring they're integers
            # Increase defaults for better concurrency support
            # minsize = int(getattr(self.connection_config, 'min_connections', 5))
            # maxsize = int(getattr(self.connection_config, 'max_connections', 50))
            minsize = 10
            maxsize = 50
            
            self._pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.database,
                autocommit=True,
                minsize=minsize,
                maxsize=maxsize,
                pool_recycle=3600,  # Recycle connections every hour
                echo=False,
                # charset='utf8mb4'
            )
            
            # Test connection
            await self._test_connection()
            
        except ImportError:
            raise ImportError("aiomysql is required for StarRocks MySQL provider")
        except Exception as e:
            if self._pool:
                self._pool.close()
                await self._pool.wait_closed()
                self._pool = None
            raise RuntimeError(f"Failed to connect to StarRocks: {e}")
        
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
                    limit=20,
                    ttl_dns_cache=300,
                    use_dns_cache=True,
                    limit_per_host=20 
                )
                
                timeout = aiohttp.ClientTimeout(total=50, connect=20)
                
                self._session = aiohttp.ClientSession(
                    auth=auth,
                    headers=headers,
                    connector=connector,
                    timeout=timeout,
                    **self._session_kwargs
                )
                self.logger.info(f"Connecting to StarRocks: host={self.host}, port={self.port}, user={self.user}, database={self.database}")
                
                
        except Exception as e:
            if self._session:
                await self._session.close()
                self._session = None
            raise RuntimeError(f"Failed to connect to StarRocks: {e}")
    
    def _stream_load_url(self) -> str:
        # Stream load endpoint: /api/{db}/{tbl}/_stream_load
        if self.database:
            return f"{self.fe_host}/api/{self.database}/{self.table}/_stream_load"
        else:
            return f"{self.fe_host}/api/{self.table}/_stream_load"
    

    async def _set_session_variables_for_connection(self, conn):
        """Set session variables for optimal performance on a specific connection"""
        try:
            # Set group_concat_max_len to 100MB to handle large concatenated strings in GROUP_CONCAT
            session_query = "SET SESSION group_concat_max_len = 104857600"
            async with conn.cursor() as cur:
                await cur.execute(session_query)
            self.logger.debug(f"StarRocks MySQL session variables configured successfully for connection {id(conn)}")
        except Exception as e:
            self.logger.warning(f"Failed to set session variables for connection {id(conn)}: {e}")
            # Don't raise exception as this is not critical for basic functionality

    async def _test_connection(self):
        """Test the connection to StarRocks"""
        try:
            test_query = "SELECT 1 as test"
            await self.execute_query(test_query)
            
            # Verify session variables are set correctly
            session_check_query = "SELECT @@session.group_concat_max_len as group_concat_max_len"
            result = await self.execute_query(session_check_query)
            if result and result[0]['group_concat_max_len'] == 104857600:
                self.logger.info("StarRocks MySQL connection test successful - session variables configured correctly")
            else:
                self.logger.warning(f"Session variables may not be set correctly. group_concat_max_len = {result[0]['group_concat_max_len'] if result else 'unknown'}")
        except Exception as e:
            raise RuntimeError(f"StarRocks MySQL connection test failed: {e}")

    async def disconnect(self) -> None:
        """Disconnect from StarRocks database"""
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
        
        if self._session:
            await self._session.close()
            self._session = None
        
        # Clear connection tracking
        self._connection_session_vars_set.clear()

    def _full_table(self) -> str:
        if self.database:
            return f"`{self.database}`.`{self.table}`"
        return f"`{self.table}`"

    async def execute_query(self, query: str, params: Optional[list] = None, action: Optional[str] = 'select') -> List[Dict]:
        """Execute query and return List[Dict]"""
        if not self._pool:
            raise RuntimeError("Not connected. Call connect() first.")

        self.logger.info(f"Executing query: {query} with params: {params}")
        # if params:
        #     self.logger.info(f"Query params: {params}")
        
        async with self._pool.acquire() as conn:
            # Set session variables for this connection if not already set
            conn_id = id(conn)
            if conn_id not in self._connection_session_vars_set:
                await self._set_session_variables_for_connection(conn)
                self._connection_session_vars_set.add(conn_id)
            
            async with conn.cursor() as cur:
                await cur.execute(query, params or [])
                if action == 'select':
                    # Fetch column names
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    rows = await cur.fetchall()
                    # Convert to list of dictionaries
                    return [dict(zip(columns, row)) for row in rows]
                elif action == 'delete':
                    await conn.commit()
                    return cur.rowcount
                elif action == 'insert':
                    await conn.commit()
                    return cur.rowcount
                else:
                    await conn.commit()
                    return cur.rowcount
    
    async def execute_http_query(
        self,
        sql: str,
        action: str = "query",
        timeout: int = 60
    ) -> Union[List[Dict], None]:
        """
        Execute a SQL query against StarRocks via the REST API.

        Parameters
        ----------
        sql : str
            The SQL statement to run.
        action : str
            A label for logging/metrics (e.g., "insert", "query").
        timeout : int
            Request timeout in seconds.

        Returns
        -------
        List[Dict] | None
            - For SELECT queries: list of rows (dicts).
            - For DML/DDL queries: None (success).
        """

        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        url = f"{self.fe_host}/query/sql"
        headers = {
            "Content-Type": "application/json",
        }
        body = {
            "sql": sql
        }

        self.logger.debug(f"[StarRocks] Executing {action}: {sql}")

        async with self._session.post(url, headers=headers, json=body, timeout=timeout) as resp:
            text = await resp.text()

            if resp.status != 200:
                raise RuntimeError(
                    f"StarRocks query failed {resp.status}: {text}\nSQL: {sql}"
                )

            try:
                res = json.loads(text)
            except json.JSONDecodeError:
                raise RuntimeError(f"Invalid response from StarRocks: {text}")

            # StarRocks returns { "status": "OK", "data": [...], "msg": "" }
            status = res.get("status", "").lower()
            if status not in ("ok", "success"):
                raise RuntimeError(f"StarRocks query reported failure: {res}")

            # SELECT → return rows
            if sql.strip().lower().startswith("select"):
                # Response has 'data' (list of rows) and 'meta' (column names)
                meta = [c["name"] for c in res.get("meta", [])]
                rows = []
                for row in res.get("data", []):
                    rows.append(dict(zip(meta, row)))
                return rows

            # INSERT/UPDATE/DDL → no rows
            return None


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

    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        if page_size is not None or offset is not None:
            order_by = [f"{c.expr} {c.direction}" for c in self.column_schema.order_columns] if self.column_schema.order_columns else [f"{self.column_schema.partition_column.expr} asc"]
        else:
            order_by = None
        query = self._build_partition_data_query(partition, order_by=order_by, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)

    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
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

    async def fetch_partition_row_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None, skip_cache=False) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        # Use HashCache for optimized fetching
        fallback_fn = lambda: self._fetch_partition_row_hashes_direct(partition, hash_algo, page_size, offset)
        if self.hash_cache and not skip_cache:
            return await self.hash_cache.fetch_partition_row_hashes(partition, self, fallback_fn, hash_algo, page_size=page_size, offset=offset)
        else:
            return await fallback_fn()

    async def fetch_child_partition_hashes(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch child partition hashes"""
        # Use HashCache for optimized fetching
        if partition is None:
            raise ValueError("Partition is required for hash cache operations")
        
        if not self.column_schema:
            raise ValueError("Column schema is required for hash cache operations")
        
        fallback_fn = lambda: self._fetch_child_partition_hashes_direct(partition, with_hash, hash_algo)
        if self.hash_cache:
            return await self.hash_cache.fetch_child_partition_hashes(
                partition, self, fallback_fn, hash_algo
            )
        else:
            return await fallback_fn()

    async def _fetch_child_partition_hashes_direct(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Direct database fetch for child partition hashes - used by HashCache"""
        query: Query = self._build_partition_hash_query(partition, hash_algo)
        query = self._rewrite_query(query)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)

    async def _fetch_partition_row_hashes_direct(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
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
        partition_column_type = str(self.column_schema.partition_column.dtype) if self.column_schema.partition_column else ""
        
        # This would need intervals from the partition context - for now return empty
        # In practice, this would be called with proper context from partition processor
        return []

    async def delete_partition_data(self, partition: Partition) -> int:
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

    async def delete_rows(self, rows: List[Dict], partition: Partition, strategy_config: StrategyConfig) -> int:
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

    async def fetch_partition_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
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
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS CHAR))" if parent_partition_id else f"CAST({expr} AS CHAR)"

        elif partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            expr = f"FLOOR((UNIX_TIMESTAMP({partition_column}) - {parent_offset}) / {intervals[level]})"
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS CHAR))" if parent_partition_id else f"CAST({expr} AS CHAR)"

        elif partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            # StarRocks UUID handling via MySQL - convert hex to integer
            expr = f"FLOOR((CONV(SUBSTR({partition_column}, 1, 8), 16, 10) - {parent_offset})/{intervals[level]})"
            return f"CONCAT('{parent_partition_id}', '-', CAST({expr} AS CHAR))" if parent_partition_id else f"CAST({expr} AS CHAR)"

        else:
            raise ValueError(f"Unsupported partition type: {partition_column}")

    def _build_rowhash_expr(self, field: Field) -> str:
        """Build row hash expression for StarRocks via MySQL"""
        metadata: RowHashMeta = field.metadata
        expr=""
        if metadata.hash_column:
            expr = metadata.hash_column
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            concat = ", ".join([f"CAST({x.expr} AS CHAR)" for x in metadata.fields])
            expr = f"CONV(SUBSTR(MD5(CONCAT({concat})), 1, 8), 16, 10)"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            concat = ", ".join([f"CAST({x.expr} AS CHAR)" for x in metadata.fields])
            expr = f"MD5(CONCAT({concat}))"
        return expr

    def _build_blockhash_expr(self, field: Field):
        metadata: BlockHashMeta = field.metadata
        inner_expr = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.MD5_SUM_HASH:
            expr = f"SUM({inner_expr})"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            expr = f"MD5(GROUP_CONCAT(LEFT({inner_expr},8) ORDER BY {metadata.order_column} SEPARATOR ''))"
        return expr

    def _rewrite_query(self, query: Query) -> Query:
        """Rewrite query to handle StarRocks-specific expressions via MySQL"""
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
        """Build SQL query for StarRocks via MySQL"""
        q = self._rewrite_query(query)
        return SqlBuilder.build(q, dialect='mysql')  # Use MySQL dialect for StarRocks MySQL connection

    def _build_partition_hash_query(self, partition: Partition, hash_algo: HashAlgo) -> Query:
        """Build partition hash query for StarRocks via MySQL"""
        start = partition.start
        end = partition.end
        partition_column: Column = self.column_schema.column(partition.column)
        partition_column_type: UniversalDataType | None = partition_column.dtype

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
        data_batch: DataBatch,
        upsert: bool = True
    ) -> int:
        """
        Insert (or upsert) data into StarRocks.
        - Uses Stream Load (JSONEachRow) for efficient bulk upserts into Primary Key tables.
        - If Stream Load fails, falls back to INSERT INTO ... VALUES via query endpoint.
        """
        data = data_batch.data
        data = self._process_pre_insert_data(data)
        if not data:
            return 0
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        column_keys = [col.expr for col in self.column_schema.columns_to_insert()]
        unique_columns = [col.expr for col in self.column_schema.unique_columns]
        if not unique_columns:
            raise ValueError("Unique keys must be defined in column_mapping")

        success, failed = 0, 0

        # Helper: stream load a batch (JSONEachRow)
        # async def _stream_load_batch(batch_rows: List[Dict]) -> None:
        #     url = self._stream_load_url()
        #     # label must be unique per request; use timestamp
        #     label = f"stream_{int(asyncio.get_event_loop().time()*1000)}_{uuid.uuid4().hex[:6]}"
        #     headers = {
        #         "label": label,
        #         "format": "json",         # StarRocks expects format param in header
        #         "columns": ",".join(column_keys)
        #     }
        #     # StarRocks expects body as jsonlines
        #     payload = "\n".join(json.dumps({col: row.get(col) for col in column_keys}, default=str) for row in batch_rows).encode("utf-8")
        #     async with self._session.put(url, data=payload, headers=headers) as resp:
        #         text = await resp.text()
        #         if resp.status != 200:
        #             raise RuntimeError(f"Stream load failed {resp.status}: {text}\nURL: {url}\nHeaders: {headers}")
        #         # Stream load returns JSON with status/result; can inspect for errors if needed
        #         res = json.loads(text)
        #         if res.get("Status", "").lower() != "success" and not res.get("Status", "").lower().endswith("success"):
        #             # treat non-success statuses as errors to trigger fallback
        #             raise RuntimeError(f"Stream load reported failure: {res}")


        async def _stream_load_batch(batch_rows: List[Dict]) -> None:
            url = self._stream_load_url()
            label = f"stream_{int(asyncio.get_event_loop().time()*1000)}_{uuid.uuid4().hex[:6]}"

            # Write rows into CSV format in-memory
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
            for row in batch_rows:
                writer.writerow([row.get(col,"") if row.get(col) is not None else "" for col in column_keys])

            # Compress payload to reduce size
            # payload = gzip.compress(buf.getvalue().encode("utf-8"))
            payload = buf.getvalue().encode("utf-8")

            headers = {
                "label": label,
                "format": "csv",
                "columns": ",".join(column_keys),
                # "merge_type": "MERGE" if upsert else "APPEND",
                # "merge_type": "MERGE",
                # "Content-Encoding": "gzip",   # tell StarRocks we gzipped the body
                "Expect": "100-continue",
                "column_separator": ","
            }
            try:

                async with self._session.put(url, data=payload, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status in (502, 503, 504):
                        raise ConnectionError(f"StarRocks unavailable (HTTP {resp.status}): {text}")

                    if resp.status != 200:
                        raise RuntimeError(
                            f"Stream load failed {resp.status}: {text}\nURL: {url}\nHeaders: {headers}"
                        )
                                # Parse response JSON
                    try:
                        res = json.loads(text)
                    except json.JSONDecodeError:
                        raise RuntimeError(f"Invalid response from StarRocks: {text}")
                    # res = json.loads(text)
                    status = res.get("Status", "").lower()
                    if "success" not in status:
                        # Differentiate between system errors vs data errors
                        msg = res.get("Message", "").lower()
                        if "unavailable" in msg or "disconnect" in msg:
                            raise ConnectionError(f"Stream load server issue: {res}")
                        elif "too many versions" in msg:
                            raise ConnectionError(f"Stream load storage version limit: {res}")
                        else:
                            raise ValueError(f"Stream load data issue: {res}")
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError) as net_err:
                raise ConnectionError(f"Stream load network error: {net_err}") from net_err


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
            await self.execute_http_query(sql, action="insert")
        # Process in batches
        for i in range(1):
            batch = data
            # Primary Key upsert doesn't need explicit delete — stream load will upsert
            try:
                await _stream_load_batch(batch)
                success += len(batch)
            except ConnectionError as ce:
                failed += len(batch)
                # Server is down — abort the whole insert
                self.logger.error(f"Stream load aborted: server unavailable {ce}")
                raise
            except ValueError as ve:
                self.logger.warning(f"Stream load failed; falling back to INSERT for batch {i}:{i+len(batch)}: {e}")
                try:
                    await _fallback_insert_batch(batch)
                    success += len(batch)
                except Exception as ex2:
                    # As final fallback, insert row-by-row to skip bad rows
                    self.logger.error(f"Fallback batch insert failed, trying row-by-row: {ex2}")
                    for row in batch:
                        try:
                            await _fallback_insert_batch([row])
                            success += 1
                        except Exception as row_err:
                            failed += 1
                            self.logger.error(f"Failed to insert row {row}: {row_err}")
            except Exception as e:
                # Other errors → log and attempt one batch fallback
                self.logger.warning(f"Stream load failed with unexpected error; trying batch insert: {e}")
                try:
                    await _fallback_insert_batch(batch)
                    success += len(batch)
                except Exception as ex2:
                    self.logger.error(f"Batch insert failed after unexpected error: {ex2}")
                    failed += len(batch)
                    raise

        return success, failed
    

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

    # async def insert_data(
    #     self,
    #     data: List[Dict],
    #     partition: Optional[Partition] = None,
    #     batch_size: int = 5000,
    #     upsert: bool = True
    # ) -> int:
    #     """
    #     Insert (or upsert) data into StarRocks via MySQL protocol.
    #     Uses MySQL-style INSERT ... ON DUPLICATE KEY UPDATE for upserts.
    #     Improved for concurrent operations with better connection management.
    #     Falls back to row-by-row insertion if bulk insert fails.
    #     """
    #     data = self._process_pre_insert_data(data)
    #     if not data:
    #         return 0
    #     if not self._pool:
    #         raise RuntimeError("Not connected. Call connect() first.")

    #     column_keys = [col.expr for col in self.column_schema.columns_to_insert()]
    #     unique_columns = [col.expr for col in self.column_schema.unique_columns]
    #     if not unique_columns:
    #         raise ValueError("Unique keys must be defined in column_mapping")

    #     table_name = self._full_table()
    #     insert_cols = ', '.join(f"`{col}`" for col in column_keys)
    #     base_insert_sql = f"INSERT INTO {table_name} ({insert_cols}) VALUES {{}}"

    #     total_inserted = 0
    #     max_safe_rows = MAX_MYSQL_PARAMS // len(column_keys)
    #     safe_batch_size = min(batch_size, max_safe_rows)

    #     # Use a single connection for the entire insert operation
    #     async with self._pool.acquire() as conn:
    #         async with conn.cursor() as cur:
    #             for i in range(0, len(data), safe_batch_size):
    #                 batch = data[i:i + safe_batch_size]
                    
    #                 try:
    #                     # Try bulk insert first
    #                     placeholders = ', '.join(
    #                         "(" + ", ".join(["%s"] * len(column_keys)) + ")"
    #                         for _ in batch
    #                     )
    #                     flat_params = [row.get(col) for row in batch for col in column_keys]
    #                     sql = base_insert_sql.format(placeholders)
                        
    #                     await cur.execute(sql, flat_params)
    #                     total_inserted += len(batch)
                        
    #                 except Exception as e:
    #                     self.logger.error(f"Failed to insert batch {i}:{i+len(batch)}: {e}")
    #                     self.logger.info(f"Falling back to row-by-row insertion for batch {i}")
                        
    #                     # Fallback to row-by-row insertion using the same connection
    #                     batch_inserted = await self._insert_batch_row_by_row_with_cursor(batch, base_insert_sql, column_keys, cur, i)
    #                     total_inserted += batch_inserted

    #     return total_inserted

    # async def _insert_batch_row_by_row_with_cursor(
    #     self, 
    #     batch: List[Dict], 
    #     base_insert_sql: str, 
    #     column_keys: List[str], 
    #     cur,
    #     batch_index: int
    # ) -> int:
    #     """Insert batch row by row as fallback when bulk insert fails, using existing cursor"""
    #     inserted = 0
    #     for row_idx, row in enumerate(batch):
    #         try:
    #             single_placeholder = "(" + ", ".join(["%s"] * len(column_keys)) + ")"
    #             single_params = [row.get(col) for col in column_keys]
    #             single_sql = base_insert_sql.format(single_placeholder)
    #             await cur.execute(single_sql, single_params)
    #             inserted += 1
    #         except Exception as row_err:
    #             self.logger.error(f"Failed to insert row {batch_index}:{row_idx} {row}: {row_err}")
    #     return inserted

    # Schema extraction and DDL generation methods
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract StarRocks table schema via MySQL protocol"""
        # StarRocks DESCRIBE query via MySQL
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
