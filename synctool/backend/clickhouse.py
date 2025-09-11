import aiohttp
import asyncio
import json
import math
import logging
from typing import Any, Dict, List, Optional, Tuple, Set

from synctool.core.models import StrategyConfig, BackendConfig, DataStorage
from synctool.core.enums import HashAlgo, Capability
from synctool.core.query_models import Query, Field, Filter, BlockHashMeta, BlockNameMeta, RowHashMeta
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import Partition
from ..core.column_mapper import ColumnSchema
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType

logger = logging.getLogger(__name__)


class ClickHouseBackend(SqlBackend):
    """
    Async ClickHouse backend using HTTP API.
    - Default engine: MergeTree (lightweight delete + insert for updates)
    - Optionally use ReplacingMergeTree by providing replacing_column
    """

    def __init__(
        self,
        config: BackendConfig,
        column_schema: Optional[ColumnSchema] = None,
        host: str = "http://localhost:8123",
        database: Optional[str] = None,
        table: Optional[str] = None,
        engine: str = "MergeTree",
        replacing_column: Optional[str] = None,
        batch_size: int = 10_000,
        session_kwargs: Optional[dict] = None,
        logger=None,
        data_storage: Optional['DataStorage'] = None
    ):
        super().__init__(config, column_schema, logger=logger, data_storage=data_storage)
        self.host = host.rstrip("/")
        self.database = database or config.database if hasattr(config, "database") else None
        self.table = table or getattr(self, "table", None)
        self.engine = engine.lower()
        self.replacing_column = replacing_column
        self.batch_size = batch_size
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_kwargs = session_kwargs or {}
        self.logger = logger
    
    def get_capabilities(self) -> Set[Capability]:
        """Override to handle ClickHouse-specific capability logic"""
        if self._computed_capabilities is None:
            # Get base capabilities
            capabilities = super().get_capabilities()
            
            # ClickHouse doesn't support traditional UPSERT, so remove it
            capabilities.discard(Capability.UPSERT_SUPPORT)
            
            # Update caches
            self._computed_capabilities = capabilities
            self._capability_lookup = {cap: True for cap in capabilities}
        
        return self._computed_capabilities.copy()

    async def connect(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(**self._session_kwargs)

    async def disconnect(self):
        if self._session:
            await self._session.close()
            self._session = None

    def _full_table(self) -> str:
        if self.database:
            return f"{self.database}.{self.table}"
        return self.table

    async def _execute(self, sql: str, *, params: Optional[dict] = None, fmt: str = "JSON") -> Any:
        """
        Execute arbitrary SQL and return parsed JSON if possible.
        Note: Use JSONEachRow or JSON format when running SELECTs that return rows.
        """
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        url = f"{self.host}/?query={aiohttp.helpers.quote(sql)}&default_format={fmt}"
        # simple POST to support large payloads or queries with body
        async with self._session.post(url) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"ClickHouse error {resp.status}: {text}\nSQL: {sql}")
            if fmt.upper().startswith("JSON"):
                # JSON or JSONEachRow consumers will differ.
                try:
                    return json.loads(text)
                except Exception:
                    return text
            return text

    async def execute_query(self, query: str, action: Optional[str] = "select") -> List[Dict]:
        """
        Execute SELECT or ALTER/DELETE/INSERT statements.
        For SELECT returns list[dict] (JSONEachRow) to match other backends.
        """
        if action == "select":
            # we request JSONEachRow for row streaming; parse line-by-line
            if not self._session:
                raise RuntimeError("Not connected")
            url = f"{self.host}/?query={aiohttp.helpers.quote(query)}&default_format=JSONEachRow"
            async with self._session.post(url) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"ClickHouse error {resp.status}: {text}\nSQL: {query}")
                # JSONEachRow returns newline separated JSON objects
                rows = []
                for line in text.splitlines():
                    if not line.strip():
                        continue
                    rows.append(json.loads(line))
                return rows
        else:
            # For ALTER / DELETE / INSERT we don't need structured return
            res = await self._execute(query, fmt="JSON")
            return res

    async def has_data(self) -> bool:
        q = f"SELECT 1 FROM {self._full_table()} LIMIT 1"
        rows = await self.execute_query(q)
        return len(rows) > 0

    async def get_last_sync_point(self) -> Any:
        if not self.column_schema or not self.column_schema.delta_column:
            raise ValueError("No delta key in column schema")
        column = self.column_schema.delta_column.expr
        q = f"SELECT max({column}) as last_sync_point FROM {self._full_table()}"
        rows = await self.execute_query(q)
        return rows[0].get("last_sync_point")

    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        if not self.column_schema or not self.column_schema.partition_column:
            raise ValueError("No partition key")
        col = self.column_schema.partition_column.expr
        q = f"SELECT min({col}) AS min_val, max({col}) AS max_val FROM {self._full_table()}"
        rows = await self.execute_query(q)
        r = rows[0] if rows else {}
        return r.get("min_val"), r.get("max_val")

    # ---- Query rewrite helpers (adapted for ClickHouse SQL) ----
    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level
        intervals = metadata.intervals
        partition_column = metadata.partition_column
        partition_column_type = metadata.partition_column_type

        if partition_column_type == "integer":
            segments = []
            for idx in range(level + 1):
                if idx == 0:
                    expr = f"toString(intDiv({partition_column}, {intervals[idx]}))"
                else:
                    prev = intervals[idx - 1]
                    expr = f"toString(intDiv({partition_column} % {prev}, {intervals[idx]}))"
                segments.append(expr)
            return " || '-' || ".join(segments)  # ClickHouse supports || for concat in recent versions (or use concat)
        elif partition_column_type == "datetime":
            # use toUnixTimestamp for datetime handling
            segments = []
            for idx in range(level + 1):
                if idx == 0:
                    expr = f"toString(intDiv(toUnixTimestamp({partition_column}), {intervals[idx]}))"
                else:
                    prev = intervals[idx - 1]
                    expr = f"toString(intDiv(toUnixTimestamp({partition_column}) % {prev}, {intervals[idx]}))"
                segments.append(expr)
            return " || '-' || ".join(segments)
        else:
            raise ValueError(f"Unsupported partition type: {partition_column_type}")

    def _build_rowhash_expr(self, field: Field) -> str:
        metadata: RowHashMeta = field.metadata
        if metadata.hash_column:
            return metadata.hash_column
        concat = "||".join([f"toString({x.expr})" for x in metadata.fields])
        if metadata.strategy == HashAlgo.HASH_MD5_HASH:
            return f"md5({concat})"
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            # approximate numeric aggregation via hex->int
            return f"toUInt64(conv(substring(md5({concat}),1,16), 16, 10))"
        return ""

    def _build_blockhash_expr(self, field: Field) -> str:
        metadata: BlockHashMeta = field.metadata
        inner = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.HASH_MD5_HASH:
            # string_agg equivalent using groupArray + arrayStringConcat
            return f"md5(arrayStringConcat(arrayMap(x -> x, groupArray({inner}))) )"
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            return f"sum({inner})"
        return ""

    def _rewrite_query(self, query: Query) -> Query:
        rewritten = []
        for f in query.select:
            if f.type == "blockhash":
                expr = self._build_blockhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type="column"))
            elif f.type == "blockname":
                expr = self._build_group_name_expr(f)
                # ClickHouse supports concat via concat(...) or '||' on some builds; use concat for safety
                # If expr contains ' || '-' || ' markers, replace with concat(...)
                if " || '-' || " in expr:
                    parts = expr.split(" || '-' || ")
                    concat_expr = "concat(" + ", '-'," .join(parts) + ")"
                    rewritten.append(Field(expr=concat_expr, alias=f.alias, type="column"))
                else:
                    rewritten.append(Field(expr=f"concat({expr})", alias=f.alias, type="column"))
            elif f.type == "rowhash":
                expr = self._build_rowhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type="column"))
            else:
                rewritten.append(f)
        query.select = rewritten
        return query

    def _build_sql(self, query: Query) -> Tuple[str, list]:
        q = self._rewrite_query(query)
        # attempt to use the SqlBuilder if available; builder may not support clickhouse dialect
        try:
            return SqlBuilder.build(q, dialect="clickhouse")
        except Exception:
            # Fallback: naive SQL generation (very simplified)
            select = ", ".join([f.expr + (f" AS {f.alias}" if f.alias else "") for f in q.select])
            table = q.table.name if q.table and hasattr(q.table, "name") else self._full_table()
            where = ""
            if q.filters:
                where_clauses = []
                for flt in q.filters:
                    where_clauses.append(f"{flt.column} {flt.operator} {self._format_value(flt.value)}")
                where = " WHERE " + " AND ".join(where_clauses)
            sql = f"SELECT {select} FROM {table}{where}"
            return sql, []

    def _format_value(self, v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "1" if v else "0"
        return str(v)

    def _build_delete_where(self, keys: List[str], rows: List[Dict]) -> str:
        """
        Build WHERE clause for ALTER TABLE ... DELETE WHERE (ClickHouse lightweight delete).
        The result is a disjunction of conjunctions: (k1 = v1 AND k2 = v2) OR ...
        """
        clauses = []
        for row in rows:
            conds = []
            for k in keys:
                conds.append(f"{k} = {self._format_value(row.get(k))}")
            clauses.append("(" + " AND ".join(conds) + ")")
        return " OR ".join(clauses) if clauses else ""

    # ---- Data operations ----
    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        # Build a Query using _build_partition_data_query from your base class if available.
        query = self._build_partition_data_query(partition, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, _ = self._build_sql(query)
        return await self.execute_query(sql, action="select")

    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        if not self.column_schema or not self.column_schema.delta_column:
            raise ValueError("No delta key configured")
        partition_column = self.column_schema.delta_column.name
        query = self._build_partition_data_query(partition, partition_column=partition_column, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, _ = self._build_sql(query)
        return await self.execute_query(sql, action="select")

    async def fetch_partition_row_hashes(self, partition: Partition, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        return await self.fetch_partition_data(partition, with_hash=True, hash_algo=hash_algo)

    async def fetch_child_partition_hashes(self, partition: Optional[Partition] = None, with_hash=False, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        query = self._build_partition_hash_query(partition, hash_algo)
        query = self._rewrite_query(query)
        sql, _ = self._build_sql(query)
        return await self.execute_query(sql, action="select")

    async def delete_partition_data(self, partition: Partition) -> Any:
        # Build filters for partition bounds using the column_schema.partition_column
        if not self.column_schema or not self.column_schema.partition_column:
            raise ValueError("No partition key configured")
        col = self.column_schema.partition_column.expr
        where = f"{col} >= {self._format_value(partition.start)} AND {col} < {self._format_value(partition.end)}"
        sql = f"ALTER TABLE {self._full_table()} DELETE WHERE {where}"
        return await self.execute_query(sql, action="delete")

    async def delete_rows(self, rows: List[Dict], partition: Partition, strategy_config: StrategyConfig) -> Any:
        if not rows:
            return 0
        unique_columns = [col.expr for col in self.column_schema.unique_columns]
        where = self._build_delete_where(unique_columns, rows)
        sql = f"ALTER TABLE {self._full_table()} DELETE WHERE {where}"
        return await self.execute_query(sql, action="delete")

    async def insert_data(self, data: List[Dict], partition: Optional[Partition] = None, batch_size: int = 5000, upsert: bool = True) -> int:
        """
        Insert data to ClickHouse.
        For MergeTree + upsert=True: performs ALTER TABLE ... DELETE WHERE <keys from rows> then INSERT.
        For ReplacingMergeTree: just INSERT (make sure you manage the version column externally).
        """
        if not data:
            return 0
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        columns = list(data[0].keys())
        total = 0
        unique_columns = [c.expr for c in self.column_schema.unique_columns] if self.column_schema and self.column_schema.unique_columns else None

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]

            # If upsert requested and engine is MergeTree -> delete matching keys first
            if upsert and unique_columns and self.engine == "mergetree":
                where = self._build_delete_where(unique_columns, batch)
                if where:
                    del_sql = f"ALTER TABLE {self._full_table()} DELETE WHERE {where}"
                    await self.execute_query(del_sql, action="delete")

            # Prepare JSONEachRow payload for insert
            payload_lines = []
            for row in batch:
                # ensure keys order matches columns
                obj = {col: row.get(col) for col in columns}
                payload_lines.append(json.dumps(obj, default=str))

            payload = "\n".join(payload_lines)

            # Do the insert via HTTP endpoint: INSERT INTO table FORMAT JSONEachRow
            insert_sql = f"INSERT INTO {self._full_table()} ({', '.join(columns)}) FORMAT JSONEachRow"
            # use post with query as URL and payload as body
            url = f"{self.host}/?query={aiohttp.helpers.quote(insert_sql)}"
            async with self._session.post(url, data=payload.encode("utf-8")) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"ClickHouse insert failed {resp.status}: {text}\nSQL: {insert_sql}")
            total += len(batch)

        return total

    # ---- Partition hash builder (similar to Postgres version but using ClickHouse functions) ----
    def _build_partition_hash_query(self, partition: Partition, hash_algo: HashAlgo) -> Query:
        start = partition.start
        end = partition.end
        partition_column_type = partition.column_type

        hash_column = self.column_schema.hash_key.expr if self.column_schema.hash_key else None
        partition_column = self.column_schema.partition_column.expr
        order_columns = self.column_schema.order_columns
        order_column = ",".join([f"{c.expr} {d}" for c, d in order_columns]) if order_columns else None

        partition_hash_column = Field(
            expr=f"{partition_column}",
            alias="partition_hash",
            metadata=BlockHashMeta(
                order_column=order_column,
                partition_column=partition_column,
                hash_column=hash_column,
                strategy=hash_algo,
                fields=[Field(expr=x.expr) for x in self.column_schema.columns_to_fetch()],
                partition_column_type=partition_column_type
            ),
            type="blockhash"
        )

        partition_id_field = Field(expr=f"partition_id", alias="partition_id", metadata=BlockNameMeta(
            level=partition.level + 1,
            intervals=partition.intervals,
            partition_column_type=partition_column_type,
            strategy=hash_algo,
            partition_column=partition_column
        ), type="blockname")

        select = [
            Field(expr="count()", alias="num_rows", type="column"),
            partition_hash_column,
            partition_id_field
        ]
        grp_field = Field(expr="partition_id", type="column")

        filters = []
        if partition_column_type in ("datetime", "integer", "string"):
            filters += [
                Filter(column=partition_column, operator=">=", value=start),
                Filter(column=partition_column, operator="<", value=end)
            ]
        filters += self._build_filter_query()

        query = Query(
            select=select,
            table=self._build_table_query(),
            joins=self._build_join_query(),
            filters=filters,
            group_by=[grp_field]
        )
        return query

    # Schema extraction and DDL generation methods
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract ClickHouse table schema"""
        # ClickHouse DESCRIBE TABLE query
        describe_query = f"DESCRIBE TABLE {self._full_table_name(table_name)}"
        columns_raw = await self.execute_query(describe_query)
        
        # Get table engine and settings
        create_query = f"SHOW CREATE TABLE {self._full_table_name(table_name)}"
        create_result = await self.execute_query(create_query)
        create_statement = create_result[0]['statement'] if create_result else ""
        
        # Parse ClickHouse column format
        columns = []
        primary_keys = []
        
        for col in columns_raw:
            # ClickHouse DESCRIBE returns: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
            col_name = col['name']
            col_type = col['type']
            
            universal_type = self.map_source_type_to_universal(col_type)
            
            # Check if it's a primary key (ClickHouse doesn't have traditional PKs, but we can infer from ORDER BY)
            is_primary_key = False
            if 'ORDER BY' in create_statement and col_name in create_statement:
                # Simple heuristic - could be enhanced
                is_primary_key = True
                primary_keys.append(col_name)
            
            column = UniversalColumn(
                name=col_name,
                data_type=universal_type,
                nullable='Nullable' in col_type,
                primary_key=is_primary_key,
                unique=False,
                auto_increment=False,
                default_expression=col.get('default_expression'),
                comment=col.get('comment')
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            database_name=self.database,
            columns=columns,
            primary_keys=primary_keys,
            engine='MergeTree'  # Default ClickHouse engine
        )
    
    def generate_create_table_ddl(self, schema: UniversalSchema, target_table_name: Optional[str] = None) -> str:
        """Generate ClickHouse CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        if self.database:
            table_name = f"{self.database}.{table_name}"
        
        columns = []
        for col in schema.columns:
            col_type = self.map_universal_type_to_target(col.data_type)
            columns.append(f"{col.name} {col_type}")
        
        # Determine order by column
        order_by = schema.primary_keys[0] if schema.primary_keys else schema.columns[0].name
        
        return f"""CREATE TABLE {table_name} (
  {', '.join(columns)}
) ENGINE = MergeTree()
ORDER BY {order_by};"""
    
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map ClickHouse type to universal type"""
        type_mapping = {
            'Int8': UniversalDataType.SMALLINT,
            'Int16': UniversalDataType.SMALLINT,
            'Int32': UniversalDataType.INTEGER,
            'Int64': UniversalDataType.BIGINT,
            'UInt8': UniversalDataType.SMALLINT,
            'UInt16': UniversalDataType.SMALLINT,
            'UInt32': UniversalDataType.INTEGER,
            'UInt64': UniversalDataType.BIGINT,
            'Float32': UniversalDataType.FLOAT,
            'Float64': UniversalDataType.DOUBLE,
            'Decimal': UniversalDataType.DECIMAL,
            'String': UniversalDataType.TEXT,
            'FixedString': UniversalDataType.CHAR,
            'Date': UniversalDataType.DATE,
            'DateTime': UniversalDataType.TIMESTAMP,
            'DateTime64': UniversalDataType.TIMESTAMP,
            'Array': UniversalDataType.JSON,  # Simplified mapping
            'JSON': UniversalDataType.JSON,
            'UUID': UniversalDataType.UUID,
        }
        
        # Handle Nullable types
        if source_type.startswith('Nullable('):
            base_type = source_type[9:-1]  # Remove 'Nullable(' and ')'
        else:
            base_type = source_type
        
        return type_mapping.get(base_type, UniversalDataType.TEXT)
    
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to ClickHouse type"""
        type_mapping = {
            UniversalDataType.INTEGER: 'Int32',
            UniversalDataType.BIGINT: 'Int64',
            UniversalDataType.SMALLINT: 'Int16',
            UniversalDataType.FLOAT: 'Float32',
            UniversalDataType.DOUBLE: 'Float64',
            UniversalDataType.DECIMAL: 'Decimal(18,2)',
            UniversalDataType.VARCHAR: 'String',
            UniversalDataType.TEXT: 'String',
            UniversalDataType.CHAR: 'FixedString(1)',
            UniversalDataType.DATE: 'Date',
            UniversalDataType.TIME: 'DateTime',
            UniversalDataType.TIMESTAMP: 'DateTime',
            UniversalDataType.DATETIME: 'DateTime',
            UniversalDataType.BOOLEAN: 'UInt8',
            UniversalDataType.BLOB: 'String',
            UniversalDataType.JSON: 'String',
            UniversalDataType.UUID: 'UUID',
        }
        return type_mapping.get(universal_type, 'String')
