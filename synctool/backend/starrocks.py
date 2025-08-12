import aiohttp
import asyncio
import json
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

from synctool.core.models import StrategyConfig, BackendConfig
from synctool.core.enums import HashAlgo
from synctool.core.query_models import Query, Field, Filter, BlockHashMeta, BlockNameMeta, RowHashMeta
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import Partition
from ..core.column_mapper import ColumnSchema
from .base_backend import UniversalSchema, UniversalColumn, UniversalDataType

logger = logging.getLogger(__name__)


class StarRocksBackend(SqlBackend):
    """
    Async StarRocks backend using Stream Load (HTTP).
    - Assumes StarRocks FE is available (stream load endpoint).
    - For Primary Key tables, Stream Load performs upserts (no delete needed).
    - Falls back to simple INSERT ... VALUES if stream load fails.
    """

    def __init__(
        self,
        config: BackendConfig,
        column_schema: Optional[ColumnSchema] = None,
        fe_host: str = "http://localhost:8030",
        user: str = "root",
        password: str = "",
        database: Optional[str] = None,
        table: Optional[str] = None,
        batch_size: int = 5000,
        session_kwargs: Optional[dict] = None,
    ):
        super().__init__(config, column_schema)
        self.fe_host = fe_host.rstrip("/")
        self.user = user
        self.password = password
        self.database = database or getattr(config, "database", None)
        self.table = table or getattr(self, "table", None)
        self.batch_size = batch_size
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_kwargs = session_kwargs or {}
        self.logger = logger

    # ---------------- connection ----------------
    async def connect(self) -> None:
        if self._session is None:
            auth = aiohttp.BasicAuth(self.user, self.password) if (self.user or self.password) else None
            self._session = aiohttp.ClientSession(auth=auth, **self._session_kwargs)

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _full_table(self) -> str:
        if self.database:
            return f"{self.database}.{self.table}"
        return self.table

    # ---------------- low-level helpers ----------------
    def _stream_load_url(self) -> str:
        # Stream load endpoint: /api/{db}/{tbl}/_stream_load
        db = self.database or ""
        return f"{self.fe_host}/api/{db}/{self.table}/_stream_load"

    def _query_url(self) -> str:
        # Query endpoint for executing SQL statements (POST /api/{db}/_sql)
        db = self.database or ""
        return f"{self.fe_host}/api/{db}"

    async def execute_query(self, query: str, action: Optional[str] = "select") -> Any:
        """
        Execute a SQL query via FE query API.
        - For SELECT: returns list[dict]
        - For non-select: returns raw result (dict)
        """
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        # StarRocks supports /api/{db}/_sql POST with JSON body {"sql": "..."}
        url = self._query_url() + "/_sql"
        body = {"sql": query}
        headers = {"Content-Type": "application/json"}
        async with self._session.post(url, json=body, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"StarRocks query failed {resp.status}: {text}\nSQL: {query}")
            try:
                payload = json.loads(text)
            except Exception:
                return text

            # For selects, StarRocks returns {"columns": [...], "rows": [[...],...], ...}
            if action == "select":
                # Normalize to list[dict]
                cols = payload.get("columns")
                rows = payload.get("rows")
                if cols and rows:
                    col_names = [c["name"] for c in cols]
                    return [dict(zip(col_names, r)) for r in rows]
                # fallback: sometimes response is already JSON array
                return payload
            else:
                return payload

    # ---------------- small helpers ----------------
    def _format_value(self, v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)

    def _quote_ident(self, ident: str) -> str:
        # StarRocks supports backticks
        return f"`{ident}`"

    # ---------------- CRUD and metadata ----------------
    async def has_data(self) -> bool:
        q = f"SELECT 1 FROM {self._full_table()} LIMIT 1"
        rows = await self.execute_query(q, action="select")
        return bool(rows)

    async def get_last_sync_point(self) -> Any:
        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_key.expr
        q = f"SELECT MAX({column}) AS last_sync_point FROM {self._full_table()}"
        rows = await self.execute_query(q, action="select")
        return rows[0].get("last_sync_point") if rows else None

    async def get_max_sync_point(self) -> Any:
        # same as last_sync_point for StarRocks
        return await self.get_last_sync_point()

    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        if not self.column_schema or not self.column_schema.partition_key:
            raise ValueError("No partition key configured in column schema")
        col = self.column_schema.partition_key.expr
        q = f"SELECT MIN({col}) AS min_val, MAX({col}) AS max_val FROM {self._full_table()}"
        rows = await self.execute_query(q, action="select")
        if not rows:
            return None, None
        r = rows[0]
        return r.get("min_val"), r.get("max_val")

    # ---------------- fetch / partition helpers (use SqlBuilder where possible) ----------------
    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        query = self._build_partition_data_query(partition, order_by=None if (page_size is None and offset is None) else [f"{c.expr} {d}" for c,d in self.column_schema.order_keys] if self.column_schema and self.column_schema.order_keys else None, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, _ = self._build_sql(query)
        return await self.execute_query(sql, action="select")

    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo: HashAlgo = HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        partition_column = self.column_schema.delta_key.name
        query = self._build_partition_data_query(partition, partition_column=partition_column, order_by=None if (page_size is None and offset is None) else [f"{c.expr} {d}" for c,d in self.column_schema.order_keys] if self.column_schema and self.column_schema.order_keys else None, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
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
        # StarRocks supports DELETE ... WHERE for primary key tables; it's synchronous via txn
        if not self.column_schema or not self.column_schema.partition_key:
            raise ValueError("No partition key configured")
        col = self.column_schema.partition_key.expr
        where = f"{col} >= {self._format_value(partition.start)} AND {col} < {self._format_value(partition.end)}"
        sql = f"DELETE FROM {self._full_table()} WHERE {where}"
        return await self.execute_query(sql, action="delete")

    async def delete_rows(self, rows: List[Dict], partition: Partition, strategy_config: StrategyConfig) -> Any:
        if not rows:
            return 0
        unique_keys = [col.expr for col in self.column_schema.unique_keys]
        clauses = []
        for r in rows:
            conds = []
            for k in unique_keys:
                conds.append(f"{k} = {self._format_value(r.get(k))}")
            clauses.append("(" + " AND ".join(conds) + ")")
        where = " OR ".join(clauses)
        sql = f"DELETE FROM {self._full_table()} WHERE {where}"
        return await self.execute_query(sql, action="delete")

    # ---------------- insert / upsert ----------------
    async def insert_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:
        """
        Insert (or upsert) data into StarRocks.
        - Uses Stream Load (JSONEachRow) for efficient bulk upserts into Primary Key tables.
        - If Stream Load fails, falls back to INSERT INTO ... VALUES via query endpoint.
        """
        if not data:
            return 0
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")

        columns = list(data[0].keys())
        total = 0

        # Helper: stream load a batch (JSONEachRow)
        async def _stream_load_batch(batch_rows: List[Dict]) -> None:
            url = self._stream_load_url()
            # label must be unique per request; use timestamp
            label = f"stream_{int(asyncio.get_event_loop().time()*1000)}"
            headers = {
                "label": label,
                "format": "json",         # StarRocks expects format param in header, but stream load supports JSONEachRow payload
                "columns": ",".join(columns)
            }
            # StarRocks expects body as jsonlines or CSV depending on format. We'll send JSONEachRow.
            payload = "\n".join(json.dumps({col: row.get(col) for col in columns}, default=str) for row in batch_rows).encode("utf-8")
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
                vals = [self._format_value(r.get(c)) for c in columns]
                values_parts.append("(" + ", ".join(vals) + ")")
            values_sql = ", ".join(values_parts)
            cols_sql = ", ".join([self._quote_ident(c) for c in columns])
            sql = f"INSERT INTO {self._full_table()} ({cols_sql}) VALUES {values_sql}"
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
                            self.logger.error(f"Failed to insert row {row}: {row_err}")

        return total

    async def insert_partition_data(self, data: List[Dict], partition: Optional[Partition] = None, batch_size: int = 5000, upsert: bool = True) -> int:
        return await self.insert_data(data, partition, batch_size, upsert=upsert)

    async def insert_delta_data(self, data: List[Dict], partition: Optional[Partition] = None, batch_size: int = 5000, upsert: bool = True) -> int:
        return await self.insert_data(data, partition, batch_size, upsert=upsert)

    # ---------------- query rewriting / hash expressions ----------------
    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level
        intervals = metadata.intervals
        partition_column = metadata.partition_column
        partition_column_type = metadata.partition_column_type

        if partition_column_type == "int":
            parts = []
            for idx in range(level + 1):
                if idx == 0:
                    parts.append(f"CAST({partition_column} / {intervals[idx]} AS VARCHAR)")
                else:
                    prev = intervals[idx - 1]
                    parts.append(f"CAST(({partition_column} % {prev}) / {intervals[idx]} AS VARCHAR)")
            return " || '-' || ".join(parts)
        elif partition_column_type == "datetime":
            parts = []
            for idx in range(level + 1):
                if idx == 0:
                    parts.append(f"CAST(TO_UNIXTIME({partition_column}) / {intervals[idx]} AS VARCHAR)")
                else:
                    prev = intervals[idx - 1]
                    parts.append(f"CAST((TO_UNIXTIME({partition_column}) % {prev}) / {intervals[idx]} AS VARCHAR)")
            return " || '-' || ".join(parts)
        else:
            raise ValueError(f"Unsupported partition type: {partition_column_type}")

    def _build_rowhash_expr(self, field: Field) -> str:
        metadata: RowHashMeta = field.metadata
        if metadata.hash_column:
            return metadata.hash_column
        concat = " || ".join([f"CAST({x.expr} AS VARCHAR)" for x in metadata.fields])
        if metadata.strategy == HashAlgo.HASH_MD5_HASH:
            return f"MD5({concat})"
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            return f"CONV(SUBSTR(MD5({concat}), 1, 16), 16, 10)"
        return ""

    def _build_blockhash_expr(self, field: Field) -> str:
        metadata: BlockHashMeta = field.metadata
        inner = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.HASH_MD5_HASH:
            # approximate aggregation via GROUP_CONCAT equivalent if available; StarRocks supports GROUP_CONCAT
            return f"MD5(GROUP_CONCAT({inner} ORDER BY {metadata.order_column} SEPARATOR ''))"
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            return f"SUM({inner})"
        return ""

    def _rewrite_query(self, query: Query) -> Query:
        rewritten = []
        for f in query.select:
            if f.type == "blockhash":
                expr = self._build_blockhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type="column"))
            elif f.type == "blockname":
                expr = self._build_group_name_expr(f)
                # convert ' || '-' || ' pattern into concat() for safety
                if " || '-' || " in expr:
                    parts = expr.split(" || '-' || ")
                    concat_expr = "CONCAT(" + ", '-'," .join(parts) + ")"
                    rewritten.append(Field(expr=concat_expr, alias=f.alias, type="column"))
                else:
                    rewritten.append(Field(expr=expr, alias=f.alias, type="column"))
            elif f.type == "rowhash":
                expr = self._build_rowhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type="column"))
            else:
                rewritten.append(f)
        query.select = rewritten
        return query

    def _build_sql(self, query: Query) -> Tuple[str, list]:
        q = self._rewrite_query(query)
        try:
            return SqlBuilder.build(q, dialect="starrocks")
        except Exception:
            # Basic fallback SQL generation
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

    def _build_partition_hash_query(self, partition: Partition, hash_algo: HashAlgo) -> Query:
        start = partition.start
        end = partition.end
        partition_column_type = partition.column_type

        hash_column = self.column_schema.hash_key.expr if self.column_schema.hash_key else None
        partition_column = self.column_schema.partition_key.expr
        order_keys = self.column_schema.order_keys
        order_column = ",".join([f"{c.expr} {d}" for c, d in order_keys]) if order_keys else None

        partition_hash_field = Field(
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
        partition_id_field = Field(expr="partition_id", alias="partition_id", metadata=BlockNameMeta(
            level=partition.level + 1,
            intervals=partition.intervals,
            partition_column_type=partition_column_type,
            strategy=hash_algo,
            partition_column=partition_column
        ), type="blockname")

        select = [
            Field(expr="COUNT(1)", alias="num_rows", type="column"),
            partition_hash_field,
            partition_id_field
        ]
        grp_field = Field(expr="partition_id", type="column")

        filters = []
        if partition_column_type in ("datetime", "int", "str"):
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
        """Extract StarRocks table schema"""
        # StarRocks DESCRIBE query
        describe_query = f"DESCRIBE {self._full_table_name(table_name)}"
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
                default=col.get('Default'),
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
            columns.append(f"{col.name} {col_type}")
        
        # StarRocks Primary Key table
        if schema.primary_keys:
            pk_columns = ', '.join(schema.primary_keys)
            return f"""CREATE TABLE {table_name} (
  {', '.join(columns)},
  PRIMARY KEY ({pk_columns})
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
            'BOOLEAN': UniversalDataType.BOOLEAN,
            'JSON': UniversalDataType.JSON,
            'UUID': UniversalDataType.UUID,
        }
        
        normalized_type = source_type.upper().strip()
        return type_mapping.get(normalized_type, UniversalDataType.TEXT)
    
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
