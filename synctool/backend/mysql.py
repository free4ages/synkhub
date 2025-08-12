import math
import io
from typing import Any, List, Optional, Dict, Tuple
from synctool.core.models import StrategyConfig, BackendConfig
from synctool.core.enums import HashAlgo
from synctool.core.query_models import BlockHashMeta, BlockNameMeta, Field, Filter, Query, RowHashMeta
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import Partition
from ..core.column_mapper import ColumnSchema
from .base_backend import UniversalSchema, UniversalColumn, UniversalDataType

MAX_MYSQL_PARAMS = 65535  # MySQL limit for placeholders


class MySQLBackend(SqlBackend):
    """MySQL implementation of Backend"""

    def __init__(self, config: BackendConfig, column_schema: Optional[ColumnSchema] = None):
        super().__init__(config, column_schema)
        self._pool = None

    def _get_default_schema(self) -> str:
        return self.connection_config.database or "default"

    async def connect(self):
        try:
            import aiomysql
            self._pool = await aiomysql.create_pool(
                host=self.connection_config.host,
                port=self.connection_config.port or 3306,
                user=self.connection_config.user,
                password=self.connection_config.password,
                db=self.connection_config.database,
                autocommit=True
            )
        except ImportError:
            raise ImportError("aiomysql is required for MySQL provider")

    async def disconnect(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    async def execute_query(self, query: str, params: Optional[list] = None, action: Optional[str] = 'select') -> List[Dict]:
        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params or [])
                if action == 'select':
                    return await cur.fetchall()
                elif action == 'delete':
                    await conn.commit()
                    return cur.rowcount
                else:
                    raise ValueError(f"Invalid action: {action}")

    async def has_data(self) -> bool:
        table = self._get_full_table_name(self.table)
        query = f"SELECT COUNT(*) as count FROM {table} LIMIT 1"
        result = await self.execute_query(query)
        return result[0]['count'] > 0

    async def get_last_sync_point(self) -> Any:
        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_key.expr
        table = self._get_full_table_name(self.table)
        query = f"SELECT MAX({column}) as last_sync_point FROM {table}"
        result = await self.execute_query(query)
        return result[0]['last_sync_point']

    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        if not self.column_schema or not self.column_schema.partition_key:
            raise ValueError("No partition key configured in column schema")
        partition_column = self.column_schema.partition_key.expr
        table = self._get_full_table_name(self.table)
        query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table}"
        result = await self.execute_query(query)
        return result[0]['min_val'], result[0]['max_val']

    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level
        intervals = metadata.intervals
        col = metadata.partition_column
        col_type = metadata.partition_column_type

        if col_type == "int":
            segments = []
            for idx in range(level + 1):
                if idx == 0:
                    expr = f"FLOOR({col} / {intervals[idx]})"
                else:
                    prev = intervals[idx - 1]
                    expr = f"FLOOR(MOD({col}, {prev}) / {intervals[idx]})"
                segments.append(f"CAST({expr} AS CHAR)")
            return " , '-' , ".join(segments)

        elif col_type == "datetime":
            segments = []
            for idx in range(level + 1):
                if idx == 0:
                    expr = f"FLOOR(UNIX_TIMESTAMP({col}) / {intervals[idx]})"
                else:
                    prev = intervals[idx - 1]
                    expr = f"FLOOR(MOD(UNIX_TIMESTAMP({col}), {prev}) / {intervals[idx]})"
                segments.append(f"CAST({expr} AS CHAR)")
            return " , '-' , ".join(segments)
        else:
            raise ValueError(f"Unsupported partition type: {col_type}")

    def _build_rowhash_expr(self, field: Field) -> str:
        metadata: RowHashMeta = field.metadata
        if metadata.hash_column:
            return metadata.hash_column
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            concat = ", ".join([f"{x.expr}" for x in metadata.fields])
            return f"MD5(CONCAT({concat}))"
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            concat = ", ".join([f"{x.expr}" for x in metadata.fields])
            return f"CONV(LEFT(MD5(CONCAT({concat})), 8), 16, 10)"
        return ""

    def _build_blockhash_expr(self, field: Field):
        metadata: BlockHashMeta = field.metadata
        inner_expr = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.MD5_SUM_HASH:
            return f"SUM(CAST({inner_expr} AS UNSIGNED))"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            return f"MD5(GROUP_CONCAT({inner_expr} ORDER BY {metadata.order_column} SEPARATOR ''))"

    def _rewrite_query(self, query: Query) -> Query:
        rewritten = []
        for f in query.select:
            if f.type == 'blockhash':
                expr = self._build_blockhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            elif f.type == "blockname":
                expr = self._build_group_name_expr(f)
                rewritten.append(Field(expr=f"CONCAT({expr})", alias=f.alias, type='column'))
            elif f.type == "rowhash":
                expr = self._build_rowhash_expr(f)
                rewritten.append(Field(expr=expr, alias=f.alias, type='column'))
            else:
                rewritten.append(f)
        query.select = rewritten
        return query

    def _build_sql(self, query: Query) -> Tuple[str, list]:
        q = self._rewrite_query(query)
        return SqlBuilder.build(q, dialect='mysql')

    async def insert_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:
        data = self._process_pre_insert_data(data)
        if not data:
            return 0

        column_keys = [col.expr for col in self.column_schema.columns_to_insert()]
        unique_keys = [col.expr for col in self.column_schema.unique_keys]
        if not unique_keys:
            raise ValueError("Unique keys must be defined")

        non_conflict_cols = [col for col in column_keys if col not in unique_keys]
        table_name = self._get_full_table_name(self.table)

        insert_cols = ', '.join(f"`{col}`" for col in column_keys)
        if upsert:
            update_clause = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in non_conflict_cols)
            base_insert_sql = f"INSERT INTO {table_name} ({insert_cols}) VALUES {{}} ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            base_insert_sql = f"INSERT INTO {table_name} ({insert_cols}) VALUES {{}}"

        total_inserted = 0
        max_safe_rows = MAX_MYSQL_PARAMS // len(column_keys)
        safe_batch_size = min(batch_size, max_safe_rows)

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(data), safe_batch_size):
                    batch = data[i:i + safe_batch_size]
                    placeholders = ', '.join(
                        "(" + ", ".join(["%s"] * len(column_keys)) + ")"
                        for _ in batch
                    )
                    flat_params = [row[col] for row in batch for col in column_keys]
                    sql = base_insert_sql.format(placeholders)
                    await cur.execute(sql, flat_params)
                    total_inserted += len(batch)
        return total_inserted

    # Schema extraction and DDL generation methods
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract MySQL table schema"""
        # MySQL DESCRIBE query
        describe_query = f"DESCRIBE {self._get_full_table_name(table_name)}"
        columns_raw = await self.execute_query(describe_query)
        
        columns = []
        primary_keys = []
        
        for col in columns_raw:
            col_name = col['Field']
            col_type = col['Type']
            col_key = col.get('Key')
            col_extra = col.get('Extra', '')
            
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
                unique=col_key == 'UNI',
                auto_increment='auto_increment' in col_extra.lower(),
                default=col.get('Default'),
                extra=col_extra
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            database_name=self.connection_config.database,
            columns=columns,
            primary_keys=primary_keys
        )
    
    def generate_create_table_ddl(self, schema: UniversalSchema, target_table_name: Optional[str] = None) -> str:
        """Generate MySQL CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        
        columns = []
        for col in schema.columns:
            col_def = f"{col.name} {self.map_universal_type_to_target(col.data_type)}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.auto_increment:
                col_def += " AUTO_INCREMENT"
            
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            columns.append(col_def)
        
        # Add primary key constraint
        if schema.primary_keys:
            pk_columns = ', '.join(schema.primary_keys)
            columns.append(f"PRIMARY KEY ({pk_columns})")
        
        return f"""CREATE TABLE {table_name} (
  {', '.join(columns)}
);"""
    
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map MySQL type to universal type"""
        type_mapping = {
            'TINYINT': UniversalDataType.SMALLINT,
            'SMALLINT': UniversalDataType.SMALLINT,
            'INT': UniversalDataType.INTEGER,
            'BIGINT': UniversalDataType.BIGINT,
            'FLOAT': UniversalDataType.FLOAT,
            'DOUBLE': UniversalDataType.DOUBLE,
            'DECIMAL': UniversalDataType.DECIMAL,
            'VARCHAR': UniversalDataType.VARCHAR,
            'TEXT': UniversalDataType.TEXT,
            'CHAR': UniversalDataType.CHAR,
            'DATE': UniversalDataType.DATE,
            'TIME': UniversalDataType.TIME,
            'DATETIME': UniversalDataType.DATETIME,
            'TIMESTAMP': UniversalDataType.TIMESTAMP,
            'BOOLEAN': UniversalDataType.BOOLEAN,
            'BOOL': UniversalDataType.BOOLEAN,
            'BLOB': UniversalDataType.BLOB,
            'JSON': UniversalDataType.JSON,
        }
        
        normalized_type = source_type.upper().strip()
        
        # Special handling for CHAR(36) which is commonly used for UUIDs in MySQL
        if normalized_type == 'CHAR(36)':
            return UniversalDataType.UUID
        
        # Remove length specification for other types
        base_type = normalized_type.split('(')[0] if '(' in normalized_type else normalized_type
        return type_mapping.get(base_type, UniversalDataType.TEXT)
    
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to MySQL type"""
        type_mapping = {
            UniversalDataType.INTEGER: 'INT',
            UniversalDataType.BIGINT: 'BIGINT',
            UniversalDataType.SMALLINT: 'SMALLINT',
            UniversalDataType.FLOAT: 'FLOAT',
            UniversalDataType.DOUBLE: 'DOUBLE',
            UniversalDataType.DECIMAL: 'DECIMAL(18,2)',
            UniversalDataType.VARCHAR: 'VARCHAR(255)',
            UniversalDataType.TEXT: 'TEXT',
            UniversalDataType.CHAR: 'CHAR(1)',
            UniversalDataType.DATE: 'DATE',
            UniversalDataType.TIME: 'TIME',
            UniversalDataType.TIMESTAMP: 'TIMESTAMP',
            UniversalDataType.DATETIME: 'DATETIME',
            UniversalDataType.BOOLEAN: 'BOOLEAN',
            UniversalDataType.BLOB: 'BLOB',
            UniversalDataType.JSON: 'JSON',
            UniversalDataType.UUID: 'CHAR(36)',
        }
        return type_mapping.get(universal_type, 'TEXT')
