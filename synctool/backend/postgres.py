import math
import io
from synctool.core.models import StrategyConfig
from synctool.core.models import BackendConfig
from typing import Any, List, Optional, Dict, Tuple
from datetime import timedelta

from synctool.core.enums import HashAlgo
from synctool.core.query_models import BlockHashMeta, BlockNameMeta, Field, Filter, Join, Query, RowHashMeta, Table
from synctool.utils.sql_builder import SqlBuilder
from .base_backend import SqlBackend
from ..core.models import Partition, BackendConfig
from ..core.column_mapper import ColumnSchema
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType

MAX_PG_PARAMS = 31000

class PostgresBackend(SqlBackend):
    """PostgreSQL implementation of Backend"""
    
    def __init__(self, config: BackendConfig, column_schema: Optional[ColumnSchema] = None):
        super().__init__(config, column_schema)
        self._connection = None
    
    def _get_default_schema(self) -> str:
        return 'public'
    
    async def connect(self):
        """Connect to PostgreSQL database"""
        try:
            import asyncpg
            self._connection = await asyncpg.create_pool(
                host=self.connection_config.host,
                port=self.connection_config.port,
                user=self.connection_config.user,
                password=self.connection_config.password,
                database=self.connection_config.database or self.connection_config.dbname
            )
        except ImportError:
            raise ImportError("asyncpg is required for PostgreSQL provider")
    
    async def disconnect(self):
        """Disconnect from PostgreSQL database"""
        if self._connection:
            await self._connection.close()
    
    async def execute_query(self, query: str, params: Optional[list] = None, action: Optional[str] = 'select') -> List[Dict]:
        """Execute query and return DataFrame"""
        # print(query)
        if action == 'select':
            rows = await self._connection.fetch(query, *(params if params else []))
            return [dict(row) for row in rows]
        elif action == 'delete':
            return await self._connection.execute(query, *(params if params else []))
        else:
            raise ValueError(f"Invalid action: {action}")

    
    async def has_data(self) -> bool:
        """Check if the provider has any data"""
        table = self._get_full_table_name(self.table)
        query = f"SELECT COUNT(*) as count FROM {table} LIMIT 1"
        result = await self.execute_query(query)
        return result[0]['count'] > 0
    
    async def get_last_sync_point(self) -> Any:
        """Get the last sync point for a specific column type"""
        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_key.expr
        table = self._get_full_table_name(self.table)
        query = f"SELECT MAX({column}) as last_sync_point FROM {table}"
        result = await self.execute_query(query)
        return result[0]['last_sync_point']
    
    async def get_max_sync_point(self) -> Any:
        """Get the maximum sync point for a specific column type"""
        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        column = self.column_schema.delta_key.expr
        column_type = self.column_schema.delta_key.dtype
        table = self._get_full_table_name(self.table)
        query = f"SELECT MAX({column}) as max_sync_point FROM {table}"
        result = await self.execute_query(query)
        max_sync_point = result[0]['max_sync_point']
        return max_sync_point
    
    async def get_partition_bounds(self) -> Tuple[Any, Any]:
        """Get min and max values for partition column"""
        if not self.column_schema or not self.column_schema.partition_key:
            raise ValueError("No partition key configured in column schema")
        partition_column = self.column_schema.partition_key.expr
        table = self._get_full_table_name(self.table)
        query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table}"
        result = await self.execute_query(query)
        min_val, max_val = result[0]['min_val'], result[0]['max_val']
        return min_val, max_val

    async def fetch_partition_data(self, partition: Partition, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""
        if page_size is not None or offset is not None:
            order_by = [f"{c.expr} {d}" for c,d in self.column_schema.order_keys] if self.column_schema.order_keys else [f"{self.column_schema.partition_key.expr} asc"]
        else:
            order_by = None
        query = self._build_partition_data_query(partition, order_by=order_by, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        # print(sql, params)
        return await self.execute_query(sql, params)
    
    async def fetch_delta_data(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH, page_size: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Fetch data based on partition bounds with optional pagination"""

        if not self.column_schema or not self.column_schema.delta_key:
            raise ValueError("No delta key configured in column schema")
        if page_size is not None or offset is not None:
            order_by = [f"{c.expr} {d}" for c,d in self.column_schema.order_keys] if self.column_schema.order_keys else [f"{self.column_schema.partition_key.expr} asc"]
        else:
            order_by = None
        partition_column = self.column_schema.delta_key.name
        query = self._build_partition_data_query(partition, partition_column=partition_column, order_by=order_by, with_hash=with_hash, hash_algo=hash_algo, page_size=page_size, offset=offset)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)
    
    async def fetch_partition_row_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        return await self.fetch_partition_data(partition, with_hash=True, hash_algo=hash_algo)
    
    async def fetch_child_partition_hashes(self, partition: Optional[Partition] = None, with_hash=False, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch hashes based on partition bounds"""
        # query = self._build_partition_data_query(partition, partition_column=self.column_mapping.delta_key, with_hash=with_hash, hash_algo=hash_algo)
        query = self._build_partition_hash_query(partition, hash_algo)
        query = self._rewrite_query(query)
        sql, params = self._build_sql(query)
        # print(sql, params)
        return await self.execute_query(sql, params)
    
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
        unique_keys = self.column_schema.unique_keys
        unique_keys_values = [tuple(row[key] for key in unique_keys) for row in rows]
        filters += [
            Filter(column=self.column_schema.column(key).expr, operator='in', value=unique_keys_values)
            for key in unique_keys
        ]
        query = Query(action='delete', table=self._build_table_query(), filters=filters)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)
        
    async def insert_partition_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:      
        return await self.insert_data(data, partition, batch_size, upsert=upsert)
    
    async def fetch_partition_hashes(self, partition: Partition, hash_algo=HashAlgo.HASH_MD5_HASH) -> List[Dict]:
        """Fetch partition row hashes from destination along with state columns"""
        return await self.data_backend.fetch_partition_row_hashes(partition, hash_algo)
    
    async def insert_delta_data(
        self,
        data: List[Dict],
        partition: Optional[Partition] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:
        return await self.insert_data(data, partition, batch_size, upsert=upsert)
    
    async def delete_partition_data(self, partition: Optional[Partition] = None) -> int:
        """Delete partition data from destination"""
        filters = self._build_filter_query()
        filters += [
            Filter(column=self.column_mapping.partition_key, operator='>=', value=partition.start), 
            Filter(column=self.column_mapping.partition_key, operator='<', value=partition.end)
        ]
        query = Query(action='delete', table=self._build_table_query(), filters=filters)
        sql, params = self._build_sql(query)
        return await self.execute_query(sql, params)    

    def _build_group_name_expr(self, field: Field) -> str:
        metadata: BlockNameMeta = field.metadata
        level = metadata.level
        intervals = metadata.intervals
        partition_column = metadata.partition_column
        partition_column_type = metadata.partition_column_type

        if partition_column_type == UniversalDataType.INTEGER:
            # For integer partition columns, we divide by the interval
            segments = []
            for idx in range(level+1):
                fct = intervals[idx]
                if idx == 0:
                    expr = f"FLOOR({partition_column} / {intervals[idx]})"
                else:
                    prev = intervals[idx-1]
                    expr = f"FLOOR(mod({partition_column}, {prev}) / {intervals[idx]})"
                segments.append(f"{expr}::text")
            return " || '-' || ".join(segments)

        elif partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            segments = []
            for idx in range(level+1):
                fct = intervals[idx]
                if idx == 0:
                    expr = f"FLOOR(EXTRACT(EPOCH FROM {partition_column}) / {fct})"
                else:
                    prev = intervals[idx-1]
                    expr = f"FLOOR(mod(EXTRACT(EPOCH FROM {partition_column}) ,{prev}) / {fct})"
                    
                segments.append(f"{expr}::text")
            return " || '-' || ".join(segments)
        else:
            raise ValueError(f"Unsupported partition type: {partition_column}")

    def _build_rowhash_expr(self, field: Field) -> str:
        """Build row hash expression for PostgreSQL"""
        metadata: RowHashMeta = field.metadata
        expr=""
        if metadata.hash_column:
            expr = metadata.hash_column
        elif metadata.strategy == HashAlgo.MD5_SUM_HASH:
            concat = ",".join([f"{x.expr}" for x in metadata.fields])
            expr = f"(('x'||substr(md5(CONCAT({concat})),1,8))::bit(32)::int)"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            concat = ",".join([f"{x.expr}" for x in metadata.fields])
            expr = f"md5(CONCAT({concat}))"
        return expr
    
    def _build_blockhash_expr(self, field: Field):
        metadata: BlockHashMeta = field.metadata
        inner_expr = self._build_rowhash_expr(field)
        if metadata.strategy == HashAlgo.MD5_SUM_HASH:
            expr = f"sum({inner_expr}::numeric)"
        elif metadata.strategy == HashAlgo.HASH_MD5_HASH:
            expr = f"md5(string_agg({inner_expr},'' order by {metadata.order_column}))"
        return expr


    def _rewrite_query(self, query: Query) -> Query:
        """Rewrite query to handle PostgreSQL-specific expressions"""
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
        """Build SQL query for PostgreSQL"""
        q = self._rewrite_query(query)
        return SqlBuilder.build(q, dialect='asyncpg')

    def _build_partition_hash_query(self, partition: Partition, hash_algo: HashAlgo) -> Query:
        """Build partition hash query for PostgreSQL"""
        start = partition.start
        end = partition.end
        partition_column_type = partition.column_type

        hash_column = self.column_schema.hash_key.expr if self.column_schema.hash_key else None
        partition_column = self.column_schema.partition_key.expr
        order_keys = self.column_schema.order_keys
        order_column = ",".join([f"{c.expr} {d}" for c,d in order_keys]) if order_keys else None
        
        partition_hash_field = Field(
            expr=f"{partition_column}",
            alias="partition_hash",
            metadata=BlockHashMeta(
                order_column=order_column,
                partition_column = partition_column,
                hash_column = hash_column,
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
            partition_column=partition_column
        ), type="blockname")

        select = [
            Field(expr='COUNT(1)', alias='num_rows', type='column'),
            partition_hash_field,
            partition_id_field
        ]
        grp_field = Field(expr="partition_id", type="column")

        filters = []
        if partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            filters += [
                Filter(column=partition_column, operator='>=', value=start), 
                Filter(column=partition_column, operator='<', value=end)
            ]
        elif partition_column_type == UniversalDataType.INTEGER:
            filters += [
                Filter(column=partition_column, operator='>=', value=start), 
                Filter(column=partition_column, operator='<', value=end)
            ]
        elif partition_column_type == UniversalDataType.VARCHAR:
            filters += [
                Filter(column=partition_column, operator='>=', value=start), 
                Filter(column=partition_column, operator='<', value=end)
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
    

    


    # async def insert_data(
    #     self,
    #     data: List[Dict],
    #     partition: Optional[Partition] = None,
    #     batch_size: int = 5000,
    #     upsert: bool = True
    # ) -> int:
    #     """Efficiently upserts data into PostgreSQL using asyncpg."""
    #     data = self._process_pre_insert_data(data)
    #     if not data:
    #         return 0

    #     column_keys = [col.expr for col in self.column_schema.columns_to_insert()]

    #     total_upserted = 0
    #     unique_keys = [col.expr for col in self.column_schema.unique_keys]
    #     non_conflict_cols = [col for col in column_keys if col not in unique_keys]

    #     if not unique_keys:
    #         raise ValueError("Unique keys must be defined in column_mapping")

    #     table_name = self._get_full_table_name(self.table)
    #     insert_cols = ', '.join(f'"{col}"' for col in column_keys)
    #     conflict_target = ', '.join(f'"{col}"' for col in unique_keys)
    #     update_clause = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in non_conflict_cols)

    #     if upsert:
    #         query = f"""
    #             INSERT INTO {table_name} ({insert_cols})
    #             VALUES {{}}
    #             ON CONFLICT ({conflict_target}) DO UPDATE
    #             SET {update_clause}
    #         """
    #     else:
    #         query = f"""
    #             INSERT INTO {table_name} ({insert_cols})
    #             VALUES {{}}
    #         """

    #     async with self._connection.acquire() as conn:
    #         for i in range(0, len(data), batch_size):
    #             batch = data[i:i+batch_size]
    #             values = [tuple(row[col] for col in column_keys) for row in batch]

    #             # Generate parameterized placeholder string
    #             placeholders = ', '.join(
    #                 f"({', '.join(f'${j + i * len(column_keys) + 1}' for j in range(len(column_keys)))})"
    #                 for i in range(len(values))
    #             )

    #             flat_params = [val for row in values for val in row]

    #             final_query = query.format(placeholders)
    #             await conn.execute(final_query, *flat_params)

    #             total_upserted += len(values)

    #     return total_upserted


    async def insert_data(
        self,
        data: List[Dict],
        partition: Optional["Partition"] = None,
        batch_size: int = 5000,
        upsert: bool = True
    ) -> int:
        """Efficiently upserts data into PostgreSQL using asyncpg with fallback error handling."""
        data = self._process_pre_insert_data(data)
        if not data:
            return 0

        column_keys = [col.expr for col in self.column_schema.columns_to_insert()]
        unique_keys = [col.expr for col in self.column_schema.unique_keys]
        if not unique_keys:
            raise ValueError("Unique keys must be defined in column_mapping")

        non_conflict_cols = [col for col in column_keys if col not in unique_keys]
        table_name = self._get_full_table_name(self.table)

        insert_cols = ', '.join(f'"{col}"' for col in column_keys)
        conflict_target = ', '.join(f'"{col}"' for col in unique_keys)
        update_clause = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in non_conflict_cols)

        # Base query templates
        if upsert:
            base_insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols})
                VALUES {{}}
                ON CONFLICT ({conflict_target}) DO UPDATE
                SET {update_clause}
            """
        else:
            base_insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols})
                VALUES {{}}
            """

        # Calculate safe batch size for direct parameterized insert
        max_safe_rows = MAX_PG_PARAMS // len(column_keys)
        safe_batch_size = min(batch_size, max_safe_rows)

        total_upserted = 0
        async with self._connection.acquire() as conn:
            for i in range(0, len(data), safe_batch_size):
                batch = data[i:i + safe_batch_size]

                # If batch fits in safe parameter size → normal multi-row insert
                if len(batch) * len(column_keys) <= MAX_PG_PARAMS:
                    try:
                        values = [tuple(row[col] for col in column_keys) for row in batch]
                        placeholders = ', '.join(
                            f"({', '.join(f'${j + r * len(column_keys) + 1}' for j in range(len(column_keys)))})"
                            for r in range(len(values))
                        )
                        flat_params = [val for row in values for val in row]
                        final_query = base_insert_sql.format(placeholders)
                        # print(final_query, flat_params)
                        await conn.execute(final_query, *flat_params)
                        total_upserted += len(batch)
                        continue
                    except Exception as e:
                        self.logger.error(f"Batch insert failed, falling back to per-row insert: {e}")

                # If batch too big or failed → use COPY into temp table
                temp_table = f"temp_upsert_{int(math.floor(i))}"
                await conn.execute(f"""
                    CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP
                """)

                # COPY data into temp table
                try:
                    buf = io.StringIO()
                    for row in batch:
                        buf.write('\t'.join(
                            '' if row[col] is None else str(row[col])
                            for col in column_keys
                        ) + '\n')
                    buf.seek(0)
                    await conn.copy_to_table(
                        temp_table,
                        source=buf,
                        format='csv',
                        delimiter='\t',
                        null='',
                        columns=column_keys
                    )

                    # Merge from temp table into target
                    if upsert:
                        await conn.execute(f"""
                            INSERT INTO {table_name} ({insert_cols})
                            SELECT {insert_cols} FROM {temp_table}
                            ON CONFLICT ({conflict_target}) DO UPDATE
                            SET {update_clause}
                        """)
                    else:
                        await conn.execute(f"""
                            INSERT INTO {table_name} ({insert_cols})
                            SELECT {insert_cols} FROM {temp_table}
                        """)
                    total_upserted += len(batch)

                except Exception as e:
                    self.logger.error(f"Temp table COPY failed, inserting rows individually: {e}")
                    # Per-row insert to skip bad rows
                    for row in batch:
                        try:
                            placeholders = ', '.join(f"${j+1}" for j in range(len(column_keys)))
                            final_query = base_insert_sql.format(f"({placeholders})")
                            await conn.execute(final_query, *(row[col] for col in column_keys))
                            total_upserted += 1
                        except Exception as row_err:
                            self.logger.error(f"Failed to insert row {row}: {row_err}")

        return total_upserted
    
    # Schema extraction and DDL generation methods
    async def extract_table_schema(self, table_name: str) -> UniversalSchema:
        """Extract PostgreSQL table schema"""
        schema_name = self.schema or 'public'
        full_table = f"{schema_name}.{table_name}"
        
        # Get column information
        columns_query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default,
            ordinal_position,
            is_identity
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """
        columns_data = await self.execute_query(columns_query, [schema_name, table_name])
        
        # Get primary key information
        pk_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY' 
            AND tc.table_schema = $1 
            AND tc.table_name = $2
        """
        pk_result = await self.execute_query(pk_query, [schema_name, table_name])
        primary_keys = [row['column_name'] for row in pk_result]
        
        # Get index information
        indexes_query = """
        SELECT 
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname = $1 AND tablename = $2
        """
        indexes = await self.execute_query(indexes_query, [schema_name, table_name])
        
        # Convert to universal format
        columns = []
        for col_data in columns_data:
            universal_type = self.map_source_type_to_universal(col_data['data_type'])
            
            column = UniversalColumn(
                name=col_data['column_name'],
                data_type=universal_type,
                nullable=col_data['is_nullable'] == 'YES',
                primary_key=col_data['column_name'] in primary_keys,
                unique=False,  # Would need additional query for unique constraints
                auto_increment=col_data['is_identity'] == 'YES',
                default_value=col_data['column_default'],
                max_length=col_data['character_maximum_length']
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            schema_name=schema_name,
            database_name=self.connection_config.database,
            columns=columns,
            primary_keys=primary_keys,
            indexes=indexes
        )
    
    def generate_create_table_ddl(self, schema: UniversalSchema, target_table_name: Optional[str] = None) -> str:
        """Generate PostgreSQL CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        if schema.schema_name:
            table_name = f"{schema.schema_name}.{table_name}"
        
        columns = []
        for col in schema.columns:
            col_def = f"{col.name} {self.map_universal_type_to_target(col.data_type)}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
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
        """Map PostgreSQL type to universal type"""
        type_mapping = {
            'integer': UniversalDataType.INTEGER,
            'int': UniversalDataType.INTEGER,
            'bigint': UniversalDataType.BIGINT,
            'smallint': UniversalDataType.SMALLINT,
            'serial': UniversalDataType.INTEGER,  # Auto-incrementing integer
            'bigserial': UniversalDataType.BIGINT,  # Auto-incrementing bigint
            'smallserial': UniversalDataType.SMALLINT,  # Auto-incrementing smallint
            'real': UniversalDataType.FLOAT,
            'double precision': UniversalDataType.DOUBLE,
            'numeric': UniversalDataType.DECIMAL,
            'decimal': UniversalDataType.DECIMAL,
            'varchar': UniversalDataType.VARCHAR,
            'character varying': UniversalDataType.VARCHAR,  # PostgreSQL's actual VARCHAR type
            'text': UniversalDataType.TEXT,
            'char': UniversalDataType.CHAR,
            'character': UniversalDataType.CHAR,  # PostgreSQL's actual CHAR type
            'date': UniversalDataType.DATE,
            'time': UniversalDataType.TIME,
            'timestamp': UniversalDataType.TIMESTAMP,
            'timestamp without time zone': UniversalDataType.TIMESTAMP,
            'timestamp with time zone': UniversalDataType.TIMESTAMP,
            'timestamptz': UniversalDataType.TIMESTAMP,
            'datetime': UniversalDataType.DATETIME,
            'boolean': UniversalDataType.BOOLEAN,
            'bool': UniversalDataType.BOOLEAN,
            'bytea': UniversalDataType.BLOB,
            'json': UniversalDataType.JSON,
            'jsonb': UniversalDataType.JSON,
            'uuid': UniversalDataType.UUID,
        }
        
        normalized_type = source_type.lower().strip()
        
        # Handle types with precision/scale (e.g., timestamp(6), varchar(255))
        base_type = normalized_type.split('(')[0] if '(' in normalized_type else normalized_type
        return type_mapping.get(base_type, UniversalDataType.TEXT)
    
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to PostgreSQL type"""
        type_mapping = {
            UniversalDataType.INTEGER: 'INTEGER',
            UniversalDataType.BIGINT: 'BIGINT',
            UniversalDataType.SMALLINT: 'SMALLINT',
            UniversalDataType.FLOAT: 'REAL',
            UniversalDataType.DOUBLE: 'DOUBLE PRECISION',
            UniversalDataType.DECIMAL: 'NUMERIC',
            UniversalDataType.VARCHAR: 'VARCHAR(255)',
            UniversalDataType.TEXT: 'TEXT',
            UniversalDataType.CHAR: 'CHAR(1)',
            UniversalDataType.DATE: 'DATE',
            UniversalDataType.TIME: 'TIME',
            UniversalDataType.TIMESTAMP: 'TIMESTAMP',
            UniversalDataType.DATETIME: 'TIMESTAMP',
            UniversalDataType.BOOLEAN: 'BOOLEAN',
            UniversalDataType.BLOB: 'BYTEA',
            UniversalDataType.JSON: 'JSONB',
            UniversalDataType.UUID: 'UUID',
        }
        return type_mapping.get(universal_type, 'TEXT')