"""
PostgreSQL datastore implementation.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from .base_datastore import BaseDatastore
from ..core.models import ConnectionConfig
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType


class PostgresDatastore(BaseDatastore):
    """
    PostgreSQL datastore implementation using asyncpg.
    
    Features:
    - Connection pooling with asyncpg
    - Lazy loading of asyncpg driver
    - Raw query execution with parameter binding
    - Automatic connection management
    """
    
    def __init__(self, name: str, connection_config: ConnectionConfig):
        super().__init__(name, connection_config)
    
    async def _create_connection(self) -> None:
        """Create PostgreSQL connection pool using asyncpg"""
        try:
            import asyncpg
        except ImportError:
            raise ImportError(
                "asyncpg is required for PostgreSQL datastore. "
                "Install it with: pip install asyncpg"
            )
        
        database_name = self.connection_config.database or self.connection_config.dbname
        
        self._connection_pool = await asyncpg.create_pool(
            host=self.connection_config.host,
            port=self.connection_config.port or 5432,
            user=self.connection_config.user,
            password=self.connection_config.password,
            database=database_name,
            min_size=self.connection_config.min_connections,
            max_size=self.connection_config.max_connections
        )
        
        # Test connection
        async with self._connection_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    
    async def _cleanup_connections(self) -> None:
        """Clean up PostgreSQL connection pool"""
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        action: str = 'select'
    ) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a raw SQL query against PostgreSQL.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            action: Type of query ('select', 'insert', 'update', 'delete')
            
        Returns:
            For SELECT queries: List of dictionaries representing rows
            For DML queries: Number of affected rows
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"PostgreSQL datastore {self.name} is not connected")
        
        self._logger.info(f"Executing PostgreSQL query: {query} with params: {params}")
        if params:
            self._logger.debug(f"Query parameters: {params}")
        
        try:
            if action.lower() == 'select':
                # For SELECT queries, return rows as list of dictionaries
                rows = await self._connection_pool.fetch(query, *(params or []))
                return [dict(row) for row in rows]
            else:
                # For DML queries, return number of affected rows
                result = await self._connection_pool.execute(query, *(params or []))
                # asyncpg returns a string like "INSERT 0 5" for INSERT queries
                # Extract the number of affected rows
                if isinstance(result, str):
                    parts = result.split()
                    if len(parts) >= 2 and parts[-1].isdigit():
                        return int(parts[-1])
                    return 0
                return result
        except Exception as e:
            self._logger.error(f"PostgreSQL query failed: {e}")
            raise
    
    async def execute_copy_from(
        self,
        table: str,
        source,
        columns: Optional[List[str]] = None,
        format: str = 'csv',
        delimiter: str = '\t',
        null: str = ''
    ) -> int:
        """
        Execute COPY FROM operation for bulk data loading.
        
        Args:
            table: Target table name
            source: Data source (file-like object or string)
            columns: Optional list of column names
            format: Data format ('csv', 'text', 'binary')
            delimiter: Field delimiter for CSV format
            null: Null value representation
            
        Returns:
            Number of rows copied
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"PostgreSQL datastore {self.name} is not connected")
        
        async with self._connection_pool.acquire() as conn:
            return await conn.copy_to_table(
                table,
                source=source,
                columns=columns,
                format=format,
                delimiter=delimiter,
                null=null
            )
    
    async def create_temp_table(
        self,
        temp_table_name: str,
        like_table: str,
        on_commit: str = 'DROP'
    ) -> None:
        """
        Create a temporary table based on an existing table structure.
        
        Args:
            temp_table_name: Name of the temporary table
            like_table: Name of the table to copy structure from
            on_commit: What to do on transaction commit ('DROP', 'DELETE ROWS', 'PRESERVE ROWS')
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"PostgreSQL datastore {self.name} is not connected")
        
        query = f"""
            CREATE TEMP TABLE {temp_table_name} 
            (LIKE {like_table} INCLUDING DEFAULTS) 
            ON COMMIT {on_commit}
        """
        
        await self.execute_query(query, action='create')
    
    # Schema management methods
    
    async def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if a table exists in PostgreSQL"""
        schema = schema_name or self.connection_config.schema or 'public'
        
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 
            AND table_name = $2
        )
        """
        
        result = await self.execute_query(query, [schema, table_name])
        return result[0]['exists'] if result else False
    
    async def extract_table_schema(self, table_name: str, schema_name: Optional[str] = None) -> UniversalSchema:
        """Extract PostgreSQL table schema"""
        schema = schema_name or self.connection_config.schema or 'public'
        
        # Get column information
        columns_query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            ordinal_position,
            is_identity
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """
        columns_data = await self.execute_query(columns_query, [schema, table_name])
        
        if not columns_data:
            raise ValueError(f"Table {schema}.{table_name} does not exist or has no columns")
        
        # Get primary key information
        pk_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY' 
            AND tc.table_schema = $1 
            AND tc.table_name = $2
        """
        pk_result = await self.execute_query(pk_query, [schema, table_name])
        primary_keys = [row['column_name'] for row in pk_result]
        
        # Get index information
        indexes_query = """
        SELECT 
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname = $1 AND tablename = $2
        """
        indexes = await self.execute_query(indexes_query, [schema, table_name])
        
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
                max_length=col_data['character_maximum_length'],
                precision=col_data['numeric_precision'],
                scale=col_data['numeric_scale']
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            schema_name=schema,
            database_name=self.connection_config.database or self.connection_config.dbname,
            columns=columns,
            primary_keys=primary_keys,
            indexes=indexes
        )
    
    def generate_create_table_ddl(
        self, 
        schema: UniversalSchema, 
        target_table_name: Optional[str] = None,
        if_not_exists: bool = False
    ) -> str:
        """Generate PostgreSQL CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        if schema.schema_name:
            table_name = f"{schema.schema_name}.{table_name}"
        
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        columns = []
        for col in schema.columns:
            # Get base type and apply precision/scale/length if applicable
            col_type = self._get_column_type_with_params(col)
            col_def = f"{col.name} {col_type}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            columns.append(col_def)
        
        # Add primary key constraint
        if schema.primary_keys:
            pk_columns = ', '.join(schema.primary_keys)
            columns.append(f"PRIMARY KEY ({pk_columns})")
        
        columns_str = ',\n  '.join(columns)
        return f"""CREATE TABLE {if_not_exists_clause}{table_name} (
  {columns_str}
);"""
    
    def generate_alter_table_ddl(
        self,
        table_name: str,
        changes: List[Dict[str, Any]],
        schema_name: Optional[str] = None
    ) -> List[str]:
        """Generate PostgreSQL ALTER TABLE DDL statements"""
        full_table = f"{schema_name}.{table_name}" if schema_name else table_name
        ddl_statements = []
        
        for change in changes:
            if change['type'] == 'add_column':
                col = change['column']
                col_type = self.map_universal_type_to_target(col.data_type)
                nullable = "" if col.nullable else " NOT NULL"
                default = f" DEFAULT {col.default_value}" if col.default_value else ""
                ddl = f"ALTER TABLE {full_table} ADD COLUMN {col.name} {col_type}{default}{nullable};"
                ddl_statements.append(ddl)
            
            elif change['type'] == 'modify_column':
                col = change['column']
                col_type = self.map_universal_type_to_target(col.data_type)
                # PostgreSQL requires separate ALTER COLUMN statements
                ddl = f"ALTER TABLE {full_table} ALTER COLUMN {col.name} TYPE {col_type};"
                ddl_statements.append(ddl)
                
                # Handle nullable change if needed
                if not col.nullable:
                    ddl = f"ALTER TABLE {full_table} ALTER COLUMN {col.name} SET NOT NULL;"
                    ddl_statements.append(ddl)
            
            elif change['type'] == 'drop_column':
                ddl = f"ALTER TABLE {full_table} DROP COLUMN {change['column_name']};"
                ddl_statements.append(ddl)
        
        return ddl_statements
    
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map PostgreSQL type to universal type"""
        type_mapping = {
            'integer': UniversalDataType.INTEGER,
            'int': UniversalDataType.INTEGER,
            'bigint': UniversalDataType.BIGINT,
            'smallint': UniversalDataType.SMALLINT,
            'serial': UniversalDataType.INTEGER,
            'bigserial': UniversalDataType.BIGINT,
            'smallserial': UniversalDataType.SMALLINT,
            'real': UniversalDataType.FLOAT,
            'double precision': UniversalDataType.DOUBLE,
            'numeric': UniversalDataType.DECIMAL,
            'decimal': UniversalDataType.DECIMAL,
            'varchar': UniversalDataType.VARCHAR,
            'character varying': UniversalDataType.VARCHAR,
            'text': UniversalDataType.TEXT,
            'char': UniversalDataType.CHAR,
            'character': UniversalDataType.CHAR,
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
            UniversalDataType.UUID_TEXT: 'CHAR(32)',      # UUID without dashes (32 hex chars)
            UniversalDataType.UUID_TEXT_DASH: 'CHAR(36)', # UUID with dashes (36 chars)
        }
        return type_mapping.get(universal_type, 'TEXT')
    
    def _get_column_type_with_params(self, col: UniversalColumn) -> str:
        """Get column type with precision/scale/length parameters"""
        base_type = col.data_type
        
        # Handle VARCHAR with custom length
        if base_type == UniversalDataType.VARCHAR:
            if col.max_length:
                return f'VARCHAR({col.max_length})'
            return 'VARCHAR(255)'  # Default
        
        # Handle CHAR with custom length
        if base_type == UniversalDataType.CHAR:
            if col.max_length:
                return f'CHAR({col.max_length})'
            return 'CHAR(1)'  # Default
        
        # Handle DECIMAL/NUMERIC with precision and scale
        if base_type == UniversalDataType.DECIMAL:
            if col.precision and col.scale is not None:
                return f'NUMERIC({col.precision},{col.scale})'
            elif col.precision:
                return f'NUMERIC({col.precision})'
            return 'NUMERIC'  # Default
        
        # For all other types, use the standard mapping
        return self.map_universal_type_to_target(base_type)
