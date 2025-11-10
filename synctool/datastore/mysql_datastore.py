"""
MySQL datastore implementation.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from .base_datastore import BaseDatastore
from ..core.models import ConnectionConfig
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType


class MySQLDatastore(BaseDatastore):
    """
    MySQL datastore implementation using aiomysql.
    
    Features:
    - Connection pooling with aiomysql
    - Lazy loading of aiomysql driver
    - Raw query execution with parameter binding
    - Automatic connection management
    """
    
    def __init__(self, name: str, connection_config: ConnectionConfig):
        super().__init__(name, connection_config)
    
    async def _create_connection(self) -> None:
        """Create MySQL connection pool using aiomysql"""
        try:
            import aiomysql
        except ImportError:
            raise ImportError(
                "aiomysql is required for MySQL datastore. "
                "Install it with: pip install aiomysql"
            )
        
        self._connection_pool = await aiomysql.create_pool(
            host=self.connection_config.host,
            port=self.connection_config.port or 3306,
            user=self.connection_config.user,
            password=self.connection_config.password,
            db=self.connection_config.database,
            autocommit=True,
            minsize=self.connection_config.min_connections,
            maxsize=self.connection_config.max_connections
        )
        
        # Test connection
        async with self._connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                if not result or result[0] != 1:
                    raise RuntimeError("MySQL connection test failed")
    
    async def _cleanup_connections(self) -> None:
        """Clean up MySQL connection pool"""
        if self._connection_pool:
            self._connection_pool.close()
            await self._connection_pool.wait_closed()
            self._connection_pool = None
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        action: str = 'select'
    ) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a raw SQL query against MySQL.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            action: Type of query ('select', 'insert', 'update', 'delete')
            
        Returns:
            For SELECT queries: List of dictionaries representing rows
            For DML queries: Number of affected rows
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"MySQL datastore {self.name} is not connected")
        
        self._logger.info(f"Executing MySQL query: {query}")
        if params:
            self._logger.debug(f"Query parameters: {params}")
        
        try:
            async with self._connection_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, params or [])
                    
                    if action.lower() == 'select':
                        # For SELECT queries, return rows as list of dictionaries
                        columns = [desc[0] for desc in cur.description] if cur.description else []
                        rows = await cur.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    else:
                        # For DML queries, return number of affected rows
                        await conn.commit()
                        return cur.rowcount
        except Exception as e:
            self._logger.error(f"MySQL query failed: {e}")
            raise
    
    async def execute_batch_query(
        self,
        query: str,
        params_list: List[List[Any]],
        batch_size: int = 1000
    ) -> int:
        """
        Execute a query with multiple parameter sets in batches.
        
        Args:
            query: SQL query string with placeholders
            params_list: List of parameter lists
            batch_size: Number of queries to execute in each batch
            
        Returns:
            Total number of affected rows
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"MySQL datastore {self.name} is not connected")
        
        total_affected = 0
        
        async with self._connection_pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(params_list), batch_size):
                    batch = params_list[i:i + batch_size]
                    
                    for params in batch:
                        await cur.execute(query, params)
                        total_affected += cur.rowcount
                    
                    await conn.commit()
        
        return total_affected
    
    async def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get table structure information using DESCRIBE.
        
        Args:
            table_name: Name of the table to describe
            
        Returns:
            List of dictionaries containing column information
        """
        query = f"DESCRIBE {table_name}"
        return await self.execute_query(query, action='select')
    
    async def get_table_status(self, table_name: str) -> Dict[str, Any]:
        """
        Get table status information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table status information
        """
        query = f"SHOW TABLE STATUS LIKE '{table_name}'"
        result = await self.execute_query(query, action='select')
        return result[0] if result else {}
    
    # Schema management methods
    
    async def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if a table exists in MySQL"""
        database = schema_name or self.connection_config.database
        
        query = """
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name = %s
        """
        
        result = await self.execute_query(query, [database, table_name])
        return result[0]['count'] > 0 if result else False
    
    async def extract_table_schema(self, table_name: str, schema_name: Optional[str] = None) -> UniversalSchema:
        """Extract MySQL table schema"""
        database = schema_name or self.connection_config.database
        
        # Use DESCRIBE to get column information
        describe_query = f"DESCRIBE {table_name}"
        columns_raw = await self.execute_query(describe_query)
        
        if not columns_raw:
            raise ValueError(f"Table {table_name} does not exist or has no columns")
        
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
            
            # Extract length/precision/scale from type string
            # e.g., "varchar(100)", "char(36)", "decimal(10,2)"
            max_length = None
            precision = None
            scale = None
            
            col_type_lower = col_type.lower()
            if '(' in col_type_lower:
                # Extract parameters from type
                import re
                match = re.match(r'(\w+)\(([^)]+)\)', col_type_lower)
                if match:
                    base_type = match.group(1)
                    params = match.group(2)
                    
                    if base_type in ['varchar', 'char']:
                        # For VARCHAR/CHAR, it's just the length
                        max_length = int(params)
                    elif base_type in ['decimal', 'numeric']:
                        # For DECIMAL, it's precision,scale
                        if ',' in params:
                            prec_scale = params.split(',')
                            precision = int(prec_scale[0].strip())
                            scale = int(prec_scale[1].strip())
                        else:
                            precision = int(params)
            
            column = UniversalColumn(
                name=col_name,
                data_type=universal_type,
                nullable=col['Null'] == 'YES',
                primary_key=is_primary_key,
                unique=col_key == 'UNI',
                auto_increment='auto_increment' in col_extra.lower(),
                default_value=col.get('Default'),
                max_length=max_length,
                precision=precision,
                scale=scale
            )
            columns.append(column)
        
        return UniversalSchema(
            table_name=table_name,
            database_name=database,
            columns=columns,
            primary_keys=primary_keys
        )
    
    def generate_create_table_ddl(
        self, 
        schema: UniversalSchema, 
        target_table_name: Optional[str] = None,
        if_not_exists: bool = False
    ) -> str:
        """Generate MySQL CREATE TABLE DDL"""
        table_name = target_table_name or schema.table_name
        if schema.database_name:
            table_name = f"`{schema.database_name}`.`{table_name}`"
        else:
            table_name = f"`{table_name}`"
        
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        columns = []
        for col in schema.columns:
            # Get base type and apply precision/scale/length if applicable
            col_type = self._get_column_type_with_params(col)
            col_def = f"`{col.name}` {col_type}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.auto_increment:
                col_def += " AUTO_INCREMENT"
            elif col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            columns.append(col_def)
        
        # Add primary key constraint
        if schema.primary_keys:
            pk_columns = ', '.join([f"`{pk}`" for pk in schema.primary_keys])
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
        """Generate MySQL ALTER TABLE DDL statements"""
        full_table = f"`{schema_name}`.`{table_name}`" if schema_name else f"`{table_name}`"
        ddl_statements = []
        
        for change in changes:
            if change['type'] == 'add_column':
                col = change['column']
                col_type = self.map_universal_type_to_target(col.data_type)
                nullable = "" if col.nullable else " NOT NULL"
                default = f" DEFAULT {col.default_value}" if col.default_value else ""
                ddl = f"ALTER TABLE {full_table} ADD COLUMN `{col.name}` {col_type}{default}{nullable};"
                ddl_statements.append(ddl)
            
            elif change['type'] == 'modify_column':
                col = change['column']
                col_type = self.map_universal_type_to_target(col.data_type)
                nullable = "" if col.nullable else " NOT NULL"
                ddl = f"ALTER TABLE {full_table} MODIFY COLUMN `{col.name}` {col_type}{nullable};"
                ddl_statements.append(ddl)
            
            elif change['type'] == 'drop_column':
                ddl = f"ALTER TABLE {full_table} DROP COLUMN `{change['column_name']}`;"
                ddl_statements.append(ddl)
        
        return ddl_statements
    
    def map_source_type_to_universal(self, source_type: str) -> UniversalDataType:
        """Map MySQL type to universal type"""
        type_mapping = {
            'int': UniversalDataType.INTEGER,
            'integer': UniversalDataType.INTEGER,
            'tinyint': UniversalDataType.SMALLINT,
            'smallint': UniversalDataType.SMALLINT,
            'mediumint': UniversalDataType.INTEGER,
            'bigint': UniversalDataType.BIGINT,
            'float': UniversalDataType.FLOAT,
            'double': UniversalDataType.DOUBLE,
            'decimal': UniversalDataType.DECIMAL,
            'numeric': UniversalDataType.DECIMAL,
            'varchar': UniversalDataType.VARCHAR,
            'char': UniversalDataType.CHAR,
            'text': UniversalDataType.TEXT,
            'tinytext': UniversalDataType.TEXT,
            'mediumtext': UniversalDataType.TEXT,
            'longtext': UniversalDataType.TEXT,
            'date': UniversalDataType.DATE,
            'time': UniversalDataType.TIME,
            'datetime': UniversalDataType.DATETIME,
            'timestamp': UniversalDataType.TIMESTAMP,
            'boolean': UniversalDataType.BOOLEAN,
            'bool': UniversalDataType.BOOLEAN,
            'blob': UniversalDataType.BLOB,
            'tinyblob': UniversalDataType.BLOB,
            'mediumblob': UniversalDataType.BLOB,
            'longblob': UniversalDataType.BLOB,
            'binary': UniversalDataType.BINARY,
            'varbinary': UniversalDataType.BINARY,
            'json': UniversalDataType.JSON,
        }
        
        normalized_type = source_type.lower().strip()
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
            UniversalDataType.DECIMAL: 'DECIMAL(10,2)',
            UniversalDataType.VARCHAR: 'VARCHAR(255)',
            UniversalDataType.TEXT: 'TEXT',
            UniversalDataType.CHAR: 'CHAR(1)',
            UniversalDataType.DATE: 'DATE',
            UniversalDataType.TIME: 'TIME',
            UniversalDataType.TIMESTAMP: 'TIMESTAMP',
            UniversalDataType.DATETIME: 'DATETIME',
            UniversalDataType.BOOLEAN: 'BOOLEAN',
            UniversalDataType.BLOB: 'BLOB',
            UniversalDataType.BINARY: 'BINARY',
            UniversalDataType.JSON: 'JSON',
            UniversalDataType.UUID: 'CHAR(36)',
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
        
        # Handle DECIMAL with precision and scale
        if base_type == UniversalDataType.DECIMAL:
            if col.precision and col.scale is not None:
                return f'DECIMAL({col.precision},{col.scale})'
            elif col.precision:
                return f'DECIMAL({col.precision})'
            return 'DECIMAL(10,2)'  # Default
        
        # For all other types, use the standard mapping
        return self.map_universal_type_to_target(base_type)
