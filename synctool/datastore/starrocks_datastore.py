"""
StarRocks datastore implementation.
"""
import asyncio
import csv
import io
import json
import logging
import uuid
from typing import Dict, List, Optional, Any, Union

from .base_datastore import BaseDatastore
from ..core.models import ConnectionConfig
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType


class StarRocksDatastore(BaseDatastore):
    """
    StarRocks datastore implementation using MySQL protocol and HTTP API.
    
    Features:
    - MySQL protocol connection pooling with aiomysql
    - HTTP session for stream load operations
    - Lazy loading of aiomysql and aiohttp drivers
    - Raw query execution with parameter binding
    - Stream load capabilities for bulk data loading
    - Automatic session variable management
    """
    
    def __init__(self, name: str, connection_config: ConnectionConfig):
        super().__init__(name, connection_config)
        
        # StarRocks-specific connection details
        self.host = connection_config.host or 'localhost'
        self.port = int(connection_config.port or 9030)
        self.http_port = getattr(connection_config, 'http_port', 8030)
        self.user = connection_config.user or 'root'
        self.password = connection_config.password or ''
        self.database = connection_config.database
        self.fe_host = f"http://{self.host}:{self.http_port}"
    
    async def _create_connection(self) -> None:
        """Create StarRocks MySQL connection pool and HTTP session"""
        # Import drivers with proper error messages
        try:
            import aiomysql
        except ImportError:
            raise ImportError(
                "aiomysql is required for StarRocks MySQL protocol. "
                "Install it with: pip install aiomysql"
            )
        
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "aiohttp is required for StarRocks HTTP operations. "
                "Install it with: pip install aiohttp"
            )
        # Create MySQL connection pool
        self._connection_pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            autocommit=True,
            minsize=self.connection_config.min_connections or 10,
            maxsize=self.connection_config.max_connections or 50,
            pool_recycle=3600,  # Recycle connections every hour
            echo=False
        )
        
        # Test MySQL connection and set session variables
        await self._test_mysql_connection()
        
        # Create HTTP session for stream load operations
        auth = None
        if self.user and self.password:
            import aiohttp
            auth = aiohttp.BasicAuth(self.user, self.password)
        elif self.user:
            import aiohttp
            auth = aiohttp.BasicAuth(self.user, '')
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'synctool-starrocks-client/1.0'
        }
        
        import aiohttp
        connector = aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            limit_per_host=20 
        )
        
        timeout = aiohttp.ClientTimeout(total=50, connect=20)
        
        self._http_session = aiohttp.ClientSession(
            auth=auth,
            headers=headers,
            connector=connector,
            timeout=timeout
        )
    
    async def _test_mysql_connection(self) -> None:
        """Test StarRocks MySQL connection and set session variables"""
        if not self._connection_pool:
            raise RuntimeError("Connection pool not initialized")
        
        async with self._connection_pool.acquire() as conn:
            # Set session variables for optimal performance
            try:
                session_query = "SET SESSION group_concat_max_len = 104857600"
                async with conn.cursor() as cur:
                    await cur.execute(session_query)
                self._logger.debug("StarRocks MySQL session variables configured successfully")
            except Exception as e:
                self._logger.warning(f"Failed to set session variables: {e}")
            
            # Test basic connectivity
            try:
                test_query = "SELECT 1 as test"
                async with conn.cursor() as cur:
                    await cur.execute(test_query)
                    result = await cur.fetchone()
                    if not result or result[0] != 1:
                        raise RuntimeError("Connection test failed - unexpected result")
            except Exception as e:
                raise RuntimeError(f"StarRocks MySQL connection test failed: {e}")
    
    async def _cleanup_connections(self) -> None:
        """Clean up StarRocks connections"""
        # Close MySQL connection pool
        if self._connection_pool:
            self._connection_pool.close()
            await self._connection_pool.wait_closed()
            self._connection_pool = None
        
        # Close HTTP session
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        
        # Clear connection tracking
        self._connection_session_vars_set.clear()
    
    async def set_session_variables_for_connection(self, conn, logger: Optional[logging.Logger] = None):
        """Set session variables for StarRocks connections - track to avoid duplicates"""
        conn_id = id(conn)
        if conn_id not in self._connection_session_vars_set:
            try:
                session_query = "SET SESSION group_concat_max_len = 104857600"
                async with conn.cursor() as cur:
                    await cur.execute(session_query)
                self._connection_session_vars_set.add(conn_id)
                if logger:
                    logger.debug(f"StarRocks session variables configured for connection {conn_id}")
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to set session variables for connection {conn_id}: {e}")
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        action: str = 'select'
    ) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a raw SQL query against StarRocks via MySQL protocol.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            action: Type of query ('select', 'insert', 'update', 'delete')
            
        Returns:
            For SELECT queries: List of dictionaries representing rows
            For DML queries: Number of affected rows
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"StarRocks datastore {self.name} is not connected")
        
        self._logger.info(f"Executing StarRocks query: {query}")
        if params:
            self._logger.debug(f"Query parameters: {params}")
        
        try:
            async with self._connection_pool.acquire() as conn:
                # Set session variables for this connection if not already set
                await self.set_session_variables_for_connection(conn, self._logger)
                
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
            self._logger.error(f"StarRocks query failed: {e}")
            raise
    
    async def execute_http_query(
        self,
        sql: str,
        action: str = "query",
        timeout: int = 60
    ) -> Union[List[Dict], None]:
        """
        Execute a SQL query against StarRocks via the REST API.
        
        Args:
            sql: The SQL statement to run
            action: A label for logging/metrics (e.g., "insert", "query")
            timeout: Request timeout in seconds
            
        Returns:
            For SELECT queries: list of rows (dicts)
            For DML/DDL queries: None (success)
        """
        if not self._is_connected or not self._http_session:
            raise RuntimeError(f"StarRocks datastore {self.name} is not connected")

        url = f"{self.fe_host}/query/sql"
        headers = {
            "Content-Type": "application/json",
        }
        body = {
            "sql": sql
        }

        self._logger.debug(f"[StarRocks] Executing {action}: {sql}")

        async with self._http_session.post(url, headers=headers, json=body, timeout=timeout) as resp:
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
    
    def _stream_load_url(self, table: str) -> str:
        """Generate stream load URL for a table"""
        if self.database:
            return f"{self.fe_host}/api/{self.database}/{table}/_stream_load"
        else:
            return f"{self.fe_host}/api/{table}/_stream_load"
    
    async def stream_load_csv(
        self,
        table: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Perform stream load operation with CSV data.
        
        Args:
            table: Target table name
            data: List of dictionaries representing rows
            columns: List of column names to load
            batch_size: Maximum number of rows per batch
            
        Returns:
            Dictionary containing load results
        """
        if not self._is_connected or not self._http_session:
            raise RuntimeError(f"StarRocks datastore {self.name} is not connected")
        
        url = self._stream_load_url(table)
        label = f"stream_{int(asyncio.get_event_loop().time()*1000)}_{uuid.uuid4().hex[:6]}"
        
        # Convert data to CSV format
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        
        for row in data:
            writer.writerow([
                row.get(col, "") if row.get(col) is not None else "" 
                for col in columns
            ])
        
        payload = buf.getvalue().encode("utf-8")
        
        headers = {
            "label": label,
            "format": "csv",
            "columns": ",".join(columns),
            "Expect": "100-continue",
            "column_separator": ","
        }
        
        async with self._http_session.put(url, data=payload, headers=headers) as resp:
            text = await resp.text()
            
            if resp.status != 200:
                raise RuntimeError(
                    f"Stream load failed {resp.status}: {text}\nURL: {url}\nHeaders: {headers}"
                )
            
            try:
                result = json.loads(text)
            except json.JSONDecodeError:
                raise RuntimeError(f"Invalid response from StarRocks: {text}")
            
            status = result.get("Status", "").lower()
            if "success" not in status:
                raise RuntimeError(f"Stream load failed: {result}")
            
            return result
    
    # Schema management methods
    
    async def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """Check if a table exists in StarRocks"""
        database = schema_name or self.database
        
        query = """
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name = %s
        """
        
        result = await self.execute_query(query, [database, table_name])
        return result[0]['count'] > 0 if result else False
    
    async def extract_table_schema(self, table_name: str, schema_name: Optional[str] = None) -> UniversalSchema:
        """Extract StarRocks table schema"""
        database = schema_name or self.database
        
        # Use DESCRIBE to get column information (StarRocks uses MySQL syntax)
        describe_query = f"DESCRIBE {table_name}"
        columns_raw = await self.execute_query(describe_query)
        
        if not columns_raw:
            raise ValueError(f"Table {table_name} does not exist or has no columns")
        
        columns = []
        primary_keys = []
        
        for col in columns_raw:
            col_name = col['Field']
            col_type = col['Type']
            col_key = col.get('Key', '')
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
        """Generate StarRocks CREATE TABLE DDL"""
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
            
            if col.default_value and not col.auto_increment:
                col_def += f" DEFAULT {col.default_value}"
            
            columns.append(col_def)
        
        # Add primary key constraint if present
        pk_clause = ""
        if schema.primary_keys:
            pk_columns = ', '.join([f"`{pk}`" for pk in schema.primary_keys])
            columns.append(f"PRIMARY KEY ({pk_columns})")
        
        # StarRocks requires ENGINE and distribution spec
        # Using a simple default - users should customize as needed
        engine_spec = "ENGINE=OLAP"
        distribution_spec = ""
        if schema.primary_keys:
            distribution_spec = f"DISTRIBUTED BY HASH(`{schema.primary_keys[0]}`) BUCKETS 10"
        
        columns_str = ',\n  '.join(columns)
        ddl = f"""CREATE TABLE {if_not_exists_clause}{table_name} (
  {columns_str}
) {engine_spec}"""
        
        if distribution_spec:
            ddl += f"\n{distribution_spec}"
        
        ddl += ";"
        
        return ddl
    
    def generate_alter_table_ddl(
        self,
        table_name: str,
        changes: List[Dict[str, Any]],
        schema_name: Optional[str] = None
    ) -> List[str]:
        """Generate StarRocks ALTER TABLE DDL statements"""
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
        """Map StarRocks type to universal type"""
        type_mapping = {
            'int': UniversalDataType.INTEGER,
            'integer': UniversalDataType.INTEGER,
            'tinyint': UniversalDataType.SMALLINT,
            'smallint': UniversalDataType.SMALLINT,
            'bigint': UniversalDataType.BIGINT,
            'largeint': UniversalDataType.BIGINT,
            'float': UniversalDataType.FLOAT,
            'double': UniversalDataType.DOUBLE,
            'decimal': UniversalDataType.DECIMAL,
            'varchar': UniversalDataType.VARCHAR,
            'char': UniversalDataType.CHAR,
            'string': UniversalDataType.TEXT,
            'text': UniversalDataType.TEXT,
            'date': UniversalDataType.DATE,
            'datetime': UniversalDataType.DATETIME,
            'timestamp': UniversalDataType.TIMESTAMP,
            'boolean': UniversalDataType.BOOLEAN,
            'bool': UniversalDataType.BOOLEAN,
            'json': UniversalDataType.JSON,
            'array': UniversalDataType.TEXT,  # Map array to TEXT for simplicity
            'map': UniversalDataType.TEXT,    # Map to TEXT for simplicity
            'struct': UniversalDataType.TEXT, # Map to TEXT for simplicity
        }
        
        normalized_type = source_type.lower().strip()
        base_type = normalized_type.split('(')[0] if '(' in normalized_type else normalized_type
        # Handle array types like array<int>
        if '<' in base_type:
            base_type = base_type.split('<')[0]
        return type_mapping.get(base_type, UniversalDataType.TEXT)
    
    def map_universal_type_to_target(self, universal_type: UniversalDataType) -> str:
        """Map universal type to StarRocks type"""
        type_mapping = {
            UniversalDataType.INTEGER: 'INT',
            UniversalDataType.BIGINT: 'BIGINT',
            UniversalDataType.SMALLINT: 'SMALLINT',
            UniversalDataType.FLOAT: 'FLOAT',
            UniversalDataType.DOUBLE: 'DOUBLE',
            UniversalDataType.DECIMAL: 'DECIMAL(10,2)',
            UniversalDataType.VARCHAR: 'VARCHAR(255)',
            UniversalDataType.TEXT: 'STRING',  # StarRocks uses STRING for text
            UniversalDataType.CHAR: 'CHAR(1)',
            UniversalDataType.DATE: 'DATE',
            UniversalDataType.TIME: 'TIME',
            UniversalDataType.TIMESTAMP: 'DATETIME',  # StarRocks DATETIME is similar to TIMESTAMP
            UniversalDataType.DATETIME: 'DATETIME',
            UniversalDataType.BOOLEAN: 'BOOLEAN',
            UniversalDataType.BLOB: 'STRING',  # StarRocks doesn't have BLOB, use STRING
            UniversalDataType.BINARY: 'STRING',
            UniversalDataType.JSON: 'JSON',
            UniversalDataType.UUID: 'CHAR(36)',
            UniversalDataType.UUID_TEXT: 'CHAR(32)',      # UUID without dashes (32 hex chars)
            UniversalDataType.UUID_TEXT_DASH: 'CHAR(36)', # UUID with dashes (36 chars)
        }
        return type_mapping.get(universal_type, 'STRING')
    
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
