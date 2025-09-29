"""
PostgreSQL datastore implementation.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from .base_datastore import BaseDatastore
from ..core.models import ConnectionConfig


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
