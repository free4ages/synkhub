"""
MySQL datastore implementation.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from .base_datastore import BaseDatastore
from ..core.models import ConnectionConfig


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
