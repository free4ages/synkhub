"""
Base datastore interface for all database connection implementations.
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union

from ..core.models import ConnectionConfig


class BaseDatastore(ABC):
    """
    Abstract base class for all datastore implementations.
    
    Provides common interface for database connection management with:
    - Lazy loading of database drivers
    - Idempotent connect/disconnect operations
    - Thread-safe connection management
    - Raw query execution capabilities
    """
    
    def __init__(self, name: str, connection_config: ConnectionConfig):
        self.name = name
        self.connection_config = connection_config
        self._connection_pool = None
        self._http_session = None  # For datastores that need HTTP (like StarRocks)
        self._connection_lock = asyncio.Lock()
        self._is_connected = False
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Connection tracking for session variables (StarRocks specific)
        self._connection_session_vars_set = set()
    
    @property
    def is_connected(self) -> bool:
        """Check if the datastore is currently connected"""
        return self._is_connected
    
    @abstractmethod
    async def _create_connection(self) -> None:
        """Create the actual database connection - implemented by subclasses"""
        pass
    
    @abstractmethod
    async def _cleanup_connections(self) -> None:
        """Clean up database connections - implemented by subclasses"""
        pass
    
    @abstractmethod
    async def execute_query(
        self, 
        query: str, 
        params: Optional[List[Any]] = None,
        action: str = 'select'
    ) -> Union[List[Dict[str, Any]], int]:
        """
        Execute a raw SQL query against the datastore.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            action: Type of query ('select', 'insert', 'update', 'delete')
            
        Returns:
            For SELECT queries: List of dictionaries representing rows
            For DML queries: Number of affected rows
        """
        pass
    
    async def connect(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Connect to the datastore - idempotent operation.
        
        Args:
            logger: Optional logger for connection messages
        """
        async with self._connection_lock:
            if self._is_connected:
                return
            
            if logger:
                logger.info(f"Connecting to {self.__class__.__name__}: {self.name}")
            
            try:
                await self._create_connection()
                self._is_connected = True
                if logger:
                    logger.info(f"Successfully connected to {self.__class__.__name__}: {self.name}")
                    
            except Exception as e:
                if logger:
                    logger.error(f"Failed to connect to {self.__class__.__name__} {self.name}: {e}")
                await self._cleanup_connections()
                raise
    
    async def disconnect(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Disconnect from the datastore - idempotent operation.
        
        Args:
            logger: Optional logger for disconnection messages
        """
        async with self._connection_lock:
            if not self._is_connected:
                return
            
            if logger:
                logger.info(f"Disconnecting from {self.__class__.__name__}: {self.name}")
            
            try:
                await self._cleanup_connections()
                self._is_connected = False
                if logger:
                    logger.info(f"Successfully disconnected from {self.__class__.__name__}: {self.name}")
            except Exception as e:
                if logger:
                    logger.error(f"Error during disconnect from {self.__class__.__name__} {self.name}: {e}")
                # Still mark as disconnected even if cleanup failed
                self._is_connected = False
    
    async def get_connection_pool(self):
        """
        Get the connection pool for database operations.
        
        Returns:
            Database connection pool
            
        Raises:
            RuntimeError: If datastore is not connected
        """
        if not self._is_connected or not self._connection_pool:
            raise RuntimeError(f"Datastore {self.name} is not connected. Call connect() first.")
        return self._connection_pool
    
    async def get_http_session(self):
        """
        Get the HTTP session for HTTP-based operations (StarRocks stream load).
        
        Returns:
            HTTP session object
            
        Raises:
            RuntimeError: If datastore doesn't support HTTP or is not connected
        """
        if not self._is_connected:
            raise RuntimeError(f"Datastore {self.name} is not connected. Call connect() first.")
        if not hasattr(self, '_http_session') or not self._http_session:
            raise RuntimeError(f"Datastore {self.name} does not support HTTP sessions")
        return self._http_session
