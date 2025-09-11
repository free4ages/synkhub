import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime

from .config_store import ConfigStore, ConfigMetadata
from .config_loader import ConfigLoader
from .config_serializer import ConfigSerializer
from ..core.models import PipelineJobConfig, DataStorage


class DatabaseConfigStore(ConfigStore):
    """Database-based configuration store using PostgreSQL/MySQL"""
    
    def __init__(self, connection_config: dict, table_prefix: str = "synctool_config"):
        self.connection_config = connection_config
        self.table_prefix = table_prefix
        self.pipeline_configs_table = f"{table_prefix}_pipeline_configs"
        self.datastores_table = f"{table_prefix}_datastores"
        self._pool = None
        self._db_type = connection_config.get('type', 'postgres')
    
    async def _get_connection_pool(self):
        """Get or create database connection pool"""
        if self._pool is None:
            if self._db_type == 'postgres':
                try:
                    import asyncpg
                    self._pool = await asyncpg.create_pool(
                        host=self.connection_config['host'],
                        port=self.connection_config.get('port', 5432),
                        user=self.connection_config['user'],
                        password=self.connection_config['password'],
                        database=self.connection_config['database'],
                        min_size=1,
                        max_size=10
                    )
                except ImportError:
                    raise ImportError("asyncpg is required for PostgreSQL support. Install with: pip install asyncpg")
            
            elif self._db_type == 'mysql':
                try:
                    import aiomysql
                    self._pool = await aiomysql.create_pool(
                        host=self.connection_config['host'],
                        port=self.connection_config.get('port', 3306),
                        user=self.connection_config['user'],
                        password=self.connection_config['password'],
                        db=self.connection_config['database'],
                        minsize=1,
                        maxsize=10
                    )
                except ImportError:
                    raise ImportError("aiomysql is required for MySQL support. Install with: pip install aiomysql")
            
            else:
                raise ValueError(f"Unsupported database type: {self._db_type}")
        
        return self._pool
    
    async def initialize_tables(self):
        """Initialize database tables for config storage"""
        pool = await self._get_connection_pool()
        
        if self._db_type == 'postgres':
            # PostgreSQL syntax
            create_pipeline_configs_table = f"""
            CREATE TABLE IF NOT EXISTS {self.pipeline_configs_table} (
                name VARCHAR(255) PRIMARY KEY,
                config_data JSONB NOT NULL,
                version VARCHAR(50) DEFAULT '1.0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tags TEXT[],
                description TEXT
            )
            """
            
            create_datastores_table = f"""
            CREATE TABLE IF NOT EXISTS {self.datastores_table} (
                id SERIAL PRIMARY KEY,
                config_data JSONB NOT NULL,
                version VARCHAR(50) DEFAULT '1.0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            )
            """
        
        else:  # MySQL
            create_pipeline_configs_table = f"""
            CREATE TABLE IF NOT EXISTS {self.pipeline_configs_table} (
                name VARCHAR(255) PRIMARY KEY,
                config_data JSON NOT NULL,
                version VARCHAR(50) DEFAULT '1.0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                tags JSON,
                description TEXT
            )
            """
            
            create_datastores_table = f"""
            CREATE TABLE IF NOT EXISTS {self.datastores_table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                config_data JSON NOT NULL,
                version VARCHAR(50) DEFAULT '1.0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                description TEXT
            )
            """
        
        async with pool.acquire() as conn:
            if self._db_type == 'postgres':
                await conn.execute(create_pipeline_configs_table)
                await conn.execute(create_datastores_table)
            else:  # MySQL
                cursor = await conn.cursor()
                await cursor.execute(create_pipeline_configs_table)
                await cursor.execute(create_datastores_table)
                await cursor.close()
    
    async def save_pipeline_config(self, config: PipelineJobConfig, metadata: ConfigMetadata = None) -> bool:
        """Save a pipeline configuration to database"""
        try:
            pool = await self._get_connection_pool()
            config_dict = ConfigSerializer.config_to_dict(config)
            
            if not metadata:
                metadata = ConfigMetadata(name=config.name)
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    await conn.execute(f"""
                        INSERT INTO {self.pipeline_configs_table} 
                        (name, config_data, version, created_at, updated_at, tags, description)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (name) DO UPDATE SET
                            config_data = $2,
                            version = $3,
                            updated_at = $5,
                            tags = $6,
                            description = $7
                    """, config.name, json.dumps(config_dict), metadata.version,
                        metadata.created_at, metadata.updated_at, metadata.tags, metadata.description)
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        INSERT INTO {self.pipeline_configs_table} 
                        (name, config_data, version, created_at, updated_at, tags, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            config_data = VALUES(config_data),
                            version = VALUES(version),
                            updated_at = VALUES(updated_at),
                            tags = VALUES(tags),
                            description = VALUES(description)
                    """, (config.name, json.dumps(config_dict), metadata.version,
                         metadata.created_at, metadata.updated_at, json.dumps(metadata.tags), metadata.description))
                    await cursor.close()
            
            return True
        except Exception as e:
            print(f"Failed to save pipeline config {config.name}: {e}")
            return False
    
    async def load_pipeline_config(self, name: str) -> Optional[PipelineJobConfig]:
        """Load a pipeline configuration from database"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"""
                        SELECT config_data FROM {self.pipeline_configs_table} 
                        WHERE name = $1
                    """, name)
                    
                    if row:
                        config_dict = json.loads(row['config_data'])
                        return ConfigLoader.load_from_dict(config_dict)
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        SELECT config_data FROM {self.pipeline_configs_table} 
                        WHERE name = %s
                    """, (name,))
                    row = await cursor.fetchone()
                    await cursor.close()
                    
                    if row:
                        config_dict = json.loads(row[0])
                        return ConfigLoader.load_from_dict(config_dict)
            
            return None
        except Exception as e:
            print(f"Failed to load pipeline config {name}: {e}")
            return None
    
    async def list_pipeline_configs(self, tags: List[str] = None) -> List[ConfigMetadata]:
        """List all pipeline configurations"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                query = f"""
                    SELECT name, version, created_at, updated_at, tags, description
                    FROM {self.pipeline_configs_table}
                """
                
                if tags:
                    # PostgreSQL array overlap operator
                    query += " WHERE tags && $1"
                    params = [tags]
                else:
                    params = []
                
                async with pool.acquire() as conn:
                    rows = await conn.fetch(query, *params)
                    
                    return [
                        ConfigMetadata(
                            name=row['name'],
                            version=row['version'],
                            created_at=row['created_at'],
                            updated_at=row['updated_at'],
                            tags=row['tags'] or [],
                            description=row['description']
                        )
                        for row in rows
                    ]
            
            else:  # MySQL
                query = f"""
                    SELECT name, version, created_at, updated_at, tags, description
                    FROM {self.pipeline_configs_table}
                """
                
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(query)
                    rows = await cursor.fetchall()
                    await cursor.close()
                    
                    result = []
                    for row in rows:
                        row_tags = json.loads(row[4]) if row[4] else []
                        
                        # Filter by tags if specified
                        if tags and not any(tag in row_tags for tag in tags):
                            continue
                        
                        result.append(ConfigMetadata(
                            name=row[0],
                            version=row[1],
                            created_at=row[2],
                            updated_at=row[3],
                            tags=row_tags,
                            description=row[5]
                        ))
                    
                    return result
        
        except Exception as e:
            print(f"Failed to list pipeline configs: {e}")
            return []
    
    async def delete_pipeline_config(self, name: str) -> bool:
        """Delete a pipeline configuration"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    result = await conn.execute(f"""
                        DELETE FROM {self.pipeline_configs_table} WHERE name = $1
                    """, name)
                    
                    return result == "DELETE 1"
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        DELETE FROM {self.pipeline_configs_table} WHERE name = %s
                    """, (name,))
                    affected = cursor.rowcount
                    await cursor.close()
                    
                    return affected > 0
        
        except Exception as e:
            print(f"Failed to delete pipeline config {name}: {e}")
            return False
    
    async def save_datastores_config(self, datastores: DataStorage, metadata: ConfigMetadata = None) -> bool:
        """Save datastores configuration"""
        try:
            pool = await self._get_connection_pool()
            datastores_dict = ConfigSerializer.datastores_to_dict(datastores)
            
            if not metadata:
                metadata = ConfigMetadata(name="datastores")
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    # Clear existing datastores config
                    await conn.execute(f"DELETE FROM {self.datastores_table}")
                    
                    # Insert new config
                    await conn.execute(f"""
                        INSERT INTO {self.datastores_table} 
                        (config_data, version, created_at, updated_at, description)
                        VALUES ($1, $2, $3, $4, $5)
                    """, json.dumps(datastores_dict), metadata.version,
                        metadata.created_at, metadata.updated_at, metadata.description)
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    # Clear existing datastores config
                    await cursor.execute(f"DELETE FROM {self.datastores_table}")
                    
                    # Insert new config
                    await cursor.execute(f"""
                        INSERT INTO {self.datastores_table} 
                        (config_data, version, created_at, updated_at, description)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (json.dumps(datastores_dict), metadata.version,
                         metadata.created_at, metadata.updated_at, metadata.description))
                    await cursor.close()
            
            return True
        except Exception as e:
            print(f"Failed to save datastores config: {e}")
            return False
    
    async def load_datastores_config(self) -> Optional[DataStorage]:
        """Load datastores configuration"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"""
                        SELECT config_data FROM {self.datastores_table} 
                        ORDER BY created_at DESC LIMIT 1
                    """)
                    
                    if row:
                        config_dict = json.loads(row['config_data'])
                        return ConfigLoader.load_datastores_from_dict(config_dict)
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        SELECT config_data FROM {self.datastores_table} 
                        ORDER BY created_at DESC LIMIT 1
                    """)
                    row = await cursor.fetchone()
                    await cursor.close()
                    
                    if row:
                        config_dict = json.loads(row[0])
                        return ConfigLoader.load_datastores_from_dict(config_dict)
            
            return None
        except Exception as e:
            print(f"Failed to load datastores config: {e}")
            return None
    
    async def config_exists(self, name: str) -> bool:
        """Check if a configuration exists"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"""
                        SELECT 1 FROM {self.pipeline_configs_table} WHERE name = $1
                    """, name)
                    
                    return row is not None
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        SELECT 1 FROM {self.pipeline_configs_table} WHERE name = %s
                    """, (name,))
                    row = await cursor.fetchone()
                    await cursor.close()
                    
                    return row is not None
        
        except Exception as e:
            print(f"Failed to check config existence {name}: {e}")
            return False
    
    async def get_config_metadata(self, name: str) -> Optional[ConfigMetadata]:
        """Get metadata for a configuration"""
        try:
            pool = await self._get_connection_pool()
            
            if self._db_type == 'postgres':
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(f"""
                        SELECT name, version, created_at, updated_at, tags, description
                        FROM {self.pipeline_configs_table} WHERE name = $1
                    """, name)
                    
                    if row:
                        return ConfigMetadata(
                            name=row['name'],
                            version=row['version'],
                            created_at=row['created_at'],
                            updated_at=row['updated_at'],
                            tags=row['tags'] or [],
                            description=row['description']
                        )
            
            else:  # MySQL
                async with pool.acquire() as conn:
                    cursor = await conn.cursor()
                    await cursor.execute(f"""
                        SELECT name, version, created_at, updated_at, tags, description
                        FROM {self.pipeline_configs_table} WHERE name = %s
                    """, (name,))
                    row = await cursor.fetchone()
                    await cursor.close()
                    
                    if row:
                        return ConfigMetadata(
                            name=row[0],
                            version=row[1],
                            created_at=row[2],
                            updated_at=row[3],
                            tags=json.loads(row[4]) if row[4] else [],
                            description=row[5]
                        )
            
            return None
        except Exception as e:
            print(f"Failed to get config metadata {name}: {e}")
            return None
    
    async def close(self):
        """Close database connection pool"""
        if self._pool:
            if self._db_type == 'postgres':
                await self._pool.close()
            else:  # MySQL
                self._pool.close()
                await self._pool.wait_closed()
            self._pool = None
