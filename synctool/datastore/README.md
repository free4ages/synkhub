# Datastore Module

This module provides centralized database connection management with lazy driver loading for the synctool project.

## Features

- **Lazy Driver Loading**: Database drivers (asyncpg, aiomysql, aiohttp) are only loaded when `connect()` is called
- **Idempotent Operations**: `connect()` and `disconnect()` methods can be called multiple times safely
- **Thread-Safe**: Uses `asyncio.Lock()` to ensure connection operations are atomic
- **Raw Query Execution**: Each datastore provides a `execute_query()` method for raw SQL execution
- **Connection Pooling**: Automatic connection pool management for optimal performance
- **Type-Specific Features**: Each datastore type has specialized capabilities (e.g., StarRocks stream load)

## Supported Datastores

### PostgresDatastore
- **Driver**: asyncpg
- **Features**: Connection pooling, COPY operations, temporary table creation
- **Install**: `pip install asyncpg`

### MySQLDatastore  
- **Driver**: aiomysql
- **Features**: Connection pooling, batch operations, table introspection
- **Install**: `pip install aiomysql`

### StarRocksDatastore
- **Drivers**: aiomysql + aiohttp
- **Features**: MySQL protocol + HTTP API, stream load, session management
- **Install**: `pip install aiomysql aiohttp`

## Usage Examples

### Basic Usage

```python
from synctool.core.models import DataStore, ConnectionConfig

# Create datastore
postgres_config = ConnectionConfig(
    host="localhost",
    port=5432,
    user="postgres", 
    password="password",
    database="mydb"
)

datastore = DataStore(
    name="main_db",
    type="postgres",
    connection=postgres_config
)

# Connect (driver loaded lazily)
await datastore.connect(logger)

# Execute raw queries
result = await datastore.execute_query("SELECT * FROM users", action='select')
print(result)  # List of dictionaries

# Execute DML
rows_affected = await datastore.execute_query(
    "UPDATE users SET active = $1 WHERE id = $2",
    params=[True, 123],
    action='update'
)

# Disconnect
await datastore.disconnect(logger)
```

### Using DataStorage for Multiple Datastores

```python
from synctool.core.models import DataStorage, DataStore, ConnectionConfig

# Create storage collection
storage = DataStorage()

# Add multiple datastores
storage.add_datastore(DataStore("pg_main", "postgres", pg_config))
storage.add_datastore(DataStore("mysql_cache", "mysql", mysql_config))

# Connect all
async with storage:
    # Use individual datastores
    pg_store = storage.get_datastore("pg_main")
    result = await pg_store.execute_query("SELECT version()")
```

### StarRocks Advanced Features

```python
starrocks_store = DataStore("analytics", "starrocks", starrocks_config)
await starrocks_store.connect()

# MySQL protocol queries
result = await starrocks_store.execute_query("SELECT COUNT(*) FROM events")

# HTTP API queries (StarRocks specific)
starrocks_impl = starrocks_store._datastore_impl
http_result = await starrocks_impl.execute_http_query("SHOW TABLES")

# Stream load (bulk data loading)
data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
result = await starrocks_impl.stream_load_csv('users', data, ['id', 'name'])
```

## Architecture

```
DataStore (models.py)
├── _datastore_impl: BaseDatastore
│   ├── PostgresDatastore
│   ├── MySQLDatastore  
│   └── StarRocksDatastore
└── Capability management, connection delegation

BaseDatastore (Abstract)
├── Connection lifecycle management
├── Thread-safe operations
└── Raw query interface

Concrete Datastores
├── Driver-specific connection logic
├── Specialized features per database
└── Lazy driver loading
```

## Error Handling

The datastore system provides comprehensive error handling:

- **ImportError**: Raised when required drivers are not installed
- **RuntimeError**: Raised for connection failures or invalid operations  
- **Connection Errors**: Database-specific connection issues
- **Query Errors**: SQL execution failures

## Connection Configuration

All datastores use the `ConnectionConfig` class:

```python
@dataclass
class ConnectionConfig:
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    # Connection pool settings
    max_connections: int = 10
    min_connections: int = 1
    # StarRocks specific
    http_port: Optional[int] = None  # For StarRocks HTTP API
```

## Thread Safety

All datastore operations are thread-safe:
- Connection operations use `asyncio.Lock()`
- Connection pools handle concurrent access
- Session state is tracked per connection

## Performance Considerations

- **Connection Pooling**: All datastores use connection pools for optimal performance
- **Lazy Loading**: Drivers only loaded when needed, reducing memory footprint
- **Session Management**: StarRocks automatically manages session variables
- **Bulk Operations**: Specialized methods for bulk data loading (StarRocks stream load, PostgreSQL COPY)

## Testing

Each datastore can be tested independently:

```python
# Test if driver is available without connecting
try:
    datastore = DataStore("test", "postgres", config)
    # Driver import happens only on connect()
    await datastore.connect()
    print("PostgreSQL driver available")
except ImportError:
    print("PostgreSQL driver not installed")
```
