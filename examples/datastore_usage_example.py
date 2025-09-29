"""
Example demonstrating how to use the new datastore system with lazy driver loading.
"""
import asyncio
import logging
from synctool.core.models import DataStore, DataStorage, ConnectionConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def postgres_example():
    """Example using PostgreSQL datastore"""
    print("\n=== PostgreSQL Datastore Example ===")
    
    # Create PostgreSQL datastore
    postgres_config = ConnectionConfig(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        database="testdb",
        min_connections=2,
        max_connections=10
    )
    
    postgres_store = DataStore(
        name="main_postgres",
        type="postgres",
        connection=postgres_config,
        description="Main PostgreSQL database"
    )
    
    try:
        # Connect (driver loaded lazily)
        await postgres_store.connect(logger)
        
        # Execute raw queries
        result = await postgres_store.execute_query("SELECT version()", action='select')
        print(f"PostgreSQL Version: {result[0] if result else 'Unknown'}")
        
        # Test async connection pool access
        connection_pool = await postgres_store.get_connection_pool()
        print(f"Connection pool type: {type(connection_pool).__name__}")
        
        # Execute DML query
        await postgres_store.execute_query(
            "CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR(100))",
            action='create'
        )
        
        # Insert data
        rows_affected = await postgres_store.execute_query(
            "INSERT INTO test_table (name) VALUES ($1), ($2)",
            params=['Alice', 'Bob'],
            action='insert'
        )
        print(f"Inserted {rows_affected} rows")
        
        # Query data
        data = await postgres_store.execute_query(
            "SELECT * FROM test_table ORDER BY id",
            action='select'
        )
        print(f"Retrieved data: {data}")
        
    except ImportError as e:
        print(f"Driver not available: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await postgres_store.disconnect(logger)

async def mysql_example():
    """Example using MySQL datastore"""
    print("\n=== MySQL Datastore Example ===")
    
    # Create MySQL datastore
    mysql_config = ConnectionConfig(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="testdb",
        min_connections=2,
        max_connections=10
    )
    
    mysql_store = DataStore(
        name="main_mysql",
        type="mysql",
        connection=mysql_config,
        description="Main MySQL database"
    )
    
    try:
        # Connect (driver loaded lazily)
        await mysql_store.connect(logger)
        
        # Execute raw queries
        result = await mysql_store.execute_query("SELECT VERSION()", action='select')
        print(f"MySQL Version: {result[0] if result else 'Unknown'}")
        
        # Check connection status
        print(f"MySQL connected: {mysql_store.is_connected}")
        
    except ImportError as e:
        print(f"Driver not available: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await mysql_store.disconnect(logger)

async def starrocks_example():
    """Example using StarRocks datastore"""
    print("\n=== StarRocks Datastore Example ===")
    
    # Create StarRocks datastore
    starrocks_config = ConnectionConfig(
        host="localhost",
        port=9030,
        http_port=8030,
        user="root",
        password="",
        database="testdb",
        min_connections=5,
        max_connections=20
    )
    
    starrocks_store = DataStore(
        name="analytics_starrocks",
        type="starrocks",
        connection=starrocks_config,
        description="Analytics StarRocks database"
    )
    
    try:
        # Connect (drivers loaded lazily)
        await starrocks_store.connect(logger)
        
        # Execute MySQL protocol query
        result = await starrocks_store.execute_query("SELECT VERSION()", action='select')
        print(f"StarRocks Version: {result[0] if result else 'Unknown'}")
        
        # Execute HTTP query (if StarRocks implementation supports it)
        starrocks_impl = starrocks_store._datastore_impl
        if hasattr(starrocks_impl, 'execute_http_query'):
            http_result = await starrocks_impl.execute_http_query(
                "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES",
                action="query"
            )
            print(f"HTTP Query Result: {http_result}")
        
        # Stream load example (if data available)
        if hasattr(starrocks_impl, 'stream_load_csv'):
            sample_data = [
                {'id': 1, 'name': 'Alice', 'age': 30},
                {'id': 2, 'name': 'Bob', 'age': 25}
            ]
            # Note: This would require an existing table
            # result = await starrocks_impl.stream_load_csv('test_table', sample_data, ['id', 'name', 'age'])
            print("Stream load capability available")
        
    except ImportError as e:
        print(f"Driver not available: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await starrocks_store.disconnect(logger)

async def data_storage_example():
    """Example using DataStorage for managing multiple datastores"""
    print("\n=== DataStorage Management Example ===")
    
    # Create multiple datastores
    postgres_store = DataStore(
        name="postgres_main",
        type="postgres",
        connection=ConnectionConfig(host="localhost", port=5432, user="postgres", password="password", database="main")
    )
    
    mysql_store = DataStore(
        name="mysql_cache",
        type="mysql", 
        connection=ConnectionConfig(host="localhost", port=3306, user="root", password="password", database="cache")
    )
    
    # Create storage collection
    data_storage = DataStorage()
    data_storage.add_datastore(postgres_store)
    data_storage.add_datastore(mysql_store)
    
    print(f"Available datastores: {data_storage.list_datastores()}")
    print(f"PostgreSQL datastores: {[ds.name for ds in data_storage.get_datastores_by_type('postgres')]}")
    
    try:
        # Connect all at once
        await data_storage.connect_all(logger)
        
        # Use individual datastores
        pg_store = data_storage.get_datastore("postgres_main")
        if pg_store and pg_store.is_connected:
            result = await pg_store.execute_query("SELECT 'Hello from PostgreSQL'", action='select')
            print(f"PostgreSQL says: {result}")
        
        mysql_store_retrieved = data_storage.get_datastore("mysql_cache")
        if mysql_store_retrieved and mysql_store_retrieved.is_connected:
            result = await mysql_store_retrieved.execute_query("SELECT 'Hello from MySQL'", action='select')
            print(f"MySQL says: {result}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Disconnect all
        await data_storage.disconnect_all(logger)

async def main():
    """Run all examples"""
    print("=== Datastore System Examples ===")
    print("Note: These examples require actual database connections.")
    print("Drivers are loaded lazily only when connect() is called.")
    
    await postgres_example()
    await mysql_example() 
    await starrocks_example()
    await data_storage_example()

if __name__ == "__main__":
    asyncio.run(main())
