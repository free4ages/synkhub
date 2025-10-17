"""
Test cases for DDL generation across different datastores.

Tests the schema_manager.py functionality for PostgreSQL, MySQL, and StarRocks.
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from synctool.utils.schema_manager import SchemaManager
from synctool.core.models import Column, DataStore, ConnectionConfig
from synctool.core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType
from synctool.datastore.postgres_datastore import PostgresDatastore
from synctool.datastore.mysql_datastore import MySQLDatastore
from synctool.datastore.starrocks_datastore import StarRocksDatastore


# Fixtures for test columns
def get_test_columns():
    """Get test columns for schema testing"""
    return [
        Column(
            name='id',
            expr='id',
            data_type=UniversalDataType.INTEGER,
            unique_column=True,
            hash_column=False
        ),
        Column(
            name='user_name',
            expr='user_name',
            data_type=UniversalDataType.VARCHAR,
            max_length=100,
            unique_column=False,
            hash_column=True
        ),
        Column(
            name='created_at',
            expr='created_at',
            data_type=UniversalDataType.TIMESTAMP,
            unique_column=False,
            hash_column=True
        ),
        Column(
            name='amount',
            expr='amount',
            data_type=UniversalDataType.DECIMAL,
            precision=10,
            scale=2,
            unique_column=False,
            hash_column=True
        )
    ]


def get_existing_schema_postgres():
    """Get mock existing schema from PostgreSQL"""
    return UniversalSchema(
        table_name='test_table',
        schema_name='public',
        database_name='testdb',
        columns=[
            UniversalColumn(
                name='id',
                data_type=UniversalDataType.INTEGER,
                nullable=False,
                primary_key=True
            ),
            UniversalColumn(
                name='user_name',
                data_type=UniversalDataType.VARCHAR,
                nullable=True,
                primary_key=False
            ),
            UniversalColumn(
                name='old_column',
                data_type=UniversalDataType.TEXT,
                nullable=True,
                primary_key=False
            )
        ],
        primary_keys=['id']
    )


class TestPostgresDatastoreDDL:
    """Test DDL generation for PostgreSQL"""
    
    def test_map_universal_type_to_postgres(self):
        """Test type mapping from universal to PostgreSQL types"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_universal_type_to_target(UniversalDataType.INTEGER) == 'INTEGER'
        assert datastore.map_universal_type_to_target(UniversalDataType.BIGINT) == 'BIGINT'
        assert datastore.map_universal_type_to_target(UniversalDataType.VARCHAR) == 'VARCHAR(255)'
        assert datastore.map_universal_type_to_target(UniversalDataType.TIMESTAMP) == 'TIMESTAMP'
        assert datastore.map_universal_type_to_target(UniversalDataType.DATETIME) == 'TIMESTAMP'  # Both map to TIMESTAMP
        assert datastore.map_universal_type_to_target(UniversalDataType.BOOLEAN) == 'BOOLEAN'
        assert datastore.map_universal_type_to_target(UniversalDataType.JSON) == 'JSONB'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID) == 'UUID'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT) == 'CHAR(32)'  # UUID without dashes
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT_DASH) == 'CHAR(36)'  # UUID with dashes
    
    def test_column_type_with_varchar_length(self):
        """Test VARCHAR with custom length"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        col = UniversalColumn(name='email', data_type=UniversalDataType.VARCHAR, max_length=100)
        assert datastore._get_column_type_with_params(col) == 'VARCHAR(100)'
        
        col = UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR, max_length=50)
        assert datastore._get_column_type_with_params(col) == 'VARCHAR(50)'
        
        # Default length
        col = UniversalColumn(name='text', data_type=UniversalDataType.VARCHAR)
        assert datastore._get_column_type_with_params(col) == 'VARCHAR(255)'
    
    def test_column_type_with_decimal_precision_scale(self):
        """Test DECIMAL with precision and scale"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        # With precision and scale
        col = UniversalColumn(name='price', data_type=UniversalDataType.DECIMAL, precision=10, scale=2)
        assert datastore._get_column_type_with_params(col) == 'NUMERIC(10,2)'
        
        # With only precision
        col = UniversalColumn(name='count', data_type=UniversalDataType.DECIMAL, precision=8)
        assert datastore._get_column_type_with_params(col) == 'NUMERIC(8)'
        
        # Without precision or scale
        col = UniversalColumn(name='value', data_type=UniversalDataType.DECIMAL)
        assert datastore._get_column_type_with_params(col) == 'NUMERIC'
    
    def test_column_type_with_char_length(self):
        """Test CHAR with custom length"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        col = UniversalColumn(name='code', data_type=UniversalDataType.CHAR, max_length=2)
        assert datastore._get_column_type_with_params(col) == 'CHAR(2)'
        
        # Default length
        col = UniversalColumn(name='flag', data_type=UniversalDataType.CHAR)
        assert datastore._get_column_type_with_params(col) == 'CHAR(1)'
    
    def test_map_postgres_type_to_universal(self):
        """Test type mapping from PostgreSQL to universal types"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_source_type_to_universal('integer') == UniversalDataType.INTEGER
        assert datastore.map_source_type_to_universal('bigint') == UniversalDataType.BIGINT
        assert datastore.map_source_type_to_universal('character varying') == UniversalDataType.VARCHAR
        assert datastore.map_source_type_to_universal('timestamp without time zone') == UniversalDataType.TIMESTAMP
        assert datastore.map_source_type_to_universal('timestamp with time zone') == UniversalDataType.TIMESTAMP
        assert datastore.map_source_type_to_universal('boolean') == UniversalDataType.BOOLEAN
        assert datastore.map_source_type_to_universal('jsonb') == UniversalDataType.JSON
        assert datastore.map_source_type_to_universal('uuid') == UniversalDataType.UUID
    
    def test_generate_create_table_ddl_postgres(self):
        """Test CREATE TABLE DDL generation for PostgreSQL"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        schema = UniversalSchema(
            table_name='users',
            schema_name='public',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, nullable=False, primary_key=True),
                UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR, nullable=False),
                UniversalColumn(name='email', data_type=UniversalDataType.VARCHAR, nullable=True),
                UniversalColumn(name='created_at', data_type=UniversalDataType.TIMESTAMP, nullable=True)
            ],
            primary_keys=['id']
        )
        
        ddl = datastore.generate_create_table_ddl(schema)
        
        assert 'CREATE TABLE public.users' in ddl
        assert 'id INTEGER NOT NULL' in ddl
        assert 'name VARCHAR(255) NOT NULL' in ddl
        assert 'email VARCHAR(255)' in ddl
        assert 'created_at TIMESTAMP' in ddl
        assert 'PRIMARY KEY (id)' in ddl
    
    def test_generate_create_table_ddl_with_if_not_exists(self):
        """Test CREATE TABLE IF NOT EXISTS for PostgreSQL"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        schema = UniversalSchema(
            table_name='users',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, nullable=False, primary_key=True)
            ],
            primary_keys=['id']
        )
        
        ddl = datastore.generate_create_table_ddl(schema, if_not_exists=True)
        
        assert 'CREATE TABLE IF NOT EXISTS users' in ddl
    
    def test_generate_alter_table_ddl_postgres(self):
        """Test ALTER TABLE DDL generation for PostgreSQL"""
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        changes = [
            {
                'type': 'add_column',
                'column': UniversalColumn(name='new_col', data_type=UniversalDataType.VARCHAR, nullable=True)
            },
            {
                'type': 'modify_column',
                'column': UniversalColumn(name='existing_col', data_type=UniversalDataType.INTEGER, nullable=False)
            },
            {
                'type': 'drop_column',
                'column_name': 'old_col'
            }
        ]
        
        ddl_statements = datastore.generate_alter_table_ddl('users', changes, 'public')
        
        assert len(ddl_statements) == 4  # add, modify type, set not null, drop
        assert 'ALTER TABLE public.users ADD COLUMN new_col VARCHAR(255)' in ddl_statements[0]
        assert 'ALTER TABLE public.users ALTER COLUMN existing_col TYPE INTEGER' in ddl_statements[1]
        assert 'ALTER TABLE public.users ALTER COLUMN existing_col SET NOT NULL' in ddl_statements[2]
        assert 'ALTER TABLE public.users DROP COLUMN old_col' in ddl_statements[3]


class TestMySQLDatastoreDDL:
    """Test DDL generation for MySQL"""
    
    def test_map_universal_type_to_mysql(self):
        """Test type mapping from universal to MySQL types"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_universal_type_to_target(UniversalDataType.INTEGER) == 'INT'
        assert datastore.map_universal_type_to_target(UniversalDataType.BIGINT) == 'BIGINT'
        assert datastore.map_universal_type_to_target(UniversalDataType.VARCHAR) == 'VARCHAR(255)'
        assert datastore.map_universal_type_to_target(UniversalDataType.TIMESTAMP) == 'TIMESTAMP'
        assert datastore.map_universal_type_to_target(UniversalDataType.DATETIME) == 'DATETIME'
        assert datastore.map_universal_type_to_target(UniversalDataType.BOOLEAN) == 'BOOLEAN'
        assert datastore.map_universal_type_to_target(UniversalDataType.JSON) == 'JSON'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID) == 'CHAR(36)'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT) == 'CHAR(32)'  # UUID without dashes
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT_DASH) == 'CHAR(36)'  # UUID with dashes
    
    def test_column_type_with_precision_scale_mysql(self):
        """Test MySQL DECIMAL with precision and scale"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost'))
        
        # With precision and scale
        col = UniversalColumn(name='price', data_type=UniversalDataType.DECIMAL, precision=12, scale=4)
        assert datastore._get_column_type_with_params(col) == 'DECIMAL(12,4)'
        
        # With only precision
        col = UniversalColumn(name='count', data_type=UniversalDataType.DECIMAL, precision=15)
        assert datastore._get_column_type_with_params(col) == 'DECIMAL(15)'
        
        # Without precision or scale (uses MySQL default)
        col = UniversalColumn(name='value', data_type=UniversalDataType.DECIMAL)
        assert datastore._get_column_type_with_params(col) == 'DECIMAL(10,2)'
    
    def test_column_type_with_varchar_length_mysql(self):
        """Test MySQL VARCHAR with custom length"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost'))
        
        col = UniversalColumn(name='email', data_type=UniversalDataType.VARCHAR, max_length=200)
        assert datastore._get_column_type_with_params(col) == 'VARCHAR(200)'
        
        col = UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR)
        assert datastore._get_column_type_with_params(col) == 'VARCHAR(255)'
    
    def test_map_mysql_type_to_universal(self):
        """Test type mapping from MySQL to universal types"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_source_type_to_universal('int') == UniversalDataType.INTEGER
        assert datastore.map_source_type_to_universal('bigint') == UniversalDataType.BIGINT
        assert datastore.map_source_type_to_universal('varchar') == UniversalDataType.VARCHAR
        assert datastore.map_source_type_to_universal('timestamp') == UniversalDataType.TIMESTAMP
        assert datastore.map_source_type_to_universal('datetime') == UniversalDataType.DATETIME
        assert datastore.map_source_type_to_universal('tinyint') == UniversalDataType.SMALLINT
        assert datastore.map_source_type_to_universal('json') == UniversalDataType.JSON
    
    def test_generate_create_table_ddl_mysql(self):
        """Test CREATE TABLE DDL generation for MySQL"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost', database='testdb'))
        
        schema = UniversalSchema(
            table_name='users',
            database_name='testdb',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, nullable=False, primary_key=True, auto_increment=True),
                UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR, nullable=False),
                UniversalColumn(name='created_at', data_type=UniversalDataType.DATETIME, nullable=True)
            ],
            primary_keys=['id']
        )
        
        ddl = datastore.generate_create_table_ddl(schema)
        
        assert 'CREATE TABLE `testdb`.`users`' in ddl
        assert '`id` INT NOT NULL AUTO_INCREMENT' in ddl
        assert '`name` VARCHAR(255) NOT NULL' in ddl
        assert '`created_at` DATETIME' in ddl
        assert 'PRIMARY KEY (`id`)' in ddl
    
    def test_generate_alter_table_ddl_mysql(self):
        """Test ALTER TABLE DDL generation for MySQL"""
        datastore = MySQLDatastore('test', ConnectionConfig(host='localhost'))
        
        changes = [
            {
                'type': 'add_column',
                'column': UniversalColumn(name='new_col', data_type=UniversalDataType.VARCHAR, nullable=True)
            },
            {
                'type': 'modify_column',
                'column': UniversalColumn(name='existing_col', data_type=UniversalDataType.INTEGER, nullable=False)
            }
        ]
        
        ddl_statements = datastore.generate_alter_table_ddl('users', changes, 'testdb')
        
        assert len(ddl_statements) == 2
        assert 'ALTER TABLE `testdb`.`users` ADD COLUMN `new_col` VARCHAR(255)' in ddl_statements[0]
        assert 'ALTER TABLE `testdb`.`users` MODIFY COLUMN `existing_col` INT NOT NULL' in ddl_statements[1]


class TestStarRocksDatastoreDDL:
    """Test DDL generation for StarRocks"""
    
    def test_map_universal_type_to_starrocks(self):
        """Test type mapping from universal to StarRocks types"""
        datastore = StarRocksDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_universal_type_to_target(UniversalDataType.INTEGER) == 'INT'
        assert datastore.map_universal_type_to_target(UniversalDataType.BIGINT) == 'BIGINT'
        assert datastore.map_universal_type_to_target(UniversalDataType.VARCHAR) == 'VARCHAR(255)'
        assert datastore.map_universal_type_to_target(UniversalDataType.TEXT) == 'STRING'  # StarRocks uses STRING
        assert datastore.map_universal_type_to_target(UniversalDataType.TIMESTAMP) == 'DATETIME'
        assert datastore.map_universal_type_to_target(UniversalDataType.DATETIME) == 'DATETIME'
        assert datastore.map_universal_type_to_target(UniversalDataType.JSON) == 'JSON'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID) == 'CHAR(36)'
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT) == 'CHAR(32)'  # UUID without dashes
        assert datastore.map_universal_type_to_target(UniversalDataType.UUID_TEXT_DASH) == 'CHAR(36)'  # UUID with dashes
    
    def test_map_starrocks_type_to_universal(self):
        """Test type mapping from StarRocks to universal types"""
        datastore = StarRocksDatastore('test', ConnectionConfig(host='localhost'))
        
        assert datastore.map_source_type_to_universal('int') == UniversalDataType.INTEGER
        assert datastore.map_source_type_to_universal('bigint') == UniversalDataType.BIGINT
        assert datastore.map_source_type_to_universal('varchar') == UniversalDataType.VARCHAR
        assert datastore.map_source_type_to_universal('string') == UniversalDataType.TEXT
        assert datastore.map_source_type_to_universal('datetime') == UniversalDataType.DATETIME
        assert datastore.map_source_type_to_universal('largeint') == UniversalDataType.BIGINT
        assert datastore.map_source_type_to_universal('json') == UniversalDataType.JSON
    
    def test_generate_create_table_ddl_starrocks(self):
        """Test CREATE TABLE DDL generation for StarRocks"""
        datastore = StarRocksDatastore('test', ConnectionConfig(host='localhost', database='testdb'))
        
        schema = UniversalSchema(
            table_name='users',
            database_name='testdb',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, nullable=False, primary_key=True),
                UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR, nullable=False),
                UniversalColumn(name='created_at', data_type=UniversalDataType.DATETIME, nullable=True)
            ],
            primary_keys=['id']
        )
        
        ddl = datastore.generate_create_table_ddl(schema)
        
        assert 'CREATE TABLE `testdb`.`users`' in ddl
        assert '`id` INT NOT NULL' in ddl
        assert '`name` VARCHAR(255) NOT NULL' in ddl
        assert '`created_at` DATETIME' in ddl
        assert 'PRIMARY KEY (`id`)' in ddl
        assert 'ENGINE=OLAP' in ddl
        assert 'DISTRIBUTED BY HASH(`id`) BUCKETS 10' in ddl


class TestSchemaManager:
    """Test SchemaManager utility class"""
    
    def test_virtual_columns_excluded(self):
        """Test that virtual columns are excluded from DDL generation"""
        manager = SchemaManager()
        
        columns = [
            Column(
                name='id',
                expr='id',
                data_type=UniversalDataType.INTEGER,
                unique_column=True,
                hash_column=False,
                virtual=False
            ),
            Column(
                name='hash_value',
                expr='hash_value',
                data_type=UniversalDataType.BIGINT,
                unique_column=False,
                hash_column=True,
                hash_key=True,
                virtual=True  # This should be excluded
            ),
            Column(
                name='name',
                expr='name',
                data_type=UniversalDataType.VARCHAR,
                unique_column=False,
                hash_column=True,
                virtual=False
            )
        ]
        
        schema = manager.columns_to_universal_schema(
            columns=columns,
            table_name='test_table'
        )
        
        # Should only have 2 columns (virtual one excluded)
        assert len(schema.columns) == 2
        column_names = [col.name for col in schema.columns]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'hash_value' not in column_names  # Virtual column excluded
    
    def test_column_expr_used_for_name(self):
        """Test that column.expr is used as the actual column name in DDL"""
        manager = SchemaManager()
        
        columns = [
            Column(
                name='order_id',  # Alias name
                expr='id',        # Actual database column name
                data_type=UniversalDataType.INTEGER,
                unique_column=True,
                hash_column=False
            ),
            Column(
                name='user_email',  # Alias name
                expr='email',       # Actual database column name
                data_type=UniversalDataType.VARCHAR,
                max_length=100,
                unique_column=False,
                hash_column=True
            ),
            Column(
                name='price',  # No expr - should use name
                expr='price',
                data_type=UniversalDataType.DECIMAL,
                precision=10,
                scale=2,
                unique_column=False,
                hash_column=True
            )
        ]
        
        schema = manager.columns_to_universal_schema(
            columns=columns,
            table_name='orders'
        )
        
        # Check that expr is used as column name
        column_names = [col.name for col in schema.columns]
        assert 'id' in column_names      # Should use expr 'id', not name 'order_id'
        assert 'email' in column_names   # Should use expr 'email', not name 'user_email'
        assert 'price' in column_names   # Should use expr 'price'
        
        assert 'order_id' not in column_names  # Alias not used
        assert 'user_email' not in column_names  # Alias not used
        
        # Check primary key uses expr name
        assert schema.primary_keys == ['id']  # Should use expr, not 'order_id'
    
    def test_columns_to_universal_schema(self):
        """Test conversion from config columns to UniversalSchema"""
        manager = SchemaManager()
        columns = get_test_columns()
        
        schema = manager.columns_to_universal_schema(
            columns=columns,
            table_name='test_table',
            schema_name='public',
            database_name='testdb'
        )
        
        assert schema.table_name == 'test_table'
        assert schema.schema_name == 'public'
        assert schema.database_name == 'testdb'
        assert len(schema.columns) == 4
        assert schema.primary_keys == ['id']  # unique_column=True becomes primary key
        
        # Check column conversion
        id_col = next(c for c in schema.columns if c.name == 'id')
        assert id_col.data_type == UniversalDataType.INTEGER
        assert id_col.primary_key is True
        assert id_col.nullable is False
        
        # Check VARCHAR with max_length
        user_name_col = next(c for c in schema.columns if c.name == 'user_name')
        assert user_name_col.data_type == UniversalDataType.VARCHAR
        assert user_name_col.max_length == 100
        
        # Check DECIMAL with precision and scale
        amount_col = next(c for c in schema.columns if c.name == 'amount')
        assert amount_col.data_type == UniversalDataType.DECIMAL
        assert amount_col.precision == 10
        assert amount_col.scale == 2
    
    def test_columns_with_precision_scale_to_ddl(self):
        """Test full flow: columns with precision/scale to DDL generation"""
        manager = SchemaManager()
        
        # Create columns with specific precision/scale/length
        columns = [
            Column(
                name='id',
                expr='id',
                data_type=UniversalDataType.INTEGER,
                unique_column=True,
                hash_column=False
            ),
            Column(
                name='price',
                expr='price',
                data_type=UniversalDataType.DECIMAL,
                precision=15,
                scale=4,
                unique_column=False,
                hash_column=False
            ),
            Column(
                name='email',
                expr='email',
                data_type=UniversalDataType.VARCHAR,
                max_length=150,
                unique_column=False,
                hash_column=False
            )
        ]
        
        # Convert to universal schema
        schema = manager.columns_to_universal_schema(
            columns=columns,
            table_name='products',
            schema_name='public'
        )
        
        # Verify precision/scale/length are preserved
        price_col = next(c for c in schema.columns if c.name == 'price')
        assert price_col.precision == 15
        assert price_col.scale == 4
        
        email_col = next(c for c in schema.columns if c.name == 'email')
        assert email_col.max_length == 150
        
        # Generate DDL for PostgreSQL
        from synctool.datastore.postgres_datastore import PostgresDatastore
        from synctool.core.models import ConnectionConfig
        
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        ddl = datastore.generate_create_table_ddl(schema)
        
        # Verify DDL contains correct types
        assert 'NUMERIC(15,4)' in ddl
        assert 'VARCHAR(150)' in ddl
    
    def test_compare_schemas_no_existing_table(self):
        """Test schema comparison when table doesn't exist"""
        manager = SchemaManager()
        columns = get_test_columns()
        
        desired = manager.columns_to_universal_schema(columns, 'test_table')
        comparison = manager._compare_schemas(None, desired)
        
        assert comparison['action'] == 'create'
        assert len(comparison['changes']) == 0
    
    def test_compare_schemas_no_changes(self):
        """Test schema comparison when schemas match"""
        manager = SchemaManager()
        
        existing = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, primary_key=True, nullable=False)
            ],
            primary_keys=['id']
        )
        
        desired = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, primary_key=True, nullable=False)
            ],
            primary_keys=['id']
        )
        
        comparison = manager._compare_schemas(existing, desired)
        
        assert comparison['action'] == 'no_change'
        assert len(comparison['changes']) == 0
    
    def test_compare_schemas_add_column(self):
        """Test schema comparison detecting new columns"""
        manager = SchemaManager()
        
        existing = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, primary_key=True, nullable=False)
            ],
            primary_keys=['id']
        )
        
        desired = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, primary_key=True, nullable=False),
                UniversalColumn(name='name', data_type=UniversalDataType.VARCHAR, nullable=True)
            ],
            primary_keys=['id']
        )
        
        comparison = manager._compare_schemas(existing, desired)
        
        assert comparison['action'] == 'alter'
        assert len(comparison['changes']) == 1
        assert comparison['changes'][0]['type'] == 'add_column'
        assert comparison['changes'][0]['column'].name == 'name'
    
    def test_compare_schemas_with_datastore_impl(self):
        """Test schema comparison with datastore for accurate type comparison"""
        manager = SchemaManager()
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        # Existing has TIMESTAMP, desired has DATETIME
        # Both map to PostgreSQL TIMESTAMP, so should be no change
        existing = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='created_at', data_type=UniversalDataType.TIMESTAMP, nullable=True)
            ],
            primary_keys=[]
        )
        
        desired = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='created_at', data_type=UniversalDataType.DATETIME, nullable=True)
            ],
            primary_keys=[]
        )
        
        # Without datastore impl - would flag as different
        comparison_without = manager._compare_schemas(existing, desired, datastore_impl=None)
        assert comparison_without['action'] == 'alter'
        assert len(comparison_without['changes']) == 1
        
        # With datastore impl - should recognize they're the same in PostgreSQL
        comparison_with = manager._compare_schemas(existing, desired, datastore_impl=datastore)
        assert comparison_with['action'] == 'no_change'
        assert len(comparison_with['changes']) == 0
    
    def test_compare_schemas_real_type_change(self):
        """Test schema comparison detecting real type differences"""
        manager = SchemaManager()
        datastore = PostgresDatastore('test', ConnectionConfig(host='localhost'))
        
        # BIGINT vs INTEGER - these are different types
        existing = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='amount', data_type=UniversalDataType.BIGINT, nullable=True)
            ],
            primary_keys=[]
        )
        
        desired = UniversalSchema(
            table_name='test',
            columns=[
                UniversalColumn(name='amount', data_type=UniversalDataType.INTEGER, nullable=True)
            ],
            primary_keys=[]
        )
        
        comparison = manager._compare_schemas(existing, desired, datastore_impl=datastore)
        
        assert comparison['action'] == 'alter'
        assert len(comparison['changes']) == 1
        assert comparison['changes'][0]['type'] == 'modify_column'
        assert 'BIGINT' in comparison['changes'][0]['description']
        assert 'INTEGER' in comparison['changes'][0]['description']
    
    @pytest.mark.asyncio
    async def test_ensure_table_schema_create(self):
        """Test ensure_table_schema when table needs to be created"""
        manager = SchemaManager()
        
        # Mock datastore
        mock_datastore = Mock()
        mock_impl = Mock()
        mock_datastore._datastore_impl = mock_impl
        
        # Mock methods
        mock_impl.table_exists = AsyncMock(return_value=False)
        mock_impl.generate_create_table_ddl = Mock(return_value="CREATE TABLE test (id INT);")
        
        columns = [
            Column(name='id', expr='id', data_type=UniversalDataType.INTEGER, unique_column=True, hash_column=False)
        ]
        
        result = await manager.ensure_table_schema(
            datastore=mock_datastore,
            columns=columns,
            table_name='test_table',
            apply=False
        )
        
        assert result['action'] == 'create'
        assert result['table_exists'] is False
        assert len(result['ddl_statements']) == 1
        assert 'CREATE TABLE' in result['ddl_statements'][0]
        assert result['applied'] is False
    
    @pytest.mark.asyncio
    async def test_ensure_table_schema_no_changes(self):
        """Test ensure_table_schema when schema matches"""
        manager = SchemaManager()
        
        # Mock datastore
        mock_datastore = Mock()
        mock_impl = Mock()
        mock_datastore._datastore_impl = mock_impl
        
        existing_schema = UniversalSchema(
            table_name='test_table',
            columns=[
                UniversalColumn(name='id', data_type=UniversalDataType.INTEGER, primary_key=True, nullable=False)
            ],
            primary_keys=['id']
        )
        
        # Mock methods
        mock_impl.table_exists = AsyncMock(return_value=True)
        mock_impl.extract_table_schema = AsyncMock(return_value=existing_schema)
        mock_impl.map_universal_type_to_target = Mock(return_value='INTEGER')
        
        columns = [
            Column(name='id', expr='id', data_type=UniversalDataType.INTEGER, unique_column=True, hash_column=False)
        ]
        
        result = await manager.ensure_table_schema(
            datastore=mock_datastore,
            columns=columns,
            table_name='test_table',
            apply=False
        )
        
        assert result['action'] == 'no_change'
        assert result['table_exists'] is True
        assert len(result['ddl_statements']) == 0
        assert result['applied'] is False


# Integration test helpers
def create_mock_datastore(datastore_type='postgres'):
    """Create a mock DataStore for testing"""
    connection_config = ConnectionConfig(
        host='localhost',
        port=5432 if datastore_type == 'postgres' else 3306,
        user='test',
        password='test',
        database='testdb'
    )
    
    with patch('synctool.core.models.PostgresDatastore'):
        datastore = DataStore(
            name='test_datastore',
            type=datastore_type,
            connection=connection_config
        )
    
    return datastore


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

