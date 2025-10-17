"""Test cases for ConfigSerializer - serialization and deserialization of pipeline configs."""

import pytest
from datetime import datetime
from synctool.config.config_serializer import ConfigSerializer
from synctool.config.config_loader import ConfigLoader
from synctool.core.models import (
    PipelineJobConfig, DataStorage, DataStore, ConnectionConfig,
    BackendConfig, Column, GlobalStageConfig, StrategyConfig,
    JoinConfig, FilterConfig
)
from synctool.core.enums import SyncStrategy, HashAlgo
from synctool.core.schema_models import UniversalDataType


class TestConfigSerializer:
    """Test suite for ConfigSerializer."""
    
    @pytest.fixture
    def sample_connection_config(self):
        """Create a sample ConnectionConfig."""
        return ConnectionConfig(
            host='localhost',
            port=5432,
            user='testuser',
            password='testpass',
            database='testdb',
            max_connections=5,
            min_connections=1
        )
    
    @pytest.fixture
    def sample_datastore(self, sample_connection_config):
        """Create a sample DataStore."""
        return DataStore(
            name='test_postgres',
            type='postgres',
            connection=sample_connection_config,
            description='Test PostgreSQL datastore',
            tags=['test', 'postgres']
        )
    
    @pytest.fixture
    def sample_data_storage(self, sample_datastore):
        """Create a sample DataStorage."""
        storage = DataStorage()
        storage.add_datastore(sample_datastore)
        return storage
    
    @pytest.fixture
    def sample_columns(self):
        """Create sample columns."""
        return [
            Column(
                expr='id',
                name='id',
                data_type=UniversalDataType.INTEGER,
                unique_column=True,
                hash_column=True
            ),
            Column(
                expr='name',
                name='name',
                data_type=UniversalDataType.VARCHAR,
                hash_column=True,
                max_length=255
            ),
            Column(
                expr='created_at',
                name='created_at',
                data_type=UniversalDataType.TIMESTAMP,
                order_column=True,
                hash_column=False
            ),
            Column(
                expr='price',
                name='price',
                data_type=UniversalDataType.DECIMAL,
                precision=10,
                scale=2,
                hash_column=True
            )
        ]
    
    @pytest.fixture
    def sample_backend_config(self, sample_columns):
        """Create a sample BackendConfig with db_columns."""
        return BackendConfig(
            type='postgres',
            datastore_name='test_postgres',
            name='source_backend',
            table='test_table',
            schema='public',
            alias='t',
            join=[
                JoinConfig(table='other_table', on='t.id = o.id', type='left', alias='o')
            ],
            filters=[
                FilterConfig(column='status', operator='=', value='active')
            ],
            group_by=['id', 'name'],
            columns=sample_columns,
            db_columns=sample_columns,  # Same as columns for this test
            supports_update=True
        )
    
    @pytest.fixture
    def sample_strategy_config(self):
        """Create a sample StrategyConfig."""
        return StrategyConfig(
            name='hash_strategy',
            type=SyncStrategy.HASH,
            enabled=True,
            default=True,
            use_pagination=True,
            page_size=1000,
            max_concurrent_partitions=2
        )
    
    @pytest.fixture
    def sample_stage_config(self, sample_backend_config, sample_strategy_config):
        """Create a sample GlobalStageConfig."""
        return GlobalStageConfig(
            name='sync_stage',
            type='sync',
            enabled=True,
            source=sample_backend_config,
            destination=sample_backend_config,
            strategies=[sample_strategy_config]
        )
    
    @pytest.fixture
    def sample_pipeline_config(self, sample_backend_config, sample_stage_config, sample_strategy_config, sample_columns):
        """Create a sample PipelineJobConfig."""
        return PipelineJobConfig(
            name='test_pipeline',
            description='Test pipeline configuration',
            sync_type='row-level',
            hash_algo=HashAlgo.HASH_MD5_HASH,
            backends=[sample_backend_config],
            columns=sample_columns,
            strategies=[sample_strategy_config],
            stages=[sample_stage_config],
            max_concurrent_partitions=2,
            enabled=True
        )
    
    def test_column_to_dict(self, sample_columns):
        """Test converting Column to dictionary."""
        column = sample_columns[0]  # id column
        result = ConfigSerializer._column_to_dict(column)
        
        assert result['name'] == 'id'
        assert result['expr'] == 'id'
        assert result['data_type'] == 'integer'
        assert result['unique_column'] is True
        assert result['hash_column'] is True
        
    def test_column_to_dict_with_precision_scale(self, sample_columns):
        """Test converting Column with precision/scale to dictionary."""
        column = sample_columns[3]  # price column with precision and scale
        result = ConfigSerializer._column_to_dict(column)
        
        assert result['name'] == 'price'
        assert result['expr'] == 'price'
        assert result['data_type'] == 'decimal'
        assert result['precision'] == 10
        assert result['scale'] == 2
    
    def test_backend_to_dict(self, sample_backend_config):
        """Test converting BackendConfig to dictionary."""
        result = ConfigSerializer._backend_to_dict(sample_backend_config)
        
        assert result['type'] == 'postgres'
        assert result['datastore_name'] == 'test_postgres'
        assert result['name'] == 'source_backend'
        assert result['table'] == 'test_table'
        assert result['schema'] == 'public'
        assert result['alias'] == 't'
        assert result['supports_update'] is True
        
        # Check joins
        assert len(result['join']) == 1
        assert result['join'][0]['table'] == 'other_table'
        
        # Check filters
        assert len(result['filters']) == 1
        assert result['filters'][0]['column'] == 'status'
        
        # Check group_by
        assert result['group_by'] == ['id', 'name']
        
        # Check columns and db_columns
        assert 'columns' in result
        assert len(result['columns']) == 4
        
        assert 'db_columns' in result
        assert len(result['db_columns']) == 4
        assert result['db_columns'][0]['name'] == 'id'
    
    def test_strategy_to_dict(self, sample_strategy_config):
        """Test converting StrategyConfig to dictionary."""
        result = ConfigSerializer._strategy_to_dict(sample_strategy_config)
        
        assert result['name'] == 'hash_strategy'
        assert result['type'] == 'hash'
        assert result['enabled'] is True
        assert result['default'] is True
        assert result['use_pagination'] is True
        # page_size = 1000 is the default, so it's not included
        assert 'page_size' not in result
        assert result['max_concurrent_partitions'] == 2
    
    def test_stage_to_dict(self, sample_stage_config):
        """Test converting GlobalStageConfig to dictionary."""
        result = ConfigSerializer._stage_to_dict(sample_stage_config)
        
        assert result['name'] == 'sync_stage'
        assert result['type'] == 'sync'
        assert result['enabled'] is True
        assert 'source' in result
        assert 'destination' in result
        assert 'strategies' in result
        # GlobalStageConfig doesn't have page_size or max_concurrent_partitions attributes
    
    def test_config_to_dict(self, sample_pipeline_config):
        """Test converting PipelineJobConfig to dictionary."""
        result = ConfigSerializer.config_to_dict(sample_pipeline_config)
        
        assert result['name'] == 'test_pipeline'
        assert result['description'] == 'Test pipeline configuration'
        assert result['sync_type'] == 'row-level'
        assert result['hash_algo'] == 'hash_md5_hash'
        assert result['max_concurrent_partitions'] == 2
        assert result['enabled'] is True
        
        # Check backends are included
        assert 'backends' in result
        assert len(result['backends']) == 1
        assert result['backends'][0]['name'] == 'source_backend'
        assert 'db_columns' in result['backends'][0]
        
        # Check stages
        assert 'stages' in result
        assert len(result['stages']) == 1
        
        # Check strategies
        assert 'strategies' in result
        assert len(result['strategies']) == 1
    
    def test_datastores_to_dict(self, sample_data_storage):
        """Test converting DataStorage to dictionary."""
        result = ConfigSerializer.datastores_to_dict(sample_data_storage)
        
        assert 'datastores' in result
        assert 'test_postgres' in result['datastores']
        
        ds = result['datastores']['test_postgres']
        assert ds['type'] == 'postgres'
        assert ds['description'] == 'Test PostgreSQL datastore'
        assert ds['tags'] == ['test', 'postgres']
        
        # Check connection
        assert 'connection' in ds
        assert ds['connection']['host'] == 'localhost'
        assert ds['connection']['port'] == 5432
        assert ds['connection']['user'] == 'testuser'
        assert ds['connection']['database'] == 'testdb'
    
    def test_config_to_json_dict(self, sample_pipeline_config, sample_data_storage):
        """Test converting config and datastores to JSON-serializable dict."""
        result = ConfigSerializer.config_to_json_dict(
            sample_pipeline_config,
            sample_data_storage
        )
        
        assert 'config' in result
        assert 'data_storage' in result
        assert 'timestamp' in result
        assert 'version' in result
        
        # Verify config
        assert result['config']['name'] == 'test_pipeline'
        assert 'backends' in result['config']
        
        # Verify data_storage
        assert 'test_postgres' in result['data_storage']['datastores']
    
    def test_json_dict_to_config_roundtrip(self, sample_pipeline_config, sample_data_storage):
        """Test round-trip: config -> dict -> config."""
        # Serialize
        json_dict = ConfigSerializer.config_to_json_dict(
            sample_pipeline_config,
            sample_data_storage
        )
        
        # Deserialize
        restored_config, restored_storage = ConfigSerializer.json_dict_to_config(json_dict)
        
        # Verify config
        assert restored_config.name == sample_pipeline_config.name
        assert restored_config.description == sample_pipeline_config.description
        assert restored_config.sync_type == sample_pipeline_config.sync_type
        assert restored_config.max_concurrent_partitions == sample_pipeline_config.max_concurrent_partitions
        assert restored_config.enabled == sample_pipeline_config.enabled
        
        # Verify backends
        assert len(restored_config.backends) == len(sample_pipeline_config.backends)
        restored_backend = restored_config.backends[0]
        original_backend = sample_pipeline_config.backends[0]
        
        assert restored_backend.name == original_backend.name
        assert restored_backend.table == original_backend.table
        assert restored_backend.schema == original_backend.schema
        
        # Verify db_columns are preserved
        assert len(restored_backend.db_columns) == len(original_backend.db_columns)
        assert restored_backend.db_columns[0].name == original_backend.db_columns[0].name
        assert restored_backend.db_columns[0].expr == original_backend.db_columns[0].expr
        
        # Verify stages
        assert len(restored_config.stages) == len(sample_pipeline_config.stages)
        
        # Verify strategies
        assert len(restored_config.strategies) == len(sample_pipeline_config.strategies)
        
        # Verify data storage
        assert restored_storage is not None
        assert 'test_postgres' in restored_storage.datastores
        restored_ds = restored_storage.datastores['test_postgres']
        assert restored_ds.type == 'postgres'
        assert restored_ds.connection.host == 'localhost'
        assert restored_ds.connection.port == 5432
    
    def test_json_dict_to_config_without_datastores(self, sample_pipeline_config):
        """Test deserialization without datastores."""
        # Serialize without datastores
        json_dict = ConfigSerializer.config_to_json_dict(sample_pipeline_config, None)
        
        # Deserialize
        restored_config, restored_storage = ConfigSerializer.json_dict_to_config(json_dict)
        
        assert restored_config.name == sample_pipeline_config.name
        assert restored_storage is None
    
    def test_backends_with_group_by_serialization(self, sample_backend_config):
        """Test that group_by field is properly serialized."""
        result = ConfigSerializer._backend_to_dict(sample_backend_config)
        
        assert 'group_by' in result
        assert result['group_by'] == ['id', 'name']
    
    def test_backends_with_filters_and_joins_serialization(self, sample_backend_config):
        """Test that filters and joins are properly serialized."""
        result = ConfigSerializer._backend_to_dict(sample_backend_config)
        
        # Check filters
        assert 'filters' in result
        assert len(result['filters']) == 1
        filter_dict = result['filters'][0]
        assert filter_dict['column'] == 'status'
        assert filter_dict['operator'] == '='
        assert filter_dict['value'] == 'active'
        
        # Check joins
        assert 'join' in result
        assert len(result['join']) == 1
        join_dict = result['join'][0]
        assert join_dict['table'] == 'other_table'
        assert join_dict['on'] == 't.id = o.id'
        assert join_dict['type'] == 'left'
        assert join_dict['alias'] == 'o'
    
    def test_column_expr_always_included(self):
        """Test that expr field is always included in serialization."""
        column = Column(
            expr='user_id',
            name='user_id',
            data_type=UniversalDataType.INTEGER
        )
        
        result = ConfigSerializer._column_to_dict(column)
        
        # expr should always be present, even if it matches name
        assert 'expr' in result
        assert result['expr'] == 'user_id'
        assert result['name'] == 'user_id'
    
    def test_virtual_column_serialization(self):
        """Test serialization of virtual columns."""
        column = Column(
            expr='NOW()',
            name='current_timestamp',
            data_type=UniversalDataType.TIMESTAMP,
            virtual=True,
            hash_column=False
        )
        
        result = ConfigSerializer._column_to_dict(column)
        
        assert result['name'] == 'current_timestamp'
        assert result['expr'] == 'NOW()'
        assert result['virtual'] is True
        assert 'hash_column' not in result  # False is default, not included
    
    def test_db_columns_deserialization_with_data_types(self):
        """Test that db_columns with data_type strings are properly converted to Column objects."""
        # Create a backend config with db_columns
        backend = BackendConfig(
            type='postgres',
            datastore_name='test_postgres',
            name='test_backend',
            table='test_table',
            columns=[
                Column(expr='id', name='id', data_type=UniversalDataType.INTEGER),
                Column(expr='name', name='name', data_type=UniversalDataType.VARCHAR)
            ],
            db_columns=[
                Column(expr='id', name='id', data_type=UniversalDataType.INTEGER),
                Column(expr='name', name='name', data_type=UniversalDataType.VARCHAR)
            ]
        )
        
        config = PipelineJobConfig(
            name='test_db_columns',
            description='Test db_columns deserialization',
            backends=[backend],
            stages=[]
        )
        
        # Serialize to JSON dict
        json_dict = ConfigSerializer.config_to_json_dict(config, None)
        
        # Verify db_columns are serialized
        assert 'backends' in json_dict['config']
        assert len(json_dict['config']['backends']) == 1
        assert 'db_columns' in json_dict['config']['backends'][0]
        
        # Verify data_type is a string in serialized form
        db_col = json_dict['config']['backends'][0]['db_columns'][0]
        assert isinstance(db_col['data_type'], str)
        assert db_col['data_type'] == 'integer'
        
        # Deserialize
        restored_config, _ = ConfigSerializer.json_dict_to_config(json_dict)
        
        # Verify backends exist
        assert len(restored_config.backends) == 1
        restored_backend = restored_config.backends[0]
        
        # Verify db_columns are Column objects, not dicts
        assert len(restored_backend.db_columns) == 2
        for db_col in restored_backend.db_columns:
            assert isinstance(db_col, Column), f"Expected Column object, got {type(db_col)}"
            assert hasattr(db_col, 'name'), "Column object should have 'name' attribute"
            assert hasattr(db_col, 'expr'), "Column object should have 'expr' attribute"
            assert hasattr(db_col, 'data_type'), "Column object should have 'data_type' attribute"
        
        # Verify data_type is enum, not string
        assert isinstance(restored_backend.db_columns[0].data_type, UniversalDataType)
        assert restored_backend.db_columns[0].data_type == UniversalDataType.INTEGER
        assert restored_backend.db_columns[1].data_type == UniversalDataType.VARCHAR
    
    def test_load_real_yaml_and_serialize(self):
        """Test loading a real YAML config and serializing it."""
        # Try to load a real config file if it exists
        config_path = '/Users/rohitanand/projects/synctooln/examples/configs/pipelines/postgres_postgres_join.yaml'
        
        try:
            # Load config
            config = ConfigLoader.load_from_yaml(config_path)
            
            # Serialize to dict
            config_dict = ConfigSerializer.config_to_dict(config)
            
            # Verify basic structure
            assert 'name' in config_dict
            assert 'stages' in config_dict
            
            # If backends exist, verify they're serialized
            if hasattr(config, 'backends') and config.backends:
                assert 'backends' in config_dict
                assert len(config_dict['backends']) > 0
                
                # Check first backend has required fields
                backend = config_dict['backends'][0]
                assert 'type' in backend
                assert 'datastore_name' in backend
                
                # If db_columns exist, verify they're serialized
                if config.backends[0].db_columns:
                    assert 'db_columns' in backend
                    assert len(backend['db_columns']) > 0
                    
                    # Check first db_column has expr
                    db_col = backend['db_columns'][0]
                    assert 'expr' in db_col
                    assert 'name' in db_col
            
        except FileNotFoundError:
            pytest.skip("Config file not found, skipping real YAML test")
        except Exception as e:
            pytest.fail(f"Failed to load and serialize real config: {e}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

