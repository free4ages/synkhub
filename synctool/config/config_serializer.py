from typing import Dict, Any, List, Optional
from datetime import datetime
from ..core.models import (
    PipelineJobConfig, DataStorage, DataStore, ConnectionConfig,
    BackendConfig, Column, GlobalStageConfig, StrategyConfig,
    TransformationConfig, JoinConfig, FilterConfig
)
from ..core.enums import SyncStrategy
from ..core.schema_models import UniversalDataType


class ConfigSerializer:
    """Utility class for serializing/deserializing configuration objects"""
    
    @staticmethod
    def config_to_dict(config: PipelineJobConfig) -> Dict[str, Any]:
        """Convert PipelineJobConfig to dictionary for YAML/JSON serialization"""
        return {
            'name': config.name,
            'description': config.description,
            'columns': [ConfigSerializer._column_to_dict(col) for col in config.columns],
            'stages': [ConfigSerializer._stage_to_dict(stage) for stage in config.stages]
        }
    
    @staticmethod
    def datastores_to_dict(datastores: DataStorage) -> Dict[str, Any]:
        """Convert DataStorage to dictionary for YAML/JSON serialization"""
        return {
            'datastores': {
                name: ConfigSerializer._datastore_to_dict(ds)
                for name, ds in datastores.datastores.items()
            }
        }
    
    @staticmethod
    def _column_to_dict(column: Column) -> Dict[str, Any]:
        """Convert Column to dictionary"""
        result = {
            'name': column.name,
            'expr': column.expr,
        }
        
        # Only include non-default values
        if column.dtype is not None:
            result['dtype'] = column.dtype.value if hasattr(column.dtype, 'value') else str(column.dtype)
        
        if not column.hash_column:
            result['hash_column'] = column.hash_column
        if not column.data_column:
            result['data_column'] = column.data_column
        if column.unique_column:
            result['unique_column'] = column.unique_column
        if column.order_column:
            result['order_column'] = column.order_column
        if column.direction != "asc":
            result['direction'] = column.direction
        if column.delta_column:
            result['delta_column'] = column.delta_column
        if column.partition_column:
            result['partition_column'] = column.partition_column
        if column.hash_key:
            result['hash_key'] = column.hash_key
        
        return result
    
    @staticmethod
    def _stage_to_dict(stage: GlobalStageConfig) -> Dict[str, Any]:
        """Convert GlobalStageConfig to dictionary"""
        result = {
            'name': stage.name,
            'enabled': stage.enabled,
        }
        
        if stage.type:
            result['type'] = stage.type
        if stage.source:
            result['source'] = ConfigSerializer._backend_to_dict(stage.source)
        if stage.destination:
            result['destination'] = ConfigSerializer._backend_to_dict(stage.destination)
        if stage.config:
            result['config'] = stage.config
        if stage.class_path:
            result['class_path'] = stage.class_path
        if stage.columns:
            result['columns'] = [ConfigSerializer._column_to_dict(col) for col in stage.columns]
        if stage.transformations:
            result['transformations'] = [ConfigSerializer._transformation_to_dict(trans) for trans in stage.transformations]
        if stage.page_size is not None:
            result['page_size'] = stage.page_size
        if stage.strategies:
            result['strategies'] = [ConfigSerializer._strategy_to_dict(strat) for strat in stage.strategies]
        if stage.max_concurrent_partitions != 1:
            result['max_concurrent_partitions'] = stage.max_concurrent_partitions
        if stage.target_batch_size is not None:
            result['target_batch_size'] = stage.target_batch_size
        if stage.use_pagination is not None:
            result['use_pagination'] = stage.use_pagination
        
        return result
    
    @staticmethod
    def _backend_to_dict(backend: BackendConfig) -> Dict[str, Any]:
        """Convert BackendConfig to dictionary"""
        result = {
            'type': backend.type,
            'datastore_name': backend.datastore_name,
        }
        
        if backend.table:
            result['table'] = backend.table
        if backend.schema:
            result['schema'] = backend.schema
        if backend.alias:
            result['alias'] = backend.alias
        if backend.join:
            result['join'] = [ConfigSerializer._join_to_dict(join) for join in backend.join]
        if backend.filters:
            result['filters'] = [ConfigSerializer._filter_to_dict(filter_) for filter_ in backend.filters]
        if backend.columns:
            result['columns'] = [ConfigSerializer._column_to_dict(col) for col in backend.columns]
        if backend.config:
            result['config'] = backend.config
        if backend.hash_cache:
            result['hash_cache'] = backend.hash_cache
        if backend.index_cache:
            result['index_cache'] = backend.index_cache
        if backend.supports_update:
            result['supports_update'] = backend.supports_update
        
        return result
    
    @staticmethod
    def _strategy_to_dict(strategy: StrategyConfig) -> Dict[str, Any]:
        """Convert StrategyConfig to dictionary"""
        result = {
            'name': strategy.name,
            'type': strategy.type.value if hasattr(strategy.type, 'value') else str(strategy.type),
            'enabled': strategy.enabled,
        }
        
        if strategy.column:
            result['column'] = strategy.column
        if strategy.column_type:
            result['column_type'] = strategy.column_type
        if not strategy.use_sub_partition:
            result['use_sub_partition'] = strategy.use_sub_partition
        if strategy.sub_partition_step != 100:
            result['sub_partition_step'] = strategy.sub_partition_step
        if strategy.min_sub_partition_step != 10:
            result['min_sub_partition_step'] = strategy.min_sub_partition_step
        if strategy.interval_reduction_factor != 2:
            result['interval_reduction_factor'] = strategy.interval_reduction_factor
        if strategy.intervals:
            result['intervals'] = strategy.intervals
        if not strategy.prevent_update_unless_changed:
            result['prevent_update_unless_changed'] = strategy.prevent_update_unless_changed
        if strategy.use_pagination:
            result['use_pagination'] = strategy.use_pagination
        if strategy.page_size != 1000:
            result['page_size'] = strategy.page_size
        if strategy.cron:
            result['cron'] = strategy.cron
        if strategy.partition_column:
            result['partition_column'] = strategy.partition_column
        if strategy.partition_step:
            result['partition_step'] = strategy.partition_step
        if strategy.pipeline_config:
            result['pipeline_config'] = strategy.pipeline_config
        if not strategy.enable_pipeline:
            result['enable_pipeline'] = strategy.enable_pipeline
        
        return result
    
    @staticmethod
    def _transformation_to_dict(transformation: TransformationConfig) -> Dict[str, Any]:
        """Convert TransformationConfig to dictionary"""
        result = {
            'transform': transformation.transform,
            'expr': transformation.expr,
        }
        
        if transformation.dtype:
            result['dtype'] = transformation.dtype
        if transformation.columns:
            result['columns'] = transformation.columns
        
        return result
    
    @staticmethod
    def _join_to_dict(join: JoinConfig) -> Dict[str, Any]:
        """Convert JoinConfig to dictionary"""
        result = {
            'table': join.table,
            'on': join.on,
        }
        
        if join.type != "inner":
            result['type'] = join.type
        if join.alias:
            result['alias'] = join.alias
        
        return result
    
    @staticmethod
    def _filter_to_dict(filter_: FilterConfig) -> Dict[str, Any]:
        """Convert FilterConfig to dictionary"""
        return {
            'column': filter_.column,
            'operator': filter_.operator,
            'value': filter_.value
        }
    
    @staticmethod
    def _datastore_to_dict(datastore: DataStore) -> Dict[str, Any]:
        """Convert DataStore to dictionary"""
        result = {
            'type': datastore.type,
            'connection': ConfigSerializer._connection_to_dict(datastore.connection),
        }
        
        if datastore.description:
            result['description'] = datastore.description
        if datastore.tags:
            result['tags'] = datastore.tags
        if datastore.additional_capabilities:
            result['additional_capabilities'] = datastore.additional_capabilities
        if datastore.disabled_capabilities:
            result['disabled_capabilities'] = datastore.disabled_capabilities
        
        return result
    
    @staticmethod
    def _connection_to_dict(connection: ConnectionConfig) -> Dict[str, Any]:
        """Convert ConnectionConfig to dictionary"""
        result = {}
        
        # Only include non-None values
        if connection.host:
            result['host'] = connection.host
        if connection.port:
            result['port'] = connection.port
        if connection.user:
            result['user'] = connection.user
        if connection.password:
            result['password'] = connection.password
        if connection.database:
            result['database'] = connection.database
        if connection.dbname:
            result['dbname'] = connection.dbname
        if connection.schema:
            result['schema'] = connection.schema
        if connection.s3_bucket:
            result['s3_bucket'] = connection.s3_bucket
        if connection.s3_prefix:
            result['s3_prefix'] = connection.s3_prefix
        if connection.aws_access_key_id:
            result['aws_access_key_id'] = connection.aws_access_key_id
        if connection.aws_secret_access_key:
            result['aws_secret_access_key'] = connection.aws_secret_access_key
        if connection.region:
            result['region'] = connection.region
        if connection.max_connections != 10:
            result['max_connections'] = connection.max_connections
        if connection.min_connections != 1:
            result['min_connections'] = connection.min_connections
        
        return result
