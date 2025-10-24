from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import pickle
import hashlib
from pathlib import Path
import logging

from ..core.models import (
    PipelineJobConfig, DataStorage, DataStore, ConnectionConfig,
    BackendConfig, Column, GlobalStageConfig, StrategyConfig,
    TransformationConfig, JoinConfig, FilterConfig, DimensionPartitionConfig
)
from ..core.enums import SyncStrategy, HashAlgo
from ..core.schema_models import UniversalDataType


class ConfigSerializer:
    """Utility class for serializing/deserializing configuration objects"""
    
    @staticmethod
    def config_to_dict(config: PipelineJobConfig) -> Dict[str, Any]:
        """Convert PipelineJobConfig to dictionary for YAML/JSON serialization"""
        result = {
            'name': config.name,
            'description': config.description,
            'stages': [ConfigSerializer._stage_to_dict(stage) for stage in config.stages]
        }
        
        # Add optional fields
        if hasattr(config, 'sync_type') and config.sync_type:
            result['sync_type'] = config.sync_type
        
        if hasattr(config, 'hash_algo') and config.hash_algo:
            result['hash_algo'] = config.hash_algo.value if hasattr(config.hash_algo, 'value') else str(config.hash_algo)
        
        if hasattr(config, 'max_concurrent_partitions') and config.max_concurrent_partitions != 1:
            result['max_concurrent_partitions'] = config.max_concurrent_partitions
        
        # Add enabled field
        if hasattr(config, 'enabled'):
            result['enabled'] = config.enabled

        if hasattr(config, 'strategies') and config.strategies:
            result['strategies'] = [ConfigSerializer._strategy_to_dict(strat) for strat in config.strategies]
        
        # Add backends serialization (includes db_columns)
        if hasattr(config, 'backends') and config.backends:
            result['backends'] = [ConfigSerializer._backend_to_dict(backend) for backend in config.backends]
        
        # Add columns serialization if not empty (commented out as columns are typically in stages/backends)
        # if hasattr(config, 'columns') and config.columns:
        #     result['columns'] = [ConfigSerializer._column_to_dict(col) for col in config.columns]
        
        return result
    
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
            'expr': column.expr,  # Always include expr since it's required
        }
        
        # Only include non-default values
        if column.data_type is not None:
            result['data_type'] = column.data_type.value if hasattr(column.data_type, 'value') else str(column.data_type)
        
        if column.hash_column:
            result['hash_column'] = column.hash_column
        # if not column.data_column:
        #     result['data_column'] = column.data_column
        if column.unique_column:
            result['unique_column'] = column.unique_column
        if column.order_column:
            result['order_column'] = column.order_column
        if column.direction != "asc":
            result['direction'] = column.direction
        # if column.delta_column:
        #     result['delta_column'] = column.delta_column
        # if column.partition_column:
        #     result['partition_column'] = column.partition_column
        if column.hash_key:
            result['hash_key'] = column.hash_key
        
        # New fields
        if hasattr(column, 'virtual') and column.virtual:
            result['virtual'] = column.virtual
        if hasattr(column, 'precision') and column.precision is not None:
            result['precision'] = column.precision
        if hasattr(column, 'scale') and column.scale is not None:
            result['scale'] = column.scale
        if hasattr(column, 'max_length') and column.max_length is not None:
            result['max_length'] = column.max_length
        
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
        # if hasattr(stage, 'page_size') and stage.page_size is not None:
        #     result['page_size'] = stage.page_size
        # if hasattr(stage, 'strategies') and stage.strategies:
        #     result['strategies'] = [ConfigSerializer._strategy_to_dict(strat) for strat in stage.strategies]
        # if hasattr(stage, 'max_concurrent_partitions') and stage.max_concurrent_partitions != 1:
        #     result['max_concurrent_partitions'] = stage.max_concurrent_partitions
        # if hasattr(stage, 'target_batch_size') and stage.target_batch_size is not None:
        #     result['target_batch_size'] = stage.target_batch_size
        # if hasattr(stage, 'use_pagination') and stage.use_pagination is not None:
        #     result['use_pagination'] = stage.use_pagination
        
        return result
    
    @staticmethod
    def _backend_to_dict(backend: BackendConfig) -> Dict[str, Any]:
        """Convert BackendConfig to dictionary"""
        result = {
            'type': backend.type,
            'datastore_name': backend.datastore_name,
        }
        
        if backend.name:
            result['name'] = backend.name
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
        if backend.group_by:
            result['group_by'] = backend.group_by
        if backend.columns:
            result['columns'] = [ConfigSerializer._column_to_dict(col) for col in backend.columns]
        if backend.db_columns:
            result['db_columns'] = [ConfigSerializer._column_to_dict(col) for col in backend.db_columns]
        if backend.config:
            result['config'] = backend.config
        # if backend.hash_cache:
        #     result['hash_cache'] = backend.hash_cache
        # if backend.index_cache:
        #     result['index_cache'] = backend.index_cache
        # if backend.supports_update:
        #     result['supports_update'] = backend.supports_update
        
        return result
    
    @staticmethod
    def _strategy_to_dict(strategy: StrategyConfig) -> Dict[str, Any]:
        """Convert StrategyConfig to dictionary"""
        result = {
            'name': strategy.name,
            'type': strategy.type.value if hasattr(strategy.type, 'value') else str(strategy.type),
            'enabled': strategy.enabled,
        }
        
        # Add default field if present
        if hasattr(strategy, 'default') and strategy.default:
            result['default'] = strategy.default
        
        # New multi-dimensional partition fields
        if hasattr(strategy, 'primary_partitions') and strategy.primary_partitions:
            result['primary_partitions'] = [
                ConfigSerializer._dimension_partition_to_dict(p) for p in strategy.primary_partitions
            ]
        if hasattr(strategy, 'secondary_partitions') and strategy.secondary_partitions:
            result['secondary_partitions'] = [
                ConfigSerializer._dimension_partition_to_dict(p) for p in strategy.secondary_partitions
            ]
        if hasattr(strategy, 'delta_partitions') and strategy.delta_partitions:
            result['delta_partitions'] = [
                ConfigSerializer._dimension_partition_to_dict(p) for p in strategy.delta_partitions
            ]
        
        # Legacy single partition fields (for backwards compatibility)
        if hasattr(strategy, 'column') and strategy.column:
            result['column'] = strategy.column
        if hasattr(strategy, 'data_type') and strategy.data_type:
            result['data_type'] = strategy.data_type
        if hasattr(strategy, 'partition_column') and strategy.partition_column:
            result['partition_column'] = strategy.partition_column
        if hasattr(strategy, 'partition_step') and strategy.partition_step:
            result['partition_step'] = strategy.partition_step
        
        # Sub-partition settings
        if hasattr(strategy, 'use_sub_partition') and not strategy.use_sub_partition:
            result['use_sub_partition'] = strategy.use_sub_partition
        if hasattr(strategy, 'sub_partition_step') and strategy.sub_partition_step != 100:
            result['sub_partition_step'] = strategy.sub_partition_step
        if hasattr(strategy, 'min_sub_partition_step') and strategy.min_sub_partition_step != 10:
            result['min_sub_partition_step'] = strategy.min_sub_partition_step
        if hasattr(strategy, 'interval_reduction_factor') and strategy.interval_reduction_factor != 2:
            result['interval_reduction_factor'] = strategy.interval_reduction_factor
        if hasattr(strategy, 'intervals') and strategy.intervals:
            result['intervals'] = strategy.intervals
        
        # Change detection settings
        if hasattr(strategy, 'prevent_update_unless_changed') and not strategy.prevent_update_unless_changed:
            result['prevent_update_unless_changed'] = strategy.prevent_update_unless_changed
        
        # Pagination settings
        if hasattr(strategy, 'use_pagination') and strategy.use_pagination:
            result['use_pagination'] = strategy.use_pagination
        if hasattr(strategy, 'page_size') and strategy.page_size != 1000:
            result['page_size'] = strategy.page_size
        
        # Scheduling
        if hasattr(strategy, 'cron') and strategy.cron:
            result['cron'] = strategy.cron
        
        # Concurrency
        if hasattr(strategy, 'max_concurrent_partitions') and strategy.max_concurrent_partitions != 1:
            result['max_concurrent_partitions'] = strategy.max_concurrent_partitions
        
        # Pipeline settings
        if hasattr(strategy, 'pipeline_config') and strategy.pipeline_config:
            result['pipeline_config'] = strategy.pipeline_config
        if hasattr(strategy, 'enable_pipeline') and not strategy.enable_pipeline:
            result['enable_pipeline'] = strategy.enable_pipeline
        
        return result
    
    @staticmethod
    def _dimension_partition_to_dict(partition: DimensionPartitionConfig) -> Dict[str, Any]:
        """Convert DimensionPartitionConfig to dictionary"""
        result = {
            'column': partition.column,
            'step': partition.step,
        }
        
        # Add optional fields
        if partition.step_unit:
            result['step_unit'] = partition.step_unit
        if partition.data_type:
            result['data_type'] = partition.data_type.value if hasattr(partition.data_type, 'value') else str(partition.data_type)
        if partition.start is not None:
            result['start'] = partition.start
        if partition.end is not None:
            result['end'] = partition.end
        if partition.type != "range":
            result['type'] = partition.type
        if partition.value:
            result['value'] = partition.value
        if partition.lower_bound is not None:
            result['lower_bound'] = partition.lower_bound
        if partition.upper_bound is not None:
            result['upper_bound'] = partition.upper_bound
        if partition.bounded:
            result['bounded'] = partition.bounded
        
        return result
    
    @staticmethod
    def _transformation_to_dict(transformation: TransformationConfig) -> Dict[str, Any]:
        """Convert TransformationConfig to dictionary"""
        result = {}
        
        # Required fields
        if hasattr(transformation, 'transform') and transformation.transform:
            result['transform'] = transformation.transform
        if hasattr(transformation, 'expr') and transformation.expr:
            result['expr'] = transformation.expr
        
        # Optional fields
        if hasattr(transformation, 'name') and transformation.name:
            result['name'] = transformation.name
        if hasattr(transformation, 'data_type') and transformation.data_type:
            result['data_type'] = transformation.data_type.value if hasattr(transformation.data_type, 'value') else str(transformation.data_type)
        if hasattr(transformation, 'columns') and transformation.columns:
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
    
    @staticmethod
    def config_to_json_dict(config: PipelineJobConfig, data_storage: Optional[DataStorage] = None) -> Dict[str, Any]:
        """
        Serialize config and data_storage to a JSON-serializable dictionary.
        This can be passed directly as job parameters to workers.
        
        Args:
            config: Pipeline configuration
            data_storage: Optional DataStorage object with datastores
        
        Returns:
            JSON-serializable dictionary
        """
        result = {
            'config': ConfigSerializer.config_to_dict(config),
            'timestamp': datetime.now().isoformat(),
            'version': '1.0'
        }
        
        if data_storage:
            result['data_storage'] = ConfigSerializer.datastores_to_dict(data_storage)
        
        return result
    
    @staticmethod
    def json_dict_to_config(data: Dict[str, Any]) -> Tuple[PipelineJobConfig, Optional[DataStorage]]:
        """
        Deserialize config and data_storage from JSON dictionary.
        
        Args:
            data: JSON dictionary from config_to_json_dict()
        
        Returns:
            Tuple of (PipelineJobConfig, DataStorage or None)
        """
        from ..config.config_loader import ConfigLoader
        
        # Load config
        config_dict = data['config']
        config = ConfigLoader.load_from_dict(config_dict)
        
        # Load data_storage if present
        data_storage = None
        if 'data_storage' in data:
            # Reconstruct DataStorage from dict
            datastores_dict = data['data_storage'].get('datastores', {})
            data_storage = DataStorage()
            
            for name, ds_dict in datastores_dict.items():
                connection_config = ConnectionConfig(**ds_dict['connection'])
                datastore = DataStore(
                    name=name,
                    type=ds_dict['type'],
                    connection=connection_config,
                    description=ds_dict.get('description'),
                    tags=ds_dict.get('tags', []),
                    additional_capabilities=ds_dict.get('additional_capabilities'),
                    disabled_capabilities=ds_dict.get('disabled_capabilities')
                )
                data_storage.add_datastore(datastore)
        
        return config, data_storage
    
    @staticmethod
    def pickle_config(
        config: PipelineJobConfig,
        data_storage: Optional[DataStorage] = None,
        output_dir: str = "./data/pickled_configs"
    ) -> str:
        """
        Serialize config and data_storage to a pickle file.
        (Deprecated: Use config_to_json_dict() for distributed systems)
        
        Args:
            config: Pipeline configuration to pickle
            data_storage: Optional DataStorage object with datastores
            output_dir: Directory to store pickle files
        
        Returns:
            Path to the pickled file
        """
        pickle_dir = Path(output_dir)
        pickle_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a serializable package
        config_package = {
            'config': config,
            'data_storage': data_storage,
            'timestamp': datetime.now().isoformat(),
            'version': '1.0'  # For future compatibility
        }
        
        # Generate filename from config name and hash
        config_hash = hashlib.md5(config.name.encode()).hexdigest()[:8]
        pickle_file = pickle_dir / f"{config.name}_{config_hash}.pkl"
        
        with open(pickle_file, 'wb') as f:
            pickle.dump(config_package, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        return str(pickle_file)
    
    @staticmethod
    def unpickle_config(pickle_path: str) -> Tuple[PipelineJobConfig, Optional[DataStorage]]:
        """
        Deserialize config and data_storage from a pickle file.
        
        Args:
            pickle_path: Path to the pickled file
        
        Returns:
            Tuple of (PipelineJobConfig, DataStorage or None)
        """
        with open(pickle_path, 'rb') as f:
            config_package = pickle.load(f)
        
        # Extract config and data_storage
        config = config_package['config']
        data_storage = config_package.get('data_storage')
        
        return config, data_storage
    
    @staticmethod
    def cleanup_old_pickles(
        output_dir: str = "./data/pickled_configs",
        max_age_days: int = 7
    ):
        """
        Remove pickle files older than max_age_days.
        
        Args:
            output_dir: Directory containing pickle files
            max_age_days: Maximum age of files to keep
        """
        import time
        
        pickle_dir = Path(output_dir)
        if not pickle_dir.exists():
            return
        
        cutoff = time.time() - (max_age_days * 86400)
        
        for pickle_file in pickle_dir.glob("*.pkl"):
            if pickle_file.stat().st_mtime < cutoff:
                try:
                    pickle_file.unlink()
                except Exception as e:
                    # Log but don't fail
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Failed to remove old pickle {pickle_file}: {e}")
