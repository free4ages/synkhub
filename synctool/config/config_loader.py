import yaml
import os
from typing import Dict, Any, List
from ..core.models import (
    PipelineJobConfig, DataStorage, DataStore, ConnectionConfig, 
    BackendConfig, Column, GlobalStageConfig, StrategyConfig, 
    TransformationConfig, JoinConfig, FilterConfig
)
from ..core.enums import SyncStrategy
from ..core.schema_models import UniversalDataType


class ConfigLoader:
    """Load and validate sync job configurations"""
    
    @staticmethod
    def load_from_yaml(file_path: str) -> PipelineJobConfig:
        """Load configuration from YAML file"""
        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)
        
        if config_dict is None:
            raise ValueError(f"Empty or invalid YAML file: {file_path}")
        
        return ConfigLoader.load_from_dict(config_dict)
    
    @staticmethod
    def load_from_dict(config_dict: Dict[str, Any]) -> PipelineJobConfig:
        """Load configuration from dictionary"""
        processed_config = config_dict.copy()
        
        # Process top-level columns
        global_columns = []
        if 'columns' in processed_config:
            global_columns = ConfigLoader._process_columns(processed_config['columns'])
            processed_config['columns'] = global_columns
        
        # Process stages
        if 'stages' in processed_config:
            processed_stages = []
            for stage_dict in processed_config['stages']:
                stage = ConfigLoader._process_stage(stage_dict, global_columns)
                processed_stages.append(stage)
            processed_config['stages'] = processed_stages
        
        return PipelineJobConfig(**processed_config)
    
    @staticmethod
    def load_datastores_from_yaml(file_path: str) -> DataStorage:
        """Load DataStorage configuration from YAML file"""
        with open(file_path, 'r') as file:
            config_dict = yaml.safe_load(file)
        
        if config_dict is None:
            raise ValueError(f"Empty or invalid datastores YAML file: {file_path}")
        
        return ConfigLoader.load_datastores_from_dict(config_dict)
    
    @staticmethod
    def load_datastores_from_dict(config_dict: Dict[str, Any]) -> DataStorage:
        """Load DataStorage configuration from dictionary"""
        datastores = {}
        
        # Support environment variable substitution for sensitive data
        def resolve_env_vars(value: Any) -> Any:
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]  # Remove ${ and }
                default_value = ""
                if ":" in env_var:
                    env_var, default_value = env_var.split(":", 1)
                return os.getenv(env_var, default_value)
            elif isinstance(value, dict):
                return {k: resolve_env_vars(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_env_vars(item) for item in value]
            return value
        
        for store_name, store_config in config_dict.get('datastores', {}).items():
            # Resolve environment variables in connection config
            resolved_config = resolve_env_vars(store_config)
            
            # Create ConnectionConfig from connection data
            connection_data = resolved_config.get('connection', {})
            connection_config = ConnectionConfig(**connection_data)
            
            # Create DataStore
            datastore = DataStore(
                name=store_name,
                type=resolved_config.get('type', ''),
                connection=connection_config,
                description=resolved_config.get('description'),
                tags=resolved_config.get('tags', [])
            )
            
            datastores[store_name] = datastore
        
        return DataStorage(datastores=datastores)
    
    @staticmethod
    def load_pipeline_with_datastores(config_path: str, datastores_path: str) -> tuple[PipelineJobConfig, DataStorage]:
        """Load both pipeline configuration and datastores together"""
        pipeline_config = ConfigLoader.load_from_yaml(config_path)
        datastores = ConfigLoader.load_datastores_from_yaml(datastores_path)
        
        # Validate that all referenced datastores exist
        issues = []
        for stage in pipeline_config.stages:
            if stage.source and stage.source.datastore_name:
                if not datastores.get_datastore(stage.source.datastore_name):
                    issues.append(f"Stage '{stage.name}' source references unknown datastore: {stage.source.datastore_name}")
            
            if stage.destination and stage.destination.datastore_name:
                if not datastores.get_datastore(stage.destination.datastore_name):
                    issues.append(f"Stage '{stage.name}' destination references unknown datastore: {stage.destination.datastore_name}")
        
        if issues:
            raise ValueError("Configuration validation failed:\n" + "\n".join(issues))
        
        return pipeline_config, datastores
    
    @staticmethod
    def _process_columns(columns_data: List[Dict[str, Any]]) -> List[Column]:
        """Process column configurations into Column objects"""
        columns = []
        for col_dict in columns_data:
            # Convert dtype string to UniversalDataType enum if present
            if 'dtype' in col_dict and isinstance(col_dict['dtype'], str):
                try:
                    col_dict['dtype'] = UniversalDataType(col_dict['dtype'])
                except ValueError:
                    # Keep as string if not a valid enum value
                    pass
            
            # Set expr to name if not provided (for top-level columns)
            if 'expr' not in col_dict:
                col_dict['expr'] = col_dict['name']
            
            columns.append(Column(**col_dict))
        return columns
    
    @staticmethod
    def _process_stage(stage_dict: Dict[str, Any], global_columns: List[Column]) -> GlobalStageConfig:
        """Process stage configuration into GlobalStageConfig object"""
        processed_stage = stage_dict.copy()
        
        # Process source backend if present
        if 'source' in processed_stage and isinstance(processed_stage['source'], dict):
            source_config = ConfigLoader._process_backend_config(
                processed_stage['source'], global_columns
            )
            processed_stage['source'] = source_config
        
        # Process destination backend if present
        if 'destination' in processed_stage and isinstance(processed_stage['destination'], dict):
            dest_config = ConfigLoader._process_backend_config(
                processed_stage['destination'], global_columns
            )
            processed_stage['destination'] = dest_config
        
        # Process columns - inherit from global if not specified
        stage_columns = []
        if 'columns' in processed_stage:
            stage_columns = ConfigLoader._process_columns(processed_stage['columns'])
            # Apply inheritance from global columns
            stage_columns = ConfigLoader._inherit_columns(stage_columns, global_columns)
        else:
            # If no stage-specific columns, use global columns
            stage_columns = global_columns.copy()
        
        processed_stage['columns'] = stage_columns
        
        # Process transformations if present
        if 'transformations' in processed_stage:
            transformations = []
            for trans_dict in processed_stage['transformations']:
                transformations.append(TransformationConfig(**trans_dict))
            processed_stage['transformations'] = transformations
        
        # Process strategies if present
        if 'strategies' in processed_stage:
            strategies = []
            for strat_dict in processed_stage['strategies']:
                strategies.append(ConfigLoader._process_strategy(strat_dict))
            processed_stage['strategies'] = strategies
        
        return GlobalStageConfig(**processed_stage)
    
    @staticmethod
    def _process_backend_config(backend_dict: Dict[str, Any], global_columns: List[Column]) -> BackendConfig:
        """Process backend configuration into BackendConfig object"""
        processed_backend = backend_dict.copy()
        
        # Process columns - inherit from global if not specified
        backend_columns = []
        if 'columns' in processed_backend:
            backend_columns = ConfigLoader._process_columns(processed_backend['columns'])
            # Apply inheritance from global columns
            backend_columns = ConfigLoader._inherit_columns(backend_columns, global_columns)
        else:
            # If no backend-specific columns, use global columns
            backend_columns = global_columns.copy()
        
        processed_backend['columns'] = backend_columns
        
        # Process joins if present
        if 'join' in processed_backend:
            joins = []
            for join_dict in processed_backend['join']:
                joins.append(JoinConfig(**join_dict))
            processed_backend['join'] = joins
        
        # Process filters if present
        if 'filters' in processed_backend:
            filters = []
            for filter_dict in processed_backend['filters']:
                filters.append(FilterConfig(**filter_dict))
            processed_backend['filters'] = filters
        
        return BackendConfig(**processed_backend)
    
    @staticmethod
    def _process_strategy(strategy_dict: Dict[str, Any]) -> StrategyConfig:
        """Process strategy configuration into StrategyConfig object"""
        processed_strategy = strategy_dict.copy()
        
        # Convert type string to SyncStrategy enum
        if 'type' in processed_strategy and isinstance(processed_strategy['type'], str):
            try:
                processed_strategy['type'] = SyncStrategy(processed_strategy['type'])
            except ValueError:
                raise ValueError(f"Invalid strategy type: {processed_strategy['type']}")
        
        # Handle typo in YAML: "parition_key" -> "partition_key"
        if 'parition_key' in processed_strategy:
            processed_strategy['partition_key'] = processed_strategy.pop('parition_key')
        
        # Handle inconsistency: "use_sub_partition" -> "use_sub_partitions"
        if 'use_sub_partition' in processed_strategy:
            processed_strategy['use_sub_partitions'] = processed_strategy.pop('use_sub_partition')
        
        return StrategyConfig(**processed_strategy)
    
    @staticmethod
    def _inherit_columns(stage_columns: List[Column], global_columns: List[Column]) -> List[Column]:
        """Apply column inheritance - stage columns override global columns by name"""
        # Create a map of global columns by name
        global_column_map = {col.name: col for col in global_columns}
        
        # Create a map of stage columns by name
        stage_column_map = {col.name: col for col in stage_columns}
        
        # Start with global columns and override with stage-specific ones
        result_columns = []
        
        # Add all global columns, potentially overridden by stage columns
        for global_col in global_columns:
            if global_col.name in stage_column_map:
                # Use stage column but inherit missing fields from global
                stage_col = stage_column_map[global_col.name]
                inherited_col = ConfigLoader._merge_column_properties(global_col, stage_col)
                result_columns.append(inherited_col)
            else:
                # Use global column as-is
                result_columns.append(global_col)
        
        # Add any stage columns that don't exist in global columns
        for stage_col in stage_columns:
            if stage_col.name not in global_column_map:
                result_columns.append(stage_col)
        
        return result_columns
    
    @staticmethod
    def _merge_column_properties(global_col: Column, stage_col: Column) -> Column:
        """Merge column properties, with stage column taking precedence"""
        # Create a new column with global properties as defaults
        merged_dict = {
            'name': stage_col.name,
            'expr': stage_col.expr if stage_col.expr != stage_col.name else global_col.expr,
            'dtype': stage_col.dtype if stage_col.dtype is not None else global_col.dtype,
            'hash_field': stage_col.hash_field if hasattr(stage_col, 'hash_field') else global_col.hash_field,
            'data_field': stage_col.data_field if hasattr(stage_col, 'data_field') else global_col.data_field,
            'unique_key': stage_col.unique_key if hasattr(stage_col, 'unique_key') else global_col.unique_key,
            'order_key': stage_col.order_key if hasattr(stage_col, 'order_key') else global_col.order_key,
            'direction': stage_col.direction if hasattr(stage_col, 'direction') else global_col.direction,
            'delta_key': stage_col.delta_key if hasattr(stage_col, 'delta_key') else global_col.delta_key,
            'partition_key': stage_col.partition_key if hasattr(stage_col, 'partition_key') else global_col.partition_key,
            'hash_key': stage_col.hash_key if hasattr(stage_col, 'hash_key') else global_col.hash_key,
        }
        
        return Column(**merged_dict)
    
    @staticmethod
    def validate_config(config: PipelineJobConfig) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        if not config.name:
            issues.append("Job name is required")
        
        if not config.stages:
            issues.append("At least one stage must be defined")
        
        # Validate each stage
        for stage in config.stages:
            if not stage.name:
                issues.append("Stage name is required")
            
            # Validate strategies within stages
            for strategy in stage.strategies:
                if not strategy.name:
                    issues.append(f"Strategy name is required in stage '{stage.name}'")
                
                # Check if strategy has either column or partition_key specified
                if not strategy.column and not strategy.partition_key:
                    issues.append(f"Strategy '{strategy.name}' in stage '{stage.name}' must specify either 'column' or 'partition_key'")
        
        return issues
    
    @staticmethod
    def validate_datastores_config(data_storage: DataStorage) -> List[str]:
        """Validate DataStorage configuration and return list of issues"""
        issues = []
        
        if not data_storage.datastores:
            issues.append("At least one datastore must be defined")
        
        for name, datastore in data_storage.datastores.items():
            if not datastore.name:
                issues.append(f"Datastore name is required")
            if not datastore.type:
                issues.append(f"Datastore '{name}' must specify a type")
            if not datastore.connection:
                issues.append(f"Datastore '{name}' must have connection configuration")
            
            # Validate connection based on type
            conn = datastore.connection
            if datastore.type in ['postgres', 'mysql']:
                if not conn.host:
                    issues.append(f"Datastore '{name}' must specify host")
                if not conn.user:
                    issues.append(f"Datastore '{name}' must specify user")
                if not (conn.database or conn.dbname):
                    issues.append(f"Datastore '{name}' must specify database/dbname")
            elif datastore.type == 'object_storage':
                if not conn.s3_bucket:
                    issues.append(f"Datastore '{name}' must specify s3_bucket")
        
        return issues
