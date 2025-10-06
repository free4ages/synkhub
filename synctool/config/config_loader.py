import yaml
import os
from typing import Dict, Any, List
from ..core.models import (
    PipelineJobConfig, DataStorage, DataStore, ConnectionConfig, 
    BackendConfig, Column, GlobalStageConfig, StrategyConfig, 
    TransformationConfig, JoinConfig, FilterConfig, DimensionPartitionConfig
)
from ..core.enums import SyncStrategy, HashAlgo
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
        
        # Process pipeline_columns FIRST - this is the source of truth
        pipeline_columns_map = {}
        if 'pipeline_columns' in processed_config:
            for col_dict in processed_config['pipeline_columns']:
                # Store by name for easy lookup
                pipeline_columns_map[col_dict['name']] = col_dict.copy()
        
        backends_map = {}
        if 'backends' in processed_config:
            for backend_dict in processed_config['backends']:
                backends_map[backend_dict['name']] = backend_dict
                if not 'columns' in backend_dict:
                    raise ValueError(f"Columns are required for backend {backend_dict['name']}")
                
                # Process columns with inheritance from pipeline_columns
                backend_dict['db_columns'] = [
                    Column(**col) for col in ConfigLoader._process_columns(
                        backend_dict['columns'], 
                        pipeline_columns_map
                    )
                ]
                # if 'hash_columns' in backend_dict:
                #     #prepare hash columns and hash_key column
                #     hash_columns = []
                #     columns_map = {col['name']: col for col in backend_dict['columns']}
                #     for name in backend_dict['hash_columns']:
                #         # get column expr and data_type
                #         col = columns_map.get(name)
                #         if not col:
                #             raise ValueError(f"Column {name} not found in backend {backend_dict['name']}")
                #         hash_columns.append(Column(name=name, expr=(col.get("expr") or name), data_type=col['data_type']))
                #     backend_dict['hash_columns'] = hash_columns
                # if 'hash_key_column' in backend_dict:
                #     # columns_map = {col['name']: col for col in backend_dict['columns']}
                #     hash_key_column = next((col for col in backend_dict['columns'] if col['name'] == backend_dict['hash_key_column']), None)
                #     if not hash_key_column:
                #         raise ValueError(f"Hash key column {backend_dict['hash_key_column']} not found in backend {backend_dict['name']}")
                #     backend_dict['hash_key_column'] = Column(name=backend_dict['hash_key_column'], expr=(hash_key_column.get("expr") or backend_dict['hash_key_column']), data_type=hash_key_column['data_type'])
                
                # if 'columns' in backend_dict:
                #     for col_dict in backend_dict['columns']:
                #         global_column = global_columns_map.get(col_dict['name']) or {}
                #         for key, value in col_dict.items():
                #             if key!='expr' and key not in global_column:
                #                 global_column[key] = value
                #         global_columns_map[col_dict['name']] = global_column 
        
        # Process top-level columns
        # if global_columns_map:
        #     global_columns_dict = ConfigLoader._process_columns(list(global_columns_map.values()))
        #     processed_config['columns'] = [Column(**x) for x in global_columns_dict]
        global_max_concurrent_partitions = 1
        if 'max_concurrent_partitions' in processed_config:
            global_max_concurrent_partitions = processed_config['max_concurrent_partitions']

        if 'strategies' in processed_config:
            global_strategies = ConfigLoader._process_strategies(
                processed_config['strategies'],
                global_max_concurrent_partitions,
                pipeline_columns_map  # Pass pipeline_columns
            )
            processed_config['strategies'] = global_strategies
        else:
            processed_config['strategies'] = []
        hash_algo = HashAlgo.HASH_MD5_HASH
        if 'hash_algo' in processed_config:
            hash_algo = processed_config['hash_algo']
        # Process stages
        if 'stages' in processed_config:
            processed_stages = []
            for stage_dict in processed_config['stages']:
                stage_dict['hash_algo'] = hash_algo
                stage = ConfigLoader._process_stage(
                    stage_dict, 
                    backends_map, 
                    global_strategies,
                    pipeline_columns_map  # Pass pipeline_columns
                )
                processed_stages.append(stage)
            processed_config['stages'] = processed_stages
        processed_config.pop("pipeline_columns", None)
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
    def _process_columns(columns_data: List[Dict[str, Any]], pipeline_columns_map: Dict[str, Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Process column configurations into Column objects with inheritance from pipeline_columns"""
        if pipeline_columns_map is None:
            pipeline_columns_map = {}
        
        columns = []
        for col_dict in columns_data:
            col_dict = col_dict.copy()  # Don't mutate original
            
            col_name = col_dict.get('name')
            
            # Inherit from pipeline_columns if available
            if col_name and col_name in pipeline_columns_map:
                # Start with pipeline column definition
                inherited = pipeline_columns_map[col_name].copy()
                # Override with backend/stage-specific settings
                # Backend/stage settings take precedence
                inherited.update(col_dict)
                col_dict = inherited
            
            # Convert data_type string to UniversalDataType enum if present
            if 'data_type' in col_dict and isinstance(col_dict['data_type'], str):
                try:
                    col_dict['data_type'] = UniversalDataType(col_dict['data_type'])
                except ValueError:
                    # Keep as string if not a valid enum value
                    pass
            
            # Set expr to name if not provided (for top-level columns)
            if 'expr' not in col_dict:
                col_dict['expr'] = col_dict['name']
            
            columns.append(col_dict)
        return columns
    
    @staticmethod
    def _process_stage(stage_dict: Dict[str, Any], backends_map: Dict[str, Any], global_strategies: List[StrategyConfig], pipeline_columns_map: Dict[str, Dict[str, Any]] = None) -> GlobalStageConfig:
        """Process stage configuration into GlobalStageConfig object"""
        if pipeline_columns_map is None:
            pipeline_columns_map = {}
        
        processed_stage = stage_dict.copy()
        stage_columns_dict = []
        processed_stage['columns'] = []
        
        # Process source backend if present
        if 'source' in processed_stage and isinstance(processed_stage['source'], dict):
            if 'name' in processed_stage['source'] and processed_stage['source']['name'] in backends_map:
                if 'columns' in backends_map[processed_stage['source']['name']]:
                    stage_columns_dict = ConfigLoader._process_columns(
                        backends_map[processed_stage['source']['name']]['columns'],
                        pipeline_columns_map
                    )
                source_dict = processed_stage['source'].copy()
                processed_stage['source'].update(backends_map[processed_stage['source']['name']])
                processed_stage['source'].update(source_dict)
        
            source_config = ConfigLoader._process_backend_config(
                processed_stage['source'], 
                stage_columns_dict,
                pipeline_columns_map
            )
            processed_stage['source'] = source_config
        
        # Process destination backend if present
        if 'destination' in processed_stage and isinstance(processed_stage['destination'], dict):
            if 'name' in processed_stage['destination'] and processed_stage['destination']['name'] in backends_map:
                if 'columns' in backends_map[processed_stage['destination']['name']]:
                    stage_columns_dict = ConfigLoader._process_columns(
                        backends_map[processed_stage['destination']['name']]['columns'],
                        pipeline_columns_map
                    )
                dest_dict = processed_stage['destination'].copy()
                processed_stage['destination'].update(backends_map[processed_stage['destination']['name']])
                processed_stage['destination'].update(dest_dict)
        
            dest_config = ConfigLoader._process_backend_config(
                processed_stage['destination'], 
                stage_columns_dict,
                pipeline_columns_map
            )
            processed_stage['destination'] = dest_config
        
        # Process transformations with data_type inheritance
        if 'transformations' in processed_stage:
            transformations = []
            for trans_dict in processed_stage['transformations']:
                trans_dict = trans_dict.copy()
                
                # Inherit data_type from pipeline_columns if not specified
                trans_name = trans_dict.get('name')
                if trans_name and trans_name in pipeline_columns_map:
                    pipeline_col = pipeline_columns_map[trans_name]
                    
                    # Only inherit data_type if not already specified
                    if 'data_type' not in trans_dict and 'data_type' in pipeline_col:
                        trans_dict['data_type'] = pipeline_col['data_type']
                
                # Convert data_type to UniversalDataType if it's a string
                if 'data_type' in trans_dict and isinstance(trans_dict['data_type'], str):
                    try:
                        trans_dict['data_type'] = UniversalDataType(trans_dict['data_type'])
                    except ValueError:
                        pass
                
                transformations.append(TransformationConfig(**trans_dict))
            processed_stage['transformations'] = transformations
        
        # Process strategies if present
        if 'strategies' in processed_stage:
            strategies = []
            for strat_dict in processed_stage['strategies']:
                strategies.append(ConfigLoader._process_strategy(
                    strat_dict,
                    pipeline_columns_map
                ))
            processed_stage['strategies'] = strategies
        else:
            processed_stage['strategies'] = global_strategies
        
        return GlobalStageConfig(**processed_stage)
    
    @staticmethod
    def _process_strategies(strategies_data: List[Dict[str, Any]], global_max_concurrent_partitions: int, pipeline_columns_map: Dict[str, Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Process strategies configurations into StrategyConfig objects"""
        if pipeline_columns_map is None:
            pipeline_columns_map = {}
        
        strategies = []
        for strat_dict in strategies_data:
            if not 'max_concurrent_partitions' in strat_dict:
                strat_dict['max_concurrent_partitions'] = global_max_concurrent_partitions
            strategies.append(ConfigLoader._process_strategy(strat_dict, pipeline_columns_map))
        return strategies
    
    
    @staticmethod
    def _process_backend_config(backend_dict: Dict[str, Any], global_columns: List[Dict[str, Any]], pipeline_columns_map: Dict[str, Dict[str, Any]] = None) -> BackendConfig:
        """Process backend configuration into BackendConfig object"""
        if pipeline_columns_map is None:
            pipeline_columns_map = {}
        
        processed_backend = backend_dict.copy()
        
        # Process columns - inherit from global if not specified
        backend_columns = []
        if 'columns' in processed_backend:
            backend_columns = ConfigLoader._process_columns(
                processed_backend['columns'],
                pipeline_columns_map  # Pass pipeline_columns
            )
            # Apply inheritance from global columns
            backend_columns = ConfigLoader._inherit_columns(backend_columns, global_columns)
        else:
            # If no backend-specific columns, use global columns
            backend_columns = global_columns.copy()
        
        processed_backend['columns'] = [Column(**x) for x in backend_columns]
        
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
    def _process_strategy(strategy_dict: Dict[str, Any], pipeline_columns_map: Dict[str, Dict[str, Any]] = None) -> StrategyConfig:
        """Process strategy configuration into StrategyConfig object"""
        if pipeline_columns_map is None:
            pipeline_columns_map = {}
        
        processed_strategy = strategy_dict.copy()

        # Process partition configs with data_type inheritance
        if 'primary_partitions' in processed_strategy:
            processed_strategy['primary_partitions'] = [
                ConfigLoader._process_partition_config(x, pipeline_columns_map) 
                for x in processed_strategy['primary_partitions']
            ]
        if 'secondary_partitions' in processed_strategy:
            processed_strategy['secondary_partitions'] = [
                ConfigLoader._process_partition_config(x, pipeline_columns_map)
                for x in processed_strategy['secondary_partitions']
            ]
        if 'delta_partitions' in processed_strategy:
            processed_strategy['delta_partitions'] = [
                ConfigLoader._process_partition_config(x, pipeline_columns_map)
                for x in processed_strategy['delta_partitions']
            ]
        
        # Convert type string to SyncStrategy enum
        if 'type' in processed_strategy and isinstance(processed_strategy['type'], str):
            try:
                processed_strategy['type'] = SyncStrategy(processed_strategy['type'])
            except ValueError:
                raise ValueError(f"Invalid strategy type: {processed_strategy['type']}")
        

        
        return StrategyConfig(**processed_strategy)

    @staticmethod
    def _process_partition_config(partition_dict: Dict[str, Any], pipeline_columns_map: Dict[str, Dict[str, Any]]) -> DimensionPartitionConfig:
        """Process partition config with data_type inheritance from pipeline_columns"""
        partition_dict = partition_dict.copy()
        
        # Inherit data_type from pipeline_columns if not specified
        column_name = partition_dict.get('column')
        if column_name and column_name in pipeline_columns_map:
            pipeline_col = pipeline_columns_map[column_name]
            
            # Only inherit data_type if not already specified
            if 'data_type' not in partition_dict and 'data_type' in pipeline_col:
                data_type = pipeline_col['data_type']
                # Convert to enum if string
                if isinstance(data_type, str):
                    try:
                        data_type = UniversalDataType(data_type)
                    except ValueError:
                        pass
                partition_dict['data_type'] = data_type
        
        return DimensionPartitionConfig(**partition_dict)

    @staticmethod
    def _inherit_columns(stage_columns: List[Dict[str,Any]], global_columns: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
        """Apply column inheritance - stage columns override global columns by name"""
        # Create a map of global columns by name
        global_column_map = {col['name']: col for col in global_columns}
        
        # Create a map of stage columns by name
        stage_column_map = {col['name']: col for col in stage_columns}
        
        # Start with global columns and override with stage-specific ones
        result_columns = []
        
        # # Add all global columns, potentially overridden by stage columns
        # for global_col in global_columns:
        #     if global_col['name'] in stage_column_map:
        #         # Use stage column but inherit missing fields from global
        #         stage_col = stage_column_map[global_col['name']]
        #         inherited_col = ConfigLoader._merge_column_properties(global_col, stage_col)
        #         result_columns.append(inherited_col)
        #     else:
        #         # Use global column as-is
        #         result_columns.append(global_col)
        
        # Add any stage columns that don't exist in global columns
        for stage_col in stage_columns:
            if stage_col['name'] in global_column_map:
                global_col = global_column_map[stage_col['name']]
                inherited_col = ConfigLoader._merge_column_properties(global_col, stage_col)
                result_columns.append(inherited_col)
            else:
                result_columns.append(stage_col)
        
        return result_columns
    
    @staticmethod
    def _merge_column_properties(global_col: Dict[str,Any], stage_col: Dict[str,Any]) -> Dict[str,Any]:
        """Merge column properties, with stage column taking precedence"""
        # Create a new column with global properties as defaults
        merged_dict = global_col.copy()
        merged_dict.update(stage_col)
        

        # merged_dict = {
        #     'name': stage_col['name'],
        #     'expr': stage_col['expr'] if stage_col.get('expr') != stage_col['name'] else global_col.get('expr',None),
        #     'data_type': stage_col['data_type'] if stage_col.get('data_type') is not None else global_col.get('data_type',None),
        #     'hash_column': stage_col['hash_column'] if (stage_col.get('hash_column') is not None) else global_col.get('hash_column',False),
        #     'data_column': stage_col['data_column'] if (stage_col.get('data_column') is not None) else global_col.get('data_column',False),
        #     'unique_column': stage_col['unique_column'] if (stage_col.get('unique_column') is not None) else global_col.get('unique_column',False),
        #     'order_column': stage_col['order_column'] if (stage_col.get('order_column') is not None) else global_col.get('order_column',False),
        #     'direction': stage_col['direction'] if (stage_col.get('direction') is not None) else global_col.get('direction','asc'),
        #     'delta_column': stage_col['delta_column'] if (stage_col.get('delta_column') is not None) else global_col.get('delta_column',False),
        #     'partition_column': stage_col['partition_column'] if (stage_col.get('partition_column') is not None) else global_col.get('partition_column',False),
        #     'hash_key': stage_col['hash_key'] if (stage_col.get('hash_key') is not None) else global_col.get('hash_key',False),
        # }
        
        return merged_dict
    
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
                
                # # Check if strategy has either column or partition_column specified
                # if  not strategy.partition_column:
                #     issues.append(f"Strategy '{strategy.name}' in stage '{stage.name}' must specify 'partition_column'")
        
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

    @staticmethod
    def validate_datastore_references(job_config: PipelineJobConfig, data_storage: DataStorage) -> List[str]:
        """Validate that all referenced datastores exist in the job configuration"""
        issues = []
        
        if not data_storage:
            issues.append("No datastores configuration provided")
            return issues
        
        # Check all stages for datastore references
        for stage in job_config.stages:
            # Check source backend datastore
            if stage.source and stage.source.datastore_name:
                datastore_name = stage.source.datastore_name
                if not data_storage.get_datastore(datastore_name):
                    issues.append(f"Stage '{stage.name}' source references unknown datastore: {datastore_name}")
            
            # Check destination backend datastore
            if stage.destination and stage.destination.datastore_name:
                datastore_name = stage.destination.datastore_name
                if not data_storage.get_datastore(datastore_name):
                    issues.append(f"Stage '{stage.name}' destination references unknown datastore: {datastore_name}")
        
        return issues

    @staticmethod
    def validate_pipeline_with_datastores(job_config: PipelineJobConfig, data_storage: DataStorage) -> List[str]:
        """Comprehensive validation of pipeline configuration with datastores"""
        issues = []
        
        # Validate basic config structure
        config_issues = ConfigLoader.validate_config(job_config)
        issues.extend(config_issues)
        
        # Validate datastores configuration
        datastore_issues = ConfigLoader.validate_datastores_config(data_storage)
        issues.extend(datastore_issues)
        
        # Validate datastore references
        reference_issues = ConfigLoader.validate_datastore_references(job_config, data_storage)
        issues.extend(reference_issues)
        
        return issues
