from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import yaml
import json
# from ..state import get_state  # Not needed for this router

router = APIRouter(prefix="/api/configure", tags=["configure"])

class DatabaseConnection(BaseModel):
    type: str  # postgres, clickhouse, starrocks, mysql
    host: str
    port: int
    database: str
    user: str
    password: str
    db_schema: Optional[str] = None

class TableInfo(BaseModel):
    table_name: str
    alias: str
    schema_name: Optional[str] = None

class FilterCondition(BaseModel):
    field: str
    operator: str  # =, !=, >, <, >=, <=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL
    value: Optional[Any] = None
    table_alias: Optional[str] = None

class JoinCondition(BaseModel):
    table: str
    alias: str
    on: str
    type: str  # INNER, LEFT, RIGHT, FULL

class EnrichmentTransformation(BaseModel):
    columns: List[str]
    transform: str
    dest: str
    dtype: str

class SchemaExtractionRequest(BaseModel):
    source_connection: DatabaseConnection
    table_name: str
    additional_tables: Optional[List[TableInfo]] = []

class ColumnMapping(BaseModel):
    name: str
    src: Optional[str] = None
    dest: Optional[str] = None
    dtype: Optional[str] = None
    unique_key: bool = False  # For sync operations, not database constraints
    order_key: bool = False
    hash_key: bool = False
    insert: bool = True
    direction: Optional[str] = "asc"

class MigrationConfig(BaseModel):
    name: str
    description: str
    source_provider: Dict[str, Any]
    destination_provider: Dict[str, Any]
    column_map: List[ColumnMapping]
    partition_key: Optional[str] = None
    partition_step: Optional[int] = None
    # New fields for enhanced functionality
    source_filters: Optional[List[FilterCondition]] = None
    destination_filters: Optional[List[FilterCondition]] = None
    joins: Optional[List[JoinCondition]] = None
    strategies: Optional[List[Dict[str, Any]]] = None
    enrichment: Optional[Dict[str, Any]] = None

class DDLGenerationRequest(BaseModel):
    config: MigrationConfig
    destination_connection: DatabaseConnection

class GenerateSuggestedConfigRequest(BaseModel):
    schema_data: Dict[str, Any]
    selected_columns: List[str]

@router.post("/extract-schema")
async def extract_schema(request: SchemaExtractionRequest):
    """Extract schema from source database"""
    try:
        from synctool.backend import PostgresBackend, ClickHouseBackend, StarRocksBackend, MySQLBackend
        from synctool.core.models import BackendConfig
        
        # Create backend config
        backend_config = BackendConfig(
            type=request.source_connection.type,
            connection={
                'host': request.source_connection.host,
                'port': request.source_connection.port,
                'database': request.source_connection.database,
                'user': request.source_connection.user,
                'password': request.source_connection.password,
                'schema': request.source_connection.db_schema
            },
            table=request.table_name
        )
        
        # Create backend instance
        if request.source_connection.type == 'postgres':
            backend = PostgresBackend(backend_config)
        elif request.source_connection.type == 'clickhouse':
            backend = ClickHouseBackend(backend_config)
        elif request.source_connection.type == 'starrocks':
            backend = StarRocksBackend(backend_config)
        elif request.source_connection.type == 'mysql':
            backend = MySQLBackend(backend_config)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported database type: {request.source_connection.type}")
        
        # Extract schema using backend method
        await backend.connect()
        universal_schema = await backend.extract_table_schema(request.table_name)
        await backend.disconnect()
        
        # Convert to JSON-serializable format
        schema_data: Dict[str, Any] = {
            'table_name': universal_schema.table_name,
            'schema_name': universal_schema.schema_name,
            'database_name': universal_schema.database_name,
            'columns': [
                {
                    'name': col.name,
                    'data_type': col.data_type.value,
                    'nullable': col.nullable,
                    'primary_key': col.primary_key,
                    'unique': col.unique,
                    'auto_increment': col.auto_increment,
                    'default_value': col.default_value,
                    'max_length': col.max_length,
                    'comment': col.comment,
                    'table_alias': 'main' if request.additional_tables else None  # Main table alias
                }
                for col in universal_schema.columns
            ],
            'primary_keys': universal_schema.primary_keys,
            'indexes': universal_schema.indexes,
            'tables': [
                {
                    'table_name': request.table_name,
                    'alias': 'main' if request.additional_tables else None,
                    'schema_name': request.source_connection.db_schema
                }
            ]
        }
        
        # Add additional tables if provided
        if request.additional_tables:
            for table_info in request.additional_tables:
                # Extract schema for additional table
                additional_backend_config = BackendConfig(
                    type=request.source_connection.type,
                    connection={
                        'host': request.source_connection.host,
                        'port': request.source_connection.port,
                        'database': request.source_connection.database,
                        'user': request.source_connection.user,
                        'password': request.source_connection.password,
                        'schema': table_info.schema_name or request.source_connection.db_schema
                    },
                    table=table_info.table_name
                )
                
                await backend.connect()
                additional_schema = await backend.extract_table_schema(table_info.table_name)
                await backend.disconnect()
                
                # Add columns from additional table
                columns_list = schema_data['columns']
                for col in additional_schema.columns:
                    columns_list.append({
                        'name': col.name,
                        'data_type': col.data_type.value,
                        'nullable': col.nullable,
                        'primary_key': col.primary_key,
                        'unique': col.unique,
                        'auto_increment': col.auto_increment,
                        'default_value': col.default_value,
                        'max_length': col.max_length,
                        'comment': col.comment,
                        'table_alias': table_info.alias
                    })
                
                # Add table info
                tables_list = schema_data['tables']
                tables_list.append({
                    'table_name': table_info.table_name,
                    'alias': table_info.alias,
                    'schema_name': table_info.schema_name
                })
        
        return {
            'success': True,
            'schema': schema_data
        }
        
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema extraction failed: {str(e)}")

@router.post("/generate-suggested-config")
async def generate_suggested_config(request: GenerateSuggestedConfigRequest):
    """Generate suggested configuration based on selected columns"""
    try:
        # Filter columns based on selection
        selected_columns = []
        for col in request.schema_data['columns']:
            if col['name'] in request.selected_columns:
                selected_columns.append(col)
        
        # Create a mock universal schema with selected columns
        from synctool.backend.base_backend import UniversalSchema, UniversalColumn, UniversalDataType
        
        columns = []
        for col_data in selected_columns:
            column = UniversalColumn(
                name=col_data['name'],
                data_type=UniversalDataType(col_data['data_type']),
                nullable=col_data['nullable'],
                primary_key=col_data['primary_key'],
                unique=col_data['unique'],
                auto_increment=col_data['auto_increment'],
                default_value=col_data.get('default_value'),
                max_length=col_data.get('max_length'),
                comment=col_data.get('comment')
            )
            columns.append(column)
        
        mock_schema = UniversalSchema(
            table_name=request.schema_data['table_name'],
            columns=columns,
            primary_keys=[col.name for col in columns if col.primary_key]
        )
        
        # Generate config based on selected columns
        config = _generate_suggested_config_from_columns(mock_schema, selected_columns)
        
        return config
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate suggested config: {str(e)}")

@router.post("/generate-ddl")
async def generate_ddl(request: DDLGenerationRequest):
    """Generate DDL for destination database"""
    try:
        from synctool.backend import PostgresBackend, ClickHouseBackend, StarRocksBackend, MySQLBackend
        from synctool.core.models import BackendConfig
        
        # Create destination backend for DDL generation
        backend_config = BackendConfig(
            type=request.destination_connection.type,
            connection={
                'host': request.destination_connection.host,
                'port': request.destination_connection.port,
                'database': request.destination_connection.database,
                'user': request.destination_connection.user,
                'password': request.destination_connection.password,
                'schema': request.destination_connection.db_schema
            }
        )
        
        # Create backend instance
        if request.destination_connection.type == 'postgres':
            backend = PostgresBackend(backend_config)
        elif request.destination_connection.type == 'clickhouse':
            backend = ClickHouseBackend(backend_config)
        elif request.destination_connection.type == 'starrocks':
            backend = StarRocksBackend(backend_config)
        elif request.destination_connection.type == 'mysql':
            backend = MySQLBackend(backend_config)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported database type: {request.destination_connection.type}")
        
        # Convert config to UniversalSchema
        universal_schema = _config_to_universal_schema(request.config)
        
        # Generate DDL using backend method
        ddl = backend.generate_create_table_ddl(universal_schema)
        
        return {
            'success': True,
            'ddl': ddl,
            'config_yaml': _config_to_yaml(request.config),
            'config_json': _config_to_json(request.config)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DDL generation failed: {str(e)}")

@router.post("/validate-config")
async def validate_config(config: MigrationConfig):
    """Validate migration configuration"""
    try:
        # Basic validation
        errors = []
        
        if not config.name:
            errors.append("Name is required")
        
        if not config.column_map:
            errors.append("At least one column mapping is required")
        
        # Check for required columns
        has_unique_key = any(col.unique_key for col in config.column_map)
        if not has_unique_key:
            errors.append("At least one unique key is required for sync operations")
        
        # Check for required key configuration
        if not config.partition_key:
            errors.append("Partition key is required")
        
        if not config.partition_step:
            errors.append("Partition step is required")
        
        if config.partition_key and not any(col.name == config.partition_key for col in config.column_map):
            errors.append(f"Partition key '{config.partition_key}' not found in column map")
        
        # Validate strategies if present
        if config.strategies:
            # Check that only one strategy of each type is enabled
            enabled_strategies = [s for s in config.strategies if s.get('enabled')]
            enabled_types = [s.get('type') for s in enabled_strategies]
            duplicate_enabled_types = [t for t in set(enabled_types) if enabled_types.count(t) > 1]
            
            if duplicate_enabled_types:
                errors.append(f"Multiple strategies of the same type cannot be enabled simultaneously: {', '.join(str(t) for t in duplicate_enabled_types)}")
            
            for i, strategy in enumerate(config.strategies):
                if strategy.get('enabled') and strategy.get('column') and not any(col.name == strategy['column'] for col in config.column_map):
                    errors.append(f"Strategy '{strategy.get('name', f'Strategy {i+1}')}' column '{strategy['column']}' not found in column map")
        
        # Validate joins if multiple tables are involved
        if config.joins and len(config.joins) > 0:
            for join in config.joins:
                if not join.table or not join.alias or not join.on:
                    errors.append("Join conditions must specify table, alias, and join condition")
        
        # Validate enrichment if present
        if config.enrichment and config.enrichment.get('enabled'):
            transformations = config.enrichment.get('transformations', [])
            for i, transform in enumerate(transformations):
                if not transform.get('columns') or not transform.get('transform') or not transform.get('dest'):
                    errors.append(f"Enrichment transformation {i+1} must specify columns, transform, and destination")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
        
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Validation failed: {str(e)}"]
        }

# def _generate_suggested_config(universal_schema, source_connection: DatabaseConnection, additional_tables: List[TableInfo]) -> Dict[str, Any]:
#     """Generate suggested configuration based on extracted schema"""
#     column_map = []
    
#     for col in universal_schema.columns:
#         column_mapping = {
#             'name': col.name,
#             'src': col.name,  # Default to same name
#             'dest': col.name,
#             'dtype': col.data_type.value,
#             'unique_key': col.primary_key,  # Suggest primary keys as unique keys for sync
#             'order_key': col.primary_key,   # Suggest primary keys as order keys
#             'hash_key': False,  # Default to false
#             'insert': True,
#             'direction': 'asc' if col.primary_key else None  # 'asc' if order_key is True, else null
#         }
#         column_map.append(column_mapping)
    
#     # Add a computed hash key column for sync operations
#     hash_key_column = {
#         'name': 'checksum',
#         'src': None,  # Computed column
#         'dest': 'checksum',
#         'dtype': 'varchar',
#         'unique_key': False,
#         'order_key': False,
#         'hash_key': True,  # This is the hash key for change detection
#         'insert': True,
#         'direction': None  # No direction for hash key column
#     }
#     column_map.append(hash_key_column)
    
#     # Build source provider config
#     source_provider = {
#         'data_backend': {
#             'type': source_connection.type,
#             'connection': {
#                 'host': source_connection.host,
#                 'port': source_connection.port,
#                 'database': source_connection.database,
#                 'user': source_connection.user,
#                 'password': source_connection.password,
#                 'schema': source_connection.db_schema
#             },
#             'table': universal_schema.table_name,
#             'alias': 'main'
#         }
#     }
    
#     # Add joins if additional tables are provided
#     if additional_tables:
#         joins = []
#         for table_info in additional_tables:
#             joins.append({
#                 'table': table_info.table_name,
#                 'alias': table_info.alias,
#                 'on': f"main.id = {table_info.alias}.main_id",  # Default join condition
#                 'type': 'LEFT'
#             })
#         source_provider['data_backend']['joins'] = joins
    
#     return {
#         'name': f'{universal_schema.table_name}_migration',
#         'description': f'Migration from {source_connection.type} to destination with sync configuration',
#         'source_provider': source_provider,
#         'destination_provider': {
#             'data_backend': {
#                 'type': 'postgres',  # Default destination
#                 'connection': {
#                     'host': 'localhost',
#                     'port': 5432,
#                     'database': 'destination_db',
#                     'user': 'user',
#                     'password': 'password'
#                 },
#                 'table': f'{universal_schema.table_name}_migrated'
#             }
#         },
#         'column_map': column_map,
#         'partition_key': universal_schema.primary_keys[0] if universal_schema.primary_keys else None,
#         'partition_step': 1000,
#         'source_filters': [],
#         'destination_filters': [],
#         'joins': additional_tables if additional_tables else None,
#         'enrichment': {
#             'enabled': False,
#             'transformations': []
#         }
#     }

def _generate_suggested_config_from_columns(universal_schema, selected_columns: List[Dict]) -> Dict[str, Any]:
    """Generate suggested configuration based on selected columns"""
    column_map = []
    primary_key = None
    for col_data in selected_columns:
        if col_data['primary_key']:
            primary_key = col_data['name']
        column_mapping = {
            'name': col_data['name'],
            'src': col_data['table_alias'] + '.' + col_data['name'] if col_data.get('table_alias') else col_data['name'],
            'dest': col_data['name'],
            'dtype': col_data['data_type'],
            'unique_key': col_data['primary_key'],
            'order_key': col_data['primary_key'],
            'hash_key': False,
            'insert': True,
            'direction': 'asc' if col_data['primary_key'] else None
        }
        column_map.append(column_mapping)
    
    # # Add a computed hash key column for sync operations
    # hash_key_column = {
    #     'name': 'checksum',
    #     'src': None,
    #     'dest': 'checksum',
    #     'dtype': 'varchar',
    #     'unique_key': False,
    #     'order_key': False,
    #     'hash_key': True,
    #     'insert': True,
    #     'direction': None
    # }
    # column_map.append(hash_key_column)
    
    return {
        'name': f'{universal_schema.table_name}_migration',
        'description': f'Migration with selected columns',
        'source_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'source_db',
                    'user': 'user',
                    'password': 'password',
                    'schema': 'public'
                },
                'table': universal_schema.table_name,
                'alias': 'main'
            }
        },
        'destination_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'destination_db',
                    'user': 'user',
                    'password': 'password'
                },
                'table': f'{universal_schema.table_name}_migrated'
            }
        },
        'column_map': column_map,
        'partition_key': universal_schema.primary_keys[0] if universal_schema.primary_keys else None,
        'partition_step': 1000,
        'source_filters': [],
        'destination_filters': [],
        'joins': None,
        'strategies':[
            {
                'name': 'delta',
                'enabled': False,
                'type': 'delta',
                'column': None,
                'sub_partition_step': 30*60,
                'interval_reduction_factor': 2,
                'intervals': [],
                'prevent_update_unless_changed': True,
                'page_size': 1000,
                'cron': None
            },
            {
                'name': 'hash',
                'type': 'hash',
                'enabled': True,
                'column': primary_key,
                'sub_partition_step': 4000,
                'interval_reduction_factor': 2,
                'intervals': [],
                'prevent_update_unless_changed': True,
                'page_size': 1000,
                'cron': None
            },
            {
                'name': 'full', 
                'type': 'full',
                'enabled': True,
                'column': primary_key,
                'sub_partition_step': 5000,
                'interval_reduction_factor': 2,
                'intervals': [],
                'prevent_update_unless_changed': True,
                'page_size': 5000,
                'cron': None
            }
        ],
        'enrichment': {
            'enabled': False,
            'transformations': [
                {
                    'columns': ['hash__'],
                    'transform': 'lambda x: x["hash__"]',
                    'dest': 'checksum',
                    'dtype': 'varchar'
                }
            ]
        }
    }

def _config_to_universal_schema(config: MigrationConfig):
    """Convert migration config to UniversalSchema"""
    from synctool.backend.base_backend import UniversalSchema, UniversalColumn, UniversalDataType
    
    columns = []
    
    for col_mapping in config.column_map:
        column = UniversalColumn(
            name=col_mapping.dest or col_mapping.name,
            data_type=UniversalDataType(col_mapping.dtype) if col_mapping.dtype else UniversalDataType.TEXT,
            nullable=True,
            primary_key=col_mapping.unique_key,  # For sync operations
            unique=col_mapping.unique_key,
            auto_increment=False
        )
        columns.append(column)
    
    return UniversalSchema(
        table_name=config.destination_provider['data_backend']['table'],
        columns=columns,
        primary_keys=[col.name for col in columns if col.primary_key]
    )

def _config_to_yaml(config: MigrationConfig) -> str:
    """Convert config to YAML string"""
    config_dict = config.dict()
    return str(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))

def _config_to_json(config: MigrationConfig) -> str:
    """Convert config to JSON string"""
    config_dict = config.dict()
    return json.dumps(config_dict, indent=2)
