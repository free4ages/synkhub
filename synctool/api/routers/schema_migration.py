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

class SchemaExtractionRequest(BaseModel):
    source_connection: DatabaseConnection
    table_name: str

class ColumnMapping(BaseModel):
    name: str
    src: Optional[str] = None
    dest: Optional[str] = None
    dtype: Optional[str] = None
    unique_column: bool = False  # For sync operations, not database constraints
    order_column: bool = False
    hash_key: bool = False
    insert: bool = True
    direction: Optional[str] = "asc"

class MigrationConfig(BaseModel):
    name: str
    description: str
    source_provider: Dict[str, Any]
    destination_provider: Dict[str, Any]
    column_map: List[ColumnMapping]
    partition_column: Optional[str] = None
    partition_step: Optional[int] = None

class DDLGenerationRequest(BaseModel):
    config: MigrationConfig
    destination_connection: DatabaseConnection

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
        schema_data = {
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
                    'comment': col.comment
                }
                for col in universal_schema.columns
            ],
            'primary_keys': universal_schema.primary_keys,
            'indexes': universal_schema.indexes
        }
        return {
            'success': True,
            'schema': schema_data,
            'suggested_config': _generate_suggested_config(universal_schema, request.source_connection)
        }
        
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema extraction failed: {str(e)}")

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
        has_unique_column = any(col.unique_column for col in config.column_map)
        if not has_unique_column:
            errors.append("At least one unique key is required for sync operations")
        
        if config.partition_column and not any(col.name == config.partition_column for col in config.column_map):
            errors.append(f"Partition key '{config.partition_column}' not found in column map")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
        
    except Exception as e:
        return {
            'valid': False,
            'errors': [f"Validation failed: {str(e)}"]
        }

def _generate_suggested_config(universal_schema, source_connection: DatabaseConnection) -> Dict[str, Any]:
    """Generate suggested configuration based on extracted schema"""
    column_map = []
    
    for col in universal_schema.columns:
        column_mapping = {
            'name': col.name,
            'src': col.name,  # Default to same name
            'dest': col.name,
            'dtype': col.data_type.value,
            'unique_column': col.primary_key,  # Suggest primary keys as unique keys for sync
            'order_column': col.primary_key,   # Suggest primary keys as order keys
            'hash_key': False,  # Default to false
            'insert': True,
            'direction': 'asc' if col.primary_key else None  # 'asc' if order_column is True, else null
        }
        column_map.append(column_mapping)
    
    # Add a computed hash key column for sync operations
    hash_key_column = {
        'name': 'checksum',
        'src': None,  # Computed column
        'dest': 'checksum',
        'dtype': 'varchar',
        'unique_column': False,
        'order_column': False,
        'hash_key': True,  # This is the hash key for change detection
        'insert': True,
        'direction': None  # No direction for hash key column
    }
    column_map.append(hash_key_column)
    
    return {
        'name': f'{universal_schema.table_name}_migration',
        'description': f'Migration from {source_connection.type} to destination with sync configuration',
        'source_provider': {
            'data_backend': {
                'type': source_connection.type,
                'connection': {
                    'host': source_connection.host,
                    'port': source_connection.port,
                    'database': source_connection.database,
                    'user': source_connection.user,
                    'password': source_connection.password,
                    'schema': source_connection.db_schema
                },
                'table': universal_schema.table_name
            }
        },
        'destination_provider': {
            'data_backend': {
                'type': 'postgres',  # Default destination
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
        'partition_column': universal_schema.primary_keys[0] if universal_schema.primary_keys else None,
        'partition_step': 1000
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
            primary_key=col_mapping.unique_column,  # For sync operations
            unique=col_mapping.unique_column,
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
    return yaml.dump(config_dict, default_flow_style=False, sort_keys=False)

def _config_to_json(config: MigrationConfig) -> str:
    """Convert config to JSON string"""
    config_dict = config.dict()
    return json.dumps(config_dict, indent=2)
