from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from ..state import app_state
from ...core.models import DataStorage, DataStore
from ...config.config_loader import ConfigLoader

router = APIRouter(prefix="/api/datastores", tags=["datastores"])


class DataStoreResponse(BaseModel):
    """Response model for datastore information"""
    name: str
    type: str
    description: Optional[str] = None
    tags: List[str] = []
    connection_summary: Dict[str, Any] = {}  # Sanitized connection info (no passwords)
    
    class Config:
        from_attributes = True


class DataStorageResponse(BaseModel):
    """Response model for all datastores"""
    datastores: List[DataStoreResponse]
    total_count: int


def get_data_storage() -> DataStorage:
    """Dependency to get DataStorage from scheduler"""
    from ...scheduler.file_scheduler import FileBasedScheduler
    
    # Try to get from app state first (if scheduler is running)
    scheduler = app_state.get("scheduler")
    if scheduler and hasattr(scheduler, 'get_data_storage'):
        data_storage = scheduler.get_data_storage()
        if data_storage:
            return data_storage
    
    # Fallback: load directly from config file
    try:
        config = app_state.get("scheduler_config")
        if config:
            config_dir = config.config_dir
            datastores_file = f"{config_dir}/datastores.yaml"
            return ConfigLoader.load_datastores_from_yaml(datastores_file)
    except Exception:
        pass
    
    # Return empty DataStorage if nothing found
    return DataStorage()


def sanitize_connection_info(connection_config) -> Dict[str, Any]:
    """Remove sensitive information from connection config"""
    if not connection_config:
        return {}
    
    # Convert to dict if it's a ConnectionConfig object
    if hasattr(connection_config, '__dict__'):
        conn_dict = connection_config.__dict__.copy()
    else:
        conn_dict = dict(connection_config)
    
    # Remove sensitive fields
    sensitive_fields = ['password', 'aws_secret_access_key', 'secret_key']
    for field in sensitive_fields:
        if field in conn_dict:
            conn_dict[field] = "***" if conn_dict[field] else None
    
    # Remove None values for cleaner response
    return {k: v for k, v in conn_dict.items() if v is not None}


@router.get("/", response_model=DataStorageResponse)
async def list_datastores(data_storage: DataStorage = Depends(get_data_storage)):
    """Get all available datastores"""
    datastores = []
    
    for name, datastore in data_storage.datastores.items():
        datastores.append(DataStoreResponse(
            name=datastore.name,
            type=datastore.type,
            description=datastore.description,
            tags=datastore.tags,
            connection_summary=sanitize_connection_info(datastore.connection)
        ))
    
    return DataStorageResponse(
        datastores=datastores,
        total_count=len(datastores)
    )


@router.get("/{datastore_name}", response_model=DataStoreResponse)
async def get_datastore(
    datastore_name: str, 
    data_storage: DataStorage = Depends(get_data_storage)
):
    """Get details of a specific datastore"""
    datastore = data_storage.get_datastore(datastore_name)
    
    if not datastore:
        raise HTTPException(
            status_code=404, 
            detail=f"Datastore '{datastore_name}' not found"
        )
    
    return DataStoreResponse(
        name=datastore.name,
        type=datastore.type,
        description=datastore.description,
        tags=datastore.tags,
        connection_summary=sanitize_connection_info(datastore.connection)
    )


@router.get("/types/summary")
async def get_datastore_types_summary(data_storage: DataStorage = Depends(get_data_storage)):
    """Get summary of datastores by type"""
    type_summary = {}
    
    for datastore in data_storage.datastores.values():
        if datastore.type not in type_summary:
            type_summary[datastore.type] = {
                "count": 0,
                "datastores": []
            }
        type_summary[datastore.type]["count"] += 1
        type_summary[datastore.type]["datastores"].append(datastore.name)
    
    return type_summary


@router.get("/tags/summary")
async def get_datastore_tags_summary(data_storage: DataStorage = Depends(get_data_storage)):
    """Get summary of datastores by tags"""
    tag_summary = {}
    
    for datastore in data_storage.datastores.values():
        for tag in datastore.tags:
            if tag not in tag_summary:
                tag_summary[tag] = {
                    "count": 0,
                    "datastores": []
                }
            tag_summary[tag]["count"] += 1
            tag_summary[tag]["datastores"].append(datastore.name)
    
    return tag_summary


@router.post("/test-connection/{datastore_name}")
async def test_datastore_connection(
    datastore_name: str,
    data_storage: DataStorage = Depends(get_data_storage)
):
    """Test connection to a specific datastore"""
    datastore = data_storage.get_datastore(datastore_name)
    
    if not datastore:
        raise HTTPException(
            status_code=404,
            detail=f"Datastore '{datastore_name}' not found"
        )
    
    try:
        # Create a backend instance to test connection
        from ...core.models import BackendConfig
        from ...core.provider import Provider
        
        # Create a temporary backend config
        backend_config = BackendConfig(
            type=datastore.type,
            datastore_name=datastore_name,
            table="test"  # Temporary table name for connection test
        )
        
        # Create provider with the datastore
        provider = Provider(
            config={"data_backend": backend_config.__dict__},
            data_storage=data_storage
        )
        
        # Test connection
        await provider.data_backend.connect()
        await provider.data_backend.disconnect()
        
        return {
            "status": "success",
            "message": f"Successfully connected to datastore '{datastore_name}'"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to connect to datastore '{datastore_name}': {str(e)}"
        }
