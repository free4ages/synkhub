"""
Backend capability system for synctool.

This module defines the default capabilities for different storage types
and provides utilities for capability resolution.
"""

from typing import Dict, Set
from .enums import Capability

# Default capabilities for each storage type
_STORAGE_TYPE_CAPABILITIES: Dict[str, Set[Capability]] = {
    "postgres": {
        Capability.FETCH_HASH_WITH_DATA,
        Capability.FETCH_PARTITION_DATA,
        Capability.FETCH_DELTA_DATA,
        Capability.FETCH_ROW_HASHES,
        Capability.UPSERT_SUPPORT,
        Capability.BULK_INSERT,
        Capability.BATCH_DELETE,
        Capability.MD5_HASH_SUPPORT,
        Capability.ROW_LEVEL_HASHING,
        Capability.SCHEMA_EXTRACTION,
        Capability.DDL_GENERATION,
        Capability.PARTITION_SUPPORT,
        Capability.DATE_PARTITIONING,
        Capability.NUMERIC_PARTITIONING,
        Capability.CONNECTION_POOLING,
        Capability.TRANSACTION_SUPPORT,
        Capability.ASYNC_OPERATIONS,
    },
    
    "mysql": {
        Capability.FETCH_HASH_WITH_DATA,
        Capability.FETCH_PARTITION_DATA,
        Capability.FETCH_DELTA_DATA,
        Capability.FETCH_ROW_HASHES,
        Capability.UPSERT_SUPPORT,
        Capability.BULK_INSERT,
        Capability.BATCH_DELETE,
        Capability.MD5_HASH_SUPPORT,
        Capability.ROW_LEVEL_HASHING,
        Capability.SCHEMA_EXTRACTION,
        Capability.DDL_GENERATION,
        Capability.PARTITION_SUPPORT,
        Capability.DATE_PARTITIONING,
        Capability.NUMERIC_PARTITIONING,
        Capability.CONNECTION_POOLING,
        Capability.TRANSACTION_SUPPORT,
        Capability.ASYNC_OPERATIONS,
    },
    
    "clickhouse": {
        Capability.FETCH_HASH_WITH_DATA,
        Capability.FETCH_PARTITION_DATA,
        Capability.FETCH_DELTA_DATA,
        Capability.FETCH_CHILD_PARTITION_HASHES,
        Capability.FETCH_ROW_HASHES,
        Capability.BULK_INSERT,
        Capability.STREAM_INSERT,
        Capability.BATCH_DELETE,
        Capability.MD5_HASH_SUPPORT,
        Capability.CUSTOM_HASH_FUNCTIONS,
        Capability.ROW_LEVEL_HASHING,
        Capability.SCHEMA_EXTRACTION,
        Capability.DDL_GENERATION,
        Capability.PARTITION_SUPPORT,
        Capability.DATE_PARTITIONING,
        Capability.NUMERIC_PARTITIONING,
        Capability.CONNECTION_POOLING,
        Capability.ASYNC_OPERATIONS,
        Capability.COLUMNAR_STORAGE,
        Capability.COMPRESSED_STORAGE,
        Capability.REAL_TIME_SYNC,
    },
    
    "starrocks": {
        Capability.FETCH_HASH_WITH_DATA,
        Capability.FETCH_PARTITION_DATA,
        Capability.FETCH_DELTA_DATA,
        Capability.FETCH_ROW_HASHES,
        Capability.BULK_INSERT,
        Capability.STREAM_INSERT,
        Capability.UPSERT_SUPPORT,
        Capability.MD5_HASH_SUPPORT,
        Capability.ROW_LEVEL_HASHING,
        Capability.SCHEMA_EXTRACTION,
        Capability.DDL_GENERATION,
        Capability.PARTITION_SUPPORT,
        Capability.DATE_PARTITIONING,
        Capability.NUMERIC_PARTITIONING,
        Capability.CONNECTION_POOLING,
        Capability.ASYNC_OPERATIONS,
        Capability.COLUMNAR_STORAGE,
        Capability.COMPRESSED_STORAGE,
    },
    
    "duckdb": {
        Capability.FETCH_HASH_WITH_DATA,
        Capability.FETCH_PARTITION_DATA,
        Capability.FETCH_DELTA_DATA,
        Capability.FETCH_ROW_HASHES,
        Capability.BULK_INSERT,
        Capability.MD5_HASH_SUPPORT,
        Capability.ROW_LEVEL_HASHING,
        Capability.SCHEMA_EXTRACTION,
        Capability.DDL_GENERATION,
        Capability.PARTITION_SUPPORT,
        Capability.DATE_PARTITIONING,
        Capability.NUMERIC_PARTITIONING,
        Capability.ASYNC_OPERATIONS,
        Capability.COLUMNAR_STORAGE,
        Capability.COMPRESSED_STORAGE,
    },
    
    "object_storage": {
        Capability.BULK_INSERT,
        Capability.OBJECT_STORAGE,
        Capability.COMPRESSED_STORAGE,
        Capability.ASYNC_OPERATIONS,
        Capability.LARGE_FILE_SUPPORT,
        Capability.VERSIONING_SUPPORT,
    },
    
    "s3": {
        Capability.BULK_INSERT,
        Capability.OBJECT_STORAGE,
        Capability.COMPRESSED_STORAGE,
        Capability.ASYNC_OPERATIONS,
        Capability.LARGE_FILE_SUPPORT,
        Capability.VERSIONING_SUPPORT,
    },
    
    "gcs": {
        Capability.BULK_INSERT,
        Capability.OBJECT_STORAGE,
        Capability.COMPRESSED_STORAGE,
        Capability.ASYNC_OPERATIONS,
        Capability.LARGE_FILE_SUPPORT,
        Capability.VERSIONING_SUPPORT,
    },
    
    "elasticsearch": {
        Capability.BULK_INSERT,
        Capability.FULL_TEXT_SEARCH,
        Capability.DYNAMIC_SCHEMA,
        Capability.ASYNC_OPERATIONS,
        Capability.REAL_TIME_SYNC,
        Capability.COMPRESSED_STORAGE,
    },
}


def get_storage_capabilities(storage_type: str) -> Set[Capability]:
    """
    Get default capabilities for a storage type.
    
    Args:
        storage_type: The storage type (e.g., 'postgres', 'clickhouse', etc.)
        
    Returns:
        Set of capabilities supported by the storage type
    """
    return _STORAGE_TYPE_CAPABILITIES.get(storage_type, set())


def list_all_capabilities() -> Set[Capability]:
    """Get all available capabilities across all storage types."""
    all_caps = set()
    for caps in _STORAGE_TYPE_CAPABILITIES.values():
        all_caps.update(caps)
    return all_caps


def get_storage_types_with_capability(capability: Capability) -> Set[str]:
    """
    Get all storage types that support a specific capability.
    
    Args:
        capability: The capability to search for
        
    Returns:
        Set of storage type names that support the capability
    """
    storage_types = set()
    for storage_type, capabilities in _STORAGE_TYPE_CAPABILITIES.items():
        if capability in capabilities:
            storage_types.add(storage_type)
    return storage_types
