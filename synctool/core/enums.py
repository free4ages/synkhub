from enum import Enum


class SyncStrategy(str,Enum):
    FULL = "full"
    HASH = "hash" 
    DELTA = "delta"


class PartitionType(Enum):
    INT = "int"
    DATETIME = "datetime"


class BackendType(Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    CLICKHOUSE = "clickhouse"
    STARROCKS = "starrocks"
    STARROCKS_MYSQL = "starrocks_mysql"
    DUCKDB = "duckdb"
    REDIS = "redis"
    OBJECT_STORAGE = "object_storage"

class HashAlgo(str,Enum):
    MD5_SUM_HASH = "md5_sum_hash"
    HASH_MD5_HASH = "hash_md5_hash"

class DataStatus(str,Enum):
    ADDED = "A"
    MODIFIED = "M"
    UNCHANGED = "N"
    DELETED = "D"
    UNKNOWN = "U"


class Capability(str, Enum):
    """Backend capabilities that can be checked"""
    
    # Data fetching capabilities
    FETCH_HASH_WITH_DATA = "fetch_hash_with_data"
    FETCH_PARTITION_DATA = "fetch_partition_data"
    FETCH_DELTA_DATA = "fetch_delta_data"
    FETCH_CHILD_PARTITION_HASHES = "fetch_child_partition_hashes"
    FETCH_ROW_HASHES = "fetch_row_hashes"
    
    # Data writing capabilities
    UPSERT_SUPPORT = "upsert_support"
    BULK_INSERT = "bulk_insert"
    STREAM_INSERT = "stream_insert"
    BATCH_DELETE = "batch_delete"
    
    # Hash and comparison capabilities
    MD5_HASH_SUPPORT = "md5_hash_support"
    CUSTOM_HASH_FUNCTIONS = "custom_hash_functions"
    ROW_LEVEL_HASHING = "row_level_hashing"
    
    # Schema and metadata capabilities
    SCHEMA_EXTRACTION = "schema_extraction"
    DDL_GENERATION = "ddl_generation"
    DYNAMIC_SCHEMA = "dynamic_schema"
    
    # Partitioning capabilities
    PARTITION_SUPPORT = "partition_support"
    DATE_PARTITIONING = "date_partitioning"
    NUMERIC_PARTITIONING = "numeric_partitioning"
    
    # Connection and transaction capabilities
    CONNECTION_POOLING = "connection_pooling"
    TRANSACTION_SUPPORT = "transaction_support"
    ASYNC_OPERATIONS = "async_operations"
    
    # Storage-specific capabilities
    OBJECT_STORAGE = "object_storage"
    COLUMNAR_STORAGE = "columnar_storage"
    COMPRESSED_STORAGE = "compressed_storage"
    
    # Advanced features
    REAL_TIME_SYNC = "real_time_sync"
    CHANGE_DATA_CAPTURE = "change_data_capture"
    FULL_TEXT_SEARCH = "full_text_search"
    CUSTOM_AGGREGATE_FUNCTIONS = "custom_aggregate_functions"
    ADVANCED_COMPRESSION = "advanced_compression"
    LARGE_FILE_SUPPORT = "large_file_support"
    VERSIONING_SUPPORT = "versioning_support"

