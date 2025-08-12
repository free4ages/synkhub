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
    CLICKHOUSE = "clickhouse"
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

