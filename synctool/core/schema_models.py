
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum


class UniversalDataType(str,Enum):
    """Universal data types that map to all supported databases"""
    # Numeric types
    INTEGER = "integer"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    
    # String types
    VARCHAR = "varchar"
    TEXT = "text"
    CHAR = "char"
    
    # Date/Time types
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    
    # Boolean types
    BOOLEAN = "boolean"
    
    # Binary types
    BLOB = "blob"
    BINARY = "binary"
    
    # JSON types
    JSON = "json"
    
    # Special types
    UUID = "uuid"
    UUID_TEXT = "uuid_text"
    UUID_TEXT_DASH = "uuid_text_dash"


@dataclass
class UniversalColumn:
    """Universal column definition"""
    name: str
    data_type: UniversalDataType
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    auto_increment: bool = False
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None


@dataclass
class UniversalSchema:
    """Universal schema definition"""
    table_name: str
    columns: List[UniversalColumn]
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    primary_keys: Optional[List[str]] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    engine: Optional[str] = None
    comment: Optional[str] = None
    
    def __post_init__(self):
        if self.primary_keys is None:
            self.primary_keys = []
        if self.indexes is None:
            self.indexes = []