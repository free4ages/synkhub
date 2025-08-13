from typing import Any, Dict, List, Union
from datetime import datetime, date, time
import decimal
import uuid
import json

from ..core.schema_models import UniversalDataType, UniversalSchema


def cast_value(value: Any, target_type: UniversalDataType) -> Any:
    """
    Generic cast function that converts a value to the specified UniversalDataType.
    
    Args:
        value: The value to cast
        target_type: The target UniversalDataType to cast to
        
    Returns:
        The casted value
        
    Raises:
        ValueError: If the value cannot be cast to the target type
        TypeError: If the target type is not supported
    """
    if value is None:
        return None
    
    try:
        if target_type == UniversalDataType.INTEGER:
            return int(value)
        
        elif target_type == UniversalDataType.BIGINT:
            return int(value)
        
        elif target_type == UniversalDataType.SMALLINT:
            return int(value)
        
        elif target_type == UniversalDataType.FLOAT:
            return float(value)
        
        elif target_type == UniversalDataType.DOUBLE:
            return float(value)
        
        elif target_type == UniversalDataType.DECIMAL:
            if isinstance(value, (int, float, str)):
                return decimal.Decimal(str(value))
            elif isinstance(value, decimal.Decimal):
                return value
            else:
                raise ValueError(f"Cannot convert {type(value)} to decimal")
        
        elif target_type == UniversalDataType.VARCHAR:
            return str(value)
        
        elif target_type == UniversalDataType.TEXT:
            return str(value)
        
        elif target_type == UniversalDataType.CHAR:
            return str(value)
        
        elif target_type == UniversalDataType.DATE:
            if isinstance(value, str):
                return datetime.strptime(value, "%Y-%m-%d").date()
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, date):
                return value
            else:
                raise ValueError(f"Cannot convert {type(value)} to date")
        
        elif target_type == UniversalDataType.TIME:
            if isinstance(value, str):
                return datetime.strptime(value, "%H:%M:%S").time()
            elif isinstance(value, datetime):
                return value.time()
            elif isinstance(value, time):
                return value
            else:
                raise ValueError(f"Cannot convert {type(value)} to time")
        
        elif target_type == UniversalDataType.TIMESTAMP:
            if isinstance(value, str):
                # Try common timestamp formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Cannot parse timestamp string: {value}")
            elif isinstance(value, datetime):
                return value
            elif isinstance(value, (int, float)):
                # Unix timestamp
                return datetime.fromtimestamp(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to timestamp")
        
        elif target_type == UniversalDataType.DATETIME:
            if isinstance(value, str):
                # Try common datetime formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Cannot parse datetime string: {value}")
            elif isinstance(value, datetime):
                return value
            elif isinstance(value, (int, float)):
                # Unix timestamp
                return datetime.fromtimestamp(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to datetime")
        
        elif target_type == UniversalDataType.BOOLEAN:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                lower_val = value.lower()
                if lower_val in ('true', '1', 'yes', 'on'):
                    return True
                elif lower_val in ('false', '0', 'no', 'off'):
                    return False
                else:
                    raise ValueError(f"Cannot convert string '{value}' to boolean")
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to boolean")
        
        elif target_type == UniversalDataType.BLOB:
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode('utf-8')
            else:
                raise ValueError(f"Cannot convert {type(value)} to blob")
        
        elif target_type == UniversalDataType.BINARY:
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode('utf-8')
            else:
                raise ValueError(f"Cannot convert {type(value)} to binary")
        
        elif target_type == UniversalDataType.JSON:
            if isinstance(value, str):
                return json.loads(value)
            elif isinstance(value, (dict, list)):
                return value
            else:
                raise ValueError(f"Cannot convert {type(value)} to JSON")
        
        elif target_type == UniversalDataType.UUID:
            if isinstance(value, uuid.UUID):
                return value
            elif isinstance(value, str):
                return uuid.UUID(value)
            else:
                raise ValueError(f"Cannot convert {type(value)} to UUID")
        
        elif target_type == UniversalDataType.UUID_TEXT:
            if isinstance(value, uuid.UUID):
                return str(value)
            elif isinstance(value, str):
                # Try to parse as UUID first, then return as string
                try:
                    uuid.UUID(value)
                    return value
                except ValueError:
                    raise ValueError(f"Invalid UUID format: {value}")
            else:
                raise ValueError(f"Cannot convert {type(value)} to UUID_TEXT")
        
        elif target_type == UniversalDataType.UUID_TEXT_DASH:
            if isinstance(value, uuid.UUID):
                return str(value)
            elif isinstance(value, str):
                # Remove dashes if present, then add them back in standard format
                clean_uuid = value.replace('-', '')
                if len(clean_uuid) == 32:
                    formatted_uuid = f"{clean_uuid[:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:]}"
                    return formatted_uuid
                else:
                    raise ValueError(f"Invalid UUID format: {value}")
            else:
                raise ValueError(f"Cannot convert {type(value)} to UUID_TEXT_DASH")
        
        else:
            raise TypeError(f"Unsupported target type: {target_type}")
            
    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to cast {value} (type: {type(value)}) to {target_type}: {str(e)}")


def cast_data_to_schema(data: Union[Dict[str, Any], List[Dict[str, Any]]], schema: UniversalSchema) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Cast data to match the provided UniversalSchema.
    
    Args:
        data: Single dictionary or list of dictionaries to cast
        schema: UniversalSchema defining the target data types
        
    Returns:
        Casted data with the same structure but proper types
        
    Raises:
        ValueError: If data cannot be cast to the schema
    """
    # Create a mapping of column names to their data types
    column_types = {col.name: col.data_type for col in schema.columns}
    
    def cast_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """Cast a single row of data"""
        casted_row = {}
        for col_name, value in row.items():
            if col_name in column_types:
                try:
                    casted_row[col_name] = cast_value(value, column_types[col_name])
                except Exception as e:
                    raise ValueError(f"Failed to cast column '{col_name}' with value '{value}': {str(e)}")
            else:
                # Keep unknown columns as-is
                casted_row[col_name] = value
        return casted_row
    
    if isinstance(data, dict):
        return cast_row(data)
    elif isinstance(data, list):
        return [cast_row(row) for row in data]
    else:
        raise ValueError(f"Data must be a dictionary or list of dictionaries, got {type(data)}")
