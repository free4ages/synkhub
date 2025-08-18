from typing import AsyncIterator, Dict, Any, List, Callable
from ..base import BatchProcessor, DataBatch


class TransformStage(BatchProcessor):
    """Stage that applies data transformations"""
    
    def __init__(self, config: Dict[str, Any] = None, logger=None):
        super().__init__("transform", config, logger)
        self.transformations = self._build_transformations()
    
    def _build_transformations(self) -> List[Callable]:
        """Build transformation functions from config"""
        transformations = []
        transform_configs = self.config.get('transformations', [])
        
        for transform_config in transform_configs:
            transform_func = self._create_transform_function(transform_config)
            transformations.append(transform_func)
        
        return transformations
    
    def _create_transform_function(self, config: Dict[str, Any]) -> Callable:
        """Create a transformation function from config"""
        transform_type = config.get('type')
        
        if transform_type == 'column_rename':
            mapping = config.get('mapping', {})
            return lambda row: {mapping.get(k, k): v for k, v in row.items()}
        
        elif transform_type == 'column_cast':
            cast_config = config.get('casts', {})
            return lambda row: self._cast_columns(row, cast_config)
        
        elif transform_type == 'column_add':
            column_name = config.get('column')
            default_value = config.get('default_value')
            return lambda row: {**row, column_name: default_value}
        
        elif transform_type == 'column_remove':
            columns_to_remove = config.get('columns', [])
            return lambda row: {k: v for k, v in row.items() if k not in columns_to_remove}
        
        elif transform_type == 'value_map':
            column = config.get('column')
            value_mapping = config.get('mapping', {})
            return lambda row: self._map_column_values(row, column, value_mapping)
        
        elif transform_type == 'custom_function':
            # Allow custom transformation functions
            func_name = config.get('function')
            return getattr(self, func_name, lambda row: row)
        
        else:
            # Default: no transformation
            return lambda row: row
    
    def _cast_columns(self, row: Dict[str, Any], cast_config: Dict[str, str]) -> Dict[str, Any]:
        """Cast columns to specified types"""
        result = row.copy()
        
        for column, target_type in cast_config.items():
            if column in result:
                try:
                    if target_type == 'int':
                        result[column] = int(result[column]) if result[column] is not None else None
                    elif target_type == 'float':
                        result[column] = float(result[column]) if result[column] is not None else None
                    elif target_type == 'str':
                        result[column] = str(result[column]) if result[column] is not None else None
                    elif target_type == 'bool':
                        result[column] = bool(result[column]) if result[column] is not None else None
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to cast {column} to {target_type}: {e}")
        
        return result
    
    def _map_column_values(self, row: Dict[str, Any], column: str, value_mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Map column values according to mapping configuration"""
        result = row.copy()
        if column in result:
            original_value = result[column]
            result[column] = value_mapping.get(original_value, original_value)
        return result
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Apply transformations to the batch data"""
        if not batch.data or not self.transformations:
            return batch
        
        try:
            transformed_data = []
            
            for row in batch.data:
                transformed_row = row
                
                # Apply all transformations in sequence
                for transform_func in self.transformations:
                    transformed_row = transform_func(transformed_row)
                
                transformed_data.append(transformed_row)
            
            batch.data = transformed_data
            batch.batch_metadata["transformed"] = True
            batch.batch_metadata["transform_count"] = len(self.transformations)
            
            self.logger.debug(f"Applied {len(self.transformations)} transformations to {len(batch.data)} rows")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Transformation failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["transform_error"] = str(e)
            raise
