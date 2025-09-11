from typing import AsyncIterator, Dict, Any, List, Callable, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from ..base import  DataBatch, StageConfig
from ...core.models import DataStorage, TransformationConfig, GlobalStageConfig
from ...core.schema_models import UniversalDataType
from ...utils import safe_eval
from ..base import PipelineStage
if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager


@dataclass
class TransformStageConfig(StageConfig):
    transformations: List[TransformationConfig] = field(default_factory=list)

class TransformStage(PipelineStage):
    """Stage that applies data transformations"""
    
    def __init__(self, sync_engine: 'SyncEngine', config: GlobalStageConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        # stage_config = ChangeDetectionConfig(**config)
        config = TransformStageConfig.from_global_stage_config(config)
        super().__init__(config.name, config, logger)
        self.transformations = self._build_transformations(config.transformations)
        self.progress_manager = progress_manager
    
    def _build_transformations(self, transformations: List[TransformationConfig]) -> List[Callable]:
        """Build transformation functions from config"""      
        for transform_config in transformations:
            if isinstance(transform_config.transform, str) and transform_config.transform.startswith('lambda'):
                transform_config.transform = safe_eval(transform_config.transform)
        return transformations
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Process the input stream and yield output"""
        async for batch in input_stream:
            yield await self.process_batch(batch)
    
    # def _create_transform_function(self, config: Dict[str, Any]) -> Callable:
    #     """Create a transformation function from config"""
    #     transform_type = config.get('type')
        
    #     if transform_type == 'column_rename':
    #         mapping = config.get('mapping', {})
    #         return lambda row: {mapping.get(k, k): v for k, v in row.items()}
        
    #     elif transform_type == 'column_cast':
    #         cast_config = config.get('casts', {})
    #         return lambda row: self._cast_columns(row, cast_config)
        
    #     elif transform_type == 'column_add':
    #         column_name = config.get('column')
    #         default_value = config.get('default_value')
    #         return lambda row: {**row, column_name: default_value}
        
    #     elif transform_type == 'column_remove':
    #         columns_to_remove = config.get('columns', [])
    #         return lambda row: {k: v for k, v in row.items() if k not in columns_to_remove}
        
    #     elif transform_type == 'value_map':
    #         column = config.get('column')
    #         value_mapping = config.get('mapping', {})
    #         return lambda row: self._map_column_values(row, column, value_mapping)
        
    #     elif transform_type == 'custom_function':
    #         # Allow custom transformation functions
    #         func_name = config.get('function')
    #         return getattr(self, func_name, lambda row: row)
        
    #     else:
    #         # Default: no transformation
    #         return lambda row: row
    
    # def _cast_columns(self, row: Dict[str, Any], cast_config: Dict[str, str]) -> Dict[str, Any]:
    #     """Cast columns to specified types"""
    #     result = row.copy()
        
    #     for column, target_type in cast_config.items():
    #         if column in result:
    #             try:
    #                 if target_type == 'int':
    #                     result[column] = int(result[column]) if result[column] is not None else None
    #                 elif target_type == 'float':
    #                     result[column] = float(result[column]) if result[column] is not None else None
    #                 elif target_type == 'str':
    #                     result[column] = str(result[column]) if result[column] is not None else None
    #                 elif target_type == 'bool':
    #                     result[column] = bool(result[column]) if result[column] is not None else None
    #             except (ValueError, TypeError) as e:
    #                 self.logger.warning(f"Failed to cast {column} to {target_type}: {e}")
        
    #     return result
    
    # def _map_column_values(self, row: Dict[str, Any], column: str, value_mapping: Dict[str, Any]) -> Dict[str, Any]:
    #     """Map column values according to mapping configuration"""
    #     result = row.copy()
    #     if column in result:
    #         original_value = result[column]
    #         result[column] = value_mapping.get(original_value, original_value)
    #     return result
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Apply transformations to the batch data"""
        # import pdb; pdb.set_trace()
        if not batch.data or not self.transformations:
            return batch
        
        try:
            for row in batch.data:
                # Apply all transformations in sequence
                for transform_config in self.transformations:
                    transform_func = transform_config.transform
                    value = transform_func(row)
                    row[transform_config.expr] = value
            
            # batch.data = transformed_data
            batch.batch_metadata["transformed"] = True
            batch.batch_metadata["transform_count"] = len(self.transformations)
            
            self.logger.debug(f"Applied {len(self.transformations)} transformations to {len(batch.data)} rows")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Transformation failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["transform_error"] = str(e)
            raise
