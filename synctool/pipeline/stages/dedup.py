import asyncio
from typing import AsyncIterator, List, Dict, Any, TYPE_CHECKING, Optional, Union, Tuple
from datetime import datetime
from uuid import UUID
from ..base import PipelineStage, DataBatch
from ...core.models import GlobalStageConfig, DataStorage, PipelineJobConfig

if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager


class DedupStage(PipelineStage[DataBatch, DataBatch]):
    """High-performance stage that deduplicates data based on specified key combinations, keeping the last occurrence"""
    
    def __init__(self, sync_engine: 'SyncEngine', config: GlobalStageConfig, pipeline_config: PipelineJobConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        super().__init__(config.name, config, pipeline_config, logger)
        self.dedup_keys = self.config.config.get('dedup_keys', [])
        self.progress_manager = progress_manager
        
        # Validate configuration
        if not self.dedup_keys:
            raise ValueError("DedupStage requires 'dedup_keys' to be specified in config")
        
        if isinstance(self.dedup_keys, str):
            self.dedup_keys = [self.dedup_keys]
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Deduplicate data in each batch, keeping the last occurrence of each unique key combination"""
        
        async for batch in input_stream:
            if batch.data:
                batch.data = self._deduplicate_data(batch.data)
            yield batch
    
    def _deduplicate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        High-performance deduplication that keeps the last occurrence of each unique key combination.
        Optimized for datetime, string, UUID, and int key types.
        """
        # import pdb; pdb.set_trace()
        if not data:
            return data
        
        # Use dict to maintain insertion order (Python 3.7+) and automatically handle duplicates
        # The key is the combination of dedup key values, value is the record
        unique_records = {}
        
        for record in data:
            try:
                # Build composite key tuple - optimized for common types
                key_parts = []
                for key_name in self.dedup_keys:
                    value = record.get(key_name)
                    
                    if value is None:
                        key_parts.append(None)
                    elif isinstance(value, (int, str)):
                        # Fast path for int and string (most common)
                        key_parts.append(value)
                    elif isinstance(value, datetime):
                        # Convert datetime to timestamp for consistent comparison
                        key_parts.append(value.timestamp())
                    elif isinstance(value, UUID):
                        # Convert UUID to string for comparison
                        key_parts.append(str(value))
                    else:
                        # Fallback for other types
                        key_parts.append(str(value))
                
                composite_key = tuple(key_parts)
                # This automatically overwrites previous occurrences, keeping the last one
                unique_records[composite_key] = record
                
            except (KeyError, AttributeError):
                # If key is missing or there's an error, include the record
                # Use a unique fallback key to avoid overwriting
                fallback_key = (id(record),)
                unique_records[fallback_key] = record
        
        # Return the values (last occurrence of each unique key combination)
        return list(unique_records.values())