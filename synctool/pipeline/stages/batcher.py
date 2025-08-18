import asyncio
from typing import AsyncIterator, List, Dict, Any
from ..base import PipelineStage, DataBatch


class BatcherStage(PipelineStage[DataBatch, DataBatch]):
    """Stage that re-batches data to optimize downstream processing"""
    
    def __init__(self, config: Dict[str, Any] = None, logger=None):
        super().__init__("batcher", config, logger)
        self.target_batch_size = self.config.get('target_batch_size', 1000)
        self.max_wait_time_ms = self.config.get('max_wait_time_ms', 5000)
        self.preserve_batch_metadata = self.config.get('preserve_batch_metadata', True)
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Re-batch the input stream"""
        current_batch_data = []
        current_context = None
        batch_count = 0
        accumulated_metadata = {}
        
        async for batch in input_stream:
            # If this is the first batch or context changed, start new batch
            if current_context is None:
                current_context = batch.context
            
            # Accumulate metadata if configured
            if self.preserve_batch_metadata:
                self._accumulate_metadata(accumulated_metadata, batch.batch_metadata)
            
            # Add data to current batch
            current_batch_data.extend(batch.data)
            
            # Yield batch if it's large enough
            if len(current_batch_data) >= self.target_batch_size:
                batch_count += 1
                rebatched_data = current_batch_data[:self.target_batch_size]
                
                yield DataBatch(
                    data=rebatched_data,
                    context=current_context,
                    batch_metadata={
                        "rebatched": True,
                        "original_batch_count": batch_count,
                        "target_size": self.target_batch_size,
                        **accumulated_metadata
                    }
                )
                
                # Keep remaining data for next batch
                current_batch_data = current_batch_data[self.target_batch_size:]
                # Reset accumulated metadata for next batch
                accumulated_metadata = {}
        
        # Yield any remaining data
        if current_batch_data and current_context:
            batch_count += 1
            yield DataBatch(
                data=current_batch_data,
                context=current_context,
                batch_metadata={
                    "rebatched": True,
                    "original_batch_count": batch_count,
                    "final_batch": True,
                    **accumulated_metadata
                }
            )
    
    def _accumulate_metadata(self, accumulated: Dict[str, Any], batch_metadata: Dict[str, Any]):
        """Accumulate metadata from multiple batches"""
        for key, value in batch_metadata.items():
            if key in accumulated:
                # Handle different types of accumulation
                if isinstance(value, (int, float)):
                    accumulated[key] = accumulated[key] + value
                elif isinstance(value, list):
                    accumulated[key].extend(value)
                elif isinstance(value, bool):
                    accumulated[key] = accumulated[key] or value
                else:
                    # For other types, keep the latest value
                    accumulated[key] = value
            else:
                accumulated[key] = value
