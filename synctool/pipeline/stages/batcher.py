import asyncio
from typing import AsyncIterator, List, Dict, Any, TYPE_CHECKING, Optional
from ..base import PipelineStage, DataBatch
from ...core.models import GlobalStageConfig, DataStorage
from ...core.enums import DataStatus

if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager


class BatcherStage(PipelineStage[DataBatch, DataBatch]):
    """Stage that re-batches data to optimize downstream processing"""
    def __init__(self, sync_engine: 'SyncEngine', config: GlobalStageConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        super().__init__(config.name, config, logger)
        self.target_batch_size = self.config.config.get('target_batch_size', 1000)
        # self.max_wait_time_ms = self.config.get('max_wait_time_ms', 5000)
        self.preserve_batch_metadata = self.config.config.get('preserve_batch_metadata', False)
        self.progress_manager = progress_manager
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Re-batch the input stream by change type"""
        # Dictionary to hold batches grouped by change type
        change_type_batches: Dict[DataStatus, Dict[str, Any]] = {}
        current_context = None
        processed_partitions = []


        
        async for batch in input_stream:
            # Set context from first batch
            
            if current_context is None:
                current_context = batch.context

            partition = batch.batch_metadata.get("partition")
            partition_id = partition.partition_id if partition else None
            if partition_id not in processed_partitions:
                processed_partitions.append(partition_id)
                self.progress_manager.update_progress(processed_partitions=1)
            
            # Get change type from batch metadata
            change_type = batch.batch_metadata.get("change_type")
            if not change_type:
                # If no change type specified, treat as MODIFIED for backwards compatibility
                change_type = DataStatus.MODIFIED
            
            # If the change type is deleted, yield the batch and continue
            if change_type == DataStatus.DELETED:
                yield batch
                continue
            
            # Initialize change type batch if it doesn't exist
            if change_type not in change_type_batches:
                change_type_batches[change_type] = {
                    "data": [],
                    "metadata": {},
                    "batch_count": 0
                }
            
            # Add data to the appropriate change type batch
            change_type_batches[change_type]["data"].extend(batch.data)
            
            # Accumulate metadata if configured
            if self.preserve_batch_metadata:
                self._accumulate_metadata(
                    change_type_batches[change_type]["metadata"], 
                    batch.batch_metadata
                )
            
            # Check if any change type batch is ready to yield
            for ct, batch_info in change_type_batches.items():
                if len(batch_info["data"]) >= self.target_batch_size:
                    # Yield batch for this change type
                    batch_info["batch_count"] += 1
                    rebatched_data = batch_info["data"][:self.target_batch_size]
                    
                    yield DataBatch(
                        data=rebatched_data,
                        context=current_context,
                        batch_metadata={
                            "rebatched": True,
                            "change_type": ct,
                            "original_batch_count": batch_info["batch_count"],
                            "target_size": self.target_batch_size,
                            **batch_info["metadata"]
                        }
                    )
                    
                    # Keep remaining data for next batch
                    batch_info["data"] = batch_info["data"][self.target_batch_size:]
                    # Reset accumulated metadata for next batch
                    batch_info["metadata"] = {}
        
        # Yield any remaining data for each change type
        for change_type, batch_info in change_type_batches.items():
            if batch_info["data"] and current_context:
                batch_info["batch_count"] += 1
                # import pdb; pdb.set_trace()
                yield DataBatch(
                    data=batch_info["data"],
                    context=current_context,
                    batch_metadata={
                        "rebatched": True,
                        "change_type": change_type,
                        "original_batch_count": batch_info["batch_count"],
                        "final_batch": True,
                        **batch_info["metadata"]
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
