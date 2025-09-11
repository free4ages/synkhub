from typing import AsyncIterator, Dict, Any, TYPE_CHECKING, Optional, List
from dataclasses import dataclass, field
import traceback
from ..base import PipelineStage, DataBatch, StageConfig
from ...core.enums import SyncStrategy, DataStatus
from ...core.models import DataStorage, BackendConfig, GlobalStageConfig, Column
from ...core.column_mapper import ColumnSchema


if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager

@dataclass
class PopulateStageConfig(StageConfig):
    destination: BackendConfig = None
    columns: List[Column] = field(default_factory=list)
    enabled: bool = True
    config: Dict[str, Any] = field(default_factory=dict)


class PopulateStage(PipelineStage):
    """Stage that populates data into destination systems"""
    
    def __init__(self, sync_engine: Any, config: GlobalStageConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        config = PopulateStageConfig.from_global_stage_config(config)
        super().__init__(config.name, config, logger)
        self.sync_engine = sync_engine
        self.columns = config.columns
        self.column_schema = ColumnSchema(config.columns)
        self.destination_backend = sync_engine.create_backend(self.config.destination)
        self.progress_manager = progress_manager
    
    async def setup(self, context: Any):
        await self.destination_backend.connect()
    
    async def teardown(self, context: Any):
        await self.destination_backend.disconnect()
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        async for batch in input_stream:
            yield await self.process_batch(batch)

    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Populate data to destination"""
        # import pdb; pdb.set_trace()
        metadata = batch.batch_metadata
        change_type = metadata.get("change_type")
        partition = metadata.get("partition")
        complete_partition = metadata.get("complete_partition")
        # strategy = metadata.get("strategy_type")
        
        try:
            rows_processed = 0
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            failed_partitions = set()
            
            if change_type == DataStatus.DELETED:
                # Handle deletion
                if partition and complete_partition:
                    try:
                        await self.destination_backend.delete_partition_data(partition)
                        rows_deleted = partition.num_rows
                        batch.batch_metadata["rows_deleted"] = rows_deleted
                    except Exception as e:
                        traceback.print_exc()
                        rows_failed = len(batch.data) or partition.num_rows
                        batch.batch_metadata["rows_failed"] = rows_failed
                    
                    # Update progress with deletion count
                    if self.progress_manager:
                        self.progress_manager.update_progress(rows_deleted=rows_deleted)
                else:
                    pass
            elif change_type == DataStatus.ADDED:
                try:
                    success, failed = await self.destination_backend.insert_data(batch, upsert=True)
                    rows_inserted = success
                    rows_failed = failed
                    batch.batch_metadata["rows_inserted"] = rows_inserted
                    batch.batch_metadata["rows_failed"] = rows_failed
                except Exception as e:
                    traceback.print_exc()
                    rows_failed = len(batch.data) or partition.num_rows
                    batch.batch_metadata["rows_failed"] = rows_failed
                
                # Update progress with insertion count
                if self.progress_manager:
                    self.progress_manager.update_progress(rows_inserted=rows_inserted, rows_failed=rows_failed)
                
            elif change_type == DataStatus.MODIFIED:
                try:
                    success, failed = await self.destination_backend.insert_data(batch, upsert=True)
                    rows_updated = success
                    rows_failed = failed
                    batch.batch_metadata["rows_failed"] = rows_failed
                    batch.batch_metadata["rows_updated"] = rows_updated
                except Exception as e:
                    traceback.print_exc()
                    rows_failed = len(batch.data) or partition.num_rows
                    batch.batch_metadata["rows_failed"] = rows_failed
                    batch.batch_metadata["rows_updated"] = rows_updated
                
                # Update progress with update count
                if self.progress_manager:
                    self.progress_manager.update_progress(rows_updated=rows_updated, rows_failed=rows_failed)
            if rows_failed > 0:
                for x in batch.data:
                    if x.get("failed__"):
                        failed_partitions.add(x["partition__"])
                self.progress_manager.mark_failed_partitions(list(failed_partitions))
                batch.batch_metadata["failed_partitions"] = list(failed_partitions)
            

            # batch.batch_metadata["populated"] = True
            # batch.batch_metadata["rows_processed"] = rows_inserted + rows_updated + rows_deleted
            
            
            self.logger.debug(f"Populated batch {batch.batch_id}: "
                            f"{rows_inserted} inserted, {rows_updated} updated, {rows_deleted} deleted")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Population failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["population_error"] = str(e)
            raise
