from typing import AsyncIterator, Dict, Any, TYPE_CHECKING, Optional, List
from dataclasses import dataclass, field

from synctool.core.query_models import RowHashMeta
from ..base import PipelineStage, DataBatch, StageConfig
from ...utils.data_comparator import calculate_row_status
from ...core.enums import DataStatus, HashAlgo, SyncStrategy
from ...core.models import DataStorage, BackendConfig, Column
from ...core.models import StrategyConfig


if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager

@dataclass
class DataFetchStageConfig(StageConfig):
    source: BackendConfig = None
    destination: BackendConfig = None
    columns: List[Column] = field(default_factory=list)
    hash_algo: HashAlgo = field(default=HashAlgo.HASH_MD5_HASH)
    enabled: bool = True


class DataFetchStage(PipelineStage):
    """Stage that fetches data from source systems"""
    
    def __init__(self, sync_engine: Any, config: Dict[str, Any] = None, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        config = DataFetchStageConfig.from_global_stage_config(config)
        super().__init__(config.name, config, logger)
        self.sync_engine = sync_engine
        self.source_backend = sync_engine.create_backend(self.config.source)
        self.destination_backend = None
        if self.config.destination:
            self.destination_backend = sync_engine.create_backend(self.config.destination)
        self.progress_manager = progress_manager
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        async for batch in input_stream:
            async for batch in self.process_batch(batch):
                yield batch
    
    async def setup(self, context: Any):
        await self.source_backend.connect()
        if self.destination_backend:
            await self.destination_backend.connect()
    async def teardown(self, context: Any):
        await self.source_backend.disconnect()
        if self.destination_backend:
            await self.destination_backend.disconnect()
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Fetch data for the batch"""
        metadata = batch.batch_metadata       
        partition = metadata["partition"]
        change_type = metadata["change_type"]
        job_context = batch.context
        strategy_config = job_context.metadata.get("strategy_config")
        
        try:
            if change_type in (DataStatus.ADDED, DataStatus.MODIFIED):
                async for batch in self._fetch_partition_row_hashes(self.source_backend, batch):
                    # import pdb; pdb.set_trace()
                    yield batch
                # if complete_partition:
                #     data = await self._fetch_partition_data(partition)
                # else:
                #     data = await self._fetch_partition_data(partition)
                # if change_type == "full_sync":
                #     data = await self._fetch_partition_data(partition)
                # elif change_type == "added":
                #     data = await self._fetch_partition_data(partition)
                # elif change_type == "modified":
                #     if metadata.get("needs_row_comparison", False):
                #         # Fetch row hashes for comparison and process them
                #         data = await self._fetch_and_compare_rows(partition, batch)
                #     else:
                #         data = await self._fetch_partition_data(partition)
                # else:
                #     data = await self._fetch_partition_data(partition)
                
                # batch.data = data
                # batch.batch_metadata["data_fetched"] = len(data)
                # batch.batch_metadata["fetch_queries"] = 1
                # self.logger.debug(f"Fetched {len(data)} rows for partition {partition.partition_id}")
                
            # return batch
            
        except Exception as e:
            self.logger.error(f"Failed to fetch data for partition {partition.partition_id}: {e}")
            batch.batch_metadata["fetch_error"] = str(e)
            raise
    
    # async def _fetch_partition_data(self, partition):
    #     """Fetch partition data with pagination support"""
    #     if self.use_pagination:
    #         all_data = []
    #         offset = 0
            
    #         while True:
    #             data = await self.sync_engine.source_provider.fetch_partition_data(
    #                 partition,
    #                 hash_algo=self.sync_engine.hash_algo,
    #                 page_size=self.page_size,
    #                 offset=offset
    #             )
                
    #             if not data:
    #                 break
                
    #             all_data.extend(data)
                
    #             if len(data) < self.page_size:
    #                 break
                
    #             offset += self.page_size
            
    #         return all_data
    #     else:
    #         return await self.sync_engine.source_provider.fetch_partition_data(
    #             partition, hash_algo=self.sync_engine.hash_algo
    #         )
    
    # async def _fetch_and_compare_rows(self, partition, batch: DataBatch):
    #     """Fetch row hashes, compare them, and return the appropriate data"""
    #     # Fetch row hashes from both source and destination
    #     src_hashes = await self.sync_engine.source_provider.fetch_partition_row_hashes(
    #         partition, hash_algo=self.sync_engine.hash_algo
    #     )
    #     dest_hashes = await self.sync_engine.destination_provider.fetch_partition_row_hashes(
    #         partition, hash_algo=self.sync_engine.hash_algo
    #     )
        
    #     batch.batch_metadata["hash_queries"] = 1
        
    #     if not src_hashes:
    #         return []
        
    #     # Compare hashes to determine what rows need processing
    #     unique_columns = [x.name for x in self.sync_engine.column_mapper.schemas["common"].unique_columns]
    #     calculated_rows, statuses = calculate_row_status(src_hashes, dest_hashes, unique_columns)
        
    #     # Separate rows by their status
    #     added_rows = []
    #     modified_rows = []
    #     deleted_rows = []
        
    #     for row, r_status in zip(calculated_rows, statuses):
    #         if r_status == DataStatus.ADDED:
    #             added_rows.append(row)
    #         elif r_status == DataStatus.MODIFIED:
    #             modified_rows.append(row)
    #         elif r_status == DataStatus.DELETED:
    #             deleted_rows.append(row)
        
    #     # Store the different types of rows in metadata for downstream processing
    #     batch.batch_metadata["added_rows"] = added_rows
    #     batch.batch_metadata["modified_rows"] = modified_rows
    #     batch.batch_metadata["deleted_rows"] = deleted_rows
    #     batch.batch_metadata["row_comparison_done"] = True
        
    #     # Return all rows that need to be processed (added + modified)
    #     # Note: deleted rows will be handled separately in the populate stage
    #     return added_rows + modified_rows

    async def _fetch_partition_row_hashes(self, backend, batch: DataBatch):
        """Fetch partition data with pagination support using generator pattern"""
        partition = batch.batch_metadata.get("partition")
        strategy_config = batch.context.metadata.get("strategy_config")
        job_context = batch.context
        batch_metadata = batch.batch_metadata or {}
        use_pagination = strategy_config.use_pagination
        page_size = strategy_config.page_size
        change_type = batch_metadata.get("change_type")
        if use_pagination:
            offset = 0
            
            while True:
                data = await backend.fetch_partition_row_hashes(
                    partition,
                    hash_algo=self.config.hash_algo,
                    page_size=page_size,
                    offset=offset
                )
                
                if not data:
                    break
                batch.data = data
                batch_metadata["paginated"] = True
                batch_metadata["page_size"] = page_size
                batch_metadata["offset"] = offset
                batch_metadata["complete_partition"] = False

                # In case of hash starategy, rows detected will be in change detection stage
                if strategy_config.type == SyncStrategy.HASH:
                    rows_detected = 0
                else:
                    rows_detected = len(data)

                # Update progress with fetched data count
                if self.progress_manager:
                    self.progress_manager.update_progress(rows_fetched=len(data), rows_detected=rows_detected, hash_query_count=1)

                batch.metadata = batch_metadata
                # Yield the batch for this page
                yield batch
                
                if len(data) < page_size:
                    break
                
                offset += page_size
        else:
            # For non-paginated case, still yield each row
            data = await backend.fetch_partition_row_hashes(
                partition, hash_algo=self.config.hash_algo
            )
            batch.data = data
            batch_metadata["paginated"] = False
            batch_metadata["complete_partition"] = True

            if strategy_config.type == SyncStrategy.HASH:
                rows_detected = 0
            else:
                rows_detected = len(data)
            # Update progress with fetched data count
            if self.progress_manager:
                self.progress_manager.update_progress(rows_fetched=len(data), rows_detected=rows_detected, hash_query_count=1)
            
            batch.metadata = batch_metadata
            yield batch
