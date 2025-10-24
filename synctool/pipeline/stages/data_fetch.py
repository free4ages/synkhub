from typing import AsyncIterator, Dict, Any, TYPE_CHECKING, Optional, List, Tuple, Generator
from dataclasses import dataclass, field

# from synctool.core.query_models import RowHashMeta
from ..base import PipelineStage, DataBatch, StageConfig
from ...utils.data_comparator import calculate_row_status
from ...core.enums import DataStatus, HashAlgo, SyncStrategy
from ...core.models import DataStorage, BackendConfig, Column
from ...core.models import StrategyConfig, PipelineJobConfig, MultiDimensionPartition, DimensionPartitionConfig
from ...utils.multi_dimensional_partition_generator import build_multi_dimension_partitions_for_delta_data



if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager

@dataclass
class DataFetchStageConfig(StageConfig):
    source: BackendConfig = None
    destination: BackendConfig = None
    columns: List[Column] = field(default_factory=list)
    # hash_algo: HashAlgo = field(default=HashAlgo.HASH_MD5_HASH)
    enabled: bool = True
    config: Dict[str, Any] = field(default_factory=dict)


class DataFetchStage(PipelineStage):
    """Stage that fetches data from source systems"""
    
    def __init__(self, sync_engine: Any, config: Dict[str, Any] , pipeline_config: PipelineJobConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        config = DataFetchStageConfig.from_global_stage_config(config)
        super().__init__(config.name, config, pipeline_config, logger)
        self.sync_engine = sync_engine
        self.source_backend = sync_engine.create_backend(self.config.source)
        self.destination_backend = None
        self.hash_algo = pipeline_config.hash_algo
        if self.config.destination:
            self.destination_backend = sync_engine.create_backend(self.config.destination)
        self.progress_manager = progress_manager
        # import pdb; pdb.set_trace()
        unique_columns = self.config.config.get("unique_columns", []) if self.config.config else []
        # if not unique_columns:
        #     src_column_schema = self.source_backend.column_schema
        #     unique_columns = [col.name for col in src_column_schema.unique_columns] if src_column_schema else []
        # Always pick unique columns from destination
        dest_column_schema = self.destination_backend.column_schema
        if not unique_columns:
            unique_columns = dest_column_schema.unique_columns
        else:
            unique_columns = [dest_column_schema.column(col) for col in unique_columns]
        self.unique_columns = unique_columns

        self.hash_compare_key = self.destination_backend.column_schema.hash_key.name

    
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
        job_context = batch.context
        strategy_config = job_context.metadata.get("strategy_config")

        # do some basic validation
        prevent_update_unless_changed = strategy_config.prevent_update_unless_changed
        delta_partitions = strategy_config.delta_partitions
        sync_type = self.pipeline_config.sync_type
        if strategy_config.type == SyncStrategy.DELTA and sync_type == "aggregate" and not delta_partitions:
            raise ValueError("Delta partitions are required for aggregate sync type for delta strategy")
        elif strategy_config.type == SyncStrategy.DELTA and sync_type == "row-level" and not strategy_config.delta_partitions and prevent_update_unless_changed and not self.unique_columns:
            raise ValueError("Unique columns are required for row-level sync type for delta strategy with prevent_update_unless_changed")

        try:
            if strategy_config.type == SyncStrategy.DELTA:
                async for batch in self._process_delta_batch(batch, strategy_config):
                    yield batch
            elif strategy_config.type == SyncStrategy.FULL:
                async for batch in self._process_full_batch(batch, strategy_config):
                    yield batch
            elif strategy_config.type == SyncStrategy.HASH:
                async for batch in self._process_hash_batch(batch, strategy_config):
                    yield batch
            # batch_metadata = batch.batch_metadata
            # self.progress_manager.update_progress(rows_detected=batch_metadata.get("rows_detected", 0), rows_fetched=batch_metadata.get("rows_fetched", 0), hash_query_count=batch_metadata.get("hash_query_count", 0))
        except Exception as e:
            self.logger.error(f"Failed to fetch data for partition {partition.partition_id}: {e}")
            batch.batch_metadata["fetch_error"] = str(e)
            raise
    
    async def _process_full_batch(self, batch: DataBatch, strategy_config: StrategyConfig):
        partition = batch.batch_metadata.get("partition")
        use_pagination = strategy_config.use_pagination
        if self.pipeline_config.sync_type == "aggregate":
            use_pagination = False
        async for data, fetch_metadata in self._fetch_partition_data(self.source_backend, partition, strategy_config, use_pagination=use_pagination, with_hash=True):
            yield DataBatch(
                data=data,
                context=batch.context,
                batch_metadata={
                    **batch.batch_metadata,
                    **fetch_metadata
                }
            )
    
    async def _process_hash_batch(self, batch: DataBatch, strategy_config: StrategyConfig):
        
        partition = batch.batch_metadata.get("partition")
        prevent_update_unless_changed = strategy_config.prevent_update_unless_changed
        sync_type = self.pipeline_config.sync_type
        use_pagination = strategy_config.use_pagination
        if sync_type == "aggregate":
            use_pagination = False
        change_type = batch.batch_metadata.get("change_type")
        if change_type == DataStatus.DELETED:
            yield batch
            return
        # if count_diff is less than 0, it means some row has been deleted in the modified batch.
        # However for hot partition and for aggregation it wont work as the count of rows in source
        # is always much greater than the count of rows in destination and for hot partition some new data will
        # always be there to mitigate the deletion.
        # so we need to need yo set prevent_update_unless_changed for these cases if
        # some data deletion is expected. Doesnot make sense to fetch the destination rows just to handle deletion
        if partition.count_diff >= 0 and (not prevent_update_unless_changed or change_type == DataStatus.ADDED):
            async for data, fetch_metadata in self._fetch_partition_data(self.source_backend, partition, strategy_config, use_pagination=use_pagination, with_hash=True):
                yield DataBatch(
                    data=data,
                    context=batch.context,
                    batch_metadata={
                        **batch.batch_metadata,
                        **fetch_metadata
                    }
                )
        else:
            use_pagination = False
            async for data, fetch_metadata in self._fetch_diff_data(partition, strategy_config, use_pagination):
                if fetch_metadata.get("change_type") == DataStatus.DELETED:
                    partition_dimensions = self.get_parition_config_from_unique_columns()
                    for new_partition, data in build_multi_dimension_partitions_for_delta_data(data, partition_dimensions):
                        fetch_metadata["partition"] = new_partition
                        yield DataBatch(
                            data=data,
                            context=batch.context,
                            batch_metadata={
                                **batch.batch_metadata,
                                **fetch_metadata
                            }
                        )
                
                else:
                    yield DataBatch(
                        data=data,
                        context=batch.context,
                        batch_metadata={
                            **batch.batch_metadata,
                            **fetch_metadata
                        }
                    )
    
    async def _process_delta_batch(self, batch: DataBatch, strategy_config: StrategyConfig):
        partition = batch.batch_metadata.get("partition")
        prevent_update_unless_changed = strategy_config.prevent_update_unless_changed
        sync_type = self.pipeline_config.sync_type
        # import pdb; pdb.set_trace()
        async for new_partition, partition_data in self._generate_delta_partitions(partition, strategy_config.delta_partitions, strategy_config):
            if not prevent_update_unless_changed:
                if sync_type == "row-level":
                    # if row level we just need to return already fetched data
                    yield DataBatch(
                        data=partition_data,
                        context=batch.context,
                        batch_metadata={
                            **batch.batch_metadata
                            # **fetch_metadata
                        }
                    )
                else:
                    # if aggregate we need to fetch the aggregate data from the source with no pagination seems to be redundant
                    async for data, fetch_metadata in self._fetch_partition_data(self.source_backend, new_partition, strategy_config, use_pagination=False, with_hash=True):
                        yield DataBatch(
                            data=data,
                            context=batch.context,
                            batch_metadata={
                                **batch.batch_metadata,
                                **fetch_metadata
                            }
                        )
            elif sync_type == "aggregate":
                # if aggregate we need to fetch the aggregate data from the source and compare it with the destination
                async for data, fetch_metadata in self._fetch_and_compare_data(new_partition, strategy_config):
                    yield DataBatch(
                        data=data,
                        context=batch.context,
                        batch_metadata={
                            **batch.batch_metadata,
                            **fetch_metadata
                        }
                    )
            else:
                source_data = partition_data
                async for dest_data, fetch_metadata in self._fetch_partition_data(self.destination_backend, new_partition, strategy_config, use_pagination=False, with_hash=True):
                    # if self.progress_manager:
                    #     self.progress_manager.update_progress(rows_fetched=fetch_metadata.get("rows_fetched", 0), rows_detected=fetch_metadata.get("rows_detected", 0), hash_query_count=fetch_metadata.get("hash_query_count", 0))
                    for data, metadata in self._compare_data_sets(source_data, dest_data):
                        yield DataBatch(
                            data=data,
                            context=batch.context,
                            batch_metadata={
                                **batch.batch_metadata,
                                **metadata
                            }
                        )   
    def get_parition_config_from_unique_columns(self) -> List[DimensionPartitionConfig]:
        return [DimensionPartitionConfig(column=col.name, step=-1, data_type=col.data_type, type="value") for col in self.unique_columns]
    
    async def _generate_delta_partitions(self, partition: MultiDimensionPartition, partition_dimensions: List[DimensionPartitionConfig], strategy_config: StrategyConfig) -> List[Dict[str, Any]]:
        source_backend = self.source_backend
        prevent_update_unless_changed = strategy_config.prevent_update_unless_changed
        sync_type = self.pipeline_config.sync_type
        async for partition_data, fetch_metadata in self._fetch_partition_data(source_backend, partition, strategy_config, use_pagination=strategy_config.use_pagination):
            if not prevent_update_unless_changed and sync_type == "row-level":
                # We dont need to fetch individual data from destination
                yield partition, partition_data
            elif strategy_config.delta_partitions:
                # if delta partitions are present we need to generate new partitions
                for new_partition,data in build_multi_dimension_partitions_for_delta_data(partition_data, partition_dimensions):
                    yield new_partition, data
            else:
                # if no delta partitions are present we need to build it from unique columns
                partition_dimensions = self.get_parition_config_from_unique_columns()
                for new_partition, data in build_multi_dimension_partitions_for_delta_data(partition_data, partition_dimensions):
                    yield new_partition, data
            # if self.progress_manager:
            #     self.progress_manager.update_progress(rows_fetched=fetch_metadata.get("rows_fetched", 0), rows_detected=fetch_metadata.get("rows_detected", 0), hash_query_count=fetch_metadata.get("hash_query_count", 0))

    async def _fetch_diff_data(self, partition: Any, strategy_config: StrategyConfig, use_pagination: bool):
        """Fetch diff data for the batch"""
        # @TODO: Implement pagination for diff data
        # if use_pagination:
        #     # For paginated case, we need to accumulate all data first before comparison
        #     async for data, fetch_metadata in self._fetch_and_compare_paginated_data(partition, strategy_config):
        #         yield data, fetch_metadata
        # else:
            # For non-paginated case, we can compare directly
        async for data, fetch_metadata in self._fetch_and_compare_data(partition, strategy_config):
            yield data, fetch_metadata


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

    async def _fetch_partition_data(self, backend, partition: Any, strategy_config: StrategyConfig, use_pagination: bool, with_hash: bool = True) -> AsyncIterator[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """Fetch partition data with pagination support using generator pattern
        
        Args:
            backend: The backend to fetch data from(source or destination)
            partition: The partition to fetch data for
            strategy_config: Strategy configuration containing pagination and sync settings
            
        Yields:
            Tuple of (data, metadata) where:
            - data: List of rows fetched
            - metadata: Dictionary containing fetch metadata
        """
        page_size = strategy_config.page_size
        
        if use_pagination:
            offset = 0
            
            while True:
                data = await backend.fetch_partition_data(
                    partition,
                    with_hash=with_hash,
                    hash_algo=self.hash_algo,
                    page_size=page_size,
                    offset=offset
                )
                
                if not data:
                    break
                
                metadata = {
                    "paginated": True,
                    "page_size": page_size,
                    "offset": offset,
                    "complete_partition": False
                }

                # In case of hash strategy, rows detected will be in change detection stage
                if strategy_config.type == SyncStrategy.HASH:
                    rows_detected = 0
                else:
                    rows_detected = len(data)
                metadata["rows_detected"] = rows_detected
                metadata["rows_fetched"] = len(data)
                metadata["hash_query_count"] = 1

                # Update progress with fetched data count
                if self.progress_manager:
                    self.progress_manager.update_progress(rows_fetched=len(data), rows_detected=rows_detected, hash_query_count=1)

                # Yield the data and metadata for this page
                yield data, metadata
                
                if len(data) < page_size:
                    break
                
                offset += page_size
        else:
            # For non-paginated case, fetch all data at once
            data = await backend.fetch_partition_data(
                partition,
                with_hash=with_hash,
                hash_algo=self.hash_algo
            )
            
            metadata = {
                "paginated": False,
                "complete_partition": True
            }

            if strategy_config.type == SyncStrategy.HASH:
                rows_detected = 0
            else:
                rows_detected = len(data)
            metadata["rows_detected"] = rows_detected
            metadata["rows_fetched"] = len(data)
            metadata["hash_query_count"] = 1
            # Update progress with fetched data count
            if self.progress_manager:
                self.progress_manager.update_progress(rows_fetched=len(data), rows_detected=rows_detected, hash_query_count=1)
            yield data, metadata

    async def _fetch_and_compare_data(self, partition: Any, strategy_config: StrategyConfig) -> AsyncIterator[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """Fetch and compare data from source and destination for non-paginated case"""
        # Fetch data from both source and destination
        source_data_gen = self._fetch_partition_data(self.source_backend, partition, strategy_config, use_pagination=False, with_hash=True)
        dest_data_gen = self._fetch_partition_data(self.destination_backend, partition, strategy_config, use_pagination=False, with_hash=True)
        
        # Since it's non-paginated, we should get only one result from each
        source_data, source_metadata = await source_data_gen.__anext__()
        dest_data, dest_metadata = await dest_data_gen.__anext__()

        # if self.progress_manager:
        #     self.progress_manager.update_progress(rows_fetched=source_metadata.get("rows_fetched", 0)+dest_metadata.get("rows_fetched", 0), rows_detected=source_metadata.get("rows_detected", 0)+dest_metadata.get("rows_detected", 0), hash_query_count=source_metadata.get("hash_query_count", 0)+dest_metadata.get("hash_query_count", 0))
        
        # Compare the data and find differences
        for data, metadata in self._compare_data_sets(source_data, dest_data):
            yield data, metadata


        

    # async def _fetch_and_compare_paginated_data(self, partition: Any, strategy_config: StrategyConfig):
    #     """Handle paginated data comparison using buffer-based streaming approach with change_type grouping"""
    #     page_size = strategy_config.page_size
    #     unique_columns = self.unique_columns or []
        
    #     # Buffers as per pseudocode - now organized by change_type
    #     unmatched_src_buffer = []
    #     unmatched_destination_buffer = []
        
    #     # Separate yield buffers for each change type
    #     yield_buffers = {
    #         DataStatus.ADDED: [],
    #         DataStatus.MODIFIED: [],
    #         DataStatus.DELETED: []
    #     }
        
    #     # Statistics tracking
    #     total_source_rows = 0
    #     total_dest_rows = 0
    #     rows_added = 0
    #     rows_modified = 0
    #     rows_unchanged = 0
        
    #     # Get both generators
    #     source_generator = self._fetch_partition_data(self.source_backend, partition, strategy_config)
    #     dest_generator = self._fetch_partition_data(self.destination_backend, partition, strategy_config)
        
    #     source_exhausted = False
    #     dest_exhausted = False
        
    #     # def _add_old_prefix_to_row(dest_row: Dict[str, Any]) -> Dict[str, Any]:
    #     #     """Add old__ prefix to destination row keys"""
    #     #     return {f"old__{k}": v for k, v in dest_row.items()}
        
    #     # def _yield_batches_by_change_type():
    #     #     """Yield batches when any change_type buffer reaches page_size"""
    #     #     for change_type, buffer in yield_buffers.items():
    #     #         while len(buffer) >= page_size:
    #     #             page_metadata = {
    #     #                 "paginated": True,
    #     #                 "page_size": page_size,
    #     #                 "offset": 0,
    #     #                 "complete_partition": False,
    #     #                 "page_diff_rows": page_size,
    #     #                 "comparison_done": False,
    #     #                 "comparison_type": "buffer_based_streaming_grouped",
    #     #                 "change_type": change_type,
    #     #                 "rows_added": rows_added,
    #     #                 "rows_modified": rows_modified,
    #     #                 "rows_unchanged": rows_unchanged,
    #     #                 "processed_source_rows": total_source_rows,
    #     #                 "processed_dest_rows": total_dest_rows,
    #     #                 "unmatched_src_buffer_size": len(unmatched_src_buffer),
    #     #                 "unmatched_dest_buffer_size": len(unmatched_destination_buffer)
    #     #             }
                    
    #     #             new_batch = DataBatch(
    #     #                 data=buffer[:page_size],
    #     #                 context=batch.context,
    #     #                 batch_metadata={
    #     #                     **batch.batch_metadata,
    #     #                     **page_metadata
    #     #                 }
    #     #             )
                    
    #     #             # Keep remaining data in buffer
    #     #             yield_buffers[change_type] = buffer[page_size:]
    #     #             yield new_batch
        
    #     while not source_exhausted or not dest_exhausted:
    #         # Fetch source page
    #         src_page = []
    #         if not source_exhausted:
    #             try:
    #                 src_page, source_metadata = await source_generator.__anext__()
    #                 total_source_rows += len(src_page)
    #             except StopAsyncIteration:
    #                 source_exhausted = True
            
    #         # Fetch destination page
    #         dest_page = []
    #         if not dest_exhausted:
    #             try:
    #                 dest_page, dest_metadata = await dest_generator.__anext__()
    #                 total_dest_rows += len(dest_page)
    #             except StopAsyncIteration:
    #                 dest_exhausted = True
            
    #         # If both are exhausted, break
    #         if source_exhausted and dest_exhausted:
    #             break
            
    #         # Process destination page against unmatched source buffer
    #         for dest_row in dest_page:
    #             if not unique_columns:
    #                 # No unique columns - add to unmatched destination buffer
    #                 unmatched_destination_buffer.append(dest_row)
    #                 continue
                
    #             dest_key = tuple(dest_row.get(col) for col in unique_columns)
                
    #             # Check for match in unmatched source buffer
    #             matched_src_idx = None
    #             for i, src_row in enumerate(unmatched_src_buffer):
    #                 src_key = tuple(src_row.get(col) for col in unique_columns)
    #                 if src_key == dest_key:
    #                     matched_src_idx = i
    #                     break
                
    #             if matched_src_idx is not None:
    #                 # Match found in unmatched source buffer
    #                 matched_src_row = unmatched_src_buffer.pop(matched_src_idx)
                    
    #                 # Check if hash unmatched
    #                 if strategy_config.type == SyncStrategy.HASH:
    #                     src_hash = matched_src_row.get('_row_hash')
    #                     dest_hash = dest_row.get('_row_hash')
    #                     if src_hash != dest_hash:
    #                         # Add to MODIFIED yield buffer with old__ prefixed dest data
    #                         modified_row = {
    #                             **matched_src_row,
    #                             **_add_old_prefix_to_row(dest_row)
    #                         }
    #                         yield_buffers[DataStatus.MODIFIED].append(modified_row)
    #                         rows_modified += 1
    #                     else:
    #                         # Ignore - unchanged
    #                         rows_unchanged += 1
    #                 else:
    #                     # Full row comparison
    #                     if matched_src_row != dest_row:
    #                         # Add to MODIFIED yield buffer with old__ prefixed dest data
    #                         modified_row = {
    #                             **matched_src_row,
    #                             **_add_old_prefix_to_row(dest_row)
    #                         }
    #                         yield_buffers[DataStatus.MODIFIED].append(modified_row)
    #                         rows_modified += 1
    #                     else:
    #                         # Ignore - unchanged
    #                         rows_unchanged += 1
    #             else:
    #                 # No match found - add to unmatched destination buffer
    #                 unmatched_destination_buffer.append(dest_row)
            
    #         # Process source page against destination page
    #         for src_row in src_page:
    #             if not unique_columns:
    #                 # No unique columns - treat all source rows as ADDED
    #                 yield_buffers[DataStatus.ADDED].append(src_row)
    #                 rows_added += 1
    #                 continue
                
    #             src_key = tuple(src_row.get(col) for col in unique_columns)
                
    #             # Check for row in destination page
    #             matched_dest_row = None
    #             for dest_row in dest_page:
    #                 dest_key = tuple(dest_row.get(col) for col in unique_columns)
    #                 if src_key == dest_key:
    #                     matched_dest_row = dest_row
    #                     break
                
    #             if matched_dest_row is not None:
    #                 # Match found in destination page
    #                 if strategy_config.type == SyncStrategy.HASH:
    #                     src_hash = src_row.get('_row_hash')
    #                     dest_hash = matched_dest_row.get('_row_hash')
    #                     if src_hash != dest_hash:
    #                         # Add to MODIFIED yield buffer with old__ prefixed dest data
    #                         modified_row = {
    #                             **src_row,
    #                             **_add_old_prefix_to_row(matched_dest_row)
    #                         }
    #                         yield_buffers[DataStatus.MODIFIED].append(modified_row)
    #                         rows_modified += 1
    #                     else:
    #                         # Ignore - unchanged
    #                         rows_unchanged += 1
    #                 else:
    #                     # Full row comparison
    #                     if src_row != matched_dest_row:
    #                         # Add to MODIFIED yield buffer with old__ prefixed dest data
    #                         modified_row = {
    #                             **src_row,
    #                             **_add_old_prefix_to_row(matched_dest_row)
    #                         }
    #                         yield_buffers[DataStatus.MODIFIED].append(modified_row)
    #                         rows_modified += 1
    #                     else:
    #                         # Ignore - unchanged
    #                         rows_unchanged += 1
    #             else:
    #                 # No match found - add to unmatched source buffer
    #                 unmatched_src_buffer.append(src_row)
            
    #         # Yield batches by change_type when page_size reached
    #         for yielded_batch in _yield_batches_by_change_type():
    #             yield yielded_batch
        
    #     # Process any remaining unmatched source rows (these are ADDED rows)
    #     for src_row in unmatched_src_buffer:
    #         yield_buffers[DataStatus.ADDED].append(src_row)
    #         rows_added += 1
        
    #     # Process any remaining unmatched destination rows (these are DELETED rows)
    #     for dest_row in unmatched_destination_buffer:
    #         deleted_row = _add_old_prefix_to_row(dest_row)
    #         yield_buffers[DataStatus.DELETED].append(deleted_row)
        
    #     # Yield any remaining data in all yield buffers
    #     for change_type, buffer in yield_buffers.items():
    #         while len(buffer) > 0:
    #             batch_size = min(len(buffer), page_size)
    #             is_final = len(buffer) <= page_size
                
    #             final_metadata = {
    #                 "paginated": True,
    #                 "page_size": batch_size,
    #                 "offset": 0,
    #                 "complete_partition": is_final,
    #                 "page_diff_rows": batch_size,
    #                 "comparison_done": is_final,
    #                 "comparison_type": "buffer_based_streaming_grouped",
    #                 "change_type": change_type,
    #                 "rows_added": rows_added,
    #                 "rows_modified": rows_modified,
    #                 "rows_unchanged": rows_unchanged,
    #                 "total_source_rows": total_source_rows,
    #                 "total_dest_rows": total_dest_rows,
    #                 "unique_columns": unique_columns
    #             }
                
    #             final_batch = DataBatch(
    #                 data=buffer[:batch_size],
    #                 context=batch.context,
    #                 batch_metadata={
    #                     **batch.batch_metadata,
    #                     **final_metadata
    #                 }
    #             )
                
    #             yield_buffers[change_type] = buffer[batch_size:]
    #             yield final_batch
        
    #     # If no differences were found, yield an empty completion batch
    #     if rows_added == 0 and rows_modified == 0:
    #         empty_metadata = {
    #             "paginated": True,
    #             "page_size": 0,
    #             "offset": 0,
    #             "complete_partition": True,
    #             "page_diff_rows": 0,
    #             "comparison_done": True,
    #             "comparison_type": "buffer_based_streaming_grouped",
    #             "change_type": None,
    #             "rows_added": rows_added,
    #             "rows_modified": rows_modified,
    #             "rows_unchanged": rows_unchanged,
    #             "total_source_rows": total_source_rows,
    #             "total_dest_rows": total_dest_rows,
    #             "unique_columns": unique_columns
    #         }
            
    #         empty_batch = DataBatch(
    #             data=[],
    #             context=batch.context,
    #             batch_metadata={
    #                 **batch.batch_metadata,
    #                 **empty_metadata
    #             }
    #         )
    #         yield empty_batch

    def _compare_data_sets(self, source_data: List[Dict[str, Any]], dest_data: List[Dict[str, Any]]) -> Generator[Tuple[List[Dict[str, Any]], Dict[str, Any]], None, None]:
        """Compare source and destination data sets and return differences"""
        # Get unique columns for comparison
        # import pdb; pdb.set_trace()
        unique_columns = [col.name for col in self.unique_columns]


        if not unique_columns:
            raise ValueError("Unique columns are required for comparison in data fetch stage")
        
        # if not unique_columns:
        #     # If no unique columns specified, return all source data as diff
        #     return source_data, {
        #         "comparison_type": "full_replace",
        #         "rows_added": len(source_data),
        #         "rows_modified": 0,
        #         "rows_unchanged": 0
        #     }
        
        # Create lookup dictionaries for efficient comparison
        dest_lookup = {}
        for row in dest_data:
            # Create key from unique columns
            key = tuple(row.get(col) for col in unique_columns)
            dest_lookup[key] = row
        
        added, modified, deleted = [], [], []
        # diff_data = []
        # rows_added = 0
        # rows_modified = 0
        # rows_unchanged = 0
        
        for source_row in source_data:
            # Create key from unique columns
            key = tuple(source_row.get(col) for col in unique_columns)
            
            if key not in dest_lookup:
                # Row doesn't exist in destination - it's added
                added.append(source_row)
                # rows_added += 1
            else:
                dest_row = dest_lookup[key]
                # Compare by hash if available
                source_hash = source_row.get(self.hash_compare_key)
                dest_hash = dest_row.get(self.hash_compare_key)
                if source_hash != dest_hash:
                    source_row["old__"] = dest_row
                    modified.append(source_row)
                dest_lookup.pop(key)
        for key, dest_row in dest_lookup.items():
            deleted.append(dest_row)
        
        if added:
            metadata = {}
            metadata["change_type"] = DataStatus.ADDED
            metadata["complete_partition"] = False
            yield added, metadata
        if modified:
            metadata = {}
            metadata["change_type"] = DataStatus.MODIFIED
            metadata["complete_partition"] = False
            yield modified, metadata
        if deleted:
            metadata = {}
            metadata["change_type"] = DataStatus.DELETED
            metadata["complete_partition"] = False
            yield deleted, metadata
        
