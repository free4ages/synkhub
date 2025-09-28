import asyncio
import uuid
from typing import AsyncIterator, Dict, Any, List, Tuple, Optional, TYPE_CHECKING, Union, cast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from synctool.core.column_mapper import ColumnSchema
from synctool.core.models import Column

from ..base import PipelineStage, DataBatch, StageConfig
from ...core.models import GlobalStageConfig, MultiDimensionPartition, StrategyConfig, DataStorage, BackendConfig, DimensionPartitionConfig, MultiDimensionPartition, PartitionBound, PipelineJobConfig
from ...core.enums import DataStatus, SyncStrategy, HashAlgo
from ...core.schema_models import UniversalDataType
from ...utils.partition_generator import (
    calculate_partition_status
)
from ...utils.multi_dimensional_partition_generator import (
    build_multi_dimension_child_partitions,
    generate_multi_dimension_partitions_from_partition_bounds,
    build_multi_dimension_partitions_for_delta_data
)


if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager

@dataclass
class ChangeDetectionConfig(StageConfig):
    source: BackendConfig = None
    destination: BackendConfig = None
    columns: List[Column] = field(default_factory=list)
    hash_algo: HashAlgo = field(default=HashAlgo.HASH_MD5_HASH)
    # columns: List[Dict[str, Any]] = field(default_factory=list)
    strategies: List[Dict[str, Any]] = field(default_factory=list)
    max_concurrent_partitions: int = 1
    enabled: bool = True


class ChangeDetectionStage(PipelineStage):
    """Stage that handles strategy detection, sync bounds calculation, partition generation and change detection"""
    
    def __init__(self, sync_engine: 'SyncEngine', config: GlobalStageConfig, pipeline_config: PipelineJobConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        # stage_config = ChangeDetectionConfig(**config)
        config = ChangeDetectionConfig.from_global_stage_config(config)
        super().__init__(config.name, config, pipeline_config, logger)
        self.sync_engine = sync_engine
        self.strategies = self.config.strategies
        self.column_schema = ColumnSchema(config.columns)
        # self.source_column_schema = sync_engine.column_mapper.build_schema(self.config.source.get('columns', [])) if sync_engine.column_mapper else None
        # self.destination_column_schema = sync_engine.column_mapper.build_schema(self.config.destination.get('columns', [])) if sync_engine.column_mapper else None
        # self.data_storage = data_storage
        self.source_backend = sync_engine.create_backend(self.config.source)
        self.destination_backend = sync_engine.create_backend(self.config.destination)
        self.progress_manager = progress_manager
        # self.source_backend = sync_engine.get_backend_class(self.config.source.get('type', 'postgres'))(self.config.source, column_schema=self.source_column_schema, logger=self.logger) # type: ignore
        # self.destination_backend = sync_engine.get_backend_class(self.config.destination.get('type', 'postgres'))(self.config.destination, column_schema=self.destination_column_schema, logger=self.logger) # type: ignore
    
    async def setup(self, context: Any):
        await self.source_backend.connect()
        await self.destination_backend.connect()
    
    async def teardown(self, context: Any):
        await self.source_backend.disconnect()
        await self.destination_backend.disconnect()
    
    async def process(self, input_stream: AsyncIterator) -> AsyncIterator[DataBatch]:
        """Main processing method - handles strategy detection, bounds calculation, and partition processing"""
        async for data_batch in input_stream:
            job_context = data_batch.context
            self.logger.info(f"Starting change detection for job: {job_context.job_name}")
            strategy_config = job_context.metadata.get("strategy_config")
            sync_strategy = job_context.metadata.get("used_strategy")
            
            # # Step 1: Determine sync strategy
            # sync_strategy, strategy_config = await self._determine_strategy(
            #     job_context.user_strategy_name, 
            #     job_context.user_start, 
            #     job_context.user_end
            # )
            # self.logger.info(f"Using sync strategy: {sync_strategy.value}")
            
            # # Store strategy info in job context for other stages
            # job_context.metadata["used_strategy"] = sync_strategy
            # job_context.metadata["strategy_config"] = strategy_config
            
            # # Step 2: Get sync bounds
            # sync_start, sync_end = await self._get_partition_bounds(
            #     sync_strategy, 
            #     strategy_config, 
            #     job_context.user_start, 
            #     job_context.user_end
            # )
            
            # if sync_start == sync_end:
            #     self.logger.info(f"Sync bounds are the same, no partitions to process")
            #     job_context.metadata["change_detection_stats"] = {
            #         "total_partitions_processed": 0
            #     }
            #     return
            
            # sync_end = add_exclusive_range(sync_end)
            # self.logger.info(f"Sync bounds: {sync_start} to {sync_end}")
            # # import pdb; pdb.set_trace()
            # # Step 3: Generate all partitions for the job
            # if strategy_config is None:
            #     raise ValueError("Strategy config is required for partition generation")
            # partitions = await self._generate_job_partitions(strategy_config, sync_start, sync_end)
            # self.logger.info(f"Generated {len(partitions)} partitions for processing")
            
            # # Store partition count in context for reporting
            # job_context.metadata["change_detection_stats"] = {
            #     "total_partitions_processed": len(partitions)
            # }
            
            # Step 4: Process partitions concurrently based on strategy
            if sync_strategy == SyncStrategy.FULL:
                async for batch in self._process_full_partitions(data_batch, strategy_config, job_context):
                    yield batch
            elif sync_strategy == SyncStrategy.DELTA:
                async for batch in self._process_delta_partitions(data_batch, strategy_config, job_context):
                    yield batch
            elif sync_strategy == SyncStrategy.HASH:
                match_row_count = strategy_config.match_row_count
                async for batch in self._process_hash_partitions(data_batch, strategy_config, job_context, match_row_count):
                    yield batch
    
    # async def _determine_strategy(self, strategy_name: Optional[str], start: Any, end: Any) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
    #     """Determine which sync strategy to use"""
    #     # # Check if destination has data by looking at the populate stage configuration
    #     # populate_stage = self.sync_engine.stage_configs.get('populate')
    #     # if not populate_stage:
    #     #     raise ValueError("populate stage is required")
        
    #     # Create a temporary backend to check if destination has data
    #     destination_backend = self.destination_backend
        
      
    #     if not await destination_backend.has_data():
    #         # Find full strategy
    #         full_strategy = next((s for s in self.strategies if s.type == SyncStrategy.FULL), None)
    #         return SyncStrategy.FULL, full_strategy
    #     elif strategy_name == "full":
    #         hash_strategy = next((s for s in self.strategies if s.type == SyncStrategy.HASH), None)
    #         return SyncStrategy.HASH, hash_strategy
    
        
    #     if strategy_name:
    #         strategy_config = next((s for s in self.strategies if s.name == strategy_name), None)
    #         if strategy_config:
    #             if strategy_config.type == "delta":
    #                 return SyncStrategy.DELTA, strategy_config
    #             elif strategy_config.type == "hash":
    #                 return SyncStrategy.HASH, strategy_config
    #             else:
    #                 return SyncStrategy.FULL, strategy_config
        
    #     # Default strategy selection logic
    #     if start and end:
    #         hash_strategy = next((s for s in self.strategies if s.name == "hash"), None)
    #         return SyncStrategy.HASH, hash_strategy
    #     else:
    #         delta_strategy = next((s for s in self.strategies if s.name == "delta"), None)
    #         return SyncStrategy.DELTA, delta_strategy
    
    # async def _get_partition_bounds(self, strategy: SyncStrategy, strategy_config: Optional[StrategyConfig], 
    #                           start: Any, end: Any) -> Tuple[Any, Any]:
    #     """Get start and end bounds for sync"""
    #     if start and end:
    #         return start, end
    #     partition_column_type = strategy_config.partition_column_type if strategy_config else self.column_schema.column(strategy_config.partition_column).data_type
        
    #     if strategy == SyncStrategy.DELTA and strategy_config:
    #         dest_start = await self.destination_backend.get_last_sync_point()
    #         source_end = await self.source_backend.get_max_sync_point()
    #         return dest_start, source_end
        
    #     if strategy_config and partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
    #         start = "00000000000000000000000000000000"
    #         end = "ffffffffffffffffffffffffffffffff"
    #         if strategy_config.data_type == UniversalDataType.UUID_TEXT_DASH:
    #             return str(uuid.UUID(start)), str(uuid.UUID(end))
    #         elif strategy_config.data_type= UniversalDataType.UUID_TEXT:
    #             return start, end
    #         return uuid.UUID(start), uuid.UUID(end)


    #     return await self.source_backend.get_partition_bounds()
    
    # async def _generate_job_partitions(self, strategy_config: StrategyConfig, sync_start: Any, sync_end: Any) -> List[MultiDimensionPartition]:
    #     """Generate all partitions for the job"""
        
    #     partition_column_info = self.column_schema.column(strategy_config.partition_column)
    #     partition_column_type = partition_column_info.data_type if partition_column_info.data_type else strategy_config.partition_column_type
        
    #     partition_step = getattr(strategy_config, 'partition_step', 1000)
    #     partition_generator = PartitionGenerator(PartitionConfig(
    #         name="main_partition_{pid}",
    #         column=partition_column_info.name,
    #         data_type=partition_column_type,
    #         partition_step=partition_step
    #     ))
        
    #     return await partition_generator.generate_partitions(sync_start, sync_end)
    
    async def _process_full_partitions(self, data_batch: DataBatch, strategy_config: StrategyConfig, job_context: Any) -> AsyncIterator[DataBatch]:
        """Process full sync partitions concurrently"""
        partition = data_batch.batch_metadata.get("partition")
        if strategy_config.secondary_partitions:
            partition_bounds = await self._get_partition_bounds(strategy_config.type, strategy_config.secondary_partitions, partition)
            # generator = MultiDimensionalPartitionGenerator(strategy_config.secondary_partitions)
            sub_partitions = generate_multi_dimension_partitions_from_partition_bounds(partition_bounds, strategy_config.secondary_partitions, parent_partition=partition)
            # sub_partitions = await self._generate_secondary_partitions(partition, strategy_config)
        
            for sub_partition in sub_partitions:
                # import pdb; pdb.set_trace()
                self.progress_manager.update_progress(total_partitions=1)
                batch = DataBatch(
                    data=[],
                    context=job_context,
                    batch_metadata={
                        "partition": sub_partition,
                        "change_type": DataStatus.ADDED,
                        "complete_partition": True,
                    }
                )
                # import pdb; pdb.set_trace()
                yield batch
        else:
            self.progress_manager.update_progress(total_partitions=1)
            yield partition
    
    
    async def _process_delta_partitions(self, data_batch: DataBatch, strategy_config: StrategyConfig, job_context: Any) -> AsyncIterator[DataBatch]:
        """Process delta sync partitions concurrently"""
        partition = data_batch.batch_metadata.get("partition")
        async for batch in self._process_full_partitions(data_batch, strategy_config, job_context):
            if not strategy_config.delta_partitions:
                self.progress_manager.update_progress(total_partitions=1)
                yield batch
            else:
                async for sub_partition in self._generate_delta_partitions(partition, strategy_config.delta_partitions, strategy_config):
                    self.progress_manager.update_progress(total_partitions=1)
                    batch = DataBatch(
                        data=[],
                        context=job_context,
                        batch_metadata={
                            "partition": sub_partition,
                            "change_type": DataStatus.MODIFIED,
                            "complete_partition": False,
                        }
                    )
                    yield batch

                
        # sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
        # self.progress_manager.update_progress(total_partitions=len(sub_partitions))
        # for sub_partition in sub_partitions:
        #     batch = DataBatch(
        #         data=[],
        #         context=job_context,
        #         batch_metadata={
        #             "partition": sub_partition,
        #             "change_type": DataStatus.MODIFIED,
        #             "complete_partition": False,
        #         }
        #     )
        #     self.progress_manager.update_progress(total_partitions=1)
        #     yield batch
    
    async def _process_hash_partitions(self, data_batch: DataBatch, strategy_config: StrategyConfig, job_context: Any, match_row_count: bool = True) -> AsyncIterator[DataBatch]:
        """Process hash sync partitions concurrently"""
        partition = data_batch.batch_metadata.get("partition")
        # partition.intervals = [strategy_config.partition_step, strategy_config.sub_partition_step]
        # Calculate sub-partitions and their statuses
        async for part, status in self._calculate_sub_partitions(
            partition,
            max_level=1,
            page_size=strategy_config.page_size,
            match_row_count=match_row_count,
            partition_dimensions=strategy_config.secondary_partitions
        ):         
            if status in (DataStatus.ADDED, DataStatus.MODIFIED, DataStatus.DELETED):
                batch = DataBatch(
                    data=[],
                    context=job_context,
                    batch_metadata={
                        "partition": part,
                        "change_type": status,
                        "complete_partition": True,
                    }
                )
                self.progress_manager.update_progress(rows_detected=part.num_rows, total_partitions=1)
                yield batch
                
            else:
                self.progress_manager.update_progress(rows_detected=part.num_rows, total_partitions=1, skipped_partitions=1)


        # sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
        # for sub_partition in sub_partitions:
        #     batch = DataBatch(
        #         data=[],
        #         context=job_context,
        #         batch_metadata={
        #             "partition": sub_partition,
        #             "change_type": DataStatus.ADDED,
        #             "complete_partition": True,
        #         }
        #     )
        #     yield batch
        
        # async def process_partition(partition: MultiDimensionPartition) -> List[DataBatch]:
        #     async with semaphore:
        #         # Build intervals for hash sync
        #         if strategy_config.intervals:
        #             partition.intervals = strategy_config.intervals
        #         else:
        #             partition.intervals = build_intervals(
        #                 strategy_config.partition_step,
        #                 strategy_config.min_sub_partition_step,
        #                 strategy_config.interval_reduction_factor
        #             )
                
        #         # Calculate sub-partitions and their statuses
        #         partitions_with_status, statuses = await self._calculate_sub_partitions(
        #             partition,
        #             max_level=len(partition.intervals)-1,
        #             page_size=strategy_config.page_size
        #         )
                
        #         partitions_with_status, statuses = merge_adjacent(
        #             partitions_with_status, statuses, strategy_config.page_size
        #         )
                
        #         batches = []
        #         for part, status in zip(partitions_with_status, statuses):
        #             change_type = None
        #             needs_fetch = False
        #             needs_row_comparison = False
                    
        #             if status == DataStatus.ADDED:
        #                 change_type = "added"
        #                 needs_fetch = True
        #             elif status == DataStatus.DELETED:
        #                 change_type = "deleted"
        #                 needs_fetch = False
        #             elif status == DataStatus.MODIFIED:
        #                 change_type = "modified"
        #                 needs_fetch = True
        #                 needs_row_comparison = strategy_config.prevent_update_unless_changed
                    
        #             if change_type:
        #                 batch = DataBatch(
        #                     data=[],  # Will be populated by DataFetch stage if needed
        #                     context=job_context,
        #                     batch_metadata={
        #                         "change_type": change_type,
        #                         "partition": part,
        #                         "needs_fetch": needs_fetch,
        #                         "needs_row_comparison": needs_row_comparison,
        #                         "rows_affected": part.num_rows,
        #                         "partition_id": part.partition_id,
        #                         "strategy_config": strategy_config
        #                     }
        #                 )
        #                 batches.append(batch)
        #         return batches
        
        # # Process partitions concurrently
        # if max_concurrent == 1:
        #     # Sequential processing
        #     for partition in partitions:
        #         batches = await process_partition(partition)
        #         for batch in batches:
        #             yield batch
        # else:
        #     # Concurrent processing
        #     tasks = [process_partition(partition) for partition in partitions]
        #     batch_lists = await asyncio.gather(*tasks)
            
        #     # Yield all batches
        #     for batch_list in batch_lists:
        #         for batch in batch_list:
        #             yield batch
    
    async def _get_partition_bounds(self, sync_strategy: SyncStrategy, dimension_configs: List[DimensionPartitionConfig],parent_partition: MultiDimensionPartition):
        partition_bounds = []
        for dimension_config in dimension_configs:
            column = dimension_config.column
            bounded = False
            if parent_partition.has_column(column):
                dim_bound = parent_partition.get_bound_for_column(column)
                dim_bound.bounded = True
                partition_bounds.append(dim_bound)
            else:
                start, end = dimension_config.start, dimension_config.end
                data_type = dimension_config.data_type or self.column_schema.column(column).data_type
                dimension_config.data_type = data_type
                if data_type not in (UniversalDataType.DATE, UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP, UniversalDataType.INTEGER):
                    continue
                # perhaps should raise error instead of warning
                # raise warning if code reaches here to move this column of secondary partition to primary partition
                self.logger.warning(f"Please move this column of secondary partition to primary partition: Its expensive to calculate bounds for this column {column}")
                if sync_strategy == SyncStrategy.DELTA:
                    #not sure what to do will implement later
                    pass
                elif sync_strategy == SyncStrategy.FULL:
                    if not (start and end):
                        start, end = await self.source_backend.get_partition_bounds(column)
                    elif not dimension_config.end:
                        end = await self.source_backend.get_max_sync_point(column)
                    elif not start:
                        start,_ = await self.source_backend.get_partition_bounds(column)
                else:
                    if not (start and end):
                        source_start, source_end = await self.source_backend.get_partition_bounds(column)
                        dest_start, dest_end = await self.destination_backend.get_partition_bounds(column)
                        start = min(source_start, dest_start)
                        end = max(source_end, dest_end)
                    elif not end:
                        end = await self.source_backend.get_max_sync_point(column)
                    elif not start:
                        source_start, source_end = await self.source_backend.get_partition_bounds(column)
                        dest_start, dest_end = await self.destination_backend.get_partition_bounds(column)
                        start = min(source_start, dest_start)
                partition_bounds.append(PartitionBound(column=column, data_type=data_type, start=start, end=end, bounded=bounded))
        return partition_bounds

    # async def _generate_secondary_partitions(self, partition: MultiDimensionPartition, strategy_config: StrategyConfig) -> List[MultiDimensionPartition]:
    #     if strategy_config.secondary_partitions:
    #         secondary_partition_config = PartitionConfig(
    #             name="secondary_partition_{pid}",
    #             column=partition.column,
    #             data_type=partition.data_type,
    #             partition_step=strategy_config.secondary_partition_step
    #         )
    # async def _generate_sub_partitions(self, partition: MultiDimensionPartition, strategy_config: StrategyConfig) -> List[MultiDimensionPartition]:
    #     """Generate sub-partitions if configured"""
    #     if strategy_config.use_sub_partition:
    #         sub_partition_config = PartitionConfig(
    #             name="sub_partition_{pid}",
    #             column=partition.column,
    #             data_type=partition.data_type,
    #             partition_step=strategy_config.sub_partition_step
    #         )
    #         sub_partition_generator = PartitionGenerator(sub_partition_config)
    #         return await sub_partition_generator.generate_partitions(
    #             partition.start, partition.end, partition
    #         )
    #     else:
    #         return [partition]
    
    async def _generate_delta_partitions(self, partition: MultiDimensionPartition, partition_dimensions: List[DimensionPartitionConfig], strategy_config: StrategyConfig) -> List[Dict[str, Any]]:
        source_backend = self.source_backend
        if not partition_dimensions:
            yield partition
        data = await source_backend.fetch_partition_data(partition, with_hash=False)
        for partition in build_multi_dimension_partitions_for_delta_data(data, partition_dimensions):
            yield partition



    
    async def _calculate_sub_partitions(self, partition: MultiDimensionPartition, max_level: int = 100, 
                                      page_size: int = 1000, match_row_count: bool = True, partition_dimensions: List[DimensionPartitionConfig] = None) -> AsyncIterator[Tuple[MultiDimensionPartition, str]]:
        """Calculate sub-partitions for hash sync - now supports multi-dimensional partitions"""
        source_backend = self.source_backend
        destination_backend = self.destination_backend
        # import pdb; pdb.set_trace()
        src_rows = await source_backend.fetch_child_partition_hashes(
            partition, hash_algo=self.config.hash_algo, partition_dimensions=partition_dimensions
        )
        destination_rows = await destination_backend.fetch_child_partition_hashes(
            partition, hash_algo=self.config.hash_algo, partition_dimensions=partition_dimensions
        )
        self.progress_manager.update_progress(detection_query_count=1)
        
        # s_partitions = to_partitions(src_rows, partition)
        # d_partitions = to_partitions(destination_rows, partition)
        partitions, status_map = calculate_partition_status(src_rows, destination_rows, skip_row_count=not match_row_count)

        for p in build_multi_dimension_child_partitions(partitions, partition_dimensions, partition):
            yield p, status_map[p.partition_id]
        # for p in partitions:
        #     partition_id = p["partition_id"]
        #     st = status_map[partition_id]
        #     # will implement recursion later
        #     if st in ('M', 'A'):
        #         build
            
            # if st in ('M', 'A') and (p.num_rows > page_size and p.level < max_level):
            #     # For multi-dimensional partitions, we might want to recursively subdivide
            #     # This is a simplified approach - you could enhance this further
            #     async for deeper_partition, deeper_status in self._calculate_single_dimension_sub_partitions(
            #         p, max_level, page_size, match_row_count
            #     ):
            #         yield deeper_partition, deeper_status
            # else:
            #     yield p, st

        
        # # Check if this is a multi-dimensional partition
        # if hasattr(partition, '_multi_dimensional_partition'):
        #     async for part, status in self._calculate_multi_dimensional_sub_partitions(
        #         partition, max_level, page_size, match_row_count
        #     ):
        #         yield part, status
        # else:
        #     # Use existing single-dimension logic
        #     async for part, status in self._calculate_single_dimension_sub_partitions(
        #         partition, max_level, page_size, match_row_count
        #     ):
        #         yield part, status
    
    # async def _calculate_multi_dimensional_sub_partitions(self, partition: MultiDimensionPartition, max_level: int, 
    #                                                     page_size: int, match_row_count: bool) -> AsyncIterator[Tuple[MultiDimensionPartition, str]]:
    #     """Calculate sub-partitions for multi-dimensional partitions"""
    #     md_partition = partition._multi_dimensional_partition
        
    #     # Build WHERE clause for multi-dimensional partition
    #     where_conditions = []
    #     for column, (start, end) in md_partition.dimensions.items():
    #         where_conditions.append(f"{column} >= %s AND {column} < %s")
        
    #     where_clause = " AND ".join(where_conditions)
    #     where_params = []
    #     for column, (start, end) in md_partition.dimensions.items():
    #         where_params.extend([start, end])
        
    #     # Fetch hashes using the multi-dimensional WHERE clause
    #     src_rows = await self.source_backend.fetch_child_partition_hashes_with_where(
    #         where_clause, where_params, hash_algo=self.config.hash_algo
    #     )
    #     destination_rows = await self.destination_backend.fetch_child_partition_hashes_with_where(
    #         where_clause, where_params, hash_algo=self.config.hash_algo
    #     )
        
    #     self.progress_manager.update_progress(detection_query_count=1)
        
    #     # Convert to single-dimension partitions for compatibility with existing logic
    #     # This is a simplification - you might want to enhance this for true multi-dimensional comparison
    #     s_partitions = to_partitions(src_rows, partition)
    #     d_partitions = to_partitions(destination_rows, partition)
    #     partitions, status_map = calculate_partition_status(s_partitions, d_partitions, skip_row_count=not match_row_count)
        
    #     for p in partitions:
    #         key = (p.start, p.end, p.level)
    #         st = status_map[key]
            
    #         if st in ('M', 'A') and (p.num_rows > page_size and p.level < max_level):
    #             # For multi-dimensional partitions, we might want to recursively subdivide
    #             # This is a simplified approach - you could enhance this further
    #             async for deeper_partition, deeper_status in self._calculate_single_dimension_sub_partitions(
    #                 p, max_level, page_size, match_row_count
    #             ):
    #                 yield deeper_partition, deeper_status
    #         else:
    #             yield p, st
    
    # async def _calculate_single_dimension_sub_partitions(self, partition: MultiDimensionPartition, max_level: int, 
    #                                                    page_size: int, match_row_count: bool) -> AsyncIterator[Tuple[MultiDimensionPartition, str]]:
    #     """Calculate sub-partitions for single-dimension partitions (existing logic)"""
    #     source_backend = self.source_backend
    #     destination_backend = self.destination_backend
        
    #     src_rows = await source_backend.fetch_child_partition_hashes(
    #         partition, hash_algo=self.config.hash_algo, 
    #     )
    #     destination_rows = await destination_backend.fetch_child_partition_hashes(
    #         partition, hash_algo=self.config.hash_algo
    #     )
    #     self.progress_manager.update_progress(detection_query_count=1)
        
    #     s_partitions = to_partitions(src_rows, partition)
    #     d_partitions = to_partitions(destination_rows, partition)
    #     partitions, status_map = calculate_partition_status(s_partitions, d_partitions, skip_row_count=not match_row_count)
        
    #     for p in partitions:
    #         key = (p.start, p.end, p.level)
    #         st = status_map[key]
            
    #         if st in ('M', 'A') and (p.num_rows > page_size and p.level < max_level):
    #             async for deeper_partition, deeper_status in self._calculate_single_dimension_sub_partitions(
    #                 p, max_level, page_size, match_row_count
    #             ):
    #                 yield deeper_partition, deeper_status
    #         else:
    #             yield p, st

    # async def _fetch_partition_row_hashes(self, backend, partition, strategy_config: StrategyConfig, job_context: Any):
    #     """Fetch partition data with pagination support using generator pattern"""
    #     use_pagination = strategy_config.use_pagination
    #     page_size = strategy_config.page_size
    #     if use_pagination:
    #         offset = 0
            
    #         while True:
    #             data = await backend.fetch_partition_row_hashes(
    #                 partition,
    #                 hash_algo=self.config.hash_algo,
    #                 page_size=page_size,
    #                 offset=offset
    #             )
                
    #             if not data:
    #                 break
                
    #             batch = DataBatch(
    #                 data=data,  # Will be populated by DataFetch stage if needed
    #                 context=job_context,
    #                 batch_metadata={
    #                     "partition": partition,
    #                     "strategy_config": strategy_config,
    #                     "strategy_type": strategy_config.type,
    #                     "paginated": True,
    #                     "page_size": page_size,
    #                     "offset": offset,
    #                     "complete_partition": False,
    #                     "change_type": DataStatus.ADDED
    #                 }
    #             )
    #             # Yield the batch for this page
    #             yield batch
                
    #             if len(data) < page_size:
    #                 break
                
    #             offset += page_size
    #     else:
    #         # For non-paginated case, still yield each row
    #         data = await backend.fetch_partition_row_hashes(
    #             partition, hash_algo=self.config.hash_algo
    #         )
    #         batch = DataBatch(
    #             data=data,
    #             context=job_context,
    #             batch_metadata={
    #                 "partition": partition,
    #                 "strategy_config": strategy_config,
    #                 "strategy_type": strategy_config.type,
    #                 "paginated": False,
    #                 "complete_partition": True,
    #                 "change_type": DataStatus.ADDED
    #             }
    #         )
    #         yield batch