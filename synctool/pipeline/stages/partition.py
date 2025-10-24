from typing import AsyncIterator, Dict, Any, List, Tuple, Optional, TYPE_CHECKING, Union, cast, Generator
from dataclasses import dataclass, field

from ...core.column_mapper import ColumnSchema
from ..base import PipelineStage, DataBatch, StageConfig
from ...core.models import GlobalStageConfig, StrategyConfig, DataStorage, BackendConfig, DimensionPartitionConfig, PartitionBound, Column, PipelineJobConfig
from ...core.enums import SyncStrategy, HashAlgo
from ...core.schema_models import UniversalDataType
from ...utils.partition_generator import (
    add_exclusive_range
)
from ...utils.multi_dimensional_partition_generator import (
    generate_multi_dimension_partitions_from_partition_bounds,
)
from ...utils.safe_eval import safe_eval
from ...utils.schema_utils import cast_value


if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine
    from ...utils.progress_manager import ProgressManager

@dataclass
class PartitionStageConfig(StageConfig):
    source: BackendConfig = None
    destination: BackendConfig = None
    columns: List[Column] = field(default_factory=list)
    # hash_algo: HashAlgo = field(default=HashAlgo.HASH_MD5_HASH)
    # columns: List[Dict[str, Any]] = field(default_factory=list)
    # strategies: List[Dict[str, Any]] = field(default_factory=list)
    enabled: bool = True




class PartitionStage(PipelineStage):
    """Stage that handles strategy detection, sync bounds calculation, partition generation and change detection"""
    
    def __init__(self, sync_engine: 'SyncEngine', config: GlobalStageConfig, pipeline_config: PipelineJobConfig, logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        # stage_config = ChangeDetectionConfig(**config)
        config = PartitionStageConfig.from_global_stage_config(config)
        super().__init__(config.name, config, pipeline_config, logger)
        self.sync_engine = sync_engine
        self.strategies = pipeline_config.strategies
        self.hash_algo = pipeline_config.hash_algo
        self.column_schema = ColumnSchema(config.columns)
        # self.source_column_schema = sync_engine.column_mapper.build_schema(self.config.source.get('columns', [])) if sync_engine.column_mapper else None
        # self.destination_column_schema = sync_engine.column_mapper.build_schema(self.config.destination.get('columns', [])) if sync_engine.column_mapper else None
        # self.data_storage = data_storage
        self.source_backend = sync_engine.create_backend(self.config.source)
        self.destination_backend = sync_engine.create_backend(self.config.destination)
        self.progress_manager = progress_manager
        self.detected_strategy = None
        self.detected_strategy_config = None
        # self.source_backend = sync_engine.get_backend_class(self.config.source.get('type', 'postgres'))(self.config.source, column_schema=self.source_column_schema, logger=self.logger) # type: ignore
        # self.destination_backend = sync_engine.get_backend_class(self.config.destination.get('type', 'postgres'))(self.config.destination, column_schema=self.destination_column_schema, logger=self.logger) # type: ignore
    
    async def setup(self, context: Any):
        await self.source_backend.connect()
        await self.destination_backend.connect()
    
    async def teardown(self, context: Any):
        await self.source_backend.disconnect()
        await self.destination_backend.disconnect()
    
    async def determine_strategy_for_context(self, context: Any) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
        """Determine which sync strategy to use"""
        self.detected_strategy, self.detected_strategy_config = await self._determine_strategy(
            context.user_strategy_name, 
            context.user_bounds
        )
        return self.detected_strategy, self.detected_strategy_config
    
    async def process(self, input_stream: AsyncIterator) -> AsyncIterator[DataBatch]:
        """Main processing method - handles strategy detection, bounds calculation, and partition processing"""
        async for job_context in input_stream:
            # import pdb; pdb.set_trace()
            self.logger.info(f"Starting partition stage for job: {job_context.job_name}")
            sync_type = self.pipeline_config.sync_type
            user_bounds = job_context.user_bounds
            
            # Step 1: Determine sync strategy
            sync_strategy, strategy_config = self.detected_strategy, self.detected_strategy_config
            self.logger.info(f"Using sync strategy: {sync_strategy.value}")
            
            # Store strategy info in job context for other stages
            job_context.metadata["used_strategy"] = sync_strategy
            job_context.metadata["strategy_config"] = strategy_config

            partition_bounds = await self._get_partition_bounds(sync_strategy, strategy_config.primary_partitions, sync_type, user_bounds)
            
            # Step 2: Get sync bounds
            # partition_bounds = await self._get_partition_bounds(
            #     sync_strategy, 
            #     strategy_config, 
            #     job_context.user_start, 
            #     job_context.user_end
            # )
            for partition_bound in partition_bounds:
                if partition_bound.type == "range" and partition_bound.start == partition_bound.end:
                    self.logger.info(f"Sync bounds are the same, no partitions to process")
                    job_context.metadata["change_detection_stats"] = {
                        "total_partitions_processed": 0
                    }
                    return
            for partition_bound in partition_bounds:
                # if data_type is date datetime or timestamp or integer then add exclusive range
                if partition_bound.type == "range" and partition_bound.data_type in (UniversalDataType.DATE, UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP, UniversalDataType.INTEGER):
                    if not partition_bound.exclusive:
                        partition_bound.end = add_exclusive_range(partition_bound.end)
                        partition_bound.exclusive = True
            for partition_bound in partition_bounds:
                if partition_bound.type == "range":
                    self.logger.info(f"Partition bounds: {partition_bound.column}: {partition_bound.type} - {partition_bound.start} to {partition_bound.end}")
                elif partition_bound.type == "value":
                    self.logger.info(f"Partition bounds: {partition_bound.column}: {partition_bound.type} - {partition_bound.value}")
                # elif partition_bound.type == "upper_bound":
                #     self.logger.info(f"Sync bounds: {partition_bound.column}: {partition_bound.type} - {partition_bound.start} to {partition_bound.end} with lower limit {partition_bound.upper_bound}")
                # elif partition_bound.type == "lower_bound":
                #     self.logger.info(f"Sync bounds: {partition_bound.column}: {partition_bound.type} - {partition_bound.start} to {partition_bound.end} with upper limit {partition_bound.lower_bound}")
            # import pdb; pdb.set_trace()
            # Step 3: Generate all partitions for the job
            if strategy_config is None:
                raise ValueError("Strategy config is required for partition generation")
            # Generate partitions using generator
            
            partitions_generator = self._generate_job_partitions(strategy_config.primary_partitions, partition_bounds)
            partition_count = 0

            for partition in partitions_generator:
                self.progress_manager.update_progress(total_primary_partitions=1)
                partition_count += 1
                batch = DataBatch(
                    data=[],  # Will be populated by Other stages
                    context=job_context,
                    batch_metadata={
                        "partition": partition,
                        "strategy_config": strategy_config,
                    }
                )
                yield batch


            # self.logger.info(f"Generated {len(partitions)} partitions for processing")
            
            # # Store partition count in context for reporting
            # job_context.metadata["change_detection_stats"] = {
            #     "total_partitions_processed": len(partitions)
            # }
            
            # # Step 4: Process partitions concurrently based on strategy
            # if sync_strategy == SyncStrategy.FULL:
            #     async for batch in self._process_full_partitions_concurrent(job_context, partitions, strategy_config):
            #         yield batch
            # elif sync_strategy == SyncStrategy.DELTA:
            #     async for batch in self._process_delta_partitions_concurrent(job_context, partitions, strategy_config):
            #         yield batch
            # elif sync_strategy == SyncStrategy.HASH:
            #     async for batch in self._process_hash_partitions_concurrent(job_context, partitions, strategy_config):
            #         yield batch
    

    async def _get_partition_bounds(self, sync_strategy: SyncStrategy, dimension_configs: List[DimensionPartitionConfig], sync_type: Optional[str]='row-level', user_bounds: Optional[Dict[str, Any]]=None):
        partition_bounds = []

        # import pdb; pdb.set_trace()
        for dimension_config in dimension_configs:
            bounded = False
            exclusive = True
            column = dimension_config.column
            data_type = dimension_config.data_type or self.source_backend.db_column_schema.column(column).data_type
            dimension_config.data_type = data_type
            if dimension_config.type == "range":
                if data_type not in (UniversalDataType.DATE, UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP, UniversalDataType.INTEGER):
                    continue
                
                # decide if the bounds are bounded or not
                if sync_type == "row-level":
                    bounded = True
                elif sync_type == "aggregate" and sync_strategy == SyncStrategy.DELTA:
                    # in case of aggregate using delta strategy, it is safe to bound the values
                    bounded = True
                else:
                    bounded = False
                user_bound = user_bounds.get(column)
                start = None
                end = None
                if user_bound:
                    start = user_bound.get("start")
                    end = user_bound.get("end")
                if not start:
                    start = dimension_config.start
                if not end:
                    end = dimension_config.end
                if start:
                    start = safe_eval(start)
                    start = start() if callable(start) else start
                    start = cast_value(start, data_type) if start is not None else None
                if end:
                    end = safe_eval(end)
                    end = end() if callable(end) else end
                    end = cast_value(end, data_type) if end is not None else None

                if sync_strategy == SyncStrategy.DELTA:
                    if start is None:
                        start = await self.destination_backend.get_last_sync_point(column)
                    if start is None:
                        start, end = await self.source_backend.get_partition_bounds(column)
                        exclusive = False
                    elif end is None:
                        end = await self.source_backend.get_max_sync_point(column)
                        exclusive = False
                    start = cast_value(start, data_type) if start is not None else None
                    end = cast_value(end, data_type) if end is not None else None
                elif sync_strategy == SyncStrategy.FULL:
                    if start is None and end is None:
                        start, end = await self.source_backend.get_partition_bounds(column)
                        start = cast_value(start, data_type) if start is not None else None
                        end = cast_value(end, data_type) if end is not None else None
                        exclusive = False
                    elif end is None:
                        end = await self.source_backend.get_max_sync_point(column)
                        exclusive = False
                    elif start is None:
                        start,_ = await self.source_backend.get_partition_bounds(column)
                        start = cast_value(start, data_type) if start is not None else None
                else:
                    if start is None and end is None:
                        source_start, source_end = await self.source_backend.get_partition_bounds(column)
                        dest_start, dest_end = await self.destination_backend.get_partition_bounds(column)
                        if source_start is not None:
                            start = min(cast_value(source_start, data_type), cast_value(dest_start, data_type)) if dest_start is not None else cast_value(source_start, data_type)
                        if source_end is not None:
                            end = max(cast_value(source_end, data_type), cast_value(dest_end, data_type)) if dest_end is not None else cast_value(source_end, data_type)
                        exclusive = False
                    elif end is None:
                        end = await self.source_backend.get_max_sync_point(column)
                        end = cast_value(end, data_type) if end is not None else None
                        exclusive = False
                    elif start is None:
                        source_start, source_end = await self.source_backend.get_partition_bounds(column)
                        dest_start, dest_end = await self.destination_backend.get_partition_bounds(column)
                        start = min(cast_value(source_start, data_type), cast_value(dest_start, data_type))
                        end = max(cast_value(source_end, data_type), cast_value(dest_end, data_type))
                        exclusive = False
                partition_bounds.append(PartitionBound(column=column, start=start, end=end, data_type=data_type, type=dimension_config.type, bounded=bounded, exclusive=exclusive))
            elif dimension_config.type == "value":
                value = dimension_config.value
                partition_bounds.append(PartitionBound(column=column, value=value, data_type=data_type, type=dimension_config.type))
            # elif dimension_config.type == "upper_bound":
            #     partition_bounds.append(PartitionBound(column=column, start=dimension_config.start, end=dimension_config.end, data_type=data_type, type=dimension_config.type, lower_bound=dimension_config.lower_bound))    
            # elif dimension_config.type == "lower_bound":
            #     start = dimension_config.start
            #     partition_bounds.append(PartitionBound(column=column, start=start, end=dimension_config.end, data_type=data_type, type=dimension_config.type, upper_bound=dimension_config.upper_bound))
        
        return partition_bounds
    
    async def _determine_strategy(self, strategy_name: Optional[str], bounds: Optional[List[Dict[str, Any]]]) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
        """Determine which sync strategy to use"""
        # # Check if destination has data by looking at the populate stage configuration
        # populate_stage = self.sync_engine.stage_configs.get('populate')
        # if not populate_stage:
        #     raise ValueError("populate stage is required")
        
        # Create a temporary backend to check if destination has data
        destination_backend = self.destination_backend
        strategy_config = next((s for s in self.strategies if s.name == strategy_name), None)
      
        if not await destination_backend.has_data():
            # Find full strategy
            if strategy_config.type == SyncStrategy.FULL:
                return SyncStrategy.FULL, strategy_config
            else:
                full_strategy = next((s for s in self.strategies if s.type == SyncStrategy.FULL and s.default), None)
                return SyncStrategy.FULL, full_strategy
        elif strategy_config.type == SyncStrategy.FULL:
            hash_strategy = next((s for s in self.strategies if s.type == SyncStrategy.HASH and s.default), None)
            return SyncStrategy.HASH, hash_strategy
    
        
        if strategy_name:
            if strategy_config:
                if strategy_config.type == "delta":
                    return SyncStrategy.DELTA, strategy_config
                elif strategy_config.type == "hash":
                    return SyncStrategy.HASH, strategy_config
                else:
                    return SyncStrategy.FULL, strategy_config
        
        # Default strategy selection logic
        if bounds:
            hash_strategy = next((s for s in self.strategies if s.name == "hash" and s.default), None)
            return SyncStrategy.HASH, hash_strategy
        else:
            delta_strategy = next((s for s in self.strategies if s.name == "delta" and s.default), None)
            return SyncStrategy.DELTA, delta_strategy
    
    # async def _get_partition_bounds(self, strategy: SyncStrategy, strategy_config: StrategyConfig, 
    #                           start: Any, end: Any) -> List[Dict[str, Any]]:
    #     """Get start and end bounds for sync"""
    #     partition_bounds = []
    #     if start and end:
    #         # if strategy is delta, we need to return the bounds for the primary partition
    #         if strategy == SyncStrategy.DELTA and strategy_config:
    #             primary_partition = strategy_config.primary_partitions[0]
    #             partition_column = primary_partition.column
    #             return [{"column":partition_column, "start":start, "end":end, "data_type":self.column_schema.column(partition_column).data_type}]
    #         else:
    #             # if start and end is string convert to list then assert length are equal and assign to primary partitions
    #             # in same sequence. Also need to handle none values
    #             if isinstance(start, list) and isinstance(end, list):
    #                 assert len(start) == len(end)
    #             else:
    #                 start, end = [start], [end]
    #             for i, partition_config in enumerate(strategy_config.primary_partitions):
    #                 partition_column = self.column_schema.column(partition_config.column)
    #                 partition_column_type = partition_column.data_type
    #                 if partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
    #                     start, end = self.get_synn_bounds_for_uuid(partition_column_type)
    #                     partition_bounds.append({"column":partition_config.column, "start":start, "end":end, "data_type":partition_column_type})
    #                 elif start[i] is not None and end[i] is not None:
    #                     partition_bounds.append({"column":partition_config.column, "start":start[i], "end":end[i]})
    #                 else:
    #                     # get bounds from source and destination depending on strategy
    #                     if strategy == SyncStrategy.FULL:
    #                         start, end = await self.source_backend.get_partition_bounds(partition_config.column)
    #                         partition_bounds.append({"column":partition_config.column, "start":start, "end":end, "data_type":partition_column_type})
    #                     else:
    #                         dest_start, dest_end = await self.destination_backend.get_partition_bounds(partition_config.column)
    #                         start = min(start[i], dest_start)
    #                         end = max(end[i], dest_end)
    #                         partition_bounds.append({"column":partition_config.column, "start":start, "end":end, "data_type":partition_column_type})
    #             return partition_bounds
 
    #     if strategy == SyncStrategy.DELTA and strategy_config:
    #         primary_partition = strategy_config.primary_partitions[0]
    #         partition_column = primary_partition.column
    #         dest_start = await self.destination_backend.get_last_sync_point(partition_column)
    #         source_end = await self.source_backend.get_max_sync_point(partition_column)
    #         return [{"column":partition_column, "start":dest_start, "end":source_end, "data_type":self.column_schema.column(partition_column).data_type}]
        
    #     primary_partitions = strategy_config.primary_partitions
    #     for i, partition_config in enumerate(primary_partitions):
    #         partition_column = partition_config.column

    #         partition_column_type = self.column_schema.column(partition_column).data_type
    #         if partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
    #             start, end = self.get_partition_bounds_for_uuid(partition_column_type)
    #             partition_bounds.append({"column":partition_column, "start":start, "end":end, "data_type":partition_column_type})
    #         else:
    #             start, end = await self.source_backend.get_partition_bounds(partition_column)
    #             if strategy == SyncStrategy.FULL:
    #                 partition_bounds.append({"column":partition_column, "start":start, "end":end, "data_type":partition_column_type})
    #             else:
    #                 # if strategy is hash, we need to get the bounds from the destination
    #                 dest_start, dest_end = await self.destination_backend.get_partition_bounds(partition_column)
    #                 start = min(start, dest_start)
    #                 end = max(end, dest_end)
    #                 partition_bounds.append({"column":partition_column, "start":start, "end":end, "data_type":partition_column_type})
        
    #     return partition_bounds
    
    # def get_partition_bounds_for_uuid(self, partition_column_type: UniversalDataType) -> Tuple[Any, Any]:
    #     start = "00000000000000000000000000000000"
    #     end = "ffffffffffffffffffffffffffffffff"
    #     if partition_column_type == UniversalDataType.UUID_TEXT_DASH:
    #         return str(uuid.UUID(start)), str(uuid.UUID(end))
    #     elif partition_column_type == UniversalDataType.UUID_TEXT:
    #         return start, end
    #     else:
    #         return uuid.UUID(start), uuid.UUID(end)
    
    def _generate_job_partitions(self, partition_dimensions: List[DimensionPartitionConfig], partition_bounds: List[PartitionBound]) -> Generator[Any, None, None]:
        """Generate all partitions for the job - supports both legacy and multi-dimensional partitioning"""
        # generator = MultiDimensionalPartitionGenerator(partition_dimensions)
        # import pdb; pdb.set_trace()
        # Generate multi-dimensional partitions
        yield from generate_multi_dimension_partitions_from_partition_bounds(partition_bounds, partition_dimensions)
        # return await self._generate_multi_dimensional_partitions(strategy_config, partition_bounds)
    
    # async def _generate_multi_dimensional_partitions(self, strategy_config: StrategyConfig, 
    #                                                partition_bounds: List[Dict[str, Any]]) -> List[Partition]:
    #     """Generate multi-dimensional partitions"""
    #     # import pdb; pdb.set_trace()
        
    #     # Build sync bounds for all dimensions
    #     # partition_bounds_map = {bound["column"]: (bound["start"], bound["end"]) for bound in partition_bounds}


        
    #     # Collect all unique columns from primary and secondary partitions
    #     # all_columns = set()
    #     # for dim in strategy_config.primary_partitions:
    #     #     all_columns.add(dim.column)
        
    #     # if strategy_config.secondary_partitions:
    #     #     for dim in strategy_config.secondary_partitions:
    #     #         all_columns.add(dim.column)
        
    #     # # Get bounds for each column
    #     # for column in all_columns:
    #     #     column_info = self.column_schema.column(column)
    #     #     data_type = column_info.data_type if column_info else None
            
    #     #     # For now, use the same sync bounds for all columns
    #     #     # In the future, you might want to get column-specific bounds
    #     #     if data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
    #     #         # Use UUID bounds
    #     #         start = "00000000000000000000000000000000"
    #     #         end = "ffffffffffffffffffffffffffffffff"
    #     #         if data_type == UniversalDataType.UUID_TEXT_DASH:
    #     #             partition_bounds[column] = (str(uuid.UUID(start)), str(uuid.UUID(end)))
    #     #         elif data_type == UniversalDataType.UUID_TEXT:
    #     #             partition_bounds[column] = (start, end)
    #     #         else:
    #     #             partition_bounds[column] = (uuid.UUID(start), uuid.UUID(end))
    #     #     else:
    #     #         # Use provided bounds or get from backend
    #     #         if column == strategy_config.partition_column:
    #     #             partition_bounds[column] = (sync_start, sync_end)
    #     #         else:
    #     #             # Get bounds from backend for this column
    #     #             bounds = await self.source_backend.get_partition_bounds(column)
    #     #             partition_bounds[column] = bounds
        
    #     # Create multi-dimensional partition generator
    #     md_config = MultiDimensionalPartitionConfig(
    #         dimensions=strategy_config.primary_partitions,
    #         column_schema=self.column_schema
    #     )
        
    #     generator = MultiDimensionalPartitionGenerator(md_config)
        
    #     # Generate multi-dimensional partitions
    #     md_partitions = await generator.generate_all_partitions(partition_bounds)
        
    #     # # Convert to legacy partitions for backward compatibility
    #     # primary_column = strategy_config.primary_partitions[0].column if strategy_config.primary_partitions else strategy_config.partition_column
    #     # legacy_partitions = convert_to_legacy_partitions(md_partitions, primary_column)
        
    #     # # Set column types
    #     # for partition in legacy_partitions:
    #     #     column_info = self.column_schema.column(partition.column)
    #     #     partition.data_type = column_info.data_type if column_info else "string"
        
    #     return md_partitions
    
    # async def _generate_legacy_partitions(self, strategy_config: StrategyConfig, sync_start: Any, sync_end: Any) -> List[Partition]:
    #     """Generate legacy single-dimension partitions (existing logic)"""
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
    
    # async def _process_full_partitions_concurrent(self, job_context, partitions: List[Partition], 
    #                                             strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
    #     """Process full sync partitions concurrently"""
    #     max_concurrent = self.config.max_concurrent_partitions
    #     semaphore = asyncio.Semaphore(max_concurrent)
        
    #     async def process_partition(partition: Partition) -> AsyncIterator[DataBatch]:
    #         # import pdb; pdb.set_trace()
    #         async with semaphore:
    #             # Generate sub-partitions if configured
    #             sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
                
    #             for sub_partition in sub_partitions:
    #                 # for full sync, we need to fetch all hashed rows in the partition
    #                 async for batch in self._fetch_partition_row_hashes(self.source_backend, sub_partition, strategy_config, job_context):
    #                     yield batch
        
    #     # Process partitions concurrently
    #     if max_concurrent == 1:
    #         # Sequential processing
    #         for partition in partitions:
    #             async for batch in process_partition(partition):
    #                 yield batch
    #     else:
    #         # Concurrent processing using asyncio.as_completed for streaming
    #         tasks = [process_partition(partition) for partition in partitions]
            
    #         # Use asyncio.as_completed to yield batches as they become available
    #         for coro in asyncio.as_completed(tasks):
    #             async for batch in await coro:
    #                 yield batch
    
    # async def _process_delta_partitions_concurrent(self, job_context, partitions: List[Partition], 
    #                                              strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
    #     """Process delta sync partitions concurrently"""
    #     max_concurrent = self.config.max_concurrent_partitions
    #     semaphore = asyncio.Semaphore(max_concurrent)
        
    #     async def process_partition(partition: Partition) -> List[DataBatch]:
    #         async with semaphore:
    #             batches = []
    #             # Generate sub-partitions if configured
    #             sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
                
    #             for sub_partition in sub_partitions:
    #                 # for full sync, we need to fetch all hashed rows in the partition
    #                 async for batch in self._fetch_partition_row_hashes(self.source_backend, sub_partition, strategy_config, job_context):
    #                     yield batch
                 
        
    #     # Process partitions concurrently
    #     if max_concurrent == 1:
    #         # Sequential processing
    #         for partition in partitions:
    #             async for batch in process_partition(partition):
    #                 yield batch
    #     else:
    #         # Concurrent processing
    #         tasks = [process_partition(partition) for partition in partitions]
            
    #         for coro in asyncio.as_completed(tasks):
    #             async for batch in await coro:
    #                 yield batch
    
    # async def _process_hash_partitions_concurrent(self, job_context, partitions: List[Partition], 
    #                                             strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
    #     """Process hash sync partitions concurrently"""
    #     max_concurrent = self.sync_engine.config.max_concurrent_partitions
    #     semaphore = asyncio.Semaphore(max_concurrent)
        
    #     async def process_partition(partition: Partition) -> List[DataBatch]:
    #         async with semaphore:
    #             # Build intervals for hash sync
    #             if strategy_config.intervals:
    #                 partition.intervals = strategy_config.intervals
    #             else:
    #                 partition.intervals = build_intervals(
    #                     strategy_config.partition_step,
    #                     strategy_config.min_sub_partition_step,
    #                     strategy_config.interval_reduction_factor
    #                 )
                
    #             # Calculate sub-partitions and their statuses
    #             partitions_with_status, statuses = await self._calculate_sub_partitions(
    #                 partition,
    #                 max_level=len(partition.intervals)-1,
    #                 page_size=strategy_config.page_size
    #             )
                
    #             partitions_with_status, statuses = merge_adjacent(
    #                 partitions_with_status, statuses, strategy_config.page_size
    #             )
                
    #             batches = []
    #             for part, status in zip(partitions_with_status, statuses):
    #                 change_type = None
    #                 needs_fetch = False
    #                 needs_row_comparison = False
                    
    #                 if status == DataStatus.ADDED:
    #                     change_type = "added"
    #                     needs_fetch = True
    #                 elif status == DataStatus.DELETED:
    #                     change_type = "deleted"
    #                     needs_fetch = False
    #                 elif status == DataStatus.MODIFIED:
    #                     change_type = "modified"
    #                     needs_fetch = True
    #                     needs_row_comparison = strategy_config.prevent_update_unless_changed
                    
    #                 if change_type:
    #                     batch = DataBatch(
    #                         data=[],  # Will be populated by DataFetch stage if needed
    #                         context=job_context,
    #                         batch_metadata={
    #                             "change_type": change_type,
    #                             "partition": part,
    #                             "needs_fetch": needs_fetch,
    #                             "needs_row_comparison": needs_row_comparison,
    #                             "rows_affected": part.num_rows,
    #                             "partition_id": part.partition_id,
    #                             "strategy_config": strategy_config
    #                         }
    #                     )
    #                     batches.append(batch)
    #             return batches
        
    #     # Process partitions concurrently
    #     if max_concurrent == 1:
    #         # Sequential processing
    #         for partition in partitions:
    #             batches = await process_partition(partition)
    #             for batch in batches:
    #                 yield batch
    #     else:
    #         # Concurrent processing
    #         tasks = [process_partition(partition) for partition in partitions]
    #         batch_lists = await asyncio.gather(*tasks)
            
    #         # Yield all batches
    #         for batch_list in batch_lists:
    #             for batch in batch_list:
    #                 yield batch
    
    # async def _generate_sub_partitions(self, partition: Partition, strategy_config: StrategyConfig) -> List[Partition]:
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
    
    # async def _calculate_sub_partitions(self, partition: Partition, max_level: int = 100, 
    #                                   page_size: int = 1000) -> Tuple[List[Partition], List[str]]:
    #     """Calculate sub-partitions for hash sync"""
    #     # Create backends from stage configurations
    #     change_detection_stage = self.sync_engine.stage_configs.get('change_detection')
    #     if not change_detection_stage:
    #         raise ValueError("change_detection stage configuration not found")
        
    #     source_backend = await self.sync_engine.create_backend_from_stage(change_detection_stage, role="source")
    #     destination_backend = await self.sync_engine.create_backend_from_stage(change_detection_stage, role="destination")
        
    #     final_partitions, statuses = [], []
    #     try:
    #         src_rows = await source_backend.fetch_child_partition_hashes(
    #             partition, hash_algo=self.config.hash_algo
    #         )
    #         destination_rows = await destination_backend.fetch_child_partition_hashes(
    #             partition, hash_algo=self.config.hash_algo
    #         )
            
    #         s_partitions = to_partitions(src_rows, partition)
    #         d_partitions = to_partitions(destination_rows, partition)
    #         partitions, status_map = calculate_partition_status(s_partitions, d_partitions)
            
    #         for p in partitions:
    #             key = (p.start, p.end, p.level)
    #             st = status_map[key]
                
    #             if st in ('M', 'A') and (p.num_rows > page_size and p.level < max_level):
    #                 deeper_partitions, deeper_statuses = await self._calculate_sub_partitions(
    #                     p, max_level=max_level, page_size=page_size
    #                 )
    #                 final_partitions.extend(deeper_partitions)
    #                 statuses.extend(deeper_statuses)
    #             else:
    #                 final_partitions.append(p)
    #                 statuses.append(st)
            
    #     finally:
    #         await source_backend.disconnect()
    #         await destination_backend.disconnect()
        
    #     return final_partitions, statuses

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