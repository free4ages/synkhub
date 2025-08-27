import asyncio
import uuid
from typing import AsyncIterator, Dict, Any, List, Tuple, Optional, TYPE_CHECKING, Union, cast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from ..base import PipelineStage, DataBatch, StageConfig
from ...core.models import Partition, StrategyConfig, DataStorage
from ...core.enums import DataStatus, SyncStrategy
from ...core.schema_models import UniversalDataType
from ...utils.partition_generator import (
    PartitionConfig, PartitionGenerator, build_intervals, 
    merge_adjacent, calculate_partition_status, to_partitions, add_exclusive_range
)


if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine

@dataclass
class ChangeDetectionConfig(StageConfig):
    enabled: bool = True
    source: Dict[str, Any] = field(default_factory=dict)
    destination: Dict[str, Any] = field(default_factory=dict)
    # columns: List[Dict[str, Any]] = field(default_factory=list)
    strategies: List[Dict[str, Any]] = field(default_factory=list)


class ChangeDetectionStage(PipelineStage):
    """Stage that handles strategy detection, sync bounds calculation, partition generation and change detection"""
    _config_class = ChangeDetectionConfig
    
    def __init__(self, sync_engine: 'SyncEngine', config: Dict[str, Any], logger=None, data_storage: Optional[DataStorage] = None):
        # stage_config = ChangeDetectionConfig(**config)
        super().__init__("change_detection", config, logger)
        self.config = cast(ChangeDetectionConfig, self.config)
        self.sync_engine = sync_engine
        self.strategies = [StrategyConfig(**s) for s in self.config.strategies]
        self.source_column_schema = sync_engine.column_mapper.build_schema(self.config.source.get('columns', [])) if sync_engine.column_mapper else None
        self.destination_column_schema = sync_engine.column_mapper.build_schema(self.config.destination.get('columns', [])) if sync_engine.column_mapper else None
        self.data_storage = data_storage
        self.source_backend = sync_engine.create_backend(self.config.source, column_schema=self.source_column_schema)
        self.destination_backend = sync_engine.create_backend(self.config.destination, column_schema=self.destination_column_schema)
        # self.source_backend = sync_engine.get_backend_class(self.config.source.get('type', 'postgres'))(self.config.source, column_schema=self.source_column_schema, logger=self.logger) # type: ignore
        # self.destination_backend = sync_engine.get_backend_class(self.config.destination.get('type', 'postgres'))(self.config.destination, column_schema=self.destination_column_schema, logger=self.logger) # type: ignore
    
    async def process(self, input_stream: AsyncIterator) -> AsyncIterator[DataBatch]:
        """Main processing method - handles strategy detection, bounds calculation, and partition processing"""
        async for job_context in input_stream:
            self.logger.info(f"Starting change detection for job: {job_context.job_name}")
            
            # Step 1: Determine sync strategy
            sync_strategy, strategy_config = await self._determine_strategy(
                job_context.user_strategy_name, 
                job_context.user_start, 
                job_context.user_end
            )
            self.logger.info(f"Using sync strategy: {sync_strategy.value}")
            
            # Store strategy info in job context for other stages
            job_context.metadata["used_strategy"] = sync_strategy
            job_context.metadata["strategy_config"] = strategy_config
            
            # Step 2: Get sync bounds
            sync_start, sync_end = await self._get_sync_bounds(
                sync_strategy, 
                strategy_config, 
                job_context.user_start, 
                job_context.user_end
            )
            
            if sync_start == sync_end:
                self.logger.info(f"Sync bounds are the same, no partitions to process")
                job_context.metadata["change_detection_stats"] = {
                    "total_partitions_processed": 0
                }
                return
            
            sync_end = add_exclusive_range(sync_end)
            self.logger.info(f"Sync bounds: {sync_start} to {sync_end}")
            
            # Step 3: Generate all partitions for the job
            if strategy_config is None:
                raise ValueError("Strategy config is required for partition generation")
            partitions = await self._generate_job_partitions(strategy_config, sync_start, sync_end)
            self.logger.info(f"Generated {len(partitions)} partitions for processing")
            
            # Store partition count in context for reporting
            job_context.metadata["change_detection_stats"] = {
                "total_partitions_processed": len(partitions)
            }
            
            # Step 4: Process partitions concurrently based on strategy
            if sync_strategy == SyncStrategy.FULL:
                async for batch in self._process_full_partitions_concurrent(job_context, partitions, strategy_config):
                    yield batch
            elif sync_strategy == SyncStrategy.DELTA:
                async for batch in self._process_delta_partitions_concurrent(job_context, partitions, strategy_config):
                    yield batch
            elif sync_strategy == SyncStrategy.HASH:
                async for batch in self._process_hash_partitions_concurrent(job_context, partitions, strategy_config):
                    yield batch
    
    async def _determine_strategy(self, strategy_name: Optional[str], start: Any, end: Any) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
        """Determine which sync strategy to use"""
        # # Check if destination has data by looking at the populate stage configuration
        # populate_stage = self.sync_engine.stage_configs.get('populate')
        # if not populate_stage:
        #     raise ValueError("populate stage is required")
        
        # Create a temporary backend to check if destination has data
        destination_backend = await self.sync_engine.create_backend_from_stage(populate_stage, role="destination")
        
        try:
            if not await destination_backend.has_data():
                # Find full strategy
                full_strategy = next((s for s in self.sync_engine.strategies.values() if s.type == SyncStrategy.FULL), None)
                return SyncStrategy.FULL, full_strategy
            elif strategy_name == "full":
                hash_strategy = next((s for s in self.sync_engine.strategies.values() if s.type == SyncStrategy.HASH), None)
                return SyncStrategy.HASH, hash_strategy
        finally:
            await destination_backend.disconnect()
        
        if strategy_name:
            if strategy_name in self.sync_engine.strategies:
                strategy_config = self.sync_engine.strategies[strategy_name]
                if strategy_config.type == "delta":
                    return SyncStrategy.DELTA, strategy_config
                elif strategy_config.type == "hash":
                    return SyncStrategy.HASH, strategy_config
                else:
                    return SyncStrategy.FULL, strategy_config
        
        # Default strategy selection logic
        if start and end:
            hash_strategy = next((s for s in self.sync_engine.strategies.values() if s.name == "hash"), None)
            return SyncStrategy.HASH, hash_strategy
        else:
            delta_strategy = next((s for s in self.sync_engine.strategies.values() if s.name == "delta"), None)
            return SyncStrategy.DELTA, delta_strategy
    
    async def _get_sync_bounds(self, strategy: SyncStrategy, strategy_config: Optional[StrategyConfig], 
                              start: Any, end: Any) -> Tuple[Any, Any]:
        """Get start and end bounds for sync"""
        if start and end:
            return start, end
        
        if strategy == SyncStrategy.DELTA and strategy_config:
            # Create backends from stage configurations
            source_stage = self.sync_engine.stage_configs.get('source_data_fetch') or self.sync_engine.stage_configs.get('change_detection')
            destination_stage = self.sync_engine.stage_configs.get('destination_data_fetch') or self.sync_engine.stage_configs.get('populate')
            
            if not source_stage or not destination_stage:
                raise ValueError("Source and destination stages required for delta sync")
            
            source_backend = await self.sync_engine.create_backend_from_stage(source_stage, role="source")
            destination_backend = await self.sync_engine.create_backend_from_stage(destination_stage, role="destination")
            
            try:
                dest_start = await destination_backend.get_last_sync_point()
                source_end = await source_backend.get_max_sync_point()
                return dest_start, source_end
            finally:
                await source_backend.disconnect()
                await destination_backend.disconnect()
        
        if strategy_config and strategy_config.column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            start = "00000000000000000000000000000000"
            end = "ffffffffffffffffffffffffffffffff"
            if strategy_config.column_type == UniversalDataType.UUID_TEXT_DASH:
                return str(uuid.UUID(start)), str(uuid.UUID(end))
            elif strategy_config.column_type == UniversalDataType.UUID_TEXT:
                return start, end
            return uuid.UUID(start), uuid.UUID(end)

        # Get bounds from source stage
        source_stage = self.sync_engine.stage_configs.get('source_data_fetch') or self.sync_engine.stage_configs.get('change_detection')
        if source_stage:
            source_backend = await self.sync_engine.create_backend_from_stage(source_stage, role="source")
            try:
                return await source_backend.get_partition_bounds()
            finally:
                await source_backend.disconnect()
        
        raise ValueError("Could not determine sync bounds")
    
    async def _generate_job_partitions(self, strategy_config: StrategyConfig, sync_start: Any, sync_end: Any) -> List[Partition]:
        """Generate all partitions for the job"""
        if not self.sync_engine.column_mapper:
            raise ValueError("Column mapper not initialized")
        
        column_info = self.sync_engine.column_mapper.column(strategy_config.column)
        column_type_str = column_info.dtype if column_info.dtype else "string"
        
        partition_step = getattr(strategy_config, 'partition_step', 1000)
        partition_generator = PartitionGenerator(PartitionConfig(
            name="main_partition_{pid}",
            column=strategy_config.column,
            column_type=column_type_str,
            partition_step=partition_step
        ))
        
        return await partition_generator.generate_partitions(sync_start, sync_end)
    
    async def _process_full_partitions_concurrent(self, job_context, partitions: List[Partition], 
                                                strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
        """Process full sync partitions concurrently"""
        max_concurrent = self.sync_engine.config.max_concurrent_partitions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_partition(partition: Partition) -> List[DataBatch]:
            async with semaphore:
                batches = []
                # Generate sub-partitions if configured
                sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
                
                for sub_partition in sub_partitions:
                    # for full sync, we need to fetch all hashed rows in the partition

                    # For full sync, all data in partition is considered "changed"
                    batch = DataBatch(
                        data=[],  # Will be populated by DataFetch stage
                        context=job_context,
                        batch_metadata={
                            "change_type": "full_sync",
                            "partition": sub_partition,
                            "needs_fetch": True,
                            "partition_id": sub_partition.partition_id,
                            "strategy_config": strategy_config
                        }
                    )
                    batches.append(batch)
                return batches
        
        # Process partitions concurrently
        if max_concurrent == 1:
            # Sequential processing
            for partition in partitions:
                batches = await process_partition(partition)
                for batch in batches:
                    yield batch
        else:
            # Concurrent processing
            tasks = [process_partition(partition) for partition in partitions]
            batch_lists = await asyncio.gather(*tasks)
            
            # Yield all batches
            for batch_list in batch_lists:
                for batch in batch_list:
                    yield batch
    
    async def _process_delta_partitions_concurrent(self, job_context, partitions: List[Partition], 
                                                 strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
        """Process delta sync partitions concurrently"""
        max_concurrent = self.sync_engine.config.max_concurrent_partitions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_partition(partition: Partition) -> List[DataBatch]:
            async with semaphore:
                batches = []
                # Generate sub-partitions if configured
                sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
                
                for sub_partition in sub_partitions:
                    # For delta sync, we need to fetch changed data
                    batch = DataBatch(
                        data=[],  # Will be populated by DataFetch stage
                        context=job_context,
                        batch_metadata={
                            "change_type": "delta_sync",
                            "partition": sub_partition,
                            "needs_fetch": True,
                            "partition_id": sub_partition.partition_id,
                            "strategy_config": strategy_config
                        }
                    )
                    batches.append(batch)
                return batches
        
        # Process partitions concurrently
        if max_concurrent == 1:
            # Sequential processing
            for partition in partitions:
                batches = await process_partition(partition)
                for batch in batches:
                    yield batch
        else:
            # Concurrent processing
            tasks = [process_partition(partition) for partition in partitions]
            batch_lists = await asyncio.gather(*tasks)
            
            # Yield all batches
            for batch_list in batch_lists:
                for batch in batch_list:
                    yield batch
    
    async def _process_hash_partitions_concurrent(self, job_context, partitions: List[Partition], 
                                                strategy_config: StrategyConfig) -> AsyncIterator[DataBatch]:
        """Process hash sync partitions concurrently"""
        max_concurrent = self.sync_engine.config.max_concurrent_partitions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_partition(partition: Partition) -> List[DataBatch]:
            async with semaphore:
                # Build intervals for hash sync
                if strategy_config.intervals:
                    partition.intervals = strategy_config.intervals
                else:
                    partition.intervals = build_intervals(
                        getattr(strategy_config, 'partition_step', 1000),
                        strategy_config.min_sub_partition_step,
                        strategy_config.interval_reduction_factor
                    )
                
                # Calculate sub-partitions and their statuses
                partitions_with_status, statuses = await self._calculate_sub_partitions(
                    partition,
                    max_level=len(partition.intervals)-1,
                    page_size=strategy_config.page_size
                )
                
                partitions_with_status, statuses = merge_adjacent(
                    partitions_with_status, statuses, strategy_config.page_size
                )
                
                batches = []
                for part, status in zip(partitions_with_status, statuses):
                    change_type = None
                    needs_fetch = False
                    needs_row_comparison = False
                    
                    if status == DataStatus.ADDED:
                        change_type = "added"
                        needs_fetch = True
                    elif status == DataStatus.DELETED:
                        change_type = "deleted"
                        needs_fetch = False
                    elif status == DataStatus.MODIFIED:
                        change_type = "modified"
                        needs_fetch = True
                        needs_row_comparison = strategy_config.prevent_update_unless_changed
                    
                    if change_type:
                        batch = DataBatch(
                            data=[],  # Will be populated by DataFetch stage if needed
                            context=job_context,
                            batch_metadata={
                                "change_type": change_type,
                                "partition": part,
                                "needs_fetch": needs_fetch,
                                "needs_row_comparison": needs_row_comparison,
                                "rows_affected": part.num_rows,
                                "partition_id": part.partition_id,
                                "strategy_config": strategy_config
                            }
                        )
                        batches.append(batch)
                return batches
        
        # Process partitions concurrently
        if max_concurrent == 1:
            # Sequential processing
            for partition in partitions:
                batches = await process_partition(partition)
                for batch in batches:
                    yield batch
        else:
            # Concurrent processing
            tasks = [process_partition(partition) for partition in partitions]
            batch_lists = await asyncio.gather(*tasks)
            
            # Yield all batches
            for batch_list in batch_lists:
                for batch in batch_list:
                    yield batch
    
    async def _generate_sub_partitions(self, partition: Partition, strategy_config: StrategyConfig) -> List[Partition]:
        """Generate sub-partitions if configured"""
        if strategy_config.use_sub_partitions:
            sub_partition_config = PartitionConfig(
                name="sub_partition_{pid}",
                column=partition.column,
                column_type=partition.column_type,
                partition_step=strategy_config.sub_partition_step
            )
            sub_partition_generator = PartitionGenerator(sub_partition_config)
            return await sub_partition_generator.generate_partitions(
                partition.start, partition.end, partition
            )
        else:
            return [partition]
    
    async def _calculate_sub_partitions(self, partition: Partition, max_level: int = 100, 
                                      page_size: int = 1000) -> Tuple[List[Partition], List[str]]:
        """Calculate sub-partitions for hash sync"""
        # Create backends from stage configurations
        change_detection_stage = self.sync_engine.stage_configs.get('change_detection')
        if not change_detection_stage:
            raise ValueError("change_detection stage configuration not found")
        
        source_backend = await self.sync_engine.create_backend_from_stage(change_detection_stage, role="source")
        destination_backend = await self.sync_engine.create_backend_from_stage(change_detection_stage, role="destination")
        
        final_partitions, statuses = [], []
        try:
            src_rows = await source_backend.fetch_child_partition_hashes(
                partition, hash_algo=self.sync_engine.hash_algo
            )
            destination_rows = await destination_backend.fetch_child_partition_hashes(
                partition, hash_algo=self.sync_engine.hash_algo
            )
            
            s_partitions = to_partitions(src_rows, partition)
            d_partitions = to_partitions(destination_rows, partition)
            partitions, status_map = calculate_partition_status(s_partitions, d_partitions)
            
            for p in partitions:
                key = (p.start, p.end, p.level)
                st = status_map[key]
                
                if st in ('M', 'A') and (p.num_rows > page_size and p.level < max_level):
                    deeper_partitions, deeper_statuses = await self._calculate_sub_partitions(
                        p, max_level=max_level, page_size=page_size
                    )
                    final_partitions.extend(deeper_partitions)
                    statuses.extend(deeper_statuses)
                else:
                    final_partitions.append(p)
                    statuses.append(st)
            
        finally:
            await source_backend.disconnect()
            await destination_backend.disconnect()
        
        return final_partitions, statuses