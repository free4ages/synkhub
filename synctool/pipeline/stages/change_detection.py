import asyncio
from typing import AsyncIterator, Dict, Any, List, TYPE_CHECKING

from ..base import PipelineStage, DataBatch, PipelineContext
from ...core.models import Partition, SyncStrategy
from ...core.enums import DataStatus
from ...utils.partition_generator import (
    PartitionConfig, PartitionGenerator, build_intervals, 
    merge_adjacent, calculate_partition_status, to_partitions
)

if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine


class ChangeDetectionStage(PipelineStage[PipelineContext, DataBatch]):
    """Stage that detects changes and generates data batches to process"""
    
    def __init__(self, sync_engine: Any, config: Dict[str, Any] = None, logger=None):
        super().__init__("change_detection", config, logger)
        self.sync_engine = sync_engine
    
    async def process(self, input_stream: AsyncIterator[PipelineContext]) -> AsyncIterator[DataBatch]:
        """Detect changes and yield data batches"""
        async for context in input_stream:
            strategy = context.strategy_config.type
            
            if strategy == SyncStrategy.FULL:
                async for batch in self._detect_full_changes(context):
                    yield batch
            elif strategy == SyncStrategy.DELTA:
                async for batch in self._detect_delta_changes(context):
                    yield batch
            elif strategy == SyncStrategy.HASH:
                async for batch in self._detect_hash_changes(context):
                    yield batch
    
    async def _detect_full_changes(self, context: PipelineContext) -> AsyncIterator[DataBatch]:
        """Detect changes for full sync - all data needs to be processed"""
        partition = context.partition
        strategy_config = context.strategy_config
        
        # Generate sub-partitions if configured
        sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
        
        for sub_partition in sub_partitions:
            # For full sync, all data in partition is considered "changed"
            batch = DataBatch(
                data=[],  # Will be populated by DataFetch stage
                context=context,
                batch_metadata={
                    "change_type": "full_sync",
                    "partition": sub_partition,
                    "needs_fetch": True
                }
            )
            yield batch
    
    async def _detect_delta_changes(self, context: PipelineContext) -> AsyncIterator[DataBatch]:
        """Detect changes for delta sync"""
        partition = context.partition
        strategy_config = context.strategy_config
        
        # Generate sub-partitions if configured
        sub_partitions = await self._generate_sub_partitions(partition, strategy_config)
        
        for sub_partition in sub_partitions:
            # For delta sync, we need to fetch changed data
            batch = DataBatch(
                data=[],  # Will be populated by DataFetch stage
                context=context,
                batch_metadata={
                    "change_type": "delta_sync",
                    "partition": sub_partition,
                    "needs_fetch": True
                }
            )
            yield batch
    
    async def _detect_hash_changes(self, context: PipelineContext) -> AsyncIterator[DataBatch]:
        """Detect changes for hash sync using hash comparison"""
        partition = context.partition
        strategy_config = context.strategy_config
        
        # Build intervals for hash sync
        if strategy_config.intervals:
            partition.intervals = strategy_config.intervals
        else:
            partition.intervals = build_intervals(
                self.sync_engine.config.partition_step,
                strategy_config.min_sub_partition_step,
                strategy_config.interval_reduction_factor
            )
        
        # Calculate sub-partitions and their statuses
        partitions, statuses = await self._calculate_sub_partitions(
            partition,
            max_level=len(partition.intervals)-1,
            page_size=strategy_config.page_size
        )
        
        partitions, statuses = merge_adjacent(partitions, statuses, strategy_config.page_size)
        
        for part, status in zip(partitions, statuses):
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
                    context=context,
                    batch_metadata={
                        "change_type": change_type,
                        "partition": part,
                        "needs_fetch": needs_fetch,
                        "needs_row_comparison": needs_row_comparison,
                        "rows_affected": part.num_rows
                    }
                )
                yield batch
    
    async def _generate_sub_partitions(self, partition: Partition, strategy_config) -> List[Partition]:
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
                                      page_size: int = 1000):
        """Calculate sub-partitions for hash sync"""
        src_rows = await self.sync_engine.source_provider.fetch_child_partition_hashes(
            partition, hash_algo=self.sync_engine.hash_algo
        )
        destination_rows = await self.sync_engine.destination_provider.fetch_child_partition_hashes(
            partition, hash_algo=self.sync_engine.hash_algo
        )
        
        s_partitions = to_partitions(src_rows, partition)
        d_partitions = to_partitions(destination_rows, partition)
        partitions, status_map = calculate_partition_status(s_partitions, d_partitions)
        
        final_partitions, statuses = [], []
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
        
        return final_partitions, statuses
