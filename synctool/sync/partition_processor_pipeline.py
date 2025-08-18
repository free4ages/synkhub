import logging
import traceback
from typing import Dict, Any, TYPE_CHECKING

from ..core.models import Partition, StrategyConfig, SyncStrategy
from ..utils.sync_result_builder import SyncResultBuilder
from ..pipeline.pipeline_builder import PipelineBuilder

if TYPE_CHECKING:
    from .sync_engine import SyncEngine


class PipelinePartitionProcessor:
    """Pipeline-based partition processor"""
    
    def __init__(self, sync_engine: Any, partition: Partition, strategy: SyncStrategy, 
                 strategy_config: StrategyConfig, logger=None):
        self.sync_engine = sync_engine
        self.partition = partition
        self.strategy = strategy
        self.strategy_config = strategy_config
        logger_name = f"{logger.name}.partition.{self.partition.partition_id}" if logger else f"{__name__}.partition.{self.partition.partition_id}"
        self.logger = logging.getLogger(logger_name)
        
        # Create pipeline builder
        self.pipeline_builder = PipelineBuilder(sync_engine, self.logger)
    
    async def process(self) -> Dict[str, Any]:
        """Process a single partition using pipeline architecture"""
        try:
            self.logger.info(f"Processing partition {self.partition.partition_id} with strategy {self.strategy.value} "
                           f"from {self.partition.start} to {self.partition.end}")
            
            if not self.strategy_config.use_sub_partitions and not self.strategy_config.use_pagination:
                self.logger.warning("use_sub_partitions and use_pagination are both False, this will be memory intensive")
            
            # Create pipeline context
            context = self.pipeline_builder.create_context(self.partition, self.strategy_config)
            
            # Get pipeline configuration from strategy config or use defaults
            pipeline_config = getattr(self.strategy_config, 'pipeline_config', None)
            if pipeline_config is None:
                pipeline_config = PipelineBuilder.get_default_pipeline_config(self.strategy_config)
            
            # Build pipeline based on strategy configuration
            pipeline = self.pipeline_builder.build_pipeline(
                self.partition, 
                self.strategy_config, 
                pipeline_config
            )
            
            # Execute pipeline
            stats = await pipeline.execute(context)
            
            # Extract statistics from context and pipeline stats
            populate_stats = context.stage_results.get("populate_stats", {})
            
            # Build result from pipeline statistics
            return SyncResultBuilder.build_partition_success_result(
                partition_id=self.partition.partition_id,
                strategy=self.strategy,
                rows_detected=stats.total_rows,
                rows_fetched=self._extract_stage_stat(context, 'data_fetch', 'data_fetched', 0),
                rows_inserted=populate_stats.get('total_rows_inserted', 0),
                rows_updated=populate_stats.get('total_rows_updated', 0),
                rows_deleted=populate_stats.get('total_rows_deleted', 0),
                hash_query_count=self._extract_stage_stat(context, 'data_fetch', 'hash_queries', 0),
                data_query_count=self._extract_stage_stat(context, 'data_fetch', 'fetch_queries', 0)
            )
            
        except Exception as e:
            self.logger.error(f"Failed processing partition {self.partition.partition_id}: {str(e)}\n"
                            f"Stacktrace:\n{traceback.format_exc()}")
            raise e
    
    def _extract_stage_stat(self, context, stage_name: str, stat_name: str, default_value: Any) -> Any:
        """Extract a specific statistic from stage results"""
        stage_result = context.stage_results.get(stage_name)
        if stage_result and hasattr(stage_result, 'metadata'):
            return stage_result.metadata.get(stat_name, default_value)
        return default_value


# Hybrid processor that can use either legacy or pipeline approach
class HybridPartitionProcessor:
    """Processor that can switch between legacy and pipeline approaches"""
    
    def __init__(self, sync_engine: Any, partition: Partition, strategy: SyncStrategy, 
                 strategy_config: StrategyConfig, logger=None):
        self.sync_engine = sync_engine
        self.partition = partition
        self.strategy = strategy
        self.strategy_config = strategy_config
        self.logger = logger
        
        # Choose processor based on configuration
        if getattr(strategy_config, 'enable_pipeline', True):
            self.processor = PipelinePartitionProcessor(
                sync_engine, partition, strategy, strategy_config, logger
            )
        else:
            # Import legacy processor
            from .partition_processor import PartitionProcessor as LegacyPartitionProcessor
            self.processor = LegacyPartitionProcessor(
                sync_engine, partition, strategy, strategy_config, logger
            )
    
    async def process(self) -> Dict[str, Any]:
        """Delegate to the chosen processor"""
        return await self.processor.process()
