import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, AsyncIterator, TypeVar, Generic, Union, TYPE_CHECKING, Type
from datetime import datetime
import uuid

from ..core.models import GlobalStageConfig, StrategyConfig, PipelineJobConfig, MultiDimensionalPartition


# Type variables for pipeline data
T = TypeVar('T')
U = TypeVar('U')

@dataclass
class StageConfig:
    """Configuration for a single pipeline stage"""
    name: str = ""
    type: str = ""
    enabled: bool = True

    @classmethod
    def from_global_stage_config(cls, global_stage_config: GlobalStageConfig) -> 'StageConfig':
        cdict = {}
        for field in cls.__dataclass_fields__.keys():
            if hasattr(global_stage_config, field):
                cdict[field] = getattr(global_stage_config, field)
        return cls(**cdict)

@dataclass
class PipelineContext:
    """Context object that flows through the pipeline stages"""
    partition: MultiDimensionalPartition
    strategy_config: StrategyConfig
    metadata: Dict[str, Any] = field(default_factory=dict)
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    stage_results: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class DataBatch:
    """Data batch flowing through pipeline"""
    data: List[Dict[str, Any]]
    context: Any  # JobContext - using Any to avoid circular imports
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    batch_metadata: Dict[str, Any] = field(default_factory=dict)
    size: int = field(init=False)
    
    def __post_init__(self):
        self.size = len(self.data)


@dataclass
class StageResult:
    """Result from a pipeline stage"""
    stage_name: str
    success: bool
    data_processed: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: Optional[float] = None


class PipelineStage(ABC, Generic[T, U]):
    """Base class for all pipeline stages"""
    
    def __init__(self, name: str, config: StageConfig, logger: Optional[logging.Logger] = None):
        self.name = name
        self.config = config
        self.logger = logger or logging.getLogger(f"{__name__}.{name}")
        self.enabled = self.config.enabled
        self.stage_id = str(uuid.uuid4())
    
    @abstractmethod
    def process(self, input_stream: AsyncIterator[T]) -> AsyncIterator[U]:
        """Process the input stream and yield output"""
        pass
    
    async def setup(self, context: Any) -> None:
        """Setup stage before processing (optional override)"""
        pass
    
    async def teardown(self, context: Any) -> None:
        """Cleanup after processing (optional override)"""
        pass
    
    def should_process(self, context: Any) -> bool:
        """Determine if this stage should process the given context"""
        return self.enabled


# class BatchProcessor(PipelineStage[DataBatch, DataBatch]):
#     """Base class for stages that process data batches"""
    
#     def __init__(self, name: str, config: Union[StageConfig, Dict[str, Any]], logger: Optional[logging.Logger] = None):
#         super().__init__(name, config, logger)
#         # StageConfig is a dataclass, access attributes directly with defaults
#         self.max_batch_size = getattr(config, 'max_batch_size', 1000)
#         self.buffer_size = getattr(config, 'buffer_size', 10)
    
#     @abstractmethod
#     async def process_batch(self, batch: DataBatch) -> DataBatch:
#         """Process a single batch"""
#         pass
    
#     async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
#         """Process batches with concurrency control"""
#         semaphore = asyncio.Semaphore(self.buffer_size)
        
#         async def process_with_semaphore(batch: DataBatch) -> DataBatch:
#             async with semaphore:
#                 if not self.should_process(batch.context):
#                     return batch
                
#                 start_time = datetime.now()
#                 try:
#                     result = await self.process_batch(batch)
#                     duration = (datetime.now() - start_time).total_seconds() * 1000
                    
#                     # Record stage result in job context metadata
#                     stage_stats = result.context.metadata.get(f"{self.name}_stats", {})
#                     stage_stats.update({
#                         "success": True,
#                         "data_processed": batch.size,
#                         "duration_ms": duration
#                     })
#                     result.context.metadata[f"{self.name}_stats"] = stage_stats
                    
#                     self.logger.debug(f"Processed batch {batch.batch_id} in {duration:.2f}ms")
#                     return result
                    
#                 except Exception as e:
#                     duration = (datetime.now() - start_time).total_seconds() * 1000
#                     stage_stats = batch.context.metadata.get(f"{self.name}_stats", {})
#                     stage_stats.update({
#                         "success": False,
#                         "error": str(e),
#                         "duration_ms": duration
#                     })
#                     batch.context.metadata[f"{self.name}_stats"] = stage_stats
#                     self.logger.error(f"Failed to process batch {batch.batch_id}: {e}")
#                     raise
        
#         # Process batches with controlled concurrency
#         tasks: List[asyncio.Task[DataBatch]] = []
#         async for batch in input_stream:
#             task = asyncio.create_task(process_with_semaphore(batch))
#             tasks.append(task)
            
#             # Limit concurrent tasks
#             if len(tasks) >= self.buffer_size:
#                 completed_tasks, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
#                 for task in completed_tasks:
#                     yield await task
#                 tasks = list(pending_tasks)
        
#         # Process remaining tasks
#         if tasks:
#             for task in asyncio.as_completed(tasks):
#                 yield await task


@dataclass
class PipelineConfig:
    """Configuration for the entire pipeline"""
    name: str
    stages: List[Dict[str, Any]] = field(default_factory=list)
    max_concurrent_batches: int = 10
    batch_size: int = 1000
    enable_metrics: bool = True
    timeout_seconds: Optional[int] = None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution"""
    pipeline_id: str
    stage_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class Pipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config: PipelineJobConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(f"{__name__}.pipeline.{self.config.name}")
        self.stages: List[PipelineStage] = []
        self.stats = PipelineStats(pipeline_id=str(uuid.uuid4()))
    
    def add_stage(self, stage: PipelineStage) -> 'Pipeline':
        """Add a stage to the pipeline"""
        self.stages.append(stage)
        return self
    
    async def execute(self, context: Any) -> PipelineStats:
        """Execute the pipeline for a given context"""
        self.stats.start_time = datetime.now()
        
        try:
            # Setup all stages
            for stage in self.stages:
                if stage.should_process(context):
                    await stage.setup(context)
            
            # Check if we should use concurrent partition processing
            partition_stage = self._get_partition_stage()
            if partition_stage:
                strategy, strategy_config = await partition_stage.determine_strategy_for_context(context)
                max_concurrent_partitions = strategy_config.max_concurrent_partitions if strategy_config else 1
                if max_concurrent_partitions > 1:
                    await self._execute_with_concurrent_partitions(context, partition_stage, max_concurrent_partitions)
                else:
                    await self._execute_sequential(context)
            else:
                await self._execute_sequential(context)
            
            self.stats.end_time = datetime.now()
            self.logger.info(f"Pipeline completed in {self.stats.duration_seconds:.2f}s")
            
        except Exception as e:
            self.stats.end_time = datetime.now()
            self.logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Teardown all stages
            for stage in reversed(self.stages):
                if stage.should_process(context):
                    try:
                        await stage.teardown(context)
                    except Exception as e:
                        self.logger.error(f"Error in stage teardown {stage.name}: {e}")
        
        return self.stats
    
    async def _execute_with_concurrent_partitions(self, context: Any, partition_stage, max_concurrent_partitions: int):
        """Execute with concurrent partition processing - MUCH FASTER for IO-intensive work"""
        # Step 1: Run change detection to get all partition batches
        initial_stream = self._create_context_stream(context)
        partition_stream = partition_stage.process(initial_stream)
        
        # Collect all partition batches
        partition_batches = []
        async for batch in partition_stream:
            partition_batches.append(batch) 
        
        # Step 2: Process each partition concurrently through remaining stages
        max_concurrent = max_concurrent_partitions
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_partition_pipeline(batch: DataBatch):
            async with semaphore:
                try:
                    # Process this partition through all downstream stages
                    current_stream = self._create_single_batch_stream(batch)
                    
                    for stage in self.stages[1:]:  # Skip partition_stage (index 0)
                        if stage.should_process(batch.context):
                            current_stream = stage.process(current_stream)
                    
                    # Consume the stream to drive processing
                    async for final_batch in current_stream:
                        # self.stats.processed_batches += 1
                        # self.stats.total_rows += final_batch.size
                        self.logger.debug(f"Completed partition {batch.batch_metadata.get('partition_id', 'unknown')}")
                
                except Exception as e:
                    self.logger.error(f"Failed processing partition {batch.batch_metadata.get('partition_id', 'unknown')}: {e}")
                    raise
        
        # Step 3: Execute all partition pipelines concurrently
        if partition_batches:
            self.logger.info(f"Processing {len(partition_batches)} partitions concurrently (max_concurrent={max_concurrent})")
            tasks = [process_partition_pipeline(batch) for batch in partition_batches]
            await asyncio.gather(*tasks, return_exceptions=False)
    
    async def _execute_sequential(self, context: Any):
        """Execute pipeline sequentially (original behavior)"""
        # Create initial stream from context
        current_stream = self._create_context_stream(context)
        
        # Chain stages together
        for stage in self.stages:
            if stage.should_process(context):
                self.logger.info(f"Executing stage: {stage.name}")
                current_stream = stage.process(current_stream)
        
        # Consume the final stream to drive the pipeline
        async for batch in current_stream:
            # self.stats.processed_batches += 1
            # self.stats.total_rows += batch.size
            self.logger.debug(f"Pipeline processed batch {batch.batch_id}")
    
    def _get_partition_stage(self):
        """Get the change detection stage"""
        for stage in self.stages:
            if isinstance(stage.__class__.__name__, str) and 'PartitionStage' in stage.__class__.__name__:
                return stage
        return None
    
    async def _create_single_batch_stream(self, batch: DataBatch) -> AsyncIterator[DataBatch]:
        """Create stream from a single batch"""
        yield batch
    
    async def _create_context_stream(self, context: Any) -> AsyncIterator[Any]:
        """Create stream from single context"""
        yield context
