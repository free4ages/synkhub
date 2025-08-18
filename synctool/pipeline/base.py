import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, AsyncIterator, TypeVar, Generic, Union
from datetime import datetime
import uuid

from ..core.models import Partition, StrategyConfig

# Type variables for pipeline data
T = TypeVar('T')
U = TypeVar('U')


@dataclass
class PipelineContext:
    """Context object that flows through the pipeline stages"""
    partition: Partition
    strategy_config: StrategyConfig
    metadata: Dict[str, Any] = field(default_factory=dict)
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    stage_results: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class DataBatch:
    """Data batch flowing through pipeline"""
    data: List[Dict[str, Any]]
    context: PipelineContext
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
    
    def __init__(self, name: str, config: Dict[str, Any] = None, logger: Optional[logging.Logger] = None):
        self.name = name
        self.config = config or {}
        self.logger = logger or logging.getLogger(f"{__name__}.{name}")
        self.enabled = self.config.get('enabled', True)
        self.stage_id = str(uuid.uuid4())
    
    @abstractmethod
    async def process(self, input_stream: AsyncIterator[T]) -> AsyncIterator[U]:
        """Process the input stream and yield output"""
        pass
    
    async def setup(self, context: PipelineContext) -> None:
        """Setup stage before processing (optional override)"""
        pass
    
    async def teardown(self, context: PipelineContext) -> None:
        """Cleanup after processing (optional override)"""
        pass
    
    def should_process(self, context: PipelineContext) -> bool:
        """Determine if this stage should process the given context"""
        return self.enabled


class BatchProcessor(PipelineStage[DataBatch, DataBatch]):
    """Base class for stages that process data batches"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None, logger: Optional[logging.Logger] = None):
        super().__init__(name, config, logger)
        self.max_batch_size = self.config.get('max_batch_size', 1000)
        self.buffer_size = self.config.get('buffer_size', 10)
    
    @abstractmethod
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Process a single batch"""
        pass
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Process batches with concurrency control"""
        semaphore = asyncio.Semaphore(self.buffer_size)
        
        async def process_with_semaphore(batch: DataBatch) -> DataBatch:
            async with semaphore:
                if not self.should_process(batch.context):
                    return batch
                
                start_time = datetime.now()
                try:
                    result = await self.process_batch(batch)
                    duration = (datetime.now() - start_time).total_seconds() * 1000
                    
                    # Record stage result
                    stage_result = StageResult(
                        stage_name=self.name,
                        success=True,
                        data_processed=batch.size,
                        duration_ms=duration
                    )
                    result.context.stage_results[self.name] = stage_result
                    
                    self.logger.debug(f"Processed batch {batch.batch_id} in {duration:.2f}ms")
                    return result
                    
                except Exception as e:
                    duration = (datetime.now() - start_time).total_seconds() * 1000
                    stage_result = StageResult(
                        stage_name=self.name,
                        success=False,
                        error=str(e),
                        duration_ms=duration
                    )
                    batch.context.stage_results[self.name] = stage_result
                    self.logger.error(f"Failed to process batch {batch.batch_id}: {e}")
                    raise
        
        # Process batches with controlled concurrency
        tasks = []
        async for batch in input_stream:
            task = asyncio.create_task(process_with_semaphore(batch))
            tasks.append(task)
            
            # Limit concurrent tasks
            if len(tasks) >= self.buffer_size:
                completed_tasks, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in completed_tasks:
                    yield await task
                tasks = list(pending_tasks)
        
        # Process remaining tasks
        if tasks:
            for task in asyncio.as_completed(tasks):
                yield await task


@dataclass
class PipelineConfig:
    """Configuration for the entire pipeline"""
    name: str
    stages: List[Dict[str, Any]] = field(default_factory=list)
    global_config: Dict[str, Any] = field(default_factory=dict)
    max_concurrent_batches: int = 10
    batch_size: int = 1000
    enable_metrics: bool = True
    timeout_seconds: Optional[int] = None


@dataclass
class PipelineStats:
    """Statistics for pipeline execution"""
    pipeline_id: str
    total_batches: int = 0
    processed_batches: int = 0
    failed_batches: int = 0
    total_rows: int = 0
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
    
    def __init__(self, config: PipelineConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger(f"{__name__}.pipeline.{config.name}")
        self.stages: List[PipelineStage] = []
        self.stats = PipelineStats(pipeline_id=str(uuid.uuid4()))
    
    def add_stage(self, stage: PipelineStage) -> 'Pipeline':
        """Add a stage to the pipeline"""
        self.stages.append(stage)
        return self
    
    async def execute(self, context: PipelineContext) -> PipelineStats:
        """Execute the pipeline for a given context"""
        self.stats.start_time = datetime.now()
        
        try:
            # Setup all stages
            for stage in self.stages:
                if stage.should_process(context):
                    await stage.setup(context)
            
            # Create initial stream from context
            current_stream = self._create_context_stream(context)
            
            # Chain stages together
            for stage in self.stages:
                if stage.should_process(context):
                    self.logger.info(f"Executing stage: {stage.name}")
                    current_stream = stage.process(current_stream)
            
            # Consume the final stream to drive the pipeline
            async for batch in current_stream:
                self.stats.processed_batches += 1
                self.stats.total_rows += batch.size
                self.logger.debug(f"Pipeline processed batch {batch.batch_id}")
            
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
    
    async def _create_context_stream(self, context: PipelineContext) -> AsyncIterator[PipelineContext]:
        """Create stream from single context"""
        yield context
