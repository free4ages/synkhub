import asyncio
import logging
from datetime import datetime
# from tkinter import W
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from ..core.models import DataStorage, GlobalStageConfig

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector
    from ..utils.progress_manager import ProgressManager

from ..core.enums import HashAlgo
if TYPE_CHECKING:
    from ..sync.sync_engine import SyncEngine







@dataclass
class PipelineJobConfig:
    """Complete pipeline-based sync job configuration"""
    name: str
    description: str
    max_concurrent_partitions: int = 1
    
    # Global column definitions that all stages inherit
    columns: List[Dict[str, Any]] = field(default_factory=list)
    
    # Pipeline stages configuration
    stages: List[Dict[str, Any]] = field(default_factory=list)
    
    # Optional enrichment configuration
    # enrichment: Optional[Dict[str, Any]] = None
    hash_algo: Optional[HashAlgo] = HashAlgo.HASH_MD5_HASH


@dataclass
class JobContext:
    """Context for the entire sync job - not tied to a specific partition or strategy"""
    job_name: str
    user_strategy_name: Optional[str] = None  # Strategy name requested by user
    user_bounds: Optional[Dict[str, Any]] = None  # Bounds requested by user
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineBuilder:
    """Pipeline builder that works with stage configurations"""
    
    def __init__(self, sync_engine: 'SyncEngine', logger=None, data_storage: Optional[DataStorage] = None, progress_manager: Optional['ProgressManager'] = None):
        self.sync_engine = sync_engine
        self.logger = logger
        self.data_storage = data_storage
        self.progress_manager = progress_manager
    
    def build_pipeline_from_stages(self, pipeline_config: PipelineJobConfig):
        """Build a pipeline from stage configurations"""
        from .base import Pipeline
        from .stages.change_detection import ChangeDetectionStage
        from .stages.data_fetch import DataFetchStage
        from .stages.transform import TransformStage
        from .stages.enrich import EnrichStage
        from .stages.batcher import BatcherStage
        from .stages.populate import PopulateStage
        from .stages.partition import PartitionStage
        from .stages.dedup import DedupStage
        from .stages.join import JoinStage
        
        pipeline = Pipeline(pipeline_config, self.logger)
        
        # Add stages in the order they appear in configuration
        for stage_config in pipeline.config.stages:
            if not stage_config.enabled:
                continue
            if stage_config.type == 'partition':
                stage = PartitionStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
                
            elif stage_config.type == 'change_detection':
                stage = ChangeDetectionStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'data_fetch':
                stage = DataFetchStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'transform':
                stage = TransformStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'batcher':
                stage = BatcherStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'dedup':
                stage = DedupStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'custom':
                # Handle custom stages
                if stage_config.class_path:
                    # Import and instantiate custom stage
                    module_path, class_name = stage_config.class_path.rsplit('.', 1)
                    module = __import__(module_path, fromlist=[class_name])
                    custom_class = getattr(module, class_name)
                    stage = custom_class(
                        self.sync_engine,
                        stage_config,
                        pipeline_config,
                        self.logger,
                        self.data_storage,
                        self.progress_manager
                    )
                    pipeline.add_stage(stage)
            elif stage_config.type == 'populate':
                stage = PopulateStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'join':
                stage = JoinStage(
                    self.sync_engine,
                    stage_config,
                    pipeline_config,
                    self.logger,
                    self.data_storage,
                    self.progress_manager
                )
                pipeline.add_stage(stage)
        
        return pipeline
    
    def create_job_context(self, user_strategy_name: Optional[str], user_bounds: Optional[List[Dict[str, Any]]]) -> JobContext:
        """Create context for the entire job"""
        return JobContext(
            job_name=self.sync_engine.config.name,
            user_strategy_name=user_strategy_name,
            user_bounds={bound["column"]: bound for bound in user_bounds} if user_bounds else {}
        )