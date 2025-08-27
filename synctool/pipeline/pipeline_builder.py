import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from ..core.models import DataStorage

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector

from ..core.enums import HashAlgo
if TYPE_CHECKING:
    from ..sync.sync_engine import SyncEngine


@dataclass
class GlobalStageConfig:
    """Configuration for a single pipeline stage"""
    name: str
    type: Optional[str] = None
    enabled: bool = True
    source: Optional[Dict[str, Any]] = None
    destination: Optional[Dict[str, Any]] = None
    backend: Optional[Dict[str, Any]] = None
    config: Optional[Dict[str, Any]] = None
    class_path: Optional[str] = None
    columns: List[Dict[str, Any]] = field(default_factory=list)
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    use_pagination: Optional[bool] = None
    page_size: Optional[int] = None
    target_batch_size: Optional[int] = None
    batch_timeout: Optional[int] = None
    batch_timeout_unit: Optional[str] = None
    batch_timeout_value: Optional[int] = None
    strategies: List[Dict[str, Any]] = field(default_factory=list)




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
    user_start: Any = None  # Start bounds requested by user
    user_end: Any = None    # End bounds requested by user
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineBuilder:
    """Pipeline builder that works with stage configurations"""
    
    def __init__(self, sync_engine: 'SyncEngine', logger=None, data_storage: Optional[DataStorage] = None):
        self.sync_engine = sync_engine
        self.logger = logger
        self.data_storage = data_storage
    
    def build_pipeline_from_stages(self, stage_configs: Dict[str, GlobalStageConfig]):
        """Build a pipeline from stage configurations"""
        from .base import Pipeline, PipelineConfig
        from .stages.change_detection import ChangeDetectionStage
        from .stages.data_fetch import DataFetchStage
        from .stages.transform import TransformStage
        from .stages.enrich import EnrichStage
        from .stages.batcher import BatcherStage
        from .stages.populate import PopulateStage
        
        pipeline_cfg = PipelineConfig(
            name=f"{self.sync_engine.config.name}",
            max_concurrent_batches=self.sync_engine.config.max_concurrent_partitions,
            batch_size=1000
        )
        
        pipeline = Pipeline(pipeline_cfg, self.logger)
        
        # Add stages in the order they appear in configuration
        for stage_config in stage_configs:
            if not stage_config.enabled:
                continue
                
            if stage_config.type == 'change_detection':
                stage = ChangeDetectionStage(
                    self.sync_engine,
                    stage_config.__dict__,
                    self.logger,
                    self.data_storage
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'data_fetch':
                stage = DataFetchStage(
                    self.sync_engine,
                    stage_config.__dict__,
                    self.logger,
                    self.data_storage
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'transform':
                stage = TransformStage(
                    stage_config.__dict__,
                    self.logger,
                    self.data_storage
                )
                pipeline.add_stage(stage)
            elif stage_config.type == 'batcher':
                stage = BatcherStage(
                    stage_config.__dict__,
                    self.logger,
                    self.data_storage
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
                        stage_config.config or {},
                        self.logger,
                        self.data_storage
                    )
                    pipeline.add_stage(stage)
            elif stage_config.type == 'populate':
                stage = PopulateStage(
                    self.sync_engine,
                    stage_config.__dict__,
                    self.logger,
                    self.data_storage
                )
                pipeline.add_stage(stage)
        
        return pipeline
    
    def create_job_context(self, user_strategy_name: Optional[str], user_start: Any, user_end: Any) -> JobContext:
        """Create context for the entire job"""
        return JobContext(
            job_name=self.sync_engine.config.name,
            user_strategy_name=user_strategy_name,
            user_start=user_start,
            user_end=user_end
        )