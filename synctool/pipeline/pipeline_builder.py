from typing import Dict, Any, List, Optional, TYPE_CHECKING
from .base import Pipeline, PipelineConfig, PipelineContext
from .stages.change_detection import ChangeDetectionStage
from .stages.data_fetch import DataFetchStage
from .stages.transform import TransformStage
from .stages.enrich import EnrichStage
from .stages.batcher import BatcherStage
from .stages.populate import PopulateStage

if TYPE_CHECKING:
    from ..sync.sync_engine import SyncEngine


class PipelineBuilder:
    """Builder for creating data processing pipelines"""
    
    def __init__(self, sync_engine: Any, logger=None):
        self.sync_engine = sync_engine
        self.logger = logger
    
    def build_pipeline(self, partition, strategy_config, pipeline_config: Optional[Dict[str, Any]] = None) -> Pipeline:
        """Build a complete pipeline based on configuration"""
        config = pipeline_config or {}
        
        pipeline_cfg = PipelineConfig(
            name=f"sync_pipeline_{partition.partition_id}",
            max_concurrent_batches=config.get('max_concurrent_batches', 10),
            batch_size=config.get('batch_size', 1000)
        )
        
        pipeline = Pipeline(pipeline_cfg, self.logger)
        
        # Always add ChangeDetection stage (required)
        change_detection = ChangeDetectionStage(
            self.sync_engine,
            config.get('change_detection', {}),
            self.logger
        )
        pipeline.add_stage(change_detection)
        
        # Add optional stages based on configuration
        stage_configs = config.get('stages', {})
        
        # DataFetch stage - usually enabled unless explicitly disabled
        if stage_configs.get('data_fetch', {}).get('enabled', True):
            data_fetch_config = stage_configs.get('data_fetch', {})
            # Inherit pagination settings from strategy config if not explicitly set
            if 'use_pagination' not in data_fetch_config:
                data_fetch_config['use_pagination'] = strategy_config.use_pagination
            if 'page_size' not in data_fetch_config:
                data_fetch_config['page_size'] = strategy_config.page_size
                
            data_fetch = DataFetchStage(
                self.sync_engine,
                data_fetch_config,
                self.logger
            )
            pipeline.add_stage(data_fetch)
        
        # Transform stage - optional, disabled by default
        if stage_configs.get('transform', {}).get('enabled', False):
            transform = TransformStage(
                stage_configs.get('transform', {}),
                self.logger
            )
            pipeline.add_stage(transform)
        
        # Enrich stage - enabled by default if enrichment engine exists
        if stage_configs.get('enrich', {}).get('enabled', True):
            enrich = EnrichStage(
                self.sync_engine,
                stage_configs.get('enrich', {}),
                self.logger
            )
            pipeline.add_stage(enrich)
        
        # Batcher stage - optional, disabled by default
        if stage_configs.get('batcher', {}).get('enabled', False):
            batcher_config = stage_configs.get('batcher', {})
            # Set default target batch size from strategy config if not specified
            if 'target_batch_size' not in batcher_config:
                batcher_config['target_batch_size'] = strategy_config.page_size
                
            batcher = BatcherStage(
                batcher_config,
                self.logger
            )
            pipeline.add_stage(batcher)
        
        # Always add Populate stage (required for final output)
        populate = PopulateStage(
            self.sync_engine,
            stage_configs.get('populate', {}),
            self.logger
        )
        pipeline.add_stage(populate)
        
        return pipeline
    
    def create_context(self, partition, strategy_config) -> PipelineContext:
        """Create pipeline context"""
        return PipelineContext(
            partition=partition,
            strategy_config=strategy_config
        )
    
    @staticmethod
    def get_default_pipeline_config(strategy_config) -> Dict[str, Any]:
        """Get default pipeline configuration based on strategy config"""
        return {
            'max_concurrent_batches': 10,
            'batch_size': strategy_config.page_size,
            'stages': {
                'change_detection': {
                    'enabled': True
                },
                'data_fetch': {
                    'enabled': True,
                    'use_pagination': strategy_config.use_pagination,
                    'page_size': strategy_config.page_size
                },
                'transform': {
                    'enabled': False,
                    'transformations': []
                },
                'enrich': {
                    'enabled': True
                },
                'batcher': {
                    'enabled': False,
                    'target_batch_size': strategy_config.page_size
                },
                'populate': {
                    'enabled': True
                }
            }
        }
