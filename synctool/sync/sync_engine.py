import logging
from typing import Dict, Optional, Any, Callable, TYPE_CHECKING, List
from datetime import datetime
from typing import Type

from ..core.models import StrategyConfig, SyncProgress, DataStorage, BackendConfig
from ..core.enums import HashAlgo
from ..core.column_mapper import ColumnMapper, ColumnSchema
from ..utils.hash_calculator import HashCalculator
from ..utils.sync_result_builder import SyncResultBuilder
from ..utils.progress_manager import ProgressManager
from ..enrichment.enrichment_engine import EnrichmentEngine
from ..pipeline.pipeline_builder import PipelineBuilder, PipelineJobConfig, GlobalStageConfig, JobContext
from ..backend.base_backend import Backend

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector

class SyncEngine:
    """Pipeline-based sync engine that orchestrates the sync process using stages"""
    
    def __init__(self, config: PipelineJobConfig, metrics_collector: Optional['MetricsCollector'] = None, 
                 logger=None, data_storage: Optional[DataStorage] = None):
        self.config = config
        # self.strategies: Dict[str, StrategyConfig] = {}
        # self.enrichment_engine: Optional[EnrichmentEngine] = None
        # self.hash_calculator = HashCalculator()
        self.progress = SyncProgress()
        logger_name = f"{logger.name}.sync_engine" if logger else f"{__name__}.sync_engine"
        self.logger = logging.getLogger(logger_name)
        # self.hash_algo = config.hash_algo or HashAlgo.HASH_MD5_HASH
        # self.column_mapper: Optional[ColumnMapper] = None
        self.metrics_collector = metrics_collector
        self.data_storage = data_storage
        self.column_schema = ColumnSchema(config.columns)
        
        # # Store stage configurations by name for easy access
        # self.stage_configs: List[GlobalStageConfig] = []
        # for stage_config_dict in config.stages:
        #     stage_config = GlobalStageConfig(**stage_config_dict)
        #     self.stage_configs.append(stage_config)
    
    async def initialize(self):
        """Initialize all components for pipeline processing"""
        pass
        # Parse strategies from change_detection stage

        # @TODO Move this logic inside change detection stage
        # change_detection_stage = self.stage_configs.get('change_detection')
        # if not change_detection_stage:
        #     raise ValueError("change_detection stage is required")
            
        # # Extract strategies from change_detection stage
        # for strategy_config_dict in change_detection_stage.strategies:
        #     if not strategy_config_dict.get('enabled', True):
        #         continue
            
        #     strategy_config_copy = strategy_config_dict.copy()
        #     strategy = StrategyConfig(**strategy_config_copy)
        #     self.strategies[strategy.name] = strategy
        
        # Initialize enrichment engine if configured
        # if self.config.enrichment:
        #     from ..core.models import EnrichmentConfig
        #     enrichment_config = EnrichmentConfig(**self.config.enrichment)
        #     self.enrichment_engine = EnrichmentEngine(enrichment_config)
        #     await self.enrichment_engine.connect()

        
        # self.column_mapper = ColumnMapper(self.config.columns)
    
    async def sync(self, strategy_name: Optional[str] = None, bounds: Optional[List[Dict[str, Any]]] = None,
                   progress_callback: Optional[Callable[[SyncProgress], None]] = None) -> Dict[str, Any]:
        """Main sync method - simplified to just create and execute pipeline"""
        # import pdb;pdb.set_trace()
        try:
            await self.initialize()
            self.progress.start_time = datetime.now()
            self.logger.info(f"Starting pipeline sync job: {self.config.name}")
            
            # Create progress manager for centralized progress handling
            progress_manager = ProgressManager(
                progress=self.progress,
                metrics_collector=self.metrics_collector,
                progress_callback=progress_callback,
                logger=self.logger
            )
            
            # Initialize metrics
            progress_manager.update_progress()
            
            # Create pipeline builder with progress manager
            pipeline_builder = PipelineBuilder(self, self.logger, self.data_storage, progress_manager)
            
            # Create job context with user parameters
            job_context = pipeline_builder.create_job_context(strategy_name, bounds)
            
            # Build pipeline from stage configurations
            pipeline = pipeline_builder.build_pipeline_from_stages(self.config)
            
            self.logger.info(f"Executing pipeline {self.config.name}")
            
            # Execute pipeline - ChangeDetectionStage will handle everything
            stats = await pipeline.execute(job_context)
            
            # # Extract statistics from job context and pipeline stats
            # populate_stats = job_context.metadata.get("populate_stats", {})
            # change_detection_stats = job_context.metadata.get("change_detection_stats", {})
            
            # # Get the strategy that was actually used
            # used_strategy = job_context.metadata.get("used_strategy")
            # if used_strategy is None:
            #     # Fallback if strategy detection failed
            #     from ..core.enums import SyncStrategy
            #     used_strategy = SyncStrategy.FULL
            
            # # Build success result using centralized builder
            # sync_result = SyncResultBuilder.build_success_result(
            #     job_name=self.config.name,
            #     strategy=used_strategy,
            #     progress=self.progress,
            #     partitions_count=change_detection_stats.get('total_partitions_processed', stats.processed_batches),
            #     partition_results=[{
            #         'status': 'success',
            #         'rows_detected': stats.total_rows,
            #         'rows_fetched': self._extract_stat(job_context, 'data_fetch', 'data_fetched', 0),
            #         'rows_inserted': populate_stats.get('total_rows_inserted', 0),
            #         'rows_updated': populate_stats.get('total_rows_updated', 0),
            #         'rows_deleted': populate_stats.get('total_rows_deleted', 0),
            #         'hash_query_count': self._extract_stat(job_context, 'data_fetch', 'hash_queries', 0),
            #         'data_query_count': self._extract_stat(job_context, 'data_fetch', 'fetch_queries', 0)
            #     }],
            #     start_time=self.progress.start_time
            # )
            progress = self.progress
            sync_result = {
                'job_name': self.config.name,
                'strategy': job_context.metadata.get("used_strategy"),
                'status': 'completed',
                'total_partitions': progress.total_partitions,
                'processed_partitions': progress.processed_partitions,
                'skipped_partitions': progress.skipped_partitions,
                'failed_partitions': progress.failed_partitions,
                'total_rows_detected': progress.rows_detected,
                'total_rows_fetched': progress.rows_fetched,
                'total_rows_inserted': progress.rows_inserted,
                'total_rows_updated': progress.rows_updated,
                'total_rows_deleted': progress.rows_deleted,
                'total_rows_failed': progress.rows_failed,
                'detection_query_count': progress.detection_query_count,
                'hash_query_count': progress.hash_query_count,
                'data_query_count': progress.data_query_count,
                'total_primary_partitions': progress.total_primary_partitions,
                'duration': f"{(datetime.now() - self.progress.start_time).total_seconds()} seconds"
            }
            
            # Finalize metrics
            progress_manager.log_completion(sync_result['duration'])
            return sync_result
            
        except Exception as e:
            import traceback
            self.logger.error(f"Pipeline sync job failed: {traceback.format_exc()}")
            
            # Build failure result using centralized builder
            used_strategy = None
            try:
                if 'job_context' in locals():
                    used_strategy = job_context.metadata.get("used_strategy")
            except (NameError, AttributeError):
                pass
            
            return dict(
                job_name=self.config.name,
                error=str(e),
                progress=self.progress,
                start_time=self.progress.start_time,
                strategy=used_strategy
            )
        finally:
            await self.cleanup()
    
    # def _extract_stat(self, job_context: JobContext, stage_name: str, stat_name: str, default_value: Any) -> Any:
    #     """Extract a specific statistic from job context metadata"""
    #     stage_result = job_context.metadata.get(f"{stage_name}_stats", {})
    #     return stage_result.get(stat_name, default_value)
    
    def get_backend_class(self, backend_type: str) -> Type[Backend]: # type: ignore
        """Get backend class based on type"""
        if backend_type == 'postgres':
            from ..backend.postgres import PostgresBackend
            return PostgresBackend
        elif backend_type == 'mysql':
            from ..backend.mysql import MySQLBackend
            return MySQLBackend
        elif backend_type == 'clickhouse':
            from ..backend.clickhouse import ClickHouseBackend
            return ClickHouseBackend
        elif backend_type == 'duckdb':
            from ..backend.duckdb import DuckDBBackend
            return DuckDBBackend
        elif backend_type == 'starrocks':
            from ..backend.starrocks import StarRocksBackend
            return StarRocksBackend
        elif backend_type == 'starrocks_mysql':
            from ..backend.starrocks_mysql import StarRocksMySQLBackend
            return StarRocksMySQLBackend
        else:
            raise ValueError(f"Unsupported backend type: {backend_type}")
    
    def create_backend(self, backend_config: BackendConfig):
        """Create a backend instance with column schema"""
        column_schema = ColumnSchema(backend_config.columns)
        return self.get_backend_class(backend_config.type)(backend_config, column_schema, self.logger, self.data_storage)
    
    # async def create_backend_from_stage(self, stage_config: GlobalStageConfig, role: Optional[str] = None):
    #     """Create a backend directly from a stage configuration"""
    #     from ..backend.postgres import PostgresBackend
    #     from ..backend.mysql import MySQLBackend
    #     from ..backend.clickhouse import ClickHouseBackend
    #     from ..backend.duckdb import DuckDBBackend
        
    #     # Determine which backend configuration to use
    #     backend_config = None
    #     if hasattr(stage_config, 'source') and stage_config.source and role == "source":
    #         backend_config = stage_config.source
    #     elif hasattr(stage_config, 'destination') and stage_config.destination and role == "destination":
    #         backend_config = stage_config.destination
    #     elif hasattr(stage_config, 'backend') and stage_config.backend:
    #         backend_config = stage_config.backend
        
    #     if not backend_config:
    #         raise ValueError(f"No backend configuration found for stage {stage_config.name} with role {role}")
        
    #     # Create backend directly based on type
    #     backend_type = backend_config.get('type', '').lower()
        
    #     # Convert dict to BackendConfig-like object for now
    #     # TODO: Proper backend config handling
    #     if backend_type == 'postgres':
    #         backend = PostgresBackend(backend_config, logger=self.logger)
    #     elif backend_type == 'mysql':
    #         backend = MySQLBackend(backend_config, logger=self.logger)
    #     elif backend_type == 'clickhouse':
    #         backend = ClickHouseBackend(backend_config, logger=self.logger)
    #     elif backend_type == 'duckdb':
    #         backend = DuckDBBackend(backend_config, logger=self.logger)
    #     else:
    #         raise ValueError(f"Unsupported backend type: {backend_type}")
        
    #     await backend.connect()
    #     return backend
    
    # def get_stage_backend(self, stage_name: str, role: Optional[str] = None):
    #     """Get backend configuration for a specific stage"""
    #     stage_config = self.stage_configs.get(stage_name)
    #     if not stage_config:
    #         raise ValueError(f"Stage {stage_name} not found")
        
    #     return self.create_backend_from_stage(stage_config, role)
    
    async def cleanup(self):
        """Clean up resources"""
        pass
        # try:
        #     # Close enrichment engine connection
        #     if self.enrichment_engine:
        #         await self.enrichment_engine.disconnect()
                
        # except Exception as e:
        #     self.logger.error(f"Error during cleanup: {str(e)}")