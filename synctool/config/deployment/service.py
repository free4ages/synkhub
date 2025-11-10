"""
Deployment service for pipeline configurations.
"""
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

from ...core.models import PipelineJobConfig, DataStorage
from ...utils.schema_manager import SchemaManager
from ..config_loader import ConfigLoader
from ..build.manager import ConfigBuildManager
from ..build.scanner import ConfigScanner
from ..build.metadata import MetadataManager
from .models import DeploymentResult, DeploymentStatus


class DeploymentService:
    """
    Service for deploying pipeline configurations.
    Handles DDL application and config building.
    """
    
    def __init__(self, build_manager: ConfigBuildManager):
        """
        Initialize deployment service.
        
        Args:
            build_manager: ConfigBuildManager instance
        """
        self.build_manager = build_manager
        self.scanner = build_manager.scanner
        self.metadata_manager = build_manager.metadata_manager
        self.schema_manager = SchemaManager(logging.getLogger(__name__))
        self.logger = logging.getLogger(__name__)
    
    async def deploy_config(
        self,
        config_name: Union[str, Path],
        apply_ddl: bool = False,
        if_not_exists: bool = True
    ) -> DeploymentResult:
        """
        Deploy a single config with DDL management.
        
        Args:
            config_name: Name of pipeline config
            apply_ddl: Whether to apply DDL changes
            if_not_exists: Add IF NOT EXISTS clause for CREATE TABLE
            
        Returns:
            DeploymentResult
        """
        self.logger.info(f"Deploying config: {config_name}")
        async with self.build_manager.lock_manager:
            try:
                # Load config
                config_path = self.scanner.get_config_path(config_name)
                
                if not config_path:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error=f"Config '{config_name}' not found"
                    )
                
                config = self.scanner.load_config(config_path)
                if not config:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error="Failed to load config"
                    )
                
                # Load datastores
                datastores = self.scanner.load_datastores()
                if not datastores:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error="Failed to load datastores"
                    )
                
                # Validate config
                issues = ConfigLoader.validate_pipeline_with_datastores(config, datastores)
                if issues:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error=f"Validation failed: {', '.join(issues)}"
                    )
                
                # Check and apply DDL if needed
                ddl_result = await self._handle_ddl(
                    config,
                    datastores,
                    apply_ddl,
                    if_not_exists
                )
                
                if ddl_result['error']:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error=ddl_result['error'],
                        ddl_statements=ddl_result['ddl_statements']
                    )
                
                # Build config
                success = await self.build_manager._build_config_atomic(
                    config_name,
                    config,
                    config_path,
                    datastores
                )
                
                if not success:
                    return DeploymentResult(
                        config_name=config_name,
                        status=DeploymentStatus.FAILED,
                        error="Failed to build config"
                    )
                
                # Get version
                metadata = self.metadata_manager.load_metadata(config_name)
                version = metadata.version if metadata else None
                
                return DeploymentResult(
                    config_name=config_name,
                    status=DeploymentStatus.SUCCESS,
                    version=version,
                    ddl_applied=ddl_result['applied'],
                    ddl_statements=ddl_result['ddl_statements'],
                    message="Config deployed successfully"
                )
                
            except Exception as e:
                self.logger.error(f"Deployment failed for {config_name}: {e}", exc_info=True)
                return DeploymentResult(
                    config_name=config_name,
                    status=DeploymentStatus.FAILED,
                    error=str(e)
                )
    
    async def _handle_ddl(
        self,
        config: PipelineJobConfig,
        datastores: DataStorage,
        apply_ddl: bool,
        if_not_exists: bool
    ) -> Dict[str, Any]:
        """
        Handle DDL checks and application.
        
        Returns:
            Dict with 'applied', 'ddl_statements', and 'error' keys
        """
        result = {
            'applied': False,
            'ddl_statements': [],
            'error': None
        }
        
        # Find populate stage
        populate_stage = None
        for stage in config.stages:
            if stage.type == 'populate':
                populate_stage = stage
                break
        
        if not populate_stage:
            # No populate stage, no DDL needed
            return result
        
        if not populate_stage.destination:
            return result
        
        # Get datastore
        datastore = datastores.get_datastore(populate_stage.destination.datastore_name)
        if not datastore:
            result['error'] = f"Datastore not found: {populate_stage.destination.datastore_name}"
            return result
        
        # Connect to datastore
        try:
            await datastore.connect(self.logger)
            
            # Check DDL first (without applying)
            schema_result = await self.schema_manager.ensure_table_schema(
                datastore=datastore,
                columns=populate_stage.destination.columns,
                table_name=populate_stage.destination.table,
                schema_name=populate_stage.destination.schema,
                apply=False,
                if_not_exists=if_not_exists
            )
            
            result['ddl_statements'] = schema_result.get('ddl_statements', [])
            
            # Apply DDL if requested and statements exist
            if apply_ddl and result['ddl_statements']:
                self.logger.info(f"Applying {len(result['ddl_statements'])} DDL statement(s)")
                
                apply_result = await self.schema_manager.ensure_table_schema(
                    datastore=datastore,
                    columns=populate_stage.destination.columns,
                    table_name=populate_stage.destination.table,
                    schema_name=populate_stage.destination.schema,
                    apply=True,
                    if_not_exists=if_not_exists
                )
                
                result['applied'] = True
                self.logger.info("DDL applied successfully")
            
        except Exception as e:
            result['error'] = f"DDL operation failed: {str(e)}"
            self.logger.error(f"DDL operation failed: {e}", exc_info=True)
        
        finally:
            try:
                await datastore.disconnect(self.logger)
            except:
                pass
        
        return result
    
    async def deploy_all_pending(
        self,
        apply_ddl: bool = False,
        ddl_only: bool = False
    ) -> List[DeploymentResult]:
        """
        Deploy all configs that require manual deployment.
        
        Args:
            apply_ddl: Whether to apply DDL changes
            ddl_only: Only deploy configs that require DDL
            
        Returns:
            List of DeploymentResults
        """
        results = []
        
        # Get pending configs
        pending = self.metadata_manager.list_pending_configs()
        
        if not pending:
            self.logger.info("No configs pending deployment")
            return results
        
        # Filter if ddl_only
        if ddl_only:
            pending = [p for p in pending if p.get('ddl_required', False)]
        
        self.logger.info(f"Deploying {len(pending)} pending configs")
        
        for pending_config in pending:
            config_name = pending_config['name']
            
            result = await self.deploy_config(
                config_name,
                apply_ddl=apply_ddl
            )
            
            results.append(result)
        
        return results
    
    def get_deployment_status(self) -> Dict[str, Any]:
        """
        Get deployment status of all configs.
        
        Returns:
            Dict with deployment status information
        """
        pending = self.metadata_manager.list_pending_configs()
        built = self.metadata_manager.list_built_configs()
        
        return {
            'total_built': len(built),
            'pending_deployment': len(pending),
            'pending_configs': pending
        }
    
    def get_pending_configs(self) -> List[Dict[str, Any]]:
        """
        Get list of configs pending deployment.
        
        Returns:
            List of pending config info
        """
        return self.metadata_manager.list_pending_configs()

