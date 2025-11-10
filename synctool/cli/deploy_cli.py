"""
CLI for deploying pipeline configurations.
Thin wrapper over DeploymentService.
"""
import asyncio
import click
import logging
from pathlib import Path

from ..config.build.manager import ConfigBuildManager
from ..config.deployment.service import DeploymentService
from ..config.deployment.models import DeploymentStatus
from ..config.global_config_loader import load_global_config


@click.group()
def deploy():
    """Deploy pipeline configurations"""
    pass


@deploy.command()
@click.option('--config', 'config_name', required=True, help='Pipeline name to deploy')
@click.option('--yes', '-y', is_flag=True, help='Auto-approve and apply DDL changes')
@click.option('--if-not-exists', is_flag=True, help='Add IF NOT EXISTS clause for CREATE TABLE')
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', help='Log level')
def config(config_name: str, yes: bool, if_not_exists: bool, global_config: str, log_level: str):
    """Deploy a single pipeline configuration"""
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    async def run_deploy():
        # Load global config
        if global_config:
            global_cfg = load_global_config(global_config)
        else:
            global_cfg = load_global_config()
        
        # Initialize build manager
        build_manager = ConfigBuildManager(
            user_config_dir=Path(global_cfg.build_system.user_config_dir),
            built_config_dir=Path(global_cfg.build_system.built_config_dir),
            datastores_path=Path(global_cfg.build_system.datastores_path),
            ddl_check_on_build=global_cfg.build_system.ddl_check_on_build,
            on_build_failure=global_cfg.build_system.on_build_failure,
            remove_orphaned_configs=global_cfg.build_system.remove_orphaned_configs,
            allow_empty_source_dir=global_cfg.build_system.allow_empty_source_dir,
            build_lock_timeout=global_cfg.build_system.build_lock_timeout
        )
        
        # Initialize deployment service
        deployment_service = DeploymentService(build_manager)
        
        # Check if DDL is required
        pending = deployment_service.get_pending_configs()
        pending_config = None
        for p in pending:
            if p['name'] == config_name:
                pending_config = p
                break
        
        apply_ddl = yes
        
        # If DDL required and not auto-approved, show DDL and prompt
        if pending_config and pending_config.get('ddl_statements'):
            click.echo(f"\n{'='*80}")
            click.echo(f"DDL Changes Required for: {config_name}")
            click.echo(f"{'='*80}\n")
            
            for ddl in pending_config['ddl_statements']:
                click.echo(ddl)
                click.echo()
            
            if not yes:
                apply_ddl = click.confirm("\nDo you want to apply these DDL changes?")
                if not apply_ddl:
                    click.echo("\nDeployment cancelled. DDL changes were not applied.")
                    return
        
        # Deploy config
        logger.info(f"Deploying config: {config_name}")
        
        result = await deployment_service.deploy_config(
            config_name=config_name,
            apply_ddl=apply_ddl,
            if_not_exists=if_not_exists
        )
        
        # Display result
        click.echo(f"\n{'='*80}")
        
        if result.status == DeploymentStatus.SUCCESS:
            click.echo(f"✅ Deployment Successful")
            click.echo(f"{'='*80}")
            click.echo(f"Config: {result.config_name}")
            click.echo(f"Version: {result.version}")
            click.echo(f"DDL Applied: {'Yes' if result.ddl_applied else 'No'}")
            if result.message:
                click.echo(f"Message: {result.message}")
            click.echo(f"\n📝 Note: Restart scheduler to pick up changes")
        else:
            click.echo(f"❌ Deployment Failed")
            click.echo(f"{'='*80}")
            click.echo(f"Config: {result.config_name}")
            click.echo(f"Error: {result.error}")
        
        click.echo(f"{'='*80}\n")
    
    try:
        asyncio.run(run_deploy())
    except Exception as e:
        logger.error(f"Deployment failed: {e}", exc_info=True)
        raise click.ClickException(str(e))


@deploy.command('all')
@click.option('--yes', '-y', is_flag=True, help='Auto-approve all DDL changes')
@click.option('--ddl-only', is_flag=True, help='Only deploy configs requiring DDL')
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', help='Log level')
def deploy_all(yes: bool, ddl_only: bool, global_config: str, log_level: str):
    """Deploy all pending configurations"""
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    async def run_deploy():
        # Load global config
        if global_config:
            global_cfg = load_global_config(global_config)
        else:
            global_cfg = load_global_config()
        
        # Initialize build manager
        build_manager = ConfigBuildManager(
            user_config_dir=Path(global_cfg.build_system.user_config_dir),
            built_config_dir=Path(global_cfg.build_system.built_config_dir),
            datastores_path=Path(global_cfg.build_system.datastores_path),
            ddl_check_on_build=global_cfg.build_system.ddl_check_on_build,
            on_build_failure=global_cfg.build_system.on_build_failure,
            remove_orphaned_configs=global_cfg.build_system.remove_orphaned_configs,
            allow_empty_source_dir=global_cfg.build_system.allow_empty_source_dir,
            build_lock_timeout=global_cfg.build_system.build_lock_timeout
        )
        
        # Initialize deployment service
        deployment_service = DeploymentService(build_manager)
        
        # Get pending configs
        pending = deployment_service.get_pending_configs()
        
        if ddl_only:
            pending = [p for p in pending if p.get('ddl_required', False)]
        
        if not pending:
            click.echo("No configs pending deployment")
            return
        
        # Display summary
        click.echo(f"\n{'='*80}")
        click.echo(f"Found {len(pending)} config(s) requiring deployment:")
        click.echo(f"{'='*80}\n")
        
        for p in pending:
            click.echo(f"  - {p['name']}: {p.get('manual_deploy_reason', 'Unknown reason')}")
        
        click.echo()
        
        if not yes:
            proceed = click.confirm("Do you want to deploy these configs?")
            if not proceed:
                click.echo("Deployment cancelled")
                return
        
        # Deploy all
        logger.info(f"Deploying {len(pending)} configs")
        
        results = await deployment_service.deploy_all_pending(
            apply_ddl=yes,
            ddl_only=ddl_only
        )
        
        # Display results
        successful = sum(1 for r in results if r.status == DeploymentStatus.SUCCESS)
        failed = sum(1 for r in results if r.status == DeploymentStatus.FAILED)
        
        click.echo(f"\n{'='*80}")
        click.echo(f"Deployment Complete")
        click.echo(f"{'='*80}")
        click.echo(f"✅ Successful: {successful}")
        click.echo(f"❌ Failed: {failed}")
        click.echo(f"{'='*80}\n")
        
        if failed > 0:
            click.echo("Failed deployments:")
            for r in results:
                if r.status == DeploymentStatus.FAILED:
                    click.echo(f"  - {r.config_name}: {r.error}")
            click.echo()
    
    try:
        asyncio.run(run_deploy())
    except Exception as e:
        logger.error(f"Deployment failed: {e}", exc_info=True)
        raise click.ClickException(str(e))


@deploy.command('status')
@click.option('--global-config', default=None, help='Path to global config YAML')
def status(global_config: str):
    """Show deployment status of all configs"""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    async def show_status():
        # Load global config
        if global_config:
            global_cfg = load_global_config(global_config)
        else:
            global_cfg = load_global_config()
        
        # Initialize build manager
        build_manager = ConfigBuildManager(
            user_config_dir=Path(global_cfg.build_system.user_config_dir),
            built_config_dir=Path(global_cfg.build_system.built_config_dir),
            datastores_path=Path(global_cfg.build_system.datastores_path),
            ddl_check_on_build=global_cfg.build_system.ddl_check_on_build,
            on_build_failure=global_cfg.build_system.on_build_failure,
            remove_orphaned_configs=global_cfg.build_system.remove_orphaned_configs,
            allow_empty_source_dir=global_cfg.build_system.allow_empty_source_dir,
            build_lock_timeout=global_cfg.build_system.build_lock_timeout
        )
        
        # Initialize deployment service
        deployment_service = DeploymentService(build_manager)
        
        # Get status
        status_info = deployment_service.get_deployment_status()
        pending = status_info['pending_configs']
        
        click.echo(f"\n{'='*80}")
        click.echo(f"Deployment Status")
        click.echo(f"{'='*80}")
        click.echo(f"Total built configs: {status_info['total_built']}")
        click.echo(f"Pending deployment: {status_info['pending_deployment']}")
        click.echo(f"{'='*80}\n")
        
        if pending:
            click.echo("Configs requiring deployment:\n")
            for p in pending:
                click.echo(f"  📦 {p['name']}")
                click.echo(f"     Reason: {p.get('manual_deploy_reason', 'Unknown')}")
                click.echo(f"     DDL Required: {'Yes' if p.get('ddl_required') else 'No'}")
                click.echo()
            
            click.echo(f"Run: synctool deploy config --config <name>")
            click.echo(f"Or:  synctool deploy all\n")
        else:
            click.echo("✅ All configs are up to date\n")
    
    try:
        asyncio.run(show_status())
    except Exception as e:
        logger.error(f"Failed to get status: {e}", exc_info=True)
        raise click.ClickException(str(e))


if __name__ == '__main__':
    deploy()

