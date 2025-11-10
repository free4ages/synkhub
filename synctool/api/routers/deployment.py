"""
API endpoints for config deployment.
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from ...config.deployment.service import DeploymentService
from ...config.deployment.models import DeploymentStatus

router = APIRouter(prefix="/api/deploy", tags=["deployment"])

# Global deployment service (will be initialized by main app)
_deployment_service: Optional[DeploymentService] = None


def set_deployment_service(service: DeploymentService):
    """Set the deployment service instance"""
    global _deployment_service
    _deployment_service = service


def get_deployment_service() -> DeploymentService:
    """Get the deployment service instance"""
    if _deployment_service is None:
        raise HTTPException(status_code=500, detail="Deployment service not initialized")
    return _deployment_service


class DeployConfigRequest(BaseModel):
    """Request model for deploying a config"""
    apply_ddl: bool = False
    if_not_exists: bool = False


class DeployAllRequest(BaseModel):
    """Request model for deploying all pending configs"""
    apply_ddl: bool = False
    ddl_only: bool = False


@router.post("/config/{config_name}")
async def deploy_config(
    config_name: str,
    request: DeployConfigRequest
):
    """
    Deploy a single pipeline configuration.
    
    Args:
        config_name: Name of the pipeline config
        request: Deployment options
        
    Returns:
        Deployment result
    """
    service = get_deployment_service()
    
    result = await service.deploy_config(
        config_name=config_name,
        apply_ddl=request.apply_ddl,
        if_not_exists=request.if_not_exists
    )
    
    if result.status == DeploymentStatus.FAILED:
        raise HTTPException(status_code=400, detail=result.to_dict())
    
    return result.to_dict()


@router.post("/all")
async def deploy_all_pending(request: DeployAllRequest):
    """
    Deploy all pending configurations.
    
    Args:
        request: Deployment options
        
    Returns:
        List of deployment results
    """
    service = get_deployment_service()
    
    results = await service.deploy_all_pending(
        apply_ddl=request.apply_ddl,
        ddl_only=request.ddl_only
    )
    
    return {
        "total": len(results),
        "results": [r.to_dict() for r in results]
    }


@router.get("/status")
async def get_deployment_status():
    """
    Get deployment status of all configs.
    
    Returns:
        Deployment status information
    """
    service = get_deployment_service()
    return service.get_deployment_status()


@router.get("/pending")
async def get_pending_configs():
    """
    Get list of configs pending manual deployment.
    
    Returns:
        List of pending configs
    """
    service = get_deployment_service()
    pending = service.get_pending_configs()
    
    return {
        "total": len(pending),
        "configs": pending
    }

