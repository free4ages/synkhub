"""
Models for deployment domain.
"""
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any
from enum import Enum


class DeploymentStatus(Enum):
    """Status of deployment"""
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class DeploymentResult:
    """Result of deploying a config"""
    config_name: str
    status: DeploymentStatus
    version: Optional[int] = None
    ddl_applied: bool = False
    ddl_statements: List[str] = field(default_factory=list)
    message: Optional[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = asdict(self)
        result['status'] = self.status.value
        return result

