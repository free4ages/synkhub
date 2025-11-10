"""
Models for build system domain.
"""
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum
from pathlib import Path


class BuildStatus(Enum):
    """Status of built config"""
    SUCCESS = "success"
    REQUIRES_DEPLOY = "requires_deploy"
    FAILED_VALIDATION = "failed_validation"
    FAILED_BUILD = "failed_build"


class ChangeType(Enum):
    """Type of config change"""
    NEW = "new"
    MODIFIED = "modified"
    DATASTORE_CHANGED = "datastore_changed"
    DISABLED = "disabled"
    ORPHANED = "orphaned"
    UNCHANGED = "unchanged"


@dataclass
class ConfigMetadata:
    """Metadata for built config"""
    name: str
    version: int
    source_path: str
    config_hash: str
    datastore_refs: List[str]
    datastore_hashes: Dict[str, str]
    built_at: str
    build_status: str  # BuildStatus value
    requires_manual_deploy: bool
    manual_deploy_reason: Optional[str] = None
    pending_changes_hash: Optional[str] = None
    enabled: bool = True
    strategies_enabled: List[str] = field(default_factory=list)
    has_populate_stage: bool = False
    last_ddl_check: Optional[str] = None
    ddl_applied: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConfigMetadata':
        """Create from dictionary"""
        return cls(**data)


@dataclass
class ConfigChange:
    """Represents a config change"""
    name: str
    change_type: ChangeType
    source_path: Optional[Path] = None
    reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'change_type': self.change_type.value,
            'source_path': str(self.source_path) if self.source_path else None,
            'reason': self.reason
        }


@dataclass
class ConfigChanges:
    """Collection of config changes"""
    new: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)
    datastore_changed: List[str] = field(default_factory=list)
    disabled: List[str] = field(default_factory=list)
    orphaned: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    
    def get_configs_to_build(self) -> List[str]:
        """Get list of configs that need building"""
        return self.new + self.modified + self.datastore_changed
    
    def get_configs_to_remove(self) -> List[str]:
        """Get list of configs that need removal"""
        return self.disabled + self.orphaned
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class ValidationResult:
    """Result of config validation"""
    valid: bool
    config_name: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    datastore_refs: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class DDLCheckResult:
    """Result of DDL check"""
    config_name: str
    requires_ddl: bool
    table_exists: bool
    ddl_statements: List[str] = field(default_factory=list)
    changes: List[Dict[str, Any]] = field(default_factory=list)
    connection_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class BuildResult:
    """Result of building a single config"""
    config_name: str
    success: bool
    version: Optional[int] = None
    error: Optional[str] = None
    warning: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class BuildReport:
    """Comprehensive build report"""
    successful: List[str] = field(default_factory=list)
    requires_manual_deploy: List[Dict[str, str]] = field(default_factory=list)
    failed_validation: List[Dict[str, str]] = field(default_factory=list)
    failed_build: List[Dict[str, str]] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    
    def total_configs(self) -> int:
        """Total number of configs processed"""
        return (
            len(self.successful) + 
            len(self.requires_manual_deploy) + 
            len(self.failed_validation) +
            len(self.failed_build) +
            len(self.removed) +
            len(self.unchanged)
        )
    
    def has_issues(self) -> bool:
        """Check if there are any issues"""
        return bool(
            self.requires_manual_deploy or 
            self.failed_validation or 
            self.failed_build
        )
    
    def print_summary(self):
        """Print human-readable summary"""
        print(f"\n{'='*80}")
        print(f"BUILD REPORT")
        print(f"{'='*80}")
        print(f"✅ Successfully built: {len(self.successful)} configs")
        
        if self.successful:
            for name in self.successful:
                print(f"   - {name}")
        
        if self.requires_manual_deploy:
            print(f"\n⚠️  Requires manual deploy: {len(self.requires_manual_deploy)} configs")
            for item in self.requires_manual_deploy:
                print(f"   - {item['name']}: {item['reason']}")
            print(f"\n   Run: synctool deploy --config <name>")
            print(f"   Or:  synctool deploy --all")
        
        if self.failed_validation:
            print(f"\n❌ Failed validation: {len(self.failed_validation)} configs")
            for item in self.failed_validation:
                print(f"   - {item['name']}: {item['error']}")
        
        if self.failed_build:
            print(f"\n❌ Failed build: {len(self.failed_build)} configs")
            for item in self.failed_build:
                print(f"   - {item['name']}: {item['error']}")
        
        if self.removed:
            print(f"\n🗑️  Removed: {len(self.removed)} configs (disabled/orphaned)")
            for name in self.removed:
                print(f"   - {name}")
        
        print(f"\n⏭️  Unchanged: {len(self.unchanged)} configs")
        print(f"{'='*80}\n")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

