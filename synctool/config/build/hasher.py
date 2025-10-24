"""
Canonical hash computation for configs.
Hashes parsed config objects to avoid formatting-based changes.
"""
import hashlib
import json
from typing import Dict, Any
import logging

from ...core.models import PipelineJobConfig, DataStore
from ..config_serializer import ConfigSerializer


class ConfigHasher:
    """Computes canonical hashes for configs"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def compute_config_hash(self, config: PipelineJobConfig) -> str:
        """
        Compute hash from parsed config object (not raw file).
        Uses canonical JSON serialization to ignore formatting/comments.
        
        Args:
            config: Parsed pipeline config
            
        Returns:
            SHA256 hash hex string
        """
        try:
            # Convert to dict using existing serializer
            config_dict = ConfigSerializer.config_to_dict(config)
            
            # Create canonical JSON (sorted keys, no whitespace)
            canonical_json = json.dumps(
                config_dict,
                sort_keys=True,
                separators=(',', ':'),
                default=str  # Handle any non-serializable objects
            )
            
            # Compute hash
            hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
            return hash_obj.hexdigest()
            
        except Exception as e:
            self.logger.error(f"Failed to compute config hash: {e}")
            raise
    
    def compute_datastore_hash(self, datastore: DataStore) -> str:
        """
        Compute hash of datastore connection settings.
        Only includes connection-relevant fields (not passwords).
        
        Args:
            datastore: DataStore object
            
        Returns:
            SHA256 hash hex string
        """
        try:
            # Extract connection-relevant fields
            connection_dict = {
                'type': datastore.type,
                'host': datastore.connection.host,
                'port': datastore.connection.port,
                'database': datastore.connection.database,
                'user': datastore.connection.user,
                # Note: Intentionally exclude password
                # Password changes should not trigger rebuild
            }
            
            # Add optional fields if present
            if hasattr(datastore.connection, 'schema'):
                connection_dict['schema'] = datastore.connection.schema
            
            # Create canonical JSON
            canonical_json = json.dumps(
                connection_dict,
                sort_keys=True,
                separators=(',', ':')
            )
            
            # Compute hash
            hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
            return hash_obj.hexdigest()
            
        except Exception as e:
            self.logger.error(f"Failed to compute datastore hash: {e}")
            raise
    
    def compute_file_content_hash(self, file_path: str) -> str:
        """
        Compute hash of raw file content.
        Useful for quick change detection before parsing.
        
        Args:
            file_path: Path to file
            
        Returns:
            SHA256 hash hex string
        """
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            
            hash_obj = hashlib.sha256(content)
            return hash_obj.hexdigest()
            
        except Exception as e:
            self.logger.error(f"Failed to compute file hash for {file_path}: {e}")
            raise

