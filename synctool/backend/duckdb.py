import pandas as pd
from typing import Optional, Dict
from .base_backend import Backend
from ..core.models import BackendConfig, DataStorage
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType

class DuckDBBackend(Backend):
    """DuckDB implementation of Backend with S3 backend support"""
    
    def __init__(self, config: BackendConfig, column_schema=None, logger=None, data_storage: Optional[DataStorage] = None):
        super().__init__(config, column_schema=column_schema, logger=logger, data_storage=data_storage)
        self._connection = None
    
    async def connect(self):
        """Connect to DuckDB database"""
        try:
            import duckdb
            
            if self.backend == "object_storage":
                # Configure S3 settings
                self._connection = duckdb.connect()
                await self._setup_s3_config()
            else:
                self._connection = duckdb.connect(self.provider_config.get('database', ':memory:'))
                
        except ImportError:
            raise ImportError("duckdb is required for DuckDB provider")
    
    async def _setup_s3_config(self):
        """Setup S3 configuration for DuckDB"""
        if self.provider_config:
            self._connection.execute(f"""
                SET s3_region='{self.provider_config.get('region', 'us-east-1')}';
                SET s3_access_key_id='{self.provider_config.get('aws_access_key_id', '')}';
                SET s3_secret_access_key='{self.provider_config.get('aws_secret_access_key', '')}';
            """)
    
    async def disconnect(self):
        """Disconnect from DuckDB database"""
        if self._connection:
            self._connection.close()
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            if self.backend == "object_storage":
                # Modify query to use S3 paths
                s3_path = f"s3://{self.provider_config.get('s3_bucket')}/{self.provider_config.get('s3_prefix')}/{self.table}.parquet"
                query = query.replace(self.table, f"'{s3_path}'")
            
            result = self._connection.execute(query).fetchdf()
            return result
        except Exception as e:
            raise
