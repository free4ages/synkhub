from typing import Dict, List, Optional, Any
import pandas as pd
import logging
import boto3
from io import StringIO

from ..core import BackendType, ConnectionConfig
from .base import Provider


class S3Provider(Provider):
    """S3 implementation of Provider"""
    
    def __init__(self, connection: ConnectionConfig, table: str, **kwargs):
        super().__init__(BackendType.OBJECT_STORAGE, connection, table, **kwargs)
        self.s3_client = None
    
    async def connect(self):
        """Connect to S3"""
        try:
            # Create S3 client
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.connection.aws_access_key_id,
                aws_secret_access_key=self.connection.aws_secret_access_key,
                region_name=self.connection.region
            )
        except Exception as e:
            raise Exception(f"Failed to connect to S3: {str(e)}")
    
    async def disconnect(self):
        """Disconnect from S3 (no-op for S3)"""
        pass
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        # For S3, we interpret the "query" as an object key pattern
        # This is a simplified implementation
        try:
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.connection.s3_bucket,
                Key=query  # In this case, query is the object key
            )
            
            # Read CSV content
            content = response['Body'].read().decode('utf-8')
            
            # Convert to DataFrame
            df = pd.read_csv(StringIO(content))
            return df
        except Exception as e:
            self.logger.error(f"S3 object retrieval failed: {str(e)}")
            raise
    
    async def insert_data(self, data: pd.DataFrame) -> int:
        """Insert data to S3 as CSV"""
        try:
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            data.to_csv(csv_buffer, index=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.connection.s3_bucket,
                Key=self.table,  # table attribute is used as the object key
                Body=csv_buffer.getvalue()
            )
            
            return len(data)
        except Exception as e:
            self.logger.error(f"S3 object upload failed: {str(e)}")
            raise
