"""Pytest configuration and fixtures for SyncTool tests."""

import asyncio
import os
import sys
from pathlib import Path
from typing import AsyncGenerator, Dict, Any
import pytest
import pytest_asyncio
import asyncpg
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from synctool.config.config_loader import ConfigLoader
from synctool.sync.sync_job_manager import SyncJobManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def db_connection():
    """Create a database connection for tests."""
    connection_params = {
        'host': 'localhost',
        'port': 5437,  # Docker compose port
        'database': 'synctool',
        'user': 'rohitanand',
        'password': 'testpassword'
    }
    
    # Wait for database to be ready
    max_retries = 10
    retry_delay = 1
    
    conn = None
    for attempt in range(max_retries):
        try:
            conn = await asyncpg.connect(**connection_params)
            logger.info("Successfully connected to test database")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Could not connect to test database after {max_retries} attempts: {e}")
            logger.warning(f"Database connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
    
    try:
        yield conn
    finally:
        if conn:
            await conn.close()


@pytest_asyncio.fixture
async def clean_destination_table(db_connection):
    """Clean the destination table before each test."""
    await db_connection.execute("TRUNCATE TABLE public_copy.user_profile")
    yield
    # Cleanup after test
    await db_connection.execute("TRUNCATE TABLE public_copy.user_profile")


@pytest.fixture
def base_sync_config() -> Dict[str, Any]:
    """Base sync configuration for tests."""
    return {
        'name': 'test_sync_postgres_to_postgres',
        'description': 'Test sync configuration',
        'source_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5437,
                    'dbname': 'synctool',
                    'user': 'rohitanand',
                    'password': 'testpassword',
                },
                'table': 'users',
                'alias': 'u',
                'schema': 'public',
                'join': [
                    {
                        'table': 'user_profiles',
                        'alias': 'p',
                        'on': 'u.id = p.user_id',
                        'type': 'left'
                    }
                ],
                'filters': []
            }
        },
        'destination_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5437,
                    'dbname': 'synctool',
                    'user': 'rohitanand',
                    'password': 'testpassword',
                },
                'table': 'user_profile',
                'schema': 'public_copy',
                'supports_update': True,
            }
        },
        'column_map': [
            {'name': 'user_id', 'src': 'u.id', 'dest': 'user_id', 'roles': ['partition_column','unique_column','order_column'], 'data_type':'int', 'order': 'asc'},
            {'name':'first_name','src': 'u.first_name', 'dest': 'first_name', 'insert': False, 'data_type':'varchar'},
            {'name':'last_name','src': 'u.last_name', 'dest': 'last_name', 'insert': False, 'data_type':'varchar'},
            {'name':'created_at','src': 'u.created_at', 'dest': 'source_created_at', 'data_type':'datetime'},
            {'name':'updated_at','src': 'u.updated_at', 'dest': 'source_updated_at', 'data_type':'datetime', 'roles': ['delta_column']},
            {'name':'email','src': 'p.email', 'dest': 'email', 'data_type':'varchar'},
            {'name':'status','src': 'u.status', 'dest': 'status'},
            {'name':'name','src': None, 'dest': 'name'},
            {'name':'checksum','src': None, 'dest': 'checksum', 'data_type':'varchar', 'roles': ['hash_key']}
        ],
        'enrichment': {
            'enabled': True,
            'transformations': [
                {
                    'columns':['first_name', 'last_name'],
                    'transform': 'lambda x: f"{x[\"first_name\"]} {x[\"last_name\"]}"',
                    'dest': 'name'
                }
            ]
        },
        'max_concurrent_partitions': 1,
    }


@pytest.fixture
def sync_job_manager() -> SyncJobManager:
    """Create a sync job manager for tests."""
    return SyncJobManager(max_concurrent_jobs=1)
