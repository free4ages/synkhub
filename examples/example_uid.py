from pathlib import Path
import sys
import traceback
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
import asyncio
import logging
from typing import Dict, Any

from synctool.config.config_loader import ConfigLoader
from synctool.sync.sync_job_manager import SyncJobManager
from synctool.core.models import SyncProgress
from synctool.core.enums import SyncStrategy


async def run_example_sync():
    """Example usage of the sync engine"""
    # Configuration matching the new format
    config_dict = {
        'name': 'sync_uid_users_postgres_to_postgres',
        'description': 'Sync user and profile data from Postgres to ClickHouse',
        'max_concurrent_partitions': 1,
        'source_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5432,
                    'dbname': 'synctool',
                    'user': 'rohitanand',
                    'password': '',
                },
                'table': 'users1',
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
                'filters': [
                    # {"column":"u.status", "operator":"=", "value": "active"}
                ]
            }
        },
        'destination_provider': {
            'data_backend': {
                'type': 'postgres',
                'connection': {
                    'host': 'localhost',
                    'port': 5432,
                    'dbname': 'synctool',
                    'user': 'rohitanand',
                    'password': '',
                },
                'table': 'user_profile1',
                'schema': 'public_copy',
                'supports_update': True
            }
        },
        'column_map': [
            {'name': 'user_id', 'src': 'u.id', 'dest': 'user_id', 'unique_column': True, 'order_column': True, 'data_type':'uuid', 'direction': 'asc'},
            {'name':'first_name','src': 'u.first_name', 'dest': 'first_name', 'insert': False, 'data_type':'varchar'},
            {'name':'created_at','src': 'u.created_at', 'dest': 'source_created_at', 'data_type':'datetime'},
            {'name':'updated_at','src': 'u.updated_at', 'dest': 'source_updated_at', 'data_type':'datetime'},
            {'name':'email','src': 'p.email', 'dest': 'email', 'data_type':'varchar'},
            {'name':'bio','src': 'p.bio', 'dest': 'bio', 'data_type':'text'},
            {'name':'last_name','src': 'u.last_name', 'dest': 'last_name', 'insert': False},
            {'name':'status','src': 'u.status', 'dest': 'status'},
            {'name':'name','src': None, 'dest': 'name'},
            {'name':'checksum','src': None, 'dest': 'checksum', 'data_type':'varchar', 'hash_key':True}
        ],
        'partition_column': 'user_id',
        'partition_step': 10,
        'strategies': [
            {
                'name': 'delta',
                'type': 'delta',
                'column': 'updated_at',
                'page_size': 10,
            },
            {
                'name': 'hash',
                'type': 'hash',
                'enabled': True,
                'column': 'user_id',
                'page_size': 10
            },
            {
                'name': 'full', 
                'type': 'full',
                'enabled': True,
                'column': 'user_id',
                'page_size': 10
            }
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
        }
        
    }
    
    # Load configuration
    config = ConfigLoader.load_from_dict(config_dict)
    
    # Validate configuration
    issues = ConfigLoader.validate_config(config)
    if issues:
        print(f"Configuration issues: {issues}")
        return
    
    # Create job manager
    job_manager = SyncJobManager(max_concurrent_jobs=2)
    
    # Progress callback
    def progress_callback(job_name: str, progress: SyncProgress):
        print(f"Job {job_name}: {progress.completed_partitions}/{progress.total_partitions} partitions completed")
    
    # Run sync job
    try:
        result = await job_manager.run_sync_job(
            config=config,
            strategy_name="hash",
            progress_callback=progress_callback
        )
        print(f"Sync completed: {result}")
    except Exception as e:
        traceback.print_exc()
        print(f"Sync failed: {str(e)}")


"""
-- Create source schema
CREATE SCHEMA IF NOT EXISTS public;

-- Users table with UUID and some nullable columns
CREATE TABLE IF NOT EXISTS public.users1 (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status VARCHAR(50)
);

-- User profiles table with UUID FK
CREATE TABLE IF NOT EXISTS public.user_profiles1 (
    user_id UUID NOT NULL REFERENCES public.users1(id),
    email VARCHAR(50),
    bio TEXT NULL -- nullable column
);

-- Create destination schema and table in `public_copy`
CREATE SCHEMA IF NOT EXISTS public_copy;

CREATE TABLE IF NOT EXISTS public_copy.user_profile1 (
    user_id UUID,
    email VARCHAR(50),
    source_created_at TIMESTAMP,
    source_updated_at TIMESTAMP,
    status VARCHAR(50),
    checksum VARCHAR(50),
    name VARCHAR(50),  -- result of enrichment: first_name + last_name
    bio TEXT NULL
);

-- Insert into public.users with random nulls
INSERT INTO public.users1 (first_name, last_name, created_at, updated_at, status)
SELECT
    'FirstName_' || i,
    'LastName_' || i,
    NOW() - (i || ' days')::INTERVAL,
    NOW() - (i % 5 || ' hours')::INTERVAL,
    CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END
FROM generate_series(1, 1000) AS s(i);

-- Insert into public.user_profiles with matching UUIDs and random nulls
INSERT INTO public.user_profiles1 (user_id, email, bio)
SELECT
    u.id,
    'user' || u.id || '@example.com',
    CASE WHEN random() < 0.4 THEN NULL ELSE 'Bio for user ' || u.id END
FROM public.users1 u;
"""


logging.basicConfig(level=logging.DEBUG)

def main():
    """Main function to parse arguments and run the application."""
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    
    # Run the sync job
    success = asyncio.run(run_example_sync())
    


if __name__ == "__main__":
    main()