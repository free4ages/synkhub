"""Simple database connection test to verify setup."""

import pytest
import asyncpg
import asyncio


@pytest.mark.asyncio
async def test_database_connection():
    """Test basic database connection."""
    connection_params = {
        'host': 'localhost',
        'port': 5437,
        'database': 'synctool',
        'user': 'rohitanand',
        'password': 'testpassword'
    }
    
    conn = None
    try:
        conn = await asyncpg.connect(**connection_params)
        
        # Test basic query
        result = await conn.fetchval("SELECT 1")
        assert result == 1
        
        # Test source table exists
        count = await conn.fetchval("SELECT COUNT(*) FROM public.users")
        assert count > 0
        
        # Test destination table exists
        await conn.execute("SELECT COUNT(*) FROM public_copy.user_profile")
        
        print(f"✅ Database connection successful, {count} users found")
        
    finally:
        if conn:
            await conn.close()


@pytest.mark.asyncio
async def test_table_cleanup():
    """Test table cleanup functionality."""
    connection_params = {
        'host': 'localhost',
        'port': 5437,
        'database': 'synctool',
        'user': 'rohitanand',
        'password': 'testpassword'
    }
    
    conn = None
    try:
        conn = await asyncpg.connect(**connection_params)
        
        # Clean destination table
        await conn.execute("TRUNCATE TABLE public_copy.user_profile")
        
        # Verify it's empty
        count = await conn.fetchval("SELECT COUNT(*) FROM public_copy.user_profile")
        assert count == 0
        
        print("✅ Table cleanup successful")
        
    finally:
        if conn:
            await conn.close()
