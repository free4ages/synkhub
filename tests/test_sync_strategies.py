"""Integration tests for SyncTool sync strategies."""

import pytest
import asyncpg
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta

from synctool.config.config_loader import ConfigLoader
from synctool.sync.sync_job_manager import SyncJobManager
from synctool.core.models import SyncProgress

logger = logging.getLogger(__name__)



class TestSyncStrategies:
    """Test class for sync strategy integration tests."""

    async def _get_destination_count(self, db_connection) -> int:
        """Get count of records in destination table."""
        result = await db_connection.fetchval("SELECT COUNT(*) FROM public_copy.user_profile")
        return result or 0

    async def _get_destination_records(self, db_connection, limit: Optional[int] = None) -> List[Dict]:
        """Get records from destination table."""
        query = "SELECT * FROM public_copy.user_profile ORDER BY user_id"
        if limit:
            query += f" LIMIT {limit}"
        
        records = await db_connection.fetch(query)
        return [dict(record) for record in records]

    async def _get_source_count(self, db_connection) -> int:
        """Get count of records in source table."""
        result = await db_connection.fetchval("""
            SELECT COUNT(*) 
            FROM public.users u 
            LEFT JOIN public.user_profiles p ON u.id = p.user_id
        """)
        return result or 0

    async def _verify_enrichment(self, db_connection) -> bool:
        """Verify that enrichment (name field) is working correctly."""
        records = await db_connection.fetch("""
            SELECT count(*)
            FROM public_copy.user_profile up
            LEFT JOIN public.users u ON up.user_id = u.id
            WHERE up.name <> CONCAT(u.first_name, ' ', u.last_name)
        """)
        return records[0]['count'] == 0

    async def _verify_hash_values(self, db_connection) -> bool:
        """Verify that hash values (checksum) are generated."""
        count = await db_connection.fetchval("""
            SELECT COUNT(*) 
            FROM public_copy.user_profile 
            WHERE checksum IS NOT NULL AND checksum != ''
        """)
        
        total_count = await self._get_source_count(db_connection)
        return count == total_count

    def _create_config_with_strategy(self, base_config: Dict[str, Any], strategy_config: list[Dict[str, Any]] | Dict[str, Any]) -> Any:
        """Create a sync configuration with a specific strategy."""
        config = base_config.copy()
        config['strategies'] = [strategy_config] if isinstance(strategy_config, dict) else strategy_config
        return ConfigLoader.load_from_dict(config)

    @pytest.mark.asyncio
    async def test_full_sync_strategy(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test full sync strategy."""
        logger.info("Testing full sync strategy")
        
        # Configure full sync strategy
        strategy_config = {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 50,
            'sub_partition_step': 10
        }
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        # Validate configuration
        issues = ConfigLoader.validate_config(config)
        assert not issues, f"Configuration validation failed: {issues}"
        
        # Track progress
        progress_updates = []
        def progress_callback(job_name: str, progress: SyncProgress):
            progress_updates.append({
                'job_name': job_name,
                'completed': progress.completed_partitions,
                'total': progress.total_partitions
            })
            logger.info(f"Progress: {progress.completed_partitions}/{progress.total_partitions} partitions")
        
        # Run full sync
        result = await sync_job_manager.run_sync_job(
            config=config,
            strategy_name="full",
            progress_callback=progress_callback
        )
        
        # Verify results
        assert result is not None, "Sync job should return a result"
        
        # Check destination table has correct number of records
        source_count = await self._get_source_count(db_connection)
        dest_count = await self._get_destination_count(db_connection)
        
        assert dest_count > 0, "Destination table should have records after sync"
        assert dest_count == source_count, f"Expected {source_count} records, got {dest_count}"
        
        # Verify enrichment worked
        enrichment_ok = await self._verify_enrichment(db_connection)
        assert enrichment_ok, "Enrichment should work correctly"
        
        # Verify hash values are generated
        hash_ok = await self._verify_hash_values(db_connection)
        assert hash_ok, "Hash values should be generated for all records"
        
        # Check that we received progress updates (optional - may not be implemented)
        logger.info(f"Progress updates received: {len(progress_updates)}")
        
        logger.info(f"Full sync completed successfully: {dest_count} records synced")

    @pytest.mark.asyncio
    async def test_hash_sync_strategy(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test hash sync strategy."""
        logger.info("Testing hash sync strategy")
        
        # Configure hash sync strategy
        strategy_config = [{
            'name': 'hash',
            'type': 'hash',
            'column': 'user_id',
            'partition_step': 50,
            'sub_partition_step': 10
        },
        {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 50,
            'sub_partition_step': 10
        }]
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        # Validate configuration
        issues = ConfigLoader.validate_config(config)
        assert not issues, f"Configuration validation failed: {issues}"
        
        # Run initial hash sync
        result = await sync_job_manager.run_sync_job(
            config=config,
            strategy_name="hash"
        )
        
        assert result is not None, "Initial sync should succeed"
        
        initial_count = await self._get_destination_count(db_connection)
        assert initial_count > 0, "Initial sync should populate destination table"
        
        # Run hash sync again (should detect no changes)
        result2 = await sync_job_manager.run_sync_job(
            config=config,
            strategy_name="hash"
        )
        
        assert result2 is not None, "Second sync should succeed"
        
        final_count = await self._get_destination_count(db_connection)
        assert final_count == initial_count, "Hash sync should not duplicate records"
        
        # Verify hash values are consistent
        hash_ok = await self._verify_hash_values(db_connection)
        assert hash_ok, "Hash values should be consistent"
        
        logger.info(f"Hash sync completed successfully: {final_count} records")

    @pytest.mark.asyncio
    async def test_delta_sync_strategy(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test delta sync strategy."""
        logger.info("Testing delta sync strategy")
        
        # Configure delta sync strategy
        strategy_config =[{
            'name': 'delta',
            'type': 'delta',
            'column': 'updated_at',
            'partition_step': 2*60*60,  # 2 hours in seconds
            'sub_partition_step': 30*60  # 30 minutes in seconds
        },
        {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 50,
            'sub_partition_step': 10
        }]
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        # Validate configuration
        issues = ConfigLoader.validate_config(config)
        assert not issues, f"Configuration validation failed: {issues}"
        
        # Run initial delta sync
        result = await sync_job_manager.run_sync_job(
            config=config,
            strategy_name="delta"
        )
        
        assert result is not None, "Initial delta sync should succeed"
        
        initial_count = await self._get_destination_count(db_connection)
        assert initial_count > 0, "Initial delta sync should populate destination table"
        
        # Update some records in source to test incremental sync
        await db_connection.execute("""
            UPDATE public.users 
            SET updated_at = NOW(), first_name = first_name || '_updated' 
            WHERE id IN (1, 2, 3, 4, 5)
        """)
        
        # Run delta sync again to catch updates
        result2 = await sync_job_manager.run_sync_job(
            config=config,
            strategy_name="delta"
        )
        
        assert result2 is not None, "Second delta sync should succeed"
        
        # Check that updated records are reflected in destination
        updated_records = await db_connection.fetch("""
            SELECT * FROM public_copy.user_profile 
            WHERE user_id IN (1, 2, 3, 4, 5) 
            ORDER BY user_id
        """)
        
        assert len(updated_records) == 5, "Should have 5 updated records"
        
        # Verify the updates were applied
        for record in updated_records:
            assert '_updated' in record['name'], f"Record {record['user_id']} should be updated"
        
        logger.info(f"Delta sync completed successfully: {initial_count} records, 5 updated")

    @pytest.mark.asyncio
    async def test_sync_strategy_data_integrity(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test data integrity across sync strategies."""
        logger.info("Testing data integrity across sync strategies")
        
        # Test with full sync first
        full_strategy = {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 25,
            'sub_partition_step': 10
        }
        
        config = self._create_config_with_strategy(base_sync_config, full_strategy)
        
        await sync_job_manager.run_sync_job(config=config, strategy_name="full")
        
        # Get sample records to verify data integrity
        sample_records = await self._get_destination_records(db_connection, limit=10)
        
        # Verify all required fields are populated
        for record in sample_records:
            assert record['user_id'] is not None, "user_id should not be null"
            assert record['email'] is not None, "email should not be null"
            assert record['name'] is not None, "enriched name should not be null"
            assert record['checksum'] is not None, "checksum should not be null"
            assert record['source_created_at'] is not None, "source_created_at should not be null"
            assert record['source_updated_at'] is not None, "source_updated_at should not be null"
            assert record['status'] in ['active', 'inactive'], "status should be valid"
            
            # Verify email format
            assert '@example.com' in record['email'], "email should have correct format"
            
            # # Verify enriched name matches first_name + last_name
            # expected_name = f"{record['first_name']} {record['last_name']}"
            # assert record['name'] == expected_name, f"Enriched name mismatch: expected '{expected_name}', got '{record['name']}'"
        
        logger.info("Data integrity verification passed")

    # @pytest.mark.asyncio
    # async def test_concurrent_sync_safety(
    #     self, 
    #     db_connection, 
    #     clean_destination_table,
    #     base_sync_config: Dict[str, Any]
    # ):
    #     """Test that concurrent syncs are handled safely."""
    #     logger.info("Testing concurrent sync safety")
        
    #     # Create multiple job managers to simulate concurrent access
    #     job_manager1 = SyncJobManager(max_concurrent_jobs=1)
    #     job_manager2 = SyncJobManager(max_concurrent_jobs=1)
        
    #     strategy_config = {
    #         'name': 'full',
    #         'type': 'full',
    #         'enabled': True,
    #         'column': 'user_id',
    #         'partition_step': 30,
    #         'sub_partition_step': 10
    #     }
        
    #     config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
    #     # Run two sync jobs concurrently
    #     import asyncio
        
    #     results = await asyncio.gather(
    #         job_manager1.run_sync_job(config=config, strategy_name="full"),
    #         job_manager2.run_sync_job(config=config, strategy_name="full"),
    #         return_exceptions=True
    #     )
        
    #     # At least one should succeed
    #     successful_results = [r for r in results if not isinstance(r, Exception)]
    #     assert len(successful_results) >= 1, "At least one sync should succeed"
        
    #     # Verify data integrity after concurrent syncs
    #     final_count = await self._get_destination_count(db_connection)
    #     source_count = await self._get_source_count(db_connection)
        
    #     # Should not have duplicate records
    #     assert final_count == source_count, f"Expected {source_count} records, got {final_count}"
        
    #     logger.info(f"Concurrent sync safety test passed: {final_count} records")
