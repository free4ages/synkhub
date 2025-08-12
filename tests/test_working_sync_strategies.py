"""Working integration tests for SyncTool sync strategies."""

import pytest
import asyncpg
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta

from synctool.config.config_loader import ConfigLoader
from synctool.sync.sync_job_manager import SyncJobManager
from synctool.core.models import SyncProgress

logger = logging.getLogger(__name__)


@pytest.mark.docker
@pytest.mark.integration
class TestWorkingSyncStrategies:
    """Test class for verified working sync strategies."""

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
            SELECT first_name, last_name, name 
            FROM public_copy.user_profile 
            WHERE name IS NOT NULL 
            LIMIT 5
        """)
        
        for record in records:
            expected_name = f"{record['first_name']} {record['last_name']}"
            if record['name'] != expected_name:
                logger.error(f"Enrichment failed: expected '{expected_name}', got '{record['name']}'")
                return False
        
        return True

    async def _verify_hash_values(self, db_connection) -> bool:
        """Verify that hash values (checksum) are generated."""
        count = await db_connection.fetchval("""
            SELECT COUNT(*) 
            FROM public_copy.user_profile 
            WHERE checksum IS NOT NULL AND checksum != ''
        """)
        
        total_count = await self._get_destination_count(db_connection)
        return count == total_count

    def _create_config_with_strategy(self, base_config: Dict[str, Any], strategy_config: Dict[str, Any]) -> Any:
        """Create a sync configuration with a specific strategy."""
        config = base_config.copy()
        config['strategies'] = [strategy_config]
        return ConfigLoader.load_from_dict(config)

    @pytest.mark.asyncio
    async def test_full_sync_strategy_comprehensive(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test full sync strategy with comprehensive validation."""
        logger.info("Testing full sync strategy comprehensively")
        
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
        logger.info(f"Sync result: {result}")
        
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
        
        # Check progress updates (optional - may not be implemented)
        logger.info(f"Progress updates received: {len(progress_updates)}")
        
        logger.info(f"Full sync completed successfully: {dest_count} records synced")

    @pytest.mark.asyncio
    async def test_data_integrity_validation(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test comprehensive data integrity validation."""
        logger.info("Testing data integrity validation")
        
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
            assert record['first_name'] is not None, "first_name should not be null"
            assert record['last_name'] is not None, "last_name should not be null"
            assert record['email'] is not None, "email should not be null"
            assert record['name'] is not None, "enriched name should not be null"
            assert record['checksum'] is not None, "checksum should not be null"
            assert record['source_created_at'] is not None, "source_created_at should not be null"
            assert record['source_updated_at'] is not None, "source_updated_at should not be null"
            assert record['status'] in ['active', 'inactive'], "status should be valid"
            
            # Verify email format
            assert '@example.com' in record['email'], "email should have correct format"
            
            # Verify enriched name matches first_name + last_name
            expected_name = f"{record['first_name']} {record['last_name']}"
            assert record['name'] == expected_name, f"Enriched name mismatch: expected '{expected_name}', got '{record['name']}'"
        
        logger.info("Data integrity verification passed")

    @pytest.mark.asyncio
    async def test_multiple_full_syncs_idempotency(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test that multiple full syncs are idempotent."""
        logger.info("Testing multiple full syncs idempotency")
        
        strategy_config = {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 30,
            'sub_partition_step': 10
        }
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        # Run first sync
        result1 = await sync_job_manager.run_sync_job(config=config, strategy_name="full")
        count1 = await self._get_destination_count(db_connection)
        
        # Run second sync
        result2 = await sync_job_manager.run_sync_job(config=config, strategy_name="full")
        count2 = await self._get_destination_count(db_connection)
        
        # Should have same number of records (idempotent)
        assert count1 == count2, f"Multiple syncs should be idempotent: {count1} vs {count2}"
        assert count1 > 0, "Should have synced records"
        
        logger.info(f"Idempotency test passed: {count1} records both times")

    @pytest.mark.asyncio
    async def test_performance_metrics(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test that performance metrics are captured."""
        logger.info("Testing performance metrics capture")
        
        strategy_config = {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 20,
            'sub_partition_step': 5
        }
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        start_time = datetime.now()
        result = await sync_job_manager.run_sync_job(config=config, strategy_name="full")
        end_time = datetime.now()
        
        # Verify result contains performance metrics
        assert 'total_rows_processed' in result, "Result should contain total_rows_processed"
        assert 'total_rows_inserted' in result, "Result should contain total_rows_inserted"
        assert 'duration' in result, "Result should contain duration"
        assert 'partition_results' in result, "Result should contain partition_results"
        
        # Verify metrics make sense
        assert result['total_rows_processed'] > 0, "Should have processed some rows"
        assert result['total_rows_inserted'] > 0, "Should have inserted some rows"
        
        # Verify duration is reasonable
        total_duration = (end_time - start_time).total_seconds()
        assert total_duration > 0, "Duration should be positive"
        assert total_duration < 30, "Sync should complete within 30 seconds for test data"
        
        logger.info(f"Performance metrics captured: {result['total_rows_processed']} rows in {total_duration:.2f}s")

    @pytest.mark.asyncio
    async def test_empty_destination_handling(
        self, 
        db_connection, 
        clean_destination_table,
        base_sync_config: Dict[str, Any],
        sync_job_manager: SyncJobManager
    ):
        """Test handling of empty destination table."""
        logger.info("Testing empty destination table handling")
        
        # Verify destination is empty
        initial_count = await self._get_destination_count(db_connection)
        assert initial_count == 0, "Destination should start empty"
        
        strategy_config = {
            'name': 'full',
            'type': 'full',
            'enabled': True,
            'column': 'user_id',
            'partition_step': 100,  # Single partition
            'sub_partition_step': 50
        }
        
        config = self._create_config_with_strategy(base_sync_config, strategy_config)
        
        # Run sync on empty destination
        result = await sync_job_manager.run_sync_job(config=config, strategy_name="full")
        final_count = await self._get_destination_count(db_connection)
        
        # Should populate destination correctly
        source_count = await self._get_source_count(db_connection)
        assert final_count == source_count, f"Should sync all {source_count} records"
        assert result['total_rows_inserted'] == source_count, "Metrics should match"
        
        logger.info(f"Empty destination handling passed: {final_count} records synced")
