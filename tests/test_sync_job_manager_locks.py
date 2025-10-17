"""
Test cases for SyncJobManager lock acquisition and management.
Tests the unified lock logic that works for both CLI and scheduler execution.
"""

import pytest
import asyncio
from contextlib import asynccontextmanager
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from synctool.sync.sync_job_manager import SyncJobManager
from synctool.core.models import PipelineJobConfig, StageConfig, StrategyConfig, StrategyType, DestinationConfig
from synctool.scheduler.execution_lock_manager import ExecutionLockManager


@pytest.fixture
def mock_execution_lock_manager():
    """Create a mock execution lock manager"""
    manager = Mock(spec=ExecutionLockManager)
    manager.pipeline_lock_timeout = 3600
    return manager


@pytest.fixture
def mock_metrics_storage():
    """Create a mock metrics storage"""
    storage = Mock()
    return storage


@pytest.fixture
def mock_logs_storage():
    """Create a mock logs storage"""
    storage = Mock()
    return storage


@pytest.fixture
def sample_config():
    """Create a sample pipeline config"""
    strategy = StrategyConfig(
        name="test_strategy",
        type=StrategyType.FULL,
        enabled=True
    )
    stage = StageConfig(
        name="test_stage",
        type="populate",
        strategies=[strategy],
        destination=DestinationConfig(
            datastore="test_datastore",
            schema="public",
            table="test_table"
        )
    )
    config = PipelineJobConfig(
        name="test_pipeline",
        description="Test",
        stages=[stage]
    )
    return config


class TestPipelineLockAcquisition:
    """Test pipeline lock acquisition logic"""
    
    @pytest.mark.asyncio
    async def test_no_lock_manager_runs_without_locking(
        self, sample_config, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job runs without locking when no lock manager provided"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=None  # No lock manager
        )
        
        lock_acquired_called = False
        
        def on_locks_acquired():
            nonlocal lock_acquired_called
            lock_acquired_called = True
        
        # Mock _execute_job to avoid actual execution
        with patch.object(job_manager, '_execute_job', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"status": "success"}
            
            # Execute
            result = await job_manager.run_sync_job(
                config=sample_config,
                strategy_name="test_strategy",
                on_locks_acquired=on_locks_acquired
            )
        
        # Assert
        assert result["status"] == "success"
        assert lock_acquired_called  # Callback should still be called
        mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_pipeline_lock_acquired_successfully(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test successful pipeline lock acquisition"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition - return True
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True  # Lock acquired
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        mock_execution_lock_manager.is_table_locked = Mock(return_value=False)
        
        lock_acquired_called = False
        
        def on_locks_acquired():
            nonlocal lock_acquired_called
            lock_acquired_called = True
        
        # Mock _execute_job
        with patch.object(job_manager, '_execute_job', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"status": "success", "rows": 100}
            
            # Execute
            result = await job_manager.run_sync_job(
                config=sample_config,
                strategy_name="test_strategy",
                wait_for_pipeline_lock=False,
                on_locks_acquired=on_locks_acquired
            )
        
        # Assert
        assert result["status"] == "success"
        assert lock_acquired_called
        mock_execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_pipeline_lock_unavailable_skips_job(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job is skipped when pipeline lock unavailable"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition - return False
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield False  # Lock NOT acquired
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        
        skip_called = False
        
        def on_skip():
            nonlocal skip_called
            skip_called = True
        
        # Execute
        result = await job_manager.run_sync_job(
            config=sample_config,
            strategy_name="test_strategy",
            wait_for_pipeline_lock=False,
            on_skip=on_skip
        )
        
        # Assert
        assert result["status"] == "skipped"
        assert result["reason"] == "pipeline_lock_unavailable"
        assert "Pipeline lock not available" in result["message"]
        assert skip_called


class TestTableDDLLockCheck:
    """Test table DDL lock checking logic"""
    
    @pytest.mark.asyncio
    async def test_table_not_locked_proceeds(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job proceeds when table is not locked"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True  # Pipeline lock acquired
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        mock_execution_lock_manager.is_table_locked = Mock(return_value=False)  # Table NOT locked
        
        # Mock _execute_job
        with patch.object(job_manager, '_execute_job', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"status": "success"}
            
            # Execute
            result = await job_manager.run_sync_job(
                config=sample_config,
                strategy_name="test_strategy",
                wait_for_table_lock=False
            )
        
        # Assert
        assert result["status"] == "success"
        mock_execution_lock_manager.is_table_locked.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_table_locked_no_wait_skips(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job skips immediately when table is locked and not waiting"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True  # Pipeline lock acquired
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        mock_execution_lock_manager.is_table_locked = Mock(return_value=True)  # Table IS locked
        
        skip_called = False
        
        def on_skip():
            nonlocal skip_called
            skip_called = True
        
        # Execute
        result = await job_manager.run_sync_job(
            config=sample_config,
            strategy_name="test_strategy",
            wait_for_table_lock=False,  # Don't wait
            on_skip=on_skip
        )
        
        # Assert
        assert result["status"] == "skipped"
        assert result["reason"] == "table_ddl_lock_active"
        assert skip_called
    
    @pytest.mark.asyncio
    async def test_table_locked_wait_then_proceeds(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job waits for table lock and then proceeds"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        
        # Table locked initially, then becomes available
        call_count = 0
        def mock_is_table_locked(*args):
            nonlocal call_count
            call_count += 1
            return call_count == 1  # Locked first time, then available
        
        mock_execution_lock_manager.is_table_locked = mock_is_table_locked
        
        # Mock _execute_job
        with patch.object(job_manager, '_execute_job', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"status": "success"}
            
            # Execute
            result = await job_manager.run_sync_job(
                config=sample_config,
                strategy_name="test_strategy",
                wait_for_table_lock=True,
                table_lock_wait_timeout=5  # Short timeout for test
            )
        
        # Assert
        assert result["status"] == "success"
        assert call_count >= 2  # Checked at least twice
    
    @pytest.mark.asyncio
    async def test_table_locked_wait_timeout_skips(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that job skips after table lock wait timeout"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        mock_execution_lock_manager.is_table_locked = Mock(return_value=True)  # Always locked
        
        skip_called = False
        
        def on_skip():
            nonlocal skip_called
            skip_called = True
        
        # Execute
        result = await job_manager.run_sync_job(
            config=sample_config,
            strategy_name="test_strategy",
            wait_for_table_lock=True,
            table_lock_wait_timeout=2,  # 2 second timeout
            on_skip=on_skip
        )
        
        # Assert
        assert result["status"] == "skipped"
        assert result["reason"] == "table_ddl_lock_timeout"
        assert "timeout" in result["message"].lower()
        assert skip_called


class TestCallbacks:
    """Test callback functionality"""
    
    @pytest.mark.asyncio
    async def test_on_locks_acquired_called_after_lock_success(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that on_locks_acquired is called after successful lock acquisition"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield True
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        mock_execution_lock_manager.is_table_locked = Mock(return_value=False)
        
        callback_order = []
        
        def on_locks_acquired():
            callback_order.append("locks_acquired")
        
        # Mock _execute_job
        with patch.object(job_manager, '_execute_job', new_callable=AsyncMock) as mock_execute:
            def execute_side_effect(*args, **kwargs):
                callback_order.append("execute")
                return {"status": "success"}
            
            mock_execute.side_effect = execute_side_effect
            
            # Execute
            result = await job_manager.run_sync_job(
                config=sample_config,
                strategy_name="test_strategy",
                on_locks_acquired=on_locks_acquired
            )
        
        # Assert
        assert result["status"] == "success"
        assert callback_order == ["locks_acquired", "execute"]
    
    @pytest.mark.asyncio
    async def test_on_skip_called_when_lock_unavailable(
        self, sample_config, mock_execution_lock_manager, mock_metrics_storage, mock_logs_storage
    ):
        """Test that on_skip is called when lock is unavailable"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage,
            execution_lock_manager=mock_execution_lock_manager
        )
        
        # Mock lock acquisition - fail
        @asynccontextmanager
        async def mock_acquire_lock(*args, **kwargs):
            yield False
        
        mock_execution_lock_manager.acquire_pipeline_lock = mock_acquire_lock
        
        skip_called = False
        
        def on_skip():
            nonlocal skip_called
            skip_called = True
        
        # Execute
        result = await job_manager.run_sync_job(
            config=sample_config,
            strategy_name="test_strategy",
            on_skip=on_skip
        )
        
        # Assert
        assert result["status"] == "skipped"
        assert skip_called


class TestStrategyConfigExtraction:
    """Test helper methods for extracting strategy configuration"""
    
    def test_get_strategy_config_found(self, sample_config, mock_metrics_storage, mock_logs_storage):
        """Test that strategy config is found"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage
        )
        
        # Execute
        strategy_config = job_manager._get_strategy_config(sample_config, "test_strategy")
        
        # Assert
        assert strategy_config is not None
        assert strategy_config.name == "test_strategy"
    
    def test_get_strategy_config_not_found(self, sample_config, mock_metrics_storage, mock_logs_storage):
        """Test that None is returned when strategy not found"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage
        )
        
        # Execute
        strategy_config = job_manager._get_strategy_config(sample_config, "nonexistent")
        
        # Assert
        assert strategy_config is None
    
    def test_get_destination_table_info(self, sample_config, mock_metrics_storage, mock_logs_storage):
        """Test extraction of destination table information"""
        # Setup
        job_manager = SyncJobManager(
            max_concurrent_jobs=1,
            metrics_storage=mock_metrics_storage,
            logs_storage=mock_logs_storage
        )
        
        # Execute
        dest_info = job_manager._get_destination_table_info(sample_config)
        
        # Assert
        assert dest_info is not None
        assert dest_info['datastore'] == 'test_datastore'
        assert dest_info['schema'] == 'public'
        assert dest_info['table'] == 'test_table'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

