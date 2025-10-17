"""
Example: Using the Enhanced Scheduler + Worker System

This example demonstrates:
1. Starting the enhanced scheduler
2. Monitoring strategy states
3. Checking locks
4. Managing retries
"""

import asyncio
from datetime import datetime, timezone
from synctool.config.config_manager import ConfigManager
from synctool.scheduler.enhanced_arq_scheduler import EnhancedARQScheduler
from synctool.scheduler.strategy_state_manager import StrategyStateManager
from synctool.scheduler.execution_lock_manager import ExecutionLockManager
from synctool.config.global_config_loader import load_global_config


async def main():
    """Main example function"""
    
    # Load global configuration
    global_config = load_global_config("./synctool/config/global_config.yaml")
    print(f"Loaded global config: {global_config}")
    
    # Initialize config manager
    config_manager = ConfigManager(
        config_store_type="yaml",
        base_path="./examples/configs"
    )
    await config_manager.initialize_database_stores()
    
    # Example 1: Monitor Strategy States
    print("\n=== Example 1: Monitoring Strategy States ===")
    await monitor_strategy_states(global_config)
    
    # Example 2: Check Execution Locks
    print("\n=== Example 2: Checking Execution Locks ===")
    await check_locks(global_config)
    
    # Example 3: Manage Strategy Retries
    print("\n=== Example 3: Managing Strategy Retries ===")
    await manage_retries(global_config)
    
    # Example 4: Start Scheduler (commented out - runs indefinitely)
    # print("\n=== Example 4: Starting Enhanced Scheduler ===")
    # await start_scheduler(config_manager, global_config)
    
    await config_manager.close()


async def monitor_strategy_states(global_config):
    """Monitor and display strategy states"""
    state_manager = StrategyStateManager(
        state_dir=global_config.storage.state_dir
    )
    
    # List all strategies across all pipelines
    all_strategies = state_manager.list_all_strategies()
    
    print(f"\nFound {len(all_strategies)} pipelines with strategies:")
    
    for pipeline_id, strategies in all_strategies.items():
        print(f"\nPipeline: {pipeline_id}")
        print(f"  Strategies: {len(strategies)}")
        
        for strategy_name, state in strategies.items():
            print(f"\n  Strategy: {strategy_name}")
            print(f"    Status: {state.status}")
            print(f"    Last Scheduled: {state.last_scheduled_at or 'Never'}")
            print(f"    Last Success: {state.last_success_run or 'Never'}")
            print(f"    Last Attempted: {state.last_attempted_at or 'Never'}")
            print(f"    Retry Count: {state.retry_count}")
            print(f"    Run ID: {state.run_id or 'N/A'}")
            print(f"    Worker: {state.worker or 'N/A'}")
            
            if state.error:
                print(f"    Error: {state.error}")
            
            # Get recent history
            history = state_manager.get_strategy_history(
                pipeline_id, strategy_name, limit=5
            )
            
            if history:
                print(f"    Recent History ({len(history)} runs):")
                for run in history[:3]:  # Show last 3
                    duration = "N/A"
                    if run.started_at and run.completed_at:
                        start = datetime.fromisoformat(run.started_at)
                        end = datetime.fromisoformat(run.completed_at)
                        duration = f"{(end - start).total_seconds():.1f}s"
                    
                    print(f"      - {run.status} | {run.started_at} | Duration: {duration}")


async def check_locks(global_config):
    """Check current execution locks"""
    lock_manager = ExecutionLockManager(
        redis_url=global_config.redis.url
    )
    
    # Get all active pipeline locks
    pipeline_locks = lock_manager.get_all_pipeline_locks()
    print(f"\nActive Pipeline Locks: {len(pipeline_locks)}")
    for lock in pipeline_locks:
        print(f"  - {lock}")
    
    # Get all active table locks
    table_locks = lock_manager.get_all_table_locks()
    print(f"\nActive Table DDL Locks: {len(table_locks)}")
    for lock in table_locks:
        print(f"  - {lock}")
    
    # Example: Check if specific pipeline is locked
    pipeline_id = "users_sync_pipeline"
    is_locked = lock_manager.is_pipeline_locked(pipeline_id)
    print(f"\nIs '{pipeline_id}' locked? {is_locked}")
    
    # Example: Check if specific table is locked
    is_table_locked = lock_manager.is_table_locked(
        "postgres_target", "public", "users_synced"
    )
    print(f"Is 'postgres_target.public.users_synced' locked? {is_table_locked}")


async def manage_retries(global_config):
    """Demonstrate retry management"""
    state_manager = StrategyStateManager(
        state_dir=global_config.storage.state_dir
    )
    
    pipeline_id = "users_sync_pipeline"
    strategy_name = "hourly_hash_sync"
    
    # Get current state
    state = state_manager.get_strategy_state(pipeline_id, strategy_name)
    
    if state:
        print(f"\nCurrent state for {pipeline_id}:{strategy_name}")
        print(f"  Retry Count: {state.retry_count}")
        print(f"  Status: {state.status}")
        
        # Example: Reset retry count (on successful run)
        if state.retry_count > 0:
            print(f"\n  Resetting retry count...")
            state_manager.reset_strategy_retry_count(pipeline_id, strategy_name)
            
            # Verify
            updated_state = state_manager.get_strategy_state(pipeline_id, strategy_name)
            print(f"  New Retry Count: {updated_state.retry_count}")
        
        # Example: Increment retry count (on failure)
        print(f"\n  Simulating failure - incrementing retry count...")
        new_count = state_manager.increment_retry_count(pipeline_id, strategy_name)
        print(f"  New Retry Count: {new_count}")
    else:
        print(f"\nNo state found for {pipeline_id}:{strategy_name}")


async def start_scheduler(config_manager, global_config):
    """Start the enhanced scheduler (runs indefinitely)"""
    scheduler = EnhancedARQScheduler(
        config_manager=config_manager,
        global_config=global_config
    )
    
    print("Starting Enhanced ARQ Scheduler...")
    print(f"HTTP API will be available at http://localhost:{global_config.scheduler.http_port}")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        await scheduler.start()
    except KeyboardInterrupt:
        print("\nStopping scheduler...")
        await scheduler.stop()


async def demonstrate_api_usage():
    """Demonstrate using the scheduler HTTP API"""
    import aiohttp
    
    base_url = "http://localhost:8001"
    
    async with aiohttp.ClientSession() as session:
        # Check health
        print("\n=== Checking Scheduler Health ===")
        async with session.get(f"{base_url}/health") as resp:
            health = await resp.json()
            print(f"Status: {health['status']}")
            print(f"Scheduler: {health['scheduler']}")
        
        # List all strategies
        print("\n=== Listing All Strategies ===")
        async with session.get(f"{base_url}/api/strategies/all") as resp:
            data = await resp.json()
            print(f"Found {data['pipeline_count']} pipelines")
        
        # Get specific pipeline strategies
        pipeline_id = "users_sync_pipeline"
        print(f"\n=== Strategies for {pipeline_id} ===")
        async with session.get(f"{base_url}/api/pipelines/{pipeline_id}/strategies") as resp:
            data = await resp.json()
            print(f"Strategies: {data['count']}")
            for name, state in data['strategies'].items():
                print(f"  {name}: {state['status']}")
        
        # Get strategy history
        strategy_name = "hourly_hash_sync"
        print(f"\n=== History for {pipeline_id}:{strategy_name} ===")
        async with session.get(
            f"{base_url}/api/pipelines/{pipeline_id}/strategies/{strategy_name}/history?limit=10"
        ) as resp:
            data = await resp.json()
            print(f"History entries: {data['count']}")
            for entry in data['history'][:5]:
                print(f"  {entry['status']} at {entry['started_at']}")


async def demonstrate_lock_usage():
    """Demonstrate using execution locks in custom code"""
    from synctool.config.global_config_loader import get_global_config
    
    global_config = get_global_config()
    lock_manager = ExecutionLockManager(
        redis_url=global_config.redis.url
    )
    
    pipeline_id = "my_pipeline"
    strategy_name = "my_strategy"
    
    print("\n=== Demonstrating Lock Usage ===")
    
    # Example 1: Non-blocking pipeline lock
    print("\n1. Non-blocking pipeline lock:")
    async with lock_manager.acquire_pipeline_lock(
        pipeline_id=pipeline_id,
        strategy_name=strategy_name,
        wait_for_lock=False
    ) as acquired:
        if acquired:
            print("   ✓ Lock acquired, executing...")
            await asyncio.sleep(1)  # Simulate work
            print("   ✓ Work completed")
        else:
            print("   ✗ Lock not available, skipping")
    
    # Example 2: Blocking pipeline lock with timeout
    print("\n2. Blocking pipeline lock with timeout:")
    async with lock_manager.acquire_pipeline_lock(
        pipeline_id=pipeline_id,
        strategy_name=strategy_name,
        wait_for_lock=True,
        wait_interval=0.5
    ) as acquired:
        if acquired:
            print("   ✓ Lock acquired after waiting")
            await asyncio.sleep(1)
            print("   ✓ Work completed")
        else:
            print("   ✗ Timeout waiting for lock")
    
    # Example 3: Table DDL lock
    print("\n3. Table DDL lock:")
    async with lock_manager.acquire_table_ddl_lock(
        datastore_name="postgres_prod",
        schema_name="public",
        table_name="users",
        operation="read",
        wait_for_lock=False
    ) as acquired:
        if acquired:
            print("   ✓ Table lock acquired, safe to read")
            await asyncio.sleep(1)
            print("   ✓ Read operation completed")
        else:
            print("   ✗ Table locked for DDL, skipping")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())
    
    # Uncomment to run API usage examples (requires scheduler to be running)
    # asyncio.run(demonstrate_api_usage())
    
    # Uncomment to run lock usage examples
    # asyncio.run(demonstrate_lock_usage())

