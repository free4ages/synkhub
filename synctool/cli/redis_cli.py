#!/usr/bin/env python3
"""
Synctool Redis CLI

This CLI provides commands to manage Redis locks and monitor Redis state.
"""

import asyncio
import click
import json
import logging
import sys
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import redis

from ..config.global_config_loader import load_global_config


class RedisCLI:
    """Command-line interface for managing Redis locks"""
    
    def __init__(self, global_config):
        self.logger = logging.getLogger(__name__)
        self.global_config = global_config
        self.redis_client = None
        
    def _ensure_redis_connection(self):
        """Ensure Redis connection is established"""
        if self.redis_client is None:
            try:
                redis_url = self.global_config.redis.url
                self.redis_client = redis.from_url(redis_url)
                # Test connection
                self.redis_client.ping()
                self.logger.info(f"Connected to Redis at {redis_url}")
            except Exception as e:
                self.logger.error(f"Failed to connect to Redis: {e}")
                sys.exit(1)
        return self.redis_client
    
    def _close_redis_connection(self):
        """Close Redis connection"""
        if self.redis_client:
            try:
                self.redis_client.close()
                self.logger.info("Closed Redis connection")
            except Exception as e:
                self.logger.warning(f"Error closing Redis connection: {e}")
            finally:
                self.redis_client = None
    
    def _parse_lock_key(self, key: str) -> Dict[str, Any]:
        """Parse lock key to extract metadata"""
        if isinstance(key, bytes):
            key = key.decode()
        
        parts = key.split(":")
        
        if key.startswith("synctool:exec:pipeline:"):
            pipeline_id = key.replace("synctool:exec:pipeline:", "")
            return {
                "type": "pipeline_execution",
                "key": key,
                "pipeline_id": pipeline_id
            }
        
        elif key.startswith("synctool:enqueue:pipeline:"):
            pipeline_id = key.replace("synctool:enqueue:pipeline:", "")
            return {
                "type": "pipeline_enqueue",
                "key": key,
                "pipeline_id": pipeline_id
            }
        
        elif key.startswith("synctool:exec:table:"):
            # Format: synctool:exec:table:{datastore}:{schema}:{table}:{operation}
            remaining = key.replace("synctool:exec:table:", "")
            parts = remaining.split(":")
            if len(parts) >= 4:
                return {
                    "type": "table_ddl",
                    "key": key,
                    "datastore": parts[0],
                    "schema": parts[1],
                    "table": parts[2],
                    "operation": parts[3]
                }
        
        # Unknown lock type
        return {
            "type": "unknown",
            "key": key
        }
    
    def _get_lock_value_and_ttl(self, key: str) -> tuple[Optional[str], Optional[int]]:
        """Get lock value and TTL"""
        try:
            value = self.redis_client.get(key)
            ttl = self.redis_client.ttl(key)
            
            value_str = value.decode() if value else None
            ttl_val = ttl if ttl > 0 else None
            
            return value_str, ttl_val
        except Exception as e:
            self.logger.error(f"Error getting lock value/TTL for {key}: {e}")
            return None, None
    
    def list_locks(self, lock_type: Optional[str] = None, pattern: Optional[str] = None):
        """List all locks in Redis"""
        client = self._ensure_redis_connection()
        
        try:
            # Determine search pattern
            if pattern:
                search_pattern = pattern
            elif lock_type:
                if lock_type == "pipeline":
                    search_pattern = "synctool:exec:pipeline:*"
                elif lock_type == "enqueue":
                    search_pattern = "synctool:enqueue:pipeline:*"
                elif lock_type == "table":
                    search_pattern = "synctool:exec:table:*"
                else:
                    click.echo(f"Unknown lock type: {lock_type}")
                    click.echo("Valid types: pipeline, enqueue, table")
                    sys.exit(1)
            else:
                search_pattern = "synctool:*"
            
            # Get all matching keys
            keys = client.keys(search_pattern)
            
            if not keys:
                click.echo("\nNo locks found")
                return
            
            click.echo(f"\n{'='*100}")
            click.echo(f"Found {len(keys)} lock(s)")
            click.echo(f"{'='*100}\n")
            
            # Group locks by type
            locks_by_type = {
                "pipeline_execution": [],
                "pipeline_enqueue": [],
                "table_ddl": [],
                "unknown": []
            }
            
            for key in keys:
                lock_info = self._parse_lock_key(key)
                value, ttl = self._get_lock_value_and_ttl(key)
                lock_info["value"] = value
                lock_info["ttl"] = ttl
                locks_by_type[lock_info["type"]].append(lock_info)
            
            # Display pipeline execution locks
            if locks_by_type["pipeline_execution"]:
                click.echo(f"Pipeline Execution Locks ({len(locks_by_type['pipeline_execution'])}):")
                click.echo("-" * 100)
                for lock in locks_by_type["pipeline_execution"]:
                    ttl_str = f"{lock['ttl']}s" if lock['ttl'] else "No expiry"
                    click.echo(f"  Pipeline: {lock['pipeline_id']}")
                    click.echo(f"    Key: {lock['key']}")
                    click.echo(f"    Value: {lock['value']}")
                    click.echo(f"    TTL: {ttl_str}")
                    click.echo()
            
            # Display pipeline enqueue locks
            if locks_by_type["pipeline_enqueue"]:
                click.echo(f"Pipeline Enqueue Locks ({len(locks_by_type['pipeline_enqueue'])}):")
                click.echo("-" * 100)
                for lock in locks_by_type["pipeline_enqueue"]:
                    ttl_str = f"{lock['ttl']}s" if lock['ttl'] else "No expiry"
                    click.echo(f"  Pipeline: {lock['pipeline_id']}")
                    click.echo(f"    Key: {lock['key']}")
                    click.echo(f"    Value: {lock['value']}")
                    click.echo(f"    TTL: {ttl_str}")
                    click.echo()
            
            # Display table DDL locks
            if locks_by_type["table_ddl"]:
                click.echo(f"Table DDL Locks ({len(locks_by_type['table_ddl'])}):")
                click.echo("-" * 100)
                for lock in locks_by_type["table_ddl"]:
                    ttl_str = f"{lock['ttl']}s" if lock['ttl'] else "No expiry"
                    click.echo(f"  Table: {lock['datastore']}.{lock['schema']}.{lock['table']}")
                    click.echo(f"    Operation: {lock['operation']}")
                    click.echo(f"    Key: {lock['key']}")
                    click.echo(f"    Value: {lock['value']}")
                    click.echo(f"    TTL: {ttl_str}")
                    click.echo()
            
            # Display unknown locks
            if locks_by_type["unknown"]:
                click.echo(f"Other Synctool Keys ({len(locks_by_type['unknown'])}):")
                click.echo("-" * 100)
                for lock in locks_by_type["unknown"]:
                    ttl_str = f"{lock['ttl']}s" if lock['ttl'] else "No expiry"
                    click.echo(f"  Key: {lock['key']}")
                    click.echo(f"    Value: {lock['value']}")
                    click.echo(f"    TTL: {ttl_str}")
                    click.echo()
        
        except Exception as e:
            self.logger.error(f"Error listing locks: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def clear_locks(self, lock_type: Optional[str] = None, pattern: Optional[str] = None, 
                    pipeline_id: Optional[str] = None, yes: bool = False):
        """Clear/remove locks from Redis"""
        client = self._ensure_redis_connection()
        
        try:
            # Determine which locks to delete
            keys_to_delete = []
            
            if pipeline_id:
                # Clear specific pipeline locks
                exec_key = f"synctool:exec:pipeline:{pipeline_id}"
                enqueue_key = f"synctool:enqueue:pipeline:{pipeline_id}"
                
                if client.exists(exec_key):
                    keys_to_delete.append(exec_key)
                if client.exists(enqueue_key):
                    keys_to_delete.append(enqueue_key)
                
                if not keys_to_delete:
                    click.echo(f"No locks found for pipeline: {pipeline_id}")
                    return
            
            elif pattern:
                # Clear locks matching pattern
                keys_to_delete = client.keys(pattern)
            
            elif lock_type:
                # Clear locks by type
                if lock_type == "pipeline":
                    keys_to_delete = client.keys("synctool:exec:pipeline:*")
                elif lock_type == "enqueue":
                    keys_to_delete = client.keys("synctool:enqueue:pipeline:*")
                elif lock_type == "table":
                    keys_to_delete = client.keys("synctool:exec:table:*")
                elif lock_type == "all":
                    keys_to_delete = client.keys("synctool:*")
                else:
                    click.echo(f"Unknown lock type: {lock_type}")
                    click.echo("Valid types: pipeline, enqueue, table, all")
                    sys.exit(1)
            
            else:
                # Default: clear all synctool locks
                keys_to_delete = client.keys("synctool:*")
            
            if not keys_to_delete:
                click.echo("\nNo locks found to clear")
                return
            
            # Display what will be deleted
            click.echo(f"\n{'='*100}")
            click.echo(f"Found {len(keys_to_delete)} lock(s) to clear:")
            click.echo(f"{'='*100}\n")
            
            for key in keys_to_delete[:20]:  # Show first 20
                if isinstance(key, bytes):
                    key = key.decode()
                lock_info = self._parse_lock_key(key)
                click.echo(f"  - {lock_info['type']}: {key}")
            
            if len(keys_to_delete) > 20:
                click.echo(f"  ... and {len(keys_to_delete) - 20} more")
            
            click.echo()
            
            # Confirm deletion
            if not yes:
                if not click.confirm(f"Do you want to delete these {len(keys_to_delete)} lock(s)?"):
                    click.echo("Cancelled")
                    return
            
            # Delete locks
            deleted_count = 0
            for key in keys_to_delete:
                try:
                    if client.delete(key):
                        deleted_count += 1
                except Exception as e:
                    self.logger.error(f"Error deleting key {key}: {e}")
            
            click.echo(f"\n✓ Successfully deleted {deleted_count} lock(s)\n")
        
        except Exception as e:
            self.logger.error(f"Error clearing locks: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def get_redis_info(self):
        """Get Redis server information"""
        client = self._ensure_redis_connection()
        
        try:
            info = client.info()
            
            click.echo(f"\n{'='*100}")
            click.echo("Redis Server Information")
            click.echo(f"{'='*100}\n")
            
            # Server section
            click.echo("Server:")
            click.echo(f"  Redis Version: {info.get('redis_version', 'N/A')}")
            click.echo(f"  OS: {info.get('os', 'N/A')}")
            click.echo(f"  Uptime (days): {info.get('uptime_in_days', 'N/A')}")
            click.echo()
            
            # Clients section
            click.echo("Clients:")
            click.echo(f"  Connected Clients: {info.get('connected_clients', 'N/A')}")
            click.echo(f"  Blocked Clients: {info.get('blocked_clients', 'N/A')}")
            click.echo()
            
            # Memory section
            click.echo("Memory:")
            used_memory_mb = info.get('used_memory', 0) / (1024 * 1024)
            click.echo(f"  Used Memory: {used_memory_mb:.2f} MB")
            click.echo(f"  Peak Memory: {info.get('used_memory_peak_human', 'N/A')}")
            click.echo()
            
            # Stats section
            click.echo("Stats:")
            click.echo(f"  Total Connections Received: {info.get('total_connections_received', 'N/A')}")
            click.echo(f"  Total Commands Processed: {info.get('total_commands_processed', 'N/A')}")
            click.echo(f"  Instantaneous Ops/Sec: {info.get('instantaneous_ops_per_sec', 'N/A')}")
            click.echo()
            
            # Keyspace section
            click.echo("Keyspace:")
            db_info = info.get('db0', {})
            if db_info:
                click.echo(f"  DB 0 Keys: {db_info.get('keys', 'N/A')}")
                click.echo(f"  DB 0 Expires: {db_info.get('expires', 'N/A')}")
            else:
                click.echo("  No keys in DB 0")
            click.echo()
            
            # Count synctool keys
            synctool_keys = client.keys("synctool:*")
            click.echo(f"Synctool Keys: {len(synctool_keys)}")
            
            # Break down by type
            pipeline_exec = len(client.keys("synctool:exec:pipeline:*"))
            pipeline_enqueue = len(client.keys("synctool:enqueue:pipeline:*"))
            table_ddl = len(client.keys("synctool:exec:table:*"))
            
            click.echo(f"  Pipeline Execution Locks: {pipeline_exec}")
            click.echo(f"  Pipeline Enqueue Locks: {pipeline_enqueue}")
            click.echo(f"  Table DDL Locks: {table_ddl}")
            click.echo()
        
        except Exception as e:
            self.logger.error(f"Error getting Redis info: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def check_lock(self, pipeline_id: str):
        """Check if a specific pipeline is locked"""
        client = self._ensure_redis_connection()
        
        try:
            exec_key = f"synctool:exec:pipeline:{pipeline_id}"
            enqueue_key = f"synctool:enqueue:pipeline:{pipeline_id}"
            
            click.echo(f"\n{'='*100}")
            click.echo(f"Lock Status for Pipeline: {pipeline_id}")
            click.echo(f"{'='*100}\n")
            
            # Check execution lock
            exec_exists = client.exists(exec_key)
            if exec_exists:
                value, ttl = self._get_lock_value_and_ttl(exec_key)
                ttl_str = f"{ttl}s" if ttl else "No expiry"
                click.echo(f"Execution Lock: LOCKED")
                click.echo(f"  Key: {exec_key}")
                click.echo(f"  Value: {value}")
                click.echo(f"  TTL: {ttl_str}")
            else:
                click.echo(f"Execution Lock: AVAILABLE")
            
            click.echo()
            
            # Check enqueue lock
            enqueue_exists = client.exists(enqueue_key)
            if enqueue_exists:
                value, ttl = self._get_lock_value_and_ttl(enqueue_key)
                ttl_str = f"{ttl}s" if ttl else "No expiry"
                click.echo(f"Enqueue Lock: LOCKED")
                click.echo(f"  Key: {enqueue_key}")
                click.echo(f"  Value: {value}")
                click.echo(f"  TTL: {ttl_str}")
            else:
                click.echo(f"Enqueue Lock: AVAILABLE")
            
            click.echo()
            
            if exec_exists or enqueue_exists:
                click.echo(f"Overall Status: LOCKED")
            else:
                click.echo(f"Overall Status: AVAILABLE")
            
            click.echo()
        
        except Exception as e:
            self.logger.error(f"Error checking lock: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def list_arq_jobs(self, show_completed: bool = False):
        """List ARQ jobs in the queue"""
        client = self._ensure_redis_connection()
        
        try:
            # ARQ uses these key patterns:
            # - arq:queue - main job queue (Redis zset/sorted set in newer ARQ, or list in older)
            # - arq:in-progress - jobs currently being processed (set)
            # - arq:job:{job_id} - job data
            # - arq:result:{job_id} - job results
            
            click.echo(f"\n{'='*100}")
            click.echo("ARQ Job Queue Status")
            click.echo(f"{'='*100}\n")
            
            # Check if ARQ keys exist at all
            arq_keys = client.keys("arq:*")
            if not arq_keys:
                click.echo("No ARQ keys found in Redis.")
                click.echo("This could mean:")
                click.echo("  - ARQ scheduler has never been started")
                click.echo("  - No jobs have been enqueued yet")
                click.echo("  - Redis was recently cleared")
                click.echo()
                return
            
            # Get queued jobs - ARQ can use either zset (newer) or list (older)
            queue_length = 0
            queued_jobs = []
            try:
                if client.exists("arq:queue"):
                    key_type = client.type("arq:queue").decode() if isinstance(client.type("arq:queue"), bytes) else client.type("arq:queue")
                    
                    if key_type == "zset":
                        # Newer ARQ uses sorted set
                        queue_length = client.zcard("arq:queue")
                        if queue_length > 0:
                            # Get jobs ordered by score (enqueue time)
                            queued_jobs = client.zrange("arq:queue", 0, -1, withscores=False)
                    elif key_type == "list":
                        # Older ARQ uses list
                        queue_length = client.llen("arq:queue")
                        if queue_length > 0:
                            queued_jobs = client.lrange("arq:queue", 0, -1)
                    else:
                        click.echo(f"Warning: arq:queue exists but is type '{key_type}', expected 'zset' or 'list'")
            except Exception as e:
                click.echo(f"Warning: Could not read arq:queue - {e}")
            
            click.echo(f"Queued Jobs: {queue_length}")
            
            if queue_length > 0 and queued_jobs:
                click.echo("\nQueued Jobs:")
                click.echo("-" * 100)
                
                for i, job_data in enumerate(queued_jobs, 1):
                    try:
                        # Try to import and use msgpack
                        try:
                            import msgpack
                            msgpack_available = True
                        except ImportError:
                            msgpack_available = False
                            click.echo(f"\n  Job {i}: (msgpack not available, install with: pip install msgpack)")
                        
                        if msgpack_available:
                            try:
                                job_info = msgpack.unpackb(job_data, raw=False)
                                
                                job_id = job_info.get('job_id', 'unknown')
                                function = job_info.get('function', 'unknown')
                                enqueue_time_ms = job_info.get('enqueue_time_ms', 0)
                                
                                # Convert enqueue time to human-readable
                                if enqueue_time_ms:
                                    from datetime import datetime, timezone
                                    enqueue_time = datetime.fromtimestamp(enqueue_time_ms / 1000, tz=timezone.utc)
                                    enqueue_str = enqueue_time.strftime("%Y-%m-%d %H:%M:%S UTC")
                                else:
                                    enqueue_str = "Unknown"
                                
                                click.echo(f"\n  Job {i}:")
                                click.echo(f"    Job ID: {job_id}")
                                click.echo(f"    Function: {function}")
                                click.echo(f"    Enqueued: {enqueue_str}")
                                
                                # Try to get more details from job data
                                job_key = f"arq:job:{job_id}"
                                if client.exists(job_key):
                                    job_details = client.get(job_key)
                                    if job_details:
                                        try:
                                            job_dict = msgpack.unpackb(job_details, raw=False)
                                            # Extract pipeline and strategy info if available
                                            if 'args' in job_dict and len(job_dict['args']) >= 5:
                                                pipeline_id = job_dict['args'][4] if len(job_dict['args']) > 4 else None
                                                strategy_name = job_dict['args'][1] if len(job_dict['args']) > 1 else None
                                                run_id = job_dict['args'][2] if len(job_dict['args']) > 2 else None
                                                
                                                if pipeline_id:
                                                    click.echo(f"    Pipeline: {pipeline_id}")
                                                if strategy_name:
                                                    click.echo(f"    Strategy: {strategy_name}")
                                                if run_id:
                                                    click.echo(f"    Run ID: {run_id}")
                                        except Exception as parse_err:
                                            self.logger.debug(f"Could not parse job details: {parse_err}")
                            except Exception as unpack_err:
                                # Show raw info if we can't unpack
                                click.echo(f"\n  Job {i}:")
                                click.echo(f"    Raw data size: {len(job_data)} bytes")
                                click.echo(f"    Parse error: {unpack_err}")
                                # Try to show hex preview
                                hex_preview = job_data[:50].hex() if isinstance(job_data, bytes) else str(job_data)[:100]
                                click.echo(f"    Data preview: {hex_preview}...")
                        
                    except Exception as e:
                        click.echo(f"\n  Job {i}: Error displaying job - {e}")
            
            # Get in-progress jobs - ARQ uses individual keys like arq:in-progress:{job_id}
            in_progress_keys = None
            try:
                # First check if there's a single set (older ARQ)
                if client.exists("arq:in-progress"):
                    key_type = client.type("arq:in-progress").decode() if isinstance(client.type("arq:in-progress"), bytes) else client.type("arq:in-progress")
                    if key_type == "set":
                        in_progress_keys = client.smembers("arq:in-progress")
                
                # If not, look for individual in-progress keys (newer ARQ)
                if not in_progress_keys:
                    pattern_keys = client.keys("arq:in-progress:*")
                    if pattern_keys:
                        # Extract job IDs from keys
                        in_progress_keys = set()
                        for key in pattern_keys:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            # Extract job ID from arq:in-progress:{job_id}
                            job_id = key_str.replace("arq:in-progress:", "")
                            in_progress_keys.add(job_id.encode() if isinstance(key, bytes) else job_id)
            except Exception as e:
                click.echo(f"\nWarning: Could not read in-progress jobs - {e}")
            
            if in_progress_keys:
                click.echo(f"\n\nIn-Progress Jobs: {len(in_progress_keys)}")
                click.echo("-" * 100)
                
                for i, job_id_bytes in enumerate(in_progress_keys, 1):
                    job_id = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
                    
                    click.echo(f"\n  Job {i}:")
                    click.echo(f"    Job ID: {job_id}")
                    
                    # Try to get job details
                    job_key = f"arq:job:{job_id}"
                    if client.exists(job_key):
                        try:
                            import msgpack
                            job_details = client.get(job_key)
                            job_dict = msgpack.unpackb(job_details, raw=False)
                            
                            function = job_dict.get('function', 'unknown')
                            click.echo(f"    Function: {function}")
                            
                            # Extract pipeline and strategy info if available
                            if 'args' in job_dict and len(job_dict['args']) >= 5:
                                pipeline_id = job_dict['args'][4] if len(job_dict['args']) > 4 else None
                                strategy_name = job_dict['args'][1] if len(job_dict['args']) > 1 else None
                                run_id = job_dict['args'][2] if len(job_dict['args']) > 2 else None
                                
                                if pipeline_id:
                                    click.echo(f"    Pipeline: {pipeline_id}")
                                if strategy_name:
                                    click.echo(f"    Strategy: {strategy_name}")
                                if run_id:
                                    click.echo(f"    Run ID: {run_id}")
                        except Exception as e:
                            self.logger.debug(f"Could not parse job details: {e}")
            else:
                click.echo(f"\n\nIn-Progress Jobs: 0")
            
            # Show completed jobs if requested
            if show_completed:
                # Look for result keys
                result_keys = client.keys("arq:result:*")
                if result_keys:
                    click.echo(f"\n\nCompleted Jobs (with results): {len(result_keys)}")
                    click.echo("-" * 100)
                    
                    for i, result_key in enumerate(result_keys[:20], 1):  # Limit to 20
                        result_key_str = result_key.decode() if isinstance(result_key, bytes) else result_key
                        job_id = result_key_str.replace("arq:result:", "")
                        
                        click.echo(f"\n  Job {i}:")
                        click.echo(f"    Job ID: {job_id}")
                        
                        # Get TTL
                        ttl = client.ttl(result_key)
                        if ttl > 0:
                            click.echo(f"    Result TTL: {ttl}s")
                        
                        # Try to get result
                        try:
                            import msgpack
                            result_data = client.get(result_key)
                            if result_data:
                                result = msgpack.unpackb(result_data, raw=False)
                                click.echo(f"    Result: {result}")
                        except Exception as e:
                            self.logger.debug(f"Could not parse result: {e}")
                    
                    if len(result_keys) > 20:
                        click.echo(f"\n  ... and {len(result_keys) - 20} more")
            
            # Get retry jobs
            retry_keys = client.keys("arq:retry:*")
            if retry_keys:
                click.echo(f"\n\nRetrying Jobs: {len(retry_keys)}")
                click.echo("-" * 100)
                
                for i, retry_key in enumerate(retry_keys[:10], 1):
                    retry_key_str = retry_key.decode() if isinstance(retry_key, bytes) else retry_key
                    job_id = retry_key_str.replace("arq:retry:", "")
                    
                    try:
                        retry_count = client.get(retry_key)
                        retry_count_str = retry_count.decode() if retry_count and isinstance(retry_count, bytes) else str(retry_count)
                        ttl = client.ttl(retry_key)
                        ttl_str = f"{ttl}s" if ttl > 0 else "no expiry"
                        
                        click.echo(f"\n  Job {i}:")
                        click.echo(f"    Job ID: {job_id[:36]}")
                        click.echo(f"    Retry Count: {retry_count_str}")
                        click.echo(f"    TTL: {ttl_str}")
                    except Exception as e:
                        click.echo(f"\n  Job {i}: Error reading retry info - {e}")
                
                if len(retry_keys) > 10:
                    click.echo(f"\n  ... and {len(retry_keys) - 10} more")
            
            # Summary
            click.echo(f"\n\n{'='*100}")
            click.echo("Summary:")
            click.echo(f"  Total Queued: {queue_length}")
            click.echo(f"  Total In-Progress: {len(in_progress_keys) if in_progress_keys else 0}")
            click.echo(f"  Total Retrying: {len(retry_keys) if retry_keys else 0}")
            if show_completed:
                click.echo(f"  Total With Results: {len(result_keys) if result_keys else 0}")
            click.echo(f"{'='*100}\n")
        
        except Exception as e:
            self.logger.error(f"Error listing ARQ jobs: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def debug_arq_keys(self):
        """Debug ARQ keys to diagnose type issues"""
        client = self._ensure_redis_connection()
        
        try:
            click.echo(f"\n{'='*100}")
            click.echo("ARQ Key Diagnostics")
            click.echo(f"{'='*100}\n")
            
            # Get all ARQ keys
            arq_keys = client.keys("arq:*")
            
            if not arq_keys:
                click.echo("No ARQ keys found in Redis.")
                click.echo("\nPossible reasons:")
                click.echo("  1. ARQ scheduler has not enqueued any jobs yet")
                click.echo("  2. All jobs have been processed and cleaned up")
                click.echo("  3. ARQ is using a different Redis database")
                click.echo()
                
                # Show what database we're connected to
                info = client.info("keyspace")
                click.echo(f"Connected to Redis database: {client.connection_pool.connection_kwargs.get('db', 0)}")
                click.echo(f"Total keys in all databases: {info}")
                click.echo()
                return
            
            click.echo(f"Found {len(arq_keys)} ARQ key(s):\n")
            
            # Group by key pattern
            keys_by_pattern = {}
            for key in arq_keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                
                # Determine pattern
                if key_str == "arq:queue":
                    pattern = "arq:queue"
                elif key_str == "arq:in-progress":
                    pattern = "arq:in-progress"
                elif key_str.startswith("arq:job:"):
                    pattern = "arq:job:*"
                elif key_str.startswith("arq:result:"):
                    pattern = "arq:result:*"
                else:
                    pattern = "arq:other"
                
                if pattern not in keys_by_pattern:
                    keys_by_pattern[pattern] = []
                keys_by_pattern[pattern].append(key_str)
            
            # Display grouped keys
            for pattern, keys in sorted(keys_by_pattern.items()):
                click.echo(f"{pattern}: {len(keys)} key(s)")
                
                # Show details for main keys
                if pattern in ["arq:queue", "arq:in-progress"]:
                    for key in keys:
                        key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                        ttl = client.ttl(key)
                        
                        # ARQ queue can be zset (newer) or list (older)
                        if pattern == "arq:queue":
                            expected_type = "zset or list"
                            status = "✓" if key_type in ["zset", "list"] else f"✗ (expected {expected_type})"
                        else:
                            expected_type = "set"
                            status = "✓" if key_type == expected_type else f"✗ (expected {expected_type})"
                        
                        click.echo(f"  {key}")
                        click.echo(f"    Type: {key_type} {status}")
                        if ttl >= 0:
                            click.echo(f"    TTL: {ttl}s")
                        else:
                            click.echo(f"    TTL: No expiry")
                        
                        # Show size
                        try:
                            if key_type == "list":
                                size = client.llen(key)
                                click.echo(f"    Size: {size} items")
                            elif key_type == "set":
                                size = client.scard(key)
                                click.echo(f"    Size: {size} members")
                            elif key_type == "zset":
                                size = client.zcard(key)
                                click.echo(f"    Size: {size} members")
                            elif key_type == "string":
                                value = client.get(key)
                                click.echo(f"    Value: {value}")
                        except Exception as e:
                            click.echo(f"    Error reading size: {e}")
                else:
                    click.echo(f"  (showing first 5)")
                    for key in keys[:5]:
                        key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                        click.echo(f"  {key} (type: {key_type})")
                    
                    if len(keys) > 5:
                        click.echo(f"  ... and {len(keys) - 5} more")
                
                click.echo()
            
            # Check for type mismatches
            issues = []
            if "arq:queue" in keys_by_pattern:
                for key in keys_by_pattern["arq:queue"]:
                    key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                    if key_type not in ["zset", "list"]:
                        issues.append(f"arq:queue is type '{key_type}' but should be 'zset' (newer ARQ) or 'list' (older ARQ)")
            
            if "arq:in-progress" in keys_by_pattern:
                for key in keys_by_pattern["arq:in-progress"]:
                    key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                    if key_type != "set":
                        issues.append(f"arq:in-progress is type '{key_type}' but should be 'set'")
            
            if issues:
                click.echo("⚠️  Issues Detected:")
                for issue in issues:
                    click.echo(f"  - {issue}")
                click.echo()
                click.echo("To fix, delete the problematic keys:")
                click.echo("  python -m synctool.cli.redis_cli clear --pattern 'arq:queue' --yes")
                click.echo("  python -m synctool.cli.redis_cli clear --pattern 'arq:in-progress' --yes")
                click.echo()
            else:
                click.echo("✓ No type issues detected\n")
        
        except Exception as e:
            self.logger.error(f"Error debugging ARQ keys: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def clear_arq_queue(self, yes: bool = False, include_in_progress: bool = False, 
                        include_retry: bool = False, include_all: bool = False):
        """Clear jobs from ARQ queue with various options"""
        client = self._ensure_redis_connection()
        
        try:
            # Collect what we're going to delete
            queue_length = 0
            in_progress_keys = []
            retry_keys = []
            job_keys = []
            result_keys = []
            
            # Get queue size
            try:
                if client.exists("arq:queue"):
                    key_type = client.type("arq:queue").decode() if isinstance(client.type("arq:queue"), bytes) else client.type("arq:queue")
                    if key_type == "zset":
                        queue_length = client.zcard("arq:queue")
                    elif key_type == "list":
                        queue_length = client.llen("arq:queue")
            except Exception as e:
                click.echo(f"\nWarning: Could not read arq:queue - {e}")
            
            # Get in-progress jobs
            if include_in_progress or include_all:
                try:
                    # Check for single set (older ARQ)
                    if client.exists("arq:in-progress"):
                        key_type = client.type("arq:in-progress").decode() if isinstance(client.type("arq:in-progress"), bytes) else client.type("arq:in-progress")
                        if key_type == "set":
                            in_progress_keys.append(b"arq:in-progress")
                    
                    # Check for individual keys (newer ARQ)
                    pattern_keys = client.keys("arq:in-progress:*")
                    if pattern_keys:
                        in_progress_keys.extend(pattern_keys)
                except Exception as e:
                    click.echo(f"Warning: Could not read in-progress jobs - {e}")
            
            # Get retry jobs
            if include_retry or include_all:
                try:
                    retry_keys = client.keys("arq:retry:*")
                except Exception as e:
                    click.echo(f"Warning: Could not read retry jobs - {e}")
            
            # Get job data and results if clearing queue
            if queue_length > 0 or include_all:
                job_keys = client.keys("arq:job:*")
                result_keys = client.keys("arq:result:*")
            
            # Check if there's anything to clear
            if (queue_length == 0 and not in_progress_keys and not retry_keys and 
                not job_keys and not result_keys):
                click.echo("\nNo ARQ jobs to clear")
                return
            
            # Display what will be cleared
            click.echo(f"\n{'='*100}")
            click.echo("ARQ Jobs Clear")
            click.echo(f"{'='*100}\n")
            click.echo(f"Queued jobs: {queue_length}")
            click.echo(f"In-progress jobs: {len(in_progress_keys)}")
            click.echo(f"Retrying jobs: {len(retry_keys)}")
            click.echo(f"Job data entries: {len(job_keys)}")
            click.echo(f"Result entries: {len(result_keys)}")
            click.echo()
            
            if in_progress_keys:
                click.echo("⚠️  WARNING: Clearing in-progress jobs may cause workers to fail!")
            if retry_keys:
                click.echo("⚠️  WARNING: Clearing retry jobs will prevent failed jobs from retrying!")
            
            click.echo()
            
            if not yes:
                if not click.confirm("Do you want to clear these ARQ jobs?"):
                    click.echo("Cancelled")
                    return
            
            # Clear the items
            deleted_count = 0
            
            # Clear queue
            if queue_length > 0:
                client.delete("arq:queue")
                deleted_count += 1
                click.echo(f"✓ Cleared queue ({queue_length} jobs)")
            
            # Clear in-progress
            if in_progress_keys:
                for key in in_progress_keys:
                    client.delete(key)
                deleted_count += len(in_progress_keys)
                click.echo(f"✓ Cleared {len(in_progress_keys)} in-progress job(s)")
            
            # Clear retry
            if retry_keys:
                for key in retry_keys:
                    client.delete(key)
                deleted_count += len(retry_keys)
                click.echo(f"✓ Cleared {len(retry_keys)} retrying job(s)")
            
            # Clear job data
            if job_keys:
                for key in job_keys:
                    client.delete(key)
                deleted_count += len(job_keys)
                click.echo(f"✓ Cleared {len(job_keys)} job data entrie(s)")
            
            # Clear results
            if result_keys:
                for key in result_keys:
                    client.delete(key)
                deleted_count += len(result_keys)
                click.echo(f"✓ Cleared {len(result_keys)} result entrie(s)")
            
            click.echo(f"\n✓ Successfully cleared {deleted_count} total key(s)\n")
        
        except Exception as e:
            self.logger.error(f"Error clearing ARQ jobs: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def snapshot_redis(self):
        """Take a complete snapshot of Redis state"""
        client = self._ensure_redis_connection()
        
        try:
            click.echo(f"\n{'='*100}")
            click.echo("Redis Complete Snapshot")
            click.echo(f"{'='*100}\n")
            
            # Get database info
            db_num = client.connection_pool.connection_kwargs.get('db', 0)
            click.echo(f"Database: {db_num}")
            
            # Get all keys
            all_keys = client.keys("*")
            click.echo(f"Total keys: {len(all_keys)}\n")
            
            if not all_keys:
                click.echo("No keys found in Redis.\n")
                return
            
            # Group by prefix
            keys_by_prefix = {}
            for key in all_keys:
                key_str = key.decode() if isinstance(key, bytes) else key
                prefix = key_str.split(":")[0]
                
                if prefix not in keys_by_prefix:
                    keys_by_prefix[prefix] = []
                keys_by_prefix[prefix].append(key_str)
            
            # Display by prefix
            for prefix in sorted(keys_by_prefix.keys()):
                keys = keys_by_prefix[prefix]
                click.echo(f"\n{prefix}:* ({len(keys)} key(s))")
                click.echo("-" * 80)
                
                # Show first 10 keys of each prefix
                for key in keys[:10]:
                    key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                    ttl = client.ttl(key)
                    
                    # Get size/count
                    size_info = ""
                    try:
                        if key_type == "string":
                            value = client.get(key)
                            if value:
                                value_str = value.decode() if isinstance(value, bytes) else str(value)
                                size_info = f"value: {value_str[:50]}{'...' if len(value_str) > 50 else ''}"
                        elif key_type == "list":
                            size = client.llen(key)
                            size_info = f"{size} items"
                        elif key_type == "set":
                            size = client.scard(key)
                            size_info = f"{size} members"
                        elif key_type == "zset":
                            size = client.zcard(key)
                            size_info = f"{size} members"
                        elif key_type == "hash":
                            size = client.hlen(key)
                            size_info = f"{size} fields"
                    except Exception as e:
                        size_info = f"error: {e}"
                    
                    ttl_str = f"{ttl}s" if ttl > 0 else "no expiry"
                    click.echo(f"  {key} ({key_type}, {size_info}, ttl: {ttl_str})")
                
                if len(keys) > 10:
                    click.echo(f"  ... and {len(keys) - 10} more")
            
            click.echo(f"\n{'='*100}")
            
            # Show ARQ-specific information
            arq_queue_keys = [k for k in all_keys if b'arq:queue' in k or 'arq:queue' in str(k)]
            if arq_queue_keys:
                click.echo("\n⚠️  ARQ Queue Keys Detected:")
                for key in arq_queue_keys:
                    key_str = key.decode() if isinstance(key, bytes) else key
                    key_type = client.type(key).decode() if isinstance(client.type(key), bytes) else client.type(key)
                    
                    if key_type == "zset":
                        count = client.zcard(key)
                        click.echo(f"  {key_str}: {count} jobs (zset)")
                        
                        # Show first few jobs
                        if count > 0:
                            jobs = client.zrange(key, 0, 2, withscores=True)
                            click.echo(f"    First {min(3, count)} job(s):")
                            for job_data, score in jobs:
                                try:
                                    import msgpack
                                    job_info = msgpack.unpackb(job_data, raw=False)
                                    job_id = job_info.get('job_id', 'unknown')[:36]
                                    function = job_info.get('function', 'unknown')
                                    click.echo(f"      - {job_id} ({function}) score: {score}")
                                except:
                                    click.echo(f"      - Unable to parse job")
                    elif key_type == "list":
                        count = client.llen(key)
                        click.echo(f"  {key_str}: {count} jobs (list)")
            
            click.echo()
        
        except Exception as e:
            self.logger.error(f"Error taking Redis snapshot: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


@click.group()
@click.option('--global-config', default=None, help='Path to global config YAML')
@click.option('--log-level', default='INFO', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
              help='Set the logging level')
@click.pass_context
def cli(ctx, global_config, log_level):
    """Synctool Redis CLI - Manage Redis locks and monitor Redis state"""
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Load global config
    if global_config:
        global_cfg = load_global_config(global_config)
    else:
        global_cfg = load_global_config()
    
    ctx.ensure_object(dict)
    ctx.obj['global_config'] = global_cfg
    ctx.obj['cli'] = RedisCLI(global_cfg)


@cli.command('list')
@click.option('--type', 'lock_type', help='Filter by lock type (pipeline, enqueue, table)')
@click.option('--pattern', help='Custom Redis key pattern (e.g., "synctool:exec:*")')
@click.pass_context
def list_locks(ctx, lock_type, pattern):
    """List all locks in Redis"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.list_locks(lock_type=lock_type, pattern=pattern)
    finally:
        cli_instance._close_redis_connection()


@cli.command('clear')
@click.option('--type', 'lock_type', help='Clear locks by type (pipeline, enqueue, table, all)')
@click.option('--pattern', help='Clear locks matching custom pattern')
@click.option('--pipeline-id', help='Clear locks for specific pipeline')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def clear_locks(ctx, lock_type, pattern, pipeline_id, yes):
    """Clear/remove locks from Redis"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.clear_locks(
            lock_type=lock_type, 
            pattern=pattern, 
            pipeline_id=pipeline_id,
            yes=yes
        )
    finally:
        cli_instance._close_redis_connection()


@cli.command('info')
@click.pass_context
def redis_info(ctx):
    """Get Redis server information and statistics"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.get_redis_info()
    finally:
        cli_instance._close_redis_connection()


@cli.command('check')
@click.option('--pipeline-id', required=True, help='Pipeline ID to check')
@click.pass_context
def check_lock(ctx, pipeline_id):
    """Check if a specific pipeline is locked"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.check_lock(pipeline_id)
    finally:
        cli_instance._close_redis_connection()


@cli.command('jobs')
@click.option('--show-completed', is_flag=True, help='Show completed jobs with results')
@click.pass_context
def list_jobs(ctx, show_completed):
    """List all ARQ jobs in the queue"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.list_arq_jobs(show_completed=show_completed)
    finally:
        cli_instance._close_redis_connection()


@cli.command('clear-queue')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
@click.option('--in-progress', is_flag=True, help='Also clear in-progress jobs (may cause worker failures)')
@click.option('--retry', is_flag=True, help='Also clear retrying jobs (prevents retry attempts)')
@click.option('--all', 'clear_all', is_flag=True, help='Clear everything: queue, in-progress, retry, job data, and results')
@click.pass_context
def clear_queue(ctx, yes, in_progress, retry, clear_all):
    """Clear jobs from ARQ queue with various options
    
    Examples:
    
      # Clear only queued jobs (safe)
      synctool-redis clear-queue --yes
      
      # Clear queued + in-progress jobs
      synctool-redis clear-queue --in-progress --yes
      
      # Clear queued + retrying jobs
      synctool-redis clear-queue --retry --yes
      
      # Clear everything (nuclear option)
      synctool-redis clear-queue --all --yes
    """
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.clear_arq_queue(
            yes=yes, 
            include_in_progress=in_progress,
            include_retry=retry,
            include_all=clear_all
        )
    finally:
        cli_instance._close_redis_connection()


@cli.command('debug-arq')
@click.pass_context
def debug_arq(ctx):
    """Debug ARQ keys and diagnose type issues"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.debug_arq_keys()
    finally:
        cli_instance._close_redis_connection()


@cli.command('snapshot')
@click.pass_context
def snapshot(ctx):
    """Take a complete snapshot of all Redis keys"""
    cli_instance = ctx.obj['cli']
    try:
        cli_instance.snapshot_redis()
    finally:
        cli_instance._close_redis_connection()


if __name__ == "__main__":
    cli()

